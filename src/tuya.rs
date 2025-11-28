use anyhow::{anyhow, Context, Result};
use futures::future::select_all;
use futures::future::FutureExt;
use log::{debug, warn};
use rumqttc::QoS;
use rust_async_tuyapi::mesparse::CommandType;
use rust_async_tuyapi::PayloadStruct;
use rust_async_tuyapi::{tuyadevice::TuyaDevice, Payload};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::{net::IpAddr, str::FromStr, time::Duration};
use tokio::sync::RwLock;
use tokio::time::timeout;

use crate::mqtt::Capabilities;
use crate::mqtt::Ct;
use crate::mqtt::DeviceColor;
use crate::mqtt::Hs;
use crate::mqtt::MAX_SUPPORTED_CT;
use crate::mqtt::MIN_SUPPORTED_CT;
use crate::mqtt::{MqttClient, MqttDevice};

const DEFAULT_POWER_ON_FIELD: &str = "20";
const DEFAULT_MODE_FIELD: &str = "21";
const DEFAULT_BRIGHTNESS_FIELD: &str = "22";
const DEFAULT_COLOR_TEMP_FIELD: &str = "23";
const DEFAULT_COLOR_FIELD: &str = "24";

/// Polling interval for querying device status (in milliseconds)
/// Community research shows aggressive polling (< 10s) can trigger resource
/// exhaustion in v3.4 device firmware, leading to "stuck" devices
const POLL_INTERVAL_MS: u64 = 3_000;

/// Heartbeat interval to keep TCP connection alive (in milliseconds)
/// v3.4 devices may close idle connections; heartbeats prevent this
const HEARTBEAT_INTERVAL_MS: u64 = 15_000;

/// Timeout for individual operations (connect, get, set)
const OPERATION_TIMEOUT_MS: u64 = 5_000;

/// Timeout for initial connection establishment
const CONNECT_TIMEOUT_MS: u64 = 9_000;

/// Timeout for receiving messages before considering connection stale
/// Must be longer than HEARTBEAT_INTERVAL_MS + OPERATION_TIMEOUT_MS to avoid
/// false positives when waiting for heartbeat responses
const RECEIVE_TIMEOUT_MS: u64 = 25_000;

#[derive(Clone, Debug, Deserialize)]
pub struct TuyaDeviceConfig {
    pub name: String,
    pub id: String,
    pub local_key: String,
    pub ip: String,
    pub version: String,
    pub max_brightness: Option<f32>,
    pub power_on_field: Option<String>,
    pub capabilities: Option<Capabilities>,
    pub topic: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TuyaConfig {
    pub devices: HashMap<String, TuyaDeviceConfig>,
}

type TuyaDps = serde_json::Value;

#[derive(Deserialize)]
struct TuyaDpsPayload {
    dps: Option<TuyaDps>,
}

pub fn tuya_to_mqtt(
    messages: Vec<rust_async_tuyapi::mesparse::Message>,
    config: &TuyaDeviceConfig,
) -> Result<MqttDevice> {
    let first = messages
        .first()
        .context("Expected Tuya response to contain at least one message")?;

    let dps_value = match &first.payload {
        Payload::Struct(s) => s.dps.clone(),
        Payload::String(s) => match first.command {
            Some(CommandType::ControlNew) => {
                return Err(anyhow!("Ignore next message (ControlNew)"))
            }
            Some(CommandType::DpQueryNew | CommandType::DpQuery) => {
                let payload: Option<TuyaDpsPayload> = serde_json::from_str(s).ok();
                payload.and_then(|p| p.dps)
            }
            _ => return Err(anyhow!("Unexpected Tuya command type {:?}", first.command)),
        },
        _ => return Err(anyhow!("Unexpected Tuya device state struct")),
    }
    .context("Expected to find dps struct in Tuya response")?;

    let dps: HashMap<String, serde_json::Value> = serde_json::from_value(dps_value.clone())?;

    let power = if let Some(Value::Bool(value)) = dps.get(
        config
            .power_on_field
            .as_deref()
            .unwrap_or(DEFAULT_POWER_ON_FIELD),
    ) {
        Some(*value)
    } else {
        Some(true)
    };

    let mode = dps.get(DEFAULT_MODE_FIELD);
    let color = if mode == Some(&Value::String("colour".to_string())) {
        let value = dps
            .get(DEFAULT_COLOR_FIELD)
            .context("Expected to find device color in COLOR_FIELD when device is in color mode")?;
        let color = value
            .as_str()
            .context("Could not deserialize color as string")?;

        let h = i32::from_str_radix(&color[0..4], 16)?;
        let s = i32::from_str_radix(&color[4..8], 16)?;
        Some(DeviceColor::Hs(Hs {
            h: h as u16,
            s: s as f32 / 1000.,
        }))
    } else if mode == Some(&Value::String("white".to_string())) {
        let value = dps.get(DEFAULT_COLOR_TEMP_FIELD).context("Expected to find device color temperature in COLOR_TEMP_FIELD when device is in CT mode")?;
        let ct = value
            .as_u64()
            .context("Could not deserialize color temperature as u64")?;

        // Scale range to 0-1 and convert to kelvin
        let q = ct as f32 / 1000.0;
        let k = q * (MAX_SUPPORTED_CT - MIN_SUPPORTED_CT) as f32 + MIN_SUPPORTED_CT as f32;

        Some(DeviceColor::Ct(Ct { ct: k as u16 }))
    } else {
        None
    };

    let brightness = match mode {
        Some(Value::String(s)) if s == "white" => {
            let value = dps.get(DEFAULT_BRIGHTNESS_FIELD).context(
                "Expected to find device brightness in BRIGHTNESS_FIELD when device is in CT mode",
            )?;
            let brightness = value
                .as_u64()
                .context("Could not deserialize brightness as u64")?;

            // Scale range to 0-990
            let brightness = brightness - 10;

            // Scale range to 0-1
            Some(brightness as f32 / 990.0)
        }
        Some(Value::String(s)) if s == "colour" => {
            let value = dps.get(DEFAULT_COLOR_FIELD).context(
                "Expected to find device color in COLOR_FIELD when device is in color mode",
            )?;
            let color = value
                .as_str()
                .context("Could not deserialize color as string")?;

            Some(i32::from_str_radix(&color[8..12], 16)? as f32 / 1000.0)
        }
        _ => None,
    };

    let device = MqttDevice {
        id: config.id.clone(),
        name: Some(config.name.clone()),
        power,
        brightness,
        color,
        transition_ms: Some(500.0),
        sensor_value: None,
        capabilities: Some(config.capabilities.clone().unwrap_or_default()),
        raw: Some(dps_value),
    };

    Ok(device)
}

pub fn mqtt_to_tuya(mqtt_device: MqttDevice, device_config: &TuyaDeviceConfig) -> TuyaDps {
    let mut dps = serde_json::Map::new();

    if let Some(power) = mqtt_device.power {
        dps.insert(
            device_config
                .power_on_field
                .as_deref()
                .unwrap_or(DEFAULT_POWER_ON_FIELD)
                .to_string(),
            json!(power),
        );
    }

    if let Some(brightness) = mqtt_device.brightness {
        // Brightness goes from 10 to 1000 ¯\_(ツ)_/¯
        let tuya_brightness = f32::floor(brightness * 990.0) as u32 + 10;
        dps.insert(DEFAULT_BRIGHTNESS_FIELD.to_string(), json!(tuya_brightness));
    }

    // NOTE: Very important that MODE_FIELD comes last in the dps struct, at
    // least my Tuya lamps will not set the provided color unless this is the
    // case. Note also that this is why we need to enable the preserve_order
    // feature of serde_json.
    if let Some(DeviceColor::Hs(color)) = mqtt_device.color {
        // HSV is represented as a string of i16 hex values
        let hue: f32 = color.h as f32;
        let saturation = color.s * 1000.0;

        let value = {
            let brightness = mqtt_device.brightness.unwrap_or(1.0);
            let brightness = brightness.min(device_config.max_brightness.unwrap_or(1.0));
            brightness * 1000.0
        };

        let tuya_color_string = format!(
            "{:0>4x}{:0>4x}{:0>4x}",
            hue as i16, saturation as i16, value as i16
        );

        dps.insert(DEFAULT_COLOR_FIELD.to_string(), json!(tuya_color_string));
        dps.insert(DEFAULT_MODE_FIELD.to_string(), json!("colour"));
    } else if let Some(DeviceColor::Ct(Ct { ct })) = mqtt_device.color {
        // Scale the value into 0.0 - 1.0 range
        let q = (ct - MIN_SUPPORTED_CT) as f32 / (MAX_SUPPORTED_CT - MIN_SUPPORTED_CT) as f32;
        let q = q.clamp(0.0, 1.0);

        // Scale the value into 0 - 1000 range
        let tuya_ct = f32::floor(q * 1000.0) as u32;

        dps.insert(DEFAULT_COLOR_TEMP_FIELD.to_string(), json!(tuya_ct));
        dps.insert(DEFAULT_MODE_FIELD.to_string(), json!("white"));
    }

    serde_json::Value::Object(dps)
}

pub async fn connect_and_poll(
    device_config: TuyaDeviceConfig,
    mqtt_client: MqttClient,
) -> Result<()> {
    let mqtt_rx_map = mqtt_client.rx_map.clone();
    let id = device_config.id.clone();

    let tuya_device = Arc::new(RwLock::new(
        TuyaDevice::new(
            &device_config.version,
            &id,
            Some(&device_config.local_key),
            IpAddr::from_str(&device_config.ip).unwrap(),
        )
        .unwrap(),
    ));

    // Connect to the device
    let mut rx = {
        debug!("Connecting to {}", device_config.name);
        let mut tuya_device = tuya_device.write().await;
        timeout(
            Duration::from_millis(CONNECT_TIMEOUT_MS),
            tuya_device.connect(),
        )
        .await??
    };

    // Signal successful connection for backoff reset
    debug!("Successfully connected to {}", device_config.name);

    // Tuya -> MQTT
    let tuya2mqtt = {
        let device_config = device_config.clone();
        let mqtt_client = mqtt_client.clone();

        async move {
            // Ignore some garbage data that at least my Tuya lamps send after a
            // ControlNew message response
            let mut ignore_next = false;

            loop {
                // Add timeout on receive to detect stale connections that don't close cleanly
                let messages = timeout(Duration::from_millis(RECEIVE_TIMEOUT_MS), rx.recv()).await;

                let messages = match messages {
                    Ok(Some(result)) => result?,
                    Ok(None) => {
                        // Channel closed
                        return Err(anyhow!("Receive channel closed"));
                    }
                    Err(_) => {
                        // Timeout - connection may be stale
                        warn!(
                            "Receive timeout on {}, connection may be stale",
                            device_config.name
                        );
                        return Err(anyhow!("Receive timeout - connection stale"));
                    }
                };

                if ignore_next {
                    ignore_next = false;
                    continue;
                }

                let mqtt_device = tuya_to_mqtt(messages, &device_config);

                // TODO: use thiserror if this pattern repeats somewhere else :-)
                if let Err(e) = &mqtt_device {
                    if e.to_string().contains("Ignore next message (ControlNew)") {
                        ignore_next = true;
                    }
                }

                if let Ok(mqtt_device) = mqtt_device {
                    let json = serde_json::to_string(&mqtt_device)?;

                    let topic = device_config
                        .topic
                        .clone()
                        .unwrap_or_else(|| mqtt_client.topic.replacen('+', &device_config.id, 1));

                    let res = mqtt_client
                        .client
                        .publish(topic, QoS::AtLeastOnce, true, json)
                        .await;

                    if let Err(e) = res {
                        eprintln!("Error while publishing to MQTT: {:?}", e);
                    }
                }
            }

            #[allow(unreachable_code)]
            anyhow::Ok(())
        }
    };

    // MQTT -> Tuya
    let mqtt2tuya = {
        let tuya_device = tuya_device.clone();
        let device_config = device_config.clone();

        let mut mqtt_rx = mqtt_rx_map
            .get(&device_config.id)
            .context(format!(
                "Could not find configured MQTT device with id {}",
                device_config.id
            ))?
            .clone();

        async move {
            loop {
                let res = {
                    mqtt_rx.changed().await?;
                    let value = &*mqtt_rx.borrow();
                    value
                        .clone()
                        .context("Expected to receive mqtt message from rx channel")?
                };

                let dps = mqtt_to_tuya(res, &device_config);

                let mut tuya_device = tuya_device.write().await;
                timeout(
                    Duration::from_millis(OPERATION_TIMEOUT_MS),
                    tuya_device.set_values(dps),
                )
                .await??;
            }

            #[allow(unreachable_code)]
            anyhow::Ok(())
        }
    };

    // Tuya status poll - reduced frequency to prevent device memory corruption
    let tuya_poll = {
        let tuya_device = tuya_device.clone();
        let id = id.clone();

        async move {
            loop {
                {
                    let mut tuya_device = tuya_device.write().await;
                    timeout(
                        Duration::from_millis(OPERATION_TIMEOUT_MS),
                        tuya_device.get(Payload::Struct(PayloadStruct {
                            dev_id: id.to_string(),
                            gw_id: Some(id.to_string()),
                            uid: Some(id.to_string()),
                            t: Some("0".to_string()),
                            dp_id: None,
                            dps: None,
                        })),
                    )
                    .await??;
                }

                // Reduced from 1s to 30s - aggressive polling triggers v3.4 firmware bugs
                tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
            }

            #[allow(unreachable_code)]
            anyhow::Ok(())
        }
    };

    // Heartbeat task - keeps v3.4 connections alive
    let heartbeat = {
        let tuya_device = tuya_device.clone();
        let device_name = device_config.name.clone();

        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(HEARTBEAT_INTERVAL_MS)).await;

                let mut tuya_device = tuya_device.write().await;
                let result = timeout(
                    Duration::from_millis(OPERATION_TIMEOUT_MS),
                    tuya_device.heartbeat(),
                )
                .await;

                match result {
                    Ok(Ok(())) => {
                        debug!("Heartbeat sent to {}", device_name);
                    }
                    Ok(Err(e)) => {
                        warn!("Heartbeat failed for {}: {:?}", device_name, e);
                        return Err(anyhow!("Heartbeat failed: {:?}", e));
                    }
                    Err(_) => {
                        warn!("Heartbeat timeout for {}", device_name);
                        return Err(anyhow!("Heartbeat timeout"));
                    }
                }
            }

            #[allow(unreachable_code)]
            anyhow::Ok(())
        }
    };

    // Loop until either future encounters an error
    select_all(vec![
        mqtt2tuya.boxed(),
        tuya2mqtt.boxed(),
        tuya_poll.boxed(),
        heartbeat.boxed(),
    ])
    .await
    .0
}

/// Maximum reconnection delay (10 seconds)
const MAX_RECONNECT_DELAY_MS: u64 = 10_000;

/// Initial reconnection delay (1 second)
const INITIAL_RECONNECT_DELAY_MS: u64 = 1_000;

pub async fn init_tuya(device_config: TuyaDeviceConfig, mqtt_client: MqttClient) {
    tokio::spawn(async move {
        let mut reconnect_delay = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);

        loop {
            let device_config = device_config.clone();
            let mqtt_client = mqtt_client.clone();

            let name = device_config.name.clone();
            let res = connect_and_poll(device_config, mqtt_client).await;

            match res {
                Ok(()) => {
                    // This shouldn't happen as connect_and_poll runs indefinitely
                    reconnect_delay = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);
                }
                Err(e) => {
                    // Check if this was a connection-phase error or a runtime error
                    // Runtime errors (after successful connection) should reset backoff
                    let error_str = format!("{:?}", e);
                    let is_connection_error = error_str.contains("connect")
                        || error_str.contains("Connect")
                        || error_str.contains("timeout")
                        || error_str.contains("InvalidSessionKey");

                    if !is_connection_error {
                        // Connection was established but failed later - reset backoff
                        reconnect_delay = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);
                    }

                    eprintln!(
                        "Error while polling {}: {:?}, retrying in {:?}",
                        name, e, reconnect_delay
                    );

                    // Wait before reconnecting with exponential backoff
                    tokio::time::sleep(reconnect_delay).await;

                    // Exponential backoff: double the delay, up to maximum
                    reconnect_delay =
                        (reconnect_delay * 2).min(Duration::from_millis(MAX_RECONNECT_DELAY_MS));
                }
            }
        }
    });
}
