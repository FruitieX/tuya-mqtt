use anyhow::{anyhow, Context, Result};
use futures::future::select_all;
use futures::future::FutureExt;
use log::debug;
use palette::Hsv;
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

use crate::mqtt::{MqttClient, MqttDevice};

const DEFAULT_POWER_ON_FIELD: &str = "20";
const DEFAULT_MODE_FIELD: &str = "21";
const DEFAULT_BRIGHTNESS_FIELD: &str = "22";
const DEFAULT_COLOR_TEMP_FIELD: &str = "23";
const DEFAULT_COLOR_FIELD: &str = "24";

// Assume (probably incorrectly) that supported range is from 2700K - 6500K
const MIN_SUPPORTED_CT: f32 = 2700.0;
const MAX_SUPPORTED_CT: f32 = 6500.0;

#[derive(Clone, Debug, Deserialize)]
pub struct TuyaDeviceConfig {
    pub name: String,
    pub id: String,
    pub local_key: String,
    pub ip: String,
    pub version: String,
    pub max_brightness: Option<f32>,
    pub power_on_field: Option<String>,
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

    let dps = match &first.payload {
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

    let dps: HashMap<String, serde_json::Value> = serde_json::from_value(dps)?;

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
    let cct = if mode == Some(&Value::String("white".to_string())) {
        let value = dps.get(DEFAULT_COLOR_TEMP_FIELD).context("Expected to find device color temperature in COLOR_TEMP_FIELD when device is in CT mode")?;
        let ct = value
            .as_u64()
            .context("Could not deserialize color temperature as u64")?;

        // Scale range to 0-1
        let q = ct as f32 / 1000.0;

        Some(q * (MAX_SUPPORTED_CT - MIN_SUPPORTED_CT) + MIN_SUPPORTED_CT)
    } else {
        None
    };

    let color = if mode == Some(&Value::String("colour".to_string())) {
        let value = dps
            .get(DEFAULT_COLOR_FIELD)
            .context("Expected to find device color in COLOR_FIELD when device is in color mode")?;
        let color = value
            .as_str()
            .context("Could not deserialize color as string")?;

        let h = i32::from_str_radix(&color[0..4], 16)?;
        let s = i32::from_str_radix(&color[4..8], 16)?;
        Some(Hsv::new(h as f32, s as f32 / 1000., 1.))
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
        cct,
        color,
        transition_ms: Some(500.0),
        sensor_value: None,
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
    if let Some(color) = mqtt_device.color {
        // HSV is represented as a string of i16 hex values
        let hue: f32 = color.hue.into_positive_degrees();
        let saturation = color.saturation * 1000.0;

        let value = {
            let brightness = mqtt_device.brightness.unwrap_or(1.0) * color.value;
            let brightness = brightness.min(device_config.max_brightness.unwrap_or(1.0));
            brightness * 1000.0
        };

        let tuya_color_string = format!(
            "{:0>4x}{:0>4x}{:0>4x}",
            hue as i16, saturation as i16, value as i16
        );

        dps.insert(DEFAULT_COLOR_FIELD.to_string(), json!(tuya_color_string));
        dps.insert(DEFAULT_MODE_FIELD.to_string(), json!("colour"));
    } else if let Some(cct) = mqtt_device.cct {
        // Scale the value into 0.0 - 1.0 range
        let q = (cct - MIN_SUPPORTED_CT) / (MAX_SUPPORTED_CT - MIN_SUPPORTED_CT);
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
        timeout(Duration::from_millis(9000), tuya_device.connect()).await??
    };

    // Tuya -> MQTT
    let tuya2mqtt = {
        let device_config = device_config.clone();
        let mqtt_client = mqtt_client.clone();

        async move {
            // Ignore some garbage data that at least my Tuya lamps send after a
            // ControlNew message response
            let mut ignore_next = false;

            while let Ok(messages) = rx
                .recv()
                .await
                .context("Error receiving message from device")?
            {
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

                    mqtt_client
                        .client
                        .publish(topic, QoS::AtLeastOnce, true, json)
                        .await?;
                }
            }

            anyhow::Ok(())
        }
    };

    // MQTT -> Tuya
    let mqtt2tuya = {
        let tuya_device = tuya_device.clone();

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
                timeout(Duration::from_millis(3000), tuya_device.set_values(dps)).await??;
            }

            #[allow(unreachable_code)]
            anyhow::Ok(())
        }
    };

    // Tuya status poll
    let tuya_poll = async move {
        loop {
            let mut tuya_device = tuya_device.write().await;
            timeout(
                Duration::from_millis(3000),
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

            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        #[allow(unreachable_code)]
        anyhow::Ok(())
    };

    // Loop until either future encounters an error
    select_all(vec![
        mqtt2tuya.boxed(),
        tuya2mqtt.boxed(),
        tuya_poll.boxed(),
    ])
    .await
    .0
}

pub async fn init_tuya(device_config: TuyaDeviceConfig, mqtt_client: MqttClient) {
    loop {
        let device_config = device_config.clone();
        let mqtt_client = mqtt_client.clone();

        let res = connect_and_poll(device_config, mqtt_client).await;

        if let Err(e) = res {
            eprintln!("Error while polling Tuya device: {:?}", e);
        }

        // Wait before reconnecting
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
}
