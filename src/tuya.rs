use anyhow::{anyhow, Context, Result};
use futures::{future::select_all, FutureExt};
use log::{debug, warn};
use palette::Hsv;
use rumqttc::QoS;
use rust_async_tuyapi::{tuyadevice::TuyaDevice, Payload, PayloadStruct};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use std::{net::IpAddr, str::FromStr, time::Duration};
use tokio::{sync::RwLock, task, time::timeout};

use crate::mqtt::{MqttClient, MqttDevice};

const POWER_ON_FIELD: &str = "20";
const MODE_FIELD: &str = "21";
const BRIGHTNESS_FIELD: &str = "22";
const COLOR_TEMP_FIELD: &str = "23";
const COLOR_FIELD: &str = "24";

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
}

#[derive(Clone, Debug, Deserialize)]
pub struct TuyaConfig {
    pub devices: HashMap<String, TuyaDeviceConfig>,
}

type TuyaDps = serde_json::Value;

#[derive(Deserialize)]
struct TuyaThreeFourPayload {
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
        Payload::String(s) => match config.version.as_str() {
            "3.4" => {
                let payload: Option<TuyaThreeFourPayload> = serde_json::from_str(s).ok();
                payload.and_then(|p| p.dps)
            }
            _ => {
                let dps: Option<TuyaDps> = serde_json::from_str(s).ok();
                dps
            }
        },
        _ => return Err(anyhow!("Unexpected Tuya device state struct")),
    }
    .context("Expected to find dps struct in Tuya response")?;

    let dps: HashMap<String, serde_json::Value> = serde_json::from_value(dps)?;

    let power = if let Some(Value::Bool(value)) = dps.get(POWER_ON_FIELD) {
        Some(*value)
    } else {
        Some(true)
    };

    let mode = dps.get(MODE_FIELD);
    let cct = if mode == Some(&Value::String("white".to_string())) {
        let value = dps.get(COLOR_TEMP_FIELD).context("Expected to find device color temperature in COLOR_TEMP_FIELD when device is in CT mode")?;
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
            .get(COLOR_FIELD)
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
            let value = dps.get(BRIGHTNESS_FIELD).context(
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
            let value = dps.get(COLOR_FIELD).context(
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
        name: config.name.clone(),
        power,
        brightness,
        cct,
        color,
        transition_ms: Some(500.0),
        sensor_value: None,
    };

    Ok(device)
}

pub fn mqtt_to_tuya(mqtt_device: MqttDevice) -> TuyaDps {
    let mut dps = serde_json::Map::new();

    if let Some(power) = mqtt_device.power {
        dps.insert(POWER_ON_FIELD.to_string(), json!(power));
    }

    if let Some(brightness) = mqtt_device.brightness {
        // Brightness goes from 10 to 1000 ¯\_(ツ)_/¯
        let tuya_brightness = f32::floor(brightness * 990.0) as u32 + 10;
        dps.insert(BRIGHTNESS_FIELD.to_string(), json!(tuya_brightness));
    }

    // NOTE: Very important that MODE_FIELD comes last in the dps struct, at
    // least my Tuya lamps will not set the provided color unless this is the
    // case. Note also that this is why we need to enable the preserve_order
    // feature of serde_json.
    if let Some(color) = mqtt_device.color {
        // HSV is represented as a string of i16 hex values
        let hue: f32 = color.hue.to_positive_degrees();
        let saturation = (color.saturation as f32) * 1000.0;
        let value = mqtt_device.brightness.unwrap_or(1.0) * (color.value as f32) * 1000.0;
        let tuya_color_string = format!(
            "{:0>4x}{:0>4x}{:0>4x}",
            hue as i16, saturation as i16, value as i16
        );

        dps.insert(COLOR_FIELD.to_string(), json!(tuya_color_string));
        dps.insert(MODE_FIELD.to_string(), json!("colour"));
    } else if let Some(cct) = mqtt_device.cct {
        // Scale the value into 0.0 - 1.0 range
        let q = (cct - MIN_SUPPORTED_CT) / (MAX_SUPPORTED_CT - MIN_SUPPORTED_CT);
        let q = q.clamp(0.0, 1.0);

        // Scale the value into 0 - 1000 range
        let tuya_ct = f32::floor(q * 1000.0) as u32;

        dps.insert(COLOR_TEMP_FIELD.to_string(), json!(tuya_ct));
        dps.insert(MODE_FIELD.to_string(), json!("white"));
    }

    serde_json::Value::Object(dps)
}

pub async fn init_tuya(device_config: TuyaDeviceConfig, mqtt_client: MqttClient) -> Result<()> {
    let tuya_device = Arc::new(RwLock::new(TuyaDevice::new(
        &device_config.version,
        &device_config.id,
        Some(&device_config.local_key),
        IpAddr::from_str(&device_config.ip).unwrap(),
    )?));

    task::spawn(async move {
        loop {
            let tuya_device = tuya_device.clone();

            // Connect to the device
            let connected = {
                let mut tuya_device = tuya_device.write().await;
                debug!("Connecting to {}", device_config.name);
                let res = timeout(Duration::from_millis(9000), tuya_device.connect()).await;
                debug!("Connected to {}", device_config.name);

                match res {
                    Ok(Err(e)) => {
                        eprintln!(
                            "Error while connecting to Tuya device {}: {:?}",
                            device_config.name, e
                        );
                        false
                    }
                    Err(e) => {
                        eprintln!(
                            "Timeout while connecting to Tuya device {}: {:?}",
                            device_config.name, e
                        );
                        false
                    }
                    _ => true,
                }
            };

            if connected {
                let mqtt_client = mqtt_client.clone();

                // Loop until there's an error of any kind
                let poll_future = {
                    let tuya_device = tuya_device.clone();
                    let device_config = device_config.clone();

                    (|| async move {
                        loop {
                            let mut tuya_device = tuya_device.write().await;

                            let messages = timeout(
                                Duration::from_millis(5000),
                                tuya_device.get(Payload::Struct(PayloadStruct {
                                    dev_id: device_config.id.to_string(),
                                    gw_id: Some(device_config.id.to_string()),
                                    uid: Some(device_config.id.to_string()),
                                    t: Some("0".to_string()),
                                    dp_id: None,
                                    dps: None,
                                })),
                            )
                            .await??;

                            let mqtt_device = tuya_to_mqtt(messages, &device_config)?;

                            let json = serde_json::to_string(&mqtt_device)?;

                            let topic = format!("home/lights/tuya/{}", device_config.id);
                            mqtt_client
                                .client
                                .publish(topic, QoS::AtLeastOnce, true, json)
                                .await?;

                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }

                        #[allow(unreachable_code)]
                        Ok::<(), Box<dyn std::error::Error>>(())
                    })()
                };

                let send_future = {
                    let mqtt_rx_map = mqtt_client.rx_map.clone();
                    let tuya_device = tuya_device.clone();
                    let device_config = device_config.clone();

                    (|| async move {
                        loop {
                            let res = {
                                let mqtt_rx =
                                    mqtt_rx_map.get(&device_config.id).context(format!(
                                        "Could not find configured MQTT device with id {}",
                                        device_config.id
                                    ))?;
                                let mut mqtt_rx = mqtt_rx.write().await;
                                mqtt_rx.changed().await?;
                                let value = &*mqtt_rx.borrow();
                                value
                                    .clone()
                                    .context("Expected to receive mqtt message from rx channel")?
                            };

                            let dps = mqtt_to_tuya(res);

                            let mut tuya_device = tuya_device.write().await;
                            timeout(Duration::from_millis(3000), tuya_device.set_values(dps))
                                .await??;

                            // A bug in rust_async_tuyapi causes the next read after set_values to fail
                            // with invalid payload messages. Make a dummy
                            tuya_device
                                .set(Payload::Struct(PayloadStruct {
                                    dev_id: device_config.id.to_string(),
                                    gw_id: Some(device_config.id.to_string()),
                                    uid: Some(device_config.id.to_string()),
                                    t: Some("0".to_string()),
                                    dp_id: None,
                                    dps: None,
                                }))
                                .await
                                .ok();
                        }

                        #[allow(unreachable_code)]
                        Ok::<(), Box<dyn std::error::Error>>(())
                    })()
                };

                let res = select_all(vec![poll_future.boxed(), send_future.boxed()]).await;

                if let (Err(e), future_index, _) = res {
                    if future_index == 0 {
                        warn!(
                            "Error while polling Tuya device {}: {:?}",
                            device_config.name, e
                        )
                    } else {
                        warn!(
                            "Error while sending to Tuya device {}: {:?}",
                            device_config.name, e
                        )
                    }
                }
            }

            // Wait before reconnecting
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    });

    Ok(())
}
