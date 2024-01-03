#![allow(clippy::redundant_closure_call)]

use anyhow::{Context, Result};
use rand::{distributions::Alphanumeric, Rng};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{watch::Receiver, RwLock},
    task,
};

use crate::config::MqttConfig;
use crate::tuya::TuyaConfig;

// Assume (probably incorrectly) that supported range is from 2700K - 6500K
pub const MIN_SUPPORTED_CT: u16 = 2700;
pub const MAX_SUPPORTED_CT: u16 = 6500;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Capabilities {
    /// Hue (0 - 360) and saturation (0.0 - 1.0)
    #[serde(default)]
    pub hs: bool,

    /// Color temperature (2000 - 6500)
    pub ct: Option<std::ops::Range<u16>>,
}

impl Default for Capabilities {
    fn default() -> Self {
        Capabilities {
            hs: true,
            ct: Some(MIN_SUPPORTED_CT..MAX_SUPPORTED_CT),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Hs {
    pub h: u16,
    pub s: f32,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct Ct {
    pub ct: u16,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum DeviceColor {
    Hs(Hs),
    Ct(Ct),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct MqttDevice {
    pub id: String,
    pub name: Option<String>,
    pub power: Option<bool>,
    pub brightness: Option<f32>,
    pub color: Option<DeviceColor>,
    pub transition_ms: Option<f32>,
    pub sensor_value: Option<String>,
    pub capabilities: Option<Capabilities>,
    pub raw: Option<serde_json::Value>,
}

#[derive(Clone)]
pub struct MqttClient {
    pub client: AsyncClient,
    pub rx_map: HashMap<String, Receiver<Option<MqttDevice>>>,
    pub topic: String,
}

pub async fn init_mqtt(mqtt_config: &MqttConfig, tuya_config: &TuyaConfig) -> Result<MqttClient> {
    let random_string: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();

    let mut options = MqttOptions::new(
        format!("{}-{}", mqtt_config.id.clone(), random_string),
        mqtt_config.host.clone(),
        mqtt_config.port,
    );
    options.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = AsyncClient::new(options, 10);

    let mut tx_map = HashMap::new();
    let mut rx_map = HashMap::new();

    for device in tuya_config.devices.values() {
        if let Some(topic) = &device.topic {
            client
                .subscribe(format!("{}/set", topic), QoS::AtMostOnce)
                .await?;
        }

        let (tx, rx) = tokio::sync::watch::channel(None);
        let tx = Arc::new(RwLock::new(tx));
        tx_map.insert(device.id.clone(), tx);
        rx_map.insert(device.id.clone(), rx);
    }

    {
        let client = client.clone();
        let mqtt_config = mqtt_config.clone();

        task::spawn(async move {
            loop {
                let notification = eventloop.poll().await;
                let mqtt_tx = tx_map.clone();
                let client = client.clone();
                let mqtt_config = mqtt_config.clone();

                let res = (|| async move {
                    match notification? {
                        rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_)) => {
                            client
                                .subscribe(format!("{}/set", mqtt_config.topic), QoS::AtMostOnce)
                                .await?;
                        }
                        rumqttc::Event::Incoming(rumqttc::Packet::Publish(msg)) => {
                            let device: MqttDevice = serde_json::from_slice(&msg.payload)?;

                            let device_id = &device.id;
                            let tx = mqtt_tx.get(device_id).context(format!(
                                "Could not find configured MQTT device with id {}",
                                device_id
                            ))?;
                            let tx = tx.write().await;
                            tx.send(Some(device))?;
                        }
                        _ => {}
                    }
                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                })()
                .await;

                if let Err(e) = res {
                    eprintln!("MQTT error: {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });
    }

    Ok(MqttClient {
        client,
        rx_map,
        topic: mqtt_config.topic.clone(),
    })
}
