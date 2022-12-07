use anyhow::{Context, Result};
use palette::Hsv;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{watch::Receiver, RwLock},
    task,
};

use crate::config::MqttConfig;
use crate::tuya::TuyaConfig;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct MqttDevice {
    pub id: String,
    pub name: String,
    pub power: Option<bool>,
    pub brightness: Option<f32>,
    pub cct: Option<f32>,
    pub color: Option<Hsv>,
    pub transition_ms: Option<f32>,
    pub sensor_value: Option<String>,
}

#[derive(Clone)]
pub struct MqttClient {
    pub client: AsyncClient,
    pub rx_map: HashMap<String, Arc<RwLock<Receiver<Option<MqttDevice>>>>>,
}

pub async fn init_mqtt(mqtt_config: &MqttConfig, tuya_config: &TuyaConfig) -> Result<MqttClient> {
    let mut options = MqttOptions::new(
        mqtt_config.id.clone(),
        mqtt_config.host.clone(),
        mqtt_config.port,
    );
    options.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = AsyncClient::new(options, 10);
    client
        .subscribe("home/lights/tuya/+/set", QoS::AtMostOnce)
        .await?;

    let mut tx_map = HashMap::new();
    let mut rx_map = HashMap::new();

    for device in tuya_config.devices.values() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let tx = Arc::new(RwLock::new(tx));
        let rx = Arc::new(RwLock::new(rx));
        tx_map.insert(device.id.clone(), tx);
        rx_map.insert(device.id.clone(), rx);
    }

    task::spawn(async move {
        while let Ok(notification) = eventloop.poll().await {
            let mqtt_tx = tx_map.clone();

            let res = (|| async move {
                if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(msg)) = notification {
                    let device: MqttDevice = serde_json::from_slice(&msg.payload)?;

                    let device_id = &device.id;
                    let tx = mqtt_tx.get(device_id).context(format!(
                        "Could not find configured MQTT device with id {}",
                        device_id
                    ))?;
                    let tx = tx.write().await;
                    tx.send(Some(device))?;
                }

                Ok::<(), Box<dyn std::error::Error>>(())
            })()
            .await;

            if let Err(e) = res {
                eprintln!("MQTT error: {:?}", e);
            }
        }
    });

    Ok(MqttClient { client, rx_map })
}
