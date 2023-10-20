use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;

use crate::{
    mqtt::Capabilities,
    tuya::{TuyaConfig, TuyaDeviceConfig},
};

pub type DeviceId = String;

#[derive(Clone, Deserialize, Debug)]
pub struct MqttConfig {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub topic: String,
}

#[derive(Deserialize, Debug)]
pub struct DeviceConfig {
    pub name: String,
    pub local_key: String,
    pub ip: String,
    pub version: String,
    pub max_brightness: Option<f32>,
    pub power_on_field: Option<String>,
    pub capabilities: Option<Capabilities>,
    pub topic: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub mqtt: MqttConfig,
    pub devices: HashMap<DeviceId, DeviceConfig>,
}

pub fn read_config_devices() -> Result<(MqttConfig, TuyaConfig)> {
    let builder = config::Config::builder();

    let root = std::env::current_dir().unwrap();
    let sample_path = root.join("Settings.toml.example");

    let path = root.join("Settings.toml");

    if !path.exists() && std::env::var("SKIP_SAMPLE_CONFIG").is_err() {
        println!("Settings.toml not found, generating sample configuration.");
        println!("Set SKIP_SAMPLE_CONFIG environment variable to opt out of this behavior.");
        std::fs::copy(sample_path, path).unwrap();
    }

    let builder = builder.add_source(config::File::with_name("Settings"));
    let settings = builder.build()?;

    let config: Config = settings.try_deserialize().context(
        "Failed to deserialize config, compare your config file to Settings.toml.example!",
    )?;

    let devices = config
        .devices
        .into_iter()
        .map(|(device_id, device)| {
            (
                device_id.clone(),
                TuyaDeviceConfig {
                    name: device.name,
                    id: device_id,
                    local_key: device.local_key,
                    ip: device.ip,
                    version: device.version,
                    max_brightness: device.max_brightness,
                    power_on_field: device.power_on_field,
                    topic: device.topic,
                    capabilities: device.capabilities,
                },
            )
        })
        .collect();

    let mqtt_config = config.mqtt;
    let tuya_config = TuyaConfig { devices };

    Ok((mqtt_config, tuya_config))
}
