use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;

use crate::tuya::{TuyaConfig, TuyaDeviceConfig};

pub type DeviceId = String;

#[derive(Deserialize, Debug)]
pub struct MqttConfig {
    pub id: String,
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Debug)]
pub struct DeviceConfig {
    pub name: String,
    pub local_key: String,
    pub ip: String,
    pub version: String,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub mqtt: MqttConfig,
    pub devices: HashMap<DeviceId, DeviceConfig>,
}

pub fn read_config_devices() -> Result<(MqttConfig, TuyaConfig)> {
    let mut settings = config::Config::default();

    let root = std::env::current_dir().unwrap();
    let sample_path = root.join("Settings.toml.example");

    let path = root.join("Settings.toml");

    if !path.exists() && std::env::var("SKIP_SAMPLE_CONFIG").is_err() {
        println!("Settings.toml not found, generating sample configuration.");
        println!("Set SKIP_SAMPLE_CONFIG environment variable to opt out of this behavior.");
        std::fs::copy(sample_path, path).unwrap();
    }

    settings
        .merge(config::File::with_name("Settings"))
        .context("Failed to load Settings.toml config file")?;

    let config: Config = serde_path_to_error::deserialize(settings.clone()).context(
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
                },
            )
        })
        .collect();

    let mqtt_config = config.mqtt;
    let tuya_config = TuyaConfig { devices };

    Ok((mqtt_config, tuya_config))
}
