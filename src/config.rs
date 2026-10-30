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

    parse_config(builder.add_source(config::File::with_name("Settings")))
}

fn parse_config(
    builder: config::ConfigBuilder<config::builder::DefaultState>,
) -> Result<(MqttConfig, TuyaConfig)> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_example_config() {
        let (mqtt, tuya) =
            parse_config(config::Config::builder().add_source(config::File::from_str(
                include_str!("../Settings.toml.example"),
                config::FileFormat::Toml,
            )))
            .unwrap();

        assert_eq!(mqtt.id, "tuya-mqtt");
        assert_eq!(mqtt.topic, "home/lights/tuya/+");
        assert_eq!(tuya.devices.len(), 4);

        let device = &tuya.devices["bf0000000000000000cccc"];
        assert_eq!(device.id, "bf0000000000000000cccc");
        assert_eq!(device.version, "3.4");
        assert_eq!(device.local_key.len(), 16);
        assert_eq!(device.topic, None);
        assert_eq!(
            tuya.devices["bf0000000000000000dddd"].topic.as_deref(),
            Some("my/custom/topic")
        );
    }
}
