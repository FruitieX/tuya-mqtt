use crate::config::read_config_devices;
use crate::mqtt::init_mqtt;
use crate::tuya::init_tuya;

mod config;
mod mqtt;
mod tuya;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let (mqtt_config, tuya_config) = read_config_devices()?;
    let mqtt_client = init_mqtt(&mqtt_config, &tuya_config).await?;

    for device in tuya_config.devices.into_values() {
        let mqtt_client = mqtt_client.clone();
        init_tuya(device, mqtt_client).await;
    }

    tokio::signal::ctrl_c().await?;

    Ok(())
}
