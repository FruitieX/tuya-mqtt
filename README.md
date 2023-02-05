# tuya-mqtt

This program periodically polls configured Tuya devices in your local network, and publishes current state on an MQTT topic: `/home/tuya/<device_id>`.

It also subscribes to an MQTT topic `/home/tuya/<device_id>/set` and sets device state according to incoming messages.

## Running

Make sure you have a recent version of Rust installed.

1. Clone this repo
2. Copy Settings.toml.example -> Settings.toml
3. Configure Settings.toml to match your setup (see below)
4. `cargo run`

## Configuration

For each device, you will need to retrieve and note down:

- Device ID
- Device local IP address
- Device name (does not have to match with Tuya app)
- Device local key
- Tuya LAN protocol version number (this involves trial and error for now, try 3.4 or 3.3)

You can find each device's device_id and MAC address in the Tuya Smart app under device settings -> "Device Information".
Your router settings may help you retrieve the local IP address based on the MAC address.

Retrieve the local_key of your devices via https://iot.tuya.com:

- Create an account
- Create a "Cloud Project" (Industry: Smart Home)
- Link your Tuya Smart app under the project's Devices tab
- You should now see your devices listed under Devices
- For each device, go to API Explorer and call the Get Device Information API with your device_id to retreive the device's local_key.

## MQTT protocol

NOTE: this isn't very well designed, e.g. the device id in the MQTT topic is
completely ignored and there only exist topics that make use of JSON.

MQTT messages use the following JSON format:

```
{
  "id": "<device_id>",
  "name": "Living room downlight 2",
  "power": true,
  "brightness": 0.5,
  "cct": 6500,
  "color": {
    "hue": 38,
    "saturation": 0.75,
    "value": 1
  },
  "transition_ms": 500,
}
```

If both `color` and `cct` are provided in a `/set` message, the `color` parameter will be used.

If both `brightness` and `value` are provided then the final brightness is
computed by multiplying these together. I suggest always setting `value` to 1
and adjusting `brightness` instead.