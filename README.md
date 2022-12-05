# tuya-mqtt

This program periodically polls configured Tuya devices in your local network, and publishes current state on an MQTT topic: `/home/tuya/<device_id>`.

It also subscribes to an MQTT topic `/home/tuya/<device_id>/set` and sets device state according to incoming messages.

## Running

Make sure you have a recent version of Rust installed.

1. Clone this repo
2. Copy Settings.toml.example -> Settings.toml
3. Edit Settings.toml to match your setup
4. `cargo run`

Messages use the following JSON format:

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
