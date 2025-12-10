use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use futures::future::select_all;
use futures::future::FutureExt;
use log::{debug, info, warn};
use rumqttc::QoS;
use crate::tuyapi::mesparse::CommandType;
use crate::tuyapi::PayloadStruct;
use crate::tuyapi::{tuyadevice::TuyaDevice, Payload};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{net::IpAddr, str::FromStr, time::Duration};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{timeout, Instant};

use crate::mqtt::Capabilities;
use crate::mqtt::Ct;
use crate::mqtt::DeviceColor;
use crate::mqtt::Hs;
use crate::mqtt::MAX_SUPPORTED_CT;
use crate::mqtt::MIN_SUPPORTED_CT;
use crate::mqtt::{MqttClient, MqttDevice};

const DEFAULT_POWER_ON_FIELD: &str = "20";
const DEFAULT_MODE_FIELD: &str = "21";
const DEFAULT_BRIGHTNESS_FIELD: &str = "22";
const DEFAULT_COLOR_TEMP_FIELD: &str = "23";
const DEFAULT_COLOR_FIELD: &str = "24";

/// Polling interval for querying device status (in milliseconds)
/// Community research shows aggressive polling (< 10s) can trigger resource
/// exhaustion in v3.4 device firmware, leading to "stuck" devices.
/// Increased to 15s for better device stability.
const POLL_INTERVAL_MS: u64 = 15_000;

/// Maximum random jitter added to polling interval (in milliseconds)
/// This spreads out polling across devices to avoid thundering herd
const POLL_JITTER_MS: u64 = 2_000;

/// Heartbeat interval to keep TCP connection alive (in milliseconds)
/// v3.4 devices may close idle connections; heartbeats prevent this.
/// Note: heartbeat is skipped if other activity occurred recently.
const HEARTBEAT_INTERVAL_MS: u64 = 15_000;

/// Maximum random jitter added to heartbeat interval (in milliseconds)
/// This breaks synchronization between devices after reconnects
const HEARTBEAT_JITTER_MS: u64 = 5_000;

/// Timeout for individual operations (connect, get, set)
const OPERATION_TIMEOUT_MS: u64 = 5_000;

/// Timeout for initial connection establishment
const CONNECT_TIMEOUT_MS: u64 = 9_000;

/// Timeout for receiving messages before considering connection stale
/// Must be longer than HEARTBEAT_INTERVAL_MS + OPERATION_TIMEOUT_MS to avoid
/// false positives when waiting for heartbeat responses
const RECEIVE_TIMEOUT_MS: u64 = 30_000;

/// Minimum time between commands sent to device (in milliseconds)
/// This prevents overwhelming the device with rapid-fire commands
const COMMAND_THROTTLE_MS: u64 = 1_000;

/// If any activity occurred within this time, skip the heartbeat
/// Should be less than HEARTBEAT_INTERVAL_MS
const HEARTBEAT_SKIP_IF_ACTIVITY_MS: u64 = 10_000;

/// Maximum number of events to keep in the timeline ring buffer
const EVENT_LOG_CAPACITY: usize = 100;

// ============================================================================
// Event Timeline Logging
// ============================================================================

/// Types of events that can be logged in the device timeline
#[derive(Debug, Clone)]
pub enum DeviceEventType {
    /// TCP connection established
    Connected,
    /// TCP connection closed (intentional disconnect)
    Disconnected,
    /// Heartbeat sent to device
    HeartbeatSent,
    /// Status poll request sent
    PollSent,
    /// Command sent from MQTT (contains DPS JSON)
    CommandSent(String),
    /// Message received from device (contains command type and payload summary)
    MessageReceived(String),
    /// Error occurred (contains error description)
    Error(String),
    /// Timeout occurred (operation name)
    Timeout(String),
    /// Throttled - command was delayed due to rate limiting
    Throttled { delayed_ms: u64 },
    /// Heartbeat skipped due to recent activity
    HeartbeatSkipped { last_activity_ms: u64 },
    /// Connection attempt started
    ConnectAttempt,
    /// Receive timeout - connection may be stale
    ReceiveTimeout,
}

impl std::fmt::Display for DeviceEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeviceEventType::Connected => write!(f, "CONNECTED"),
            DeviceEventType::Disconnected => write!(f, "DISCONNECTED"),
            DeviceEventType::HeartbeatSent => write!(f, "HEARTBEAT_SENT"),
            DeviceEventType::PollSent => write!(f, "POLL_SENT"),
            DeviceEventType::CommandSent(dps) => write!(f, "COMMAND_SENT: {}", dps),
            DeviceEventType::MessageReceived(msg) => write!(f, "MESSAGE_RECEIVED: {}", msg),
            DeviceEventType::Error(e) => write!(f, "ERROR: {}", e),
            DeviceEventType::Timeout(op) => write!(f, "TIMEOUT: {}", op),
            DeviceEventType::Throttled { delayed_ms } => write!(f, "THROTTLED: delayed {}ms", delayed_ms),
            DeviceEventType::HeartbeatSkipped { last_activity_ms } => {
                write!(f, "HEARTBEAT_SKIPPED: last activity {}ms ago", last_activity_ms)
            }
            DeviceEventType::ConnectAttempt => write!(f, "CONNECT_ATTEMPT"),
            DeviceEventType::ReceiveTimeout => write!(f, "RECEIVE_TIMEOUT"),
        }
    }
}

/// A single event in the device timeline
#[derive(Debug, Clone)]
pub struct DeviceEvent {
    /// Timestamp when the event occurred
    pub timestamp: DateTime<Utc>,
    /// Monotonic instant for precise timing calculations
    pub instant: u64,
    /// The type of event
    pub event_type: DeviceEventType,
}

/// Ring buffer of recent device events for debugging
#[derive(Debug)]
pub struct DeviceEventLog {
    device_name: String,
    device_id: String,
    device_version: String,
    events: VecDeque<DeviceEvent>,
    capacity: usize,
    /// Monotonic start time for calculating relative timestamps
    start_instant: Instant,
}

impl DeviceEventLog {
    pub fn new(device_name: String, device_id: String, device_version: String) -> Self {
        Self {
            device_name,
            device_id,
            device_version,
            events: VecDeque::with_capacity(EVENT_LOG_CAPACITY),
            capacity: EVENT_LOG_CAPACITY,
            start_instant: Instant::now(),
        }
    }

    /// Add an event to the timeline
    pub fn log(&mut self, event_type: DeviceEventType) {
        if self.events.len() >= self.capacity {
            self.events.pop_front();
        }

        let event = DeviceEvent {
            timestamp: Utc::now(),
            instant: self.start_instant.elapsed().as_millis() as u64,
            event_type,
        };

        self.events.push_back(event);
    }

    /// Dump the timeline to stdout in a format suitable for analysis
    pub fn dump_timeline(&self, failure_reason: &str) {
        let separator_eq = "=".repeat(80);
        let separator_dash = "-".repeat(80);

        eprintln!("\n{}", separator_eq);
        eprintln!("DEVICE FAILURE TIMELINE DUMP");
        eprintln!("{}", separator_eq);
        eprintln!("Device Name: {}", self.device_name);
        eprintln!("Device ID: {}", self.device_id);
        eprintln!("Protocol Version: {}", self.device_version);
        eprintln!("Failure Reason: {}", failure_reason);
        eprintln!("Dump Time: {}", Utc::now().to_rfc3339());
        eprintln!("Total Events: {}", self.events.len());
        eprintln!("{}", separator_dash);
        eprintln!("TIMELINE (oldest first):");
        eprintln!("{}", separator_dash);

        for (i, event) in self.events.iter().enumerate() {
            let relative_ms = event.instant;
            eprintln!(
                "[{:04}] +{:>8}ms | {} | {}",
                i,
                relative_ms,
                event.timestamp.format("%H:%M:%S%.3f"),
                event.event_type
            );
        }

        eprintln!("{}", separator_dash);

        // Print summary statistics
        let mut heartbeats = 0;
        let mut polls = 0;
        let mut commands = 0;
        let mut errors = 0;
        let mut timeouts = 0;
        let mut throttles = 0;

        for event in &self.events {
            match &event.event_type {
                DeviceEventType::HeartbeatSent => heartbeats += 1,
                DeviceEventType::PollSent => polls += 1,
                DeviceEventType::CommandSent(_) => commands += 1,
                DeviceEventType::Error(_) => errors += 1,
                DeviceEventType::Timeout(_) => timeouts += 1,
                DeviceEventType::Throttled { .. } => throttles += 1,
                _ => {}
            }
        }

        eprintln!("SUMMARY:");
        eprintln!("  Heartbeats: {}", heartbeats);
        eprintln!("  Polls: {}", polls);
        eprintln!("  Commands: {}", commands);
        eprintln!("  Errors: {}", errors);
        eprintln!("  Timeouts: {}", timeouts);
        eprintln!("  Throttled: {}", throttles);

        // Calculate time between last few operations
        if self.events.len() >= 2 {
            eprintln!("{}", separator_dash);
            eprintln!("INTER-EVENT TIMING (last 10):");
            let recent: Vec<_> = self.events.iter().rev().take(10).collect();
            for window in recent.windows(2) {
                let delta = window[0].instant.saturating_sub(window[1].instant);
                eprintln!(
                    "  {} -> {} : {}ms",
                    window[1].event_type,
                    window[0].event_type,
                    delta
                );
            }
        }

        eprintln!("{}\n", separator_eq);
    }
}

// ============================================================================
// Command Queue for Throttling
// ============================================================================

/// Commands that can be sent to the device
#[derive(Debug)]
pub enum DeviceCommand {
    /// Send a set_values command with DPS payload
    SetValues(serde_json::Value),
    /// Send a status poll (get) request
    Poll,
    /// Send a heartbeat
    Heartbeat,
}

/// Shared state for activity tracking and throttling
pub struct DeviceState {
    /// Last time any command was sent to the device (milliseconds since start)
    pub last_command_time: AtomicU64,
    /// Event log for timeline debugging
    pub event_log: Mutex<DeviceEventLog>,
    /// Start instant for monotonic timing
    pub start_instant: Instant,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TuyaDeviceConfig {
    pub name: String,
    pub id: String,
    pub local_key: String,
    pub ip: String,
    pub version: String,
    pub max_brightness: Option<f32>,
    pub power_on_field: Option<String>,
    pub capabilities: Option<Capabilities>,
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
    messages: Vec<crate::tuyapi::mesparse::Message>,
    config: &TuyaDeviceConfig,
) -> Result<MqttDevice> {
    let first = messages
        .first()
        .context("Expected Tuya response to contain at least one message")?;

    let dps_value = match &first.payload {
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

    let dps: HashMap<String, serde_json::Value> = serde_json::from_value(dps_value.clone())?;

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
    let color = if mode == Some(&Value::String("colour".to_string())) {
        let value = dps
            .get(DEFAULT_COLOR_FIELD)
            .context("Expected to find device color in COLOR_FIELD when device is in color mode")?;
        let color = value
            .as_str()
            .context("Could not deserialize color as string")?;

        let h = i32::from_str_radix(&color[0..4], 16)?;
        let s = i32::from_str_radix(&color[4..8], 16)?;
        Some(DeviceColor::Hs(Hs {
            h: h as u16,
            s: s as f32 / 1000.,
        }))
    } else if mode == Some(&Value::String("white".to_string())) {
        let value = dps.get(DEFAULT_COLOR_TEMP_FIELD).context("Expected to find device color temperature in COLOR_TEMP_FIELD when device is in CT mode")?;
        let ct = value
            .as_u64()
            .context("Could not deserialize color temperature as u64")?;

        // Scale range to 0-1 and convert to kelvin
        let q = ct as f32 / 1000.0;
        let k = q * (MAX_SUPPORTED_CT - MIN_SUPPORTED_CT) as f32 + MIN_SUPPORTED_CT as f32;

        Some(DeviceColor::Ct(Ct { ct: k as u16 }))
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
        color,
        transition_ms: Some(500.0),
        sensor_value: None,
        capabilities: Some(config.capabilities.clone().unwrap_or_default()),
        raw: Some(dps_value),
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
    if let Some(DeviceColor::Hs(color)) = mqtt_device.color {
        // HSV is represented as a string of i16 hex values
        let hue: f32 = color.h as f32;
        let saturation = color.s * 1000.0;

        let value = {
            let brightness = mqtt_device.brightness.unwrap_or(1.0);
            let brightness = brightness.min(device_config.max_brightness.unwrap_or(1.0));
            brightness * 1000.0
        };

        let tuya_color_string = format!(
            "{:0>4x}{:0>4x}{:0>4x}",
            hue as i16, saturation as i16, value as i16
        );

        dps.insert(DEFAULT_COLOR_FIELD.to_string(), json!(tuya_color_string));
        dps.insert(DEFAULT_MODE_FIELD.to_string(), json!("colour"));
    } else if let Some(DeviceColor::Ct(Ct { ct })) = mqtt_device.color {
        // Scale the value into 0.0 - 1.0 range
        let q = (ct - MIN_SUPPORTED_CT) as f32 / (MAX_SUPPORTED_CT - MIN_SUPPORTED_CT) as f32;
        let q = q.clamp(0.0, 1.0);

        // Scale the value into 0 - 1000 range
        let tuya_ct = f32::floor(q * 1000.0) as u32;

        dps.insert(DEFAULT_COLOR_TEMP_FIELD.to_string(), json!(tuya_ct));
        dps.insert(DEFAULT_MODE_FIELD.to_string(), json!("white"));
    }

    serde_json::Value::Object(dps)
}

impl DeviceState {
    pub fn new(device_name: String, device_id: String, device_version: String) -> Self {
        Self {
            last_command_time: AtomicU64::new(0),
            event_log: Mutex::new(DeviceEventLog::new(device_name, device_id, device_version)),
            start_instant: Instant::now(),
        }
    }

    /// Get elapsed time since start in milliseconds
    pub fn elapsed_ms(&self) -> u64 {
        self.start_instant.elapsed().as_millis() as u64
    }

    /// Update last command time and return how long we need to wait for throttling
    pub fn throttle_delay(&self) -> Duration {
        let now = self.elapsed_ms();
        let last = self.last_command_time.load(Ordering::Relaxed);
        let elapsed = now.saturating_sub(last);

        if elapsed >= COMMAND_THROTTLE_MS {
            Duration::ZERO
        } else {
            Duration::from_millis(COMMAND_THROTTLE_MS - elapsed)
        }
    }

    /// Mark that a command was sent
    pub fn mark_command_sent(&self) {
        self.last_command_time.store(self.elapsed_ms(), Ordering::Relaxed);
    }

    /// Check if heartbeat should be skipped due to recent activity
    pub fn should_skip_heartbeat(&self) -> Option<u64> {
        let now = self.elapsed_ms();
        let last = self.last_command_time.load(Ordering::Relaxed);
        let elapsed = now.saturating_sub(last);

        if elapsed < HEARTBEAT_SKIP_IF_ACTIVITY_MS {
            Some(elapsed)
        } else {
            None
        }
    }

    /// Log an event
    pub async fn log_event(&self, event_type: DeviceEventType) {
        let mut log = self.event_log.lock().await;
        log.log(event_type);
    }

    /// Dump timeline on failure
    pub async fn dump_timeline(&self, failure_reason: &str) {
        let log = self.event_log.lock().await;
        log.dump_timeline(failure_reason);
    }
}

/// Process commands from the queue with throttling
async fn process_command(
    tuya_device: &Arc<RwLock<TuyaDevice>>,
    device_state: &Arc<DeviceState>,
    device_id: &str,
    command: DeviceCommand,
) -> Result<()> {
    // Apply throttling
    let delay = device_state.throttle_delay();
    if !delay.is_zero() {
        device_state.log_event(DeviceEventType::Throttled {
            delayed_ms: delay.as_millis() as u64,
        }).await;
        tokio::time::sleep(delay).await;
    }

    // Mark command sent time
    device_state.mark_command_sent();

    // Execute the command
    let mut tuya = tuya_device.write().await;

    match command {
        DeviceCommand::SetValues(dps) => {
            let dps_str = serde_json::to_string(&dps).unwrap_or_default();
            device_state.log_event(DeviceEventType::CommandSent(dps_str)).await;

            let result = timeout(
                Duration::from_millis(OPERATION_TIMEOUT_MS),
                tuya.set_values(dps),
            ).await;

            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => {
                    device_state.log_event(DeviceEventType::Error(format!("set_values: {:?}", e))).await;
                    Err(anyhow!("set_values failed: {:?}", e))
                }
                Err(_) => {
                    device_state.log_event(DeviceEventType::Timeout("set_values".to_string())).await;
                    Err(anyhow!("set_values timeout"))
                }
            }
        }
        DeviceCommand::Poll => {
            device_state.log_event(DeviceEventType::PollSent).await;

            let result = timeout(
                Duration::from_millis(OPERATION_TIMEOUT_MS),
                tuya.get(Payload::Struct(PayloadStruct {
                    dev_id: device_id.to_string(),
                    gw_id: Some(device_id.to_string()),
                    uid: Some(device_id.to_string()),
                    t: Some("0".to_string()),
                    dp_id: None,
                    dps: None,
                })),
            ).await;

            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => {
                    device_state.log_event(DeviceEventType::Error(format!("poll: {:?}", e))).await;
                    Err(anyhow!("poll failed: {:?}", e))
                }
                Err(_) => {
                    device_state.log_event(DeviceEventType::Timeout("poll".to_string())).await;
                    Err(anyhow!("poll timeout"))
                }
            }
        }
        DeviceCommand::Heartbeat => {
            // Check if we should skip heartbeat due to recent activity
            if let Some(last_activity_ms) = device_state.should_skip_heartbeat() {
                device_state.log_event(DeviceEventType::HeartbeatSkipped { last_activity_ms }).await;
                return Ok(());
            }

            device_state.log_event(DeviceEventType::HeartbeatSent).await;

            let result = timeout(
                Duration::from_millis(OPERATION_TIMEOUT_MS),
                tuya.heartbeat(),
            ).await;

            match result {
                Ok(Ok(())) => {
                    debug!("Heartbeat sent");
                    Ok(())
                }
                Ok(Err(e)) => {
                    device_state.log_event(DeviceEventType::Error(format!("heartbeat: {:?}", e))).await;
                    Err(anyhow!("heartbeat failed: {:?}", e))
                }
                Err(_) => {
                    device_state.log_event(DeviceEventType::Timeout("heartbeat".to_string())).await;
                    Err(anyhow!("heartbeat timeout"))
                }
            }
        }
    }
}

pub async fn connect_and_poll_with_device(
    device_config: TuyaDeviceConfig,
    mqtt_client: MqttClient,
    tuya_device: Arc<RwLock<TuyaDevice>>,
    device_state: Arc<DeviceState>,
) -> Result<()> {
    let mqtt_rx_map = mqtt_client.rx_map.clone();
    let id = device_config.id.clone();

    // Log connection attempt
    device_state.log_event(DeviceEventType::ConnectAttempt).await;

    // Connect to the device
    let mut rx = {
        debug!("Connecting to {}", device_config.name);
        let mut tuya_device = tuya_device.write().await;
        timeout(
            Duration::from_millis(CONNECT_TIMEOUT_MS),
            tuya_device.connect(),
        )
        .await??
    };

    // Log successful connection
    device_state.log_event(DeviceEventType::Connected).await;
    info!("Successfully connected to {} (v{})", device_config.name, device_config.version);

    // Channel for decoupling MQTT publishing from Tuya receive loop
    // This prevents MQTT slowness from causing Tuya connection timeouts
    let (mqtt_tx, mut mqtt_publish_rx) = tokio::sync::mpsc::channel::<(String, String)>(16);

    // Command queue channel - all commands go through this for throttling
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<DeviceCommand>(32);

    // Tuya -> MQTT (send to channel, non-blocking)
    let tuya2mqtt = {
        let device_config = device_config.clone();
        let mqtt_client = mqtt_client.clone();
        let device_state = device_state.clone();

        async move {
            // Ignore some garbage data that at least my Tuya lamps send after a
            // ControlNew message response
            let mut ignore_next = false;

            loop {
                // Add timeout on receive to detect stale connections that don't close cleanly
                let messages = timeout(Duration::from_millis(RECEIVE_TIMEOUT_MS), rx.recv()).await;

                let messages = match messages {
                    Ok(Some(result)) => {
                        match &result {
                            Ok(msgs) => {
                                // Log received message summary
                                let summary = msgs.iter()
                                    .map(|m| format!("{:?}", m.command))
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                device_state.log_event(DeviceEventType::MessageReceived(summary)).await;
                            }
                            Err(e) => {
                                device_state.log_event(DeviceEventType::Error(format!("recv: {:?}", e))).await;
                            }
                        }
                        result?
                    }
                    Ok(None) => {
                        // Channel closed
                        device_state.log_event(DeviceEventType::Error("Receive channel closed".to_string())).await;
                        return Err(anyhow!("Receive channel closed"));
                    }
                    Err(_) => {
                        // Timeout - connection may be stale
                        device_state.log_event(DeviceEventType::ReceiveTimeout).await;
                        warn!(
                            "Receive timeout on {}, connection may be stale",
                            device_config.name
                        );
                        return Err(anyhow!("Receive timeout - connection stale"));
                    }
                };

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

                    // Send to channel instead of blocking on MQTT publish
                    // Use try_send to avoid blocking if channel is full (drop old state)
                    if let Err(e) = mqtt_tx.try_send((topic, json)) {
                        debug!("MQTT channel full, dropping message: {:?}", e);
                    }
                }
            }

            #[allow(unreachable_code)]
            anyhow::Ok(())
        }
    };

    // MQTT -> Command Queue
    let mqtt2cmd = {
        let device_config = device_config.clone();
        let cmd_tx = cmd_tx.clone();

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
                cmd_tx.send(DeviceCommand::SetValues(dps)).await?;
            }

            #[allow(unreachable_code)]
            anyhow::Ok(())
        }
    };

    // Poll scheduler -> Command Queue
    let poll_scheduler = {
        let cmd_tx = cmd_tx.clone();

        async move {
            loop {
                // Add random jitter before polling to spread out requests across devices
                let jitter: u64 = rand::random::<u64>() % (POLL_JITTER_MS + 1);
                let sleep_duration = Duration::from_millis(POLL_INTERVAL_MS + jitter);
                tokio::time::sleep(sleep_duration).await;

                cmd_tx.send(DeviceCommand::Poll).await?;
            }

            #[allow(unreachable_code)]
            anyhow::Ok(())
        }
    };

    // Heartbeat scheduler -> Command Queue
    let heartbeat_scheduler = {
        let cmd_tx = cmd_tx.clone();

        async move {
            loop {
                // Add random jitter to break synchronization between devices
                let jitter: u64 = rand::random::<u64>() % (HEARTBEAT_JITTER_MS + 1);
                tokio::time::sleep(Duration::from_millis(HEARTBEAT_INTERVAL_MS + jitter)).await;

                cmd_tx.send(DeviceCommand::Heartbeat).await?;
            }

            #[allow(unreachable_code)]
            anyhow::Ok(())
        }
    };

    // Command processor - processes commands from queue with throttling
    let command_processor = {
        let tuya_device = tuya_device.clone();
        let device_state = device_state.clone();
        let id = id.clone();

        async move {
            while let Some(command) = cmd_rx.recv().await {
                process_command(&tuya_device, &device_state, &id, command).await?;
            }

            Err(anyhow!("Command channel closed"))
        }
    };

    // MQTT publisher task - publishes messages from channel to MQTT broker
    // Decoupled from Tuya receive loop to prevent MQTT slowness from causing timeouts
    let mqtt_publisher = {
        let mqtt_client = mqtt_client.clone();
        let device_name = device_config.name.clone();

        async move {
            while let Some((topic, json)) = mqtt_publish_rx.recv().await {
                let res = mqtt_client
                    .client
                    .publish(topic, QoS::AtLeastOnce, true, json)
                    .await;

                if let Err(e) = res {
                    warn!("Error publishing to MQTT for {}: {:?}", device_name, e);
                }
            }

            // Channel closed - this shouldn't happen during normal operation
            Err(anyhow!("MQTT publish channel closed"))
        }
    };

    // Loop until any future encounters an error
    select_all(vec![
        mqtt2cmd.boxed(),
        tuya2mqtt.boxed(),
        poll_scheduler.boxed(),
        heartbeat_scheduler.boxed(),
        command_processor.boxed(),
        mqtt_publisher.boxed(),
    ])
    .await
    .0
}

/// Maximum reconnection delay (60 seconds)
const MAX_RECONNECT_DELAY_MS: u64 = 60_000;

/// Initial reconnection delay (1 second)
const INITIAL_RECONNECT_DELAY_MS: u64 = 1_000;

/// Check if an error is likely related to a device becoming unresponsive
/// These errors trigger a timeline dump for debugging
fn is_device_failure_error(error_str: &str) -> bool {
    error_str.contains("TcpStreamClosed")
        || error_str.contains("Bad read from TcpStream")
        || error_str.contains("Receive timeout")
        || error_str.contains("connection stale")
        || error_str.contains("heartbeat failed")
        || error_str.contains("heartbeat timeout")
}

/// Check if an error is transient and should reset backoff
fn is_transient_error(error_str: &str) -> bool {
    error_str.contains("Data was incomplete")
        || error_str.contains("still contains data after parsing")
        || error_str.contains("InvalidSessionKey")  // ~1/256 chance, retry immediately
}

pub async fn init_tuya(device_config: TuyaDeviceConfig, mqtt_client: MqttClient) {
    tokio::spawn(async move {
        let mut reconnect_delay = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);

        // Create shared device state for event logging and throttling
        let device_state = Arc::new(DeviceState::new(
            device_config.name.clone(),
            device_config.id.clone(),
            device_config.version.clone(),
        ));

        // Create shared device handle for explicit cleanup
        let tuya_device = Arc::new(RwLock::new(
            TuyaDevice::new(
                &device_config.version,
                &device_config.id,
                Some(&device_config.local_key),
                IpAddr::from_str(&device_config.ip).unwrap(),
            )
            .unwrap(),
        ));

        loop {
            let device_config = device_config.clone();
            let mqtt_client = mqtt_client.clone();
            let tuya_device = tuya_device.clone();
            let device_state = device_state.clone();

            let name = device_config.name.clone();
            let res = connect_and_poll_with_device(
                device_config,
                mqtt_client,
                tuya_device.clone(),
                device_state.clone(),
            ).await;

            match res {
                Ok(()) => {
                    // This shouldn't happen as connect_and_poll runs indefinitely
                    reconnect_delay = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);
                }
                Err(e) => {
                    let error_str = format!("{:?}", e);

                    // Log the error event
                    device_state.log_event(DeviceEventType::Error(error_str.clone())).await;

                    // Check if this is a device failure that warrants timeline dump
                    if is_device_failure_error(&error_str) {
                        // Dump the timeline for debugging
                        device_state.dump_timeline(&error_str).await;
                    }

                    // Check if this is a transient error that should reset backoff
                    if is_transient_error(&error_str) {
                        // Minor issue - reset backoff for quick retry
                        reconnect_delay = Duration::from_millis(INITIAL_RECONNECT_DELAY_MS);
                        info!("Transient error on {}, retrying quickly", name);
                    }

                    eprintln!(
                        "Error while polling {}: {:?}, retrying in {:?}",
                        name, e, reconnect_delay
                    );

                    // CRITICAL: Explicitly disconnect before reconnecting
                    // This ensures:
                    // 1. Background read task is aborted (prevents zombie tasks)
                    // 2. TCP FIN/RST is sent to device (allows device to reset session state)
                    // 3. v3.4 session keys are properly invalidated
                    // Without this, devices enter "Bad read from TcpStream" loop
                    {
                        device_state.log_event(DeviceEventType::Disconnected).await;
                        let mut device = tuya_device.write().await;
                        let _ = device.disconnect().await;
                    }

                    // Wait before reconnecting with exponential backoff
                    tokio::time::sleep(reconnect_delay).await;

                    // Exponential backoff: double the delay, up to maximum
                    // For persistent failures (host unreachable, timeouts, deadlines),
                    // this will back off to 60s to avoid log spam
                    reconnect_delay =
                        (reconnect_delay * 2).min(Duration::from_millis(MAX_RECONNECT_DELAY_MS));
                }
            }
        }
    });
}
