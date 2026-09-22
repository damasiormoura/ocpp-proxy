//! MQTT publisher for Home Assistant integration.
//!
//! Asynchronously publishes OCPP events to the MQTT broker, decoupled from the forwarding path.
//! Uses rumqttc for MQTT 3.1.1 connectivity with TLS, Last Will and Testament, and automatic
//! reconnection with exponential backoff.
//!
//! # Why nothing here ever awaits a publish
//!
//! rumqttc's `AsyncClient` hands requests to the `EventLoop` over a bounded
//! channel, and only `EventLoop::poll()` moves them from that channel onto the
//! wire. The publisher drives that event loop itself, from the same task, so
//! any `publish(..).await` made while `poll()` is not being awaited — inside
//! the handler for an event `poll()` just returned, say — waits for room that
//! only `poll()` can make. With more messages than the channel holds, that
//! wait never ends: the task parks, keepalives stop, the broker publishes the
//! Last Will, and the process keeps running with its Home Assistant side dead
//! and nothing in the log to say so. That is exactly what happened on
//! 2026-09-19, when a broker restart found 14 buffered messages and the
//! channel holds [`REQUEST_CHANNEL_CAPACITY`].
//!
//! So every publish goes through [`MqttPublisher::enqueue`]: the message joins
//! the FIFO buffer and [`MqttPublisher::drain_buffer`] hands as many of the
//! front of the buffer to the client as `try_publish` accepts, never blocking.
//! Whatever does not fit waits for the next event-loop tick, which drains
//! again. The buffer is therefore both the "broker unreachable" store and the
//! staging queue in front of the request channel, and FIFO order holds across
//! both roles.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use rumqttc::TlsConfiguration;
use rumqttc::{
    AsyncClient, Event, EventLoop, Incoming, LastWill, MqttOptions, QoS, SubscribeFilter, Transport,
};

use crate::snapshot_store::SnapshotStore;
use serde::Serialize;
use tokio::sync::{mpsc, Mutex};
use tokio::time;
use tracing::{debug, info, warn};

use crate::command::{
    self, CommandRequest, CommandResult, CommandRouter, CommandSource, CommandStatus,
    DispatchError, ProxyCommand, COMMAND_SUBSCRIPTIONS,
};
use crate::config::{ChargingConfig, MqttConfig};
use crate::error::ProxyError;
use crate::forwarder::MqttEvent;
use crate::models::{
    ConnectionId, ConnectionState, Direction, ExponentialBackoff, OcppMessageType,
};
use crate::state::ConnectionStateManager;

/// Depth of the request channel between the `AsyncClient` and its `EventLoop`.
///
/// Small on purpose: the channel is only staging for what `poll()` writes
/// next, and the FIFO buffer behind it (`buffers.mqtt_buffer_size`, 500 by
/// default) is where anything waiting actually lives. Any value works for
/// correctness — nothing blocks on it any more — but it is the threshold the
/// regression tests exercise, so it is named rather than inlined.
pub const REQUEST_CHANNEL_CAPACITY: usize = 10;

/// A buffered MQTT message awaiting publication.
#[derive(Debug, Clone)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}

/// JSON payload published to the MQTT broker for OCPP message events.
///
/// Contains the timestamp of when the proxy received the message, the message type,
/// and the full original OCPP message as a parsed JSON value.
#[derive(Debug, Clone, Serialize)]
pub struct MqttPayload {
    /// ISO 8601 timestamp of when the proxy received the OCPP message.
    pub timestamp: String,
    /// Message type: "Call", "CallResult", or "CallError".
    pub message_type: String,
    /// The full original OCPP message JSON array.
    pub payload: serde_json::Value,
}

/// Retained snapshot of what the charge point is currently doing.
///
/// Exists because the per-message topics are events, not state: they are
/// published non-retained, so a consumer that subscribes after the fact — Home
/// Assistant restarting, say — sees nothing until the charger next changes
/// state. That can be hours. This topic is retained, so a subscriber learns
/// the current status the moment it connects.
///
/// Deliberately excludes anything derived from MeterValues. Those arrive every
/// few seconds during a charge, and retaining them would mean a retained
/// publish per meter reading for values the consumer already gets live from
/// the MeterValues topic itself.
#[derive(Debug, Clone, Default, PartialEq, Serialize, serde::Deserialize)]
pub struct ChargePointState {
    /// Latest connector status: Available, Preparing, Charging, Faulted, ...
    pub connector_status: Option<String>,
    /// Latest error code; `NoError` when healthy.
    pub error_code: Option<String>,
    /// Transaction currently open, as assigned by the Central System.
    pub transaction_id: Option<i64>,
    /// The tag that authorised the open transaction.
    pub id_tag: Option<String>,
    /// Meter reading in Wh when the open transaction started.
    pub meter_start_wh: Option<i64>,

    // The `last_*` fields below describe the transaction that most recently
    // ENDED, so unlike the fields above they are deliberately never cleared:
    // they have to survive both the next session starting and it ending. They
    // exist because `StopTransaction` is an event topic, published
    // non-retained — a consumer that subscribes afterwards has no way to learn
    // what the previous session did until another one happens to end, which
    // can be days.
    /// Meter reading in Wh when the last completed transaction closed.
    pub last_meter_stop_wh: Option<i64>,
    /// Energy delivered by the last completed transaction, in Wh.
    ///
    /// `meterStop - meterStart`, computed while the start reading is still in
    /// the snapshot. `None` when the proxy never saw the matching
    /// `StartTransaction` — after a restart mid-session, say — because the
    /// figure is then genuinely unknown rather than zero. Not clamped: a
    /// negative would mean the charger's register went backwards, which is
    /// worth seeing rather than hiding behind a floor of zero.
    pub last_session_energy_wh: Option<i64>,
    /// Transaction id of the last completed transaction.
    pub last_transaction_id: Option<i64>,
    /// Why the last transaction ended: EVDisconnected, Local, Remote, ...
    ///
    /// Optional in OCPP 1.6; some chargers omit it.
    pub last_stop_reason: Option<String>,
    /// The charger's own timestamp for the end of the last transaction.
    ///
    /// Distinct from `last_updated`, which is when the proxy folded the
    /// message in. They differ if the charger buffered the message offline.
    pub last_stop_time: Option<String>,

    /// The charging-current limit the proxy last had the charger accept, in
    /// amps. `None` when no proxy limit is installed. `0` means paused.
    #[serde(default)]
    pub current_limit_a: Option<f64>,
    /// Outcome of the most recent limit command: `accepted`, `rejected`,
    /// `error` or `timeout`. Lets the dashboard show a limit that did not
    /// take, rather than silently keeping the previous figure.
    #[serde(default)]
    pub current_limit_status: Option<String>,
    #[serde(default)]
    pub current_limit_updated: Option<String>,

    /// ISO 8601 time this snapshot last changed.
    pub last_updated: Option<String>,
}

impl ChargePointState {
    /// Fold one forwarded OCPP message into the snapshot.
    ///
    /// Returns whether anything actually changed, so the caller only publishes
    /// on a real transition rather than on every message.
    pub fn apply(&mut self, action: &str, message_type: &OcppMessageType, raw: &str) -> bool {
        let Some(args) = ocpp_args(raw, message_type) else {
            return false;
        };
        let before = self.clone();

        match (action, message_type) {
            ("StatusNotification", OcppMessageType::Call { .. }) => {
                if let Some(v) = args.get("status").and_then(|v| v.as_str()) {
                    self.connector_status = Some(v.to_string());
                }
                if let Some(v) = args.get("errorCode").and_then(|v| v.as_str()) {
                    self.error_code = Some(v.to_string());
                }
            }
            ("StartTransaction", OcppMessageType::Call { .. }) => {
                self.meter_start_wh = args.get("meterStart").and_then(|v| v.as_i64());
                self.id_tag = args
                    .get("idTag")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
            }
            // The Central System assigns the id, so it arrives on the result.
            ("StartTransaction", OcppMessageType::CallResult) => {
                self.transaction_id = args.get("transactionId").and_then(|v| v.as_i64());
            }
            ("StopTransaction", OcppMessageType::Call { .. }) => {
                // Record the session that just ended BEFORE clearing the open
                // one: the delivered-energy delta needs `meter_start_wh`, and
                // the clear below drops it.
                let meter_stop = args.get("meterStop").and_then(|v| v.as_i64());
                self.last_meter_stop_wh = meter_stop;
                self.last_session_energy_wh = match (meter_stop, self.meter_start_wh) {
                    (Some(stop), Some(start)) => Some(stop - start),
                    _ => None,
                };
                // The message carries the id it is closing; prefer it over the
                // snapshot's, which is empty if the proxy missed the start.
                self.last_transaction_id = args
                    .get("transactionId")
                    .and_then(|v| v.as_i64())
                    .or(self.transaction_id);
                self.last_stop_reason = args
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                self.last_stop_time = args
                    .get("timestamp")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);

                self.transaction_id = None;
                self.id_tag = None;
                self.meter_start_wh = None;
            }
            _ => return false,
        }

        if *self == before {
            return false;
        }
        self.last_updated = Some(chrono::Utc::now().to_rfc3339());
        true
    }
}

impl ChargePointState {
    /// Fold the outcome of a limit command into the snapshot. Other commands
    /// leave it untouched. Returns whether anything changed.
    pub fn apply_command_result(&mut self, result: &CommandResult) -> bool {
        if !result.is_limit_result() {
            return false;
        }
        let before = self.clone();

        self.current_limit_status = Some(result.status.as_str().to_string());
        if result.status == CommandStatus::Accepted {
            self.current_limit_a = match result.action.as_deref() {
                Some("SetChargingProfile") => result.applied_limit_a,
                _ => None,
            };
        }

        if *self == before {
            return false;
        }
        let now = chrono::Utc::now().to_rfc3339();
        self.current_limit_updated = Some(now.clone());
        self.last_updated = Some(now);
        true
    }
}

/// Extract the argument object from a raw OCPP frame.
///
/// The index depends on the message type, NOT on which side sent it: a Call is
/// `[2, uniqueId, action, args]` and a CallResult is `[3, uniqueId, args]`.
/// Keying off direction instead happens to work for charger-initiated traffic
/// and breaks on anything the Central System initiates, such as
/// RemoteStartTransaction.
fn ocpp_args(
    raw: &str,
    message_type: &OcppMessageType,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let parsed: serde_json::Value = serde_json::from_str(raw).ok()?;
    let arr = parsed.as_array()?;
    let index = match message_type {
        OcppMessageType::Call { .. } => 3,
        OcppMessageType::CallResult | OcppMessageType::CallError => 2,
    };
    arr.get(index)?.as_object().cloned()
}

/// JSON payload published for connection status changes.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct StatusPayload {
    /// Current upstream connection state.
    pub upstream: String,
    /// Current downstream connection state.
    pub downstream: String,
}

/// Map an `OcppMessageType` to its string representation.
pub fn message_type_str(msg_type: &OcppMessageType) -> &'static str {
    match msg_type {
        OcppMessageType::Call { .. } => "Call",
        OcppMessageType::CallResult => "CallResult",
        OcppMessageType::CallError => "CallError",
    }
}

/// Map a `Direction` to the MQTT topic segment string.
pub fn direction_str(direction: Direction) -> &'static str {
    match direction {
        Direction::ChargerToCentral => "charger",
        Direction::CentralToCharger => "central_system",
    }
}

/// Map a `ConnectionState` to its string representation for status payloads.
pub fn connection_state_str(state: ConnectionState) -> &'static str {
    match state {
        ConnectionState::Connected => "connected",
        ConnectionState::Disconnected => "disconnected",
        ConnectionState::Reconnecting => "reconnecting",
        ConnectionState::Connecting => "connecting",
    }
}

/// Manages the MQTT connection lifecycle and event publishing.
///
/// Handles connection, reconnection with exponential backoff, LWT configuration,
/// and buffering of messages when the broker is unreachable.
pub struct MqttPublisher {
    /// The rumqttc async client for publishing messages.
    client: AsyncClient,
    /// The rumqttc event loop for processing connection events.
    eventloop: EventLoop,
    /// Charge Point ID used for the availability topic and Last Will.
    ///
    /// Message and status topics come from each event instead, because the
    /// broker connection is opened before any charger has connected and the
    /// Last Will has to be registered at that point.
    lwt_charge_point_id: Option<String>,
    /// Receiver for forwarded OCPP events from the forwarder.
    event_rx: mpsc::Receiver<MqttEvent>,
    /// Buffer for messages when broker is unreachable.
    buffer: VecDeque<MqttMessage>,
    /// Maximum buffer capacity (default: 500).
    max_buffer_size: usize,
    /// Current connection state.
    state: ConnectionState,
    /// Exponential backoff for reconnection (1s initial, 30s max).
    backoff: ExponentialBackoff,
    /// Retained snapshot of the charge point, per Charge Point ID.
    charge_point_state: std::collections::HashMap<String, ChargePointState>,
    /// Durable backing for `charge_point_state`, so a proxy restart does not
    /// lose what the last session recorded. See `snapshot_store`.
    snapshot_store: SnapshotStore,
    /// The process-wide connection state, when the publisher was given one.
    ///
    /// `/health` reads `mqtt` from here. Before this existed, `main` set it
    /// once after the startup connect and nothing ever updated it again, so
    /// the endpoint reported `connected` through a six-hour outage.
    state_manager: Option<Arc<Mutex<ConnectionStateManager>>>,
    /// Number of messages published so far by the flush that a reconnect
    /// started, while that flush is still in progress; `None` otherwise.
    ///
    /// The flush is no longer one call: it proceeds across event-loop ticks
    /// as the request channel makes room. This is what lets the log still
    /// say `Flushed buffered MQTT messages` exactly once, when the backlog
    /// the reconnect found has actually gone.
    flush_in_progress: Option<usize>,
    /// Set when charger commands are enabled: where incoming commands go.
    command_router: Option<CommandRouter>,
    charging: Option<ChargingConfig>,
    /// The command subscriptions still have to be (re)sent on this connection.
    subscribe_pending: bool,
    /// Last known (upstream, downstream) per charge point, to spot the link
    /// coming up and re-apply the standing limit.
    link_state: HashMap<String, (ConnectionState, ConnectionState)>,
}

impl MqttPublisher {
    /// Create a new `MqttPublisher` with the given configuration.
    ///
    /// Configures the MQTT client with:
    /// - TLS 1.2+ using CA cert, client cert, and client key
    /// - 60-second keepalive interval
    /// - Last Will and Testament on `ocpp/{charge_point_id}/availability` with payload "offline"
    /// - QoS 1 and retained flag for LWT
    ///
    /// Returns the publisher and does NOT initiate connection yet.
    /// Call `start()` to begin the connection and event loop.
    pub fn new(
        mqtt_config: &MqttConfig,
        lwt_charge_point_id: Option<String>,
        event_rx: mpsc::Receiver<MqttEvent>,
        max_buffer_size: usize,
        snapshot_store: SnapshotStore,
    ) -> Result<Self, ProxyError> {
        let client_id = match &lwt_charge_point_id {
            Some(id) => format!("ocpp-proxy-{}", id),
            None => "ocpp-proxy".to_string(),
        };
        let mut mqttoptions = MqttOptions::new(&client_id, &mqtt_config.host, mqtt_config.port);

        // Configure credentials
        mqttoptions.set_credentials(&mqtt_config.username, &mqtt_config.password);

        // Configure keepalive (60 seconds)
        mqttoptions.set_keep_alive(Duration::from_secs(60));

        // Configure Last Will and Testament.
        //
        // Only possible when the Charge Point ID is known from configuration:
        // the will has to be registered in the CONNECT packet, before any
        // charger has connected to tell us its ID.
        if let Some(id) = &lwt_charge_point_id {
            let availability_topic = availability_topic(id);
            mqttoptions.set_last_will(LastWill::new(
                &availability_topic,
                "offline",
                QoS::AtLeastOnce,
                true,
            ));
        } else {
            warn!(
                component = "mqtt",
                "No charge_point_id configured: connecting without a Last Will. \
                 Home Assistant will not be told if this proxy dies unexpectedly."
            );
        }

        // TLS is optional. The broker is one LAN hop away on the Home
        // Assistant VM, not across the internet, so plaintext with
        // username/password is a reasonable default.
        if let Some(tls_config) = Self::build_tls_config(mqtt_config)? {
            mqttoptions.set_transport(Transport::Tls(tls_config));
        }

        // Create client and event loop
        let (client, eventloop) = AsyncClient::new(mqttoptions, REQUEST_CHANNEL_CAPACITY);

        Ok(Self {
            client,
            eventloop,
            lwt_charge_point_id,
            event_rx,
            buffer: VecDeque::new(),
            max_buffer_size,
            state: ConnectionState::Disconnected,
            backoff: ExponentialBackoff::with_defaults(
                Duration::from_secs(1),
                Duration::from_secs(30),
            ),
            // Loaded here, before the event loop exists, so nothing can
            // fold a message in ahead of the restore.
            charge_point_state: snapshot_store.load(),
            snapshot_store,
            state_manager: None,
            flush_in_progress: None,
            command_router: None,
            charging: None,
            subscribe_pending: false,
            link_state: HashMap::new(),
        })
    }

    /// Accept charger commands from MQTT and route them to sessions. Without
    /// this the publisher never subscribes to anything.
    pub fn with_command_router(mut self, router: CommandRouter, charging: ChargingConfig) -> Self {
        self.command_router = Some(router);
        self.charging = Some(charging);
        self
    }

    pub fn commands_enabled(&self) -> bool {
        self.command_router.is_some()
    }

    /// Mirror every MQTT connection state change into the shared manager, so
    /// `/health` reports what the event loop actually sees.
    pub fn with_state_manager(mut self, manager: Arc<Mutex<ConnectionStateManager>>) -> Self {
        self.state_manager = Some(manager);
        self
    }

    /// Record a connection state change, locally and in the shared manager.
    async fn set_state(&mut self, new_state: ConnectionState) {
        self.state = new_state;
        if let Some(manager) = &self.state_manager {
            manager
                .lock()
                .await
                .transition(ConnectionId::Mqtt, new_state);
        }
    }

    /// Build TLS configuration from certificate files, if TLS is configured.
    ///
    /// Returns `None` when no CA certificate is configured, meaning a plaintext
    /// connection. Uses rustls (rumqttc's `use-rustls` feature) for TLS 1.2+.
    fn build_tls_config(mqtt_config: &MqttConfig) -> Result<Option<TlsConfiguration>, ProxyError> {
        let Some(ca_path) = mqtt_config.ca_cert_path.as_ref() else {
            return Ok(None);
        };

        let ca_cert = fs::read(ca_path).map_err(|e| ProxyError::Tls {
            description: format!("Failed to read CA certificate at '{}': {}", ca_path, e),
        })?;

        // Config validation rejects one without the other, so either both are
        // present or neither is.
        let client_auth = match (
            mqtt_config.client_cert_path.as_ref(),
            mqtt_config.client_key_path.as_ref(),
        ) {
            (Some(cert_path), Some(key_path)) => {
                let client_cert = fs::read(cert_path).map_err(|e| ProxyError::Tls {
                    description: format!(
                        "Failed to read client certificate at '{}': {}",
                        cert_path, e
                    ),
                })?;
                let client_key = fs::read(key_path).map_err(|e| ProxyError::Tls {
                    description: format!("Failed to read client key at '{}': {}", key_path, e),
                })?;
                Some((client_cert, client_key))
            }
            _ => None,
        };

        Ok(Some(TlsConfiguration::Simple {
            ca: ca_cert,
            alpn: None,
            client_auth,
        }))
    }

    /// Attempt to connect to the MQTT broker with a startup timeout.
    ///
    /// Tries to establish a connection for up to `timeout` duration.
    /// If connection is not established within the timeout, returns `Ok(false)`.
    /// The caller should proceed regardless and the event loop will continue
    /// reconnection attempts.
    pub async fn try_connect(&mut self, timeout: Duration) -> Result<bool, ProxyError> {
        self.set_state(ConnectionState::Connecting).await;
        info!(
            component = "mqtt",
            charge_point_id = self.lwt_id(),
            "Attempting MQTT connection (timeout: {:?})",
            timeout
        );

        let deadline = time::Instant::now() + timeout;

        loop {
            let remaining = deadline.saturating_duration_since(time::Instant::now());
            if remaining.is_zero() {
                warn!(
                    component = "mqtt",
                    charge_point_id = self.lwt_id(),
                    "MQTT connection timeout after {:?}, proceeding without MQTT",
                    timeout
                );
                self.set_state(ConnectionState::Reconnecting).await;
                return Ok(false);
            }

            match time::timeout(remaining, self.eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Incoming::ConnAck(_)))) => {
                    info!(
                        component = "mqtt",
                        charge_point_id = self.lwt_id(),
                        "MQTT connected successfully"
                    );
                    self.on_connack().await;
                    return Ok(true);
                }
                Ok(Ok(_)) => {
                    // Other events during connection (e.g., Outgoing::Connect) — continue polling.
                    continue;
                }
                Ok(Err(e)) => {
                    debug!(
                        component = "mqtt",
                        charge_point_id = self.lwt_id(),
                        "MQTT connection attempt failed: {}",
                        e
                    );
                    // Short sleep before retrying within the timeout window
                    let delay = self.backoff.next_delay().min(remaining);
                    time::sleep(delay).await;
                    continue;
                }
                Err(_) => {
                    // Timeout elapsed
                    warn!(
                        component = "mqtt",
                        charge_point_id = self.lwt_id(),
                        "MQTT connection timeout after {:?}, proceeding without MQTT",
                        timeout
                    );
                    self.set_state(ConnectionState::Reconnecting).await;
                    return Ok(false);
                }
            }
        }
    }

    /// What every `ConnAck` triggers, at startup and on each reconnect.
    ///
    /// Order matters and is FIFO through the buffer: `online` first, so the
    /// availability topic flips back before anything else lands; then the
    /// backlog the outage left behind; then the retained snapshots, which
    /// repair a broker that came back without its retained store. Returns as
    /// soon as the request channel is full — the remainder drains from
    /// `run()` as the event loop makes room. It never awaits a publish.
    async fn on_connack(&mut self) {
        self.set_state(ConnectionState::Connected).await;
        self.backoff.reset();
        self.publish_online();
        self.flush_buffer();
        self.republish_snapshots();
        // Sessions are clean, so subscriptions do not survive a reconnect.
        self.subscribe_pending = self.command_router.is_some();
        self.try_subscribe();
    }

    /// Send the command subscriptions if they are due. Same rule as publishes:
    /// never await the request channel from the task that drives `poll()`;
    /// if it is full now, the next event-loop tick retries.
    fn try_subscribe(&mut self) {
        if !self.subscribe_pending {
            return;
        }
        let filters = COMMAND_SUBSCRIPTIONS
            .iter()
            .map(|f| SubscribeFilter::new(f.to_string(), QoS::AtLeastOnce));
        match self.client.try_subscribe_many(filters) {
            Ok(()) => {
                self.subscribe_pending = false;
                info!(
                    component = "mqtt",
                    charge_point_id = self.lwt_id(),
                    topics = ?COMMAND_SUBSCRIPTIONS,
                    "Subscribed to charger command topics"
                );
            }
            Err(_) => {
                debug!(
                    component = "mqtt",
                    charge_point_id = self.lwt_id(),
                    "MQTT request channel full; command subscription retries on the next tick"
                );
            }
        }
    }

    /// A message arrived on a subscribed topic: a command for a charger.
    fn handle_incoming_publish(&mut self, topic: &str, payload: &[u8]) {
        let Some((charge_point_id, kind)) = command::parse_command_topic(topic) else {
            debug!(
                component = "mqtt",
                topic = %topic,
                "Ignoring incoming message on a topic that is not a command"
            );
            return;
        };

        let request = match command::parse_command_payload(kind, payload) {
            Ok(request) => request,
            Err(reason) => {
                warn!(
                    component = "mqtt",
                    charge_point_id = %charge_point_id,
                    topic = %topic,
                    reason = %reason,
                    "Rejecting malformed charger command"
                );
                self.publish_command_result(
                    &charge_point_id,
                    CommandResult::invalid_payload(reason),
                );
                return;
            }
        };

        self.dispatch_command(&charge_point_id, request);
    }

    /// Hand a request to the charger's session, or answer for it when there
    /// is none to hand it to.
    fn dispatch_command(&mut self, charge_point_id: &str, request: CommandRequest) {
        let Some(router) = &self.command_router else {
            self.publish_command_result(
                charge_point_id,
                CommandResult::for_request(
                    &request,
                    CommandStatus::Rejected,
                    Some("charger commands are disabled on this proxy".to_string()),
                ),
            );
            return;
        };

        match router.dispatch(charge_point_id, request.clone()) {
            Ok(()) => {
                debug!(
                    component = "mqtt",
                    charge_point_id = %charge_point_id,
                    command_id = %request.id,
                    action = request.command.action(),
                    "Charger command handed to its session"
                );
            }
            Err(DispatchError::NotConnected) => {
                warn!(
                    component = "mqtt",
                    charge_point_id = %charge_point_id,
                    command_id = %request.id,
                    "Charger command for a charger that is not connected"
                );
                self.publish_command_result(
                    charge_point_id,
                    CommandResult::for_request(
                        &request,
                        CommandStatus::NotConnected,
                        Some("no charger with this id is connected to the proxy".to_string()),
                    ),
                );
            }
            Err(DispatchError::QueueFull) => {
                warn!(
                    component = "mqtt",
                    charge_point_id = %charge_point_id,
                    command_id = %request.id,
                    "Charger command dropped: the session's command queue is full"
                );
                self.publish_command_result(
                    charge_point_id,
                    CommandResult::for_request(
                        &request,
                        CommandStatus::Rejected,
                        Some("the session's command queue is full".to_string()),
                    ),
                );
            }
        }
    }

    /// Publish a command's outcome and, for limit commands, fold it into the
    /// retained snapshot so the dashboard shows the standing limit.
    fn publish_command_result(&mut self, charge_point_id: &str, result: CommandResult) {
        match serde_json::to_vec(&result) {
            Ok(payload) => self.enqueue(MqttMessage {
                topic: command::command_result_topic(charge_point_id),
                payload,
                qos: QoS::AtLeastOnce,
                retain: false,
            }),
            Err(e) => warn!(
                component = "mqtt",
                charge_point_id = %charge_point_id,
                error = %e,
                "Failed to serialize command result"
            ),
        }

        let snapshot = {
            let entry = self
                .charge_point_state
                .entry(charge_point_id.to_string())
                .or_default();
            if entry.apply_command_result(&result) {
                Some(entry.clone())
            } else {
                None
            }
        };
        if let Some(snapshot) = snapshot {
            self.snapshot_store.save(&self.charge_point_state);
            self.publish_charge_point_state(charge_point_id, &snapshot);
        }
    }

    /// The charger and the Central System are both connected again. If a
    /// limit was standing, re-send it: a charger that rebooted may have
    /// forgotten its profiles, and silence would mean full current.
    fn maybe_reapply_limit(&mut self, charge_point_id: &str) {
        let Some(charging) = &self.charging else {
            return;
        };
        if !charging.reapply_on_connect {
            return;
        }
        let standing = self
            .charge_point_state
            .get(charge_point_id)
            .filter(|st| st.current_limit_status.as_deref() == Some("accepted"))
            .and_then(|st| st.current_limit_a);
        let Some(limit_a) = standing else {
            return;
        };

        info!(
            component = "mqtt",
            charge_point_id = %charge_point_id,
            limit_a = limit_a,
            "Link is up; re-applying the standing current limit"
        );
        let request = CommandRequest::new(
            format!("reapply-{}", chrono::Utc::now().timestamp_millis()),
            ProxyCommand::SetCurrentLimit { limit_a },
            CommandSource::Reapply,
        );
        self.dispatch_command(charge_point_id, request);
    }

    /// Queue a retained "online" message to the availability topic.
    ///
    /// Called after every successful connection to the broker. Goes to the
    /// front of the buffer, ahead of any backlog, so the availability topic is
    /// the first thing the broker sees change.
    fn publish_online(&mut self) {
        // Without a configured ID there is no availability topic to own, and
        // no Last Will was registered either. Nothing to announce.
        let Some(id) = self.lwt_charge_point_id.as_deref() else {
            return;
        };
        let topic = availability_topic(id);
        self.buffer.push_front(MqttMessage {
            topic: topic.clone(),
            payload: b"online".to_vec(),
            qos: QoS::AtLeastOnce,
            retain: true,
        });
        self.drain_buffer();

        debug!(
            component = "mqtt",
            charge_point_id = self.lwt_id(),
            topic = %topic,
            "Queued online availability status (retained)"
        );
    }

    /// Run the MQTT publisher event loop.
    ///
    /// Processes events from both the MQTT event loop and the forwarder channel.
    /// Handles reconnection automatically via rumqttc's built-in reconnection,
    /// combined with exponential backoff state tracking.
    ///
    /// Nothing in this loop awaits a publish (see the module docs): every
    /// event-loop tick is also a chance to move more of the buffer into the
    /// request channel, which is how a backlog larger than the channel gets
    /// out without ever stalling `poll()`.
    ///
    /// This method runs indefinitely until the event channel is closed.
    pub async fn run(&mut self) {
        info!(
            component = "mqtt",
            charge_point_id = self.lwt_id(),
            "MQTT publisher event loop started"
        );

        loop {
            tokio::select! {
                // Process MQTT event loop events
                event = self.eventloop.poll() => {
                    match event {
                        Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                            info!(
                                component = "mqtt",
                                charge_point_id = self.lwt_id(),
                                "MQTT reconnected"
                            );
                            self.on_connack().await;
                        }
                        Ok(Event::Incoming(Incoming::Publish(publish))) => {
                            self.handle_incoming_publish(&publish.topic, &publish.payload);
                            if self.state == ConnectionState::Connected {
                                self.drain_buffer();
                            }
                        }
                        Ok(_) => {
                            // Every other event (Outgoing::Publish, PubAck,
                            // PingResp, ...) means `poll()` just ran, so the
                            // request channel may have room again.
                            if self.state == ConnectionState::Connected {
                                self.try_subscribe();
                                self.drain_buffer();
                            }
                        }
                        Err(e) => {
                            if self.state == ConnectionState::Connected {
                                warn!(
                                    component = "mqtt",
                                    charge_point_id = self.lwt_id(),
                                    "MQTT connection lost: {}",
                                    e
                                );
                                self.set_state(ConnectionState::Reconnecting).await;
                            }

                            // rumqttc handles reconnection internally; we track state
                            let delay = self.backoff.next_delay();
                            debug!(
                                component = "mqtt",
                                charge_point_id = self.lwt_id(),
                                delay_ms = delay.as_millis(),
                                "MQTT reconnection backoff"
                            );
                            time::sleep(delay).await;
                        }
                    }
                }
                // Process incoming events from the forwarder
                event = self.event_rx.recv() => {
                    match event {
                        Some(mqtt_event) => {
                            self.handle_event(mqtt_event).await;
                        }
                        None => {
                            // Channel closed — publisher shutting down
                            info!(
                                component = "mqtt",
                                charge_point_id = self.lwt_id(),
                                "MQTT event channel closed, shutting down publisher"
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Handle an incoming MQTT event from the forwarder.
    async fn handle_event(&mut self, event: MqttEvent) {
        match event {
            MqttEvent::MessageForwarded {
                charge_point_id,
                frame,
                direction,
                action,
            } => {
                // Construct MQTT topic: ocpp/{charge_point_id}/{direction}/{action}
                let dir_str = direction_str(direction);
                let topic = message_topic(&charge_point_id, dir_str, &action);

                // Kept for the retained snapshot below: `frame` is consumed
                // when the per-message payload is built.
                let raw = frame.raw.clone();
                let message_type = frame.message_type.clone();

                // Parse the raw OCPP JSON for the payload field
                let payload_value = serde_json::from_str::<serde_json::Value>(&frame.raw)
                    .unwrap_or_else(|_| serde_json::Value::String(frame.raw.clone()));

                // Construct MQTT payload
                let mqtt_payload = MqttPayload {
                    timestamp: frame.received_at.to_rfc3339(),
                    message_type: message_type_str(&frame.message_type).to_string(),
                    payload: payload_value,
                };

                let payload_bytes = match serde_json::to_vec(&mqtt_payload) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        warn!(
                            component = "mqtt",
                            charge_point_id = self.lwt_id(),
                            error = %e,
                            "Failed to serialize MQTT payload"
                        );
                        return;
                    }
                };

                // QoS 1, retain=false: events, not state.
                self.enqueue(MqttMessage {
                    topic,
                    payload: payload_bytes,
                    qos: QoS::AtLeastOnce,
                    retain: false,
                });

                // Fold the message into the retained snapshot, and republish
                // only if it actually changed something.
                let snapshot = {
                    let entry = self
                        .charge_point_state
                        .entry(charge_point_id.clone())
                        .or_default();
                    if entry.apply(&action, &message_type, &raw) {
                        Some(entry.clone())
                    } else {
                        None
                    }
                };
                if let Some(snapshot) = snapshot {
                    self.snapshot_store.save(&self.charge_point_state);
                    self.publish_charge_point_state(&charge_point_id, &snapshot);
                }
            }
            MqttEvent::StateChange {
                charge_point_id,
                upstream,
                downstream,
            } => {
                // Construct status topic: ocpp/{charge_point_id}/status
                let topic = status_topic(&charge_point_id);

                // Construct status payload
                let status_payload = StatusPayload {
                    upstream: connection_state_str(upstream).to_string(),
                    downstream: connection_state_str(downstream).to_string(),
                };

                let payload_bytes = match serde_json::to_vec(&status_payload) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        warn!(
                            component = "mqtt",
                            charge_point_id = self.lwt_id(),
                            error = %e,
                            "Failed to serialize status payload"
                        );
                        return;
                    }
                };

                // QoS 1, retain=true: a subscriber that connects later must
                // see the current link state, not wait for the next change.
                self.enqueue(MqttMessage {
                    topic,
                    payload: payload_bytes,
                    qos: QoS::AtLeastOnce,
                    retain: true,
                });

                let both_up = upstream == ConnectionState::Connected
                    && downstream == ConnectionState::Connected;
                let previous = self
                    .link_state
                    .insert(charge_point_id.clone(), (upstream, downstream));
                let was_up = matches!(
                    previous,
                    Some((ConnectionState::Connected, ConnectionState::Connected))
                );
                if both_up && !was_up {
                    self.maybe_reapply_limit(&charge_point_id);
                }
            }
            MqttEvent::CommandResult {
                charge_point_id,
                result,
            } => {
                self.publish_command_result(&charge_point_id, result);
            }
        }
    }

    /// Publish the retained charge point snapshot.
    ///
    /// Retained and QoS 1: a subscriber that connects later must be told the
    /// current state immediately rather than waiting for the charger's next
    /// transition, which during an idle night is hours away.
    /// Push every snapshot we hold back onto its retained topic.
    ///
    /// Called after each ConnAck, which covers two cases. At startup it
    /// restores what the file gave us, so a subscriber sees the previous
    /// session immediately rather than after the charger's next message
    /// republishes a blank one. After a reconnect it repairs a broker that
    /// came back without its retained store.
    fn republish_snapshots(&mut self) {
        let snapshots: Vec<(String, ChargePointState)> = self
            .charge_point_state
            .iter()
            .map(|(id, st)| (id.clone(), st.clone()))
            .collect();
        if snapshots.is_empty() {
            return;
        }
        info!(
            component = "mqtt",
            charge_points = snapshots.len(),
            "Republishing retained charge point snapshots"
        );
        for (id, snapshot) in snapshots {
            self.publish_charge_point_state(&id, &snapshot);
        }
    }

    fn publish_charge_point_state(&mut self, charge_point_id: &str, snapshot: &ChargePointState) {
        let topic = charge_point_state_topic(charge_point_id);
        let bytes = match serde_json::to_vec(snapshot) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    component = "mqtt",
                    error = %e,
                    "Failed to serialize charge point state"
                );
                return;
            }
        };

        self.enqueue(MqttMessage {
            topic,
            payload: bytes,
            qos: QoS::AtLeastOnce,
            retain: true,
        });
    }

    /// Queue a message for publication and push as much as fits.
    ///
    /// The single entry point for every publish. While connected, the buffer
    /// drains straight into the request channel, so in the common case the
    /// message never waits; while disconnected it stays buffered until the
    /// next `ConnAck`. Never blocks. Public so that anything holding the
    /// publisher — a test, or a future caller outside this module — has no
    /// reason to reach for `client().publish().await` and reintroduce the
    /// deadlock described in the module docs.
    pub fn enqueue(&mut self, message: MqttMessage) {
        self.buffer_message(message);
        if self.state == ConnectionState::Connected {
            self.drain_buffer();
        }
    }

    /// Hand the front of the buffer to the client until it stops accepting.
    ///
    /// `try_publish` fails only when the request channel is full (or the
    /// event loop is gone, which cannot happen while `self` owns it), so a
    /// refusal means "wait for `poll()` to make room", and the message stays
    /// at the front for the next tick. Returns how many were accepted.
    fn drain_buffer(&mut self) -> usize {
        let mut accepted = 0;
        while let Some(msg) = self.buffer.front() {
            match self
                .client
                .try_publish(&msg.topic, msg.qos, msg.retain, msg.payload.clone())
            {
                Ok(()) => {
                    self.buffer.pop_front();
                    accepted += 1;
                }
                Err(_) => {
                    debug!(
                        component = "mqtt",
                        charge_point_id = self.lwt_id(),
                        waiting = self.buffer.len(),
                        "MQTT request channel full; the rest drains on the next event-loop tick"
                    );
                    break;
                }
            }
        }

        if let Some(published) = self.flush_in_progress {
            let published = published + accepted;
            if self.buffer.is_empty() {
                info!(
                    component = "mqtt",
                    charge_point_id = self.lwt_id(),
                    published = published,
                    remaining = 0usize,
                    "Flushed buffered MQTT messages"
                );
                self.flush_in_progress = None;
            } else {
                self.flush_in_progress = Some(published);
            }
        }

        accepted
    }

    /// Buffer a message for later publication.
    ///
    /// If the buffer is full, evicts the oldest message (FIFO).
    fn buffer_message(&mut self, message: MqttMessage) {
        if self.buffer.len() >= self.max_buffer_size {
            let evicted = self.buffer.pop_front();
            if let Some(msg) = evicted {
                warn!(
                    component = "mqtt",
                    charge_point_id = self.lwt_id(),
                    topic = %msg.topic,
                    "MQTT buffer full, evicting oldest message"
                );
            }
        }
        self.buffer.push_back(message);
    }

    /// Start flushing the backlog a reconnect found, in FIFO order.
    ///
    /// Logs `Flushing …` now and `Flushed …` when the buffer next runs empty,
    /// which may be several event-loop ticks later if the backlog is larger
    /// than the request channel. Never blocks; the old implementation awaited
    /// each publish here and parked for good once the channel was full.
    fn flush_buffer(&mut self) {
        let count = self.buffer.len();
        if count == 0 {
            return;
        }

        info!(
            component = "mqtt",
            charge_point_id = self.lwt_id(),
            count = count,
            "Flushing buffered MQTT messages"
        );

        self.flush_in_progress = Some(0);
        self.drain_buffer();
    }

    /// Get the current connection state.
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// Get the number of buffered messages.
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Get a reference to the MQTT client for external publishing.
    pub fn client(&self) -> &AsyncClient {
        &self.client
    }

    /// The Charge Point ID used for the availability topic, if configured.
    pub fn lwt_charge_point_id(&self) -> Option<&str> {
        self.lwt_charge_point_id.as_deref()
    }

    /// Display form of the Last Will charge point ID, for log fields.
    fn lwt_id(&self) -> &str {
        self.lwt_charge_point_id.as_deref().unwrap_or("-")
    }
}

/// Construct the availability topic for a given charge point ID.
pub fn availability_topic(charge_point_id: &str) -> String {
    format!("ocpp/{}/availability", charge_point_id)
}

/// Construct a message topic for a given charge point ID, direction, and action.
pub fn message_topic(charge_point_id: &str, direction: &str, action: &str) -> String {
    format!("ocpp/{}/{}/{}", charge_point_id, direction, action)
}

/// Construct the retained charge point state topic.
pub fn charge_point_state_topic(charge_point_id: &str) -> String {
    format!("ocpp/{}/state", charge_point_id)
}

/// Construct the status topic for a given charge point ID.
pub fn status_topic(charge_point_id: &str) -> String {
    format!("ocpp/{}/status", charge_point_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- retained charge point state ----

    fn call(action: &str) -> OcppMessageType {
        OcppMessageType::Call {
            action: action.to_string(),
        }
    }

    /// The deployed shape: a broker one LAN hop away, no TLS.
    fn plaintext_config() -> MqttConfig {
        MqttConfig {
            host: "127.0.0.1".to_string(),
            port: 1883,
            username: "user".to_string(),
            password: "pass".to_string(),
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
        }
    }

    #[test]
    fn test_state_topic() {
        assert_eq!(
            charge_point_state_topic("CP-EXAMPLE-0001"),
            "ocpp/CP-EXAMPLE-0001/state"
        );
    }

    /// Call args live at index 3, CallResult args at index 2. Indexing by
    /// direction instead of message type breaks on Central-System-initiated
    /// Calls, so this pins the rule down.
    #[test]
    fn test_ocpp_args_index_follows_message_type_not_direction() {
        let call_frame = r#"[2,"id","StatusNotification",{"status":"Charging"}]"#;
        let result_frame = r#"[3,"id",{"transactionId":42}]"#;

        let a = ocpp_args(call_frame, &call("StatusNotification")).unwrap();
        assert_eq!(a.get("status").unwrap(), "Charging");

        let b = ocpp_args(result_frame, &OcppMessageType::CallResult).unwrap();
        assert_eq!(b.get("transactionId").unwrap(), 42);
    }

    #[test]
    fn test_status_notification_updates_snapshot() {
        let mut st = ChargePointState::default();
        let changed = st.apply(
            "StatusNotification",
            &call("StatusNotification"),
            r#"[2,"1","StatusNotification",{"connectorId":1,"errorCode":"NoError","status":"Charging"}]"#,
        );
        assert!(changed);
        assert_eq!(st.connector_status.as_deref(), Some("Charging"));
        assert_eq!(st.error_code.as_deref(), Some("NoError"));
        assert!(st.last_updated.is_some());
    }

    /// Only real transitions should cause a retained republish.
    #[test]
    fn test_repeated_identical_status_reports_no_change() {
        let raw = r#"[2,"1","StatusNotification",{"connectorId":1,"errorCode":"NoError","status":"Charging"}]"#;
        let mut st = ChargePointState::default();
        assert!(st.apply("StatusNotification", &call("StatusNotification"), raw));
        assert!(
            !st.apply("StatusNotification", &call("StatusNotification"), raw),
            "an identical status must not trigger another retained publish"
        );
    }

    /// The whole lifecycle the dashboard depends on.
    #[test]
    fn test_full_transaction_lifecycle() {
        let mut st = ChargePointState::default();

        st.apply(
            "StatusNotification",
            &call("StatusNotification"),
            r#"[2,"1","StatusNotification",{"connectorId":1,"errorCode":"NoError","status":"Preparing"}]"#,
        );
        st.apply(
            "StartTransaction",
            &call("StartTransaction"),
            r#"[2,"2","StartTransaction",{"connectorId":1,"idTag":"a1b2c3d4","meterStart":8076445}]"#,
        );
        assert_eq!(st.meter_start_wh, Some(8076445));
        assert_eq!(st.id_tag.as_deref(), Some("a1b2c3d4"));
        assert_eq!(st.transaction_id, None, "id is not known until the result");

        st.apply(
            "StartTransaction",
            &OcppMessageType::CallResult,
            r#"[3,"2",{"idTagInfo":{"status":"Accepted"},"transactionId":1000000001}]"#,
        );
        assert_eq!(st.transaction_id, Some(1000000001));

        st.apply(
            "StatusNotification",
            &call("StatusNotification"),
            r#"[2,"3","StatusNotification",{"connectorId":1,"errorCode":"NoError","status":"Charging"}]"#,
        );
        assert_eq!(st.connector_status.as_deref(), Some("Charging"));

        // Stopping must clear the transaction, or the dashboard shows a stale
        // one as if a session were still open.
        st.apply(
            "StopTransaction",
            &call("StopTransaction"),
            r#"[2,"4","StopTransaction",{"idTag":"a1b2c3d4","meterStop":8080000,"transactionId":1000000001,"reason":"EVDisconnected","timestamp":"2026-09-01T07:22:16Z"}]"#,
        );
        assert_eq!(st.transaction_id, None);
        assert_eq!(st.meter_start_wh, None);
        assert_eq!(st.id_tag, None);
        assert_eq!(
            st.connector_status.as_deref(),
            Some("Charging"),
            "StopTransaction does not itself change connector status"
        );

        // ...and must record what the session that just ended did.
        assert_eq!(st.last_meter_stop_wh, Some(8080000));
        assert_eq!(st.last_session_energy_wh, Some(8080000 - 8076445));
        assert_eq!(st.last_transaction_id, Some(1000000001));
        assert_eq!(st.last_stop_reason.as_deref(), Some("EVDisconnected"));
        assert_eq!(st.last_stop_time.as_deref(), Some("2026-09-01T07:22:16Z"));
    }

    /// The regression this whole `last_*` group exists to prevent.
    ///
    /// A new session starting, and then ending, must not wipe what the
    /// previous one recorded before its own figures are in place — the Home
    /// Assistant sensor reads this topic continuously and would otherwise
    /// blink to null between the two.
    #[test]
    fn test_last_session_survives_the_next_session() {
        let mut st = ChargePointState::default();
        st.apply(
            "StartTransaction",
            &call("StartTransaction"),
            r#"[2,"1","StartTransaction",{"connectorId":1,"idTag":"a","meterStart":1000}]"#,
        );
        st.apply(
            "StopTransaction",
            &call("StopTransaction"),
            r#"[2,"2","StopTransaction",{"meterStop":3000,"transactionId":7}]"#,
        );
        assert_eq!(st.last_session_energy_wh, Some(2000));

        // A second session opens. The previous session's figures stay put.
        st.apply(
            "StartTransaction",
            &call("StartTransaction"),
            r#"[2,"3","StartTransaction",{"connectorId":1,"idTag":"b","meterStart":3000}]"#,
        );
        assert_eq!(st.last_meter_stop_wh, Some(3000));
        assert_eq!(st.last_session_energy_wh, Some(2000));
        assert_eq!(st.last_transaction_id, Some(7));

        st.apply(
            "StopTransaction",
            &call("StopTransaction"),
            r#"[2,"4","StopTransaction",{"meterStop":9500,"transactionId":8}]"#,
        );
        assert_eq!(st.last_meter_stop_wh, Some(9500));
        assert_eq!(st.last_session_energy_wh, Some(6500));
        assert_eq!(st.last_transaction_id, Some(8));
    }

    /// After a proxy restart mid-session the start reading is gone, so the
    /// delta is unknowable. Reporting zero would understate a real charge on
    /// the dashboard; `None` says so honestly. The meter stop reading itself
    /// is still recorded, because the message carries it.
    #[test]
    fn test_stop_without_a_seen_start_reports_no_delta() {
        let mut st = ChargePointState::default();
        st.apply(
            "StopTransaction",
            &call("StopTransaction"),
            r#"[2,"1","StopTransaction",{"meterStop":8084000,"transactionId":1000000001}]"#,
        );
        assert_eq!(st.last_meter_stop_wh, Some(8084000));
        assert_eq!(
            st.last_session_energy_wh, None,
            "an unknown delta must not be reported as zero"
        );
        assert_eq!(st.last_transaction_id, Some(1000000001));
    }

    /// `reason` and `timestamp` are optional in OCPP 1.6 and this charger
    /// omits `reason` on a locally stopped session.
    #[test]
    fn test_stop_without_optional_fields() {
        let mut st = ChargePointState::default();
        assert!(st.apply(
            "StopTransaction",
            &call("StopTransaction"),
            r#"[2,"1","StopTransaction",{"meterStop":500,"transactionId":3}]"#,
        ));
        assert_eq!(st.last_stop_reason, None);
        assert_eq!(st.last_stop_time, None);
        assert_eq!(st.last_meter_stop_wh, Some(500));
    }

    /// A retried StopTransaction must not cause a second retained publish.
    #[test]
    fn test_repeated_stop_transaction_reports_no_change() {
        let raw = r#"[2,"1","StopTransaction",{"meterStop":500,"transactionId":3}]"#;
        let mut st = ChargePointState::default();
        assert!(st.apply("StopTransaction", &call("StopTransaction"), raw));
        assert!(
            !st.apply("StopTransaction", &call("StopTransaction"), raw),
            "an identical stop must not trigger another retained publish"
        );
    }

    /// MeterValues arrives every few seconds while charging; folding it in
    /// would mean a retained publish per meter reading.
    #[test]
    fn test_meter_values_never_changes_the_snapshot() {
        let mut st = ChargePointState::default();
        let changed = st.apply(
            "MeterValues",
            &call("MeterValues"),
            r#"[2,"5","MeterValues",{"connectorId":1,"meterValue":[{"sampledValue":[{"measurand":"Power.Active.Import","value":"3651"}]}]}]"#,
        );
        assert!(!changed);
        assert_eq!(st, ChargePointState::default());
    }

    #[test]
    fn test_malformed_frame_is_ignored() {
        let mut st = ChargePointState::default();
        assert!(!st.apply(
            "StatusNotification",
            &call("StatusNotification"),
            "not json"
        ));
        assert!(!st.apply(
            "StatusNotification",
            &call("StatusNotification"),
            r#"[2,"1"]"#
        ));
        assert_eq!(st, ChargePointState::default());
    }

    #[test]
    fn test_snapshot_serializes_to_the_documented_shape() {
        let mut st = ChargePointState::default();
        st.apply(
            "StatusNotification",
            &call("StatusNotification"),
            r#"[2,"1","StatusNotification",{"connectorId":1,"errorCode":"NoError","status":"Charging"}]"#,
        );
        let v: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&st).unwrap()).unwrap();
        assert_eq!(v["connector_status"], "Charging");
        assert_eq!(v["error_code"], "NoError");
        assert!(v["transaction_id"].is_null());
        assert!(v["last_updated"].is_string());

        // The Home Assistant package reads these keys by name, so the wire
        // shape is part of the contract rather than an implementation detail.
        for key in [
            "last_meter_stop_wh",
            "last_session_energy_wh",
            "last_transaction_id",
            "last_stop_reason",
            "last_stop_time",
            "current_limit_a",
            "current_limit_status",
            "current_limit_updated",
        ] {
            assert!(
                v.get(key).is_some(),
                "{key} must be present in the retained snapshot"
            );
            assert!(v[key].is_null(), "{key} is null before any session ends");
        }

        st.apply(
            "StopTransaction",
            &call("StopTransaction"),
            r#"[2,"9","StopTransaction",{"meterStop":8084000,"transactionId":1000000001,"reason":"EVDisconnected"}]"#,
        );
        let v: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&st).unwrap()).unwrap();
        assert_eq!(v["last_meter_stop_wh"], 8084000);
        assert_eq!(v["last_transaction_id"], 1000000001);
        assert_eq!(v["last_stop_reason"], "EVDisconnected");
    }

    #[test]
    fn test_availability_topic_construction() {
        assert_eq!(availability_topic("CP001"), "ocpp/CP001/availability");
    }

    #[test]
    fn test_availability_topic_with_special_chars() {
        assert_eq!(
            availability_topic("AUTEL-12345"),
            "ocpp/AUTEL-12345/availability"
        );
    }

    #[test]
    fn test_message_topic_construction() {
        assert_eq!(
            message_topic("CP001", "charger", "BootNotification"),
            "ocpp/CP001/charger/BootNotification"
        );
    }

    #[test]
    fn test_message_topic_central_system_direction() {
        assert_eq!(
            message_topic("CP001", "central_system", "RemoteStartTransaction"),
            "ocpp/CP001/central_system/RemoteStartTransaction"
        );
    }

    #[test]
    fn test_status_topic_construction() {
        assert_eq!(status_topic("CP001"), "ocpp/CP001/status");
    }

    #[test]
    fn test_mqtt_message_buffer_eviction() {
        // Simulate buffer behavior without a real MQTT connection
        let mut buffer: VecDeque<MqttMessage> = VecDeque::new();
        let max_size = 5;

        // Fill buffer to capacity
        for i in 0..max_size {
            buffer.push_back(MqttMessage {
                topic: format!("topic/{}", i),
                payload: format!("payload-{}", i).into_bytes(),
                qos: QoS::AtLeastOnce,
                retain: false,
            });
        }
        assert_eq!(buffer.len(), 5);

        // Add one more — should evict oldest
        if buffer.len() >= max_size {
            let evicted = buffer.pop_front().unwrap();
            assert_eq!(evicted.topic, "topic/0");
        }
        buffer.push_back(MqttMessage {
            topic: "topic/5".to_string(),
            payload: "payload-5".to_string().into_bytes(),
            qos: QoS::AtLeastOnce,
            retain: false,
        });

        assert_eq!(buffer.len(), 5);
        // Oldest should now be topic/1
        assert_eq!(buffer.front().unwrap().topic, "topic/1");
        // Newest should be topic/5
        assert_eq!(buffer.back().unwrap().topic, "topic/5");
    }

    #[test]
    fn test_mqtt_options_construction() {
        // Verify that MqttOptions are constructed correctly from config
        let client_id = format!("ocpp-proxy-{}", "CP001");
        let mut mqttoptions = MqttOptions::new(&client_id, "mqtt.example.com", 8883);
        mqttoptions.set_credentials("testuser", "testpass");
        mqttoptions.set_keep_alive(Duration::from_secs(60));

        let availability_topic = format!("ocpp/{}/availability", "CP001");
        let last_will = LastWill::new(&availability_topic, "offline", QoS::AtLeastOnce, true);
        mqttoptions.set_last_will(last_will);

        // Verify keepalive
        assert_eq!(mqttoptions.keep_alive(), Duration::from_secs(60));

        // Verify last will is set (rumqttc doesn't expose LWT accessors, but construction works)
        // The fact that this doesn't panic verifies the configuration is valid.
    }

    #[test]
    fn test_mqtt_options_client_id_format() {
        let charge_point_id = "AUTEL-EV-12345";
        let expected_client_id = format!("ocpp-proxy-{}", charge_point_id);
        assert_eq!(expected_client_id, "ocpp-proxy-AUTEL-EV-12345");
    }

    #[test]
    fn test_exponential_backoff_for_mqtt() {
        // MQTT uses 1s initial, 30s max as specified in requirement 6.3
        let mut backoff =
            ExponentialBackoff::with_defaults(Duration::from_secs(1), Duration::from_secs(30));

        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
        assert_eq!(backoff.next_delay(), Duration::from_secs(4));
        assert_eq!(backoff.next_delay(), Duration::from_secs(8));
        assert_eq!(backoff.next_delay(), Duration::from_secs(16));
        // Next would be 32 but max is 30
        assert_eq!(backoff.next_delay(), Duration::from_secs(30));
        assert_eq!(backoff.next_delay(), Duration::from_secs(30));
    }

    #[test]
    fn test_exponential_backoff_reset_for_mqtt() {
        let mut backoff =
            ExponentialBackoff::with_defaults(Duration::from_secs(1), Duration::from_secs(30));

        backoff.next_delay();
        backoff.next_delay();
        backoff.next_delay();
        backoff.reset();

        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
    }

    #[test]
    fn test_buffer_message_within_capacity() {
        let mut buffer: VecDeque<MqttMessage> = VecDeque::new();
        let max_size = 500;

        let msg = MqttMessage {
            topic: "ocpp/CP001/charger/Heartbeat".to_string(),
            payload: b"{}".to_vec(),
            qos: QoS::AtLeastOnce,
            retain: false,
        };

        buffer.push_back(msg);
        assert_eq!(buffer.len(), 1);
        assert!(buffer.len() < max_size);
    }

    #[test]
    fn test_tls_config_fails_on_missing_ca_cert() {
        let config = MqttConfig {
            host: "mqtt.example.com".to_string(),
            port: 8883,
            username: "user".to_string(),
            password: "pass".to_string(),
            ca_cert_path: Some("/nonexistent/ca.pem".to_string()),
            client_cert_path: Some("/nonexistent/cert.pem".to_string()),
            client_key_path: Some("/nonexistent/key.pem".to_string()),
        };

        let result = MqttPublisher::build_tls_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.category(), "tls");
        assert!(err.description().contains("ca.pem"));
    }

    #[test]
    fn test_tls_config_fails_on_missing_client_cert() {
        // Create a temp CA file but leave client cert missing
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"fake-ca-cert").unwrap();

        let config = MqttConfig {
            host: "mqtt.example.com".to_string(),
            port: 8883,
            username: "user".to_string(),
            password: "pass".to_string(),
            ca_cert_path: Some(tmp.path().to_str().unwrap().to_string()),
            client_cert_path: Some("/nonexistent/cert.pem".to_string()),
            client_key_path: Some("/nonexistent/key.pem".to_string()),
        };

        let result = MqttPublisher::build_tls_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.category(), "tls");
        assert!(err.description().contains("client certificate"));
    }

    #[test]
    fn test_tls_config_fails_on_missing_client_key() {
        let ca_tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(ca_tmp.path(), b"fake-ca-cert").unwrap();
        let cert_tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(cert_tmp.path(), b"fake-client-cert").unwrap();

        let config = MqttConfig {
            host: "mqtt.example.com".to_string(),
            port: 8883,
            username: "user".to_string(),
            password: "pass".to_string(),
            ca_cert_path: Some(ca_tmp.path().to_str().unwrap().to_string()),
            client_cert_path: Some(cert_tmp.path().to_str().unwrap().to_string()),
            client_key_path: Some("/nonexistent/key.pem".to_string()),
        };

        let result = MqttPublisher::build_tls_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.category(), "tls");
        assert!(err.description().contains("client key"));
    }

    #[test]
    fn test_tls_config_succeeds_with_valid_files() {
        let ca_tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(ca_tmp.path(), b"fake-ca-cert").unwrap();
        let cert_tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(cert_tmp.path(), b"fake-client-cert").unwrap();
        let key_tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(key_tmp.path(), b"fake-client-key").unwrap();

        let config = MqttConfig {
            host: "mqtt.example.com".to_string(),
            port: 8883,
            username: "user".to_string(),
            password: "pass".to_string(),
            ca_cert_path: Some(ca_tmp.path().to_str().unwrap().to_string()),
            client_cert_path: Some(cert_tmp.path().to_str().unwrap().to_string()),
            client_key_path: Some(key_tmp.path().to_str().unwrap().to_string()),
        };

        let result = MqttPublisher::build_tls_config(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mqtt_publisher_new_with_valid_config() {
        let ca_tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(ca_tmp.path(), b"fake-ca-cert").unwrap();
        let cert_tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(cert_tmp.path(), b"fake-client-cert").unwrap();
        let key_tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(key_tmp.path(), b"fake-client-key").unwrap();

        let config = MqttConfig {
            host: "mqtt.example.com".to_string(),
            port: 8883,
            username: "user".to_string(),
            password: "pass".to_string(),
            ca_cert_path: Some(ca_tmp.path().to_str().unwrap().to_string()),
            client_cert_path: Some(cert_tmp.path().to_str().unwrap().to_string()),
            client_key_path: Some(key_tmp.path().to_str().unwrap().to_string()),
        };

        let (_tx, rx) = mpsc::channel(100);
        let publisher = MqttPublisher::new(
            &config,
            Some("CP001".to_string()),
            rx,
            500,
            SnapshotStore::disabled(),
        );
        assert!(publisher.is_ok());

        let publisher = publisher.unwrap();
        assert_eq!(publisher.lwt_charge_point_id().unwrap(), "CP001");
        assert_eq!(publisher.state(), ConnectionState::Disconnected);
        assert_eq!(publisher.buffer_len(), 0);
    }

    /// A publisher built against a store that already has a session in it
    /// starts with that session, not empty.
    ///
    /// This is the restart path: `new` is what a restarted process calls, and
    /// it must load before the event loop can fold anything in.
    #[tokio::test]
    async fn test_publisher_restores_the_stored_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        // What the previous process would have written.
        let store = SnapshotStore::new(Some(path.clone()));
        let mut before = crate::snapshot_store::SnapshotMap::new();
        let mut st = ChargePointState::default();
        st.apply(
            "StartTransaction",
            &call("StartTransaction"),
            r#"[2,"1","StartTransaction",{"connectorId":1,"idTag":"a1b2c3d4","meterStart":8076445}]"#,
        );
        st.apply(
            "StopTransaction",
            &call("StopTransaction"),
            r#"[2,"2","StopTransaction",{"meterStop":8084000,"transactionId":1000000001}]"#,
        );
        before.insert("CP-EXAMPLE-0001".to_string(), st);
        store.save(&before);

        // What the restarted process builds.
        let config = plaintext_config();
        let (_tx, rx) = mpsc::channel(100);
        let publisher = MqttPublisher::new(
            &config,
            Some("CP-EXAMPLE-0001".to_string()),
            rx,
            500,
            SnapshotStore::new(Some(path)),
        )
        .unwrap();

        let restored = publisher
            .charge_point_state
            .get("CP-EXAMPLE-0001")
            .expect("the stored snapshot must be loaded by `new`");
        assert_eq!(restored.last_meter_stop_wh, Some(8084000));
        assert_eq!(restored.last_session_energy_wh, Some(7555));
        assert_eq!(restored.last_transaction_id, Some(1000000001));
    }

    /// Every snapshot change is persisted, including while the broker is
    /// unreachable — the restart it has to survive is often the same event that
    /// took the broker away.
    #[tokio::test]
    async fn test_snapshot_change_is_persisted_even_when_disconnected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        let config = plaintext_config();
        let (_tx, rx) = mpsc::channel(100);
        let mut publisher = MqttPublisher::new(
            &config,
            Some("CP001".to_string()),
            rx,
            500,
            SnapshotStore::new(Some(path.clone())),
        )
        .unwrap();
        assert_eq!(publisher.state(), ConnectionState::Disconnected);

        publisher
            .handle_event(MqttEvent::MessageForwarded {
                charge_point_id: "CP001".to_string(),
                frame: crate::models::OcppFrame::parse(
                    r#"[2,"1","StopTransaction",{"meterStop":4200,"transactionId":9}]"#,
                )
                .unwrap(),
                direction: Direction::ChargerToCentral,
                action: "StopTransaction".to_string(),
            })
            .await;

        let reloaded = SnapshotStore::new(Some(path)).load();
        assert_eq!(reloaded["CP001"].last_meter_stop_wh, Some(4200));
        assert_eq!(reloaded["CP001"].last_transaction_id, Some(9));
    }

    /// MeterValues does not change the snapshot, so it must not cause a write
    /// either — it arrives every few seconds during a charge.
    #[tokio::test]
    async fn test_meter_values_does_not_write_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");

        let config = plaintext_config();
        let (_tx, rx) = mpsc::channel(100);
        let mut publisher = MqttPublisher::new(
            &config,
            Some("CP001".to_string()),
            rx,
            500,
            SnapshotStore::new(Some(path.clone())),
        )
        .unwrap();

        publisher
            .handle_event(MqttEvent::MessageForwarded {
                charge_point_id: "CP001".to_string(),
                frame: crate::models::OcppFrame::parse(
                    r#"[2,"1","MeterValues",{"connectorId":1,"meterValue":[{"sampledValue":[{"value":"3651"}]}]}]"#,
                )
                .unwrap(),
                direction: Direction::ChargerToCentral,
                action: "MeterValues".to_string(),
            })
            .await;

        assert!(!path.exists(), "MeterValues must not touch the state file");
    }

    #[test]
    fn test_mqtt_publisher_new_fails_with_invalid_certs() {
        let config = MqttConfig {
            host: "mqtt.example.com".to_string(),
            port: 8883,
            username: "user".to_string(),
            password: "pass".to_string(),
            ca_cert_path: Some("/nonexistent/ca.pem".to_string()),
            client_cert_path: Some("/nonexistent/cert.pem".to_string()),
            client_key_path: Some("/nonexistent/key.pem".to_string()),
        };

        let (_tx, rx) = mpsc::channel(100);
        let result = MqttPublisher::new(
            &config,
            Some("CP001".to_string()),
            rx,
            500,
            SnapshotStore::disabled(),
        );
        assert!(result.is_err());
    }

    // --- Tests for task 9.2: MQTT event publishing ---

    #[test]
    fn test_direction_str_charger_to_central() {
        assert_eq!(direction_str(Direction::ChargerToCentral), "charger");
    }

    #[test]
    fn test_direction_str_central_to_charger() {
        assert_eq!(direction_str(Direction::CentralToCharger), "central_system");
    }

    #[test]
    fn test_message_type_str_call() {
        let msg_type = OcppMessageType::Call {
            action: "BootNotification".to_string(),
        };
        assert_eq!(message_type_str(&msg_type), "Call");
    }

    #[test]
    fn test_message_type_str_call_result() {
        assert_eq!(message_type_str(&OcppMessageType::CallResult), "CallResult");
    }

    #[test]
    fn test_message_type_str_call_error() {
        assert_eq!(message_type_str(&OcppMessageType::CallError), "CallError");
    }

    #[test]
    fn test_connection_state_str_all_states() {
        assert_eq!(
            connection_state_str(ConnectionState::Connected),
            "connected"
        );
        assert_eq!(
            connection_state_str(ConnectionState::Disconnected),
            "disconnected"
        );
        assert_eq!(
            connection_state_str(ConnectionState::Reconnecting),
            "reconnecting"
        );
        assert_eq!(
            connection_state_str(ConnectionState::Connecting),
            "connecting"
        );
    }

    #[test]
    fn test_mqtt_payload_serialization_call() {
        let payload = MqttPayload {
            timestamp: "2024-01-15T10:30:00+00:00".to_string(),
            message_type: "Call".to_string(),
            payload: serde_json::json!([2, "abc123", "BootNotification", {"chargePointModel": "Autel"}]),
        };

        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["timestamp"], "2024-01-15T10:30:00+00:00");
        assert_eq!(json["message_type"], "Call");
        assert!(json["payload"].is_array());
        assert_eq!(json["payload"][0], 2);
        assert_eq!(json["payload"][1], "abc123");
        assert_eq!(json["payload"][2], "BootNotification");
    }

    #[test]
    fn test_mqtt_payload_serialization_call_result() {
        let payload = MqttPayload {
            timestamp: "2024-01-15T10:30:01+00:00".to_string(),
            message_type: "CallResult".to_string(),
            payload: serde_json::json!([3, "abc123", {"status": "Accepted"}]),
        };

        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["message_type"], "CallResult");
        assert_eq!(json["payload"][0], 3);
        assert_eq!(json["payload"][2]["status"], "Accepted");
    }

    #[test]
    fn test_mqtt_payload_serialization_call_error() {
        let payload = MqttPayload {
            timestamp: "2024-01-15T10:30:02+00:00".to_string(),
            message_type: "CallError".to_string(),
            payload: serde_json::json!([4, "abc123", "InternalError", "Something went wrong", {}]),
        };

        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["message_type"], "CallError");
        assert_eq!(json["payload"][0], 4);
        assert_eq!(json["payload"][2], "InternalError");
    }

    #[test]
    fn test_status_payload_serialization() {
        let payload = StatusPayload {
            upstream: "connected".to_string(),
            downstream: "disconnected".to_string(),
        };

        let json = serde_json::to_value(&payload).unwrap();
        assert_eq!(json["upstream"], "connected");
        assert_eq!(json["downstream"], "disconnected");
    }

    #[test]
    fn test_status_payload_all_state_combinations() {
        let states = [
            ConnectionState::Connected,
            ConnectionState::Disconnected,
            ConnectionState::Reconnecting,
            ConnectionState::Connecting,
        ];

        for upstream in &states {
            for downstream in &states {
                let payload = StatusPayload {
                    upstream: connection_state_str(*upstream).to_string(),
                    downstream: connection_state_str(*downstream).to_string(),
                };
                let json = serde_json::to_string(&payload).unwrap();
                let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert!(parsed["upstream"].is_string());
                assert!(parsed["downstream"].is_string());
            }
        }
    }

    #[test]
    fn test_message_topic_with_direction_helper() {
        let topic = message_topic(
            "CP001",
            direction_str(Direction::ChargerToCentral),
            "Heartbeat",
        );
        assert_eq!(topic, "ocpp/CP001/charger/Heartbeat");

        let topic = message_topic(
            "CP001",
            direction_str(Direction::CentralToCharger),
            "RemoteStartTransaction",
        );
        assert_eq!(topic, "ocpp/CP001/central_system/RemoteStartTransaction");
    }

    #[test]
    fn test_mqtt_payload_timestamp_is_iso8601() {
        use chrono::Utc;
        let now = Utc::now();
        let timestamp = now.to_rfc3339();

        let payload = MqttPayload {
            timestamp: timestamp.clone(),
            message_type: "Call".to_string(),
            payload: serde_json::json!([2, "id-1", "Heartbeat", {}]),
        };

        let json = serde_json::to_value(&payload).unwrap();
        // Verify the timestamp is a valid RFC3339/ISO8601 string
        let ts_str = json["timestamp"].as_str().unwrap();
        let parsed = chrono::DateTime::parse_from_rfc3339(ts_str);
        assert!(
            parsed.is_ok(),
            "Timestamp should be valid ISO 8601: {}",
            ts_str
        );
    }

    #[test]
    fn test_mqtt_payload_contains_full_ocpp_message() {
        // Verify that the payload field contains the full original OCPP message
        let raw_ocpp = r#"[2, "unique-42", "MeterValues", {"connectorId": 1, "meterValue": []}]"#;
        let raw_value: serde_json::Value = serde_json::from_str(raw_ocpp).unwrap();

        let payload = MqttPayload {
            timestamp: "2024-01-15T10:30:00+00:00".to_string(),
            message_type: "Call".to_string(),
            payload: raw_value.clone(),
        };

        let serialized = serde_json::to_value(&payload).unwrap();
        assert_eq!(serialized["payload"], raw_value);
        // Check it's still a JSON array
        assert!(serialized["payload"].is_array());
        assert_eq!(serialized["payload"].as_array().unwrap().len(), 4);
    }

    #[test]
    fn test_mqtt_payload_all_required_fields_present() {
        let payload = MqttPayload {
            timestamp: "2024-01-15T10:30:00+00:00".to_string(),
            message_type: "Call".to_string(),
            payload: serde_json::json!([2, "id", "Action", {}]),
        };

        let json = serde_json::to_string(&payload).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // All three required fields must exist
        assert!(parsed.get("timestamp").is_some());
        assert!(parsed.get("message_type").is_some());
        assert!(parsed.get("payload").is_some());

        // No extra fields
        let obj = parsed.as_object().unwrap();
        assert_eq!(obj.len(), 3);
    }

    // ---- non-blocking publishing (regression for the 2026-09-19 deadlock) ----
    //
    // None of these tests poll the event loop, on purpose: a request channel
    // that nobody drains is exactly the condition under which the old code
    // parked forever, so "returns at all" is the property under test.

    fn publisher_with_events(lwt: Option<&str>) -> (MqttPublisher, mpsc::Sender<MqttEvent>) {
        let (tx, rx) = mpsc::channel(64);
        let publisher = MqttPublisher::new(
            &plaintext_config(),
            lwt.map(str::to_string),
            rx,
            500,
            SnapshotStore::disabled(),
        )
        .expect("publisher");
        (publisher, tx)
    }

    fn event_message(i: usize) -> MqttMessage {
        MqttMessage {
            topic: format!("ocpp/CP/charger/MeterValues{i}"),
            payload: vec![i as u8],
            qos: QoS::AtLeastOnce,
            retain: false,
        }
    }

    #[tokio::test]
    async fn enqueue_never_blocks_on_a_full_request_channel() {
        let (mut publisher, _tx) = publisher_with_events(None);
        publisher.state = ConnectionState::Connected;

        for i in 0..(REQUEST_CHANNEL_CAPACITY + 5) {
            publisher.enqueue(event_message(i));
        }

        // The channel took what it could; the rest waits, in order.
        assert_eq!(publisher.buffer_len(), 5);
        assert_eq!(
            publisher.buffer.front().map(|m| m.topic.as_str()),
            Some(event_message(REQUEST_CHANNEL_CAPACITY).topic.as_str())
        );
    }

    #[tokio::test]
    async fn reconnect_with_a_backlog_larger_than_the_channel_returns() {
        let (mut publisher, _tx) = publisher_with_events(Some("CP"));
        publisher.state = ConnectionState::Reconnecting;

        // The incident: the broker bounced with 14 messages buffered.
        let backlog = REQUEST_CHANNEL_CAPACITY + 4;
        for i in 0..backlog {
            publisher.enqueue(event_message(i));
        }
        assert_eq!(
            publisher.buffer_len(),
            backlog,
            "not connected: everything is buffered"
        );

        // 1 online + 14 backlog against a channel of 10. The old handler
        // awaited the tenth publish here and never came back.
        time::timeout(Duration::from_secs(2), publisher.on_connack())
            .await
            .expect("on_connack must return without the event loop being polled");

        assert_eq!(publisher.state, ConnectionState::Connected);
        assert_eq!(
            publisher.buffer_len(),
            backlog + 1 - REQUEST_CHANNEL_CAPACITY
        );
        // online went first, then the oldest nine; the tenth is next in line.
        assert_eq!(
            publisher.buffer.front().map(|m| m.topic.as_str()),
            Some(event_message(REQUEST_CHANNEL_CAPACITY - 1).topic.as_str())
        );
        assert!(
            publisher.flush_in_progress.is_some(),
            "the flush stays open until the buffer has actually emptied"
        );
    }

    #[tokio::test]
    async fn online_goes_ahead_of_the_backlog() {
        let (mut publisher, _tx) = publisher_with_events(Some("CP"));

        // Fill the channel so that on_connack can hand nothing over and the
        // queue it leaves behind shows the order it chose.
        publisher.state = ConnectionState::Connected;
        for i in 0..REQUEST_CHANNEL_CAPACITY {
            publisher.enqueue(event_message(i));
        }
        assert_eq!(publisher.buffer_len(), 0);

        publisher.state = ConnectionState::Reconnecting;
        publisher.enqueue(event_message(99));
        time::timeout(Duration::from_secs(2), publisher.on_connack())
            .await
            .expect("on_connack must return");

        let queued: Vec<&str> = publisher.buffer.iter().map(|m| m.topic.as_str()).collect();
        assert_eq!(
            queued,
            vec!["ocpp/CP/availability", "ocpp/CP/charger/MeterValues99"]
        );
        let online = publisher.buffer.front().expect("online is queued");
        assert_eq!(online.payload, b"online");
        assert!(online.retain);
    }

    #[tokio::test]
    async fn connection_state_is_mirrored_into_the_shared_manager() {
        let manager = Arc::new(Mutex::new(ConnectionStateManager::new(8)));
        let (publisher, _tx) = publisher_with_events(Some("CP"));
        let mut publisher = publisher.with_state_manager(manager.clone());

        publisher.on_connack().await;
        assert_eq!(
            manager.lock().await.mqtt_state(),
            ConnectionState::Connected
        );

        publisher.set_state(ConnectionState::Reconnecting).await;
        assert_eq!(
            manager.lock().await.mqtt_state(),
            ConnectionState::Reconnecting
        );
    }

    // --- command results in the snapshot ---

    fn limit_result(action: &str, status: CommandStatus, applied: Option<f64>) -> CommandResult {
        let request = CommandRequest::new(
            "r",
            match action {
                "SetChargingProfile" => ProxyCommand::SetCurrentLimit {
                    limit_a: applied.unwrap_or(0.0),
                },
                _ => ProxyCommand::ClearCurrentLimit,
            },
            CommandSource::Mqtt,
        );
        let mut result = CommandResult::for_request(&request, status, None);
        result.action = Some(action.to_string());
        result.applied_limit_a = applied;
        result
    }

    #[test]
    fn test_accepted_limit_lands_in_the_snapshot_and_clear_removes_it() {
        let mut st = ChargePointState::default();
        assert!(st.apply_command_result(&limit_result(
            "SetChargingProfile",
            CommandStatus::Accepted,
            Some(16.0)
        )));
        assert_eq!(st.current_limit_a, Some(16.0));
        assert_eq!(st.current_limit_status.as_deref(), Some("accepted"));
        assert!(st.current_limit_updated.is_some());
        assert!(st.last_updated.is_some());

        assert!(st.apply_command_result(&limit_result(
            "ClearChargingProfile",
            CommandStatus::Accepted,
            None
        )));
        assert_eq!(st.current_limit_a, None);
        assert_eq!(st.current_limit_status.as_deref(), Some("accepted"));
    }

    #[test]
    fn test_rejected_limit_records_the_status_but_keeps_the_standing_value() {
        let mut st = ChargePointState::default();
        st.apply_command_result(&limit_result(
            "SetChargingProfile",
            CommandStatus::Accepted,
            Some(16.0),
        ));
        assert!(st.apply_command_result(&limit_result(
            "SetChargingProfile",
            CommandStatus::Rejected,
            Some(10.0)
        )));
        assert_eq!(
            st.current_limit_a,
            Some(16.0),
            "the charger still applies the last limit it accepted"
        );
        assert_eq!(st.current_limit_status.as_deref(), Some("rejected"));
    }

    #[test]
    fn test_repeated_identical_limit_result_reports_no_change() {
        let mut st = ChargePointState::default();
        let r = limit_result("SetChargingProfile", CommandStatus::Accepted, Some(16.0));
        assert!(st.apply_command_result(&r));
        assert!(!st.apply_command_result(&r));
    }

    #[test]
    fn test_non_limit_results_leave_the_snapshot_alone() {
        let mut st = ChargePointState::default();
        let request = CommandRequest::new(
            "r",
            ProxyCommand::GetConfiguration { keys: vec![] },
            CommandSource::Mqtt,
        );
        let result = CommandResult::for_request(&request, CommandStatus::Accepted, None);
        assert!(!st.apply_command_result(&result));
        assert_eq!(st, ChargePointState::default());
    }

    #[test]
    fn test_snapshot_without_limit_keys_still_loads() {
        // A state.json written before this change has none of the new keys.
        let legacy = r#"{"connector_status":"Available","error_code":"NoError",
            "transaction_id":null,"id_tag":null,"meter_start_wh":null,
            "last_meter_stop_wh":8084000,"last_session_energy_wh":7555,
            "last_transaction_id":1,"last_stop_reason":"Local","last_stop_time":null,
            "last_updated":"2026-09-01T07:22:16+00:00"}"#;
        let st: ChargePointState = serde_json::from_str(legacy).unwrap();
        assert_eq!(st.current_limit_a, None);
        assert_eq!(st.current_limit_status, None);
        assert_eq!(st.last_meter_stop_wh, Some(8084000));
    }

    #[test]
    fn test_publisher_only_subscribes_when_given_a_router() {
        let (_, event_tx) = publisher_with_events(Some("CP-1"));
        drop(event_tx);
        let (rx_tx, rx) = mpsc::channel(1);
        drop(rx_tx);
        let plain = MqttPublisher::new(
            &plaintext_config(),
            Some("CP-1".to_string()),
            rx,
            10,
            SnapshotStore::disabled(),
        )
        .unwrap();
        assert!(!plain.commands_enabled());
        let with = plain.with_command_router(
            crate::command::CommandRouter::new(),
            crate::config::ChargingConfig::default(),
        );
        assert!(with.commands_enabled());
    }
}
