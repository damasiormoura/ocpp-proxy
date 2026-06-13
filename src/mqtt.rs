//! MQTT publisher for Home Assistant integration.
//!
//! Asynchronously publishes OCPP events to the MQTT broker, decoupled from the forwarding path.
//! Uses rumqttc for MQTT 3.1.1 connectivity with TLS, Last Will and Testament, and automatic
//! reconnection with exponential backoff.

use std::collections::VecDeque;
use std::fs;
use std::time::Duration;

use rumqttc::{AsyncClient, EventLoop, Event, Incoming, MqttOptions, QoS, LastWill, Transport};
use rumqttc::TlsConfiguration;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, info, warn};

use crate::config::MqttConfig;
use crate::error::ProxyError;
use crate::forwarder::MqttEvent;
use crate::models::{ConnectionState, Direction, ExponentialBackoff, OcppMessageType};

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
    /// Charge Point ID used for topic construction.
    charge_point_id: String,
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
        charge_point_id: String,
        event_rx: mpsc::Receiver<MqttEvent>,
        max_buffer_size: usize,
    ) -> Result<Self, ProxyError> {
        let client_id = format!("ocpp-proxy-{}", charge_point_id);
        let mut mqttoptions = MqttOptions::new(
            &client_id,
            &mqtt_config.host,
            mqtt_config.port,
        );

        // Configure credentials
        mqttoptions.set_credentials(&mqtt_config.username, &mqtt_config.password);

        // Configure keepalive (60 seconds)
        mqttoptions.set_keep_alive(Duration::from_secs(60));

        // Configure Last Will and Testament
        let availability_topic = format!("ocpp/{}/availability", charge_point_id);
        let last_will = LastWill::new(
            &availability_topic,
            "offline",
            QoS::AtLeastOnce,
            true,
        );
        mqttoptions.set_last_will(last_will);

        // Configure TLS with certificate verification
        let tls_config = Self::build_tls_config(mqtt_config)?;
        mqttoptions.set_transport(Transport::Tls(tls_config));

        // Create client and event loop
        let (client, eventloop) = AsyncClient::new(mqttoptions, 10);

        Ok(Self {
            client,
            eventloop,
            charge_point_id,
            event_rx,
            buffer: VecDeque::new(),
            max_buffer_size,
            state: ConnectionState::Disconnected,
            backoff: ExponentialBackoff::with_defaults(
                Duration::from_secs(1),
                Duration::from_secs(30),
            ),
        })
    }

    /// Build TLS configuration from certificate files.
    ///
    /// Reads CA cert, client cert, and client key from the configured file paths.
    /// Uses rustls (via rumqttc's `use-rustls` feature) for TLS 1.2+.
    fn build_tls_config(mqtt_config: &MqttConfig) -> Result<TlsConfiguration, ProxyError> {
        let ca_cert = fs::read(&mqtt_config.ca_cert_path).map_err(|e| ProxyError::Tls {
            description: format!(
                "Failed to read CA certificate at '{}': {}",
                mqtt_config.ca_cert_path, e
            ),
        })?;

        let client_cert = fs::read(&mqtt_config.client_cert_path).map_err(|e| ProxyError::Tls {
            description: format!(
                "Failed to read client certificate at '{}': {}",
                mqtt_config.client_cert_path, e
            ),
        })?;

        let client_key = fs::read(&mqtt_config.client_key_path).map_err(|e| ProxyError::Tls {
            description: format!(
                "Failed to read client key at '{}': {}",
                mqtt_config.client_key_path, e
            ),
        })?;

        Ok(TlsConfiguration::Simple {
            ca: ca_cert,
            alpn: None,
            client_auth: Some((client_cert, client_key)),
        })
    }

    /// Attempt to connect to the MQTT broker with a startup timeout.
    ///
    /// Tries to establish a connection for up to `timeout` duration.
    /// If connection is not established within the timeout, returns `Ok(false)`.
    /// The caller should proceed regardless and the event loop will continue
    /// reconnection attempts.
    pub async fn try_connect(&mut self, timeout: Duration) -> Result<bool, ProxyError> {
        self.state = ConnectionState::Connecting;
        info!(
            component = "mqtt",
            charge_point_id = %self.charge_point_id,
            "Attempting MQTT connection (timeout: {:?})",
            timeout
        );

        let deadline = time::Instant::now() + timeout;

        loop {
            let remaining = deadline.saturating_duration_since(time::Instant::now());
            if remaining.is_zero() {
                warn!(
                    component = "mqtt",
                    charge_point_id = %self.charge_point_id,
                    "MQTT connection timeout after {:?}, proceeding without MQTT",
                    timeout
                );
                self.state = ConnectionState::Reconnecting;
                return Ok(false);
            }

            match time::timeout(remaining, self.eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Incoming::ConnAck(_)))) => {
                    info!(
                        component = "mqtt",
                        charge_point_id = %self.charge_point_id,
                        "MQTT connected successfully"
                    );
                    self.state = ConnectionState::Connected;
                    self.backoff.reset();

                    // Publish online availability message
                    if let Err(e) = self.publish_online().await {
                        warn!(
                            component = "mqtt",
                            charge_point_id = %self.charge_point_id,
                            "Failed to publish online status: {}",
                            e
                        );
                    }

                    return Ok(true);
                }
                Ok(Ok(_)) => {
                    // Other events during connection (e.g., Outgoing::Connect) — continue polling.
                    continue;
                }
                Ok(Err(e)) => {
                    debug!(
                        component = "mqtt",
                        charge_point_id = %self.charge_point_id,
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
                        charge_point_id = %self.charge_point_id,
                        "MQTT connection timeout after {:?}, proceeding without MQTT",
                        timeout
                    );
                    self.state = ConnectionState::Reconnecting;
                    return Ok(false);
                }
            }
        }
    }

    /// Publish a retained "online" message to the availability topic.
    ///
    /// Called after successful connection to the broker.
    pub async fn publish_online(&self) -> Result<(), ProxyError> {
        let topic = format!("ocpp/{}/availability", self.charge_point_id);
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, "online")
            .await
            .map_err(|e| ProxyError::ConnectionMqtt {
                description: format!("Failed to publish online status: {}", e),
            })?;

        debug!(
            component = "mqtt",
            charge_point_id = %self.charge_point_id,
            topic = %topic,
            "Published online availability status (retained)"
        );

        Ok(())
    }

    /// Run the MQTT publisher event loop.
    ///
    /// Processes events from both the MQTT event loop and the forwarder channel.
    /// Handles reconnection automatically via rumqttc's built-in reconnection,
    /// combined with exponential backoff state tracking.
    ///
    /// This method runs indefinitely until the event channel is closed.
    pub async fn run(&mut self) {
        info!(
            component = "mqtt",
            charge_point_id = %self.charge_point_id,
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
                                charge_point_id = %self.charge_point_id,
                                "MQTT reconnected"
                            );
                            self.state = ConnectionState::Connected;
                            self.backoff.reset();

                            // Publish online status
                            if let Err(e) = self.publish_online().await {
                                warn!(
                                    component = "mqtt",
                                    charge_point_id = %self.charge_point_id,
                                    "Failed to publish online status on reconnect: {}",
                                    e
                                );
                            }

                            // Flush buffered messages
                            self.flush_buffer().await;
                        }
                        Ok(_) => {
                            // Other events (PubAck, PingResp, etc.) — handled by rumqttc internally
                        }
                        Err(e) => {
                            if self.state == ConnectionState::Connected {
                                warn!(
                                    component = "mqtt",
                                    charge_point_id = %self.charge_point_id,
                                    "MQTT connection lost: {}",
                                    e
                                );
                                self.state = ConnectionState::Reconnecting;
                            }

                            // rumqttc handles reconnection internally; we track state
                            let delay = self.backoff.next_delay();
                            debug!(
                                component = "mqtt",
                                charge_point_id = %self.charge_point_id,
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
                                charge_point_id = %self.charge_point_id,
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
                frame,
                direction,
                action,
            } => {
                // Construct MQTT topic: ocpp/{charge_point_id}/{direction}/{action}
                let dir_str = direction_str(direction);
                let topic = message_topic(&self.charge_point_id, dir_str, &action);

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
                            charge_point_id = %self.charge_point_id,
                            error = %e,
                            "Failed to serialize MQTT payload"
                        );
                        return;
                    }
                };

                // Publish with QoS 1, retain=false
                if self.state == ConnectionState::Connected {
                    if let Err(e) = self
                        .client
                        .publish(&topic, QoS::AtLeastOnce, false, payload_bytes.clone())
                        .await
                    {
                        warn!(
                            component = "mqtt",
                            charge_point_id = %self.charge_point_id,
                            topic = %topic,
                            error = %e,
                            "Failed to publish MQTT message, buffering"
                        );
                        self.buffer_message(MqttMessage {
                            topic,
                            payload: payload_bytes,
                            qos: QoS::AtLeastOnce,
                            retain: false,
                        });
                    } else {
                        debug!(
                            component = "mqtt",
                            charge_point_id = %self.charge_point_id,
                            topic = %topic,
                            "Published OCPP event to MQTT"
                        );
                    }
                } else {
                    // Broker unreachable — buffer the message
                    debug!(
                        component = "mqtt",
                        charge_point_id = %self.charge_point_id,
                        topic = %topic,
                        "Broker unreachable, buffering MQTT message"
                    );
                    self.buffer_message(MqttMessage {
                        topic,
                        payload: payload_bytes,
                        qos: QoS::AtLeastOnce,
                        retain: false,
                    });
                }
            }
            MqttEvent::StateChange {
                upstream,
                downstream,
            } => {
                // Construct status topic: ocpp/{charge_point_id}/status
                let topic = status_topic(&self.charge_point_id);

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
                            charge_point_id = %self.charge_point_id,
                            error = %e,
                            "Failed to serialize status payload"
                        );
                        return;
                    }
                };

                // Publish with QoS 1, retain=true (retained status message)
                if self.state == ConnectionState::Connected {
                    if let Err(e) = self
                        .client
                        .publish(&topic, QoS::AtLeastOnce, true, payload_bytes.clone())
                        .await
                    {
                        warn!(
                            component = "mqtt",
                            charge_point_id = %self.charge_point_id,
                            topic = %topic,
                            error = %e,
                            "Failed to publish status message, buffering"
                        );
                        self.buffer_message(MqttMessage {
                            topic,
                            payload: payload_bytes,
                            qos: QoS::AtLeastOnce,
                            retain: true,
                        });
                    } else {
                        debug!(
                            component = "mqtt",
                            charge_point_id = %self.charge_point_id,
                            topic = %topic,
                            "Published connection status to MQTT (retained)"
                        );
                    }
                } else {
                    // Broker unreachable — buffer the message
                    debug!(
                        component = "mqtt",
                        charge_point_id = %self.charge_point_id,
                        topic = %topic,
                        "Broker unreachable, buffering status message"
                    );
                    self.buffer_message(MqttMessage {
                        topic,
                        payload: payload_bytes,
                        qos: QoS::AtLeastOnce,
                        retain: true,
                    });
                }
            }
        }
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
                    charge_point_id = %self.charge_point_id,
                    topic = %msg.topic,
                    "MQTT buffer full, evicting oldest message"
                );
            }
        }
        self.buffer.push_back(message);
    }

    /// Flush buffered messages after reconnection.
    ///
    /// Publishes all buffered messages in FIFO order.
    async fn flush_buffer(&mut self) {
        let count = self.buffer.len();
        if count == 0 {
            return;
        }

        info!(
            component = "mqtt",
            charge_point_id = %self.charge_point_id,
            count = count,
            "Flushing buffered MQTT messages"
        );

        let mut published = 0;
        while let Some(msg) = self.buffer.pop_front() {
            if let Err(e) = self.client.publish(&msg.topic, msg.qos, msg.retain, msg.payload.clone()).await {
                warn!(
                    component = "mqtt",
                    charge_point_id = %self.charge_point_id,
                    topic = %msg.topic,
                    error = %e,
                    "Failed to publish buffered message, re-buffering"
                );
                // Put it back at the front and stop flushing
                self.buffer.push_front(msg);
                break;
            }
            published += 1;
        }

        if published > 0 {
            info!(
                component = "mqtt",
                charge_point_id = %self.charge_point_id,
                published = published,
                remaining = self.buffer.len(),
                "Flushed buffered MQTT messages"
            );
        }
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

    /// Get the charge point ID.
    pub fn charge_point_id(&self) -> &str {
        &self.charge_point_id
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

/// Construct the status topic for a given charge point ID.
pub fn status_topic(charge_point_id: &str) -> String {
    format!("ocpp/{}/status", charge_point_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_availability_topic_construction() {
        assert_eq!(
            availability_topic("CP001"),
            "ocpp/CP001/availability"
        );
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
        assert_eq!(
            status_topic("CP001"),
            "ocpp/CP001/status"
        );
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
        let last_will = LastWill::new(
            &availability_topic,
            "offline",
            QoS::AtLeastOnce,
            true,
        );
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
        let mut backoff = ExponentialBackoff::with_defaults(
            Duration::from_secs(1),
            Duration::from_secs(30),
        );

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
        let mut backoff = ExponentialBackoff::with_defaults(
            Duration::from_secs(1),
            Duration::from_secs(30),
        );

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
            ca_cert_path: "/nonexistent/ca.pem".to_string(),
            client_cert_path: "/nonexistent/cert.pem".to_string(),
            client_key_path: "/nonexistent/key.pem".to_string(),
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
            ca_cert_path: tmp.path().to_str().unwrap().to_string(),
            client_cert_path: "/nonexistent/cert.pem".to_string(),
            client_key_path: "/nonexistent/key.pem".to_string(),
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
            ca_cert_path: ca_tmp.path().to_str().unwrap().to_string(),
            client_cert_path: cert_tmp.path().to_str().unwrap().to_string(),
            client_key_path: "/nonexistent/key.pem".to_string(),
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
            ca_cert_path: ca_tmp.path().to_str().unwrap().to_string(),
            client_cert_path: cert_tmp.path().to_str().unwrap().to_string(),
            client_key_path: key_tmp.path().to_str().unwrap().to_string(),
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
            ca_cert_path: ca_tmp.path().to_str().unwrap().to_string(),
            client_cert_path: cert_tmp.path().to_str().unwrap().to_string(),
            client_key_path: key_tmp.path().to_str().unwrap().to_string(),
        };

        let (_tx, rx) = mpsc::channel(100);
        let publisher = MqttPublisher::new(&config, "CP001".to_string(), rx, 500);
        assert!(publisher.is_ok());

        let publisher = publisher.unwrap();
        assert_eq!(publisher.charge_point_id(), "CP001");
        assert_eq!(publisher.state(), ConnectionState::Disconnected);
        assert_eq!(publisher.buffer_len(), 0);
    }

    #[test]
    fn test_mqtt_publisher_new_fails_with_invalid_certs() {
        let config = MqttConfig {
            host: "mqtt.example.com".to_string(),
            port: 8883,
            username: "user".to_string(),
            password: "pass".to_string(),
            ca_cert_path: "/nonexistent/ca.pem".to_string(),
            client_cert_path: "/nonexistent/cert.pem".to_string(),
            client_key_path: "/nonexistent/key.pem".to_string(),
        };

        let (_tx, rx) = mpsc::channel(100);
        let result = MqttPublisher::new(&config, "CP001".to_string(), rx, 500);
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
        assert_eq!(connection_state_str(ConnectionState::Connected), "connected");
        assert_eq!(connection_state_str(ConnectionState::Disconnected), "disconnected");
        assert_eq!(connection_state_str(ConnectionState::Reconnecting), "reconnecting");
        assert_eq!(connection_state_str(ConnectionState::Connecting), "connecting");
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
        assert!(parsed.is_ok(), "Timestamp should be valid ISO 8601: {}", ts_str);
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
}
