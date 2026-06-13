//! WebSocket client (upstream handler) for the Mobi.e Central System connection.
//!
//! Maintains the connection to the central system with exponential backoff reconnection.
//! Implements a 10-second connection timeout and a 5-minute reconnection window before
//! signaling that the downstream connection should be closed.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_tls_with_config};
use tracing::{debug, error, info, warn};
use url::Url;

use crate::error::ProxyError;
use crate::models::{ConnectionId, ConnectionState, ExponentialBackoff, OcppFrame};
use crate::state::ConnectionStateManager;

/// Default connection timeout (10 seconds).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum duration to attempt reconnection before giving up (5 minutes).
const MAX_RECONNECT_DURATION: Duration = Duration::from_secs(300);

/// Default initial backoff delay for reconnection (2 seconds).
const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_secs(2);

/// Default maximum backoff delay for reconnection (60 seconds).
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(60);

/// The OCPP 1.6J WebSocket subprotocol identifier.
const OCPP_SUBPROTOCOL: &str = "ocpp1.6";

/// Upstream WebSocket client for the Central System connection.
///
/// Manages the lifecycle of the upstream connection including initial connection,
/// message sending/receiving, reconnection with exponential backoff, and state
/// transitions communicated through the state manager.
pub struct UpstreamHandler {
    /// The base URL of the Central System (without the Charge Point ID path).
    central_system_url: Url,
    /// The Charge Point ID to append to the connection URL path.
    charge_point_id: String,
    /// The WebSocket subprotocol to forward (received from the charger).
    subprotocol: String,
    /// The active WebSocket connection, if any.
    connection: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    /// Current connection state.
    state: ConnectionState,
    /// Exponential backoff strategy for reconnection attempts.
    reconnect_strategy: ExponentialBackoff,
}

impl UpstreamHandler {
    /// Create a new `UpstreamHandler`.
    ///
    /// # Arguments
    /// * `central_system_url` - Base URL of the Central System (e.g., `wss://cs.mobi-e.pt`)
    /// * `charge_point_id` - The Charge Point ID to use in the connection path
    /// * `subprotocol` - The WebSocket subprotocol to negotiate (typically `ocpp1.6`)
    pub fn new(central_system_url: Url, charge_point_id: String, subprotocol: String) -> Self {
        Self {
            central_system_url,
            charge_point_id,
            subprotocol,
            connection: None,
            state: ConnectionState::Disconnected,
            reconnect_strategy: ExponentialBackoff::with_defaults(
                DEFAULT_INITIAL_BACKOFF,
                DEFAULT_MAX_BACKOFF,
            ),
        }
    }

    /// Build the full connection URL: `{central_system_url}/{charge_point_id}`.
    pub fn build_connection_url(&self) -> Url {
        let mut url = self.central_system_url.clone();
        // Ensure the path ends with '/' before appending charge_point_id
        let mut path = url.path().to_string();
        if !path.ends_with('/') {
            path.push('/');
        }
        path.push_str(&self.charge_point_id);
        url.set_path(&path);
        url
    }

    /// Connect to the Central System with a 10-second timeout.
    ///
    /// Emits `Connecting` → `Connected` state transitions on success,
    /// or `Connecting` → `Disconnected` on failure.
    pub async fn connect(
        &mut self,
        state_manager: &mut ConnectionStateManager,
    ) -> Result<(), ProxyError> {
        let url = self.build_connection_url();
        info!(
            charge_point_id = %self.charge_point_id,
            url = %url,
            "Connecting to Central System"
        );

        self.transition_state(ConnectionState::Connecting, state_manager);

        match self.establish_connection(&url).await {
            Ok(ws_stream) => {
                self.connection = Some(ws_stream);
                self.transition_state(ConnectionState::Connected, state_manager);
                self.reconnect_strategy.reset();
                info!(
                    charge_point_id = %self.charge_point_id,
                    "Connected to Central System"
                );
                Ok(())
            }
            Err(e) => {
                self.transition_state(ConnectionState::Disconnected, state_manager);
                error!(
                    charge_point_id = %self.charge_point_id,
                    error = %e,
                    "Failed to connect to Central System"
                );
                Err(e)
            }
        }
    }

    /// Receive the next text message from the Central System.
    ///
    /// Returns an `OcppFrame` parsed from the received text message.
    /// Returns an error if the connection is closed or a non-text message is received.
    pub async fn recv(&mut self) -> Result<OcppFrame, ProxyError> {
        let ws = self.connection.as_mut().ok_or_else(|| {
            ProxyError::ConnectionUpstream {
                description: "No active upstream connection".to_string(),
            }
        })?;

        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    debug!(
                        charge_point_id = %self.charge_point_id,
                        "Received message from Central System"
                    );
                    return OcppFrame::parse(&text);
                }
                Some(Ok(Message::Ping(data))) => {
                    // Respond to pings automatically (tungstenite handles this,
                    // but we consume the message from the stream)
                    debug!("Received ping from Central System");
                    if let Err(e) = ws.send(Message::Pong(data)).await {
                        return Err(ProxyError::ConnectionUpstream {
                            description: format!("Failed to send pong: {}", e),
                        });
                    }
                }
                Some(Ok(Message::Pong(_))) => {
                    debug!("Received pong from Central System");
                    continue;
                }
                Some(Ok(Message::Close(_))) => {
                    info!(
                        charge_point_id = %self.charge_point_id,
                        "Central System closed the connection"
                    );
                    self.connection = None;
                    return Err(ProxyError::ConnectionUpstream {
                        description: "Central System closed the connection".to_string(),
                    });
                }
                Some(Ok(Message::Binary(_))) => {
                    warn!("Received unexpected binary message from Central System, ignoring");
                    continue;
                }
                Some(Ok(Message::Frame(_))) => {
                    continue;
                }
                Some(Err(e)) => {
                    error!(
                        charge_point_id = %self.charge_point_id,
                        error = %e,
                        "WebSocket error on upstream connection"
                    );
                    self.connection = None;
                    return Err(ProxyError::ConnectionUpstream {
                        description: format!("WebSocket error: {}", e),
                    });
                }
                None => {
                    info!(
                        charge_point_id = %self.charge_point_id,
                        "Upstream WebSocket stream ended"
                    );
                    self.connection = None;
                    return Err(ProxyError::ConnectionUpstream {
                        description: "Upstream stream ended".to_string(),
                    });
                }
            }
        }
    }

    /// Send a text message (raw OCPP frame) to the Central System.
    pub async fn send(&mut self, frame: &OcppFrame) -> Result<(), ProxyError> {
        let ws = self.connection.as_mut().ok_or_else(|| {
            ProxyError::ConnectionUpstream {
                description: "No active upstream connection".to_string(),
            }
        })?;

        debug!(
            charge_point_id = %self.charge_point_id,
            unique_id = %frame.unique_id,
            "Sending message to Central System"
        );

        ws.send(Message::Text(frame.raw.clone().into())).await.map_err(|e| {
            ProxyError::ConnectionUpstream {
                description: format!("Failed to send message: {}", e),
            }
        })
    }

    /// Close the upstream connection gracefully with the given close code.
    pub async fn close(&mut self, code: CloseCode) -> Result<(), ProxyError> {
        if let Some(ref mut ws) = self.connection {
            let close_frame = CloseFrame {
                code,
                reason: "Proxy closing connection".into(),
            };
            if let Err(e) = ws.send(Message::Close(Some(close_frame))).await {
                warn!(
                    charge_point_id = %self.charge_point_id,
                    error = %e,
                    "Error sending close frame to Central System"
                );
            }
        }
        self.connection = None;
        self.state = ConnectionState::Disconnected;
        info!(
            charge_point_id = %self.charge_point_id,
            "Upstream connection closed"
        );
        Ok(())
    }

    /// Get the current connection state.
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// Attempt reconnection with exponential backoff.
    ///
    /// Retries indefinitely within a 5-minute window. If reconnection succeeds,
    /// state transitions to `Connected`. If the 5-minute window elapses without
    /// success, returns an error indicating downstream should be closed with code 1001.
    pub async fn reconnect(
        &mut self,
        state_manager: &mut ConnectionStateManager,
    ) -> Result<(), ProxyError> {
        self.transition_state(ConnectionState::Reconnecting, state_manager);
        self.reconnect_strategy.reset();

        let url = self.build_connection_url();
        let start = tokio::time::Instant::now();

        info!(
            charge_point_id = %self.charge_point_id,
            max_duration_secs = MAX_RECONNECT_DURATION.as_secs(),
            "Starting upstream reconnection attempts"
        );

        loop {
            let elapsed = start.elapsed();
            if elapsed >= MAX_RECONNECT_DURATION {
                error!(
                    charge_point_id = %self.charge_point_id,
                    elapsed_secs = elapsed.as_secs(),
                    "Reconnection window expired, downstream should be closed with 1001"
                );
                self.transition_state(ConnectionState::Disconnected, state_manager);
                return Err(ProxyError::ConnectionUpstream {
                    description: format!(
                        "Upstream reconnection failed after {} seconds; close downstream with 1001",
                        elapsed.as_secs()
                    ),
                });
            }

            let delay = self.reconnect_strategy.next_delay();
            // Don't wait longer than the remaining reconnection window
            let remaining = MAX_RECONNECT_DURATION.saturating_sub(elapsed);
            let actual_delay = delay.min(remaining);

            debug!(
                charge_point_id = %self.charge_point_id,
                delay_ms = actual_delay.as_millis(),
                "Waiting before reconnection attempt"
            );

            tokio::time::sleep(actual_delay).await;

            info!(
                charge_point_id = %self.charge_point_id,
                url = %url,
                "Attempting upstream reconnection"
            );

            match self.establish_connection(&url).await {
                Ok(ws_stream) => {
                    self.connection = Some(ws_stream);
                    self.transition_state(ConnectionState::Connected, state_manager);
                    self.reconnect_strategy.reset();
                    info!(
                        charge_point_id = %self.charge_point_id,
                        elapsed_secs = start.elapsed().as_secs(),
                        "Upstream reconnection successful"
                    );
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        charge_point_id = %self.charge_point_id,
                        error = %e,
                        "Upstream reconnection attempt failed"
                    );
                }
            }
        }
    }

    /// Establish a WebSocket connection to the given URL with timeout and subprotocol.
    async fn establish_connection(
        &self,
        url: &Url,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, ProxyError> {
        let mut request = url.as_str().into_client_request().map_err(|e| {
            ProxyError::ConnectionUpstream {
                description: format!("Failed to build WebSocket request: {}", e),
            }
        })?;

        // Set the Sec-WebSocket-Protocol header with the subprotocol
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(&self.subprotocol).map_err(|e| {
                ProxyError::ConnectionUpstream {
                    description: format!("Invalid subprotocol header value: {}", e),
                }
            })?,
        );

        let connect_future = connect_async_tls_with_config(request, None, false, None);

        let (ws_stream, response) =
            timeout(CONNECT_TIMEOUT, connect_future)
                .await
                .map_err(|_| ProxyError::ConnectionUpstream {
                    description: format!(
                        "Connection timed out after {} seconds",
                        CONNECT_TIMEOUT.as_secs()
                    ),
                })?
                .map_err(|e| ProxyError::ConnectionUpstream {
                    description: format!("WebSocket connection failed: {}", e),
                })?;

        debug!(
            status = %response.status(),
            "Upstream WebSocket handshake completed"
        );

        Ok(ws_stream)
    }

    /// Update internal state and emit the transition through the state manager.
    fn transition_state(
        &mut self,
        new_state: ConnectionState,
        state_manager: &mut ConnectionStateManager,
    ) {
        if self.state != new_state {
            debug!(
                charge_point_id = %self.charge_point_id,
                previous = ?self.state,
                current = ?new_state,
                "Upstream state transition"
            );
            self.state = new_state;
            state_manager.transition(ConnectionId::Upstream, new_state);
        }
    }

    /// Check if the upstream connection is currently active.
    pub fn is_connected(&self) -> bool {
        self.state == ConnectionState::Connected && self.connection.is_some()
    }

    /// Get the Charge Point ID associated with this upstream handler.
    pub fn charge_point_id(&self) -> &str {
        &self.charge_point_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_connection_url_basic() {
        let handler = UpstreamHandler::new(
            Url::parse("wss://cs.mobi-e.pt").unwrap(),
            "CP001".to_string(),
            OCPP_SUBPROTOCOL.to_string(),
        );
        let url = handler.build_connection_url();
        assert_eq!(url.as_str(), "wss://cs.mobi-e.pt/CP001");
    }

    #[test]
    fn test_build_connection_url_with_trailing_slash() {
        let handler = UpstreamHandler::new(
            Url::parse("wss://cs.mobi-e.pt/").unwrap(),
            "CP001".to_string(),
            OCPP_SUBPROTOCOL.to_string(),
        );
        let url = handler.build_connection_url();
        assert_eq!(url.as_str(), "wss://cs.mobi-e.pt/CP001");
    }

    #[test]
    fn test_build_connection_url_with_path() {
        let handler = UpstreamHandler::new(
            Url::parse("wss://cs.mobi-e.pt/ocpp").unwrap(),
            "CHARGE_POINT_42".to_string(),
            OCPP_SUBPROTOCOL.to_string(),
        );
        let url = handler.build_connection_url();
        assert_eq!(url.as_str(), "wss://cs.mobi-e.pt/ocpp/CHARGE_POINT_42");
    }

    #[test]
    fn test_build_connection_url_with_port() {
        let handler = UpstreamHandler::new(
            Url::parse("ws://localhost:8080").unwrap(),
            "test-cp".to_string(),
            OCPP_SUBPROTOCOL.to_string(),
        );
        let url = handler.build_connection_url();
        assert_eq!(url.as_str(), "ws://localhost:8080/test-cp");
    }

    #[test]
    fn test_build_connection_url_preserves_charge_point_id() {
        let id = "PT-MOB-CP-12345-AB";
        let handler = UpstreamHandler::new(
            Url::parse("wss://central-system.example.com").unwrap(),
            id.to_string(),
            OCPP_SUBPROTOCOL.to_string(),
        );
        let url = handler.build_connection_url();
        assert!(url.as_str().ends_with(id));
    }

    #[test]
    fn test_initial_state_is_disconnected() {
        let handler = UpstreamHandler::new(
            Url::parse("wss://cs.mobi-e.pt").unwrap(),
            "CP001".to_string(),
            OCPP_SUBPROTOCOL.to_string(),
        );
        assert_eq!(handler.state(), ConnectionState::Disconnected);
        assert!(!handler.is_connected());
    }

    #[test]
    fn test_charge_point_id_accessor() {
        let handler = UpstreamHandler::new(
            Url::parse("wss://cs.mobi-e.pt").unwrap(),
            "MY_CHARGER".to_string(),
            OCPP_SUBPROTOCOL.to_string(),
        );
        assert_eq!(handler.charge_point_id(), "MY_CHARGER");
    }

    #[test]
    fn test_backoff_configuration() {
        let handler = UpstreamHandler::new(
            Url::parse("wss://cs.mobi-e.pt").unwrap(),
            "CP001".to_string(),
            OCPP_SUBPROTOCOL.to_string(),
        );
        // Verify the backoff strategy is configured with correct defaults
        assert_eq!(handler.reconnect_strategy.initial, Duration::from_secs(2));
        assert_eq!(handler.reconnect_strategy.max, Duration::from_secs(60));
        assert_eq!(handler.reconnect_strategy.multiplier, 2.0);
    }

    #[test]
    fn test_backoff_sequence_matches_requirements() {
        let mut backoff = ExponentialBackoff::with_defaults(
            DEFAULT_INITIAL_BACKOFF,
            DEFAULT_MAX_BACKOFF,
        );
        // Per requirement 2.4: exponential backoff starting at 2s, doubling, max 60s
        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
        assert_eq!(backoff.next_delay(), Duration::from_secs(4));
        assert_eq!(backoff.next_delay(), Duration::from_secs(8));
        assert_eq!(backoff.next_delay(), Duration::from_secs(16));
        assert_eq!(backoff.next_delay(), Duration::from_secs(32));
        assert_eq!(backoff.next_delay(), Duration::from_secs(60)); // capped at max
        assert_eq!(backoff.next_delay(), Duration::from_secs(60)); // stays at max
    }

    #[test]
    fn test_connect_timeout_constant() {
        assert_eq!(CONNECT_TIMEOUT, Duration::from_secs(10));
    }

    #[test]
    fn test_max_reconnect_duration_constant() {
        assert_eq!(MAX_RECONNECT_DURATION, Duration::from_secs(300));
    }
}
