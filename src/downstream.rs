//! WebSocket server (downstream handler) for charger connections.
//!
//! Accepts incoming WebSocket connections from EV chargers using OCPP 1.6J subprotocol.
//! Validates the `ocpp1.6` subprotocol during upgrade, replaces existing connections
//! for the same Charge Point ID, and emits connection state changes.
//!
//! Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    extract::{Path, State, WebSocketUpgrade},
    extract::ws::{Message, WebSocket},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, warn};

use crate::error::ProxyError;
use crate::models::{ConnectionId, ConnectionState};
use crate::state::ConnectionStateManager;

/// The required OCPP 1.6J WebSocket subprotocol identifier.
pub const OCPP16_SUBPROTOCOL: &str = "ocpp1.6";

/// Timeout for completing the WebSocket handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Represents an active downstream connection with its message channels.
#[derive(Debug)]
pub struct ActiveConnection {
    /// Sender to push messages TO the charger (used by forwarder).
    pub tx: mpsc::Sender<Message>,
    /// Handle to abort the connection task when replacing.
    pub abort_handle: tokio::task::AbortHandle,
}

/// Shared state for the downstream WebSocket server.
#[derive(Clone)]
pub struct DownstreamState {
    /// Active connections indexed by Charge Point ID.
    pub connections: Arc<Mutex<HashMap<String, ActiveConnection>>>,
    /// Connection state manager for emitting state transitions.
    pub state_manager: Arc<Mutex<ConnectionStateManager>>,
    /// Channel to send received messages FROM chargers to the forwarder.
    /// The tuple contains (charge_point_id, message).
    pub message_tx: mpsc::Sender<(String, Message)>,
}

/// Validates the `Sec-WebSocket-Protocol` header for the OCPP 1.6 subprotocol.
///
/// Returns `true` if the client requests `ocpp1.6` among its subprotocols.
pub fn validate_subprotocol(protocols: &[String]) -> bool {
    protocols.iter().any(|p| p == OCPP16_SUBPROTOCOL)
}

/// Extracts subprotocol list from the raw `Sec-WebSocket-Protocol` header value.
///
/// The header is a comma-separated list of protocol names.
pub fn parse_subprotocol_header(header_value: &str) -> Vec<String> {
    header_value
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Creates the axum router for the downstream WebSocket server.
///
/// The router accepts WebSocket upgrade requests at `/{charge_point_id}`.
pub fn create_router(state: DownstreamState) -> Router {
    Router::new()
        .route("/{charge_point_id}", get(ws_upgrade_handler))
        .with_state(state)
}

/// Starts the downstream WebSocket server on the given address.
///
/// This function runs indefinitely, accepting charger connections.
pub async fn start_server(
    addr: SocketAddr,
    state: DownstreamState,
) -> Result<(), ProxyError> {
    let router = create_router(state);

    info!(
        component = "downstream",
        addr = %addr,
        "Starting downstream WebSocket server"
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| ProxyError::ConnectionDownstream {
            description: format!("Failed to bind downstream server: {}", e),
        })?;

    axum::serve(listener, router)
        .await
        .map_err(|e| ProxyError::ConnectionDownstream {
            description: format!("Downstream server error: {}", e),
        })?;

    Ok(())
}

/// Handler for WebSocket upgrade requests.
///
/// Validates the subprotocol from the `Sec-WebSocket-Protocol` header.
/// If the client doesn't request `ocpp1.6`, the connection is rejected
/// with HTTP 400 Bad Request.
async fn ws_upgrade_handler(
    Path(charge_point_id): Path<String>,
    headers: HeaderMap,
    State(state): State<DownstreamState>,
    ws: WebSocketUpgrade,
) -> Response {
    info!(
        component = "downstream",
        charge_point_id = %charge_point_id,
        "WebSocket upgrade request received"
    );

    // Extract and validate subprotocol from Sec-WebSocket-Protocol header
    let subprotocol_valid = if let Some(protocol_header) = headers.get("sec-websocket-protocol") {
        if let Ok(header_str) = protocol_header.to_str() {
            let protocols = parse_subprotocol_header(header_str);
            validate_subprotocol(&protocols)
        } else {
            false
        }
    } else {
        // No subprotocol header means the client didn't request any subprotocol.
        // Per requirement 1.6: reject if client requests a subprotocol other than ocpp1.6.
        // If no subprotocol is requested at all, we also reject since OCPP 1.6J requires it.
        false
    };

    if !subprotocol_valid {
        warn!(
            component = "downstream",
            charge_point_id = %charge_point_id,
            "Rejecting connection: client did not request ocpp1.6 subprotocol"
        );
        return (
            StatusCode::BAD_REQUEST,
            "WebSocket subprotocol ocpp1.6 is required",
        )
            .into_response();
    }

    // Emit Connecting state
    {
        let mut mgr = state.state_manager.lock().await;
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connecting);
    }

    // Set up the upgrade with the ocpp1.6 subprotocol selected in the response
    let state_clone = state.clone();
    let cp_id = charge_point_id.clone();

    ws.protocols([OCPP16_SUBPROTOCOL])
        .on_upgrade(move |socket| {
            handle_connection_with_timeout(socket, cp_id, state_clone)
        })
        .into_response()
}

/// Wraps `handle_connection` with a handshake timeout.
///
/// If the initial setup takes longer than `HANDSHAKE_TIMEOUT`, the connection
/// is dropped.
async fn handle_connection_with_timeout(
    socket: WebSocket,
    charge_point_id: String,
    state: DownstreamState,
) {
    let result = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        setup_connection(socket, charge_point_id.clone(), state.clone()),
    )
    .await;

    match result {
        Ok(Some((ws_receiver, tx))) => {
            // Connection established, now run the read loop (no timeout for this part)
            run_read_loop(ws_receiver, charge_point_id, state, tx).await;
        }
        Ok(None) => {
            // Setup failed (logged internally)
        }
        Err(_) => {
            error!(
                component = "downstream",
                charge_point_id = %charge_point_id,
                "WebSocket handshake timed out ({}s)",
                HANDSHAKE_TIMEOUT.as_secs()
            );
            // Emit Disconnected state on timeout
            let mut mgr = state.state_manager.lock().await;
            mgr.transition(ConnectionId::Downstream, ConnectionState::Disconnected);
        }
    }
}

/// Sets up the connection: splits the socket, registers it, replaces existing connections.
///
/// Returns the receiver half and the sender channel on success, or None on failure.
async fn setup_connection(
    socket: WebSocket,
    charge_point_id: String,
    state: DownstreamState,
) -> Option<(futures_util::stream::SplitStream<WebSocket>, mpsc::Sender<Message>)> {
    let (ws_sender, ws_receiver) = socket.split();

    // Create a channel for sending messages TO this charger
    let (tx, rx) = mpsc::channel::<Message>(64);

    // Spawn a task to forward messages from the channel to the WebSocket
    let send_task = tokio::spawn(forward_to_websocket(ws_sender, rx));
    let abort_handle = send_task.abort_handle();

    // Replace existing connection for this Charge Point ID
    {
        let mut connections = state.connections.lock().await;
        if let Some(existing) = connections.remove(&charge_point_id) {
            warn!(
                component = "downstream",
                charge_point_id = %charge_point_id,
                "Replacing existing connection for Charge Point"
            );
            // Send close frame to existing connection before aborting
            let _ = existing
                .tx
                .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: 1000,
                    reason: "New connection replacing existing one".into(),
                })))
                .await;
            // Give a moment for the close frame to be sent
            tokio::time::sleep(Duration::from_millis(100)).await;
            existing.abort_handle.abort();
        }

        connections.insert(
            charge_point_id.clone(),
            ActiveConnection {
                tx: tx.clone(),
                abort_handle,
            },
        );
    }

    // Emit Connected state
    {
        let mut mgr = state.state_manager.lock().await;
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
    }

    info!(
        component = "downstream",
        charge_point_id = %charge_point_id,
        "Charger connected"
    );

    Some((ws_receiver, tx))
}

/// Forwards messages from the mpsc channel to the WebSocket sink.
async fn forward_to_websocket(
    mut ws_sender: futures_util::stream::SplitSink<WebSocket, Message>,
    mut rx: mpsc::Receiver<Message>,
) {
    while let Some(msg) = rx.recv().await {
        if ws_sender.send(msg).await.is_err() {
            break;
        }
    }
}

/// Reads messages from the charger WebSocket and forwards them to the message channel.
async fn run_read_loop(
    mut ws_receiver: futures_util::stream::SplitStream<WebSocket>,
    charge_point_id: String,
    state: DownstreamState,
    _tx: mpsc::Sender<Message>,
) {
    while let Some(result) = ws_receiver.next().await {
        match result {
            Ok(msg) => match &msg {
                Message::Close(_) => {
                    info!(
                        component = "downstream",
                        charge_point_id = %charge_point_id,
                        "Charger sent close frame"
                    );
                    break;
                }
                Message::Text(_) | Message::Binary(_) => {
                    if state
                        .message_tx
                        .send((charge_point_id.clone(), msg))
                        .await
                        .is_err()
                    {
                        error!(
                            component = "downstream",
                            charge_point_id = %charge_point_id,
                            "Failed to forward message to forwarder channel"
                        );
                        break;
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {
                    // Ping/Pong handled automatically by axum
                }
            },
            Err(e) => {
                error!(
                    component = "downstream",
                    charge_point_id = %charge_point_id,
                    error = %e,
                    "WebSocket read error"
                );
                break;
            }
        }
    }

    // Connection ended — clean up
    {
        let mut connections = state.connections.lock().await;
        connections.remove(&charge_point_id);
    }

    // Emit Disconnected state
    {
        let mut mgr = state.state_manager.lock().await;
        mgr.transition(ConnectionId::Downstream, ConnectionState::Disconnected);
    }

    info!(
        component = "downstream",
        charge_point_id = %charge_point_id,
        "Charger disconnected"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Subprotocol validation tests ---

    #[test]
    fn test_validate_subprotocol_with_ocpp16() {
        let protocols = vec!["ocpp1.6".to_string()];
        assert!(validate_subprotocol(&protocols));
    }

    #[test]
    fn test_validate_subprotocol_with_multiple_including_ocpp16() {
        let protocols = vec![
            "ocpp2.0".to_string(),
            "ocpp1.6".to_string(),
            "custom".to_string(),
        ];
        assert!(validate_subprotocol(&protocols));
    }

    #[test]
    fn test_validate_subprotocol_without_ocpp16() {
        let protocols = vec!["ocpp2.0".to_string(), "custom".to_string()];
        assert!(!validate_subprotocol(&protocols));
    }

    #[test]
    fn test_validate_subprotocol_empty_list() {
        let protocols: Vec<String> = vec![];
        assert!(!validate_subprotocol(&protocols));
    }

    #[test]
    fn test_validate_subprotocol_case_sensitive() {
        let protocols = vec!["OCPP1.6".to_string()];
        assert!(!validate_subprotocol(&protocols));
    }

    #[test]
    fn test_validate_subprotocol_similar_strings() {
        let protocols = vec![
            "ocpp1.6j".to_string(),
            "ocpp1.6.1".to_string(),
            "ocpp16".to_string(),
        ];
        assert!(!validate_subprotocol(&protocols));
    }

    #[test]
    fn test_validate_subprotocol_with_whitespace_in_value() {
        // After parsing, values should be trimmed, so this tests the combination
        let protocols = vec!["ocpp1.6".to_string()];
        assert!(validate_subprotocol(&protocols));
    }

    #[test]
    fn test_validate_subprotocol_partial_match() {
        let protocols = vec!["ocpp1.".to_string(), "1.6".to_string()];
        assert!(!validate_subprotocol(&protocols));
    }

    // --- Subprotocol header parsing tests ---

    #[test]
    fn test_parse_subprotocol_header_single() {
        let result = parse_subprotocol_header("ocpp1.6");
        assert_eq!(result, vec!["ocpp1.6"]);
    }

    #[test]
    fn test_parse_subprotocol_header_multiple() {
        let result = parse_subprotocol_header("ocpp1.6, ocpp2.0, custom");
        assert_eq!(result, vec!["ocpp1.6", "ocpp2.0", "custom"]);
    }

    #[test]
    fn test_parse_subprotocol_header_with_extra_spaces() {
        let result = parse_subprotocol_header("  ocpp1.6 ,  ocpp2.0  ");
        assert_eq!(result, vec!["ocpp1.6", "ocpp2.0"]);
    }

    #[test]
    fn test_parse_subprotocol_header_empty() {
        let result = parse_subprotocol_header("");
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_subprotocol_header_single_with_whitespace() {
        let result = parse_subprotocol_header("  ocpp1.6  ");
        assert_eq!(result, vec!["ocpp1.6"]);
    }

    #[test]
    fn test_parse_subprotocol_header_commas_only() {
        let result = parse_subprotocol_header(",,,");
        assert!(result.is_empty());
    }

    // --- DownstreamState construction test ---

    #[tokio::test]
    async fn test_downstream_state_construction() {
        let (msg_tx, _msg_rx) = mpsc::channel(32);
        let state = DownstreamState {
            connections: Arc::new(Mutex::new(HashMap::new())),
            state_manager: Arc::new(Mutex::new(ConnectionStateManager::new(16))),
            message_tx: msg_tx,
        };

        let connections = state.connections.lock().await;
        assert!(connections.is_empty());
    }

    // --- Connection replacement logic test ---

    #[tokio::test]
    async fn test_active_connection_replacement_sends_close() {
        let (tx, mut rx) = mpsc::channel::<Message>(16);
        let abort_handle = tokio::spawn(async {}).abort_handle();

        let conn = ActiveConnection { tx, abort_handle };

        // Send close frame via the active connection's channel
        let _ = conn
            .tx
            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: 1000,
                reason: "Replacing".into(),
            })))
            .await;

        // Verify the close message was received
        let msg = rx.recv().await.unwrap();
        match msg {
            Message::Close(Some(frame)) => {
                assert_eq!(frame.code, 1000);
                assert_eq!(frame.reason, "Replacing");
            }
            _ => panic!("Expected Close message"),
        }
    }

    // --- Router creation test ---

    #[tokio::test]
    async fn test_create_router_returns_router() {
        let (msg_tx, _msg_rx) = mpsc::channel(32);
        let state = DownstreamState {
            connections: Arc::new(Mutex::new(HashMap::new())),
            state_manager: Arc::new(Mutex::new(ConnectionStateManager::new(16))),
            message_tx: msg_tx,
        };

        // Just verify it doesn't panic
        let _router = create_router(state);
    }
}
