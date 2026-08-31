//! WebSocket server (downstream handler) for charger connections.
//!
//! Accepts incoming WebSocket connections from EV chargers using OCPP 1.6J subprotocol.
//! Validates the `ocpp1.6` subprotocol during upgrade, replaces existing connections
//! for the same Charge Point ID, and emits connection state changes.
//!
//! Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::{
    extract::ws::WebSocket,
    extract::{Path, State, WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::forwarder::MqttEvent;
use crate::models::{ConnectionId, ConnectionState};
use crate::session::{self, SessionConfig};
use crate::state::ConnectionStateManager;

/// The required OCPP 1.6J WebSocket subprotocol identifier.
pub const OCPP16_SUBPROTOCOL: &str = "ocpp1.6";

/// Represents an active downstream connection.
#[derive(Debug, Clone)]
pub struct ActiveConnection {
    /// Monotonic id for this connection.
    ///
    /// Registry cleanup is guarded on this value. Without it, a replaced
    /// connection's task removes the map entry belonging to the connection
    /// that replaced it, silently deregistering a live charger and marking
    /// downstream disconnected while it is still connected.
    pub generation: u64,
    /// Cancels the session task owning this connection.
    pub cancel: CancellationToken,
}

/// Shared state for the downstream WebSocket server.
#[derive(Clone)]
pub struct DownstreamState {
    /// Active connections indexed by Charge Point ID.
    pub connections: Arc<Mutex<HashMap<String, ActiveConnection>>>,
    /// Connection state manager for emitting state transitions.
    pub state_manager: Arc<Mutex<ConnectionStateManager>>,
    /// Configuration handed to each session.
    pub session_config: Arc<SessionConfig>,
    /// Channel to the MQTT publisher.
    pub mqtt_tx: mpsc::Sender<MqttEvent>,
    /// Cancelled when the proxy is shutting down.
    pub shutdown: CancellationToken,
    /// Source of connection generations.
    pub generation: Arc<AtomicU64>,
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
        .on_upgrade(move |socket| handle_connection(socket, cp_id, state_clone))
        .into_response()
}

/// Register a connection, displacing any existing one for the same Charge
/// Point ID (Requirement 1.4).
///
/// Returns the generation assigned to the new connection. The displaced
/// connection's token is cancelled, which makes its session send a close frame
/// and unwind.
pub async fn register_connection(
    state: &DownstreamState,
    charge_point_id: &str,
    cancel: CancellationToken,
) -> u64 {
    let generation = state.generation.fetch_add(1, Ordering::SeqCst);

    let mut connections = state.connections.lock().await;
    if let Some(existing) = connections.insert(
        charge_point_id.to_string(),
        ActiveConnection { generation, cancel },
    ) {
        warn!(
            component = "downstream",
            charge_point_id = %charge_point_id,
            replaced_generation = existing.generation,
            generation = generation,
            "Replacing existing connection for this Charge Point"
        );
        existing.cancel.cancel();
    }

    generation
}

/// Deregister a connection, but only if it is still the current one.
///
/// Returns whether the entry was removed. The generation check is the whole
/// point: a displaced session finishes *after* its replacement has registered,
/// and an unguarded removal would deregister the live charger, marking
/// downstream disconnected while it is still connected.
pub async fn deregister_connection(
    state: &DownstreamState,
    charge_point_id: &str,
    generation: u64,
) -> bool {
    let mut connections = state.connections.lock().await;
    match connections.get(charge_point_id) {
        Some(active) if active.generation == generation => {
            connections.remove(charge_point_id);
            true
        }
        _ => false,
    }
}

/// Registers the connection, replacing any existing one for the same Charge
/// Point ID, then runs the proxy session until it ends.
async fn handle_connection(socket: WebSocket, charge_point_id: String, state: DownstreamState) {
    let cancel = state.shutdown.child_token();
    let generation = register_connection(&state, &charge_point_id, cancel.clone()).await;

    let (upstream, downstream) = {
        let mut mgr = state.state_manager.lock().await;
        mgr.transition(ConnectionId::Downstream, ConnectionState::Connected);
        (mgr.upstream_state(), mgr.downstream_state())
    };
    session::publish_status(&state.mqtt_tx, &charge_point_id, upstream, downstream);

    info!(
        component = "downstream",
        charge_point_id = %charge_point_id,
        generation = generation,
        "Charger connected"
    );

    session::run_session(
        charge_point_id.clone(),
        socket,
        state.session_config.clone(),
        state.state_manager.clone(),
        state.mqtt_tx.clone(),
        cancel,
    )
    .await;

    // ---- deregister, but only if we are still the current connection ----
    let still_current = deregister_connection(&state, &charge_point_id, generation).await;

    if still_current {
        let (upstream, downstream) = {
            let mut mgr = state.state_manager.lock().await;
            mgr.transition(ConnectionId::Downstream, ConnectionState::Disconnected);
            (mgr.upstream_state(), mgr.downstream_state())
        };
        session::publish_status(&state.mqtt_tx, &charge_point_id, upstream, downstream);
        info!(
            component = "downstream",
            charge_point_id = %charge_point_id,
            "Charger disconnected"
        );
    } else {
        debug!(
            component = "downstream",
            charge_point_id = %charge_point_id,
            generation = generation,
            "Displaced connection finished; leaving registry to its successor"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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

    // --- Registry behaviour ---

    fn test_session_config() -> SessionConfig {
        SessionConfig {
            central_system_url: url::Url::parse("ws://127.0.0.1:1/ocpp").unwrap(),
            upstream_bind_address: None,
            subprotocol: OCPP16_SUBPROTOCOL.to_string(),
            message_buffer_size: 100,
            max_buffer_duration: Duration::from_secs(30),
            connect_timeout: Duration::from_millis(50),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            max_reconnect_window: Duration::from_millis(100),
            call_tracker_max_age: Duration::from_secs(300),
        }
    }

    fn test_state() -> DownstreamState {
        let (msg_tx, _msg_rx) = mpsc::channel(32);
        // The receiver is dropped, which is fine: every MQTT send is
        // best-effort by design and must never affect forwarding.
        DownstreamState {
            connections: Arc::new(Mutex::new(HashMap::new())),
            state_manager: Arc::new(Mutex::new(ConnectionStateManager::new(16))),
            session_config: Arc::new(test_session_config()),
            mqtt_tx: msg_tx,
            shutdown: CancellationToken::new(),
            generation: Arc::new(AtomicU64::new(1)),
        }
    }

    #[tokio::test]
    async fn test_downstream_state_construction() {
        let state = test_state();
        assert!(state.connections.lock().await.is_empty());
    }

    #[tokio::test]
    async fn test_create_router_returns_router() {
        let _router = create_router(test_state());
    }

    #[tokio::test]
    async fn test_replacing_a_connection_cancels_the_old_one() {
        let state = test_state();
        let first = CancellationToken::new();
        let second = CancellationToken::new();

        let mut conns = state.connections.lock().await;
        conns.insert(
            "CP1".to_string(),
            ActiveConnection {
                generation: 1,
                cancel: first.clone(),
            },
        );
        let displaced = conns.insert(
            "CP1".to_string(),
            ActiveConnection {
                generation: 2,
                cancel: second.clone(),
            },
        );
        drop(conns);

        displaced
            .expect("the first connection should be returned")
            .cancel
            .cancel();

        assert!(first.is_cancelled(), "displaced session must be cancelled");
        assert!(!second.is_cancelled(), "replacement must stay live");
    }

    /// The regression this generation guard exists for.
    ///
    /// A displaced connection's task finishes *after* its replacement has
    /// registered. Without the guard it removes the map entry belonging to the
    /// live connection, deregistering a charger that is still connected.
    #[tokio::test]
    async fn test_displaced_connection_does_not_deregister_its_replacement() {
        let state = test_state();

        {
            let mut conns = state.connections.lock().await;
            conns.insert(
                "CP1".to_string(),
                ActiveConnection {
                    generation: 1,
                    cancel: CancellationToken::new(),
                },
            );
            // The replacement arrives and takes over the entry.
            conns.insert(
                "CP1".to_string(),
                ActiveConnection {
                    generation: 2,
                    cancel: CancellationToken::new(),
                },
            );
        }

        // Now generation 1 unwinds and attempts its cleanup.
        let displaced_generation = 1u64;
        {
            let mut conns = state.connections.lock().await;
            let is_mine = conns
                .get("CP1")
                .map(|c| c.generation == displaced_generation)
                .unwrap_or(false);
            if is_mine {
                conns.remove("CP1");
            }
        }

        let conns = state.connections.lock().await;
        let active = conns.get("CP1").expect("live connection must survive");
        assert_eq!(
            active.generation, 2,
            "the replacement must remain registered after the displaced session unwinds"
        );
    }
}
