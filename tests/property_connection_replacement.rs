//! Property-based tests for connection replacement.
//!
//! **Property 13: New connections replace existing connections for the same
//! Charge Point ID**
//!
//! For any sequence of connection attempts from chargers with the same
//! Charge_Point_ID, only the most recently accepted connection is active, and
//! every previously active connection has been cancelled — which is what makes
//! its session send a close frame and unwind.
//!
//! **Validates: Requirements 1.4**
//!
//! These tests call `downstream::register_connection` and
//! `downstream::deregister_connection` directly. An earlier version of this
//! file defined a local `simulate_register_connection` that copied the
//! production body, so it asserted against its own copy and would have stayed
//! green if the real implementation were deleted. It also happened to omit the
//! cleanup path, which is where the actual bug was.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use ocpp_proxy::downstream::{
    deregister_connection, register_connection, DownstreamState, OCPP16_SUBPROTOCOL,
};
use ocpp_proxy::session::SessionConfig;
use ocpp_proxy::state::ConnectionStateManager;

fn make_downstream_state() -> DownstreamState {
    let (mqtt_tx, _mqtt_rx) = mpsc::channel(64);
    DownstreamState {
        connections: Arc::new(Mutex::new(HashMap::new())),
        state_manager: Arc::new(Mutex::new(ConnectionStateManager::new(16))),
        session_config: Arc::new(SessionConfig {
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
        }),
        mqtt_tx,
        shutdown: CancellationToken::new(),
        generation: Arc::new(AtomicU64::new(1)),
    }
}

fn arb_charge_point_id() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9-]{1,20}"
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

proptest! {
    /// Only the latest connection is active, and every earlier one was cancelled.
    #[test]
    fn prop_only_latest_connection_is_active(
        charge_point_id in arb_charge_point_id(),
        connection_count in 1usize..=10,
    ) {
        rt().block_on(async {
            let state = make_downstream_state();
            let mut tokens = Vec::new();
            let mut generations = Vec::new();

            for _ in 0..connection_count {
                let token = CancellationToken::new();
                let generation =
                    register_connection(&state, &charge_point_id, token.clone()).await;
                tokens.push(token);
                generations.push(generation);
            }

            let connections = state.connections.lock().await;
            prop_assert_eq!(connections.len(), 1, "exactly one connection per ID");

            let active = connections.get(&charge_point_id).unwrap();
            prop_assert_eq!(
                active.generation,
                *generations.last().unwrap(),
                "the most recent connection must be the active one"
            );

            for (i, token) in tokens.iter().enumerate() {
                let is_last = i == tokens.len() - 1;
                prop_assert_eq!(
                    token.is_cancelled(),
                    !is_last,
                    "connection {} of {}: displaced connections are cancelled, the live one is not",
                    i,
                    connection_count
                );
            }
            Ok(())
        }).unwrap();
    }

    /// Generations strictly increase, so a displaced session can always tell
    /// that it is no longer the current connection.
    #[test]
    fn prop_generations_strictly_increase(
        charge_point_id in arb_charge_point_id(),
        connection_count in 2usize..=10,
    ) {
        rt().block_on(async {
            let state = make_downstream_state();
            let mut generations = Vec::new();
            for _ in 0..connection_count {
                generations.push(
                    register_connection(&state, &charge_point_id, CancellationToken::new()).await,
                );
            }
            for pair in generations.windows(2) {
                prop_assert!(pair[1] > pair[0], "generations must strictly increase");
            }
            Ok(())
        }).unwrap();
    }

    /// The regression the generation guard exists for.
    ///
    /// A displaced session unwinds *after* its replacement registered. Its
    /// cleanup must not remove the live connection's entry.
    #[test]
    fn prop_displaced_cleanup_never_removes_the_live_connection(
        charge_point_id in arb_charge_point_id(),
        connection_count in 2usize..=10,
    ) {
        rt().block_on(async {
            let state = make_downstream_state();
            let mut generations = Vec::new();
            for _ in 0..connection_count {
                generations.push(
                    register_connection(&state, &charge_point_id, CancellationToken::new()).await,
                );
            }
            let live_generation = *generations.last().unwrap();

            // Every displaced session now unwinds, in an arbitrary order.
            for displaced in &generations[..generations.len() - 1] {
                let removed = deregister_connection(&state, &charge_point_id, *displaced).await;
                prop_assert!(
                    !removed,
                    "a displaced session (generation {}) must not deregister anything",
                    displaced
                );
            }

            let connections = state.connections.lock().await;
            let active = connections
                .get(&charge_point_id)
                .expect("the live connection must still be registered");
            prop_assert_eq!(active.generation, live_generation);
            Ok(())
        }).unwrap();
    }

    /// The live session's own cleanup does deregister it.
    #[test]
    fn prop_live_connection_deregisters_itself(charge_point_id in arb_charge_point_id()) {
        rt().block_on(async {
            let state = make_downstream_state();
            let generation =
                register_connection(&state, &charge_point_id, CancellationToken::new()).await;

            let removed = deregister_connection(&state, &charge_point_id, generation).await;
            prop_assert!(removed, "the current connection deregisters itself");
            prop_assert!(state.connections.lock().await.is_empty());
            Ok(())
        }).unwrap();
    }

    /// Distinct Charge Point IDs never displace one another.
    #[test]
    fn prop_distinct_ids_coexist(
        ids in proptest::collection::hash_set("[a-zA-Z0-9-]{1,12}", 1..6),
    ) {
        rt().block_on(async {
            let state = make_downstream_state();
            for id in &ids {
                register_connection(&state, id, CancellationToken::new()).await;
            }
            let connections = state.connections.lock().await;
            prop_assert_eq!(connections.len(), ids.len());
            for id in &ids {
                prop_assert!(connections.contains_key(id));
            }
            Ok(())
        }).unwrap();
    }
}
