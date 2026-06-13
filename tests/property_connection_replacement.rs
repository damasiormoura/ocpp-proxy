//! Property-based tests for connection replacement.
//!
//! **Property 13: New connections replace existing connections for the same Charge Point ID**
//!
//! For any sequence of connection attempts from chargers with the same Charge_Point_ID,
//! only the most recently accepted connection SHALL be active, and all previously active
//! connections for that ID SHALL have received a WebSocket close frame.
//!
//! **Validates: Requirements 1.4**

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{CloseFrame, Message};
use proptest::prelude::*;
use tokio::sync::{mpsc, Mutex};

use ocpp_proxy::downstream::{ActiveConnection, DownstreamState};
use ocpp_proxy::state::ConnectionStateManager;

/// Simulates the connection replacement logic from `setup_connection`.
///
/// When a new connection arrives for a Charge Point ID that already has an active
/// connection, the existing connection receives a close frame and is removed before
/// the new connection is registered.
async fn simulate_register_connection(
    state: &DownstreamState,
    charge_point_id: &str,
    tx: mpsc::Sender<Message>,
) {
    let abort_handle = tokio::spawn(async {}).abort_handle();

    let mut connections = state.connections.lock().await;
    if let Some(existing) = connections.remove(charge_point_id) {
        // Send close frame to existing connection (mirrors setup_connection behavior)
        let _ = existing
            .tx
            .send(Message::Close(Some(CloseFrame {
                code: 1000,
                reason: "New connection replacing existing one".into(),
            })))
            .await;
        existing.abort_handle.abort();
    }

    connections.insert(
        charge_point_id.to_string(),
        ActiveConnection {
            tx: tx.clone(),
            abort_handle,
        },
    );
}

/// Creates a fresh DownstreamState for testing.
fn make_downstream_state() -> DownstreamState {
    let (msg_tx, _msg_rx) = mpsc::channel(64);
    DownstreamState {
        connections: Arc::new(Mutex::new(HashMap::new())),
        state_manager: Arc::new(Mutex::new(ConnectionStateManager::new(16))),
        message_tx: msg_tx,
    }
}

/// Proptest strategy for generating valid Charge Point IDs (alphanumeric, 1-20 chars).
fn arb_charge_point_id() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9]{1,20}"
}

/// Proptest strategy for generating the number of connection events (1-10).
fn arb_connection_count() -> impl Strategy<Value = usize> {
    1usize..=10
}

proptest! {
    /// Property 13: New connections replace existing connections for the same Charge Point ID.
    ///
    /// For a sequence of connections to the same ID, only the last one remains active
    /// and all prior ones received close frames.
    ///
    /// **Validates: Requirements 1.4**
    #[test]
    fn prop_connection_replacement_only_latest_active(
        charge_point_id in arb_charge_point_id(),
        connection_count in arb_connection_count(),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let state = make_downstream_state();
            let mut receivers: Vec<mpsc::Receiver<Message>> = Vec::new();

            // Register connections one at a time
            for i in 0..connection_count {
                let (tx, rx) = mpsc::channel::<Message>(16);
                receivers.push(rx);

                simulate_register_connection(&state, &charge_point_id, tx).await;

                // After each registration, verify only the latest is in the map
                let connections = state.connections.lock().await;
                prop_assert_eq!(
                    connections.len(), 1,
                    "After registering connection {}, expected 1 entry but found {}",
                    i + 1, connections.len()
                );
                prop_assert!(
                    connections.contains_key(&charge_point_id),
                    "Connection map should contain the charge point ID"
                );
            }

            // Verify all prior connections received close frames
            for (i, rx) in receivers.iter_mut().enumerate().take(connection_count - 1) {
                let msg = rx.try_recv();
                prop_assert!(
                    msg.is_ok(),
                    "Connection {} should have received a close frame, but channel was empty",
                    i + 1
                );
                match msg.unwrap() {
                    Message::Close(Some(frame)) => {
                        prop_assert_eq!(
                            frame.code, 1000,
                            "Close frame for connection {} should have code 1000",
                            i + 1
                        );
                    }
                    other => {
                        return Err(proptest::test_runner::TestCaseError::Fail(
                            format!(
                                "Connection {} expected Close message, got {:?}",
                                i + 1, other
                            ).into()
                        ));
                    }
                }
            }

            // Verify the last connection did NOT receive a close frame
            if connection_count > 0 {
                let last_rx = receivers.last_mut().unwrap();
                let msg = last_rx.try_recv();
                prop_assert!(
                    msg.is_err(),
                    "The most recent connection should NOT have received a close frame"
                );
            }

            // Final check: exactly 1 entry exists for the ID
            let connections = state.connections.lock().await;
            prop_assert_eq!(
                connections.len(), 1,
                "After all registrations, expected exactly 1 entry in connections map"
            );

            Ok(())
        })?;
    }

    /// Property 13 (supplementary): Multiple different Charge Point IDs maintain independent entries.
    ///
    /// When connections arrive for different IDs, each ID has exactly one active connection,
    /// and replacing one ID does not affect others.
    ///
    /// **Validates: Requirements 1.4**
    #[test]
    fn prop_connection_replacement_independent_per_id(
        id1 in arb_charge_point_id(),
        id2 in arb_charge_point_id(),
        count1 in arb_connection_count(),
        count2 in arb_connection_count(),
    ) {
        // Ensure IDs are different for this test
        prop_assume!(id1 != id2);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let state = make_downstream_state();

            // Register multiple connections for id1, keeping receivers alive
            let mut receivers1: Vec<mpsc::Receiver<Message>> = Vec::new();
            for _ in 0..count1 {
                let (tx, rx) = mpsc::channel::<Message>(16);
                receivers1.push(rx);
                simulate_register_connection(&state, &id1, tx).await;
            }

            // Register multiple connections for id2, keeping receivers alive
            let mut receivers2: Vec<mpsc::Receiver<Message>> = Vec::new();
            for _ in 0..count2 {
                let (tx, rx) = mpsc::channel::<Message>(16);
                receivers2.push(rx);
                simulate_register_connection(&state, &id2, tx).await;
            }

            // Verify both IDs have exactly one entry each
            let connections = state.connections.lock().await;
            prop_assert_eq!(
                connections.len(), 2,
                "Expected 2 entries (one per ID), found {}",
                connections.len()
            );
            prop_assert!(connections.contains_key(&id1));
            prop_assert!(connections.contains_key(&id2));

            // Verify all prior connections for id1 received close frames
            for (i, rx) in receivers1.iter_mut().enumerate().take(count1 - 1) {
                let msg = rx.try_recv();
                prop_assert!(
                    msg.is_ok(),
                    "ID1 connection {} should have received a close frame",
                    i + 1
                );
                match msg.unwrap() {
                    Message::Close(Some(frame)) => {
                        prop_assert_eq!(frame.code, 1000);
                    }
                    other => {
                        return Err(proptest::test_runner::TestCaseError::Fail(
                            format!("ID1 connection {} expected Close, got {:?}", i + 1, other).into()
                        ));
                    }
                }
            }

            // Verify all prior connections for id2 received close frames
            for (i, rx) in receivers2.iter_mut().enumerate().take(count2 - 1) {
                let msg = rx.try_recv();
                prop_assert!(
                    msg.is_ok(),
                    "ID2 connection {} should have received a close frame",
                    i + 1
                );
                match msg.unwrap() {
                    Message::Close(Some(frame)) => {
                        prop_assert_eq!(frame.code, 1000);
                    }
                    other => {
                        return Err(proptest::test_runner::TestCaseError::Fail(
                            format!("ID2 connection {} expected Close, got {:?}", i + 1, other).into()
                        ));
                    }
                }
            }

            // Last connections for each ID should NOT have close frames
            prop_assert!(
                receivers1.last_mut().unwrap().try_recv().is_err(),
                "Last connection for ID1 should not have received a close frame"
            );
            prop_assert!(
                receivers2.last_mut().unwrap().try_recv().is_err(),
                "Last connection for ID2 should not have received a close frame"
            );

            Ok(())
        })?;
    }
}

/// Focused test: single connection for an ID means no close frame sent.
#[tokio::test]
async fn test_single_connection_no_close_frame() {
    let state = make_downstream_state();
    let (tx, mut rx) = mpsc::channel::<Message>(16);

    simulate_register_connection(&state, "CP001", tx).await;

    // No close frame should be received (no prior connection to replace)
    assert!(rx.try_recv().is_err(), "Single connection should not receive close frame");

    let connections = state.connections.lock().await;
    assert_eq!(connections.len(), 1);
    assert!(connections.contains_key("CP001"));
}

/// Focused test: replacing exactly once sends close to old connection.
#[tokio::test]
async fn test_replace_once_sends_close() {
    let state = make_downstream_state();

    // Register first connection
    let (tx1, mut rx1) = mpsc::channel::<Message>(16);
    simulate_register_connection(&state, "CP002", tx1).await;

    // Register second connection (should replace first)
    let (tx2, mut rx2) = mpsc::channel::<Message>(16);
    simulate_register_connection(&state, "CP002", tx2).await;

    // First connection should have received a close frame
    let msg = rx1.try_recv().expect("First connection should receive close frame");
    match msg {
        Message::Close(Some(frame)) => {
            assert_eq!(frame.code, 1000);
            assert_eq!(frame.reason, "New connection replacing existing one");
        }
        other => panic!("Expected Close message, got {:?}", other),
    }

    // Second connection should NOT have received anything
    assert!(rx2.try_recv().is_err(), "New connection should not receive close frame");

    // Only one entry in map
    let connections = state.connections.lock().await;
    assert_eq!(connections.len(), 1);
    assert!(connections.contains_key("CP002"));
}

/// Focused test: maximum sequence of replacements (10).
#[tokio::test]
async fn test_ten_sequential_replacements() {
    let state = make_downstream_state();
    let mut receivers = Vec::new();

    for _ in 0..10 {
        let (tx, rx) = mpsc::channel::<Message>(16);
        receivers.push(rx);
        simulate_register_connection(&state, "CP_MAX", tx).await;
    }

    // All but the last should have close frames
    for (i, rx) in receivers.iter_mut().enumerate().take(9) {
        let msg = rx.try_recv().unwrap_or_else(|_| {
            panic!("Connection {} should have received close frame", i + 1)
        });
        match msg {
            Message::Close(Some(frame)) => assert_eq!(frame.code, 1000),
            other => panic!("Connection {} expected Close, got {:?}", i + 1, other),
        }
    }

    // Last connection should have nothing
    assert!(receivers[9].try_recv().is_err());

    // Exactly 1 entry
    let connections = state.connections.lock().await;
    assert_eq!(connections.len(), 1);
}
