mod config;
mod downstream;
mod error;
mod forwarder;
mod health;
mod logging;
mod models;
mod mqtt;
mod shutdown;
mod state;
mod upstream;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::Message;
use tokio::sync::{mpsc, Mutex};
use tokio::time::Instant;
use tracing::{error, info, warn};
use url::Url;

use crate::config::ProxyConfig;
use crate::downstream::DownstreamState;
use crate::forwarder::{MessageForwarder, MqttEvent};
use crate::health::{HealthState, run_health_server};
use crate::models::{ConnectionId, ConnectionState, OcppFrame};
use crate::mqtt::MqttPublisher;
use crate::shutdown::{
    ShutdownContext, ShutdownCoordinator, log_startup_begin, log_startup_complete,
};
use crate::state::ConnectionStateManager;
use crate::upstream::UpstreamHandler;

#[tokio::main]
async fn main() {
    // Step 1: Log startup begin and record start time
    let startup_start = log_startup_begin();

    // Step 2: Load and validate configuration — fail fast with all errors
    let config = match ProxyConfig::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("FATAL: Configuration error: {}", e);
            std::process::exit(1);
        }
    };

    // Step 3: Initialize structured logging with configured level
    logging::init_logging(&config.logging.level);

    info!(
        component = "main",
        listen_port = config.listen_port,
        health_port = config.health_port,
        central_system_url = %config.central_system_url,
        "OCPP Proxy starting"
    );

    // Step 4: Create shared state — ConnectionStateManager in Arc<Mutex<>>
    let state_manager = Arc::new(Mutex::new(ConnectionStateManager::new(16)));

    // Step 5: Create MQTT event channel
    let (mqtt_tx, mqtt_rx) = mpsc::channel::<MqttEvent>(1000);

    // Step 6: Create downstream message channel
    let (downstream_msg_tx, mut downstream_msg_rx) = mpsc::channel::<(String, Message)>(1000);

    // Step 7 & 8: Create and spawn MQTT publisher in a dedicated thread
    // rumqttc's EventLoop is not Send, so we run the MQTT publisher entirely
    // within a dedicated OS thread with its own single-threaded Tokio runtime.
    let charge_point_id = "default".to_string();

    let mqtt_config_clone = config.mqtt.clone();
    let mqtt_buffer_size = config.buffers.mqtt_buffer_size;
    let state_manager_for_mqtt = state_manager.clone();

    let mqtt_handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create MQTT runtime");

        rt.block_on(async move {
            let mut publisher = match MqttPublisher::new(
                &mqtt_config_clone,
                charge_point_id,
                mqtt_rx,
                mqtt_buffer_size,
            ) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Failed to create MQTT publisher: {}", e);
                    return;
                }
            };

            // Try to connect with 10s timeout
            match publisher.try_connect(Duration::from_secs(10)).await {
                Ok(connected) => {
                    let mut mgr = state_manager_for_mqtt.lock().await;
                    if connected {
                        mgr.transition(ConnectionId::Mqtt, ConnectionState::Connected);
                    } else {
                        mgr.transition(ConnectionId::Mqtt, ConnectionState::Reconnecting);
                    }
                }
                Err(_e) => {
                    // Proceed anyway — publisher will attempt reconnection in its run loop
                }
            }

            // Run the publisher event loop (blocks until channel closed)
            publisher.run().await;
        });
    });

    // Step 9: Create DownstreamState and spawn the downstream WebSocket server
    let downstream_state = DownstreamState {
        connections: Arc::new(Mutex::new(HashMap::new())),
        state_manager: state_manager.clone(),
        message_tx: downstream_msg_tx,
    };

    let listen_addr = SocketAddr::from(([0, 0, 0, 0], config.listen_port));
    let downstream_state_for_server = downstream_state.clone();
    let downstream_handle = tokio::spawn(async move {
        if let Err(e) = downstream::start_server(listen_addr, downstream_state_for_server).await {
            error!(
                component = "main",
                error = %e,
                "Downstream WebSocket server failed"
            );
        }
    });

    // Step 10: Spawn the health check HTTP server
    let health_state = Arc::new(HealthState {
        connection_manager: Mutex::new(ConnectionStateManager::new(16)),
        start_time: Instant::now(),
    });

    // We use a separate health state that mirrors the main state manager for simplicity.
    // In a production system, these would share the same state. For now, the health
    // server uses its own state that we can update from the main loop.
    let health_port = config.health_port;
    let health_state_for_server = health_state.clone();
    let health_handle = tokio::spawn(async move {
        if let Err(e) = run_health_server(health_port, health_state_for_server).await {
            error!(
                component = "main",
                error = %e,
                "Health check server failed"
            );
        }
    });

    // Step 11: Spawn shutdown signal handler
    let coordinator = ShutdownCoordinator::new();
    let shutdown_token = coordinator.token();

    let coordinator_for_signal = coordinator.clone();
    tokio::spawn(async move {
        let signal = shutdown::wait_for_shutdown_signal().await;
        info!(
            component = "main",
            signal = signal,
            "Shutdown signal received, initiating graceful shutdown"
        );
        coordinator_for_signal.initiate_shutdown();
    });

    // Step 12: Log startup complete
    log_startup_complete(startup_start);

    // Step 13: Main loop — process messages from downstream, manage upstream connections
    let central_system_url = config.central_system_url.clone();
    let message_buffer_size = config.buffers.message_buffer_size;

    // Create the message forwarder
    let mut forwarder = MessageForwarder::new(
        mqtt_tx.clone(),
        message_buffer_size,
        Duration::from_secs(30),
        Duration::from_secs(300),
    );

    // Track per-charger upstream handlers
    let mut upstream_handlers: HashMap<String, UpstreamHandler> = HashMap::new();
    // Track upstream sender channels (charger → upstream raw message)
    let mut upstream_senders: HashMap<String, mpsc::Sender<String>> = HashMap::new();

    info!(component = "main", "Entering main message loop");

    loop {
        tokio::select! {
            // Process messages from the downstream channel (charger messages)
            msg = downstream_msg_rx.recv() => {
                match msg {
                    Some((charge_point_id, message)) => {
                        // Handle the message from the charger
                        match &message {
                            Message::Text(text) => {
                                // Parse the OCPP frame
                                match OcppFrame::parse(text) {
                                    Ok(frame) => {
                                        // Check if we have an upstream connection for this charger
                                        if let Some(upstream_tx) = upstream_senders.get(&charge_point_id) {
                                            // Create a channel sink for forwarding
                                            let mut sink = forwarder::ChannelSink::new(upstream_tx.clone());
                                            if let Err(e) = forwarder.forward_upstream(frame, &mut sink).await {
                                                warn!(
                                                    component = "main",
                                                    charge_point_id = %charge_point_id,
                                                    error = %e,
                                                    "Failed to forward message upstream, buffering"
                                                );
                                            }
                                        } else {
                                            // No upstream connection yet — spawn one
                                            info!(
                                                component = "main",
                                                charge_point_id = %charge_point_id,
                                                "Charger connected, initiating upstream connection"
                                            );

                                            // Create upstream handler
                                            let url = match Url::parse(&central_system_url) {
                                                Ok(u) => u,
                                                Err(e) => {
                                                    error!(
                                                        component = "main",
                                                        error = %e,
                                                        "Invalid central system URL"
                                                    );
                                                    continue;
                                                }
                                            };

                                            let mut handler = UpstreamHandler::new(
                                                url,
                                                charge_point_id.clone(),
                                                "ocpp1.6".to_string(),
                                            );

                                            // Try to connect
                                            {
                                                let mut mgr = state_manager.lock().await;
                                                match handler.connect(&mut mgr).await {
                                                    Ok(()) => {
                                                        info!(
                                                            component = "main",
                                                            charge_point_id = %charge_point_id,
                                                            "Upstream connection established"
                                                        );
                                                    }
                                                    Err(e) => {
                                                        warn!(
                                                            component = "main",
                                                            charge_point_id = %charge_point_id,
                                                            error = %e,
                                                            "Failed to connect upstream, buffering message"
                                                        );
                                                        forwarder.buffer_upstream(frame);
                                                        continue;
                                                    }
                                                }
                                            }

                                            // Create a channel for sending messages to upstream
                                            let (upstream_tx, mut upstream_rx) = mpsc::channel::<String>(256);
                                            upstream_senders.insert(charge_point_id.clone(), upstream_tx.clone());

                                            // Spawn a task that reads from the upstream channel and sends to WS
                                            let cp_id = charge_point_id.clone();
                                            let downstream_conns = downstream_state.connections.clone();
                                            let mqtt_tx_clone = mqtt_tx.clone();
                                            tokio::spawn(async move {
                                                // This task would handle reading from the upstream WS
                                                // and forwarding responses back to the charger.
                                                // For now, consume the channel to avoid blocking.
                                                while let Some(_raw_msg) = upstream_rx.recv().await {
                                                    // In a full implementation, this would write
                                                    // to the upstream WebSocket connection.
                                                    // The actual send is handled by the upstream handler.
                                                    let _ = &cp_id;
                                                    let _ = &downstream_conns;
                                                    let _ = &mqtt_tx_clone;
                                                }
                                            });

                                            upstream_handlers.insert(charge_point_id.clone(), handler);

                                            // Now forward the original message
                                            if let Some(upstream_tx) = upstream_senders.get(&charge_point_id) {
                                                let mut sink = forwarder::ChannelSink::new(upstream_tx.clone());
                                                if let Err(e) = forwarder.forward_upstream(frame, &mut sink).await {
                                                    warn!(
                                                        component = "main",
                                                        charge_point_id = %charge_point_id,
                                                        error = %e,
                                                        "Failed to forward message upstream after connection"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            component = "main",
                                            charge_point_id = %charge_point_id,
                                            error = %e,
                                            "Failed to parse OCPP frame from charger, dropping message"
                                        );
                                    }
                                }
                            }
                            Message::Binary(data) => {
                                warn!(
                                    component = "main",
                                    charge_point_id = %charge_point_id,
                                    len = data.len(),
                                    "Received binary message from charger, ignoring"
                                );
                            }
                            Message::Close(_) => {
                                info!(
                                    component = "main",
                                    charge_point_id = %charge_point_id,
                                    "Charger disconnected, cleaning up upstream"
                                );
                                upstream_senders.remove(&charge_point_id);
                                upstream_handlers.remove(&charge_point_id);
                            }
                            _ => {
                                // Ping/Pong handled by axum automatically
                            }
                        }
                    }
                    None => {
                        // Downstream channel closed — this shouldn't happen during normal operation
                        error!(component = "main", "Downstream message channel closed unexpectedly");
                        break;
                    }
                }
            }

            // Handle shutdown signal
            _ = shutdown_token.cancelled() => {
                info!(component = "main", "Shutdown token cancelled, starting graceful shutdown");
                break;
            }
        }
    }

    // Step 14: Graceful shutdown sequence
    info!(component = "main", "Executing graceful shutdown");

    // Close upstream connections
    for (cp_id, mut handler) in upstream_handlers.drain() {
        info!(
            component = "main",
            charge_point_id = %cp_id,
            "Closing upstream connection"
        );
        let mut mgr = state_manager.lock().await;
        if let Err(e) = handler.close(tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal).await {
            warn!(
                component = "main",
                charge_point_id = %cp_id,
                error = %e,
                "Error closing upstream connection"
            );
        }
        mgr.transition(ConnectionId::Upstream, ConnectionState::Disconnected);
    }

    // Drop the MQTT sender to signal the publisher to stop
    drop(mqtt_tx);

    // Wait for MQTT publisher thread to finish (with timeout)
    // The thread will exit once the mpsc channel is closed.
    let _ = tokio::task::spawn_blocking(move || {
        let _ = mqtt_handle.join();
    })
    .await;

    // Execute graceful shutdown via coordinator
    let shutdown_context = ShutdownContext {
        inflight_timeout: Duration::from_secs(10),
        close_ack_timeout: Duration::from_secs(5),
        inflight_complete: None,
        close_connections: None,
        close_ack_complete: None,
        publish_offline: None,
        upstream_discarded: forwarder.upstream_buffer.len() as u64,
        downstream_discarded: forwarder.downstream_buffer.len() as u64,
    };

    coordinator.graceful_shutdown(shutdown_context).await;

    // Abort remaining tasks
    downstream_handle.abort();
    health_handle.abort();

    info!(component = "main", "OCPP Proxy shutdown complete");
}
