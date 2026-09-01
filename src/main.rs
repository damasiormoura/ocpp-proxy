//! OCPP 1.6J proxy — Autel charger ⇄ Mobi.e Central System, with MQTT for
//! Home Assistant.
//!
//! `main` is wiring only. Each charger's proxying happens in its own session
//! task (`session::run_session`), spawned by the downstream server when a
//! charger connects. There is no central message loop: an earlier design
//! funnelled every charger through one, which serialised them behind each
//! other and, as written, forwarded nothing at all.

// The binary uses the library crate rather than re-declaring every module.
// Declaring them here compiles the whole crate a second time and reports every
// item the binary happens not to call as dead code, which buries the warnings
// that matter.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tokio::time::Instant;
use tracing::{error, info, warn};
use url::Url;

use ocpp_proxy::config::ProxyConfig;
use ocpp_proxy::downstream::{self, DownstreamState};
use ocpp_proxy::forwarder::MqttEvent;
use ocpp_proxy::health::{serve_health, HealthState};
use ocpp_proxy::models::{ConnectionId, ConnectionState};
use ocpp_proxy::mqtt::MqttPublisher;
use ocpp_proxy::session::SessionConfig;
use ocpp_proxy::shutdown::{self, log_startup_begin, log_startup_complete, ShutdownCoordinator};
use ocpp_proxy::snapshot_store::SnapshotStore;
use ocpp_proxy::state::ConnectionStateManager;

/// Upstream connect timeout (Requirement 2.4).
const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Initial upstream reconnection backoff (Requirement 2.4).
const UPSTREAM_INITIAL_BACKOFF: Duration = Duration::from_secs(2);
/// How long the charger is held while upstream is down (Requirement 2.5/2.6).
const UPSTREAM_RECONNECT_WINDOW: Duration = Duration::from_secs(300);
/// Maximum time a message may sit in a buffer (Requirement 3.5).
const MAX_BUFFER_DURATION: Duration = Duration::from_secs(30);
/// Maximum age of a tracked Call awaiting its response.
const CALL_TRACKER_MAX_AGE: Duration = Duration::from_secs(300);
/// Startup budget for the first MQTT connection (Requirement 9.2).
const MQTT_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() {
    let startup_start = log_startup_begin();

    // ---- configuration: fail fast, before anything is bound ----
    let config = match ProxyConfig::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("FATAL: {}", e);
            std::process::exit(1);
        }
    };

    ocpp_proxy::logging::init_logging(&config.logging.level);

    let central_system_url = match Url::parse(&config.central_system_url) {
        Ok(url) => url,
        Err(e) => {
            eprintln!(
                "FATAL: [config] central_system_url is not a valid URL ('{}'): {}",
                config.central_system_url, e
            );
            std::process::exit(1);
        }
    };

    // Requirement 2.8 — a bind address that does not exist locally would send
    // Central System traffic out the default route instead, which cannot reach
    // an APN-only endpoint. Fail loudly rather than silently taking the wrong
    // path.
    if let Some(bind) = config.upstream_bind_address {
        if let Err(e) = verify_local_address(bind).await {
            eprintln!("FATAL: [config] upstream_bind_address {}: {}", bind, e);
            std::process::exit(1);
        }
    }

    info!(
        component = "main",
        listen_address = %config.listen_address,
        listen_port = config.listen_port,
        health_port = config.health_port,
        central_system_url = %config.central_system_url,
        charge_point_id = config.charge_point_id.as_deref().unwrap_or("-"),
        "OCPP Proxy starting"
    );

    // ---- shared state: ONE manager, used by health, downstream and sessions ----
    let state_manager = Arc::new(Mutex::new(ConnectionStateManager::new(64)));

    let (mqtt_tx, mqtt_rx) = mpsc::channel::<MqttEvent>(1000);

    let coordinator = ShutdownCoordinator::new();
    let shutdown_token = coordinator.token();

    // ---- MQTT publisher on its own OS thread ----
    // rumqttc's EventLoop is !Send, so it cannot live in the shared runtime.
    let mqtt_config = config.mqtt.clone();
    let mqtt_lwt_id = config.charge_point_id.clone();
    let mqtt_buffer_size = config.buffers.mqtt_buffer_size;
    // An empty path disables persistence; anything else is the snapshot file.
    let snapshot_store = if config.state_file.trim().is_empty() {
        info!(
            component = "main",
            "Snapshot persistence disabled; previous-session figures will not \
             survive a proxy restart"
        );
        SnapshotStore::disabled()
    } else {
        let store = SnapshotStore::new(Some(std::path::PathBuf::from(&config.state_file)));
        info!(
            component = "main",
            path = ?store.path(),
            "Charge point snapshot persisted across restarts"
        );
        store
    };
    debug_assert!(snapshot_store.is_enabled() || config.state_file.trim().is_empty());
    let state_for_mqtt = state_manager.clone();
    let mqtt_shutdown = shutdown_token.clone();

    let mqtt_handle = std::thread::Builder::new()
        .name("mqtt".to_string())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("FATAL: failed to create MQTT runtime: {}", e);
                    return;
                }
            };

            rt.block_on(async move {
                let mut publisher = match MqttPublisher::new(
                    &mqtt_config,
                    mqtt_lwt_id,
                    mqtt_rx,
                    mqtt_buffer_size,
                    snapshot_store,
                ) {
                    Ok(p) => p,
                    Err(e) => {
                        error!(component = "mqtt", error = %e, "MQTT publisher unavailable; \
                               proxying continues without Home Assistant visibility");
                        return;
                    }
                };

                // Requirement 9.2/9.3 — bounded attempt, then carry on
                // regardless. MQTT must never gate charger service.
                //
                // Raced against shutdown: a signal arriving during the startup
                // connect must not be blocked behind the remainder of the
                // 10-second attempt. The tokio runtime waits for blocking
                // tasks on drop, so a join timeout in `main` cannot rescue us
                // here — the thread has to notice for itself.
                let connected = tokio::select! {
                    result = publisher.try_connect(MQTT_STARTUP_TIMEOUT) => {
                        result.unwrap_or(false)
                    }
                    _ = mqtt_shutdown.cancelled() => {
                        info!(
                            component = "mqtt",
                            "Shutdown during startup connect; abandoning MQTT"
                        );
                        return;
                    }
                };
                {
                    let mut mgr = state_for_mqtt.lock().await;
                    mgr.transition(
                        ConnectionId::Mqtt,
                        if connected {
                            ConnectionState::Connected
                        } else {
                            ConnectionState::Reconnecting
                        },
                    );
                }

                // `run` returns when every sender is dropped. The token is a
                // backstop for the case where one is still held.
                tokio::select! {
                    _ = publisher.run() => {}
                    _ = mqtt_shutdown.cancelled() => {
                        info!(component = "mqtt", "Shutdown requested; stopping publisher");
                    }
                }

                let mut mgr = state_for_mqtt.lock().await;
                mgr.transition(ConnectionId::Mqtt, ConnectionState::Disconnected);
            });
        })
        .expect("failed to spawn MQTT thread");

    // ---- health server: shares the live state manager ----
    let health_state = Arc::new(HealthState {
        connection_manager: state_manager.clone(),
        start_time: Instant::now(),
    });
    // Bind before announcing readiness. A port clash is a configuration
    // mistake, and discovering it here — rather than from inside a spawned
    // task after startup has already logged success — is the difference
    // between an operator seeing it and Home Assistant silently losing its
    // health signal.
    let health_addr = SocketAddr::from(([0, 0, 0, 0], config.health_port));
    let health_handle = match tokio::net::TcpListener::bind(health_addr).await {
        Ok(listener) => {
            info!(component = "main", addr = %health_addr, "Health endpoint listening");
            Some(tokio::spawn(async move {
                if let Err(e) = serve_health(listener, health_state).await {
                    error!(component = "main", error = %e, "Health check server failed");
                }
            }))
        }
        Err(e) => {
            // Not fatal: charging matters more than monitoring, and refusing
            // to start would stop the charger over a monitoring problem. But
            // it must be unmistakable in the log.
            error!(
                component = "main",
                addr = %health_addr,
                error = %e,
                "COULD NOT BIND THE HEALTH ENDPOINT — the proxy will serve chargers \
                 but Home Assistant will have no health signal. Fix health_port."
            );
            None
        }
    };

    // ---- downstream server ----
    let session_config = Arc::new(SessionConfig {
        central_system_url,
        upstream_bind_address: config.upstream_bind_address,
        subprotocol: downstream::OCPP16_SUBPROTOCOL.to_string(),
        message_buffer_size: config.buffers.message_buffer_size,
        max_buffer_duration: MAX_BUFFER_DURATION,
        connect_timeout: UPSTREAM_CONNECT_TIMEOUT,
        initial_backoff: UPSTREAM_INITIAL_BACKOFF,
        // Requirement 7.4a — the configured maximum actually applies, rather
        // than the value being parsed and then ignored.
        max_backoff: Duration::from_secs(config.buffers.max_backoff_seconds),
        max_reconnect_window: UPSTREAM_RECONNECT_WINDOW,
        call_tracker_max_age: CALL_TRACKER_MAX_AGE,
    });

    let downstream_state = DownstreamState {
        connections: Arc::new(Mutex::new(HashMap::new())),
        state_manager: state_manager.clone(),
        session_config,
        mqtt_tx: mqtt_tx.clone(),
        shutdown: shutdown_token.clone(),
        generation: Arc::new(AtomicU64::new(1)),
    };

    let listen_addr = SocketAddr::new(config.listen_address, config.listen_port);
    let listener = match tokio::net::TcpListener::bind(listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(
                component = "main",
                addr = %listen_addr,
                error = %e,
                "Cannot bind the charger listener; the proxy cannot serve"
            );
            std::process::exit(1);
        }
    };

    // Bound and accepting: health may now report `idle` rather than
    // `unhealthy`.
    state_manager.lock().await.set_listener_bound(true);

    info!(
        component = "main",
        addr = %listen_addr,
        "Listening for charger connections"
    );

    let router = downstream::create_router(downstream_state.clone());
    let server_shutdown = shutdown_token.clone();
    let downstream_handle = tokio::spawn(async move {
        let served = axum::serve(listener, router)
            .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
            .await;
        if let Err(e) = served {
            error!(component = "main", error = %e, "Downstream server failed");
        }
    });

    // ---- signals ----
    let coordinator_for_signal = coordinator.clone();
    tokio::spawn(async move {
        let signal = shutdown::wait_for_shutdown_signal().await;
        info!(
            component = "main",
            signal = signal,
            "Shutdown signal received"
        );
        coordinator_for_signal.initiate_shutdown();
    });

    log_startup_complete(startup_start);

    // ---- wait for shutdown ----
    shutdown_token.cancelled().await;

    info!(component = "main", "Draining");
    state_manager.lock().await.set_listener_bound(false);

    // Sessions observe the cancelled token, send their close frames and
    // unwind. Requirement 9.1 bounds how long we wait for them.
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let remaining = downstream_state.connections.lock().await.len();
        if remaining == 0 {
            info!(component = "main", "All sessions closed cleanly");
            break;
        }
        if tokio::time::Instant::now() >= drain_deadline {
            warn!(
                component = "main",
                sessions = remaining,
                "Drain timeout reached; abandoning remaining sessions"
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ---- stop MQTT ----
    //
    // Every sender clone must be gone before the publisher's channel closes.
    // `downstream_state` holds one and each session holds another; dropping
    // only the local handle leaves the receiver open forever and the join
    // below never returns — which is how an earlier revision turned SIGTERM
    // into a hang and, eventually, a SIGKILL.
    drop(mqtt_tx);
    drop(downstream_state);

    // A plain join, not a timeout: the runtime waits for blocking tasks when
    // it is dropped, so abandoning the join would not actually let the process
    // exit any sooner — it would only produce a misleading "exiting anyway"
    // log line while the runtime blocked anyway. The thread observes the same
    // shutdown token, so it returns promptly.
    match tokio::task::spawn_blocking(move || mqtt_handle.join()).await {
        Ok(_) => info!(component = "main", "MQTT publisher stopped"),
        Err(e) => warn!(component = "main", error = %e, "MQTT thread join failed"),
    }

    downstream_handle.abort();
    if let Some(handle) = health_handle {
        handle.abort();
    }

    info!(component = "main", "OCPP Proxy shutdown complete");
}

/// Confirm an address is present on some local interface.
async fn verify_local_address(addr: std::net::IpAddr) -> Result<(), String> {
    // Binding port 0 succeeds only if the address belongs to this host, which
    // is exactly the question, and needs no privileges.
    match tokio::net::TcpSocket::new_v4() {
        Ok(_) => {}
        Err(e) => return Err(format!("cannot create socket: {}", e)),
    }
    let socket = if addr.is_ipv4() {
        tokio::net::TcpSocket::new_v4()
    } else {
        tokio::net::TcpSocket::new_v6()
    }
    .map_err(|e| format!("cannot create socket: {}", e))?;

    socket
        .bind(SocketAddr::new(addr, 0))
        .map_err(|e| format!("is not an address on any local interface ({})", e))?;
    Ok(())
}
