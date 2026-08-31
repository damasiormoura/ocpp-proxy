//! Per-charger proxy session.
//!
//! A session owns *both* sockets for one Charge Point ID: the downstream
//! WebSocket from the charger and the upstream WebSocket to the Central
//! System. It reads from each and forwards to the other, which is the whole
//! point of the proxy.
//!
//! This replaces an earlier design in which charger messages were pushed onto
//! a global channel drained by a single main loop. That arrangement never
//! wrote to the upstream socket and never read from it, so nothing was ever
//! forwarded in either direction; it also serialised every charger behind one
//! loop, so a slow upstream connect stalled all of them.
//!
//! Requirements: 2.x, 3.x, 4.x, 5.6

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message as AxumMessage, WebSocket};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::error::ProxyError;
use crate::forwarder::{MessageForwarder, MessageSink, MqttEvent};
use crate::models::{ConnectionId, ConnectionState, Direction, ExponentialBackoff, OcppFrame};
use crate::state::ConnectionStateManager;
use crate::upstream::{build_upstream_url, connect_upstream};

/// WebSocket close code 1000 — normal closure.
pub const CLOSE_NORMAL: u16 = 1000;
/// WebSocket close code 1001 — going away. Sent to the charger when the
/// upstream reconnection window expires (Requirement 2.6).
pub const CLOSE_GOING_AWAY: u16 = 1001;

/// Everything a session needs that comes from configuration.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Base Central System URL; the Charge Point ID is appended to its path.
    pub central_system_url: url::Url,
    /// Local source address for the upstream socket, if egress must be
    /// selected by source address rather than by destination route.
    pub upstream_bind_address: Option<std::net::IpAddr>,
    /// Subprotocol to mirror upstream.
    pub subprotocol: String,
    /// Maximum messages buffered per direction.
    pub message_buffer_size: usize,
    /// Maximum age of a buffered message before it is discarded.
    pub max_buffer_duration: Duration,
    /// Upstream connect timeout.
    pub connect_timeout: Duration,
    /// Initial reconnection backoff.
    pub initial_backoff: Duration,
    /// Maximum reconnection backoff.
    pub max_backoff: Duration,
    /// How long to keep the charger connected while upstream is down.
    pub max_reconnect_window: Duration,
    /// Maximum age of a tracked Call awaiting its response.
    pub call_tracker_max_age: Duration,
}

/// Sends OCPP frames to the charger through the downstream writer task.
struct ChargerSink {
    tx: mpsc::Sender<AxumMessage>,
}

#[async_trait::async_trait]
impl MessageSink for ChargerSink {
    async fn send_raw(&mut self, raw: &str) -> Result<(), ProxyError> {
        self.tx
            .send(AxumMessage::Text(raw.to_string().into()))
            .await
            .map_err(|_| ProxyError::Forwarding {
                description: "Charger connection closed".to_string(),
            })
    }
}

/// Sends OCPP frames to the Central System.
struct UpstreamSink<'a> {
    sink: &'a mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, TungMessage>,
}

#[async_trait::async_trait]
impl MessageSink for UpstreamSink<'_> {
    async fn send_raw(&mut self, raw: &str) -> Result<(), ProxyError> {
        self.sink
            .send(TungMessage::Text(raw.to_string().into()))
            .await
            .map_err(|e| ProxyError::ConnectionUpstream {
                description: format!("Failed to send to Central System: {}", e),
            })
    }
}

/// Why a session's inner forwarding loop ended.
enum LoopOutcome {
    /// The charger went away — the session is over.
    ChargerGone,
    /// The upstream connection failed — reconnect and resume.
    UpstreamLost,
    /// Shutdown was requested.
    Shutdown,
}

/// Run one charger's proxy session to completion.
///
/// Returns when the charger disconnects, the upstream reconnection window
/// expires, or shutdown is requested.
#[allow(clippy::too_many_arguments)]
pub async fn run_session(
    charge_point_id: String,
    ws: WebSocket,
    config: Arc<SessionConfig>,
    state: Arc<Mutex<ConnectionStateManager>>,
    mqtt_tx: mpsc::Sender<MqttEvent>,
    cancel: CancellationToken,
) {
    let (ws_sink, mut ws_stream) = ws.split();

    // A dedicated writer task owns the charger sink, so forwarding upstream
    // and downstream never contend for it.
    let (charger_tx, charger_rx) = mpsc::channel::<AxumMessage>(256);
    let writer = tokio::spawn(charger_writer(ws_sink, charger_rx));

    let mut forwarder = MessageForwarder::with_charge_point_id(
        mqtt_tx.clone(),
        config.message_buffer_size,
        config.max_buffer_duration,
        config.call_tracker_max_age,
        charge_point_id.clone(),
    );

    let mut backoff = ExponentialBackoff::with_defaults(config.initial_backoff, config.max_backoff);
    let mut reconnecting_since: Option<tokio::time::Instant> = None;
    let mut close_code = CLOSE_NORMAL;

    'session: loop {
        // ---- connect (or reconnect) upstream ----
        set_upstream_state(
            &state,
            &mqtt_tx,
            &charge_point_id,
            ConnectionState::Connecting,
        )
        .await;

        let url = build_upstream_url(&config.central_system_url, &charge_point_id);
        let upstream = match connect_upstream(
            &url,
            &config.subprotocol,
            config.upstream_bind_address,
            config.connect_timeout,
        )
        .await
        {
            Ok(stream) => stream,
            Err(e) => {
                let started = *reconnecting_since.get_or_insert_with(tokio::time::Instant::now);
                let elapsed = started.elapsed();

                // Requirement 2.6 — give up after the window and tell the
                // charger to go away, rather than holding a connection that
                // cannot reach Mobi.e.
                if elapsed >= config.max_reconnect_window {
                    error!(
                        component = "session",
                        charge_point_id = %charge_point_id,
                        elapsed_secs = elapsed.as_secs(),
                        error = %e,
                        "Upstream unreachable for the full reconnection window; closing charger with 1001"
                    );
                    close_code = CLOSE_GOING_AWAY;
                    break 'session;
                }

                set_upstream_state(
                    &state,
                    &mqtt_tx,
                    &charge_point_id,
                    ConnectionState::Reconnecting,
                )
                .await;

                let delay = backoff.next_delay().min(
                    config
                        .max_reconnect_window
                        .saturating_sub(elapsed)
                        .max(Duration::from_millis(1)),
                );
                warn!(
                    component = "session",
                    charge_point_id = %charge_point_id,
                    error = %e,
                    retry_in_ms = delay.as_millis(),
                    "Upstream connection failed, will retry"
                );

                tokio::select! {
                    _ = tokio::time::sleep(delay) => continue 'session,
                    _ = cancel.cancelled() => break 'session,
                    // The charger hanging up during reconnection ends the
                    // session; there is nobody left to forward for.
                    next = ws_stream.next() => {
                        if matches!(next, None | Some(Err(_)) | Some(Ok(AxumMessage::Close(_)))) {
                            info!(
                                component = "session",
                                charge_point_id = %charge_point_id,
                                "Charger disconnected while upstream was down"
                            );
                            break 'session;
                        }
                        continue 'session;
                    }
                }
            }
        };

        info!(
            component = "session",
            charge_point_id = %charge_point_id,
            url = %url,
            "Upstream connected"
        );
        backoff.reset();
        reconnecting_since = None;
        set_upstream_state(
            &state,
            &mqtt_tx,
            &charge_point_id,
            ConnectionState::Connected,
        )
        .await;

        let (mut up_sink, mut up_stream) = upstream.split();

        // Requirement 4.5 — deliver messages buffered while upstream was down,
        // in order, before anything new.
        {
            let mut sink = UpstreamSink { sink: &mut up_sink };
            match forwarder.flush_upstream(&mut sink).await {
                Ok(0) => {}
                Ok(n) => {
                    info!(
                        component = "session",
                        charge_point_id = %charge_point_id,
                        count = n,
                        "Replayed buffered messages to Central System"
                    );
                    let mut mgr = state.lock().await;
                    for _ in 0..n {
                        mgr.record_forwarded(Direction::ChargerToCentral);
                    }
                }
                Err(e) => {
                    warn!(
                        component = "session",
                        charge_point_id = %charge_point_id,
                        error = %e,
                        "Failed replaying buffer; reconnecting"
                    );
                    continue 'session;
                }
            }
        }

        let outcome = forward_loop(
            &charge_point_id,
            &mut ws_stream,
            &mut up_sink,
            &mut up_stream,
            &charger_tx,
            &mut forwarder,
            &state,
            &cancel,
            &config,
        )
        .await;

        match outcome {
            LoopOutcome::ChargerGone => break 'session,
            LoopOutcome::Shutdown => break 'session,
            LoopOutcome::UpstreamLost => {
                // Requirement 3.7 — messages queued for a charger we can no
                // longer reach are useless; drop them and say how many.
                forwarder.discard_downstream_buffer();
                set_upstream_state(
                    &state,
                    &mqtt_tx,
                    &charge_point_id,
                    ConnectionState::Reconnecting,
                )
                .await;
                reconnecting_since.get_or_insert_with(tokio::time::Instant::now);
                let delay = backoff.next_delay();
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = cancel.cancelled() => break 'session,
                }
                continue 'session;
            }
        }
    }

    // ---- teardown ----
    let dropped = forwarder.upstream_buffer.len() + forwarder.downstream_buffer.len();
    if dropped > 0 {
        warn!(
            component = "session",
            charge_point_id = %charge_point_id,
            discarded = dropped,
            "Session ending with buffered messages still undelivered"
        );
        let mut mgr = state.lock().await;
        mgr.record_dropped(
            Direction::ChargerToCentral,
            forwarder.upstream_buffer.len() as u64,
        );
        mgr.record_dropped(
            Direction::CentralToCharger,
            forwarder.downstream_buffer.len() as u64,
        );
    }

    // Requirement 9.4 / 2.6 — a close frame, not a dropped socket.
    let _ = charger_tx
        .send(AxumMessage::Close(Some(CloseFrame {
            code: close_code,
            reason: "Proxy session ending".into(),
        })))
        .await;
    // Give the writer a moment to flush the close frame before it is dropped.
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(charger_tx);
    let _ = writer.await;

    set_upstream_state(
        &state,
        &mqtt_tx,
        &charge_point_id,
        ConnectionState::Disconnected,
    )
    .await;

    info!(
        component = "session",
        charge_point_id = %charge_point_id,
        "Session ended"
    );
}

/// The steady-state forwarding loop: charger ⇄ Central System.
#[allow(clippy::too_many_arguments)]
async fn forward_loop(
    charge_point_id: &str,
    ws_stream: &mut SplitStream<WebSocket>,
    up_sink: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, TungMessage>,
    up_stream: &mut SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    charger_tx: &mpsc::Sender<AxumMessage>,
    forwarder: &mut MessageForwarder,
    state: &Arc<Mutex<ConnectionStateManager>>,
    cancel: &CancellationToken,
    config: &SessionConfig,
) -> LoopOutcome {
    // Housekeeping the previous implementation declared but never ran: without
    // it the buffers ignore their age limit and the call tracker grows without
    // bound for the lifetime of the process.
    let mut housekeeping = tokio::time::interval(Duration::from_secs(10));
    housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // ---- charger → Central System ----
            incoming = ws_stream.next() => {
                match incoming {
                    Some(Ok(AxumMessage::Text(text))) => {
                        match OcppFrame::parse(&text) {
                            Ok(frame) => {
                                let mut sink = UpstreamSink { sink: up_sink };
                                match forwarder.forward_upstream(frame, &mut sink).await {
                                    Ok(()) => {
                                        state.lock().await
                                            .record_forwarded(Direction::ChargerToCentral);
                                    }
                                    Err(e) => {
                                        warn!(
                                            component = "session",
                                            charge_point_id = %charge_point_id,
                                            error = %e,
                                            "Upstream send failed; buffering and reconnecting"
                                        );
                                        // Re-parse to buffer: forward_upstream
                                        // consumed the frame.
                                        if let Ok(frame) = OcppFrame::parse(&text) {
                                            forwarder.buffer_upstream(frame);
                                        }
                                        return LoopOutcome::UpstreamLost;
                                    }
                                }
                            }
                            Err(e) => {
                                // Forwarded anyway: the proxy is meant to be
                                // invisible, and it is the Central System's
                                // job to reject a malformed frame with its own
                                // CallError. Dropping it here would make the
                                // proxy change the conversation.
                                warn!(
                                    component = "session",
                                    charge_point_id = %charge_point_id,
                                    error = %e,
                                    "Unparseable frame from charger; forwarding verbatim"
                                );
                                let mut sink = UpstreamSink { sink: up_sink };
                                if sink.send_raw(&text).await.is_err() {
                                    return LoopOutcome::UpstreamLost;
                                }
                            }
                        }
                    }
                    Some(Ok(AxumMessage::Binary(data))) => {
                        warn!(
                            component = "session",
                            charge_point_id = %charge_point_id,
                            len = data.len(),
                            "Binary frame from charger; OCPP 1.6J is text-only, ignoring"
                        );
                    }
                    Some(Ok(AxumMessage::Close(_))) | None => {
                        info!(
                            component = "session",
                            charge_point_id = %charge_point_id,
                            "Charger closed the connection"
                        );
                        return LoopOutcome::ChargerGone;
                    }
                    Some(Ok(_)) => { /* ping/pong handled by axum */ }
                    Some(Err(e)) => {
                        warn!(
                            component = "session",
                            charge_point_id = %charge_point_id,
                            error = %e,
                            "Charger connection error"
                        );
                        return LoopOutcome::ChargerGone;
                    }
                }
            }

            // ---- Central System → charger ----
            incoming = up_stream.next() => {
                match incoming {
                    Some(Ok(TungMessage::Text(text))) => {
                        let mut sink = ChargerSink { tx: charger_tx.clone() };
                        match OcppFrame::parse(&text) {
                            Ok(frame) => {
                                match forwarder.forward_downstream(frame, &mut sink).await {
                                    Ok(()) => {
                                        state.lock().await
                                            .record_forwarded(Direction::CentralToCharger);
                                    }
                                    Err(e) => {
                                        warn!(
                                            component = "session",
                                            charge_point_id = %charge_point_id,
                                            error = %e,
                                            "Charger send failed"
                                        );
                                        return LoopOutcome::ChargerGone;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(
                                    component = "session",
                                    charge_point_id = %charge_point_id,
                                    error = %e,
                                    "Unparseable frame from Central System; forwarding verbatim"
                                );
                                if sink.send_raw(&text).await.is_err() {
                                    return LoopOutcome::ChargerGone;
                                }
                            }
                        }
                    }
                    Some(Ok(TungMessage::Ping(data))) => {
                        if up_sink.send(TungMessage::Pong(data)).await.is_err() {
                            return LoopOutcome::UpstreamLost;
                        }
                    }
                    Some(Ok(TungMessage::Close(_))) | None => {
                        info!(
                            component = "session",
                            charge_point_id = %charge_point_id,
                            "Central System closed the connection"
                        );
                        return LoopOutcome::UpstreamLost;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        warn!(
                            component = "session",
                            charge_point_id = %charge_point_id,
                            error = %e,
                            "Upstream connection error"
                        );
                        return LoopOutcome::UpstreamLost;
                    }
                }
            }

            // ---- periodic housekeeping ----
            _ = housekeeping.tick() => {
                let expired = forwarder.evict_expired_messages();
                let stale_calls = forwarder.cleanup_expired_calls();
                if expired > 0 || stale_calls > 0 {
                    debug!(
                        component = "session",
                        charge_point_id = %charge_point_id,
                        expired_messages = expired,
                        stale_calls = stale_calls,
                        max_age_secs = config.max_buffer_duration.as_secs(),
                        "Housekeeping evicted stale entries"
                    );
                }
            }

            _ = cancel.cancelled() => {
                info!(
                    component = "session",
                    charge_point_id = %charge_point_id,
                    "Shutdown requested; ending session"
                );
                return LoopOutcome::Shutdown;
            }
        }
    }
}

/// Drains the charger channel into the charger's WebSocket.
async fn charger_writer(
    mut ws_sink: SplitSink<WebSocket, AxumMessage>,
    mut rx: mpsc::Receiver<AxumMessage>,
) {
    while let Some(msg) = rx.recv().await {
        let is_close = matches!(msg, AxumMessage::Close(_));
        if ws_sink.send(msg).await.is_err() {
            break;
        }
        if is_close {
            break;
        }
    }
    let _ = ws_sink.close().await;
}

/// Record an upstream state transition and publish the pair to MQTT.
///
/// Requirement 5.6 — a retained status message on every connection state
/// change. The `StateChange` variant existed but was never constructed, so
/// `ocpp/{id}/status` was never published at all.
async fn set_upstream_state(
    state: &Arc<Mutex<ConnectionStateManager>>,
    mqtt_tx: &mpsc::Sender<MqttEvent>,
    charge_point_id: &str,
    new_state: ConnectionState,
) {
    let (upstream, downstream) = {
        let mut mgr = state.lock().await;
        if mgr.upstream_state() == new_state {
            return;
        }
        mgr.transition(ConnectionId::Upstream, new_state);
        (mgr.upstream_state(), mgr.downstream_state())
    };

    publish_status(mqtt_tx, charge_point_id, upstream, downstream);
}

/// Publish a connection status change without blocking the forwarding path.
pub fn publish_status(
    mqtt_tx: &mpsc::Sender<MqttEvent>,
    charge_point_id: &str,
    upstream: ConnectionState,
    downstream: ConnectionState,
) {
    if let Err(e) = mqtt_tx.try_send(MqttEvent::StateChange {
        charge_point_id: charge_point_id.to_string(),
        upstream,
        downstream,
    }) {
        debug!(
            component = "session",
            charge_point_id = %charge_point_id,
            error = %e,
            "Could not queue status update; MQTT is not on the critical path"
        );
    }
}
