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

use crate::command::{
    self, CommandRequest, CommandResult, CommandStatus, InFlight, LocalCallQueue,
    CENTRAL_CALL_GRACE, COMMAND_CHANNEL_CAPACITY,
};
use crate::config::ChargingConfig;
use crate::error::ProxyError;
use crate::forwarder::{MessageForwarder, MessageSink, MqttEvent};
use crate::models::{
    ConnectionId, ConnectionState, Direction, ExponentialBackoff, OcppFrame, OcppMessageType,
};
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
    /// Proxy-originated charger commands. Only the timeout and the profile
    /// shape are read here; whether commands arrive at all is decided by the
    /// MQTT publisher, which subscribes only when `charging.enabled` is set.
    pub charging: ChargingConfig,
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
    mut command_rx: mpsc::Receiver<CommandRequest>,
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

    // Proxy-originated Calls live for the whole session, across upstream
    // reconnects: the charger connection they travel on is the same one.
    let mut local_calls =
        LocalCallQueue::new(config.charging.command_timeout(), COMMAND_CHANNEL_CAPACITY);
    let mut commands_open = true;

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
            &mqtt_tx,
            &mut command_rx,
            &mut commands_open,
            &mut local_calls,
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
    // Nothing the proxy was asked can be delivered now; say so rather than
    // leaving the caller waiting on a result that never comes.
    let (queued, in_flight) = local_calls.drain();
    for request in queued {
        emit_command_result(
            &mqtt_tx,
            &charge_point_id,
            CommandResult::for_request(
                &request,
                CommandStatus::NotConnected,
                Some("charger session ended before the command was sent".to_string()),
            ),
        );
    }
    if let Some(in_flight) = in_flight {
        emit_command_result(
            &mqtt_tx,
            &charge_point_id,
            CommandResult::for_in_flight(
                &in_flight,
                CommandStatus::Timeout,
                None,
                Some("charger session ended before it answered".to_string()),
            ),
        );
    }
    while let Ok(request) = command_rx.try_recv() {
        emit_command_result(
            &mqtt_tx,
            &charge_point_id,
            CommandResult::for_request(
                &request,
                CommandStatus::NotConnected,
                Some("charger session ended before the command was sent".to_string()),
            ),
        );
    }

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
    mqtt_tx: &mpsc::Sender<MqttEvent>,
    command_rx: &mut mpsc::Receiver<CommandRequest>,
    commands_open: &mut bool,
    local: &mut LocalCallQueue,
) -> LoopOutcome {
    // Housekeeping the previous implementation declared but never ran: without
    // it the buffers ignore their age limit and the call tracker grows without
    // bound for the lifetime of the process.
    let mut housekeeping = tokio::time::interval(Duration::from_secs(10));
    housekeeping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Proxy commands time out on their own clock, finer than housekeeping's,
    // so a controller learns of an unanswered Call within a second of the
    // deadline rather than up to ten later.
    let mut command_tick = tokio::time::interval(Duration::from_secs(1));
    command_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // ---- charger → Central System ----
            incoming = ws_stream.next() => {
                match incoming {
                    Some(Ok(AxumMessage::Text(text))) => {
                        match OcppFrame::parse(&text) {
                            Ok(frame) => {
                                // The charger answering a question the proxy asked.
                                // Consumed here: the Central System never asked it,
                                // and must not see a reply to it.
                                if !matches!(frame.message_type, OcppMessageType::Call { .. }) {
                                    if let Some(in_flight) = local.take_response(&frame.unique_id) {
                                        complete_local_call(charge_point_id, mqtt_tx, in_flight, &frame);
                                        if send_local_calls(
                                            charge_point_id, forwarder, local, &config.charging,
                                            charger_tx, mqtt_tx,
                                        )
                                        .await
                                        .is_err()
                                        {
                                            return LoopOutcome::ChargerGone;
                                        }
                                        continue;
                                    }
                                }

                                let mut sink = UpstreamSink { sink: up_sink };
                                match forwarder.forward_upstream(frame, &mut sink).await {
                                    Ok(()) => {
                                        state.lock().await
                                            .record_forwarded(Direction::ChargerToCentral);
                                        // A charger response may have cleared the line
                                        // for a proxy Call that was waiting on it.
                                        if !local.is_empty()
                                            && send_local_calls(
                                                charge_point_id, forwarder, local,
                                                &config.charging, charger_tx, mqtt_tx,
                                            )
                                            .await
                                            .is_err()
                                        {
                                            return LoopOutcome::ChargerGone;
                                        }
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
            command = command_rx.recv(), if *commands_open => {
                match command {
                    Some(request) => {
                        info!(
                            component = "session",
                            charge_point_id = %charge_point_id,
                            command_id = %request.id,
                            action = request.command.action(),
                            source = ?request.source,
                            "Proxy command received"
                        );
                        for displaced in local.push(request) {
                            let (status, detail) = if displaced.command.is_limit_command() {
                                (
                                    CommandStatus::Superseded,
                                    "a newer limit command arrived before this one was sent",
                                )
                            } else {
                                (CommandStatus::Rejected, "command queue full; oldest dropped")
                            };
                            emit_command_result(
                                mqtt_tx,
                                charge_point_id,
                                CommandResult::for_request(&displaced, status, Some(detail.to_string())),
                            );
                        }
                        if send_local_calls(
                            charge_point_id, forwarder, local, &config.charging, charger_tx, mqtt_tx,
                        )
                        .await
                        .is_err()
                        {
                            return LoopOutcome::ChargerGone;
                        }
                    }
                    None => {
                        // The router dropped our sender (a newer connection for
                        // this id took over). Stop polling a closed channel.
                        *commands_open = false;
                    }
                }
            }

            _ = command_tick.tick(), if !local.is_empty() => {
                let timeout_s = config.charging.command_timeout_seconds;
                let (expired, in_flight) = local.expire(chrono::Utc::now());
                for request in expired {
                    warn!(
                        component = "session",
                        charge_point_id = %charge_point_id,
                        command_id = %request.id,
                        "Proxy command not sent within the timeout; dropping it"
                    );
                    emit_command_result(
                        mqtt_tx,
                        charge_point_id,
                        CommandResult::for_request(
                            &request,
                            CommandStatus::Timeout,
                            Some(format!(
                                "not sent within {} s (the charger was busy answering the \
                                 Central System, or the line was down)",
                                timeout_s
                            )),
                        ),
                    );
                }
                if let Some(in_flight) = in_flight {
                    warn!(
                        component = "session",
                        charge_point_id = %charge_point_id,
                        command_id = %in_flight.request.id,
                        unique_id = %in_flight.prepared.unique_id,
                        action = in_flight.prepared.action,
                        "Charger did not answer the proxy's Call within the timeout"
                    );
                    emit_command_result(
                        mqtt_tx,
                        charge_point_id,
                        CommandResult::for_in_flight(
                            &in_flight,
                            CommandStatus::Timeout,
                            None,
                            Some(format!("no answer from the charger within {} s", timeout_s)),
                        ),
                    );
                }
                if send_local_calls(
                    charge_point_id, forwarder, local, &config.charging, charger_tx, mqtt_tx,
                )
                .await
                .is_err()
                {
                    return LoopOutcome::ChargerGone;
                }
            }

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

/// Send the next queued proxy Call if the line is clear: nothing of ours in
/// flight, and no recent Central System Call still awaiting the charger's
/// answer. `Err` means the charger connection is gone.
async fn send_local_calls(
    charge_point_id: &str,
    forwarder: &MessageForwarder,
    local: &mut LocalCallQueue,
    charging: &ChargingConfig,
    charger_tx: &mpsc::Sender<AxumMessage>,
    mqtt_tx: &mpsc::Sender<MqttEvent>,
) -> Result<(), ()> {
    if local.has_in_flight() || local.is_empty() {
        return Ok(());
    }
    if forwarder.has_recent_pending_call(Direction::CentralToCharger, CENTRAL_CALL_GRACE) {
        debug!(
            component = "session",
            charge_point_id = %charge_point_id,
            queued = local.queued_len(),
            "Holding proxy command: the Central System has a Call outstanding with the charger"
        );
        return Ok(());
    }

    while let Some(request) = local.pop_next() {
        let unique_id = local.next_unique_id();
        match command::prepare_call(charging, &request, unique_id, chrono::Utc::now()) {
            Err(reason) => {
                warn!(
                    component = "session",
                    charge_point_id = %charge_point_id,
                    command_id = %request.id,
                    reason = %reason,
                    "Proxy command rejected before sending"
                );
                emit_command_result(
                    mqtt_tx,
                    charge_point_id,
                    CommandResult::for_request(&request, CommandStatus::Invalid, Some(reason)),
                );
            }
            Ok(prepared) => {
                info!(
                    component = "session",
                    charge_point_id = %charge_point_id,
                    command_id = %request.id,
                    unique_id = %prepared.unique_id,
                    action = prepared.action,
                    applied_limit_a = ?prepared.applied_limit_a,
                    "Sending proxy-originated Call to the charger"
                );
                let mut sink = ChargerSink {
                    tx: charger_tx.clone(),
                };
                if sink.send_raw(&prepared.raw).await.is_err() {
                    emit_command_result(
                        mqtt_tx,
                        charge_point_id,
                        CommandResult::for_request(
                            &request,
                            CommandStatus::NotConnected,
                            Some(
                                "charger connection closed before the command was sent".to_string(),
                            ),
                        ),
                    );
                    return Err(());
                }
                local.set_in_flight(request, prepared);
                return Ok(());
            }
        }
    }
    Ok(())
}

/// The charger answered one of our Calls: read the answer and report it.
fn complete_local_call(
    charge_point_id: &str,
    mqtt_tx: &mpsc::Sender<MqttEvent>,
    in_flight: InFlight,
    frame: &OcppFrame,
) {
    let (status, response, detail) = command::interpret_response(in_flight.prepared.action, frame);
    if status == CommandStatus::Accepted {
        info!(
            component = "session",
            charge_point_id = %charge_point_id,
            command_id = %in_flight.request.id,
            action = in_flight.prepared.action,
            applied_limit_a = ?in_flight.prepared.applied_limit_a,
            detail = detail.as_deref().unwrap_or(""),
            "Charger accepted the proxy's Call"
        );
    } else {
        warn!(
            component = "session",
            charge_point_id = %charge_point_id,
            command_id = %in_flight.request.id,
            action = in_flight.prepared.action,
            status = status.as_str(),
            detail = detail.as_deref().unwrap_or(""),
            "Charger did not accept the proxy's Call"
        );
    }
    emit_command_result(
        mqtt_tx,
        charge_point_id,
        CommandResult::for_in_flight(&in_flight, status, response, detail),
    );
}

fn emit_command_result(
    mqtt_tx: &mpsc::Sender<MqttEvent>,
    charge_point_id: &str,
    result: CommandResult,
) {
    if let Err(e) = mqtt_tx.try_send(MqttEvent::CommandResult {
        charge_point_id: charge_point_id.to_string(),
        result,
    }) {
        warn!(
            component = "session",
            charge_point_id = %charge_point_id,
            error = %e,
            "Could not queue a command result for MQTT; the caller will not hear back"
        );
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
