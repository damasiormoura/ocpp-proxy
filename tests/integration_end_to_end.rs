//! End-to-end integration test: charger → proxy → Central System → proxy → charger.
//!
//! This is the test the suite most lacked. Before it existed, 433 unit and
//! property tests passed against a binary that forwarded nothing in either
//! direction, because every test exercised a component in isolation and
//! nothing exercised the assembled wiring. A single round trip would have
//! caught it on day one.
//!
//! **Validates: Requirements 2.1, 2.2, 3.1, 3.2, 3.3, 3.4, 3.6, 5.1, 5.2**

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tokio_util::sync::CancellationToken;

use ocpp_proxy::downstream::{create_router, DownstreamState, OCPP16_SUBPROTOCOL};
use ocpp_proxy::forwarder::MqttEvent;
use ocpp_proxy::models::Direction;
use ocpp_proxy::session::SessionConfig;
use ocpp_proxy::state::ConnectionStateManager;

const CHARGE_POINT_ID: &str = "CP-EXAMPLE-0001";

/// A stand-in for the Mobi.e Central System.
struct MockCentralSystem {
    port: u16,
    /// Frames the Central System received, in order.
    received: Arc<Mutex<Vec<String>>>,
    /// Frames to send back, keyed by the frame that triggers them.
    replies: Arc<Mutex<Vec<String>>>,
}

impl MockCentralSystem {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let received = Arc::new(Mutex::new(Vec::new()));
        let replies = Arc::new(Mutex::new(Vec::new()));

        let received_task = received.clone();
        let replies_task = replies.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let received = received_task.clone();
                let replies = replies_task.clone();
                tokio::spawn(async move {
                    // Echo the subprotocol back, as a real OCPP server does.
                    //
                    // The Err type here is tungstenite's own `ErrorResponse`,
                    // not ours to shrink, and this callback never returns one.
                    #[allow(clippy::result_large_err)]
                    let ws = tokio_tungstenite::accept_hdr_async(
                        stream,
                        |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                         mut res: tokio_tungstenite::tungstenite::handshake::server::Response| {
                            if let Some(proto) = req.headers().get("Sec-WebSocket-Protocol") {
                                res.headers_mut()
                                    .insert("Sec-WebSocket-Protocol", proto.clone());
                            }
                            Ok(res)
                        },
                    )
                    .await;

                    let Ok(mut ws) = ws else { return };

                    while let Some(Ok(msg)) = ws.next().await {
                        if let TungMessage::Text(text) = msg {
                            received.lock().await.push(text.to_string());
                            // Send any queued replies.
                            let queued: Vec<String> = replies.lock().await.drain(..).collect();
                            for reply in queued {
                                if ws.send(TungMessage::Text(reply.into())).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                });
            }
        });

        Self {
            port,
            received,
            replies,
        }
    }

    fn url(&self) -> url::Url {
        url::Url::parse(&format!("ws://127.0.0.1:{}/ocpp/1.6", self.port)).unwrap()
    }

    async fn queue_reply(&self, frame: &str) {
        self.replies.lock().await.push(frame.to_string());
    }

    async fn received(&self) -> Vec<String> {
        self.received.lock().await.clone()
    }
}

/// The proxy under test, started on an ephemeral port.
struct ProxyUnderTest {
    port: u16,
    shutdown: CancellationToken,
    mqtt_rx: mpsc::Receiver<MqttEvent>,
    state: Arc<Mutex<ConnectionStateManager>>,
}

impl ProxyUnderTest {
    async fn start(central_system_url: url::Url) -> Self {
        let (mqtt_tx, mqtt_rx) = mpsc::channel(256);
        let state = Arc::new(Mutex::new(ConnectionStateManager::new(64)));
        let shutdown = CancellationToken::new();

        let session_config = Arc::new(SessionConfig {
            central_system_url,
            upstream_bind_address: None,
            subprotocol: OCPP16_SUBPROTOCOL.to_string(),
            message_buffer_size: 100,
            max_buffer_duration: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(5),
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
            max_reconnect_window: Duration::from_secs(2),
            call_tracker_max_age: Duration::from_secs(300),
        });

        let downstream_state = DownstreamState {
            connections: Arc::new(Mutex::new(HashMap::new())),
            state_manager: state.clone(),
            session_config,
            mqtt_tx,
            shutdown: shutdown.clone(),
            generation: Arc::new(AtomicU64::new(1)),
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        state.lock().await.set_listener_bound(true);

        let router = create_router(downstream_state);
        let server_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
                .await;
        });

        Self {
            port,
            shutdown,
            mqtt_rx,
            state,
        }
    }
}

/// Connect as a charger would: `ws://host:port/{ChargePointId}`, subprotocol `ocpp1.6`.
async fn connect_as_charger(
    port: u16,
    charge_point_id: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let url = format!("ws://127.0.0.1:{}/{}", port, charge_point_id);
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_static(OCPP16_SUBPROTOCOL),
    );
    let (ws, _response) = tokio_tungstenite::connect_async(request).await.unwrap();
    ws
}

/// Await a condition, polling, so tests do not depend on fixed sleeps.
async fn eventually<F, Fut>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The core test: a Call goes charger → Central System, and its CallResult
/// comes back Central System → charger.
#[tokio::test]
async fn boot_notification_round_trip() {
    let cs = MockCentralSystem::start().await;
    let proxy = ProxyUnderTest::start(cs.url()).await;

    let mut charger = connect_as_charger(proxy.port, CHARGE_POINT_ID).await;

    let call = r#"[2,"msg-1","BootNotification",{"chargePointVendor":"Autel","chargePointModel":"MaxiCharger"}]"#;
    let result =
        r#"[3,"msg-1",{"status":"Accepted","currentTime":"2026-08-31T20:00:00Z","interval":300}]"#;

    cs.queue_reply(result).await;
    charger
        .send(TungMessage::Text(call.into()))
        .await
        .expect("charger should be able to send");

    // --- charger → Central System ---
    let arrived = eventually(Duration::from_secs(5), || async {
        !cs.received().await.is_empty()
    })
    .await;
    assert!(
        arrived,
        "the Central System never received the charger's message — nothing was forwarded upstream"
    );

    let received = cs.received().await;
    assert_eq!(
        received[0], call,
        "the forwarded frame must be byte-for-byte identical (Requirement 3.3)"
    );

    // --- Central System → charger ---
    let echoed = tokio::time::timeout(Duration::from_secs(5), charger.next())
        .await
        .expect("timed out waiting for the CallResult — nothing was forwarded downstream")
        .expect("charger connection closed unexpectedly")
        .expect("websocket error");

    match echoed {
        TungMessage::Text(text) => assert_eq!(
            text.as_str(),
            result,
            "the CallResult must reach the charger byte-for-byte"
        ),
        other => panic!("expected a text frame, got {:?}", other),
    }

    proxy.shutdown.cancel();
}

/// Requirement 3.3 — exact bytes, including whitespace and unicode, in both
/// directions.
#[tokio::test]
async fn payloads_are_preserved_byte_for_byte() {
    let cs = MockCentralSystem::start().await;
    let proxy = ProxyUnderTest::start(cs.url()).await;
    let mut charger = connect_as_charger(proxy.port, CHARGE_POINT_ID).await;

    // Deliberately awkward: irregular spacing, unicode, and a nested object
    // whose key order must not be normalised by a re-serialisation.
    let call = "[ 2 , \"id-\u{00e7}\" , \"MeterValues\" , { \"zeta\":1 , \"alpha\" : \"caf\u{00e9} \u{2013} 42\" } ]";
    let reply = "[ 3 , \"id-\u{00e7}\" , {  \"custom\" : \"\u{00e5}\u{00e6}\u{00f8}\"  } ]";

    cs.queue_reply(reply).await;
    charger.send(TungMessage::Text(call.into())).await.unwrap();

    assert!(
        eventually(Duration::from_secs(5), || async {
            !cs.received().await.is_empty()
        })
        .await,
        "nothing reached the Central System"
    );
    assert_eq!(
        cs.received().await[0],
        call,
        "upstream bytes must match exactly"
    );

    let echoed = tokio::time::timeout(Duration::from_secs(5), charger.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match echoed {
        TungMessage::Text(text) => {
            assert_eq!(text.as_str(), reply, "downstream bytes must match exactly")
        }
        other => panic!("expected text, got {:?}", other),
    }

    proxy.shutdown.cancel();
}

/// Requirement 3.4 — FIFO order per direction.
#[tokio::test]
async fn message_order_is_preserved() {
    let cs = MockCentralSystem::start().await;
    let proxy = ProxyUnderTest::start(cs.url()).await;
    let mut charger = connect_as_charger(proxy.port, CHARGE_POINT_ID).await;

    let sent: Vec<String> = (0..25)
        .map(|i| format!(r#"[2,"msg-{}","Heartbeat",{{}}]"#, i))
        .collect();

    for frame in &sent {
        charger
            .send(TungMessage::Text(frame.clone().into()))
            .await
            .unwrap();
    }

    assert!(
        eventually(Duration::from_secs(5), || async {
            cs.received().await.len() == sent.len()
        })
        .await,
        "expected {} frames upstream, saw {}",
        sent.len(),
        cs.received().await.len()
    );

    assert_eq!(
        cs.received().await,
        sent,
        "frames must arrive in the order they were sent"
    );

    proxy.shutdown.cancel();
}

/// Requirement 5.1/5.2 — forwarded messages are published to MQTT under the
/// charger's real ID, not a placeholder.
#[tokio::test]
async fn mqtt_events_carry_the_real_charge_point_id() {
    let cs = MockCentralSystem::start().await;
    let mut proxy = ProxyUnderTest::start(cs.url()).await;
    let mut charger = connect_as_charger(proxy.port, CHARGE_POINT_ID).await;

    charger
        .send(TungMessage::Text(
            r#"[2,"m1","BootNotification",{}]"#.into(),
        ))
        .await
        .unwrap();

    let mut saw_forwarded = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), proxy.mqtt_rx.recv()).await {
            Ok(Some(MqttEvent::MessageForwarded {
                charge_point_id,
                direction,
                action,
                ..
            })) => {
                assert_eq!(
                    charge_point_id, CHARGE_POINT_ID,
                    "topics must use the charger's real ID"
                );
                assert_eq!(direction, Direction::ChargerToCentral);
                assert_eq!(action, "BootNotification");
                saw_forwarded = true;
                break;
            }
            Ok(Some(_)) => continue, // status events are fine, keep looking
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    assert!(saw_forwarded, "no MessageForwarded event was published");

    proxy.shutdown.cancel();
}

/// Requirement 5.6 — connection state changes are published, so Home Assistant
/// can see them. The `StateChange` variant previously existed but was never
/// constructed anywhere, so this topic was never published at all.
#[tokio::test]
async fn connection_state_changes_are_published() {
    let cs = MockCentralSystem::start().await;
    let mut proxy = ProxyUnderTest::start(cs.url()).await;
    let _charger = connect_as_charger(proxy.port, CHARGE_POINT_ID).await;

    let mut saw_state_change = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), proxy.mqtt_rx.recv()).await {
            Ok(Some(MqttEvent::StateChange {
                charge_point_id, ..
            })) => {
                assert_eq!(charge_point_id, CHARGE_POINT_ID);
                saw_state_change = true;
                break;
            }
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    assert!(saw_state_change, "no StateChange event was published");

    proxy.shutdown.cancel();
}

/// Requirement 10 — health reflects live state, and counters actually move.
#[tokio::test]
async fn health_state_tracks_a_real_session() {
    use ocpp_proxy::state::HealthStatus;

    let cs = MockCentralSystem::start().await;
    let proxy = ProxyUnderTest::start(cs.url()).await;

    // Listening, no charger: idle, never unhealthy.
    assert_eq!(proxy.state.lock().await.health_status(), HealthStatus::Idle);

    let mut charger = connect_as_charger(proxy.port, CHARGE_POINT_ID).await;
    charger
        .send(TungMessage::Text(r#"[2,"h1","Heartbeat",{}]"#.into()))
        .await
        .unwrap();

    assert!(
        eventually(Duration::from_secs(5), || async {
            let mgr = proxy.state.lock().await;
            mgr.metrics().charger_to_central_forwarded > 0
        })
        .await,
        "forwarded counter never moved — health reports would stay at zero"
    );

    proxy.shutdown.cancel();
}

/// Requirement 1.4 — a second connection for the same ID displaces the first,
/// and the replacement is the one that works afterwards.
#[tokio::test]
async fn a_second_connection_displaces_the_first_and_still_forwards() {
    let cs = MockCentralSystem::start().await;
    let proxy = ProxyUnderTest::start(cs.url()).await;

    let mut first = connect_as_charger(proxy.port, CHARGE_POINT_ID).await;
    first
        .send(TungMessage::Text(r#"[2,"a","Heartbeat",{}]"#.into()))
        .await
        .unwrap();
    assert!(
        eventually(Duration::from_secs(5), || async {
            !cs.received().await.is_empty()
        })
        .await,
        "first connection never forwarded"
    );

    // The charger reconnects, as it would after a network blip.
    let mut second = connect_as_charger(proxy.port, CHARGE_POINT_ID).await;
    second
        .send(TungMessage::Text(r#"[2,"b","Heartbeat",{}]"#.into()))
        .await
        .unwrap();

    assert!(
        eventually(Duration::from_secs(5), || async {
            cs.received().await.iter().any(|f| f.contains(r#""b""#))
        })
        .await,
        "the replacement connection did not forward — the displaced session's \
         cleanup most likely deregistered it"
    );

    proxy.shutdown.cancel();
}

/// Requirement 9.1/9.4 — shutdown ends sessions promptly and closes the
/// charger's socket with a close frame rather than dropping it.
///
/// The previous implementation could not do this: `drop(mqtt_tx)` left sender
/// clones alive, so the MQTT channel never closed, the thread join never
/// returned, and SIGTERM ended in a SIGKILL.
#[tokio::test]
async fn shutdown_closes_sessions_with_a_close_frame() {
    let cs = MockCentralSystem::start().await;
    let proxy = ProxyUnderTest::start(cs.url()).await;
    let mut charger = connect_as_charger(proxy.port, CHARGE_POINT_ID).await;

    // Make sure the session is fully established before shutting down.
    charger
        .send(TungMessage::Text(r#"[2,"s1","Heartbeat",{}]"#.into()))
        .await
        .unwrap();
    assert!(
        eventually(Duration::from_secs(5), || async {
            !cs.received().await.is_empty()
        })
        .await,
        "session never got going"
    );

    proxy.shutdown.cancel();

    // The charger must be told, not just dropped.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut saw_close = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), charger.next()).await {
            Ok(Some(Ok(TungMessage::Close(frame)))) => {
                if let Some(frame) = frame {
                    assert_eq!(
                        u16::from(frame.code),
                        1000,
                        "shutdown must use close code 1000 (normal closure)"
                    );
                }
                saw_close = true;
                break;
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => continue,
        }
    }
    assert!(
        saw_close,
        "charger never received a close frame; the socket was dropped instead"
    );
}

/// Requirement 9.1 — every MQTT sender is released when sessions end, so the
/// publisher's channel closes and its thread can exit.
///
/// This is the exact defect that turned SIGTERM into a hang: the forwarder and
/// each session held sender clones, so dropping only the local handle left the
/// receiver open forever.
#[tokio::test]
async fn all_mqtt_senders_are_released_on_shutdown() {
    let cs = MockCentralSystem::start().await;
    let (mqtt_tx, mut mqtt_rx) = mpsc::channel::<MqttEvent>(256);
    let state = Arc::new(Mutex::new(ConnectionStateManager::new(64)));
    let shutdown = CancellationToken::new();

    let session_config = Arc::new(SessionConfig {
        central_system_url: cs.url(),
        upstream_bind_address: None,
        subprotocol: OCPP16_SUBPROTOCOL.to_string(),
        message_buffer_size: 100,
        max_buffer_duration: Duration::from_secs(30),
        connect_timeout: Duration::from_secs(5),
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(200),
        max_reconnect_window: Duration::from_secs(2),
        call_tracker_max_age: Duration::from_secs(300),
    });

    let downstream_state = DownstreamState {
        connections: Arc::new(Mutex::new(HashMap::new())),
        state_manager: state.clone(),
        session_config,
        mqtt_tx,
        shutdown: shutdown.clone(),
        generation: Arc::new(AtomicU64::new(1)),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let router = create_router(downstream_state.clone());
    let server_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
            .await;
    });

    let mut charger = connect_as_charger(port, CHARGE_POINT_ID).await;
    charger
        .send(TungMessage::Text(r#"[2,"x","Heartbeat",{}]"#.into()))
        .await
        .unwrap();
    assert!(
        eventually(Duration::from_secs(5), || async {
            !cs.received().await.is_empty()
        })
        .await
    );

    shutdown.cancel();

    // Wait for sessions to unwind, then release the last sender exactly as
    // `main` does.
    assert!(
        eventually(Duration::from_secs(5), || async {
            downstream_state.connections.lock().await.is_empty()
        })
        .await,
        "sessions did not deregister after shutdown"
    );
    drop(downstream_state);

    // With every sender gone the channel must close. If any clone survived,
    // this recv would block until the timeout — which is precisely the hang.
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        while mqtt_rx.recv().await.is_some() {}
    })
    .await;

    assert!(
        closed.is_ok(),
        "the MQTT channel never closed: a sender clone outlived shutdown,          which is what makes the publisher thread join hang forever"
    );
}
