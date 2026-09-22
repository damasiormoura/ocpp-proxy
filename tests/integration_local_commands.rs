//! End-to-end tests for proxy-originated charger commands.
//!
//! A real charger WebSocket and a mock Central System sit either side of the
//! proxy, exactly as in `integration_end_to_end.rs`; commands are injected
//! through the `CommandRouter` the MQTT publisher would use. The properties:
//!
//! - a `set_current_limit` becomes one `SetChargingProfile` Call **to the
//!   charger** carrying the configured profile, and nothing goes upstream;
//! - the charger's answer is consumed and reported as a `CommandResult`, and
//!   the Central System never receives it;
//! - the Central System's own traffic is untouched while a proxy Call is in
//!   flight, and its responses still reach it byte-for-byte;
//! - a proxy Call waits while the Central System has a Call outstanding;
//! - a `CallError`, a timeout, an invalid command and a disconnected charger
//!   each produce the matching result rather than silence.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message as TungMessage;
use tokio_util::sync::CancellationToken;

use ocpp_proxy::command::{
    CommandRequest, CommandResult, CommandRouter, CommandSource, CommandStatus, DispatchError,
    ProxyCommand, UNIQUE_ID_PREFIX,
};
use ocpp_proxy::config::{ChargingConfig, ChargingProfilePurpose};
use ocpp_proxy::downstream::{create_router, DownstreamState, OCPP16_SUBPROTOCOL};
use ocpp_proxy::forwarder::MqttEvent;
use ocpp_proxy::session::SessionConfig;
use ocpp_proxy::state::ConnectionStateManager;

const CHARGE_POINT_ID: &str = "CP-EXAMPLE-0001";

type ChargerWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A Central System that records everything it receives and can be told to
/// send a frame of its own.
struct MockCentralSystem {
    port: u16,
    received: Arc<Mutex<Vec<String>>>,
    outbound: Arc<Mutex<Vec<mpsc::UnboundedSender<String>>>>,
}

impl MockCentralSystem {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let received = Arc::new(Mutex::new(Vec::new()));
        let outbound: Arc<Mutex<Vec<mpsc::UnboundedSender<String>>>> =
            Arc::new(Mutex::new(Vec::new()));

        let received_task = received.clone();
        let outbound_task = outbound.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let received = received_task.clone();
                let (tx, mut rx) = mpsc::unbounded_channel::<String>();
                outbound_task.lock().await.push(tx);
                tokio::spawn(async move {
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

                    loop {
                        tokio::select! {
                            incoming = ws.next() => match incoming {
                                Some(Ok(TungMessage::Text(text))) => {
                                    received.lock().await.push(text.to_string());
                                }
                                Some(Ok(_)) => {}
                                _ => return,
                            },
                            frame = rx.recv() => match frame {
                                Some(frame) => {
                                    if ws.send(TungMessage::Text(frame.into())).await.is_err() {
                                        return;
                                    }
                                }
                                None => return,
                            },
                        }
                    }
                });
            }
        });

        Self {
            port,
            received,
            outbound,
        }
    }

    fn url(&self) -> url::Url {
        url::Url::parse(&format!("ws://127.0.0.1:{}/ocpp/1.6", self.port)).unwrap()
    }

    async fn received(&self) -> Vec<String> {
        self.received.lock().await.clone()
    }

    /// Send a frame to the charger through the proxy, as Mobi.e would.
    async fn send(&self, frame: &str) {
        let senders = self.outbound.lock().await;
        let tx = senders.last().expect("no upstream connection yet");
        tx.send(frame.to_string()).unwrap();
    }
}

struct ProxyUnderTest {
    port: u16,
    shutdown: CancellationToken,
    mqtt_rx: mpsc::Receiver<MqttEvent>,
    router: CommandRouter,
}

impl ProxyUnderTest {
    async fn start(central_system_url: url::Url, charging: ChargingConfig) -> Self {
        let (mqtt_tx, mqtt_rx) = mpsc::channel(256);
        let state = Arc::new(Mutex::new(ConnectionStateManager::new(64)));
        let shutdown = CancellationToken::new();
        let router = CommandRouter::new();

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
            charging,
        });

        let downstream_state = DownstreamState {
            connections: Arc::new(Mutex::new(HashMap::new())),
            state_manager: state.clone(),
            session_config,
            mqtt_tx,
            shutdown: shutdown.clone(),
            generation: Arc::new(AtomicU64::new(1)),
            command_router: router.clone(),
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        state.lock().await.set_listener_bound(true);

        let app = create_router(downstream_state);
        let server_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
                .await;
        });

        Self {
            port,
            shutdown,
            mqtt_rx,
            router,
        }
    }

    /// Wait for the next `CommandResult` event, skipping forwarded messages
    /// and state changes.
    async fn next_result(&mut self) -> CommandResult {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, self.mqtt_rx.recv()).await {
                Ok(Some(MqttEvent::CommandResult { result, .. })) => return result,
                Ok(Some(_)) => continue,
                Ok(None) => panic!("MQTT event channel closed"),
                Err(_) => panic!("no CommandResult within 5 s"),
            }
        }
    }
}

async fn connect_as_charger(port: u16, charge_point_id: &str) -> ChargerWs {
    let url = format!("ws://127.0.0.1:{}/{}", port, charge_point_id);
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_static(OCPP16_SUBPROTOCOL),
    );
    let (ws, _response) = tokio_tungstenite::connect_async(request).await.unwrap();
    ws
}

/// Read the next text frame the charger receives.
async fn charger_receives(charger: &mut ChargerWs) -> String {
    let msg = tokio::time::timeout(Duration::from_secs(5), charger.next())
        .await
        .expect("timed out waiting for a frame at the charger")
        .expect("charger connection closed")
        .expect("websocket error");
    match msg {
        TungMessage::Text(text) => text.to_string(),
        other => panic!("expected a text frame, got {:?}", other),
    }
}

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

/// Open the session and wait until the upstream leg is connected, which is
/// when the session starts reading commands.
async fn charger_with_upstream(cs: &MockCentralSystem, proxy: &ProxyUnderTest) -> ChargerWs {
    let mut charger = connect_as_charger(proxy.port, CHARGE_POINT_ID).await;
    charger
        .send(TungMessage::Text(r#"[2,"boot","Heartbeat",{}]"#.into()))
        .await
        .unwrap();
    assert!(
        eventually(Duration::from_secs(5), || async {
            !cs.received().await.is_empty()
        })
        .await,
        "upstream never came up"
    );
    charger
}

fn enabled(max_limit_a: f64) -> ChargingConfig {
    ChargingConfig {
        enabled: true,
        max_limit_a,
        command_timeout_seconds: 2,
        ..ChargingConfig::default()
    }
}

fn set_limit(id: &str, limit_a: f64) -> CommandRequest {
    CommandRequest::new(
        id,
        ProxyCommand::SetCurrentLimit { limit_a },
        CommandSource::Mqtt,
    )
}

#[tokio::test]
async fn set_current_limit_reaches_the_charger_and_only_the_charger() {
    let cs = MockCentralSystem::start().await;
    let mut proxy = ProxyUnderTest::start(cs.url(), enabled(20.0)).await;
    let mut charger = charger_with_upstream(&cs, &proxy).await;
    let upstream_before = cs.received().await.len();

    proxy
        .router
        .dispatch(CHARGE_POINT_ID, set_limit("cmd-1", 16.0))
        .unwrap();

    let raw = charger_receives(&mut charger).await;
    let frame: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(frame[0], 2, "must be a Call");
    let unique_id = frame[1].as_str().unwrap().to_string();
    assert!(
        unique_id.starts_with(UNIQUE_ID_PREFIX),
        "proxy Calls carry their own id prefix, got {unique_id}"
    );
    assert_eq!(frame[2], "SetChargingProfile");
    let profile = &frame[3]["csChargingProfiles"];
    assert_eq!(frame[3]["connectorId"], 0);
    assert_eq!(profile["chargingProfilePurpose"], "TxDefaultProfile");
    assert_eq!(profile["chargingProfileKind"], "Absolute");
    assert_eq!(
        profile["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"],
        16.0
    );
    assert!(profile["chargingSchedule"]["startSchedule"].is_string());

    // The charger accepts.
    charger
        .send(TungMessage::Text(
            format!(r#"[3,"{}",{{"status":"Accepted"}}]"#, unique_id).into(),
        ))
        .await
        .unwrap();

    let result = proxy.next_result().await;
    assert_eq!(result.id, "cmd-1");
    assert_eq!(result.status, CommandStatus::Accepted);
    assert_eq!(result.action.as_deref(), Some("SetChargingProfile"));
    assert_eq!(result.applied_limit_a, Some(16.0));
    assert_eq!(result.requested_limit_a, Some(16.0));
    assert_eq!(result.charger_response.unwrap()["status"], "Accepted");
    assert_eq!(result.ocpp_request.unwrap()[1], unique_id);

    // Give any stray forwarding a moment, then check the Central System saw
    // neither the Call nor the answer.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let upstream = cs.received().await;
    assert_eq!(
        upstream.len(),
        upstream_before,
        "nothing about the proxy's command may reach the Central System: {:?}",
        &upstream[upstream_before..]
    );

    proxy.shutdown.cancel();
}

#[tokio::test]
async fn limit_above_the_ceiling_is_clamped_and_said_so() {
    let cs = MockCentralSystem::start().await;
    let mut proxy = ProxyUnderTest::start(cs.url(), enabled(20.0)).await;
    let mut charger = charger_with_upstream(&cs, &proxy).await;

    proxy
        .router
        .dispatch(CHARGE_POINT_ID, set_limit("cmd-2", 32.0))
        .unwrap();

    let raw = charger_receives(&mut charger).await;
    let frame: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"],
        20.0
    );
    let unique_id = frame[1].as_str().unwrap();
    charger
        .send(TungMessage::Text(
            format!(r#"[3,"{}",{{"status":"Accepted"}}]"#, unique_id).into(),
        ))
        .await
        .unwrap();

    let result = proxy.next_result().await;
    assert_eq!(result.status, CommandStatus::Accepted);
    assert_eq!(result.requested_limit_a, Some(32.0));
    assert_eq!(result.applied_limit_a, Some(20.0));
    assert!(result.detail.unwrap().contains("clamped"));

    proxy.shutdown.cancel();
}

#[tokio::test]
async fn central_system_traffic_is_untouched_while_a_proxy_call_is_in_flight() {
    let cs = MockCentralSystem::start().await;
    let mut proxy = ProxyUnderTest::start(cs.url(), enabled(32.0)).await;
    let mut charger = charger_with_upstream(&cs, &proxy).await;

    proxy
        .router
        .dispatch(CHARGE_POINT_ID, set_limit("cmd-3", 10.0))
        .unwrap();
    let raw = charger_receives(&mut charger).await;
    let proxy_id = serde_json::from_str::<Value>(&raw).unwrap()[1]
        .as_str()
        .unwrap()
        .to_string();

    // Meanwhile the charger reports a meter value and the Central System
    // answers it — both must pass exactly as before.
    let meter = r#"[2,"mv-1","MeterValues",{"connectorId":1,"meterValue":[]}]"#;
    charger.send(TungMessage::Text(meter.into())).await.unwrap();
    assert!(
        eventually(Duration::from_secs(5), || async {
            cs.received().await.iter().any(|f| f == meter)
        })
        .await,
        "the charger's MeterValues must still reach the Central System byte-for-byte"
    );
    let reply = r#"[3,"mv-1",{}]"#;
    cs.send(reply).await;
    assert_eq!(charger_receives(&mut charger).await, reply);

    // Now the charger answers the proxy's Call.
    charger
        .send(TungMessage::Text(
            format!(r#"[3,"{}",{{"status":"Accepted"}}]"#, proxy_id).into(),
        ))
        .await
        .unwrap();
    let result = proxy.next_result().await;
    assert_eq!(result.id, "cmd-3");
    assert_eq!(result.status, CommandStatus::Accepted);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !cs.received().await.iter().any(|f| f.contains(&proxy_id)),
        "the answer to the proxy's Call must not be forwarded upstream"
    );

    proxy.shutdown.cancel();
}

#[tokio::test]
async fn proxy_call_waits_for_an_outstanding_central_system_call() {
    let cs = MockCentralSystem::start().await;
    let proxy = ProxyUnderTest::start(cs.url(), enabled(32.0)).await;
    let mut charger = charger_with_upstream(&cs, &proxy).await;

    // Mobi.e asks the charger something and the charger has not answered yet.
    let remote_start =
        r#"[2,"cs-1","RemoteStartTransaction",{"idTag":"a1b2c3d4","connectorId":1}]"#;
    cs.send(remote_start).await;
    assert_eq!(charger_receives(&mut charger).await, remote_start);

    proxy
        .router
        .dispatch(CHARGE_POINT_ID, set_limit("cmd-4", 8.0))
        .unwrap();

    // Nothing may reach the charger while the Central System's Call is open.
    let held = tokio::time::timeout(Duration::from_millis(500), charger.next()).await;
    assert!(
        held.is_err(),
        "the proxy sent a Call while the charger still owed the Central System an answer: {:?}",
        held
    );

    // The charger answers Mobi.e; that answer goes upstream and frees the line.
    let answer = r#"[3,"cs-1",{"status":"Accepted"}]"#;
    charger
        .send(TungMessage::Text(answer.into()))
        .await
        .unwrap();
    assert!(
        eventually(Duration::from_secs(5), || async {
            cs.received().await.iter().any(|f| f == answer)
        })
        .await,
        "the charger's answer to the Central System must be forwarded"
    );

    let raw = charger_receives(&mut charger).await;
    let frame: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(frame[2], "SetChargingProfile");

    proxy.shutdown.cancel();
}

#[tokio::test]
async fn a_call_error_from_the_charger_is_reported_as_an_error() {
    let cs = MockCentralSystem::start().await;
    let mut proxy = ProxyUnderTest::start(cs.url(), enabled(32.0)).await;
    let mut charger = charger_with_upstream(&cs, &proxy).await;

    proxy
        .router
        .dispatch(
            CHARGE_POINT_ID,
            CommandRequest::new(
                "cmd-5",
                ProxyCommand::GetCompositeSchedule {
                    connector_id: None,
                    duration_s: 600,
                },
                CommandSource::Mqtt,
            ),
        )
        .unwrap();
    let raw = charger_receives(&mut charger).await;
    let frame: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(frame[2], "GetCompositeSchedule");
    let unique_id = frame[1].as_str().unwrap();

    charger
        .send(TungMessage::Text(
            format!(
                r#"[4,"{}","NotImplemented","Requested Action is not known by receiver",{{}}]"#,
                unique_id
            )
            .into(),
        ))
        .await
        .unwrap();

    let result = proxy.next_result().await;
    assert_eq!(result.status, CommandStatus::Error);
    assert_eq!(
        result.charger_response.unwrap()["errorCode"],
        "NotImplemented"
    );
    assert!(result.detail.unwrap().contains("NotImplemented"));

    proxy.shutdown.cancel();
}

#[tokio::test]
async fn an_unanswered_call_times_out() {
    let cs = MockCentralSystem::start().await;
    let mut proxy = ProxyUnderTest::start(cs.url(), enabled(32.0)).await;
    let mut charger = charger_with_upstream(&cs, &proxy).await;

    proxy
        .router
        .dispatch(CHARGE_POINT_ID, set_limit("cmd-6", 12.0))
        .unwrap();
    let _sent = charger_receives(&mut charger).await;
    // ...and the charger says nothing.

    let result = proxy.next_result().await;
    assert_eq!(result.id, "cmd-6");
    assert_eq!(result.status, CommandStatus::Timeout);
    assert!(result.detail.unwrap().contains("no answer"));

    proxy.shutdown.cancel();
}

#[tokio::test]
async fn an_invalid_command_is_refused_without_touching_the_charger() {
    let cs = MockCentralSystem::start().await;
    let mut proxy = ProxyUnderTest::start(cs.url(), enabled(32.0)).await;
    let mut charger = charger_with_upstream(&cs, &proxy).await;

    proxy
        .router
        .dispatch(
            CHARGE_POINT_ID,
            CommandRequest::new(
                "cmd-7",
                ProxyCommand::ChangeConfiguration {
                    key: "AuthorizeRemoteTxRequests".to_string(),
                    value: "false".to_string(),
                },
                CommandSource::Mqtt,
            ),
        )
        .unwrap();

    let result = proxy.next_result().await;
    assert_eq!(result.status, CommandStatus::Invalid);
    assert!(result
        .detail
        .unwrap()
        .contains("allowed_configuration_keys"));

    let nothing = tokio::time::timeout(Duration::from_millis(300), charger.next()).await;
    assert!(
        nothing.is_err(),
        "no frame may reach the charger: {:?}",
        nothing
    );

    proxy.shutdown.cancel();
}

#[tokio::test]
async fn newer_limit_supersedes_a_queued_one() {
    let cs = MockCentralSystem::start().await;
    let mut proxy = ProxyUnderTest::start(cs.url(), enabled(32.0)).await;
    let mut charger = charger_with_upstream(&cs, &proxy).await;

    // First goes out and sits unanswered; the next two queue behind it and
    // the third supersedes the second.
    proxy
        .router
        .dispatch(CHARGE_POINT_ID, set_limit("first", 10.0))
        .unwrap();
    let raw = charger_receives(&mut charger).await;
    let first_id = serde_json::from_str::<Value>(&raw).unwrap()[1]
        .as_str()
        .unwrap()
        .to_string();
    proxy
        .router
        .dispatch(CHARGE_POINT_ID, set_limit("second", 12.0))
        .unwrap();
    proxy
        .router
        .dispatch(CHARGE_POINT_ID, set_limit("third", 14.0))
        .unwrap();

    let superseded = proxy.next_result().await;
    assert_eq!(superseded.id, "second");
    assert_eq!(superseded.status, CommandStatus::Superseded);

    charger
        .send(TungMessage::Text(
            format!(r#"[3,"{}",{{"status":"Accepted"}}]"#, first_id).into(),
        ))
        .await
        .unwrap();
    let first = proxy.next_result().await;
    assert_eq!(first.id, "first");
    assert_eq!(first.status, CommandStatus::Accepted);

    let raw = charger_receives(&mut charger).await;
    let frame: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        frame[3]["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]["limit"],
        14.0,
        "the third limit follows the first; the second was never sent"
    );

    proxy.shutdown.cancel();
}

#[tokio::test]
async fn commands_for_a_disconnected_charger_are_refused_at_the_router() {
    let cs = MockCentralSystem::start().await;
    let proxy = ProxyUnderTest::start(cs.url(), enabled(32.0)).await;

    assert_eq!(
        proxy
            .router
            .dispatch(CHARGE_POINT_ID, set_limit("cmd-8", 16.0)),
        Err(DispatchError::NotConnected)
    );

    // Connect, then disconnect: the registration must go away with the session.
    let charger = charger_with_upstream(&cs, &proxy).await;
    assert!(proxy.router.is_connected(CHARGE_POINT_ID));
    drop(charger);
    assert!(
        eventually(Duration::from_secs(5), || async {
            !proxy.router.is_connected(CHARGE_POINT_ID)
        })
        .await,
        "a closed session must deregister from the router"
    );

    proxy.shutdown.cancel();
}

#[tokio::test]
async fn a_pending_command_is_reported_when_the_charger_leaves() {
    let cs = MockCentralSystem::start().await;
    let mut proxy = ProxyUnderTest::start(cs.url(), enabled(32.0)).await;
    let mut charger = charger_with_upstream(&cs, &proxy).await;

    proxy
        .router
        .dispatch(CHARGE_POINT_ID, set_limit("cmd-9", 16.0))
        .unwrap();
    let _sent = charger_receives(&mut charger).await;
    charger.close(None).await.unwrap();
    drop(charger);

    let result = proxy.next_result().await;
    assert_eq!(result.id, "cmd-9");
    assert_eq!(result.status, CommandStatus::Timeout);
    assert!(result.detail.unwrap().contains("session ended"));

    proxy.shutdown.cancel();
}

#[tokio::test]
async fn charge_point_max_profile_is_sent_on_connector_zero_when_configured() {
    let cs = MockCentralSystem::start().await;
    let charging = ChargingConfig {
        purpose: ChargingProfilePurpose::ChargePointMaxProfile,
        stack_level: 3,
        profile_id: 42,
        ..enabled(25.0)
    };
    let mut proxy = ProxyUnderTest::start(cs.url(), charging).await;
    let mut charger = charger_with_upstream(&cs, &proxy).await;

    proxy
        .router
        .dispatch(
            CHARGE_POINT_ID,
            CommandRequest::new(
                "cmd-10",
                ProxyCommand::ClearCurrentLimit,
                CommandSource::Mqtt,
            ),
        )
        .unwrap();
    let raw = charger_receives(&mut charger).await;
    let frame: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(frame[2], "ClearChargingProfile");
    assert_eq!(frame[3]["id"], 42);
    assert_eq!(frame[3]["chargingProfilePurpose"], "ChargePointMaxProfile");
    assert_eq!(frame[3]["stackLevel"], 3);
    let unique_id = frame[1].as_str().unwrap();
    charger
        .send(TungMessage::Text(
            format!(r#"[3,"{}",{{"status":"Unknown"}}]"#, unique_id).into(),
        ))
        .await
        .unwrap();
    let result = proxy.next_result().await;
    assert_eq!(
        result.status,
        CommandStatus::Accepted,
        "Unknown to a clear leaves no limit, which is the goal"
    );
    assert!(result.detail.unwrap().contains("nothing to clear"));

    proxy.shutdown.cancel();
}
