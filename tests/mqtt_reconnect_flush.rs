//! Regression test for the 2026-09-19 incident.
//!
//! The broker (EMQX on the Home Assistant VM) restarted while the proxy had 14
//! messages buffered. On `ConnAck` the publisher awaited `publish()` for the
//! `online` message and then for each buffered one — from the same task that
//! drives the rumqttc event loop, which is the only thing that drains the
//! request channel those publishes go into. The channel holds 10. The tenth
//! await never returned: no keepalives, the broker published the Last Will,
//! `/health` kept saying `mqtt: connected`, and the log's last line was
//! `Flushing buffered MQTT messages` for almost six hours.
//!
//! This drives the real publisher against a stub broker through the same
//! sequence — a backlog larger than the request channel, then a connection —
//! and checks that everything arrives, in order, `online` first; then drops
//! the link and checks the shared connection state (what `/health` reads)
//! follows the event loop down and back up.

use std::sync::Arc;
use std::time::Duration;

use ocpp_proxy::config::MqttConfig;
use ocpp_proxy::forwarder::MqttEvent;
use ocpp_proxy::models::ConnectionState;
use ocpp_proxy::mqtt::{MqttMessage, MqttPublisher, REQUEST_CHANNEL_CAPACITY};
use ocpp_proxy::snapshot_store::SnapshotStore;
use ocpp_proxy::state::ConnectionStateManager;
use rumqttc::QoS;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::{sleep, timeout, Instant};

/// What the stub broker saw, in wire order.
#[derive(Debug)]
enum Seen {
    Connect,
    Publish {
        topic: String,
        retain: bool,
        payload: Vec<u8>,
    },
}

/// The smallest MQTT 3.1.1 broker that can hold this conversation: CONNACK to
/// CONNECT, PUBACK to a QoS 1 PUBLISH, PINGRESP to PINGREQ. Reports every
/// packet it understands. `drop_link` closes whatever connection is open, the
/// way a restarting broker does.
async fn stub_broker(
    listener: TcpListener,
    seen: mpsc::UnboundedSender<Seen>,
    drop_link: Arc<Notify>,
) {
    loop {
        let Ok((socket, _)) = listener.accept().await else {
            return;
        };
        let seen = seen.clone();
        let drop_link = drop_link.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = serve(socket, seen) => {}
                _ = drop_link.notified() => {}
            }
        });
    }
}

/// One MQTT control packet: the first byte and the body after the varint length.
async fn read_packet(socket: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let first = socket.read_u8().await?;
    let mut len = 0usize;
    let mut multiplier = 1usize;
    loop {
        let byte = socket.read_u8().await?;
        len += (byte & 0x7f) as usize * multiplier;
        multiplier *= 128;
        if byte & 0x80 == 0 {
            break;
        }
    }
    let mut body = vec![0u8; len];
    socket.read_exact(&mut body).await?;
    Ok((first, body))
}

async fn serve(mut socket: TcpStream, seen: mpsc::UnboundedSender<Seen>) {
    while let Ok((first, body)) = read_packet(&mut socket).await {
        match first >> 4 {
            // CONNECT → CONNACK, session not present, accepted
            1 => {
                let _ = seen.send(Seen::Connect);
                if socket.write_all(&[0x20, 0x02, 0x00, 0x00]).await.is_err() {
                    return;
                }
            }
            // PUBLISH → PUBACK when QoS 1
            3 => {
                let qos = (first >> 1) & 0x03;
                let retain = first & 0x01 == 1;
                let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
                let topic = String::from_utf8_lossy(&body[2..2 + topic_len]).into_owned();
                let mut at = 2 + topic_len;
                if qos > 0 {
                    let id = [body[at], body[at + 1]];
                    at += 2;
                    if socket.write_all(&[0x40, 0x02, id[0], id[1]]).await.is_err() {
                        return;
                    }
                }
                let _ = seen.send(Seen::Publish {
                    topic,
                    retain,
                    payload: body[at..].to_vec(),
                });
            }
            // PINGREQ → PINGRESP
            12 => {
                if socket.write_all(&[0xD0, 0x00]).await.is_err() {
                    return;
                }
            }
            // DISCONNECT
            14 => return,
            _ => {}
        }
    }
}

fn topic_of(seen: &Seen) -> &str {
    match seen {
        Seen::Publish { topic, .. } => topic,
        Seen::Connect => panic!("expected a PUBLISH, saw CONNECT"),
    }
}

/// Receive from the stub with one overall deadline.
async fn next_seen(seen: &mut mpsc::UnboundedReceiver<Seen>, deadline: Instant) -> Seen {
    timeout(
        deadline.saturating_duration_since(Instant::now()),
        seen.recv(),
    )
    .await
    .expect("the stub broker should have seen the next packet by now")
    .expect("stub broker channel closed")
}

#[tokio::test]
async fn a_backlog_larger_than_the_request_channel_is_flushed_on_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let drop_link = Arc::new(Notify::new());
    tokio::spawn(stub_broker(listener, seen_tx, drop_link.clone()));

    let config = MqttConfig {
        host: "127.0.0.1".to_string(),
        port,
        username: "user".to_string(),
        password: "pass".to_string(),
        ca_cert_path: None,
        client_cert_path: None,
        client_key_path: None,
    };
    // Held for the whole test: dropping the last sender is what ends `run()`.
    let (_event_tx, event_rx) = mpsc::channel::<MqttEvent>(16);
    let manager = Arc::new(Mutex::new(ConnectionStateManager::new(8)));
    let mut publisher = MqttPublisher::new(
        &config,
        Some("CP-TEST".to_string()),
        event_rx,
        500,
        SnapshotStore::disabled(),
    )
    .expect("publisher")
    .with_state_manager(manager.clone());

    // The broker was unreachable for a while and the backlog outgrew the
    // request channel — 14 on 2026-09-19, against a channel of 10.
    let backlog = REQUEST_CHANNEL_CAPACITY + 4;
    for i in 0..backlog {
        publisher.enqueue(MqttMessage {
            topic: format!("ocpp/CP-TEST/charger/MeterValues{i}"),
            payload: format!("{i}").into_bytes(),
            qos: QoS::AtLeastOnce,
            retain: false,
        });
    }
    assert_eq!(publisher.buffer_len(), backlog);

    let run = publisher.run();
    tokio::pin!(run);

    let scenario = async {
        // ---- 1. connect: online first, then the whole backlog, in order ----
        let deadline = Instant::now() + Duration::from_secs(10);
        assert!(matches!(
            next_seen(&mut seen, deadline).await,
            Seen::Connect
        ));

        match next_seen(&mut seen, deadline).await {
            Seen::Publish {
                topic,
                retain,
                payload,
            } => {
                assert_eq!(topic, "ocpp/CP-TEST/availability");
                assert!(retain, "availability is retained");
                assert_eq!(payload, b"online");
            }
            Seen::Connect => panic!("expected the online publish right after CONNECT"),
        }

        let mut topics = Vec::with_capacity(backlog);
        while topics.len() < backlog {
            topics.push(topic_of(&next_seen(&mut seen, deadline).await).to_string());
        }
        let expected: Vec<String> = (0..backlog)
            .map(|i| format!("ocpp/CP-TEST/charger/MeterValues{i}"))
            .collect();
        assert_eq!(topics, expected, "the backlog arrives complete and FIFO");
        assert_eq!(
            manager.lock().await.mqtt_state(),
            ConnectionState::Connected,
            "/health sees the connection"
        );

        // ---- 2. the broker restarts: /health follows the link down ... ----
        drop_link.notify_waiters();
        let deadline = Instant::now() + Duration::from_secs(10);
        while manager.lock().await.mqtt_state() != ConnectionState::Reconnecting {
            assert!(
                Instant::now() < deadline,
                "the shared state never learned the connection was lost"
            );
            sleep(Duration::from_millis(20)).await;
        }

        // ---- ... and back up, with a fresh online ----
        // Anything the client chooses to redeliver is fine; the point is that
        // the new session announces itself.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match next_seen(&mut seen, deadline).await {
                Seen::Publish { topic, payload, .. } if topic == "ocpp/CP-TEST/availability" => {
                    assert_eq!(payload, b"online");
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(
            manager.lock().await.mqtt_state(),
            ConnectionState::Connected
        );
    };

    tokio::select! {
        _ = &mut run => panic!("the publisher loop ended while its event channel was still open"),
        _ = scenario => {}
    }
}
