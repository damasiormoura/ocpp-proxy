# Implementation Plan: OCPP Proxy HA Integration

## Overview

This plan implements an OCPP 1.6J WebSocket proxy in Rust (Tokio) that transparently forwards messages between an Autel EV charger and the Mobi.e Central System, while asynchronously publishing events to MQTT for Home Assistant. The implementation is structured to build foundational components first, then layer in forwarding logic, MQTT publishing, health monitoring, and deployment artifacts.

## Tasks

- [x] 1. Set up project structure, dependencies, and core data models
  - [x] 1.1 Initialize Rust project with Cargo workspace and dependencies
    - Create `Cargo.toml` with dependencies: tokio, tokio-tungstenite, axum, rumqttc, serde, serde_json, config, tracing, tracing-subscriber, chrono, url, proptest (dev)
    - Create `src/main.rs` with basic Tokio runtime entry point
    - Create module structure: `src/{config.rs, models.rs, error.rs, downstream.rs, upstream.rs, forwarder.rs, mqtt.rs, health.rs, state.rs}`
    - _Requirements: 7.1, 7.2, 11.1_

  - [x] 1.2 Implement core data models and error types
    - Implement `OcppFrame` struct with `raw`, `message_type`, `unique_id`, `received_at` fields
    - Implement `OcppMessageType` enum (Call, CallResult, CallError) with JSON array parsing
    - Implement `Direction` enum (ChargerToCentral, CentralToCharger)
    - Implement `ConnectionState` enum (Disconnected, Connecting, Connected, Reconnecting)
    - Implement `ConnectionId` enum (Upstream, Downstream, Mqtt)
    - Implement `StateChange` struct with previous/current state and timestamp
    - Implement `ProxyError` error enum with categories from the design
    - Implement `ExponentialBackoff` struct with `next_delay()` and `reset()` methods
    - _Requirements: 3.6, 2.4, 6.3, 8.5_

  - [x] 1.3 Write property test for exponential backoff computation
    - **Property 4: Exponential backoff computes correct delays**
    - Generate (initial, multiplier, max, attempts) tuples with proptest
    - Verify delay for attempt N equals min(initial × multiplier^(N-1), maximum)
    - Verify delay never exceeds configured maximum
    - **Validates: Requirements 2.4, 6.3**

  - [x] 1.4 Write property test for OCPP frame parsing
    - **Property 1: Message forwarding preserves payload byte-for-byte**
    - Generate random JSON arrays matching OCPP frame structure with varied whitespace, unicode, nested objects
    - Parse into OcppFrame and verify `raw` field is byte-for-byte identical to input
    - **Validates: Requirements 3.1, 3.2, 3.3**

- [x] 2. Implement configuration management
  - [x] 2.1 Implement configuration loading and validation
    - Implement `ProxyConfig`, `MqttConfig`, `LogConfig`, `BufferConfig` structs with serde deserialization
    - Implement `ProxyConfig::load()` using the `config` crate with env vars taking precedence over YAML
    - Read YAML from `CONFIG_FILE_PATH` env var, falling back to `./config.yaml`
    - Implement `ProxyConfig::validate()` checking: port range 1–65535, URL scheme ws:// or wss://, TLS file paths exist, log level validity
    - Report all missing required parameters in a single error message on startup failure
    - Support optional params with defaults: log level (INFO), message buffer (100), MQTT buffer (500), max backoff (60s)
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7_

  - [x] 2.2 Write property tests for configuration validation
    - **Property 10: Configuration validation rejects all invalid inputs**
    - Generate invalid port numbers (0, 65536+), invalid URLs (missing scheme), non-existent file paths, invalid log levels
    - Verify validator rejects each invalid value
    - **Validates: Requirements 7.5**

  - [x] 2.3 Write property test for environment variable precedence
    - **Property 11: Environment variables take precedence over YAML configuration**
    - Generate random key-value pairs present in both env and YAML
    - Verify the env value is always used
    - **Validates: Requirements 7.1**

  - [x] 2.4 Write property test for missing parameter reporting
    - **Property 12: All missing required parameters are reported together**
    - Generate random subsets of required parameters to omit
    - Verify error output lists every missing parameter from the subset
    - **Validates: Requirements 7.2, 7.3**

- [x] 3. Implement connection state manager and structured logging
  - [x] 3.1 Implement connection state manager
    - Implement `ConnectionStateManager` with upstream, downstream, and MQTT state tracking
    - Implement `transition()` method that updates state and broadcasts `StateChange` events via `tokio::sync::broadcast`
    - Implement `health_status()` method with the health logic: downstream disconnected → unhealthy; upstream+downstream connected, MQTT disconnected → degraded; upstream+downstream connected → healthy; else → unhealthy
    - Implement `subscribe()` for components to receive state change notifications
    - Track `ConnectionMetrics` (message counters for forwarded/dropped per direction)
    - _Requirements: 8.1, 10.3, 10.4, 10.5, 10.6_

  - [x] 3.2 Write property test for health status computation
    - **Property 9: Health status computation is correct for all state combinations**
    - Generate all 27 combinations of (upstream, downstream, mqtt) × 3 states
    - Verify health status matches the specification rules
    - **Validates: Requirements 10.3, 10.4, 10.5, 10.6**

  - [x] 3.3 Implement structured JSON logging
    - Configure `tracing-subscriber` with JSON output to stdout
    - Each log entry must include: timestamp (ISO 8601), level, component, message, correlation_id
    - Support configurable log levels: DEBUG, INFO, WARNING, ERROR
    - Log OCPP message summaries at DEBUG (type, action, unique ID) without full payloads at INFO
    - Log latency warnings when forwarding exceeds 500ms
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_

  - [x] 3.4 Write property test for structured log format
    - **Property 15: All log entries are valid structured JSON with required fields**
    - Generate random log events with varied levels, components, and messages
    - Verify output is valid JSON with all required fields present and correctly typed
    - **Validates: Requirements 8.5**

- [x] 4. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement WebSocket server (downstream handler)
  - [x] 5.1 Implement downstream WebSocket server with axum
    - Create axum router accepting WebSocket upgrade at `/{charge_point_id}` path
    - Validate `ocpp1.6` subprotocol during upgrade; reject connections with other subprotocols
    - Complete WebSocket handshake within 5 seconds (timeout)
    - If a new connection arrives for an existing Charge Point ID, close the existing connection with a close frame and accept the new one
    - Log handshake failures and close TCP connection with appropriate status code
    - Emit connection state changes (Connecting → Connected, Connected → Disconnected) through the state manager
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_

  - [x] 5.2 Write property test for subprotocol rejection
    - **Property 14: Non-ocpp1.6 subprotocols are rejected**
    - Generate random strings that are not exactly "ocpp1.6"
    - Verify the proxy rejects the connection for each
    - **Validates: Requirements 1.6**

  - [x] 5.3 Write property test for connection replacement
    - **Property 13: New connections replace existing connections for the same Charge Point ID**
    - Generate sequences of 1–10 connection events for the same ID
    - Verify only the most recent connection is active and all prior ones received close frames
    - **Validates: Requirements 1.4**

- [x] 6. Implement WebSocket client (upstream handler)
  - [x] 6.1 Implement upstream WebSocket client with tokio-tungstenite
    - Connect to Central System URL with the same Charge_Point_ID in the path
    - Forward the same subprotocol header received from the charger
    - Implement 10-second connection timeout for initial connection
    - Implement exponential backoff reconnection (2s initial, 60s max) on connection loss
    - Keep downstream connection open for up to 5 minutes during reconnection attempts
    - Close downstream with code 1001 if upstream cannot be re-established within 5 minutes
    - Emit connection state transitions through the state manager
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

  - [x] 6.2 Write property test for Charge Point ID preservation
    - **Property 17: Charge Point ID is preserved in upstream connection URL**
    - Generate random valid Charge_Point_IDs
    - Verify the upstream URL contains the same ID in its path
    - **Validates: Requirements 2.2**

- [x] 7. Implement message forwarder with buffering
  - [x] 7.1 Implement message forwarder priority path
    - Implement `MessageForwarder` with `forward_upstream()` and `forward_downstream()` methods
    - Forward messages without modification (byte-for-byte preservation of raw JSON)
    - Maintain FIFO order per direction
    - Implement `CallTracker` to correlate CallResult/CallError with originating Call actions (map UniqueId → action)
    - After successful forwarding, send a copy to the MQTT publisher via `mpsc::Sender<MqttEvent>`
    - Ensure forwarding completes before MQTT publishing is initiated
    - Measure forwarding latency and log WARNING if it exceeds 500ms
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.6, 4.1, 4.3, 4.4_

  - [x] 7.2 Implement message buffer with eviction
    - Implement upstream and downstream buffers using `VecDeque<OcppFrame>`
    - Buffer up to 100 messages / 30 seconds when destination is disconnected
    - Discard oldest messages (FIFO eviction) when buffer is full, logging each discard at WARNING with message unique ID
    - Implement `flush_buffer()` to deliver buffered messages in order when connection is restored
    - Discard central-to-charger buffer if downstream connection is lost
    - _Requirements: 3.5, 3.7, 4.5_

  - [x] 7.3 Write property test for message ordering
    - **Property 2: Message ordering is preserved per direction**
    - Generate random-length Vec of OcppFrames, forward in order
    - Verify output order matches input order
    - **Validates: Requirements 3.4**

  - [x] 7.4 Write property test for buffer capacity and eviction
    - **Property 3: Message buffer respects capacity and eviction policy**
    - Generate message counts from 1 to 500 while destination is disconnected
    - Verify buffer never exceeds max_buffer_size (100) and oldest messages are evicted first
    - **Validates: Requirements 3.5, 4.5**

- [x] 8. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Implement MQTT publisher
  - [x] 9.1 Implement MQTT connection management with rumqttc
    - Connect to MQTT broker using configurable host, port, username, password
    - Use TLS 1.2+ with server certificate verification
    - Configure keepalive interval of 60 seconds
    - Configure Last Will and Testament: topic `ocpp/{charge_point_id}/availability`, payload `offline`, QoS 1, retained
    - Publish retained `online` message to availability topic on connect
    - Implement reconnection with exponential backoff (1s initial, 30s max)
    - Attempt connection for up to 10 seconds at startup, proceed regardless of MQTT status
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 9.2, 9.3_

  - [x] 9.2 Implement MQTT event publishing
    - Run publisher in a dedicated Tokio task consuming from `mpsc::Receiver<MqttEvent>`
    - Publish to topic format `ocpp/{charge_point_id}/{direction}/{action}`
    - For CallResult/CallError, resolve action from CallTracker
    - Publish JSON payload with `timestamp` (ISO 8601), `message_type`, and `payload` fields
    - Use QoS 1 for all event messages
    - Publish retained status message to `ocpp/{charge_point_id}/status` on connection state changes
    - Implement MQTT message buffer (500 messages, FIFO eviction) when broker is unreachable
    - Ensure MQTT operations add no more than 5ms latency to forwarding path (fully async)
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 4.2, 4.4_

  - [x] 9.3 Write property test for MQTT topic construction
    - **Property 5: MQTT topic construction follows the format specification**
    - Generate random alphanumeric Charge_Point_IDs, directions, and action names
    - Verify topic equals `ocpp/{id}/{direction}/{action}` with no extra segments or slashes
    - **Validates: Requirements 5.2**

  - [x] 9.4 Write property test for MQTT payload structure
    - **Property 6: MQTT payload contains all required fields with correct types**
    - Generate random OCPP messages of all 3 types
    - Verify published JSON contains valid ISO 8601 timestamp, correct message_type, and full payload
    - **Validates: Requirements 5.3**

  - [x] 9.5 Write property test for MQTT buffer eviction
    - **Property 7: MQTT buffer respects capacity and eviction policy**
    - Generate event counts from 1 to 2000 while broker is unreachable
    - Verify buffer never exceeds 500 and oldest are evicted first
    - **Validates: Requirements 5.5**

  - [x] 9.6 Write property test for connection status message
    - **Property 8: Connection status message reflects actual states**
    - Generate all 9 combinations of (upstream, downstream) × 3 states
    - Verify published status JSON accurately represents both connection states
    - **Validates: Requirements 5.6**

- [x] 10. Implement health check HTTP server
  - [x] 10.1 Implement health check endpoint with axum
    - Serve HTTP endpoint on configurable port (default 8080) at `/health`
    - Return JSON response within 2 seconds with: status, upstream state, downstream state, mqtt state, wwan reachability, uptime_seconds, message counters
    - _Requirements: 10.1, 10.2_

  - [x] 10.3 Correct the health status semantics (REVISED)
    - Replace the ECS-era rules: "no charger connected" must be `idle`/200, never `unhealthy`/503
    - `healthy` 200: charger connected, upstream and MQTT connected
    - `degraded` 200: upstream reconnecting within window, or MQTT down
    - `unhealthy` 503: listener not bound, or upstream failed past its window while a charger is connected
    - Add a property assertion that no listening-and-idle combination maps to 503
    - _Requirements: 10.4, 10.5, 10.6, 10.7, 10.8_

  - [x] 10.4 Report health from live state
    - `main.rs` builds a second `ConnectionStateManager` for `HealthState` that nothing ever updates, so `/health` always reports `downstream: disconnected` and zero counters
    - Share one `Arc<Mutex<ConnectionStateManager>>` between the forwarding path and the health server
    - Increment message counters on every forward and every drop, both directions
    - _Requirements: 10.3, 10.9_

  - [x] 10.2 Write property test for health response structure
    - **Property 16: Health response contains all required fields**
    - Generate random state combinations and counter values
    - Verify response JSON contains all required fields with correct types
    - **Validates: Requirements 10.2**

- [x] 11. Implement graceful startup and shutdown
  - [x] 11.1 Startup sequence and signal handling — **fixed**. Verified against the release binary: SIGTERM now exits in ~1-250 ms. Was: `graceful_shutdown` is called with `None` for every callback, so it logs a shutdown sequence without performing one. `drop(mqtt_tx)` does not close the channel because the forwarder retains a sender clone, so `mqtt_handle.join()` blocks forever and SIGTERM ends in SIGKILL. Downstream sockets are aborted rather than sent a 1000 close frame.
    - Attempt MQTT connection for up to 10 seconds at startup
    - Begin listening for charger connections regardless of MQTT status
    - Log startup completion and total time to ready state
    - Register SIGTERM and SIGINT handlers via `tokio::signal`
    - On shutdown signal: stop accepting new connections, complete in-flight forwarding (up to 10s), send WebSocket close frame (code 1000) to both endpoints, wait up to 5s for close acks, publish offline to MQTT, log discarded message counts, exit
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

- [x] 12. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Wire components together in main — **rewritten**
  - [x] 13.1 Wire all components in main.rs. Was: The per-charger upstream task in `main.rs` is a stub that drains its channel into a no-op; nothing ever writes to the upstream WebSocket and nothing reads from it. `UpstreamHandler::{send,recv,reconnect}` and `MessageForwarder::forward_downstream` are dead code, confirmed by compiler dead-code warnings. Buffers are filled but never flushed or expired; `CallTracker` is never cleaned; charger `Close` never reaches the main loop, leaking per-charger state
    - Rewrite as one task per charger owning both sockets, replacing the single serial main loop
    - Publish `MqttEvent::StateChange` on every transition — the variant is never constructed, so `ocpp/{id}/status` is never published
    - Pass the real Charge Point ID to the MQTT publisher; it is currently hard-coded to `"default"`, so every topic is `ocpp/default/...`
    - Load and validate configuration at startup (fail fast with all errors)
    - Initialize structured logging
    - Create shared state (Arc) for connection state manager and metrics
    - Spawn downstream WebSocket server task
    - Spawn upstream connection task (initiated when charger connects)
    - Spawn MQTT publisher task with mpsc channel
    - Spawn health check HTTP server task
    - Wire message forwarder between downstream and upstream with MQTT channel
    - Implement the main select loop coordinating all tasks
    - Handle graceful shutdown orchestration
    - _Requirements: 7.3, 9.1, 9.2, 9.4, 9.5, 11.3_

  - [x] 13.2 Deployment artifacts for the Proxmox LXC (REVISED — replaces the ECS artifacts)
    - LXC provisioning runbook, host WWAN networking, watchdog and systemd units in `deploy/lxc/`
    - ECS task definition removed; Dockerfile retained for development builds only
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5_

## Revision: Proxmox LXC deployment

Added when the deployment model moved from AWS ECS to a Proxmox LXC with a 4G
APN uplink. Tasks 15–17 depend on the fixes in 10.3, 10.4, 11.1 and 13.1.

- [ ] 15. Adapt the application to the new deployment
  - [x] 15.1 Add `listen_address` configuration
    - Bind the charger listener to `listen_address` (default `0.0.0.0`); set to the LXC's `vmbr1` address so the listener is not exposed on the WWAN-egress or main-LAN legs
    - `upstream_bind_address` is **no longer required**: Mobi.e is a fixed RFC1918 range (`10.200.10.0/24`), so egress is selected by destination route on the host and in the container. Keep the parameter optional for a possible future move to a hostname
    - _Requirements: 7.4, 13.5_
  - [x] 15.2 Make MQTT TLS optional
    - `ca_cert_path`, `client_cert_path`, `client_key_path` become `Option<String>`
    - None => plaintext; ca only => server-auth TLS; all three => mutual TLS
    - Remove them from the required-parameter set
    - _Requirements: 6.6, 6.6a, 6.6b, 7.2_
  - [x] 15.3 Honour `max_backoff_seconds`
    - Currently parsed and ignored; upstream and MQTT both hard-code their maxima
    - _Requirements: 7.4a_
  - [x] 15.4 Move tokio-tungstenite from native-tls to rustls
    - Drops the OpenSSL build and runtime dependency; matches rumqttc
    - Verify the binary runs on a bare Debian LXC with no `libssl3`
  - [ ] 15.5 Report WWAN reachability in the health response — **still open**
    - _Requirements: 10.2, 11.11_

- [x] 16. Report all missing configuration parameters together
  - `try_deserialize` fails on the first missing field, so only one is ever reported
  - Deserialize into an all-`Option` shadow struct, then collect every `None`
  - Strengthen `property_config_missing.rs`, which currently asserts only that *at least one* missing field is named
  - _Requirements: 7.3_

- [ ] 17. Emit `correlation_id` in structured logs
  - Requirement 8.5 mandates `component` and `correlation_id` on every entry; `correlation_id` appears nowhere in the source and no spans are ever created
  - `property_log_format.rs` substitutes `target` for both, so the gap is untested
  - Create a per-connection span carrying the Charge Point ID and a connection identifier
  - _Requirements: 8.5_

- [x] 18. Close the test gaps that let a non-functional binary pass its suite
  - [x] 18.1 End-to-end round trip against a mock Central System — 9 integration tests in `tests/integration_end_to_end.rs`. Validated by sabotage: stubbing the upstream send fails 4 of them, including the round trip
  - [x] 18.2 Connection replacement and missing-parameter properties now call production code (`register_connection`/`deregister_connection`, and a loader that collects every missing field). MQTT buffer eviction still asserts against a local copy — `buffer_message` is private; **still open**
  - [x] 18.3 Shutdown covered by two integration tests plus a release-binary smoke test
  - [x] 18.4 Connection-replacement race covered by unit, property and integration tests

- [ ] 19. Host and network provisioning (`deploy/lxc/README.md`)
  - [ ] 19.1 Pin the dongle to `wwan0` by USB ID before inserting the SIM — the MAC-derived `enx*` name will change once a SIM registers
  - [x] 19.2 Insert SIM and record the dongle's subnet and gateway — `192.168.0.0/24` via `192.168.0.1`, LTE, full signal, `ppp_connected`, operator NOS
  - [ ] 19.3 Apply host networking: `vmbr2`, `wwan0` static with no default route, `10.200.10.0/24` destination route, MASQUERADE, MSS clamp
  - [ ] 19.4 Install the WWAN watchdog timer
  - [ ] 19.5 Create LXC 113, update `network/addressing.md` and `proxmox/inventory.md` in the mouraishikawa repo
  - [ ] 19.6 Give the charger a DHCP reservation and apply the Proxmox firewall rules on LXC 113
  - [x] 19.7 **Record the charger's original Mobi.e URL** — `ws://10.200.10.200/ocpp/1.6/MOBI-ALM-00058`. Still to capture: the rest of the charger's OCPP settings, for the Requirement 11.13 bypass
  - [x] 19.8 DNS question resolved — the endpoint is a literal IP, so no resolver configuration is needed on the APN path
  - [x] 19.9 Verify the endpoint over the APN — **done 2026-08-31 from the Proxmox host.** TCP open, ICMP 79 ms, `GET /` 200, WebSocket upgrade returned `101` from `nginx/1.6.2` with `ocpp1.6` negotiated
  - [ ] 19.11 Confirm Mobi.e actually accepts `MOBI-ALM-00058` — the 101 does not establish this. Controls returned 101 for an invalid Charge Point ID and for a request with no subprotocol, so the front end upgrades any path. Requires sending a real `BootNotification` and reading the response status, which writes to the operator's system
  - [x] 19.10 Charger located at `192.168.51.59`, confirmed by the Autel TLS certificate on its cloud session. Two MACs: `0c:dc:7e:57:7f:0c` (ESP32 WiFi, active) and `18:d7:93:60:b6:19` (Ethernet, down) — searching for the latter finds nothing because the port is unused
  - [ ] 19.12 DHCP reservation on the spare BD4 for `0c:dc:7e:57:7f:0c`, so the address is stable enough to name in firewall rules
  - [ ] 19.14 Move the charger to Ethernet. Its WiFi hop measures 65.8 ms avg / ±31.7 jitter / 125 ms max, against 6.9 ms / ±1.3 for the wired-behind-powerline TL-WPA4220. Plugging `18:d7:93:60:b6:19` into a WPA4220 LAN port removes a wireless hop from the charger's only path to Mobi.e
  - [ ] 19.15 Repoint the charger at `ws://192.168.52.30:9000/MOBI-ALM-00058` once the proxy is running
  - [x] 19.13 Verify the charger-to-proxy path ahead of time — from the IoT segment, `192.168.52.1` answers in 2-4 ms via the spare router's static route and the host's forwarding

- [ ] 14. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document using `proptest`
- Unit tests validate specific examples and edge cases
- The proxy is stateless by design (Requirement 11.3) — no local state persistence needed
- **A checked box in this file is not evidence.** Tasks 11 and 13 were marked complete while the assembled binary forwarded nothing in either direction and hung on SIGTERM. Verify against the running system, not the checklist
- MQTT publishing is entirely asynchronous and decoupled from the forwarding path
- All buffers have hard capacity limits to prevent unbounded memory growth

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2"] },
    { "id": 2, "tasks": ["1.3", "1.4", "2.1"] },
    { "id": 3, "tasks": ["2.2", "2.3", "2.4", "3.1", "3.3"] },
    { "id": 4, "tasks": ["3.2", "3.4", "5.1"] },
    { "id": 5, "tasks": ["5.2", "5.3", "6.1"] },
    { "id": 6, "tasks": ["6.2", "7.1"] },
    { "id": 7, "tasks": ["7.2"] },
    { "id": 8, "tasks": ["7.3", "7.4", "9.1"] },
    { "id": 9, "tasks": ["9.2"] },
    { "id": 10, "tasks": ["9.3", "9.4", "9.5", "9.6", "10.1"] },
    { "id": 11, "tasks": ["10.2", "11.1"] },
    { "id": 12, "tasks": ["13.1"] },
    { "id": 13, "tasks": ["13.2"] }
  ]
}
```
