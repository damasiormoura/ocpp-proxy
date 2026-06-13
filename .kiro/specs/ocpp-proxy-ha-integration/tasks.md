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
    - Return JSON response within 2 seconds with: status, upstream state, downstream state, mqtt state, uptime_seconds, message counters (forwarded/dropped per direction)
    - Return HTTP 200 with `healthy` when upstream and downstream are connected
    - Return HTTP 200 with `degraded` when MQTT is lost but WS connections are active
    - Return HTTP 503 with `unhealthy` when downstream is lost or both upstream and MQTT are lost
    - Support AWS ECS/NLB health checks (HTTP 200 when ready)
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 11.5, 11.8_

  - [x] 10.2 Write property test for health response structure
    - **Property 16: Health response contains all required fields**
    - Generate random state combinations and counter values
    - Verify response JSON contains all required fields with correct types
    - **Validates: Requirements 10.2**

- [x] 11. Implement graceful startup and shutdown
  - [x] 11.1 Implement startup sequence and signal handling
    - Attempt MQTT connection for up to 10 seconds at startup
    - Begin listening for charger connections regardless of MQTT status
    - Log startup completion and total time to ready state
    - Register SIGTERM and SIGINT handlers via `tokio::signal`
    - On shutdown signal: stop accepting new connections, complete in-flight forwarding (up to 10s), send WebSocket close frame (code 1000) to both endpoints, wait up to 5s for close acks, publish offline to MQTT, log discarded message counts, exit
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

- [x] 12. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Wire components together in main and create Dockerfile
  - [x] 13.1 Wire all components in main.rs
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

  - [x] 13.2 Create Dockerfile and deployment artifacts
    - Create multi-stage Dockerfile: build with `rust:latest`, runtime with `debian:bookworm-slim` or `distroless`
    - Ensure compressed image size does not exceed 500 MB
    - Expose configurable ports for WebSocket and health check
    - Set entrypoint to the proxy binary
    - Create sample `config.yaml` with all parameters documented
    - Create sample ECS task definition JSON referencing the container, NLB, and health check configuration
    - Ensure container starts and becomes ready within 30 seconds
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.6, 11.7, 11.8_

- [x] 14. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document using `proptest`
- Unit tests validate specific examples and edge cases
- The proxy is stateless by design (Requirement 11.3) — no local state persistence needed
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
