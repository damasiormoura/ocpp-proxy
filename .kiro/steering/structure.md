# Project Structure

```
ocpp-proxy/
├── src/
│   ├── main.rs          # Entry point: orchestrates startup, main loop, shutdown
│   ├── lib.rs           # Public module exports (for integration test access)
│   ├── config.rs        # Layered config: YAML + env vars, validation
│   ├── downstream.rs    # WebSocket server accepting charger connections (axum)
│   ├── upstream.rs      # WebSocket client to Central System (tokio-tungstenite)
│   ├── forwarder.rs     # Message routing, buffering, call tracking, MQTT events
│   ├── mqtt.rs          # MQTT publisher with TLS, LWT, buffering, reconnection
│   ├── health.rs        # GET /health endpoint (axum), status computation
│   ├── state.rs         # ConnectionStateManager, health status rules, metrics
│   ├── models.rs        # OcppFrame, message types, direction, backoff, state enums
│   ├── error.rs         # Categorized error enum (ProxyError)
│   ├── shutdown.rs      # Graceful shutdown coordinator, signal handling
│   └── logging.rs       # Structured JSON logging initialization
├── tests/
│   └── property_*.rs    # Property-based tests (proptest) for correctness properties
├── deploy/
│   └── ecs-task-definition.json  # AWS ECS task definition
├── Cargo.toml
├── Cargo.lock
├── Dockerfile           # Multi-stage Docker build
├── config.yaml.example  # Annotated example configuration
└── LICENSE              # MIT
```

## Module Responsibilities

- **downstream**: Accepts charger WebSocket connections, validates `ocpp1.6` subprotocol, handles connection replacement for same Charge Point ID, routes received messages to forwarder channel.
- **upstream**: Manages WebSocket client to Central System with 10s connect timeout, exponential backoff reconnection (2s–60s), 5-minute reconnection window.
- **forwarder**: Priority forwarding path. Sends raw bytes to sink, tracks Call→Response correlation, buffers when destination unavailable, emits MQTT events after forwarding.
- **mqtt**: Runs on a dedicated OS thread (rumqttc EventLoop is not Send). Publishes to `ocpp/{charge_point_id}/{direction}/{action}` topics. Buffers when broker unreachable.
- **state**: Central state manager with broadcast channel for state change events. Computes health: healthy (all connected), degraded (MQTT down), unhealthy (upstream or downstream down).
- **models**: Domain types. OcppFrame preserves raw JSON. ExponentialBackoff used by both upstream and MQTT.
- **error**: Single ProxyError enum with variants per category (connection_downstream, connection_upstream, connection_mqtt, forwarding, config, protocol, tls).

## Testing Conventions

- Unit tests live inside each module (`#[cfg(test)] mod tests`)
- Property-based tests live in `tests/property_*.rs` using proptest
- Tests reference the crate via `ocpp_proxy::` (using lib.rs public exports)
- Each property test file documents which correctness property it validates
- Test naming: `test_` prefix for unit tests, descriptive property names for proptest
