# Tech Stack

## Language & Edition

- **Rust** (edition 2021)
- Minimum toolchain: Rust 1.82 (per Dockerfile)

## Core Dependencies

| Crate | Purpose |
|-------|---------|
| tokio (full) | Async runtime |
| axum (ws) | Downstream WebSocket server + health HTTP endpoint |
| tokio-tungstenite (native-tls) | Upstream WebSocket client to Central System |
| rumqttc (use-rustls) | MQTT 3.1.1 client with TLS |
| futures-util | Stream/Sink utilities for WebSocket handling |
| serde / serde_json | JSON serialization for config, MQTT payloads, health |
| config | Layered configuration (YAML + env vars) |
| tracing / tracing-subscriber (json) | Structured JSON logging |
| chrono | Timestamps (ISO 8601) |
| url | URL parsing and manipulation |
| async-trait | Async trait support (MessageSink) |
| tokio-util | CancellationToken for shutdown coordination |

## Dev Dependencies

| Crate | Purpose |
|-------|---------|
| proptest | Property-based testing |
| tempfile | Temporary files for config tests |
| tower | HTTP testing utilities |
| http-body-util | Response body extraction in tests |

## Build & Commands

```bash
# Build (debug)
cargo build

# Build (release)
cargo build --release

# Run all tests (unit + property-based)
cargo test

# Run a specific property test
cargo test --test property_frame_parsing

# Run with specific log level
RUST_LOG=debug cargo run

# Docker build
docker build -t ocpp-proxy .

# Lint
cargo clippy

# Format check
cargo fmt --check
```

## Configuration

- YAML config file (default: `./config.yaml`, override via `CONFIG_FILE_PATH` env var)
- Environment variables with prefix `OCPP_PROXY_` (use `__` for nested keys)
- Env vars take precedence over YAML values

## Docker

- Multi-stage build: `rust:1.82-bookworm` (builder) → `debian:bookworm-slim` (runtime)
- Runs as non-root user `ocpp`
- Ports: 9000 (WebSocket), 8080 (health check)
- Health check via `curl -f http://localhost:8080/health`
