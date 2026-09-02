# OCPP Proxy

A transparent WebSocket proxy for OCPP 1.6J that bridges EV chargers to a Central System while publishing all OCPP messages to MQTT for Home Assistant integration.

```
                    Proxmox host (mouraishikawa)
 ┌──────────┐      ┌──────────────────────────┐        ┌────────────────┐
 │ EV       │◄─WS─►│  LXC 113  ocpp-proxy     │◄─WSS──►│ Central System │
 │ Charger  │ :9000│                          │  via   │ (Mobi.e)       │
 └──────────┘  LAN └────────────┬─────────────┘  APN   └────────────────┘
                                │              ZTE 4G USB dongle (wwan0)
                             MQTT │
                    ┌─────────────▼──────────┐
                    │ VM 110  EMQX           │
                    │ Home Assistant (HAOS)  │
                    └────────────────────────┘
```

The charger reaches the proxy over the local network. The proxy reaches Mobi.e
over a mobile APN served by a 4G dongle on the Proxmox host, selected by a
destination route: the Central System is a literal RFC1918 address inside the
APN, so no source-address policy routing is needed.

## Features

- **Transparent forwarding** — OCPP messages pass byte-for-byte without modification or re-serialization
- **MQTT publishing** — all OCPP events published asynchronously to `ocpp/{charge_point_id}/{direction}/{action}`
- **Resilient connections** — exponential backoff reconnection for both upstream WebSocket and MQTT
- **Message buffering** — FIFO buffers with configurable limits when destinations are unavailable
- **Health endpoint** — `GET /health` returns connection states and message counters (HTTP 200/503)
- **Graceful shutdown** — completes in-flight messages, sends WebSocket close frames, publishes offline status
- **Optional MQTT TLS** — plaintext on the local hop by default; server-auth or mutual TLS when certificates are configured
- **Structured logging** — JSON logs to stdout with configurable log levels

## Quick Start

### Prerequisites

- Rust 1.82+
- An MQTT broker — EMQX in this deployment, Mosquitto works too; TLS optional
- Access to an OCPP 1.6J Central System

### Build

```bash
cargo build --release
```

### Configure

Copy the example config and edit it:

```bash
cp config.yaml.example config.yaml
```

Or use environment variables (they take precedence over YAML):

```bash
export OCPP_PROXY_CENTRAL_SYSTEM_URL=wss://central-system.example.com/ocpp/v16
export OCPP_PROXY_LISTEN_PORT=9000
export OCPP_PROXY_MQTT__HOST=mqtt.example.com
export OCPP_PROXY_MQTT__PORT=8883
export OCPP_PROXY_MQTT__USERNAME=ocpp_proxy
export OCPP_PROXY_MQTT__PASSWORD=secret
export OCPP_PROXY_MQTT__CA_CERT_PATH=/path/to/ca.pem
export OCPP_PROXY_MQTT__CLIENT_CERT_PATH=/path/to/client.pem
export OCPP_PROXY_MQTT__CLIENT_KEY_PATH=/path/to/client-key.pem
```

### Run

```bash
cargo run --release
```

The proxy listens on port 9000 for charger WebSocket connections and exposes a health endpoint on port 8080.

## Configuration

Configuration uses a layered approach (highest precedence first):

1. Environment variables with prefix `OCPP_PROXY_` (use `__` for nested keys)
2. YAML file at `CONFIG_FILE_PATH` env var, or `./config.yaml`

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `central_system_url` | Yes | — | WebSocket URL of the Central System (`ws://` or `wss://`) |
| `listen_port` | Yes | — | Port for charger WebSocket connections |
| `listen_address` | No | 0.0.0.0 | Charger-facing bind address |
| `upstream_bind_address` | No | — | Local source address for the Mobi.e connection; the host's policy route keys on it |
| `health_port` | No | 8080 | Port for the health check HTTP endpoint |
| `mqtt.host` | Yes | — | MQTT broker hostname |
| `mqtt.port` | Yes | — | MQTT broker port (typically 8883 for TLS) |
| `mqtt.username` | Yes | — | MQTT authentication username |
| `mqtt.password` | Yes | — | MQTT authentication password |
| `mqtt.ca_cert_path` | No | — | CA certificate. Omit for a plaintext local connection |
| `mqtt.client_cert_path` | No | — | Client certificate, for mutual TLS |
| `mqtt.client_key_path` | No | — | Client private key, for mutual TLS |
| `logging.level` | No | INFO | Log level: DEBUG, INFO, WARNING, ERROR |
| `buffers.message_buffer_size` | No | 100 | Max OCPP messages buffered per direction |
| `buffers.mqtt_buffer_size` | No | 500 | Max MQTT messages buffered when broker unreachable |
| `buffers.max_backoff_seconds` | No | 60 | Maximum reconnection backoff interval |

See [`config.yaml.example`](config.yaml.example) for a fully documented example.

## MQTT Topics

The proxy publishes to the following topic structure:

| Topic | Payload | Retain |
|-------|---------|--------|
| `ocpp/{id}/charger/{action}` | OCPP message from charger | No |
| `ocpp/{id}/central_system/{action}` | OCPP message from central system | No |
| `ocpp/{id}/status` | `{"upstream": "...", "downstream": "..."}` | Yes |
| `ocpp/{id}/availability` | `"online"` or `"offline"` (LWT) | Yes |

Message payloads include a timestamp, message type, and the full original OCPP JSON:

```json
{
  "timestamp": "2026-01-15T10:30:00.123Z",
  "message_type": "Call",
  "payload": [2, "abc123", "BootNotification", {"chargePointModel": "Model"}]
}
```

## Health Check

`GET /health` returns JSON with HTTP 200 (healthy/degraded) or 503 (unhealthy):

```json
{
  "status": "healthy",
  "upstream": "connected",
  "downstream": "connected",
  "mqtt": "connected",
  "uptime_seconds": 3600,
  "messages": {
    "charger_to_central_forwarded": 142,
    "charger_to_central_dropped": 0,
    "central_to_charger_forwarded": 138,
    "central_to_charger_dropped": 0
  }
}
```

Health status rules:
- **Healthy** — charger connected, upstream and MQTT connected (200)
- **Idle** — listening, no charger connected (200). The normal state when no
  vehicle is plugged in; deliberately *not* a fault
- **Degraded** — upstream reconnecting within its window, or MQTT down (200)
- **Unhealthy** — not listening, or upstream failed past its reconnection
  window while a charger is connected (503)

## Docker

```bash
docker build -t ocpp-proxy .
docker run -p 9000:9000 -p 8080:8080 \
  -v /path/to/certs:/certs/mqtt:ro \
  -e OCPP_PROXY_CENTRAL_SYSTEM_URL=wss://central-system.example.com/ocpp/v16 \
  -e OCPP_PROXY_LISTEN_PORT=9000 \
  -e OCPP_PROXY_MQTT__HOST=mqtt.example.com \
  -e OCPP_PROXY_MQTT__PORT=8883 \
  -e OCPP_PROXY_MQTT__USERNAME=ocpp_proxy \
  -e OCPP_PROXY_MQTT__PASSWORD=secret \
  -e OCPP_PROXY_MQTT__CA_CERT_PATH=/certs/mqtt/ca.pem \
  -e OCPP_PROXY_MQTT__CLIENT_CERT_PATH=/certs/mqtt/client.pem \
  -e OCPP_PROXY_MQTT__CLIENT_KEY_PATH=/certs/mqtt/client-key.pem \
  ocpp-proxy
```

The image uses a multi-stage build (Rust 1.82 builder → Debian bookworm-slim runtime) and runs as a non-root user.

## Deployment

The proxy runs as an unprivileged LXC container on the Proxmox host, supervised
by systemd, with the 4G dongle owned by the host and reached by policy routing.
Full provisioning runbook, host networking, and systemd units:
[`deploy/lxc/`](deploy/lxc/README.md).

> The charger's only path to Mobi.e runs through this container. If the Proxmox
> host is down, charging and billing stop — see *Availability posture* in
> [the requirements](.kiro/specs/ocpp-proxy-ha-integration/requirements.md).

## Development

```bash
# Run all tests (unit + property-based)
cargo test

# Run a specific property test
cargo test --test property_frame_parsing

# Lint
cargo clippy

# Format
cargo fmt --check

# Build with debug logging
RUST_LOG=debug cargo run
```

### Testing Approach

- **Unit tests** — inline in each module, covering validation, state machines, and serialization
- **Property-based tests** — in `tests/property_*.rs` using proptest, validating correctness invariants like byte-for-byte preservation, buffer eviction fairness, and protocol compliance

## Architecture

The proxy is structured around independent async tasks communicating via channels:

1. **Downstream server** (axum) — accepts charger WebSocket connections, validates `ocpp1.6` subprotocol
2. **Upstream client** (tokio-tungstenite) — maintains connection to Central System per Charge Point ID
3. **Forwarder** — priority message routing path, buffers when destinations unavailable
4. **MQTT publisher** — runs on a dedicated OS thread, publishes events after forwarding completes
5. **Health server** (axum) — reports connection states and message counters
6. **Shutdown coordinator** — handles SIGTERM/SIGINT with graceful drain sequence

Key design decisions:
- MQTT publishing never blocks the forwarding path (async channel with try_send)
- MQTT runs on a separate OS thread because rumqttc's EventLoop is `!Send`
- Messages are forwarded as raw bytes — the proxy never re-serializes JSON
- Connection replacement: a new charger connection for the same ID closes the existing one
- Mobi.e egress is bound to a dedicated source address so host policy routing sends it over the APN, while MQTT and health traffic stay on the LAN

## License

MIT — see [LICENSE](LICENSE).
