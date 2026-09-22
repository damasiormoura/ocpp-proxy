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
- **Charger control (opt-in)** — set a charging-current limit from Home Assistant; the proxy sends `SetChargingProfile` to the charger and keeps the Central System out of it. See [Charger control](#charger-control)
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
| `charging.enabled` | No | false | Accept charger commands over MQTT. Off, the proxy never originates an OCPP message |
| `charging.max_limit_a` | No | 32 | Ceiling on any limit sent; higher requests are clamped. Set it under the site's contracted current |
| `charging.min_limit_a` | No | 6 | Requests below this pause charging (0 A) instead — an EV will not charge under 6 A |
| `charging.connector_id` | No | 0 | Connector the profile targets; 0 = whole charge point |
| `charging.profile_id` / `stack_level` | No | 1001 / 0 | Identity of the proxy's profile on the charger |
| `charging.purpose` | No | TxDefaultProfile | `TxDefaultProfile` or `ChargePointMaxProfile` |
| `charging.profile_kind` | No | Absolute | `Absolute` (startSchedule = now) or `Relative` |
| `charging.rate_unit` | No | A | `A`, or `W` for chargers that only accept power limits |
| `charging.number_phases` | No | 1 | `numberPhases` on the schedule period; `null` omits it |
| `charging.nominal_voltage_v` | No | 230 | Amps→watts conversion when `rate_unit` is `W` |
| `charging.command_timeout_seconds` | No | 30 | Unsent or unanswered commands are reported as timed out after this |
| `charging.reapply_on_connect` | No | true | Re-send the standing limit when the charger reconnects |
| `charging.allowed_configuration_keys` | No | MeterValue* keys | What `change_configuration` may touch |

See [`config.yaml.example`](config.yaml.example) for a fully documented example.

## MQTT Topics

The proxy publishes to the following topic structure:

| Topic | Payload | Retain |
|-------|---------|--------|
| `ocpp/{id}/charger/{action}` | OCPP message from charger | No |
| `ocpp/{id}/central_system/{action}` | OCPP message from central system | No |
| `ocpp/{id}/status` | `{"upstream": "...", "downstream": "..."}` | Yes |
| `ocpp/{id}/availability` | `"online"` or `"offline"` (LWT) | Yes |
| `ocpp/{id}/state` | Charge point snapshot: status, transaction, previous session, standing current limit | Yes |
| `ocpp/{id}/command/result` | Outcome of a charger command (see below) | No |

And, when `charging.enabled` is set, it **subscribes** to:

| Topic | Payload |
|-------|---------|
| `ocpp/{id}/command` | JSON command object, see [Charger control](#charger-control) |
| `ocpp/{id}/command/current_limit` | A bare number of amps, or `clear` — what a Home Assistant `number` publishes |

Message payloads include a timestamp, message type, and the full original OCPP JSON:

```json
{
  "timestamp": "2026-01-15T10:30:00.123Z",
  "message_type": "Call",
  "payload": [2, "abc123", "BootNotification", {"chargePointModel": "Model"}]
}
```

## Charger control

By default the proxy is a pipe: it never originates an OCPP message. With
`charging.enabled: true` it gains one deliberate exception. On a request from
MQTT it sends a small set of Calls **to the charger**, consumes the charger's
answer, and reports the outcome. The Central System never sees a proxy Call or
its reply, and nothing the proxy does alters what either endpoint sends.

The point is load management. A home on a 6.9 kVA single-phase contract has
30 A in total; a 32 A charger can take all of it by itself, and a 16 A charge
plus an oven already reaches the limit. `SetChargingProfile` is OCPP 1.6's
mechanism for capping what the charger offers the car, and this lets Home
Assistant drive it — by hand from a slider, or from an automation watching the
mains meter.

### Commands

Publish a JSON object to `ocpp/{id}/command`. `id` is optional and is echoed
in the result.

| `command` | Fields | OCPP action | What it does |
|-----------|--------|-------------|--------------|
| `set_current_limit` | `limit_a` | `SetChargingProfile` | Cap the current. `0` pauses charging (the charger reports `SuspendedEVSE`, the transaction stays open). Below `min_limit_a` is sent as `0`; above `max_limit_a` is clamped |
| `clear_current_limit` | — | `ClearChargingProfile` | Remove the proxy's profile; the charger returns to its own maximum |
| `get_configuration` | `keys` (optional list) | `GetConfiguration` | Read the charger's settings. Start here: `SupportedFeatureProfiles`, `ChargingScheduleAllowedChargingRateUnit`, `ChargeProfileMaxStackLevel`, `MeterValueSampleInterval` |
| `change_configuration` | `key`, `value` | `ChangeConfiguration` | Change one setting, restricted to `charging.allowed_configuration_keys` — by default the MeterValues cadence and contents, which a controller may want faster or richer |
| `trigger_message` | `requested_message`, `connector_id` (optional) | `TriggerMessage` | Ask for a `MeterValues` or `StatusNotification` now. The triggered message goes to the Central System as usual, and onto MQTT with it |
| `get_composite_schedule` | `connector_id`, `duration_s` (optional) | `GetCompositeSchedule` | Read back the limit the charger is actually applying after stacking every profile it holds |

The convenience topic `ocpp/{id}/command/current_limit` takes a bare number
(`16`, `16.0`) or `clear`, so a Home Assistant `number` entity can publish to
it directly.

Every command ends in exactly one message on `ocpp/{id}/command/result`:

```json
{
  "id": "cmd-1",
  "command": {"command": "set_current_limit", "limit_a": 40},
  "source": "mqtt",
  "status": "accepted",
  "action": "SetChargingProfile",
  "requested_limit_a": 40,
  "applied_limit_a": 20,
  "ocpp_request": [2, "proxy-1758560000000-1", "SetChargingProfile", {"...": "..."}],
  "charger_response": {"status": "Accepted"},
  "detail": "clamped to the configured ceiling of 20 A",
  "timestamp": "2026-09-22T18:00:00.123Z"
}
```

`status` is one of `accepted`, `rejected` (the charger said no, or the proxy
refused: queue full), `error` (a `CallError`), `timeout`, `not_connected`,
`invalid` (unparseable or failed validation) or `superseded` (a newer limit
arrived before this one was sent). `Unknown` to a clear and `RebootRequired`
to a configuration change both count as `accepted`: the desired state holds.

The retained snapshot on `ocpp/{id}/state` gains `current_limit_a` (the last
accepted limit, `null` when none), `current_limit_status` and
`current_limit_updated`, so a dashboard shows the standing limit the moment it
subscribes — and shows a limit that did *not* take, rather than silently
keeping the old figure.

### Rules the proxy keeps

- **One proxy Call in flight at a time**, and none while the Central System
  has a Call to the charger that is less than 10 s old and unanswered: OCPP
  1.6 allows a Charge Point one outstanding request. Proxy Calls wait, and the
  Central System's traffic is never delayed for them.
- **Responses are matched by unique id.** Proxy Calls carry ids prefixed
  `proxy-`; a response with one of those is consumed, everything else is
  forwarded as before.
- **Queued limit commands coalesce**, last one wins. A controller that
  publishes every few seconds does not build a backlog.
- **The standing limit is re-applied** when the charger and the Central
  System are both connected again (`reapply_on_connect`), because a charger
  that rebooted may have forgotten its profiles, and forgetting means full
  current.
- **Nothing here is on the forwarding path.** Commands and results ride the
  same non-blocking channel as every other MQTT event.

### First use

1. Enable `charging` with a conservative `max_limit_a` and deploy.
2. Publish `{"command": "get_composite_schedule"}`. A charger that answers
   `Accepted` with a schedule supports SmartCharging, and the schedule is
   the ceiling it applies today — the Autel MaxiCharger this was built
   against reported 28 A on connector 0, single phase, before any proxy
   profile existed. `{"command": "get_configuration"}` is the textbook
   probe (`SupportedFeatureProfiles`, `ChargingScheduleAllowedChargingRateUnit`,
   `ChargeProfileMaxStackLevel`), but that same Autel answers it with an
   empty `{}`, keys named or not, so do not rely on it alone.
3. With a car charging, set a limit below the current draw and watch
   `Power.Active.Import` in `MeterValues` fall within a few seconds. If the
   charger answers `Rejected` or `NotSupported`, try
   `purpose: ChargePointMaxProfile` — or the other way round.
4. `get_composite_schedule` shows what the charger believes it is applying.

The Home Assistant package in [`deploy/homeassistant/`](deploy/homeassistant/README.md)
wires all of this up as a slider, a status sensor and buttons.

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
4. **MQTT publisher** — runs on a dedicated OS thread, publishes events after forwarding completes; when charger control is enabled it also subscribes to the command topics and hands commands to the right session through the `CommandRouter`
5. **Health server** (axum) — reports connection states and message counters
6. **Shutdown coordinator** — handles SIGTERM/SIGINT with graceful drain sequence

Key design decisions:
- MQTT publishing never blocks the forwarding path (async channel with try_send)
- MQTT runs on a separate OS thread because rumqttc's EventLoop is `!Send`
- Messages are forwarded as raw bytes — the proxy never re-serializes JSON
- The proxy originates OCPP only when asked to (`charging.enabled`), only towards the charger, and consumes the replies to its own Calls so the Central System never sees them
- Connection replacement: a new charger connection for the same ID closes the existing one
- Mobi.e egress is bound to a dedicated source address so host policy routing sends it over the APN, while MQTT and health traffic stay on the LAN

## License

MIT — see [LICENSE](LICENSE).
