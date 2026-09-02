# Requirements Document

## Introduction

This document specifies requirements for an OCPP Proxy that sits between an Autel EV charger and the Mobi.e Central System in Portugal. The proxy transparently forwards all OCPP 1.6J WebSocket messages between the charger and Mobi.e while capturing OCPP events and publishing them to MQTT for Home Assistant integration. The primary constraint is that Mobi.e communication must never be disrupted — the proxy must be invisible to both endpoints and prioritize upstream connectivity above all else.

The Proxy runs as a dedicated unprivileged LXC container on the existing Proxmox host (`mouraishikawa`, `192.168.50.10`). It reaches the Mobi.e Central System over a mobile APN served by a ZTE 4G USB dongle attached to the Proxmox host, and reaches both the Charger and the MQTT_Broker over the local network.

### Availability posture

An earlier revision of this document placed the Proxy on AWS ECS Fargate and justified that with availability: the charger would keep talking to Mobi.e even when the local network was down. **That rationale is now inverted and must not be carried over.** With the APN SIM in the dongle on the Proxmox host, the Proxy is the Charger's *only* path to Mobi.e. Consequences that these requirements must address explicitly:

- Loss of the Proxmox host, the LXC, the dongle, or the APN link stops charging authorization and billing, not merely Home Assistant visibility.
- There is no automatic failover. Recovery is local resilience (restart-on-failure, start-on-boot, link watchdog) plus a documented manual bypass.
- Host maintenance that reboots Proxmox will drop live charging sessions. This is a planned-work constraint, not a fault.

The Proxy is therefore designed to fail *closed and loudly* — it must make its own unavailability obvious to Home Assistant — rather than to pretend a degraded path is healthy.

## Glossary

- **Proxy**: The OCPP Proxy application that relays WebSocket messages between the Charger and the Central_System
- **Charger**: The Autel EV charger acting as an OCPP 1.6J Charge Point client
- **Central_System**: The Mobi.e OCPP Central System server that manages charging sessions and billing
- **MQTT_Broker**: The EMQX MQTT broker running as a Home Assistant add-on on VM 110 (`homeassistant`, HAOS) at `192.168.50.167`
- **Home_Assistant**: The home automation platform running on HAOS as VM 110 on the same Proxmox host
- **Proxmox_Host**: The Proxmox VE host `mouraishikawa` (`192.168.50.10`) running the Proxy LXC and owning the 4G dongle
- **Proxy_LXC**: The dedicated unprivileged LXC container running the Proxy
- **WWAN_Link**: The network interface presented by the ZTE 4G USB dongle on the Proxmox_Host, carrying the APN path to the Central_System
- **APN**: The mobile access point name, served by the SIM in the dongle, through which the Central_System is reachable
- **OCPP_Message**: A JSON-formatted message conforming to the OCPP 1.6J specification transmitted over WebSocket
- **Upstream_Connection**: The WebSocket connection from the Proxy to the Central_System
- **Downstream_Connection**: The WebSocket connection from the Charger to the Proxy
- **Charge_Point_ID**: The unique identifier of the Charger as registered with Mobi.e
- **MQTT_Topic**: A hierarchical string used to route messages within the MQTT_Broker

## Requirements

### Requirement 1: WebSocket Server for Charger Connection

**User Story:** As a charger owner, I want the proxy to accept my charger's OCPP WebSocket connection, so that the charger can communicate through the proxy instead of directly to Mobi.e.

#### Acceptance Criteria

1. THE Proxy SHALL accept incoming WebSocket connections from the Charger on a configurable port (range 1024–65535) at the URL path `/{Charge_Point_ID}` using the OCPP 1.6J subprotocol
2. WHEN the Charger initiates a WebSocket connection, THE Proxy SHALL complete the WebSocket handshake within 5 seconds
3. THE Proxy SHALL support the `ocpp1.6` WebSocket subprotocol as defined in the OCPP 1.6J specification
4. WHEN a new connection attempt arrives from a Charger whose Charge_Point_ID already has an active Downstream_Connection, THE Proxy SHALL close the existing connection with a WebSocket close frame and accept the new connection as the active Downstream_Connection
5. IF the Charger connection fails the WebSocket handshake, THEN THE Proxy SHALL log the failure reason and close the TCP connection after sending a WebSocket close frame with an appropriate status code
6. IF the Charger requests a WebSocket subprotocol other than `ocpp1.6`, THEN THE Proxy SHALL reject the connection by completing the handshake without selecting a subprotocol and closing the connection

### Requirement 2: WebSocket Client for Central System Connection

**User Story:** As a charger owner, I want the proxy to connect to Mobi.e on behalf of my charger, so that billing and session management continue to work without interruption.

#### Acceptance Criteria

1. WHEN the Charger establishes a Downstream_Connection, THE Proxy SHALL initiate an Upstream_Connection to the Central_System using the configured Mobi.e WebSocket URL
2. THE Proxy SHALL use the same Charge_Point_ID in the Upstream_Connection URL path as received from the Charger
3. THE Proxy SHALL forward the same WebSocket subprotocol header to the Central_System as received from the Charger
4. IF the Upstream_Connection cannot be established within 10 seconds, THEN THE Proxy SHALL retry indefinitely with exponential backoff starting at 2 seconds, doubling on each attempt, up to a maximum interval of 60 seconds
5. IF the Upstream_Connection is lost, THEN THE Proxy SHALL attempt reconnection using exponential backoff starting at 2 seconds up to a maximum interval of 60 seconds while keeping the Downstream_Connection open for up to 5 minutes
6. IF the Upstream_Connection cannot be re-established within 5 minutes, THEN THE Proxy SHALL close the Downstream_Connection with WebSocket close code 1001 and log the disconnection reason
7. THE Proxy SHALL establish the Upstream_Connection with its outbound socket bound to a configurable local source address, so that host policy routing directs Central_System traffic over the WWAN_Link and not over the default LAN route
8. IF the configured upstream source address is not present on any local interface at startup, THEN THE Proxy SHALL fail to start and log the missing address, rather than silently falling back to the default route
9. THE Proxy SHALL NOT route MQTT or health-endpoint traffic over the WWAN_Link; only Central_System traffic uses that path

### Requirement 3: Transparent Message Forwarding

**User Story:** As a charger owner, I want all OCPP messages forwarded transparently between my charger and Mobi.e, so that charging sessions, billing, and remote operations work exactly as if the proxy were not present.

#### Acceptance Criteria

1. WHEN the Proxy receives an OCPP_Message from the Charger, THE Proxy SHALL forward the message to the Central_System without modification within 100 milliseconds
2. WHEN the Proxy receives an OCPP_Message from the Central_System, THE Proxy SHALL forward the message to the Charger without modification within 100 milliseconds
3. THE Proxy SHALL preserve the exact JSON payload of each OCPP_Message during forwarding, including field order and whitespace
4. THE Proxy SHALL forward OCPP_Messages in the same order they were received on each direction of the connection
5. IF the destination WebSocket connection is in a disconnected state with reconnection in progress, THEN THE Proxy SHALL buffer up to 100 messages for a maximum of 30 seconds, and discard the oldest messages first when either limit is exceeded, logging each discarded message at WARNING level with the message unique ID
6. THE Proxy SHALL support all OCPP 1.6J message types including Call, CallResult, and CallError frames
7. IF the Downstream_Connection is lost while the Proxy holds buffered messages from the Central_System, THEN THE Proxy SHALL discard the buffered Central_System messages and log the count of discarded messages at WARNING level

### Requirement 4: Mobi.e Communication Priority

**User Story:** As a charger owner, I want Mobi.e communication to always take priority, so that my billing and charging session management are never disrupted by proxy features.

#### Acceptance Criteria

1. WHEN the Proxy receives an OCPP_Message, THE Proxy SHALL complete forwarding to the destination before initiating any MQTT publishing or internal processing for that message
2. IF the MQTT_Broker is unreachable, THEN THE Proxy SHALL continue forwarding OCPP_Messages between the Charger and Central_System with no additional latency beyond the 100-millisecond forwarding threshold defined in Requirement 3
3. IF internal processing of an OCPP_Message causes an error, THEN THE Proxy SHALL forward the original message unmodified to the destination and log the processing error separately without delaying the forwarding operation
4. THE Proxy SHALL execute MQTT publishing asynchronously from the OCPP message forwarding path so that MQTT operations add no more than 5 milliseconds of latency to any OCPP_Message forwarding operation
5. WHILE the Upstream_Connection is being re-established, THE Proxy SHALL buffer up to 100 Charger messages for a maximum of 30 seconds and deliver them to the Central_System in order once the connection is restored, discarding the oldest messages if the buffer is full

### Requirement 5: OCPP Event Publishing to MQTT

**User Story:** As a Home Assistant user, I want all OCPP events published to MQTT, so that I can monitor charger status, energy consumption, and charging sessions in my dashboard.

#### Acceptance Criteria

1. WHEN the Proxy forwards an OCPP_Message, THE Proxy SHALL publish the message content to the MQTT_Broker within 500 milliseconds of forwarding on a structured MQTT_Topic
2. THE Proxy SHALL publish MQTT messages using the topic format `ocpp/{Charge_Point_ID}/{direction}/{action}` where direction is `charger` or `central_system` and action is the OCPP message action name (e.g., BootNotification, MeterValues, StatusNotification) in the case of Call messages, or the action of the originating Call for CallResult and CallError messages
3. THE Proxy SHALL publish each OCPP event as a JSON object containing: a `timestamp` field in ISO 8601 format representing the time the Proxy received the OCPP_Message, a `message_type` field indicating `Call`, `CallResult`, or `CallError`, and a `payload` field containing the full original JSON payload of the OCPP_Message
4. WHEN the Proxy publishes to the MQTT_Broker, THE Proxy SHALL use QoS level 1 to ensure at-least-once delivery
5. IF the MQTT_Broker is unreachable, THEN THE Proxy SHALL buffer up to 500 MQTT messages in FIFO order and publish them when the connection is restored, discarding the oldest messages first when the buffer is full
6. WHEN either the Upstream_Connection or Downstream_Connection state changes, THE Proxy SHALL publish a retained status message to `ocpp/{Charge_Point_ID}/status` containing a JSON object with the connection state of both connections, where state is one of `connected`, `disconnected`, or `reconnecting`

### Requirement 6: MQTT Connection Management

**User Story:** As a Home Assistant user, I want the proxy to maintain a reliable connection to my MQTT broker over the internet, so that I receive charger events consistently in my local Home Assistant.

#### Acceptance Criteria

1. THE Proxy SHALL connect to the MQTT_Broker using configurable host, port, username, and password parameters
2. WHEN the Proxy starts, THE Proxy SHALL attempt to establish a connection to the MQTT_Broker within 10 seconds, and IF the connection is not established within 10 seconds, THEN THE Proxy SHALL log the failure and retry using the reconnection backoff strategy defined in criterion 3
3. IF the MQTT_Broker connection is lost, THEN THE Proxy SHALL attempt reconnection indefinitely using exponential backoff starting at 1 second up to a maximum interval of 30 seconds
4. THE Proxy SHALL configure an MQTT Last Will and Testament message with topic `ocpp/{Charge_Point_ID}/availability`, payload `offline`, QoS level 1, and the retained flag set to true, so that the MQTT_Broker publishes it upon unexpected disconnection
5. WHEN the Proxy connects to the MQTT_Broker, THE Proxy SHALL publish a retained message to `ocpp/{Charge_Point_ID}/availability` with payload `online` using QoS level 1
6. WHERE TLS is enabled for the MQTT connection, THE Proxy SHALL use a minimum version of TLS 1.2 and SHALL verify the broker's server certificate against the configured certificate authority
6a. THE Proxy SHALL treat MQTT TLS as OPTIONAL. The MQTT_Broker is reached over a single local-network hop to VM 110, not across the internet, so TLS and client certificates are not required. IF no CA certificate path is configured, THEN THE Proxy SHALL connect without TLS using username and password authentication only
6b. IF a CA certificate path is configured without both a client certificate and client key path, THEN THE Proxy SHALL connect using server-authenticated TLS without client authentication
7. THE Proxy SHALL configure the MQTT connection with a keepalive interval of 60 seconds to enable timely detection of connection loss over the internet

### Requirement 7: Configuration Management

**User Story:** As a system administrator, I want to configure the proxy through environment variables or a configuration file, so that I can deploy and adjust settings without modifying code.

#### Acceptance Criteria

1. THE Proxy SHALL read configuration from environment variables with fallback to a YAML configuration file, where environment variables take precedence over YAML values on a per-parameter basis
2. THE Proxy SHALL require the following configuration parameters: Central_System WebSocket URL, Proxy listen port, MQTT_Broker host, MQTT_Broker port, MQTT username, and MQTT password. TLS certificate paths for MQTT are OPTIONAL per Requirement 6
3. IF a required configuration parameter is missing, THEN THE Proxy SHALL fail to start and log all missing parameters in a single error output
4. THE Proxy SHALL support optional configuration parameters with the following defaults: log level (default: INFO), message buffer size (default: 100 messages), MQTT message buffer size (default: 500 messages), reconnection maximum backoff (default: 60 seconds), and charger-facing listen address (default: 0.0.0.0)
4a. THE Proxy SHALL apply the configured reconnection maximum backoff to both the Upstream_Connection and the MQTT connection rather than using hard-coded maxima
4b. THE Proxy SHALL support an optional `upstream_bind_address` parameter naming the local source address used for the Upstream_Connection, as required by Requirement 2 criterion 7
5. THE Proxy SHALL validate all configuration parameters at startup before accepting connections, verifying that: port numbers are integers between 1 and 65535, URLs conform to the WebSocket URI format (ws:// or wss://), file paths for TLS certificates point to existing readable files, and log level is one of DEBUG, INFO, WARNING, or ERROR
6. THE Proxy SHALL load MQTT credentials from environment variables supplied by the systemd unit's `EnvironmentFile`, which SHALL be readable only by the service user. No secret SHALL appear in the YAML configuration file, in the LXC configuration, or in this repository
7. THE Proxy SHALL look for the YAML configuration file at the path specified by a `CONFIG_FILE_PATH` environment variable, falling back to `./config.yaml` in the working directory if the variable is not set

### Requirement 8: Logging and Observability

**User Story:** As a system administrator, I want comprehensive logging from the proxy, so that I can diagnose connection issues and monitor proxy health.

#### Acceptance Criteria

1. WHEN either the Upstream_Connection or Downstream_Connection changes state (connecting, connected, disconnected, or reconnecting), THE Proxy SHALL log the state transition including the previous state, new state, connection identifier, and timestamp
2. THE Proxy SHALL log OCPP_Message summaries at DEBUG level including message type, action, and unique ID without logging full message payloads at INFO level
3. IF an error occurs during message forwarding, connection handling, or MQTT publishing, THEN THE Proxy SHALL log the error at ERROR level including the connection identifier, message unique ID if applicable, error category, and a description of the failure
4. THE Proxy SHALL support configurable log levels: DEBUG, INFO, WARNING, and ERROR, with INFO as the default level when not explicitly configured
5. THE Proxy SHALL output logs in structured JSON format to stdout, where each log entry contains at minimum: timestamp in ISO 8601 format, log level, component name, message text, and a correlation identifier linking related events for the same connection
6. IF message forwarding latency exceeds 500 milliseconds, THEN THE Proxy SHALL log a warning with the measured latency and message identifier

### Requirement 9: Graceful Startup and Shutdown

**User Story:** As a system administrator, I want the proxy to start and stop gracefully, so that no messages are lost during maintenance operations.

#### Acceptance Criteria

1. WHEN the Proxy receives a termination signal (SIGTERM or SIGINT), THE Proxy SHALL stop accepting new connections and complete forwarding of in-flight messages within 10 seconds before shutting down, and if messages remain undelivered after 10 seconds, THE Proxy SHALL discard remaining messages, log the count of discarded messages, and proceed with shutdown
2. WHEN the Proxy starts, THE Proxy SHALL attempt to establish the MQTT_Broker connection for up to 10 seconds, then begin listening for Charger connections regardless of whether the MQTT connection succeeded
3. IF the Proxy cannot establish the MQTT_Broker connection within the 10-second startup timeout, THEN THE Proxy SHALL proceed to accept Charger connections and retry the MQTT connection using the reconnection strategy defined in Requirement 6
4. WHEN the Proxy shuts down, THE Proxy SHALL close the Upstream_Connection and Downstream_Connection by sending a WebSocket close frame with status code 1000 (Normal Closure) and waiting up to 5 seconds for each close acknowledgment before terminating the connection
5. THE Proxy SHALL log the startup sequence completion and the total time taken to reach the ready state, where ready is defined as the Proxy actively listening for Charger connections on the configured port

### Requirement 10: Health Monitoring

**User Story:** As a Home Assistant user, I want to know the proxy's health status, so that I can set up alerts when something goes wrong.

> **Revised from the ECS design.** The previous criteria reported `unhealthy` (HTTP 503) whenever no Charger was connected. Combined with an orchestrator that restarts on three consecutive failed checks, that made an idle proxy — the normal state when no car is plugged in — restart forever. Health now describes *the Proxy's ability to serve*, not whether a Charger happens to be connected.

#### Acceptance Criteria

1. THE Proxy SHALL expose a health check HTTP endpoint on a configurable port (default: 8080) at the path `/health` that returns the current health status and connection details
2. WHEN queried, THE health check endpoint SHALL return a JSON response within 2 seconds containing: Upstream_Connection state, Downstream_Connection state, MQTT_Broker connection state, WWAN_Link reachability, uptime in seconds, and message counters for forwarded and dropped messages in each direction
3. THE health endpoint SHALL report live values read from the same state used by the forwarding path. It SHALL NOT report from a separate or duplicated state object
4. IF the Proxy is listening on the configured charger port and no Charger is currently connected, THEN THE Proxy SHALL report health status as `idle` and return HTTP status code 200. This is the expected steady state when no vehicle is charging and SHALL NOT be treated as a fault
5. IF a Charger is connected and the Upstream_Connection is established, THEN THE Proxy SHALL report health status as `healthy` and return HTTP status code 200
6. IF a Charger is connected and the Upstream_Connection is reconnecting within its 5-minute window, THEN THE Proxy SHALL report health status as `degraded` and return HTTP status code 200
7. IF the MQTT_Broker connection is lost while OCPP forwarding is otherwise working, THEN THE Proxy SHALL report health status as `degraded` and return HTTP status code 200, because MQTT loss costs visibility but never charging
8. IF the Proxy cannot bind or has stopped listening on the configured charger port, OR a Charger is connected and the Upstream_Connection has failed beyond its 5-minute reconnection window, THEN THE Proxy SHALL report health status as `unhealthy` and return HTTP status code 503
9. THE Proxy SHALL increment the message counters reported by the health endpoint on every forwarded and every dropped message, in both directions

### Requirement 11: Proxmox LXC Deployment and Local Resilience

**User Story:** As a charger owner, I want the proxy to run on my own Proxmox host and recover from failure without me noticing, so that charging and billing keep working and I am told when they cannot.

> **Replaces the previous AWS ECS Fargate requirement in full.** There is no container orchestrator, no load balancer, no Elastic IP, no AWS Certificate Manager, no EFS, and no AWS Secrets Manager. Availability is now local resilience plus alerting, and the Proxy is a single point of failure by design — see *Availability posture* in the Introduction.

#### Acceptance Criteria

1. THE Proxy SHALL run as a dedicated unprivileged LXC container on the Proxmox_Host, with `onboot: 1` so it starts automatically when the host boots
2. THE Proxy SHALL be deployed as a single native binary managed by a systemd unit inside the Proxy_LXC, with `Restart=always` and a restart delay of 5 seconds, so that a crash is recovered without external orchestration
3. THE Proxy SHALL store no state on the LXC filesystem beyond its binary, configuration, and logs, so that the container can be rebuilt from configuration alone
4. THE Proxy SHALL become ready to accept Charger connections within 30 seconds of container start
5. THE Proxy SHALL bind its charger-facing listener to a static address on the local network that does not change across host reboots or container restarts, so the Charger's configured URL remains valid
6. THE Proxmox_Host SHALL own the WWAN_Link. The dongle SHALL NOT be passed through into the Proxy_LXC, and the Proxy_LXC SHALL remain unprivileged
7. THE Proxmox_Host SHALL name the WWAN interface deterministically by USB vendor and product ID rather than by MAC address, because the dongle presents a placeholder MAC before SIM registration and the kernel's MAC-derived `enx*` name is therefore unstable
8. THE Proxmox_Host SHALL NOT accept a default route from the WWAN_Link. The LAN default route SHALL remain the host's only default route, and Central_System traffic SHALL reach the WWAN_Link by policy routing on the Proxy's upstream source address
9. THE Proxmox_Host SHALL clamp TCP MSS to path MTU on the WWAN path, because mobile APNs commonly present an MTU below 1500 and an unclamped WebSocket connection will stall on large frames
10. THE Proxmox_Host SHALL run a periodic watchdog that verifies the WWAN_Link is up and carries a route to the Central_System, re-establishes it when it is not, and logs every corrective action to syslog
11. IF the WWAN_Link is down, THEN the Proxy SHALL continue to accept the Downstream_Connection and SHALL report the condition through both the health endpoint and MQTT, rather than refusing the Charger outright
12. THE Proxy SHALL publish its own availability to MQTT such that Home_Assistant can alert on the Proxy being offline, since no orchestrator is watching it
13. A documented manual bypass procedure SHALL exist for restoring the Charger's connectivity to Mobi.e when the Proxy or the Proxmox_Host is unavailable for an extended period
14. THE Charger SHALL reach the Proxy over the local network only. No inbound path from the internet to the Proxy SHALL be created, and the Proxy SHALL NOT be published through the existing Cloudflare Tunnel infrastructure

### Requirement 12: Network Placement and Isolation

**User Story:** As the operator of this network, I want the charger's presence not to undo the IoT isolation work already done, so that an internet-capable appliance and the physically accessible Ethernet jack beside it cannot reach my infrastructure.

> **Resolved in favour of isolation.** The TP-Link TL-WPA4220 powerline uplink was moved from the main router to a LAN port of the spare BD4, so the charger and the powerline's own Ethernet ports now land on the isolated IoT network instead of the main LAN. The decision was forced by a property of the hardware: the TL-WPA4220 has no port disable, and because a main-LAN charger would share a layer-2 segment with the infrastructure, no router firewall rule could have constrained it — intra-subnet traffic never reaches the router.

#### Acceptance Criteria

1. THE Charger SHALL connect through the TL-WPA4220 powerline access point, whose uplink terminates on a LAN port of the spare BD4, placing it in `192.168.51.0/24`
2. THE Proxy_LXC SHALL carry an interface on `vmbr1` (`192.168.52.0/24`) and SHALL bind its charger-facing listener to that address, reusing the path Home Assistant and Frigate already use
3. THE existing isolation rule (`192.168.51.0/24 -d 192.168.50.0/24 -j DROP`, reapplied by `iot-isolation-enforce.sh`) SHALL remain unmodified. The Charger reaches the Proxy at a `192.168.52.0/24` address, which that rule does not match, so no exception is required
4. THE Charger SHALL be given a DHCP reservation on the spare BD4 so its address is stable
5. THE design SHALL accept that the Charger-to-Proxy path now crosses the Proxmox host's WiFi client association (`wlp129s0f0`) to the spare router. This places a wireless link on the Charger's only path to Mobi.e. Bandwidth is not a concern for OCPP, but the association's stability is now a charging dependency and SHALL be monitored
6. THE powerline adapters SHALL be configured with a non-default powerline network name. Powerline is a shared medium: any adapter on the same electrical circuit that knows the default key, or that is paired by button press, joins the same layer-2 segment without needing physical access to the unit
7. Unused Ethernet ports on the TL-WPA4220 SHOULD be physically blocked. The model has no software port control, so this is the only available mitigation for the jack itself
8. THE Proxy SHALL NOT be reachable from the main LAN except for administrative access and its MQTT client traffic

### Requirement 13: APN Path Characteristics

**User Story:** As the operator, I want the constraints of the mobile path recorded, so that nobody later assumes it behaves like an ordinary internet connection.

> Measured 2026-08-31 against the ZTE dongle with the SIM installed, the SIM operator, LTE, `ppp_status: ppp_connected`, full signal.

#### Acceptance Criteria

1. THE APN is a closed network. Generic internet egress is blocked: ICMP to `1.1.1.1` and TCP to public HTTP endpoints both fail while the modem reports a healthy connected session
2. THE APN provides no usable DNS. The dongle offers no DNS server in its DHCP lease, does not proxy port 53 on its own gateway address, and public resolvers are unreachable over the path
3. THE Central_System endpoint is `$CENTRAL_SYSTEM_URL/{Charge_Point_ID}`, with Charge_Point_ID `$CHARGE_POINT_ID`. It is a literal RFC1918 address, so the absence of DNS on the mobile path costs nothing and no resolver configuration is required
4. THE Central_System connection is plaintext `ws://` on port 80. The private APN is the entire security boundary between the Proxy and Mobi.e. This is the Charger's pre-existing arrangement and SHALL NOT be presented as introduced by this design; the Upstream_Connection consequently requires no TLS
5. THE `$CENTRAL_SYSTEM_NETWORK` range SHALL be routed over the WWAN_Link by destination route on both the Proxmox_Host and inside the Proxy_LXC. The range collides with no local network. This supersedes the source-address policy-routing scheme in Requirement 2 criterion 7, which is retained only as an optional measure should Mobi.e later move to a hostname or a wider range
6. THE absence of generic internet egress SHALL NOT be treated as a fault. It is the expected behaviour of a private APN and confirms the SIM is provisioned for Mobi.e traffic only
7. Reachability of the WWAN_Link SHALL be tested by TCP connection to `$CENTRAL_SYSTEM_HOST:80`, not against a public host. ICMP to the Central_System does work (measured at 79 ms), but an open TCP port additionally proves the Central_System is listening
9. THE full path was verified 2026-08-31 from the Proxmox_Host: TCP open, `GET /` returning 200, and a WebSocket upgrade returning `101 Switching Protocols` from `nginx/1.6.2` with `ocpp1.6` negotiated
10. A successful WebSocket upgrade SHALL NOT be read as confirmation that the Charge_Point_ID is registered. Controls established that the endpoint returns 101 for an invalid Charge_Point_ID and for a request with no subprotocol; authorization occurs above the handshake. Verifying registration requires an OCPP `BootNotification` exchange
11. THE Central_System accepts any Charge_Point_ID over plaintext HTTP, so the private APN is the sole access control on this path. Nothing SHALL bridge the WWAN_Link to a general-purpose network
8. THE Charger's original Mobi.e endpoint is now recorded in this document and in `deploy/lxc/guest/config.yaml`, satisfying the prerequisite for the manual bypass in Requirement 11 criterion 13
