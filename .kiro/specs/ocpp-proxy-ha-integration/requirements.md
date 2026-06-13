# Requirements Document

## Introduction

This document specifies requirements for an OCPP Proxy hosted on AWS that sits between an Autel EV charger and the Mobi.e Central System in Portugal. The proxy transparently forwards all OCPP 1.6J WebSocket messages between the charger and Mobi.e while capturing OCPP events and publishing them to MQTT for Home Assistant integration running locally on a Raspberry Pi 4. Hosting the proxy on AWS provides higher availability and resilience — the charger maintains connectivity to Mobi.e even when the local Home Assistant instance is unavailable. The primary constraint is that Mobi.e communication must never be disrupted — the proxy must be invisible to both endpoints and prioritize upstream connectivity above all else.

## Glossary

- **Proxy**: The OCPP Proxy application that relays WebSocket messages between the Charger and the Central_System
- **Charger**: The Autel EV charger acting as an OCPP 1.6J Charge Point client
- **Central_System**: The Mobi.e OCPP Central System server that manages charging sessions and billing
- **MQTT_Broker**: The Mosquitto MQTT broker running as a Home Assistant add-on on the local Raspberry Pi 4
- **Home_Assistant**: The home automation platform running on HAOS on a local Raspberry Pi 4
- **AWS_Host**: The AWS infrastructure (ECS Fargate or EC2) hosting the Proxy container for high availability
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
6. THE Proxy SHALL connect to the MQTT_Broker using TLS with a minimum version of TLS 1.2 and SHALL verify the broker's server certificate against trusted certificate authorities
7. THE Proxy SHALL configure the MQTT connection with a keepalive interval of 60 seconds to enable timely detection of connection loss over the internet

### Requirement 7: Configuration Management

**User Story:** As a system administrator, I want to configure the proxy through environment variables or a configuration file, so that I can deploy and adjust settings without modifying code.

#### Acceptance Criteria

1. THE Proxy SHALL read configuration from environment variables with fallback to a YAML configuration file, where environment variables take precedence over YAML values on a per-parameter basis
2. THE Proxy SHALL require the following configuration parameters: Central_System WebSocket URL, Proxy listen port, MQTT_Broker host, MQTT_Broker port, MQTT username, MQTT password, TLS CA certificate path for MQTT, and TLS client certificate and key paths for MQTT
3. IF a required configuration parameter is missing, THEN THE Proxy SHALL fail to start and log all missing parameters in a single error output
4. THE Proxy SHALL support optional configuration parameters with the following defaults: log level (default: INFO), message buffer size (default: 100 messages), MQTT message buffer size (default: 500 messages), and reconnection maximum backoff (default: 60 seconds)
5. THE Proxy SHALL validate all configuration parameters at startup before accepting connections, verifying that: port numbers are integers between 1 and 65535, URLs conform to the WebSocket URI format (ws:// or wss://), file paths for TLS certificates point to existing readable files, and log level is one of DEBUG, INFO, WARNING, or ERROR
6. THE Proxy SHALL support loading secrets from AWS Secrets Manager or environment variables for MQTT credentials and TLS certificates
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

#### Acceptance Criteria

1. THE Proxy SHALL expose a health check HTTP endpoint on a configurable port (default: 8080) at the path `/health` that returns the current health status and connection details
2. WHEN queried, THE health check endpoint SHALL return a JSON response within 2 seconds containing: Upstream_Connection state (connected or disconnected), Downstream_Connection state (connected or disconnected), MQTT_Broker connection state (connected or disconnected), uptime in seconds, and message counters for forwarded and dropped messages in each direction (charger-to-central and central-to-charger)
3. IF the Upstream_Connection and Downstream_Connection are both connected, THEN THE Proxy SHALL report health status as `healthy` and return HTTP status code 200
4. IF the MQTT_Broker connection is lost while both the Upstream_Connection and Downstream_Connection remain connected, THEN THE Proxy SHALL report health status as `degraded` and return HTTP status code 200
5. IF both the Upstream_Connection and the MQTT_Broker connection are lost, THEN THE Proxy SHALL report health status as `unhealthy` and return HTTP status code 503
6. IF the Downstream_Connection is lost regardless of other connection states, THEN THE Proxy SHALL report health status as `unhealthy` and return HTTP status code 503

### Requirement 11: AWS Deployment and High Availability

**User Story:** As a charger owner, I want the proxy hosted on AWS with high availability, so that the charger maintains connectivity to Mobi.e even when my home network or Raspberry Pi is unavailable.

#### Acceptance Criteria

1. THE Proxy SHALL be packaged as a Docker container image compatible with AWS ECS Fargate and EC2 runtime environments, with a compressed image size not exceeding 500 MB
2. THE Proxy SHALL expose a configurable port for Charger WebSocket connections and a separate configurable port for the health check endpoint
3. THE Proxy SHALL store no runtime state locally so that a replacement container, started with the same external configuration, resumes service without data loss or manual intervention
4. THE Proxy SHALL start and become ready to accept connections within 30 seconds of container launch
5. IF the Proxy container crashes or the health check endpoint fails to return a success response for 3 consecutive checks, THEN THE AWS_Host SHALL restart the container automatically within 60 seconds
6. THE Proxy SHALL use a stable DNS endpoint or Elastic IP that persists across container restarts and redeployments so that the Charger connection URL does not change
7. THE Proxy SHALL support TLS termination for the Charger WebSocket connection using AWS Certificate Manager or a provided certificate, enforcing a minimum of TLS 1.2
8. THE Proxy SHALL respond to the AWS load balancer or ECS health check on the health check port with an HTTP 200 status when ready to accept connections, and a non-200 status otherwise
