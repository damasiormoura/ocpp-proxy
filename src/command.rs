//! Locally originated OCPP commands.
//!
//! Everything else in this proxy is transparent: frames pass byte-for-byte and
//! the proxy never speaks OCPP itself. This module is the one deliberate
//! exception. On request from Home Assistant, over MQTT, the proxy sends a
//! small set of Calls to the charger — `SetChargingProfile` to cap the charging
//! current, plus a few companions for reading the charger's settings — and
//! consumes the charger's answers to those Calls, so the Central System never
//! sees a reply to a question it did not ask.
//!
//! Invariants:
//!
//! - A proxy-originated Call only ever goes charger-wards. The proxy never
//!   originates traffic towards the Central System, and never alters what
//!   either endpoint sends.
//! - Unique IDs carry the prefix [`UNIQUE_ID_PREFIX`], so they cannot collide
//!   with either endpoint's, and the session recognises responses by that ID.
//! - At most one proxy Call is in flight at a time, and none is sent while a
//!   Central System Call to the charger is still awaiting its answer: OCPP 1.6
//!   does not allow a Charge Point to be asked two things at once.
//! - Nothing here runs unless `charging.enabled` is set. Off, the proxy does
//!   not even subscribe to the command topics.
//!
//! Flow: the MQTT publisher (its own OS thread) parses a command off the
//! `ocpp/{id}/command` topic and hands it to the [`CommandRouter`], which finds
//! the session for that charge point. The session queues it in a
//! [`LocalCallQueue`], builds the OCPP frame when the line is clear, sends it,
//! and on the charger's answer emits a [`CommandResult`] that the publisher
//! puts on `ocpp/{id}/command/result` and folds into the retained snapshot.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::config::{ChargingConfig, ChargingProfileKind, ChargingRateUnit};
use crate::models::{OcppFrame, OcppMessageType};

/// Prefix on every unique ID the proxy generates for its own Calls.
pub const UNIQUE_ID_PREFIX: &str = "proxy-";

/// Topic filters the publisher subscribes to when commands are enabled. The
/// result topic is deliberately not covered, so the proxy never reads its own
/// output back.
pub const COMMAND_SUBSCRIPTIONS: [&str; 2] = ["ocpp/+/command", "ocpp/+/command/current_limit"];

/// Commands a session will hold while the line to the charger is busy or the
/// upstream is reconnecting. Beyond this, `dispatch` reports the queue full.
pub const COMMAND_CHANNEL_CAPACITY: usize = 32;

/// A Central System Call to the charger blocks proxy Calls only while it is
/// this young. OCPP chargers answer in well under a second; a Call still
/// unanswered after this is treated as lost rather than as a reason to wait
/// the full 5-minute tracker lifetime.
pub const CENTRAL_CALL_GRACE: Duration = Duration::from_secs(10);

/// Full command topic: JSON payload with a `command` field.
pub fn command_topic(charge_point_id: &str) -> String {
    format!("ocpp/{}/command", charge_point_id)
}

/// Convenience topic: a bare number of amps, or `clear`. What a Home Assistant
/// `number` entity publishes.
pub fn current_limit_topic(charge_point_id: &str) -> String {
    format!("ocpp/{}/command/current_limit", charge_point_id)
}

/// Where the outcome of every command is published, non-retained.
pub fn command_result_topic(charge_point_id: &str) -> String {
    format!("ocpp/{}/command/result", charge_point_id)
}

/// Which of the two command topics a message arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandTopic {
    Command,
    CurrentLimit,
}

/// Recognise a command topic and extract the charge point it addresses.
pub fn parse_command_topic(topic: &str) -> Option<(String, CommandTopic)> {
    let mut parts = topic.split('/');
    if parts.next()? != "ocpp" {
        return None;
    }
    let charge_point_id = parts.next()?;
    if charge_point_id.is_empty() {
        return None;
    }
    if parts.next()? != "command" {
        return None;
    }
    match (parts.next(), parts.next()) {
        (None, _) => Some((charge_point_id.to_string(), CommandTopic::Command)),
        (Some("current_limit"), None) => {
            Some((charge_point_id.to_string(), CommandTopic::CurrentLimit))
        }
        _ => None,
    }
}

fn default_schedule_duration() -> u32 {
    3600
}

/// The commands the proxy accepts. Serialised with a `command` tag in
/// `snake_case`, so `{"command": "set_current_limit", "limit_a": 16}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum ProxyCommand {
    /// Cap the charging current. `0` pauses charging; values under the
    /// configured minimum are sent as `0` too, because an EV will not charge
    /// below 6 A.
    SetCurrentLimit { limit_a: f64 },
    /// Remove the proxy's profile, returning the charger to its own maximum
    /// (and to whatever the Central System may have installed).
    ClearCurrentLimit,
    /// Read configuration keys; all of them when `keys` is empty. The place to
    /// learn `SupportedFeatureProfiles`, `ChargingScheduleAllowedChargingRateUnit`
    /// and `ChargeProfileMaxStackLevel` before trusting a profile purpose.
    GetConfiguration {
        #[serde(default)]
        keys: Vec<String>,
    },
    /// Change one configuration key. Restricted to
    /// `charging.allowed_configuration_keys`.
    ChangeConfiguration { key: String, value: String },
    /// Ask the charger to send a message now — `MeterValues` or
    /// `StatusNotification`, typically. The triggered message goes to the
    /// Central System like any other, and onto MQTT with it.
    TriggerMessage {
        requested_message: String,
        #[serde(default)]
        connector_id: Option<u32>,
    },
    /// Read back the limit the charger is actually applying, after stacking
    /// every profile it holds.
    GetCompositeSchedule {
        #[serde(default)]
        connector_id: Option<u32>,
        #[serde(default = "default_schedule_duration")]
        duration_s: u32,
    },
}

impl ProxyCommand {
    /// The OCPP action this command becomes.
    pub fn action(&self) -> &'static str {
        match self {
            ProxyCommand::SetCurrentLimit { .. } => "SetChargingProfile",
            ProxyCommand::ClearCurrentLimit => "ClearChargingProfile",
            ProxyCommand::GetConfiguration { .. } => "GetConfiguration",
            ProxyCommand::ChangeConfiguration { .. } => "ChangeConfiguration",
            ProxyCommand::TriggerMessage { .. } => "TriggerMessage",
            ProxyCommand::GetCompositeSchedule { .. } => "GetCompositeSchedule",
        }
    }

    /// Set and clear both describe the desired limit; a newer one makes an
    /// older, unsent one pointless.
    pub fn is_limit_command(&self) -> bool {
        matches!(
            self,
            ProxyCommand::SetCurrentLimit { .. } | ProxyCommand::ClearCurrentLimit
        )
    }
}

/// Who asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandSource {
    /// Arrived on an MQTT command topic.
    Mqtt,
    /// The proxy re-sending the last accepted limit after the charger
    /// reconnected.
    Reapply,
}

/// A parsed, not yet executed command.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandRequest {
    /// Correlation id echoed in the result: the caller's `id` if it gave one,
    /// otherwise generated.
    pub id: String,
    pub command: ProxyCommand,
    pub source: CommandSource,
    pub issued_at: DateTime<Utc>,
}

impl CommandRequest {
    pub fn new(id: impl Into<String>, command: ProxyCommand, source: CommandSource) -> Self {
        Self {
            id: id.into(),
            command,
            source,
            issued_at: Utc::now(),
        }
    }

    fn generated_id(prefix: &str) -> String {
        format!("{}-{}", prefix, Utc::now().timestamp_millis())
    }
}

/// Parse the payload of a command topic into a request.
///
/// On the `current_limit` topic the payload is a bare number of amps (what a
/// Home Assistant `number` publishes, so `"16"` and `"16.0"` both work) or one
/// of `clear` / `off` / `none`. A JSON object is accepted there too. On the
/// `command` topic the payload is a JSON object with a `command` field, an
/// optional `id`, and the command's own fields.
pub fn parse_command_payload(kind: CommandTopic, payload: &[u8]) -> Result<CommandRequest, String> {
    let text = std::str::from_utf8(payload)
        .map_err(|_| "payload is not UTF-8".to_string())?
        .trim();
    if text.is_empty() {
        return Err("payload is empty".to_string());
    }

    if kind == CommandTopic::CurrentLimit && !text.starts_with('{') {
        let lowered = text.to_ascii_lowercase();
        let command = if matches!(lowered.as_str(), "clear" | "off" | "none" | "null") {
            ProxyCommand::ClearCurrentLimit
        } else {
            let limit_a: f64 = text.parse().map_err(|_| {
                format!(
                    "expected a number of amps or 'clear' on the current_limit topic, got {:?}",
                    text
                )
            })?;
            ProxyCommand::SetCurrentLimit { limit_a }
        };
        return Ok(CommandRequest::new(
            CommandRequest::generated_id("limit"),
            command,
            CommandSource::Mqtt,
        ));
    }

    let mut value: Value =
        serde_json::from_str(text).map_err(|e| format!("payload is not valid JSON: {}", e))?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| "payload must be a JSON object".to_string())?;

    let id = match object.remove("id") {
        Some(Value::String(s)) if !s.trim().is_empty() => s,
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Null) | None => CommandRequest::generated_id("cmd"),
        Some(other) => return Err(format!("id must be a string or number, got {}", other)),
    };

    // Home Assistant templates tend to produce strings; be lenient on the one
    // numeric field a person is likely to type.
    if let Some(Value::String(s)) = object.get("limit_a") {
        if let Ok(n) = s.trim().parse::<f64>() {
            object.insert("limit_a".to_string(), json!(n));
        }
    }

    let command: ProxyCommand = serde_json::from_value(value).map_err(|e| {
        format!(
            "unrecognised command ({}); expected one of set_current_limit, \
             clear_current_limit, get_configuration, change_configuration, \
             trigger_message, get_composite_schedule",
            e
        )
    })?;

    Ok(CommandRequest::new(id, command, CommandSource::Mqtt))
}

/// What a requested limit becomes once the configured floor and ceiling are
/// applied.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedLimit {
    pub requested_a: f64,
    pub applied_a: f64,
    pub detail: Option<String>,
}

/// Apply the policy: negative or non-numeric is an error; 0 pauses; below the
/// minimum pauses too (an EV will not take it); above the ceiling clamps.
pub fn resolve_limit(config: &ChargingConfig, requested_a: f64) -> Result<ResolvedLimit, String> {
    if !requested_a.is_finite() || requested_a < 0.0 {
        return Err(format!(
            "limit_a must be a non-negative number, got {}",
            requested_a
        ));
    }
    let round = |a: f64| (a * 10.0).round() / 10.0;

    if requested_a == 0.0 {
        return Ok(ResolvedLimit {
            requested_a,
            applied_a: 0.0,
            detail: Some(
                "0 A pauses charging (the charger reports SuspendedEVSE); the transaction stays open"
                    .to_string(),
            ),
        });
    }
    if requested_a < config.min_limit_a {
        return Ok(ResolvedLimit {
            requested_a,
            applied_a: 0.0,
            detail: Some(format!(
                "{} A is below the {} A minimum an EV will charge at; sending 0 A, which pauses charging",
                requested_a, config.min_limit_a
            )),
        });
    }
    if requested_a > config.max_limit_a {
        return Ok(ResolvedLimit {
            requested_a,
            applied_a: round(config.max_limit_a),
            detail: Some(format!(
                "clamped to the configured ceiling of {} A",
                config.max_limit_a
            )),
        });
    }
    Ok(ResolvedLimit {
        requested_a,
        applied_a: round(requested_a),
        detail: None,
    })
}

/// The messages `TriggerMessage` may ask for, per OCPP 1.6.
const TRIGGERABLE_MESSAGES: [&str; 6] = [
    "BootNotification",
    "DiagnosticsStatusNotification",
    "FirmwareStatusNotification",
    "Heartbeat",
    "MeterValues",
    "StatusNotification",
];

/// An OCPP Call the session is about to send.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedCall {
    pub unique_id: String,
    pub action: &'static str,
    /// The frame exactly as it goes on the wire.
    pub raw: String,
    /// Amps the profile asks for, on `SetChargingProfile` only.
    pub applied_limit_a: Option<f64>,
    /// A note about clamping or pausing, carried into the result.
    pub detail: Option<String>,
}

/// Build the OCPP frame for a request. `now` is the `startSchedule` of an
/// absolute profile; a parameter so tests can pin it.
pub fn prepare_call(
    config: &ChargingConfig,
    request: &CommandRequest,
    unique_id: String,
    now: DateTime<Utc>,
) -> Result<PreparedCall, String> {
    let (payload, applied_limit_a, detail) = match &request.command {
        ProxyCommand::SetCurrentLimit { limit_a } => {
            let resolved = resolve_limit(config, *limit_a)?;
            (
                set_charging_profile_payload(config, resolved.applied_a, now),
                Some(resolved.applied_a),
                resolved.detail,
            )
        }
        ProxyCommand::ClearCurrentLimit => (clear_charging_profile_payload(config), None, None),
        ProxyCommand::GetConfiguration { keys } => {
            let payload = if keys.is_empty() {
                json!({})
            } else {
                json!({ "key": keys })
            };
            (payload, None, None)
        }
        ProxyCommand::ChangeConfiguration { key, value } => {
            if !config.allowed_configuration_keys.iter().any(|k| k == key) {
                return Err(format!(
                    "configuration key {:?} is not in charging.allowed_configuration_keys",
                    key
                ));
            }
            (json!({ "key": key, "value": value }), None, None)
        }
        ProxyCommand::TriggerMessage {
            requested_message,
            connector_id,
        } => {
            if !TRIGGERABLE_MESSAGES.contains(&requested_message.as_str()) {
                return Err(format!(
                    "requested_message must be one of {}, got {:?}",
                    TRIGGERABLE_MESSAGES.join(", "),
                    requested_message
                ));
            }
            let mut payload = json!({ "requestedMessage": requested_message });
            if let Some(c) = connector_id {
                payload["connectorId"] = json!(c);
            }
            (payload, None, None)
        }
        ProxyCommand::GetCompositeSchedule {
            connector_id,
            duration_s,
        } => (
            json!({
                "connectorId": connector_id.unwrap_or(config.connector_id),
                "duration": duration_s,
                "chargingRateUnit": config.rate_unit.as_str(),
            }),
            None,
            None,
        ),
    };

    let action = request.command.action();
    let raw = serde_json::to_string(&json!([2, unique_id, action, payload]))
        .map_err(|e| format!("could not serialise the OCPP frame: {}", e))?;

    Ok(PreparedCall {
        unique_id,
        action,
        raw,
        applied_limit_a,
        detail,
    })
}

/// The `SetChargingProfile.req` payload for a single flat limit.
pub fn set_charging_profile_payload(
    config: &ChargingConfig,
    limit_a: f64,
    now: DateTime<Utc>,
) -> Value {
    let phases = config.number_phases.unwrap_or(1) as f64;
    let limit = match config.rate_unit {
        ChargingRateUnit::A => limit_a,
        ChargingRateUnit::W => {
            ((limit_a * config.nominal_voltage_v * phases) * 10.0).round() / 10.0
        }
    };

    let mut period = json!({ "startPeriod": 0, "limit": limit });
    if let Some(p) = config.number_phases {
        period["numberPhases"] = json!(p);
    }

    let mut schedule = json!({
        "chargingRateUnit": config.rate_unit.as_str(),
        "chargingSchedulePeriod": [period],
    });
    if config.profile_kind == ChargingProfileKind::Absolute {
        schedule["startSchedule"] = json!(now.to_rfc3339_opts(SecondsFormat::Secs, true));
    }

    json!({
        "connectorId": config.connector_id,
        "csChargingProfiles": {
            "chargingProfileId": config.profile_id,
            "stackLevel": config.stack_level,
            "chargingProfilePurpose": config.purpose.as_str(),
            "chargingProfileKind": config.profile_kind.as_str(),
            "chargingSchedule": schedule,
        }
    })
}

/// The `ClearChargingProfile.req` payload naming the proxy's own profile.
pub fn clear_charging_profile_payload(config: &ChargingConfig) -> Value {
    json!({
        "id": config.profile_id,
        "connectorId": config.connector_id,
        "chargingProfilePurpose": config.purpose.as_str(),
        "stackLevel": config.stack_level,
    })
}

/// How a command ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    /// The charger accepted it.
    Accepted,
    /// The charger answered, but said no (`Rejected`, `NotSupported`, …), or
    /// the proxy refused it (queue full, commands disabled).
    Rejected,
    /// The charger answered with a `CallError`.
    Error,
    /// No answer within `charging.command_timeout_seconds`, or the charger
    /// went away first.
    Timeout,
    /// No charger with that id is connected to the proxy.
    NotConnected,
    /// The payload could not be parsed, or the command failed validation.
    Invalid,
    /// A newer limit command arrived before this one was sent.
    Superseded,
}

impl CommandStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            CommandStatus::Accepted => "accepted",
            CommandStatus::Rejected => "rejected",
            CommandStatus::Error => "error",
            CommandStatus::Timeout => "timeout",
            CommandStatus::NotConnected => "not_connected",
            CommandStatus::Invalid => "invalid",
            CommandStatus::Superseded => "superseded",
        }
    }
}

/// The outcome published on `ocpp/{id}/command/result`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CommandResult {
    pub id: String,
    /// The command as received, or `null` when it could not be parsed.
    pub command: Value,
    pub source: CommandSource,
    pub status: CommandStatus,
    /// The OCPP action sent, once a frame was built.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_limit_a: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_limit_a: Option<f64>,
    /// The Call as sent to the charger, for the record.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ocpp_request: Option<Value>,
    /// The `CallResult` payload, or the `CallError` fields.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub charger_response: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub timestamp: String,
}

impl CommandResult {
    /// A result for a request that never produced a frame.
    pub fn for_request(
        request: &CommandRequest,
        status: CommandStatus,
        detail: Option<String>,
    ) -> Self {
        let requested_limit_a = match &request.command {
            ProxyCommand::SetCurrentLimit { limit_a } => Some(*limit_a),
            _ => None,
        };
        Self {
            id: request.id.clone(),
            command: serde_json::to_value(&request.command).unwrap_or(Value::Null),
            source: request.source,
            status,
            action: Some(request.command.action().to_string()),
            requested_limit_a,
            applied_limit_a: None,
            ocpp_request: None,
            charger_response: None,
            detail,
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        }
    }

    /// A result for a payload that could not even be parsed.
    pub fn invalid_payload(detail: String) -> Self {
        Self {
            id: CommandRequest::generated_id("invalid"),
            command: Value::Null,
            source: CommandSource::Mqtt,
            status: CommandStatus::Invalid,
            action: None,
            requested_limit_a: None,
            applied_limit_a: None,
            ocpp_request: None,
            charger_response: None,
            detail: Some(detail),
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        }
    }

    /// A result for a Call that was sent, whether or not it was answered.
    pub fn for_in_flight(
        in_flight: &InFlight,
        status: CommandStatus,
        charger_response: Option<Value>,
        detail: Option<String>,
    ) -> Self {
        let mut result = Self::for_request(&in_flight.request, status, None);
        result.action = Some(in_flight.prepared.action.to_string());
        result.applied_limit_a = in_flight.prepared.applied_limit_a;
        result.ocpp_request = serde_json::from_str(&in_flight.prepared.raw).ok();
        result.charger_response = charger_response;
        result.detail = match (in_flight.prepared.detail.clone(), detail) {
            (Some(a), Some(b)) => Some(format!("{}; {}", a, b)),
            (a, b) => a.or(b),
        };
        result
    }

    /// True for the two commands that define the standing limit.
    pub fn is_limit_result(&self) -> bool {
        matches!(
            self.action.as_deref(),
            Some("SetChargingProfile") | Some("ClearChargingProfile")
        )
    }
}

/// Read the charger's answer to one of our Calls.
///
/// Returns the status, the payload to report, and a human note. `Accepted` is
/// the literal OCPP `Accepted` plus the two answers that leave the desired
/// state in place: `Unknown` to a clear (nothing to remove) and
/// `RebootRequired` to a configuration change (stored, applies later).
pub fn interpret_response(
    action: &str,
    frame: &OcppFrame,
) -> (CommandStatus, Option<Value>, Option<String>) {
    let parsed: Value = serde_json::from_str(&frame.raw).unwrap_or(Value::Null);
    match frame.message_type {
        OcppMessageType::CallError => {
            let code = parsed
                .get(2)
                .and_then(Value::as_str)
                .unwrap_or("UnknownError");
            let description = parsed.get(3).and_then(Value::as_str).unwrap_or("");
            let response = json!({
                "errorCode": code,
                "errorDescription": description,
                "errorDetails": parsed.get(4).cloned().unwrap_or(Value::Null),
            });
            let detail = if description.is_empty() {
                format!("charger returned CallError {}", code)
            } else {
                format!("charger returned CallError {}: {}", code, description)
            };
            (CommandStatus::Error, Some(response), Some(detail))
        }
        OcppMessageType::CallResult => {
            let payload = parsed.get(2).cloned().unwrap_or(Value::Null);
            let status = payload.get("status").and_then(Value::as_str);
            let (status, detail) = match (action, status) {
                (_, None) => (CommandStatus::Accepted, None),
                (_, Some("Accepted")) => (CommandStatus::Accepted, None),
                ("ClearChargingProfile", Some("Unknown")) => (
                    CommandStatus::Accepted,
                    Some("the charger held no matching profile; nothing to clear".to_string()),
                ),
                ("ChangeConfiguration", Some("RebootRequired")) => (
                    CommandStatus::Accepted,
                    Some("stored; the charger needs a reboot before it takes effect".to_string()),
                ),
                (_, Some(other)) => (
                    CommandStatus::Rejected,
                    Some(format!("charger answered {}", other)),
                ),
            };
            (status, Some(payload), detail)
        }
        OcppMessageType::Call { .. } => (
            CommandStatus::Error,
            None,
            Some("expected a response, got a Call".to_string()),
        ),
    }
}

/// A proxy Call that has been sent and awaits the charger's answer.
#[derive(Debug, Clone, PartialEq)]
pub struct InFlight {
    pub request: CommandRequest,
    pub prepared: PreparedCall,
    pub sent_at: Instant,
}

/// The per-session queue of proxy Calls. One in flight at a time; queued
/// limit commands coalesce, last one wins.
#[derive(Debug)]
pub struct LocalCallQueue {
    queued: VecDeque<CommandRequest>,
    in_flight: Option<InFlight>,
    timeout: Duration,
    capacity: usize,
    sequence: u64,
}

impl LocalCallQueue {
    pub fn new(timeout: Duration, capacity: usize) -> Self {
        Self {
            queued: VecDeque::new(),
            in_flight: None,
            timeout,
            capacity: capacity.max(1),
            sequence: 0,
        }
    }

    /// Queue a request. Returns the requests it displaces: older unsent limit
    /// commands (superseded), and the oldest entry if the queue is full.
    pub fn push(&mut self, request: CommandRequest) -> Vec<CommandRequest> {
        let mut displaced = Vec::new();
        if request.command.is_limit_command() {
            let mut kept = VecDeque::with_capacity(self.queued.len());
            for queued in self.queued.drain(..) {
                if queued.command.is_limit_command() {
                    displaced.push(queued);
                } else {
                    kept.push_back(queued);
                }
            }
            self.queued = kept;
        }
        if self.queued.len() >= self.capacity {
            if let Some(oldest) = self.queued.pop_front() {
                displaced.push(oldest);
            }
        }
        self.queued.push_back(request);
        displaced
    }

    pub fn has_in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.queued.is_empty() && self.in_flight.is_none()
    }

    pub fn queued_len(&self) -> usize {
        self.queued.len()
    }

    /// Whether a response with this unique id belongs to us.
    pub fn is_ours(&self, unique_id: &str) -> bool {
        self.in_flight
            .as_ref()
            .is_some_and(|f| f.prepared.unique_id == unique_id)
    }

    /// The next unique id to send under: prefixed, time-stamped, sequenced.
    pub fn next_unique_id(&mut self) -> String {
        self.sequence += 1;
        format!(
            "{}{}-{}",
            UNIQUE_ID_PREFIX,
            Utc::now().timestamp_millis(),
            self.sequence
        )
    }

    /// Pop the next request to send, or `None` while one is in flight or the
    /// queue is empty.
    pub fn pop_next(&mut self) -> Option<CommandRequest> {
        if self.in_flight.is_some() {
            return None;
        }
        self.queued.pop_front()
    }

    pub fn set_in_flight(&mut self, request: CommandRequest, prepared: PreparedCall) {
        self.in_flight = Some(InFlight {
            request,
            prepared,
            sent_at: Instant::now(),
        });
    }

    /// Claim the in-flight call if this response is its answer.
    pub fn take_response(&mut self, unique_id: &str) -> Option<InFlight> {
        if self.is_ours(unique_id) {
            self.in_flight.take()
        } else {
            None
        }
    }

    /// Everything that has waited too long: queued requests older than the
    /// timeout (measured from when they were issued) and the in-flight call
    /// if the charger has not answered within it.
    pub fn expire(&mut self, now: DateTime<Utc>) -> (Vec<CommandRequest>, Option<InFlight>) {
        let max_age = chrono::Duration::from_std(self.timeout)
            .unwrap_or_else(|_| chrono::Duration::seconds(30));
        let mut expired = Vec::new();
        let mut kept = VecDeque::with_capacity(self.queued.len());
        for request in self.queued.drain(..) {
            if now - request.issued_at >= max_age {
                expired.push(request);
            } else {
                kept.push_back(request);
            }
        }
        self.queued = kept;

        let in_flight = match &self.in_flight {
            Some(f) if f.sent_at.elapsed() >= self.timeout => self.in_flight.take(),
            _ => None,
        };
        (expired, in_flight)
    }

    /// Empty the queue, for a session that is ending.
    pub fn drain(&mut self) -> (Vec<CommandRequest>, Option<InFlight>) {
        (self.queued.drain(..).collect(), self.in_flight.take())
    }
}

/// Why a command could not be handed to a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchError {
    /// No session is registered for that charge point.
    NotConnected,
    /// The session's inbox is full.
    QueueFull,
}

struct Registration {
    generation: u64,
    tx: mpsc::Sender<CommandRequest>,
}

/// Finds the live session for a charge point. Shared between the MQTT thread,
/// which dispatches, and the downstream handler, which registers each session
/// under its connection generation so a displaced session cannot unregister
/// its successor.
#[derive(Clone, Default)]
pub struct CommandRouter {
    inner: Arc<Mutex<HashMap<String, Registration>>>,
}

impl CommandRouter {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Registration>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn register(
        &self,
        charge_point_id: &str,
        generation: u64,
        tx: mpsc::Sender<CommandRequest>,
    ) {
        self.lock()
            .insert(charge_point_id.to_string(), Registration { generation, tx });
    }

    /// Remove the registration if it still belongs to this generation.
    pub fn deregister(&self, charge_point_id: &str, generation: u64) -> bool {
        let mut map = self.lock();
        match map.get(charge_point_id) {
            Some(reg) if reg.generation == generation => {
                map.remove(charge_point_id);
                true
            }
            _ => false,
        }
    }

    pub fn is_connected(&self, charge_point_id: &str) -> bool {
        self.lock().contains_key(charge_point_id)
    }

    /// Hand a request to the session. Never blocks: a full inbox is an error
    /// the caller reports, not something to wait on.
    pub fn dispatch(
        &self,
        charge_point_id: &str,
        request: CommandRequest,
    ) -> Result<(), DispatchError> {
        let map = self.lock();
        let Some(reg) = map.get(charge_point_id) else {
            return Err(DispatchError::NotConnected);
        };
        reg.tx.try_send(request).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => DispatchError::QueueFull,
            mpsc::error::TrySendError::Closed(_) => DispatchError::NotConnected,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChargingProfilePurpose;

    fn config() -> ChargingConfig {
        ChargingConfig {
            enabled: true,
            ..ChargingConfig::default()
        }
    }

    fn request(command: ProxyCommand) -> CommandRequest {
        CommandRequest::new("req-1", command, CommandSource::Mqtt)
    }

    fn fixed_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-09-22T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    // --- topics ---

    #[test]
    fn test_topics_round_trip_through_the_parser() {
        assert_eq!(
            parse_command_topic(&command_topic("CP-1")),
            Some(("CP-1".to_string(), CommandTopic::Command))
        );
        assert_eq!(
            parse_command_topic(&current_limit_topic("CP-1")),
            Some(("CP-1".to_string(), CommandTopic::CurrentLimit))
        );
    }

    #[test]
    fn test_result_and_other_topics_are_not_commands() {
        assert_eq!(parse_command_topic(&command_result_topic("CP-1")), None);
        assert_eq!(parse_command_topic("ocpp/CP-1/charger/MeterValues"), None);
        assert_eq!(parse_command_topic("ocpp/CP-1/state"), None);
        assert_eq!(parse_command_topic("ocpp//command"), None);
        assert_eq!(parse_command_topic("other/CP-1/command"), None);
        assert_eq!(
            parse_command_topic("ocpp/CP-1/command/current_limit/x"),
            None
        );
    }

    #[test]
    fn test_subscriptions_match_the_two_command_topics_only() {
        // The result topic must not match either filter, or the proxy would
        // read its own output back as a command.
        fn matches(filter: &str, topic: &str) -> bool {
            let f: Vec<&str> = filter.split('/').collect();
            let t: Vec<&str> = topic.split('/').collect();
            f.len() == t.len() && f.iter().zip(&t).all(|(a, b)| *a == "+" || a == b)
        }
        for filter in COMMAND_SUBSCRIPTIONS {
            assert!(!matches(filter, &command_result_topic("CP-1")), "{filter}");
        }
        assert!(matches(COMMAND_SUBSCRIPTIONS[0], &command_topic("CP-1")));
        assert!(matches(
            COMMAND_SUBSCRIPTIONS[1],
            &current_limit_topic("CP-1")
        ));
    }

    // --- payload parsing ---

    #[test]
    fn test_bare_number_on_the_limit_topic() {
        let req = parse_command_payload(CommandTopic::CurrentLimit, b" 16.0 ").unwrap();
        assert_eq!(req.command, ProxyCommand::SetCurrentLimit { limit_a: 16.0 });
        assert!(req.id.starts_with("limit-"));
        assert_eq!(req.source, CommandSource::Mqtt);
    }

    #[test]
    fn test_clear_words_on_the_limit_topic() {
        for word in ["clear", "OFF", "none", "null"] {
            let req = parse_command_payload(CommandTopic::CurrentLimit, word.as_bytes()).unwrap();
            assert_eq!(req.command, ProxyCommand::ClearCurrentLimit, "{word}");
        }
    }

    #[test]
    fn test_garbage_on_the_limit_topic_is_invalid() {
        assert!(parse_command_payload(CommandTopic::CurrentLimit, b"sixteen").is_err());
        assert!(parse_command_payload(CommandTopic::CurrentLimit, b"").is_err());
        assert!(parse_command_payload(CommandTopic::CurrentLimit, &[0xff, 0xfe]).is_err());
    }

    #[test]
    fn test_json_object_on_the_limit_topic_is_accepted_too() {
        let req = parse_command_payload(
            CommandTopic::CurrentLimit,
            br#"{"command":"set_current_limit","limit_a":10,"id":"x"}"#,
        )
        .unwrap();
        assert_eq!(req.command, ProxyCommand::SetCurrentLimit { limit_a: 10.0 });
        assert_eq!(req.id, "x");
    }

    #[test]
    fn test_json_commands_parse_with_ids_and_defaults() {
        let req = parse_command_payload(
            CommandTopic::Command,
            br#"{"id": 42, "command": "get_configuration"}"#,
        )
        .unwrap();
        assert_eq!(req.id, "42");
        assert_eq!(req.command, ProxyCommand::GetConfiguration { keys: vec![] });

        let req = parse_command_payload(
            CommandTopic::Command,
            br#"{"command": "get_composite_schedule"}"#,
        )
        .unwrap();
        assert_eq!(
            req.command,
            ProxyCommand::GetCompositeSchedule {
                connector_id: None,
                duration_s: 3600
            }
        );
        assert!(req.id.starts_with("cmd-"));

        let req = parse_command_payload(
            CommandTopic::Command,
            br#"{"command": "trigger_message", "requested_message": "MeterValues", "connector_id": 1}"#,
        )
        .unwrap();
        assert_eq!(
            req.command,
            ProxyCommand::TriggerMessage {
                requested_message: "MeterValues".to_string(),
                connector_id: Some(1)
            }
        );
    }

    #[test]
    fn test_limit_given_as_string_is_coerced() {
        let req = parse_command_payload(
            CommandTopic::Command,
            br#"{"command":"set_current_limit","limit_a":"12.5"}"#,
        )
        .unwrap();
        assert_eq!(req.command, ProxyCommand::SetCurrentLimit { limit_a: 12.5 });
    }

    #[test]
    fn test_unknown_or_malformed_json_commands_are_invalid() {
        let err =
            parse_command_payload(CommandTopic::Command, br#"{"command":"reboot"}"#).unwrap_err();
        assert!(err.contains("unrecognised command"), "{err}");
        assert!(parse_command_payload(CommandTopic::Command, b"[1,2,3]").is_err());
        assert!(parse_command_payload(CommandTopic::Command, b"not json").is_err());
        assert!(parse_command_payload(
            CommandTopic::Command,
            br#"{"command":"set_current_limit"}"#
        )
        .is_err());
        assert!(parse_command_payload(
            CommandTopic::Command,
            br#"{"command":"set_current_limit","limit_a":16,"id":[1]}"#
        )
        .is_err());
    }

    // --- limit policy ---

    #[test]
    fn test_limit_within_range_passes_rounded_to_a_tenth() {
        let r = resolve_limit(&config(), 16.04).unwrap();
        assert_eq!(r.applied_a, 16.0);
        assert!(r.detail.is_none());
    }

    #[test]
    fn test_zero_pauses() {
        let r = resolve_limit(&config(), 0.0).unwrap();
        assert_eq!(r.applied_a, 0.0);
        assert!(r.detail.unwrap().contains("pauses"));
    }

    #[test]
    fn test_below_minimum_pauses_rather_than_asking_for_an_impossible_current() {
        let r = resolve_limit(&config(), 4.3).unwrap();
        assert_eq!(r.applied_a, 0.0);
        assert_eq!(r.requested_a, 4.3);
        assert!(r.detail.unwrap().contains("below the 6 A minimum"));
    }

    #[test]
    fn test_above_ceiling_clamps() {
        let cfg = ChargingConfig {
            max_limit_a: 20.0,
            ..config()
        };
        let r = resolve_limit(&cfg, 32.0).unwrap();
        assert_eq!(r.applied_a, 20.0);
        assert!(r.detail.unwrap().contains("clamped"));
    }

    #[test]
    fn test_negative_or_nan_is_an_error() {
        assert!(resolve_limit(&config(), -1.0).is_err());
        assert!(resolve_limit(&config(), f64::NAN).is_err());
        assert!(resolve_limit(&config(), f64::INFINITY).is_err());
    }

    // --- frame building ---

    #[test]
    fn test_set_charging_profile_frame_shape() {
        let req = request(ProxyCommand::SetCurrentLimit { limit_a: 16.0 });
        let call = prepare_call(&config(), &req, "proxy-1-1".to_string(), fixed_now()).unwrap();
        assert_eq!(call.action, "SetChargingProfile");
        assert_eq!(call.applied_limit_a, Some(16.0));

        let frame: Value = serde_json::from_str(&call.raw).unwrap();
        assert_eq!(frame[0], 2);
        assert_eq!(frame[1], "proxy-1-1");
        assert_eq!(frame[2], "SetChargingProfile");
        assert_eq!(
            frame[3],
            json!({
                "connectorId": 0,
                "csChargingProfiles": {
                    "chargingProfileId": 1001,
                    "stackLevel": 0,
                    "chargingProfilePurpose": "TxDefaultProfile",
                    "chargingProfileKind": "Absolute",
                    "chargingSchedule": {
                        "chargingRateUnit": "A",
                        "startSchedule": "2026-09-22T10:00:00Z",
                        "chargingSchedulePeriod": [
                            { "startPeriod": 0, "limit": 16.0, "numberPhases": 1 }
                        ]
                    }
                }
            })
        );
        assert!(
            OcppFrame::parse(&call.raw).is_ok(),
            "must be a parseable OCPP Call"
        );
    }

    #[test]
    fn test_relative_profile_has_no_start_schedule_and_null_phases_omits_the_field() {
        let cfg = ChargingConfig {
            profile_kind: ChargingProfileKind::Relative,
            number_phases: None,
            purpose: ChargingProfilePurpose::ChargePointMaxProfile,
            ..config()
        };
        let payload = set_charging_profile_payload(&cfg, 10.0, fixed_now());
        let profile = &payload["csChargingProfiles"];
        assert_eq!(profile["chargingProfileKind"], "Relative");
        assert_eq!(profile["chargingProfilePurpose"], "ChargePointMaxProfile");
        assert!(profile["chargingSchedule"].get("startSchedule").is_none());
        let period = &profile["chargingSchedule"]["chargingSchedulePeriod"][0];
        assert!(period.get("numberPhases").is_none());
    }

    #[test]
    fn test_watt_unit_converts_amps_with_nominal_voltage_and_phases() {
        let cfg = ChargingConfig {
            rate_unit: ChargingRateUnit::W,
            nominal_voltage_v: 230.0,
            number_phases: Some(1),
            ..config()
        };
        let payload = set_charging_profile_payload(&cfg, 16.0, fixed_now());
        let schedule = &payload["csChargingProfiles"]["chargingSchedule"];
        assert_eq!(schedule["chargingRateUnit"], "W");
        assert_eq!(schedule["chargingSchedulePeriod"][0]["limit"], 3680.0);
    }

    #[test]
    fn test_clear_names_the_proxy_profile() {
        let req = request(ProxyCommand::ClearCurrentLimit);
        let call = prepare_call(&config(), &req, "proxy-1-2".to_string(), fixed_now()).unwrap();
        assert_eq!(call.action, "ClearChargingProfile");
        assert_eq!(call.applied_limit_a, None);
        let frame: Value = serde_json::from_str(&call.raw).unwrap();
        assert_eq!(
            frame[3],
            json!({
                "id": 1001,
                "connectorId": 0,
                "chargingProfilePurpose": "TxDefaultProfile",
                "stackLevel": 0
            })
        );
    }

    #[test]
    fn test_get_configuration_with_and_without_keys() {
        let req = request(ProxyCommand::GetConfiguration { keys: vec![] });
        let call = prepare_call(&config(), &req, "u".to_string(), fixed_now()).unwrap();
        let frame: Value = serde_json::from_str(&call.raw).unwrap();
        assert_eq!(frame[3], json!({}));

        let req = request(ProxyCommand::GetConfiguration {
            keys: vec!["SupportedFeatureProfiles".to_string()],
        });
        let call = prepare_call(&config(), &req, "u".to_string(), fixed_now()).unwrap();
        let frame: Value = serde_json::from_str(&call.raw).unwrap();
        assert_eq!(frame[3], json!({ "key": ["SupportedFeatureProfiles"] }));
    }

    #[test]
    fn test_change_configuration_respects_the_allowlist() {
        let allowed = request(ProxyCommand::ChangeConfiguration {
            key: "MeterValueSampleInterval".to_string(),
            value: "10".to_string(),
        });
        let call = prepare_call(&config(), &allowed, "u".to_string(), fixed_now()).unwrap();
        let frame: Value = serde_json::from_str(&call.raw).unwrap();
        assert_eq!(
            frame[3],
            json!({ "key": "MeterValueSampleInterval", "value": "10" })
        );

        let forbidden = request(ProxyCommand::ChangeConfiguration {
            key: "AuthorizeRemoteTxRequests".to_string(),
            value: "false".to_string(),
        });
        let err = prepare_call(&config(), &forbidden, "u".to_string(), fixed_now()).unwrap_err();
        assert!(err.contains("allowed_configuration_keys"), "{err}");
    }

    #[test]
    fn test_trigger_message_validates_the_message_name() {
        let ok = request(ProxyCommand::TriggerMessage {
            requested_message: "MeterValues".to_string(),
            connector_id: Some(1),
        });
        let call = prepare_call(&config(), &ok, "u".to_string(), fixed_now()).unwrap();
        let frame: Value = serde_json::from_str(&call.raw).unwrap();
        assert_eq!(
            frame[3],
            json!({ "requestedMessage": "MeterValues", "connectorId": 1 })
        );

        let bad = request(ProxyCommand::TriggerMessage {
            requested_message: "Reset".to_string(),
            connector_id: None,
        });
        assert!(prepare_call(&config(), &bad, "u".to_string(), fixed_now()).is_err());
    }

    #[test]
    fn test_composite_schedule_defaults_to_the_configured_connector_and_unit() {
        let req = request(ProxyCommand::GetCompositeSchedule {
            connector_id: None,
            duration_s: 600,
        });
        let call = prepare_call(&config(), &req, "u".to_string(), fixed_now()).unwrap();
        let frame: Value = serde_json::from_str(&call.raw).unwrap();
        assert_eq!(
            frame[3],
            json!({ "connectorId": 0, "duration": 600, "chargingRateUnit": "A" })
        );
    }

    #[test]
    fn test_invalid_limit_fails_at_preparation() {
        let req = request(ProxyCommand::SetCurrentLimit { limit_a: -5.0 });
        assert!(prepare_call(&config(), &req, "u".to_string(), fixed_now()).is_err());
    }

    // --- response interpretation ---

    fn frame(raw: &str) -> OcppFrame {
        OcppFrame::parse(raw).unwrap()
    }

    #[test]
    fn test_accepted_and_rejected_call_results() {
        let (s, payload, detail) = interpret_response(
            "SetChargingProfile",
            &frame(r#"[3,"p",{"status":"Accepted"}]"#),
        );
        assert_eq!(s, CommandStatus::Accepted);
        assert_eq!(payload, Some(json!({"status": "Accepted"})));
        assert!(detail.is_none());

        let (s, _, detail) = interpret_response(
            "SetChargingProfile",
            &frame(r#"[3,"p",{"status":"NotSupported"}]"#),
        );
        assert_eq!(s, CommandStatus::Rejected);
        assert_eq!(detail.unwrap(), "charger answered NotSupported");
    }

    #[test]
    fn test_answers_that_leave_the_desired_state_count_as_accepted() {
        let (s, _, detail) = interpret_response(
            "ClearChargingProfile",
            &frame(r#"[3,"p",{"status":"Unknown"}]"#),
        );
        assert_eq!(s, CommandStatus::Accepted);
        assert!(detail.unwrap().contains("nothing to clear"));

        let (s, _, detail) = interpret_response(
            "ChangeConfiguration",
            &frame(r#"[3,"p",{"status":"RebootRequired"}]"#),
        );
        assert_eq!(s, CommandStatus::Accepted);
        assert!(detail.unwrap().contains("reboot"));

        // The same word from a different action is a plain rejection.
        let (s, _, _) = interpret_response(
            "SetChargingProfile",
            &frame(r#"[3,"p",{"status":"Unknown"}]"#),
        );
        assert_eq!(s, CommandStatus::Rejected);
    }

    #[test]
    fn test_get_configuration_has_no_status_and_is_accepted() {
        let (s, payload, _) = interpret_response(
            "GetConfiguration",
            &frame(
                r#"[3,"p",{"configurationKey":[{"key":"HeartbeatInterval","readonly":false,"value":"300"}]}]"#,
            ),
        );
        assert_eq!(s, CommandStatus::Accepted);
        assert_eq!(payload.unwrap()["configurationKey"][0]["value"], "300");
    }

    #[test]
    fn test_call_error_is_reported_with_its_code() {
        let (s, payload, detail) = interpret_response(
            "SetChargingProfile",
            &frame(r#"[4,"p","NotSupported","Smart charging is off",{}]"#),
        );
        assert_eq!(s, CommandStatus::Error);
        assert_eq!(payload.unwrap()["errorCode"], "NotSupported");
        assert_eq!(
            detail.unwrap(),
            "charger returned CallError NotSupported: Smart charging is off"
        );
    }

    // --- results ---

    #[test]
    fn test_result_serialises_the_documented_shape() {
        let req = CommandRequest::new(
            "abc",
            ProxyCommand::SetCurrentLimit { limit_a: 40.0 },
            CommandSource::Mqtt,
        );
        let prepared = prepare_call(&config(), &req, "proxy-1-1".to_string(), fixed_now()).unwrap();
        let in_flight = InFlight {
            request: req,
            prepared,
            sent_at: Instant::now(),
        };
        let result = CommandResult::for_in_flight(
            &in_flight,
            CommandStatus::Accepted,
            Some(json!({"status": "Accepted"})),
            None,
        );
        let v = serde_json::to_value(&result).unwrap();
        assert_eq!(v["id"], "abc");
        assert_eq!(v["command"]["command"], "set_current_limit");
        assert_eq!(v["command"]["limit_a"], 40.0);
        assert_eq!(v["source"], "mqtt");
        assert_eq!(v["status"], "accepted");
        assert_eq!(v["action"], "SetChargingProfile");
        assert_eq!(v["requested_limit_a"], 40.0);
        assert_eq!(v["applied_limit_a"], 32.0);
        assert_eq!(v["ocpp_request"][2], "SetChargingProfile");
        assert_eq!(v["charger_response"]["status"], "Accepted");
        assert!(v["detail"].as_str().unwrap().contains("clamped"));
        assert!(v["timestamp"].is_string());
        assert!(result.is_limit_result());
    }

    #[test]
    fn test_result_for_unparsed_payload() {
        let v = serde_json::to_value(CommandResult::invalid_payload("bad".to_string())).unwrap();
        assert_eq!(v["status"], "invalid");
        assert!(v["command"].is_null());
        assert!(v.get("action").is_none());
        assert_eq!(v["detail"], "bad");
    }

    #[test]
    fn test_result_for_request_never_sent_has_no_ocpp_request() {
        let req = request(ProxyCommand::GetConfiguration { keys: vec![] });
        let r = CommandResult::for_request(&req, CommandStatus::NotConnected, None);
        assert_eq!(r.status, CommandStatus::NotConnected);
        assert_eq!(r.action.as_deref(), Some("GetConfiguration"));
        assert!(r.ocpp_request.is_none());
        assert!(!r.is_limit_result());
    }

    // --- queue ---

    #[test]
    fn test_queue_sends_one_at_a_time_and_matches_responses_by_id() {
        let mut q = LocalCallQueue::new(Duration::from_secs(30), 8);
        assert!(q.is_empty());
        assert!(q
            .push(request(ProxyCommand::GetConfiguration { keys: vec![] }))
            .is_empty());
        assert!(q.push(request(ProxyCommand::ClearCurrentLimit)).is_empty());
        assert_eq!(q.queued_len(), 2);

        let first = q.pop_next().unwrap();
        let id = q.next_unique_id();
        assert!(id.starts_with(UNIQUE_ID_PREFIX));
        let prepared = prepare_call(&config(), &first, id.clone(), fixed_now()).unwrap();
        q.set_in_flight(first, prepared);

        assert!(q.has_in_flight());
        assert!(
            q.pop_next().is_none(),
            "nothing else goes out while one is in flight"
        );
        assert!(!q.is_ours("msg-from-mobie"));
        assert!(q.take_response("msg-from-mobie").is_none());
        assert!(q.is_ours(&id));
        let done = q.take_response(&id).unwrap();
        assert_eq!(done.prepared.action, "GetConfiguration");
        assert!(!q.has_in_flight());
        assert!(q.pop_next().is_some());
    }

    #[test]
    fn test_unique_ids_are_distinct() {
        let mut q = LocalCallQueue::new(Duration::from_secs(30), 8);
        let a = q.next_unique_id();
        let b = q.next_unique_id();
        assert_ne!(a, b);
        assert!(
            a.len() <= 36 && b.len() <= 36,
            "OCPP caps uniqueId at 36 chars"
        );
    }

    #[test]
    fn test_newer_limit_supersedes_queued_limits_but_not_other_commands() {
        let mut q = LocalCallQueue::new(Duration::from_secs(30), 8);
        q.push(CommandRequest::new(
            "a",
            ProxyCommand::SetCurrentLimit { limit_a: 10.0 },
            CommandSource::Mqtt,
        ));
        q.push(CommandRequest::new(
            "b",
            ProxyCommand::GetConfiguration { keys: vec![] },
            CommandSource::Mqtt,
        ));
        // Clear is a limit command too: it supersedes the queued set.
        let displaced = q.push(CommandRequest::new(
            "c",
            ProxyCommand::ClearCurrentLimit,
            CommandSource::Mqtt,
        ));
        let ids: Vec<_> = displaced.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["a"]);

        let displaced = q.push(CommandRequest::new(
            "d",
            ProxyCommand::SetCurrentLimit { limit_a: 20.0 },
            CommandSource::Mqtt,
        ));
        let ids: Vec<_> = displaced.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["c"], "the read-only command in between is kept");
        assert_eq!(q.queued_len(), 2);
        assert_eq!(q.pop_next().unwrap().id, "b");
    }

    #[test]
    fn test_full_queue_drops_the_oldest() {
        let mut q = LocalCallQueue::new(Duration::from_secs(30), 2);
        for id in ["a", "b"] {
            assert!(q
                .push(CommandRequest::new(
                    id,
                    ProxyCommand::GetConfiguration { keys: vec![] },
                    CommandSource::Mqtt
                ))
                .is_empty());
        }
        let displaced = q.push(CommandRequest::new(
            "c",
            ProxyCommand::GetConfiguration { keys: vec![] },
            CommandSource::Mqtt,
        ));
        assert_eq!(displaced.len(), 1);
        assert_eq!(displaced[0].id, "a");
        assert_eq!(q.queued_len(), 2);
    }

    #[test]
    fn test_expiry_covers_queued_and_in_flight() {
        let mut q = LocalCallQueue::new(Duration::from_millis(0), 8);
        let mut stale = request(ProxyCommand::GetConfiguration { keys: vec![] });
        stale.issued_at = Utc::now() - chrono::Duration::seconds(60);
        q.push(stale);
        let mut fresh = CommandRequest::new(
            "fresh",
            ProxyCommand::GetConfiguration { keys: vec![] },
            CommandSource::Mqtt,
        );
        fresh.issued_at = Utc::now() + chrono::Duration::seconds(60);
        q.push(fresh);

        let (expired, in_flight) = q.expire(Utc::now());
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].id, "req-1");
        assert!(in_flight.is_none());
        assert_eq!(q.queued_len(), 1);

        let next = q.pop_next().unwrap();
        let prepared = prepare_call(&config(), &next, "u".to_string(), fixed_now()).unwrap();
        q.set_in_flight(next, prepared);
        let (_, in_flight) = q.expire(Utc::now());
        assert!(
            in_flight.is_some(),
            "a zero timeout expires the in-flight call at once"
        );
        assert!(q.is_empty());
    }

    #[test]
    fn test_drain_returns_everything() {
        let mut q = LocalCallQueue::new(Duration::from_secs(30), 8);
        q.push(request(ProxyCommand::GetConfiguration { keys: vec![] }));
        q.push(request(ProxyCommand::ClearCurrentLimit));
        let first = q.pop_next().unwrap();
        let prepared = prepare_call(&config(), &first, "u".to_string(), fixed_now()).unwrap();
        q.set_in_flight(first, prepared);
        let (queued, in_flight) = q.drain();
        assert_eq!(queued.len(), 1);
        assert!(in_flight.is_some());
        assert!(q.is_empty());
    }

    // --- router ---

    #[tokio::test]
    async fn test_router_dispatches_to_the_registered_session() {
        let router = CommandRouter::new();
        let (tx, mut rx) = mpsc::channel(2);
        assert_eq!(
            router.dispatch("CP-1", request(ProxyCommand::ClearCurrentLimit)),
            Err(DispatchError::NotConnected)
        );
        router.register("CP-1", 1, tx);
        assert!(router.is_connected("CP-1"));
        router
            .dispatch("CP-1", request(ProxyCommand::ClearCurrentLimit))
            .unwrap();
        assert_eq!(
            rx.recv().await.unwrap().command,
            ProxyCommand::ClearCurrentLimit
        );
    }

    #[tokio::test]
    async fn test_router_reports_a_full_inbox_and_a_closed_session() {
        let router = CommandRouter::new();
        let (tx, rx) = mpsc::channel(1);
        router.register("CP-1", 1, tx);
        router
            .dispatch("CP-1", request(ProxyCommand::ClearCurrentLimit))
            .unwrap();
        assert_eq!(
            router.dispatch("CP-1", request(ProxyCommand::ClearCurrentLimit)),
            Err(DispatchError::QueueFull)
        );
        drop(rx);
        assert_eq!(
            router.dispatch("CP-1", request(ProxyCommand::ClearCurrentLimit)),
            Err(DispatchError::NotConnected)
        );
    }

    #[tokio::test]
    async fn test_router_deregistration_is_generation_aware() {
        let router = CommandRouter::new();
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        router.register("CP-1", 1, tx1);
        router.register("CP-1", 2, tx2);
        assert!(
            !router.deregister("CP-1", 1),
            "a displaced session must not remove its successor"
        );
        assert!(router.is_connected("CP-1"));
        assert!(router.deregister("CP-1", 2));
        assert!(!router.is_connected("CP-1"));
    }
}
