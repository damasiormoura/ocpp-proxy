"""Render every state-topic template in the HA package against real payload
shapes and assert what each should produce.

This is the check that caught the `| first` bug: the templates are the part
with no compiler behind them, so they get exercised against captured payloads
rather than eyeballed.
"""
import json, os, sys, yaml, jinja2

# The template if it has not been rendered, the rendered copy if it has. The
# placeholders only appear in topic strings, which this check does not evaluate,
# so both work — but checking the rendered copy is what actually ships.
PKG = "deploy/.rendered/deploy/homeassistant/ocpp_proxy.yaml"
if not os.path.exists(PKG):
    PKG = "deploy/homeassistant/ocpp_proxy.yaml"

env = jinja2.Environment(undefined=jinja2.Undefined)


def ha_float(v, default=0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


env.filters["float"] = ha_float

# The three shapes a subscriber can actually receive on ocpp/{id}/state.
AFTER_SESSION = {
    "connector_status": "Available", "error_code": "NoError",
    "transaction_id": None, "id_tag": None, "meter_start_wh": None,
    "last_meter_stop_wh": 8084000, "last_session_energy_wh": 7555,
    "last_transaction_id": 1000000001, "last_stop_reason": "EVDisconnected",
    "last_stop_time": "2026-09-01T07:22:16Z",
    "current_limit_a": None, "current_limit_status": None,
    "current_limit_updated": None,
    "last_updated": "2026-09-01T07:22:16+00:00",
}
MID_SESSION = dict(AFTER_SESSION, connector_status="Charging",
                   transaction_id=1000000002, id_tag="a1b2c3d4",
                   meter_start_wh=8084000)
FRESH = {k: None for k in AFTER_SESSION}
FRESH.update(connector_status="Available", error_code="NoError")
# What the broker held before this change: the last_* keys simply absent.
LEGACY = {k: v for k, v in FRESH.items() if not k.startswith("last_") or k == "last_updated"}
# A stop the proxy saw without the matching start.
NO_DELTA = dict(AFTER_SESSION, last_session_energy_wh=None, last_stop_reason=None)
# A standing 16 A limit, a paused charger (0 A — must NOT read as "no limit"),
# and a limit the charger refused, where the previous one still stands.
LIMITED = dict(MID_SESSION, current_limit_a=16.0, current_limit_status="accepted",
               current_limit_updated="2026-09-22T18:00:00+00:00")
PAUSED = dict(LIMITED, current_limit_a=0)
REFUSED = dict(LIMITED, current_limit_status="rejected")

pkg = yaml.safe_load(open(PKG))
sensors = {s["name"]: s for s in pkg["mqtt"]["sensor"]}


def render(tpl, payload):
    return env.from_string(tpl).render(value_json=payload, this=None).strip()


def avail(sensor, payload):
    """Resolve availability_mode: all across the sensor's availability list."""
    out = "online"
    for a in sensor.get("availability", []):
        if "value_template" not in a:
            continue  # the proxy-availability topic, not this payload
        if render(a["value_template"], payload) != a["payload_available"]:
            out = "offline"
    return out


CASES = [
    # (sensor name, payload, expected availability, expected state or None)
    ("Charger Last Session Energy",    AFTER_SESSION, "online",  "8084.0"),
    ("Charger Last Session Delivered", AFTER_SESSION, "online",  "7.555"),
    ("Charger Last Session End Reason", AFTER_SESSION, "online", "EVDisconnected"),

    ("Charger Last Session Energy",    MID_SESSION,   "online",  "8084.0"),
    ("Charger Last Session Delivered", MID_SESSION,   "online",  "7.555"),
    ("Charger Session Meter Start",    MID_SESSION,   "online",  "8084.0"),

    ("Charger Last Session Energy",    FRESH,         "offline", None),
    ("Charger Last Session Delivered", FRESH,         "offline", None),
    ("Charger Last Session End Reason", FRESH,        "online",  "Unknown"),
    ("Charger Session Meter Start",    FRESH,         "offline", None),

    # The regression guard: a payload missing the keys entirely must read as
    # unavailable, NOT as a confident 0.000 kWh.
    ("Charger Last Session Energy",    LEGACY,        "offline", None),
    ("Charger Last Session Delivered", LEGACY,        "offline", None),

    ("Charger Last Session Energy",    NO_DELTA,      "online",  "8084.0"),
    ("Charger Last Session Delivered", NO_DELTA,      "offline", None),
    ("Charger Last Session End Reason", NO_DELTA,     "online",  "Unknown"),

    ("Charger Status",                 AFTER_SESSION, "online",  "Available"),
    ("Charger Transaction ID",         AFTER_SESSION, "online",  "none"),
    ("Charger Transaction ID",         MID_SESSION,   "online",  "1000000002"),

    # The limit status: `none` until a limit command has ever run, including
    # on a snapshot from before the feature existed.
    ("Charger Current Limit Status",   AFTER_SESSION, "online",  "none"),
    ("Charger Current Limit Status",   LEGACY,        "online",  "none"),
    ("Charger Current Limit Status",   LIMITED,       "online",  "accepted"),
    ("Charger Current Limit Status",   REFUSED,       "online",  "rejected"),
]

# The `number` entity's template: the standing limit, or "None" (its
# payload_reset) when there is none — including on a legacy snapshot.
NUMBER_CASES = [
    ("Charger Current Limit", LIMITED, "16.0"),
    ("Charger Current Limit", PAUSED,  "0"),
    ("Charger Current Limit", AFTER_SESSION, "None"),
    ("Charger Current Limit", LEGACY,  "None"),
]

fails = 0
for name, payload, want_avail, want_state in CASES:
    s = sensors[name]
    got_avail = avail(s, payload)
    got_state = render(s["value_template"], payload)
    label = {id(AFTER_SESSION): "after", id(MID_SESSION): "mid",
             id(FRESH): "fresh", id(LEGACY): "legacy",
             id(NO_DELTA): "no-delta", id(LIMITED): "limited",
             id(REFUSED): "refused"}[id(payload)]
    ok = got_avail == want_avail and (want_state is None or got_state == want_state)
    if not ok:
        fails += 1
    print("%-4s %-34s %-9s avail=%-8s state=%s%s" % (
        "PASS" if ok else "FAIL", name, label, got_avail, got_state,
        "" if ok else "   WANTED avail=%s state=%s" % (want_avail, want_state)))

numbers = {n["name"]: n for n in pkg["mqtt"].get("number", [])}
for name, payload, want_state in NUMBER_CASES:
    got_state = render(numbers[name]["value_template"], payload)
    ok = got_state == want_state
    if not ok:
        fails += 1
    label = {id(LIMITED): "limited", id(PAUSED): "paused",
             id(AFTER_SESSION): "after", id(LEGACY): "legacy"}[id(payload)]
    print("%-4s %-34s %-9s state=%s%s" % (
        "PASS" if ok else "FAIL", name, label, got_state,
        "" if ok else "   WANTED state=%s" % want_state))

print("\n%d cases, %d failures" % (len(CASES) + len(NUMBER_CASES), fails))
sys.exit(1 if fails else 0)
