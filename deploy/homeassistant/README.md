# Home Assistant integration

Sensors and a dashboard fed by the topics the proxy publishes. No custom
component and no HACS — plain MQTT discovery via YAML.

## Install

1. **Package file.** Copy `ocpp_proxy.yaml` to `/config/packages/ocpp_proxy.yaml`
   on the Home Assistant VM. If you have never used packages, add this to
   `configuration.yaml`:

   ```yaml
   homeassistant:
     packages: !include_dir_named packages
   ```

2. **Check and restart.** Developer Tools → YAML → *Check configuration*, then
   restart Home Assistant.

3. **Dashboard.** Settings → Dashboards → your dashboard → pencil → ⋮ → *Raw
   configuration editor*, and paste the `views:` entry from `dashboard.yaml`.

The MQTT broker is the Mosquitto add-on the proxy already publishes to, so no
broker configuration is needed.

## What you get

| Entity | Source topic | Notes |
|---|---|---|
| `sensor.ev_charger_mobi_alm_00058_charger_power` | `charger/MeterValues` | W, `measurement` |
| `sensor.ev_charger_mobi_alm_00058_charger_energy_meter` | `charger/MeterValues` | kWh lifetime, `total_increasing` |
| `sensor.ev_charger_mobi_alm_00058_charger_current_l1` | `charger/MeterValues` | A |
| `sensor.ev_charger_mobi_alm_00058_charger_voltage_l1` | `charger/MeterValues` | V |
| `sensor.ev_charger_mobi_alm_00058_charger_status` | `charger/StatusNotification` | Available / Preparing / Charging / … |
| `sensor.ev_charger_mobi_alm_00058_charger_error_code` | `charger/StatusNotification` | `NoError` when healthy |
| `sensor.ev_charger_mobi_alm_00058_charger_transaction_id` | `central_system/StartTransaction` | assigned by Mobi.e |
| `sensor.ev_charger_mobi_alm_00058_charger_authorization` | `central_system/Authorize` | Accepted / Blocked / Invalid |
| `sensor.ev_charger_mobi_alm_00058_charger_session_meter_start` | `charger/StartTransaction` | kWh at transaction open |
| `sensor.ev_charger_mobi_alm_00058_charger_last_session_energy` | `state` | kWh on the register at transaction close |
| `sensor.ev_charger_mobi_alm_00058_charger_last_session_delivered` | `state` | kWh the last session actually delivered |
| `sensor.ev_charger_mobi_alm_00058_charger_last_session_end_reason` | `state` | `EVDisconnected`, `Local`, `Remote`, … |
| `sensor.charger_session_energy` | template | energy delivered this session |
| `sensor.ev_charger_mobi_alm_00058_ocpp_proxy_upstream` / `_downstream` | `status` | retained |
| `binary_sensor.ev_charger_mobi_alm_00058_ocpp_proxy_online` | `availability` | driven by the MQTT Last Will |

## Energy dashboard

`sensor.ev_charger_mobi_alm_00058_charger_energy_meter` is `device_class: energy` with
`state_class: total_increasing`, so it can be added directly under
Settings → Dashboards → Energy → *Individual devices*. Home Assistant derives
daily and monthly totals from it; nothing extra is needed.

## Alert on the proxy going offline

`binary_sensor.ev_charger_mobi_alm_00058_ocpp_proxy_online` is the **only** thing that tells you the
proxy has died. Nothing else watches that process, and with the APN SIM in the
dongle the proxy is the charger's sole route to Mobi.e — if it stops, charging
and billing stop with it. Worth an automation:

```yaml
automation:
  - alias: "OCPP proxy offline"
    triggers:
      - trigger: state
        entity_id: binary_sensor.ev_charger_mobi_alm_00058_ocpp_proxy_online
        to: "off"
        for: "00:02:00"
    actions:
      - action: notify.persistent_notification
        data:
          title: "OCPP proxy offline"
          message: >
            The EV charger has no path to Mobi.e. Check LXC 113 on the
            Proxmox host.
```

The two-minute delay avoids firing on a routine restart, which systemd
completes in about five seconds.

## Notes on the templates

Two things in `ocpp_proxy.yaml` are load-bearing and easy to get wrong if you
edit it:

**Call vs CallResult indices.** An OCPP `Call` is
`[2, uniqueId, action, args]` and a `CallResult` is `[3, uniqueId, args]`. So
`charger/*` topics read `payload[3]` and `central_system/*` topics read
`payload[2]`. Swapping them produces sensors that sit silently at `unknown`.

**Empty measurand lists.** This charger sends reduced `MeterValues` — some
carry every measurand, some only the energy register, and `transactionId` is
present on some and absent on others. The templates materialise the filtered
list and length-check it rather than using `| first`, which raises on an empty
sequence and takes the sensor unavailable. All templates were validated
against captured live payloads including the reduced and empty cases.

**What survives a Home Assistant restart, and what does not.**

Connector status, error code, transaction ID, session meter start and the three
previous-session figures all come from the **retained** `ocpp/{id}/state` topic,
so they are correct the instant Home Assistant subscribes. Verified across a
restart: status read `Available` and transaction `none` immediately, with no
wait for the charger to do anything.

Power, current, voltage and the lifetime energy meter come from `MeterValues`,
which the charger only sends **during a session**. While nothing is plugged in
they read `unknown` after a restart, and repopulate on the next charge. That is
truthful rather than broken — there is no current power reading when no car is
connected. They are deliberately not in the retained snapshot: `MeterValues`
arrives every few seconds while charging, and retaining it would mean a
retained publish per meter reading.

The Energy dashboard is unaffected either way: it is built from long-term
statistics in the database, not from the live state.

`sensor.charger_session_meter_start` reports `unavailable` while no transaction
is open, which is why `sensor.charger_session_energy` is also unavailable then
rather than showing a stale or zero figure.

## Testing the templates

`render_check.py` renders every state-topic template against the payload shapes
the broker actually serves — mid-session, after a session, a fresh snapshot, one
where the proxy missed the StartTransaction, and the pre-change payload that has
none of the `last_*` keys — and asserts both the state and the resolved
availability for each:

```
python3 deploy/homeassistant/render_check.py     # needs pyyaml + jinja2
```

The templates are the one part of this integration with no compiler behind
them, and they fail silently: a template that raises takes its sensor
`unavailable` rather than logging anything obvious. Two real bugs were caught
this way — `| first` raising on the reduced `MeterValues` this charger sends,
and a missing key rendering a confident `0.000 kWh` instead of going
unavailable.

## The previous session, and why there are two numbers for it

`..._charger_last_session_energy` is the **lifetime register reading** at the
moment the last session closed. `..._charger_last_session_delivered` is what
that session actually **delivered** — `meterStop - meterStart`, which the proxy
computes while both readings are still in hand. For the 1 Sep session those were
8084.000 kWh and 7.555 kWh respectively; confusing the two is easy and the
second is almost always the one you want.

They are two entities rather than one because the first already had recorder
history under `state_class: total`. Redefining what it reports would have put a
step of several thousand kWh in that history.

Both were previously fed from the non-retained `charger/StopTransaction` topic
and so read `unknown` after every Home Assistant restart, until another session
happened to end. The proxy keeps `last_meter_stop_wh`, `last_session_energy_wh`,
`last_transaction_id`, `last_stop_reason` and `last_stop_time` in the retained
snapshot across the following session precisely so this topic can answer at any
time.

`..._charger_last_session_delivered` goes `unavailable`, not zero, when the
proxy never saw the matching `StartTransaction` — a proxy restart mid-session.
The delta is genuinely unknown then, and a zero would silently understate a real
charge.

**The snapshot is durable.** It survives a Home Assistant restart, which is what
it was built for, and also a *proxy* restart: the proxy writes it to
`/var/lib/ocpp-proxy/state.json` on every change and reads it back before its
event loop starts, then republishes it onto the retained topic on the first
broker connection. A deploy or an LXC reboot therefore leaves the
previous-session rows populated.

A file rather than seeding from this retained topic, which needs no disk: the
charger reconnects within a few seconds of the proxy starting, and if its first
StatusNotification were folded in before the seed arrived, the republish would
clobber the retained topic with a blank snapshot. Reading a file happens before
the event loop exists, so there is no race to lose — and it survives a broker
that came back without its own retained store, which the proxy then repairs by
republishing.

Nothing about it is fatal: an unwritable or corrupt file is a warning and an
empty snapshot, never a failure to start. The proxy is the charger's only route
to Mobi.e, so losing dashboard history must never cost charging.

Restarting the proxy mid-session restores the open transaction too, so a session
interrupted by a deploy still gets its delivered figure when it ends.
