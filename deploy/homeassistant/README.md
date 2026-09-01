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
| `sensor.ev_charger_mobi_alm_00058_charger_last_session_energy` | `charger/StopTransaction` | kWh at transaction close |
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

Connector status, error code, transaction ID and session meter start come from
the **retained** `ocpp/{id}/state` topic, so they are correct the instant Home
Assistant subscribes. Verified across a restart: status read `Available` and
transaction `none` immediately, with no wait for the charger to do anything.

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
