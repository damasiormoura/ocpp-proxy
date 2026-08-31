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
| `sensor.charger_power` | `charger/MeterValues` | W, `measurement` |
| `sensor.charger_energy_meter` | `charger/MeterValues` | kWh lifetime, `total_increasing` |
| `sensor.charger_current_l1` | `charger/MeterValues` | A |
| `sensor.charger_voltage_l1` | `charger/MeterValues` | V |
| `sensor.charger_status` | `charger/StatusNotification` | Available / Preparing / Charging / … |
| `sensor.charger_error_code` | `charger/StatusNotification` | `NoError` when healthy |
| `sensor.charger_transaction_id` | `central_system/StartTransaction` | assigned by Mobi.e |
| `sensor.charger_authorization` | `central_system/Authorize` | Accepted / Blocked / Invalid |
| `sensor.charger_session_meter_start` | `charger/StartTransaction` | kWh at transaction open |
| `sensor.charger_last_session_energy` | `charger/StopTransaction` | kWh at transaction close |
| `sensor.charger_session_energy` | template | energy delivered this session |
| `sensor.ocpp_proxy_upstream` / `_downstream` | `status` | retained |
| `binary_sensor.ocpp_proxy_online` | `availability` | driven by the MQTT Last Will |

## Energy dashboard

`sensor.charger_energy_meter` is `device_class: energy` with
`state_class: total_increasing`, so it can be added directly under
Settings → Dashboards → Energy → *Individual devices*. Home Assistant derives
daily and monthly totals from it; nothing extra is needed.

## Alert on the proxy going offline

`binary_sensor.ocpp_proxy_online` is the **only** thing that tells you the
proxy has died. Nothing else watches that process, and with the APN SIM in the
dongle the proxy is the charger's sole route to Mobi.e — if it stops, charging
and billing stop with it. Worth an automation:

```yaml
automation:
  - alias: "OCPP proxy offline"
    triggers:
      - trigger: state
        entity_id: binary_sensor.ocpp_proxy_online
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

**Session energy after a restart.** `sensor.charger_session_meter_start` comes
from a `StartTransaction` message, which is not retained. If Home Assistant
restarts mid-session, `sensor.charger_session_energy` reads `unknown` until the
next transaction begins. The lifetime meter and the Energy dashboard are
unaffected.
