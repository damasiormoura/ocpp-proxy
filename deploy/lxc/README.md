# Deployment — Proxmox LXC

Replaces the AWS ECS Fargate deployment. There is no orchestrator, no load
balancer, and no cloud secret store: a single Rust binary under systemd in an
unprivileged LXC, with the 4G dongle owned by the Proxmox host.

## Read this first

The charger's **only** path to Mobi.e now runs through this container and the
dongle attached to `mouraishikawa`. If the Proxmox host is down, charging
authorization and billing stop — not just Home Assistant visibility. That is a
deliberate accepted trade, not an oversight; see *Availability posture* in
`.kiro/specs/ocpp-proxy-ha-integration/requirements.md`. Everything below exists
to make the recoverable failures recover unattended and the unrecoverable ones
obvious.

## Target state

| Item | Value |
|---|---|
| Guest | LXC 113 `ocpp-proxy`, unprivileged, `onboot: 1` |
| Resources | 1 vCPU / 512 MB / 8 GB |
| LAN address | `192.168.50.28/24` on `vmbr0`, gw `192.168.50.1` |
| WWAN egress address | `10.80.0.2/30` on `vmbr2`, no gateway |
| Charger port | `9000` on the LAN address only |
| Health port | `8080` |
| MQTT broker | `192.168.50.167:1883` (Mosquitto on VM 110) |
| Host WWAN interface | `wwan0` (ZTE `19d2:1405`, `cdc_ether`) |

`.28` is the next free address in the container block per
`network/addressing.md` in the mouraishikawa repo. **Update that table and
`proxmox/inventory.md` when the guest is created** — that repo's convention is
that they stay in sync.

## Values still unknown

Two must be filled in before this works, both marked `__PLACEHOLDER__` in the
config files:

| Placeholder | How to obtain |
|---|---|
| `__WWAN_ADDR__`, `__WWAN_GW__` | Insert the SIM, set the APN in the dongle's web UI, then `ip link set wwan0 up && dhclient -v wwan0 && ip -4 a show wwan0 && ip r`. Note the subnet and gateway, `dhclient -r wwan0`, then hard-code them. |
| `__MOBIE_HOST__` | The WebSocket URL Mobi.e issued for this charge point. Also confirm whether its hostname resolves publicly or only via the APN's DNS. |

## Step 1 — Host: pin the dongle's name

The dongle currently enumerates as `enx344b50000000`, derived from the
placeholder MAC `34:4b:50:00:00:00` it presents with no SIM. **That MAC is
expected to change once a SIM is registered**, renaming the interface and
breaking every rule that names it. Pin it by USB ID first:

```bash
scp host/10-wwan.link           proxmox:/etc/systemd/network/10-wwan.link
scp host/99-zte-no-storage.rules proxmox:/etc/udev/rules.d/
ssh proxmox 'udevadm control --reload && udevadm trigger --subsystem-match=net'
```

Replug the dongle, then confirm:

```bash
ssh proxmox 'ip -br link show wwan0'
```

Do not proceed until the interface is called `wwan0`.

## Step 2 — Host: SIM, APN, and the egress path

Insert the SIM and configure the APN in the dongle's own web interface (it does
PPP internally — there is no ModemManager or pppd on this host and none is
needed). Determine its subnet, fill the placeholders, then:

```bash
scp host/ocpp-wwan.conf proxmox:/etc/network/interfaces.d/ocpp-wwan
ssh proxmox 'ifup vmbr2 && ifup wwan0'
```

Verify the three things most likely to be wrong:

```bash
# a) The LAN default route is still the ONLY default route
ssh proxmox 'ip route show | grep ^default'      # expect exactly one, via 192.168.50.1

# b) The APN default route lives in table 100, reachable by policy rule
ssh proxmox 'ip route show table 100; ip rule show | grep 10.80.0.2'

# c) Traffic from the proxy's source address actually leaves via wwan0
ssh proxmox 'ip route get <mobie-ip> from 10.80.0.2'   # expect "dev wwan0"
```

A default route appearing on `wwan0` in the main table is the failure to watch
for: it silently pulls Proxmox updates and the Claude runners over a metered
mobile link. This is why the interface is configured static with no `gateway`
line rather than by DHCP.

## Step 3 — Host: the watchdog

```bash
scp host/wwan-watchdog.sh            proxmox:/usr/local/sbin/
scp host/ocpp-wwan-watchdog.service  proxmox:/etc/systemd/system/
scp host/ocpp-wwan-watchdog.timer    proxmox:/etc/systemd/system/
ssh proxmox 'chmod 750 /usr/local/sbin/wwan-watchdog.sh'
ssh proxmox 'printf "WWAN_GW=__WWAN_GW__\nPROBE_HOST=\n" > /etc/default/ocpp-wwan'
ssh proxmox 'systemctl daemon-reload && systemctl enable --now ocpp-wwan-watchdog.timer'
```

It checks the link, the table-100 route, the policy rule, the NAT rule and the
MSS clamp every 5 minutes, repairs what is missing, and logs to syslog under
tag `ocpp-wwan` — which LXC 101 collects. Same check-repair-log pattern as
`iot-isolation-enforce.sh`, and for the same reason: state you set up once does
not stay set up.

## Step 4 — Create the container

```bash
ssh proxmox 'pct create 113 local:vztmpl/debian-13-standard_13.0-1_amd64.tar.zst \
  --hostname ocpp-proxy --unprivileged 1 --onboot 1 \
  --cores 1 --memory 512 --swap 512 \
  --rootfs local-lvm:8 \
  --net0 name=eth0,bridge=vmbr0,ip=192.168.50.28/24,gw=192.168.50.1,firewall=1 \
  --net1 name=eth1,bridge=vmbr2,ip=10.80.0.2/30 \
  --features nesting=0'
```

`net1` carries **no gateway** on purpose. It exists only as a source address
for the host's policy rule; the container's default route stays on the LAN.

No `nesting=1`, no `keyctl=1`, and no device passthrough — this container runs
a bare binary, not Docker, and never touches the USB device.

## Step 5 — Restrict who can reach the charger port

The charger is an internet-capable appliance on the main LAN. Use the Proxmox
firewall on the container's NIC — not host `iptables`, which does not reliably
see bridged traffic:

```
# /etc/pve/firewall/113.fw on the host
[OPTIONS]
enable: 1
policy_in: DROP
policy_out: ACCEPT

[RULES]
IN ACCEPT -source 192.168.50.<charger> -p tcp -dport 9000 -log nolog # charger OCPP
IN ACCEPT -source 192.168.50.167       -p tcp -dport 8080 -log nolog # HA health poll
IN ACCEPT -source 192.168.50.0/24      -p tcp -dport 22   -log nolog # admin
```

Give the charger a **DHCP reservation on the main router** first, so its address
is stable enough to name here.

## Step 6 — Install the proxy

```bash
cargo build --release                        # or build on LXC 106 (gha-runner)
scp target/release/ocpp-proxy        proxmox:/tmp/
scp guest/ocpp-proxy.service         proxmox:/tmp/
scp guest/config.yaml                proxmox:/tmp/
ssh proxmox 'pct exec 113 -- adduser --system --group --no-create-home ocpp
             pct exec 113 -- mkdir -p /etc/ocpp-proxy /var/lib/ocpp-proxy
             pct push 113 /tmp/ocpp-proxy    /usr/local/bin/ocpp-proxy --perms 755
             pct push 113 /tmp/config.yaml   /etc/ocpp-proxy/config.yaml --perms 644
             pct push 113 /tmp/ocpp-proxy.service /etc/systemd/system/ocpp-proxy.service'
```

Create `/etc/ocpp-proxy/secrets.env` inside the container from
`guest/secrets.env.example`, mode 600 owner `root:ocpp`, with the Mosquitto
credentials. **Do not put them in `config.yaml`, and never commit them.**

```bash
ssh proxmox 'pct exec 113 -- systemctl daemon-reload
             pct exec 113 -- systemctl enable --now ocpp-proxy'
```

## Step 7 — Point the charger at the proxy

In the Autel's OCPP settings, set the Central System URL to:

```
ws://192.168.50.28:9000/<Charge_Point_ID>
```

The Charge Point ID must be exactly the one registered with Mobi.e — the proxy
mirrors it into the upstream URL path.

Plain `ws://` is a single hop on the local wire. If the Autel firmware refuses
anything but `wss://`, terminate TLS with a self-signed certificate on the
proxy and install its CA on the charger; note that some Autel firmware does not
allow adding a custom CA, in which case `ws://` is the only workable option.

## Verification

```bash
# Proxy is up and idle (no charger connected yet) — expect 200 and "idle"
ssh proxmox 'pct exec 113 -- curl -s localhost:8080/health'

# Upstream egress leaves via the dongle, MQTT does not
ssh proxmox 'pct exec 113 -- ss -tnp | grep ocpp-proxy'

# systemd really does restart it
ssh proxmox 'pct exec 113 -- systemctl kill -s SIGKILL ocpp-proxy; sleep 8;
             pct exec 113 -- systemctl is-active ocpp-proxy'

# Home Assistant sees availability flap offline -> online on
#   ocpp/<Charge_Point_ID>/availability
```

A health status of `idle` with HTTP 200 when no car is plugged in is **correct**.
The previous ECS design reported that state as `unhealthy`/503, which combined
with a restart-on-three-failures health check would have restarted the container
forever whenever nobody was charging.

## Manual bypass

Required by Requirement 11 criterion 13. If the proxy or the host is unavailable
for an extended period and charging must be restored:

1. Move the SIM from the dongle back into the charger, if the charger has a SIM
   slot and was previously provisioned for this APN.
2. Restore the charger's OCPP URL to Mobi.e's endpoint directly, replacing
   `ws://192.168.50.28:9000/...`.
3. Record the change — the proxy will not see traffic again until it is undone,
   and Home Assistant will show the charger permanently offline.

**Capture the charger's original Mobi.e URL and its OCPP settings before
step 7 above.** Without them this bypass cannot be performed, and recovering
them from Mobi.e support during an outage is not a plan.
