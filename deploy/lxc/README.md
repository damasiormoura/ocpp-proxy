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
| LAN address | `192.168.50.28/24` on `vmbr0`, gw `192.168.50.1` — MQTT and admin only |
| Charger-facing address | `192.168.52.30/24` on `vmbr1`, no gateway |
| WWAN egress address | `10.80.0.2/30` on `vmbr2`, no gateway |
| Charger port | `9000` on the `vmbr1` address only |
| Charger network | `192.168.51.0/24` (IoT), via the TL-WPA4220 powerline AP |
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

| Item | Status |
|---|---|
| Dongle subnet and gateway | **Resolved 2026-08-31.** `192.168.0.0/24`, gateway `192.168.0.1`, DHCP offers `.169`. Modem reports LTE, full signal, `ppp_connected`, operator NOS. Confirm the DHCP pool range in the dongle UI and move the static address outside it if `.169` falls inside. |
| Mobi.e endpoint | **Resolved.** `ws://10.200.10.200/ocpp/1.6/MOBI-ALM-00058`, read from the charger's own OCPP configuration. Charge Point ID `MOBI-ALM-00058`; the proxy is configured with the base `ws://10.200.10.200/ocpp/1.6` and appends the ID. |
| Charger address | **Still blocking.** The charger has not appeared on either network — no DHCP lease and no ARP entry on `192.168.51.0/24` or `192.168.50.0/24`. |

### The APN is a closed network — measured, not assumed

Verified 2026-08-31 while the modem reported a healthy connected LTE session:

| Test | Result |
|---|---|
| Dongle gateway `192.168.0.1` | reachable, 0.8 ms |
| ICMP to `1.1.1.1` | 100% loss |
| TCP to `http://example.com` | no connection |
| ICMP to `10.200.10.200` | **works, 79 ms** — inside the garden ICMP is fine |
| TCP to `10.200.10.200:80` | **open** |
| DNS to `192.168.0.1:53` | timeout — the dongle does not proxy DNS |
| DNS to `8.8.8.8` | timeout |
| DNS server in the DHCP lease | none offered |

This is a private APN behaving correctly, **not a fault**. Two consequences
that shape the deployment:

1. **The DNS gap costs nothing.** The Central System turned out to be a literal
   private address, `10.200.10.200`, so there is nothing to resolve. Had it
   been a hostname, the absence of DNS on this path would have been a problem.
2. **Never health-check the WWAN path against a public host.** No public host
   is reachable. ICMP *does* work to Mobi.e itself, but the watchdog makes a
   TCP connection to `10.200.10.200:80` because an open port proves the Central
   System is listening where a ping only proves the host answers. Set
   `PROBE_HOST` and `PROBE_PORT` in `/etc/default/ocpp-wwan`.
3. **Nothing is encrypted between the proxy and Mobi.e.** The endpoint is
   plaintext `ws://` on port 80; the private APN is the entire security
   boundary. That is the charger's own pre-existing arrangement, not something
   this design introduced — but it means the upstream leg needs no TLS at all.

### Routing: destination, not source

Because Mobi.e sits at a fixed RFC1918 range that collides with nothing here, a
plain destination route is enough and no policy rules or routing tables are
involved:

```
host:      ip route add 10.200.10.0/24 via 192.168.0.1  dev wwan0
container: ip route add 10.200.10.0/24 via 10.80.0.1    dev eth1
```

This replaces the earlier source-address policy-routing scheme. It is simpler,
keeps the application out of routing entirely (no `upstream_bind_address` to
configure or keep in sync), and removes the two-place `ip rule` arrangement
that was the easiest thing to get wrong. The route is still needed in **both**
places: the container must hand the packet to the host across `vmbr2`, and the
host must forward it out `wwan0` rather than back to the LAN.

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

Then prove the endpoint itself is reachable. TCP, not ping — this APN drops
ICMP even when it is working:

```bash
ssh proxmox 'ip route get 10.200.10.200'                       # expect dev wwan0
ssh proxmox 'timeout 8 bash -c "</dev/tcp/10.200.10.200/80" && echo REACHABLE'
```

For a full protocol-level check, a WebSocket upgrade should return
`101 Switching Protocols` with `Sec-WebSocket-Protocol: ocpp1.6`:

```bash
ssh proxmox 'curl -i --http1.1 -m 10 \
  -H "Connection: Upgrade" -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Version: 13" \
  -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
  -H "Sec-WebSocket-Protocol: ocpp1.6" \
  http://10.200.10.200/ocpp/1.6/MOBI-ALM-00058'
```

**Verified 2026-08-31 from the Proxmox host** — the full path works:

```
HTTP/1.1 101 Switching Protocols
Server: nginx/1.6.2
Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
Sec-WebSocket-Protocol: ocpp1.6
```

A `curl: (28) Operation timed out` printed alongside the 101 is expected, not a
failure: curl completes the upgrade then waits for data, and an OCPP Central
System waits for the charge point to speak first.

> **A 101 proves reachability, not identity.** Two controls were run against
> the same endpoint: omitting the subprotocol still returned 101, and a
> deliberately invalid Charge Point ID (`MOBI-DOES-NOT-EXIST`) *also* returned
> 101 with `ocpp1.6` negotiated. The nginx front end upgrades any path.
> Confirming that Mobi.e accepts `MOBI-ALM-00058` requires an actual OCPP
> `BootNotification` and reading the response status — which writes to the
> operator's system and should be done deliberately, not as a routine check.

> Do not run this while the charger is live on the same Charge Point ID.
> Mobi.e may close the charger's session in favour of the new connection, the
> same replacement behaviour this proxy implements for its own downstream.

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
ssh proxmox 'printf "WWAN_GW=192.168.0.1\nPROBE_HOST=10.200.10.200\nPROBE_PORT=80\n" > /etc/default/ocpp-wwan'
ssh proxmox 'systemctl daemon-reload && systemctl enable --now ocpp-wwan-watchdog.timer'
```

It checks the link, the Mobi.e route, the NAT rule and the MSS clamp every
5 minutes, repairs what is missing, and logs to syslog under
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
  --net2 name=eth2,bridge=vmbr1,ip=192.168.52.30/24 \
  --features nesting=0'
```

Three legs, each with one job: `eth0` for MQTT and admin, `eth1` as the source
address for Mobi.e egress, `eth2` facing the charger on the IoT side.

`net1` and `net2` carry **no gateway** on purpose. The container's only default
route stays on the LAN; Mobi.e traffic is selected by policy rule, not by
default route.

No `nesting=1`, no `keyctl=1`, and no device passthrough — this container runs
a bare binary, not Docker, and never touches the USB device.

## Step 5 — Container-side policy routing

**This is the step most easily missed, and it fails in a misleading way.**

Binding the upstream socket to `10.80.0.2` sets the source address. It does not
choose a route. Without a rule inside the container, the kernel resolves the
Mobi.e destination against the main table, matches the default route via
`192.168.50.1`, and hands the main router a packet sourced from `10.80.0.2` —
an address it has never heard of. The symptom is "Mobi.e unreachable", not
"routing misconfigured".

The rule is therefore needed in **both** places: in the container so the packet
is sent to the host across `vmbr2`, and on the host (Step 2) so the host
forwards it out `wwan0` rather than back to the LAN.

```bash
scp guest/ocpp-wwan-policy.service proxmox:/tmp/
ssh proxmox 'pct push 113 /tmp/ocpp-wwan-policy.service /etc/systemd/system/ocpp-wwan-policy.service
             pct exec 113 -- systemctl daemon-reload
             pct exec 113 -- systemctl enable --now ocpp-wwan-policy'
```

`ocpp-proxy.service` declares `Requires=` and `After=` on this unit, so the
proxy cannot start with the route missing.

Verify from inside the container:

```bash
ssh proxmox 'pct exec 113 -- ip rule show | grep 10.80.0.2
             pct exec 113 -- ip route show table 100'
```

## Step 6 — Restrict who can reach the charger port

> **The Proxmox firewall is currently disabled datacenter-wide.** `pve-firewall
> status` reports `disabled/running`, there is no `/etc/pve/firewall/cluster.fw`,
> and no guest has `firewall=1` set. Until the datacenter firewall is enabled,
> the `firewall=1` in the `pct create` above and every rule below is inert.
> Enabling it cluster-wide needs care: a default `policy_in: DROP` at datacenter
> level will lock you out of SSH and the 8006 web UI unless the management
> allows are in place first. Treat that as its own piece of work.

With the charger now on the isolated IoT network the urgency is lower, but the
proxy is still dual-homed onto the main LAN. Use the Proxmox firewall on the
container's NIC — not host `iptables`, which does not reliably see bridged
traffic:

```
# /etc/pve/firewall/113.fw on the host
[OPTIONS]
enable: 1
policy_in: DROP
policy_out: ACCEPT

[RULES]
IN ACCEPT -source 192.168.51.<charger> -p tcp -dport 9000 -log nolog # charger OCPP, IoT side
IN ACCEPT -source 192.168.50.167       -p tcp -dport 8080 -log nolog # HA health poll
IN ACCEPT -source 192.168.50.0/24      -p tcp -dport 22   -log nolog # admin
```

Give the charger a **DHCP reservation on the spare BD4** first, so its address
is stable enough to name here.

## Step 7 — Install the proxy

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

## Step 8 — Point the charger at the proxy

In the Autel's OCPP settings, set the Central System URL to:

```
ws://192.168.52.30:9000/<Charge_Point_ID>
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
   `ws://192.168.52.30:9000/...`.
3. Record the change — the proxy will not see traffic again until it is undone,
   and Home Assistant will show the charger permanently offline.

**Capture the charger's original Mobi.e URL and its OCPP settings before
step 8 above.** Without them this bypass cannot be performed, and recovering
them from Mobi.e support during an outage is not a plan.
