#!/bin/bash
# Re-assert the Mobi.e egress path on the Proxmox host.
#
# Why this exists: the OCPP proxy is the charger's only path to Mobi.e, and
# that path depends on a consumer USB dongle plus a handful of routing state
# that nothing else re-creates. A dongle that resets, a link that drops, or a
# flushed rule all fail the same way — silently. Charging simply stops working
# and nothing says why. This runs periodically, restores what is missing, and
# logs every corrective action.
#
# Same pattern as iot-isolation-enforce.sh in the mouraishikawa repo: check,
# repair, log; never assume the state you set up is still there.
#
# Install: /usr/local/sbin/wwan-watchdog.sh (mode 750, root:root)
# Driven by ocpp-wwan-watchdog.timer.
set -uo pipefail

IFACE="wwan0"
LXC_NET="10.80.0.0/30"
MOBIE_NET="@CENTRAL_SYSTEM_NETWORK@"
LOG_TAG="ocpp-wwan"

# Filled in from /etc/default/ocpp-wwan so this script carries no site values.
WWAN_ADDR=""
WWAN_GW=""
PROBE_HOST=""
PROBE_PORT=""
[ -r /etc/default/ocpp-wwan ] && . /etc/default/ocpp-wwan

log() { logger -t "$LOG_TAG" -- "$*"; }

if [ -z "$WWAN_GW" ]; then
    log "WWAN_GW nao configurado em /etc/default/ocpp-wwan - watchdog inativo"
    exit 1
fi

repaired=0

# 1. Interface present at all? If not, the dongle is gone — a human must act.
if [ ! -e "/sys/class/net/$IFACE" ]; then
    log "FALHA: interface $IFACE ausente - dongle desconectado ou nao enumerado"
    exit 1
fi

# 2. Link up.
if [ "$(cat "/sys/class/net/$IFACE/operstate" 2>/dev/null)" != "up" ]; then
    log "interface $IFACE estava DOWN - subindo"
    ip link set "$IFACE" up && repaired=1
    sleep 3
fi

# 3. Static IPv4 address on the dongle leg.
#
#    Found 2026-09-10: the interface can sit link-up (carrier and operstate
#    both "up") with no IPv4 address at all — no ifup/hotplug event logged
#    anywhere to explain it. Every later step then degrades the same silent
#    way: "ip route replace ... dev wwan0" fails immediately with "Nexthop
#    has invalid gateway", and Mobi.e connections time out from there. This
#    host has no udev rule wiring "allow-hotplug" interfaces back up on
#    re-enumeration, so once the address is gone, only this watchdog (or a
#    human, or a reboot) restores it — hence checking for it here, not just
#    the route it enables.
if [ -n "$WWAN_ADDR" ]; then
    if ! ip -o -4 addr show dev "$IFACE" | awk '{print $4}' | grep -qx "$WWAN_ADDR"; then
        log "endereco $WWAN_ADDR estava AUSENTE em $IFACE - reaplicando"
        ip addr replace "$WWAN_ADDR" dev "$IFACE" && repaired=1
    fi
else
    log "WWAN_ADDR nao configurado em /etc/default/ocpp-wwan - endereco nao verificado"
fi

# 4. Destination route for the Mobi.e range out of the dongle.
if ! ip route show | grep -q "^$MOBIE_NET"; then
    log "rota para $MOBIE_NET estava AUSENTE - reaplicando via $WWAN_GW"
    ip route replace "$MOBIE_NET" via "$WWAN_GW" dev "$IFACE" && repaired=1
fi

# 5. NAT and MSS clamp.
if ! iptables -t nat -C POSTROUTING -s "$LXC_NET" -o "$IFACE" -j MASQUERADE 2>/dev/null; then
    log "regra de MASQUERADE estava AUSENTE - reaplicando"
    iptables -t nat -A POSTROUTING -s "$LXC_NET" -o "$IFACE" -j MASQUERADE && repaired=1
fi
if ! iptables -t mangle -C FORWARD -o "$IFACE" -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu 2>/dev/null; then
    log "clamp de MSS estava AUSENTE - reaplicando"
    iptables -t mangle -A FORWARD -o "$IFACE" -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu && repaired=1
fi

# 6. End-to-end reachability through the APN.
#
#    TCP rather than ICMP. ICMP to Mobi.e does work (~79 ms, measured), so a
#    ping probe would function — but an open TCP port proves the Central
#    System is actually listening, where a ping only proves the host answers.
#    The target is Mobi.e itself because the APN is closed: there is no public
#    host to probe against.
if [ -n "$PROBE_HOST" ] && [ -n "$PROBE_PORT" ]; then
    if ! timeout 8 bash -c "</dev/tcp/$PROBE_HOST/$PROBE_PORT" 2>/dev/null; then
        log "AVISO: $PROBE_HOST:$PROBE_PORT inalcancavel via $IFACE"
        # A failure here is a warning, not a repair trigger — the link may be
        # fine and Mobi.e down. The proxy health endpoint is authoritative.
    fi
fi

[ "$repaired" -eq 1 ] && log "caminho WWAN reparado"
exit 0
