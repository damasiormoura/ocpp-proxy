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
MOBIE_NET="10.200.10.0/24"
TABLE="100"
LOG_TAG="ocpp-wwan"

# Filled in from /etc/default/ocpp-wwan so this script carries no site values.
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

# 3. Default route in the WWAN table.
if ! ip route show table "$TABLE" 2>/dev/null | grep -q '^default'; then
    log "rota default na tabela $TABLE estava AUSENTE - reaplicando via $WWAN_GW"
    ip route replace default via "$WWAN_GW" dev "$IFACE" table "$TABLE" && repaired=1
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
#    TCP, not ICMP: this APN drops ping even when it is working, so a ping
#    probe would report a permanent false failure. There is also no public
#    host to probe — the APN is closed — so the target is Mobi.e itself.
if [ -n "$PROBE_HOST" ] && [ -n "$PROBE_PORT" ]; then
    if ! timeout 8 bash -c "</dev/tcp/$PROBE_HOST/$PROBE_PORT" 2>/dev/null; then
        log "AVISO: $PROBE_HOST:$PROBE_PORT inalcancavel via $IFACE"
        # A failure here is a warning, not a repair trigger — the link may be
        # fine and Mobi.e down. The proxy health endpoint is authoritative.
    fi
fi

[ "$repaired" -eq 1 ] && log "caminho WWAN reparado"
exit 0
