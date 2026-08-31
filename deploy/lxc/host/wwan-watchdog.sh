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
BIND_SRC="10.80.0.2"
TABLE="100"
LOG_TAG="ocpp-wwan"

# Filled in from /etc/default/ocpp-wwan so this script carries no site values.
WWAN_GW=""
PROBE_HOST=""
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

# 4. Policy rule steering the proxy's upstream socket into that table.
if ! ip rule show | grep -q "from $BIND_SRC lookup $TABLE"; then
    log "regra de policy routing de $BIND_SRC estava AUSENTE - reaplicando"
    ip rule add from "$BIND_SRC" lookup "$TABLE" priority 100 && repaired=1
fi

# 5. NAT and MSS clamp.
if ! iptables -t nat -C POSTROUTING -s 10.80.0.0/30 -o "$IFACE" -j MASQUERADE 2>/dev/null; then
    log "regra de MASQUERADE estava AUSENTE - reaplicando"
    iptables -t nat -A POSTROUTING -s 10.80.0.0/30 -o "$IFACE" -j MASQUERADE && repaired=1
fi
if ! iptables -t mangle -C FORWARD -o "$IFACE" -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu 2>/dev/null; then
    log "clamp de MSS estava AUSENTE - reaplicando"
    iptables -t mangle -A FORWARD -o "$IFACE" -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu && repaired=1
fi

# 6. End-to-end reachability through the APN, from the proxy's source address.
#    Only meaningful once PROBE_HOST is set to the Mobi.e endpoint.
if [ -n "$PROBE_HOST" ]; then
    if ! ping -c 2 -W 5 -I "$BIND_SRC" "$PROBE_HOST" >/dev/null 2>&1; then
        log "AVISO: $PROBE_HOST inalcancavel via $IFACE a partir de $BIND_SRC"
        # Some APNs drop ICMP; a failure here is a warning, not a repair
        # trigger. The proxy's own health endpoint is authoritative.
    fi
fi

[ "$repaired" -eq 1 ] && log "caminho WWAN reparado"
exit 0
