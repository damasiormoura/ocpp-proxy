#!/bin/sh
# Add the static routes the OCPP proxy needs on its two gateway-less legs.
#
# Why this is a script and not two ExecStart lines:
#
# At container boot, `network-online.target` is reached before Proxmox has
# finished applying addresses to eth1 and eth2. `ip route replace ... via`
# then fails with "Nexthop has invalid gateway", the unit exits non-zero, and
# the proxy never starts. Observed on a real reboot, not hypothesised — the
# service came back up with no routes and no proxy, and the only reason it was
# visible at all was the MQTT Last Will reporting the proxy offline.
#
# So wait for each next hop to become routable before installing anything.
#
# Install to /usr/local/sbin/ocpp-routes.sh (mode 755), driven by
# ocpp-routes.service.
set -u

MOBIE_NET="10.200.10.0/24"
MOBIE_GW="10.80.0.1"        # the Proxmox host, across vmbr2
MOBIE_DEV="eth1"

IOT_NET="192.168.51.0/24"   # the charger's segment
IOT_GW="192.168.52.1"       # the Proxmox host, across vmbr1
IOT_DEV="eth2"

TIMEOUT=60

log() { echo "ocpp-routes: $*"; }

# Wait until $1 is reachable as a directly connected next hop on $2.
wait_for_nexthop() {
    gw=$1
    dev=$2
    n=0
    while [ "$n" -lt "$TIMEOUT" ]; do
        if ip -4 route get "$gw" 2>/dev/null | grep -q "dev $dev"; then
            [ "$n" -gt 0 ] && log "$gw became routable on $dev after ${n}s"
            return 0
        fi
        n=$((n + 1))
        sleep 1
    done
    log "FAILED: $gw never became routable on $dev within ${TIMEOUT}s"
    return 1
}

rc=0

if wait_for_nexthop "$MOBIE_GW" "$MOBIE_DEV"; then
    ip route replace "$MOBIE_NET" via "$MOBIE_GW" dev "$MOBIE_DEV" \
        || { log "FAILED to add the Mobi.e route"; rc=1; }
else
    rc=1
fi

if wait_for_nexthop "$IOT_GW" "$IOT_DEV"; then
    ip route replace "$IOT_NET" via "$IOT_GW" dev "$IOT_DEV" \
        || { log "FAILED to add the charger return route"; rc=1; }
else
    rc=1
fi

[ "$rc" -eq 0 ] && log "both routes installed"
exit "$rc"
