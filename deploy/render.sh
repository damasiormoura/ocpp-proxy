#!/usr/bin/env bash
#
# Render the deployment templates from deploy/local.env.
#
# Every site-specific value — Charge Point ID, Central System address, broker
# address — is a deployment parameter rather than repository content, so the
# templates carry @PLACEHOLDER@ tokens and this fills them in. Output goes to
# deploy/.rendered/, which is gitignored. Deploy from there.
#
# Usage:  ./deploy/render.sh [path/to/local.env]
#
# Exits non-zero if any placeholder is left unsubstituted, so a parameter added
# to a template but not to local.env fails here rather than on the target.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${1:-$REPO_ROOT/deploy/local.env}"
OUT_DIR="$REPO_ROOT/deploy/.rendered"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "error: $ENV_FILE not found." >&2
  echo "       cp deploy/local.env.example deploy/local.env, then fill it in." >&2
  exit 1
fi

# shellcheck disable=SC1090
set -a; source "$ENV_FILE"; set +a

: "${CHARGE_POINT_ID:?must be set in $ENV_FILE}"
: "${CENTRAL_SYSTEM_URL:?must be set in $ENV_FILE}"
: "${MQTT_HOST:?must be set in $ENV_FILE}"

# Home Assistant builds an entity_id from the device name plus the sensor name,
# lowercasing and replacing every run of non-alphanumerics with a single
# underscore. The dashboard references those IDs, so the same slug has to be
# derived here rather than written out by hand.
CHARGE_POINT_SLUG="ev_charger_$(printf '%s' "$CHARGE_POINT_ID" \
  | tr '[:upper:]' '[:lower:]' \
  | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
export CHARGE_POINT_SLUG

: "${LISTEN_ADDRESS:=0.0.0.0}"
: "${LISTEN_PORT:=9000}"
: "${HEALTH_PORT:=8080}"
: "${MQTT_PORT:=1883}"
: "${CENTRAL_SYSTEM_NETWORK:?must be set in $ENV_FILE}"
: "${WWAN_ADDRESS:?must be set in $ENV_FILE}"
: "${WWAN_GATEWAY:?must be set in $ENV_FILE}"
: "${WWAN_NETWORK:?must be set in $ENV_FILE}"
: "${UPSTREAM_BIND_ADDRESS:=}"

TEMPLATES=(
  "deploy/lxc/guest/config.yaml"
  "deploy/lxc/guest/ocpp-routes.service"
  "deploy/lxc/guest/ocpp-routes.sh"
  "deploy/lxc/host/ocpp-wwan.conf"
  "deploy/lxc/host/wwan-watchdog.sh"
  "deploy/homeassistant/ocpp_proxy.yaml"
  "deploy/homeassistant/dashboard.yaml"
)

PARAMS=(
  CHARGE_POINT_ID CHARGE_POINT_SLUG CENTRAL_SYSTEM_URL CENTRAL_SYSTEM_NETWORK
  LISTEN_ADDRESS LISTEN_PORT HEALTH_PORT
  MQTT_HOST MQTT_PORT UPSTREAM_BIND_ADDRESS
  WWAN_ADDRESS WWAN_GATEWAY WWAN_NETWORK
)

rm -rf "$OUT_DIR"
for rel in "${TEMPLATES[@]}"; do
  out="$OUT_DIR/$rel"
  mkdir -p "$(dirname "$out")"
  cp "$REPO_ROOT/$rel" "$out"
  for name in "${PARAMS[@]}"; do
    value="${!name}"
    if [[ "$value" == *"|"* ]]; then
      echo "error: $name contains '|', which this renderer uses as its sed" >&2
      echo "       delimiter. Pick a value without one." >&2
      exit 1
    fi
    # '|' rather than '/': CENTRAL_SYSTEM_URL contains slashes.
    sed -i "s|@${name}@|${value}|g" "$out"
  done

  if grep -q '@[A-Z_][A-Z0-9_]*@' "$out"; then
    echo "error: unsubstituted placeholders left in $rel:" >&2
    grep -o '@[A-Z_][A-Z0-9_]*@' "$out" | sort -u | sed 's/^/       /' >&2
    exit 1
  fi
  echo "rendered  $rel"
done

echo
echo "Output in deploy/.rendered/ — deploy from there, not from the templates."
