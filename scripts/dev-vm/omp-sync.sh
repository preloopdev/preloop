#!/bin/sh
# (Re)write the VM's OMP provider config from what the host gateway serves now.
#   omp-sync.sh [NAME]
# Run it after logging into a new provider or adding an API key to the broker.
# No-op unless PRELOOP_DEV_GATEWAY_URL and PRELOOP_DEV_GATEWAY_TOKEN_FILE are set.
set -eu
. "$(dirname "$0")/common.sh"

NAME=${1:-${PRELOOP_DEV_NAME:-$(dev_vm_name_for)}}
CHILD=$(dev_vm_machine "$NAME")
dev_vm_require_child "$CHILD"
dev_vm_gateway_enabled || { echo "OMP gateway brokering not configured; skipping"; exit 0; }
[ -r "$PRELOOP_DEV_GATEWAY_TOKEN_FILE" ] \
  || die "gateway token unreadable: $PRELOOP_DEV_GATEWAY_TOKEN_FILE"
command -v curl >/dev/null 2>&1 || die "curl is required on the host"
command -v jq >/dev/null 2>&1 || die "jq is required on the host"

# Route every provider the gateway serves through it (pi-native transport).
PROVIDERS=$(curl -fsS --max-time 10 \
    -H "Authorization: Bearer $(cat "$PRELOOP_DEV_GATEWAY_TOKEN_FILE")" \
    "$PRELOOP_DEV_GATEWAY_URL/v1/models" \
  | jq -r '[.data[].owned_by] | unique[]') \
  || die "gateway $PRELOOP_DEV_GATEWAY_URL unreachable from the host or token rejected"
[ -n "$PROVIDERS" ] || die "gateway serves no providers; log in with: omp auth-broker login"

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT
{
  echo "providers:"
  for p in $PROVIDERS; do
    printf '  %s:\n    baseUrl: %s\n    transport: pi-native\n    apiKey: OMP_GATEWAY_TOKEN\n' \
      "$p" "$PRELOOP_DEV_GATEWAY_URL"
  done
} > "$TMP"
"$SMOLVM_BIN" machine exec --name "$CHILD" --timeout 30s -- mkdir -p /root/.omp/agent
"$SMOLVM_BIN" machine cp "$TMP" "$CHILD:/root/.omp/agent/models.yml"
echo "OMP in '$CHILD' routes through the gateway: $(echo "$PROVIDERS" | tr '\n' ' ')"
