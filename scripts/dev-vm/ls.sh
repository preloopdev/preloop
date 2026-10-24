#!/bin/sh
# List dev VMs forked from the golden.
set -eu
. "$(dirname "$0")/common.sh"
dev_vm_require_golden
"$SMOLVM_BIN" machine ls --json | jq -r --arg base "$PRELOOP_DEV_BASE" \
  '.[] | select(.parent_machine == $base) | [.name, .state] | @tsv'
