#!/bin/sh
# Shared helpers for the preloop-dev SmolVM workflow. Sourced, not executed.
#
# Environment (all optional):
#   SMOLVM_BIN                       smolvm binary (default: smolvm)
#   PRELOOP_DEV_BASE                 frozen golden machine (default: preloop-dev)
#   PRELOOP_DEV_CHILD_PREFIX         child name prefix (default: preloop-dev)
#   PRELOOP_DEV_GATEWAY_URL          OMP auth-gateway URL reachable FROM THE GUEST
#   PRELOOP_DEV_GATEWAY_TOKEN_FILE   file holding the gateway bearer token
#   PRELOOP_DEV_EXEC_TIMEOUT         per-command timeout (default: 30m)
#
# When both gateway variables are set the child's OMP is configured to use the
# host's OMP auth gateway, so provider logins and API keys stay on the host. The
# token is injected per command with `--secret-file`; it is never written to the
# machine record, a checkpoint, or the guest filesystem.

SMOLVM_BIN=${SMOLVM_BIN:-smolvm}
PRELOOP_DEV_BASE=${PRELOOP_DEV_BASE:-preloop-dev}
PRELOOP_DEV_CHILD_PREFIX=${PRELOOP_DEV_CHILD_PREFIX:-preloop-dev}

die() { printf 'dev-vm: %s\n' "$*" >&2; exit 1; }

# Sanitize into a name smolvm accepts.
dev_vm_slug() {
  printf '%s' "$1" | tr -cs 'A-Za-z0-9_.-' '-' | sed 's/^-*//; s/-*$//'
}

# Child name for a worktree path (default: the current Git worktree).
dev_vm_name_for() {
  _dir=${1:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}
  _slug=$(dev_vm_slug "$(basename "$_dir")")
  [ -n "$_slug" ] || die "cannot derive a name from $_dir"
  printf '%s\n' "$_slug"
}

dev_vm_machine() { printf '%s-%s\n' "$PRELOOP_DEV_CHILD_PREFIX" "$1"; }

dev_vm_golden_state() {
  "$SMOLVM_BIN" machine status --name "$PRELOOP_DEV_BASE" --json 2>/dev/null \
    | sed -n 's/.*"state"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1
}

dev_vm_require_golden() {
  command -v "$SMOLVM_BIN" >/dev/null 2>&1 || die "smolvm not found ($SMOLVM_BIN)"
  _state=$(dev_vm_golden_state)
  [ -n "$_state" ] || die "golden '$PRELOOP_DEV_BASE' does not exist on this host"
  case "$_state" in
    frozen|running) ;;
    *) die "golden '$PRELOOP_DEV_BASE' is '$_state'; it must be frozen or running" ;;
  esac
}

dev_vm_gateway_enabled() {
  [ -n "${PRELOOP_DEV_GATEWAY_URL:-}" ] && [ -n "${PRELOOP_DEV_GATEWAY_TOKEN_FILE:-}" ]
}

dev_vm_require_child() {
  "$SMOLVM_BIN" machine status --name "$1" >/dev/null 2>&1 \
    || die "no dev VM '$1'; run: just dev-vm-up"
}
