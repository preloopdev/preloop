#!/usr/bin/env bash
#
# e2e-setup.sh — one-time port redirect so aksh can receive traffic on port 80.
#
# The official actions/runner strips non-default ports from HTTP URLs and always
# connects to port 80. We redirect 80 → 9090 so aksh can listen on 9090.
#
# Usage:
#   sudo ./scripts/e2e-setup.sh              # apply redirect
#   sudo ./scripts/e2e-setup.sh --teardown   # undo
#   ./scripts/e2e-setup.sh --status          # check if active
#

set -euo pipefail

AKSH_PORT="${AKSH_PORT:-9090}"

red()   { printf '\033[1;31m%s\033[0m\n' "$*"; }
green() { printf '\033[1;32m%s\033[0m\n' "$*"; }
dim()   { printf '\033[2m%s\033[0m\n' "$*"; }

# ── macOS ────────────────────────────────────────────────────────────────────
#
# rdr rules are NAT rules: shown by `pfctl -sn`, NOT `pfctl -sr` (filter rules).
# `pfctl -N -f` loads ONLY nat rules, so it never disturbs filter rules.
# pfctl normalizes "port 80" → "port = 80", so we match loosely.

pfctl_has_redirect() {
    sudo pfctl -sn 2>/dev/null | grep -qE "rdr .*lo0.*port [= ]*80\b.*port [= ]*$AKSH_PORT\b"
}

macos_status() {
    if pfctl_has_redirect; then
        green "✓ redirect active: 127.0.0.1:80 → 127.0.0.1:$AKSH_PORT"
        return 0
    fi
    red "✗ redirect not active"
    echo "  Fix: sudo $0"
    return 1
}

macos_setup() {
    local rule="rdr pass on lo0 inet proto tcp from any to 127.0.0.1 port 80 -> 127.0.0.1 port $AKSH_PORT"

    if pfctl_has_redirect; then
        green "✓ redirect already active"
        return 0
    fi

    # Build NAT ruleset: existing nat rules (minus old aksh redirect) + ours
    local tmp
    tmp=$(mktemp)
    trap "rm -f '$tmp'" RETURN
    {
        sudo pfctl -sn 2>/dev/null | grep -vE "rdr .*lo0.*port [= ]*80\b" || true
        echo "$rule"
    } > "$tmp"

    dim "  Loading NAT rules..."
    # -N loads ONLY nat rules (leaves filter rules untouched); -e enables pf
    sudo pfctl -N -f "$tmp" 2>&1 | grep -vE "ALTQ|flushing|main ruleset|pf\.conf|^$" || true
    sudo pfctl -E 2>&1 | grep -vE "ALTQ|already enabled|Token|^$" || true

    if pfctl_has_redirect; then
        green "✓ redirect active: 127.0.0.1:80 → 127.0.0.1:$AKSH_PORT"
    else
        red "✗ redirect applied but verification failed"
        dim "  pfctl -sn output:"
        sudo pfctl -sn 2>/dev/null | head -5 >&2
        return 1
    fi
}

macos_teardown() {
    local tmp
    tmp=$(mktemp)
    trap "rm -f '$tmp'" RETURN

    # Reload nat rules without our redirect
    sudo pfctl -sn 2>/dev/null | grep -vE "rdr .*lo0.*port [= ]*80\b" > "$tmp" 2>/dev/null || true
    sudo pfctl -N -f "$tmp" 2>&1 | grep -vE "ALTQ|flushing|main ruleset|pf\.conf|^$" || true

    green "✓ redirect removed"
}

# ── Linux ────────────────────────────────────────────────────────────────────

linux_status() {
    # Check the exact rule the setup adds; -L output shows REDIRECT as
    # "redir ports 9090" with no colon, so a grep for ":$AKSH_PORT" never
    # matches. -C returns 0 iff an identical rule already exists.
    if sudo iptables -t nat -C OUTPUT -p tcp -d 127.0.0.1 --dport 80 \
        -j REDIRECT --to-port "$AKSH_PORT" 2>/dev/null; then
        green "✓ redirect active: 127.0.0.1:80 → 127.0.0.1:$AKSH_PORT"
        return 0
    fi
    red "✗ redirect not active"
    echo "  Fix: sudo $0"
    return 1
}

linux_setup() {
    if sudo iptables -t nat -C OUTPUT -p tcp -d 127.0.0.1 --dport 80 -j REDIRECT --to-port "$AKSH_PORT" 2>/dev/null; then
        green "✓ redirect already active"
        return 0
    fi
    sudo iptables -t nat -A OUTPUT -p tcp -d 127.0.0.1 --dport 80 -j REDIRECT --to-port "$AKSH_PORT"
    green "✓ redirect active: 127.0.0.1:80 → 127.0.0.1:$AKSH_PORT"
}

linux_teardown() {
    sudo iptables -t nat -D OUTPUT -p tcp -d 127.0.0.1 --dport 80 -j REDIRECT --to-port "$AKSH_PORT" 2>/dev/null || true
    green "✓ redirect removed"
}

# ── dispatch ─────────────────────────────────────────────────────────────────

case "${1:-}" in
    -h|--help)
        echo "Usage: sudo $0              # apply redirect (80 → ${AKSH_PORT})"
        echo "       sudo $0 --teardown   # remove redirect"
        echo "       $0 --status          # check if active"
        echo ""
        echo "Set AKSH_PORT to change the target port (default: 9090)."
        exit 0
        ;;
    --status)
        case "$(uname -s)" in
            Darwin) macos_status ;;
            Linux)  linux_status ;;
            *)      red "unsupported OS"; exit 1 ;;
        esac
        ;;
    --teardown)
        if [ "$(id -u)" -ne 0 ]; then
            red "Need root: sudo $0 --teardown"
            exit 1
        fi
        case "$(uname -s)" in
            Darwin) macos_teardown ;;
            Linux)  linux_teardown ;;
            *)      red "unsupported OS"; exit 1 ;;
        esac
        ;;
    "")
        if [ "$(id -u)" -ne 0 ]; then
            red "Need root: sudo $0"
            exit 1
        fi
        case "$(uname -s)" in
            Darwin) macos_setup ;;
            Linux)  linux_setup ;;
            *)      red "unsupported OS"; exit 1 ;;
        esac
        ;;
    *)
        red "Unknown option: $1"
        echo "Run '$0 --help' for usage."
        exit 1
        ;;
esac
