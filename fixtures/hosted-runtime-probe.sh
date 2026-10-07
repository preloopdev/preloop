#!/bin/sh
# Hosted-runtime parity assertions — the single source of truth for
# `fixtures/workflows/hosted-runtime-parity.yml` (which runs this in a step, in
# an ad-hoc `docker run` and in a `container:` job) and for the
# `hosted-runtime-parity` CI job (which runs it inside a real preloop guest).
#
# Every expected value is read from `official-image.toml` (the golden-image
# config, `golden_*` keys), which is also what build.rs compiles into the
# orchestrator's guest init — so the init, this probe and the scheduled GitHub
# drift check cannot disagree. Nothing here hardcodes an expected value.
#
# Usage: hosted-runtime-probe.sh [--adhoc] [--recursion]
#
#   --adhoc      assert the caller is an ad-hoc `docker run`: a hosted runner
#                injects none of CI/GITHUB_ACTIONS there, so their absence is
#                part of the contract.
#   --recursion  also run the deep-recursion probes (the C shape that needs
#                ~10 MiB of frames, and pydantic-core's `test_recursive_call`
#                shape; needs gcc and python3 with pydantic-core importable).
#                The CI job leaves this off so it stays seconds long and needs
#                no network beyond the checkout.
set -u

mode=step
recursion=0
for arg in "$@"; do
    case "$arg" in
        --adhoc) mode=adhoc ;;
        --recursion) recursion=1 ;;
        *)
            echo "usage: $0 [--adhoc] [--recursion]" >&2
            exit 2
            ;;
    esac
done

# The repo root is this script's parent: the config sits beside `fixtures/`.
CONFIG="${GOLDEN_RUNTIME_CONFIG:-$(dirname "$0")/../official-image.toml}"
[ -r "$CONFIG" ] || {
    echo "PARITY FAIL: cannot read $CONFIG (the golden runtime config)" >&2
    exit 2
}
cfg() {
    sed -n "s/^$1[[:space:]]*=[[:space:]]*\"\(.*\)\"[[:space:]]*$/\1/p" "$CONFIG" | head -n 1
}

fail() {
    echo "PARITY FAIL: $*" >&2
    exit 1
}

# ── process limits ──────────────────────────────────────────────────────
want_stack_soft=$(cfg golden_rlimit_stack_soft_kib)
want_stack_hard=$(cfg golden_rlimit_stack_hard)
want_nofile_soft=$(cfg golden_rlimit_nofile_soft)
want_nofile_hard=$(cfg golden_rlimit_nofile_hard)
stack_soft=$(ulimit -Ss)
stack_hard=$(ulimit -Hs)
nofile_soft=$(ulimit -Sn)
nofile_hard=$(ulimit -Hn)
echo "stack  soft=${stack_soft}KiB hard=${stack_hard}   (golden: ${want_stack_soft} / ${want_stack_hard})"
echo "nofile soft=${nofile_soft} hard=${nofile_hard}      (golden: ${want_nofile_soft} / ${want_nofile_hard})"
[ "$stack_soft" = "$want_stack_soft" ] || fail "stack soft is ${stack_soft}KiB, expected ${want_stack_soft}"
[ "$stack_hard" = "$want_stack_hard" ] || fail "stack hard is ${stack_hard}, expected ${want_stack_hard}"
[ "$nofile_soft" = "$want_nofile_soft" ] || fail "nofile soft is ${nofile_soft}, expected ${want_nofile_soft}"
[ "$nofile_hard" = "$want_nofile_hard" ] || fail "nofile hard is ${nofile_hard}, expected ${want_nofile_hard}"

# ── sysctls ─────────────────────────────────────────────────────────────
for pair in \
    "vm.max_map_count=$(cfg golden_sysctl_vm_max_map_count)" \
    "fs.inotify.max_user_watches=$(cfg golden_sysctl_fs_inotify_max_user_watches)" \
    "fs.inotify.max_user_instances=$(cfg golden_sysctl_fs_inotify_max_user_instances)"; do
    key=${pair%%=*}
    want=${pair#*=}
    [ -n "$want" ] || fail "official-image.toml is missing the golden value for ${key}"
    got=$(cat "/proc/sys/$(printf '%s' "$key" | tr '.' '/')" 2>/dev/null || echo MISSING)
    echo "${key}=${got} (golden: ${want})"
    [ "$got" = "$want" ] || fail "${key} is ${got}, expected ${want}"
done

# ── the machine's own name ──────────────────────────────────────────────
if [ "$(cfg golden_hostname_resolves_locally)" = "true" ]; then
    host=$(hostname)
    addrs=$(getent ahosts "$host" 2>/dev/null | awk '{print $1}')
    [ -n "$addrs" ] || addrs=$(getent hosts "$host" 2>/dev/null | awk '{print $1}')
    echo "hostname=${host} resolves to ${addrs:-<nothing>}"
    [ -n "$addrs" ] || fail "the machine's own hostname ${host} does not resolve"
    local_addrs="127.0.0.1 ::1 $(hostname -I 2>/dev/null) $(ip -o addr show 2>/dev/null | awk '{print $4}' | cut -d/ -f1)"
    local_ok=0
    for addr in $addrs; do
        for local in $local_addrs; do
            [ "$addr" = "$local" ] && local_ok=1
        done
    done
    [ "$local_ok" = 1 ] || fail "hostname ${host} resolves to ${addrs}, none of which is an address of this machine"
fi

# ── the step environment ────────────────────────────────────────────────
# `golden_step_env_unset` lists the keys a hosted step's environment does not
# carry: the terminal identity, which the guest's exec channel would otherwise
# leak into steps. The `dumb` a bash step prints for `$TERM` is bash's own
# default for an unset TERM, not an exported variable.
for key in $(printf '%s' "$(cfg golden_step_env_unset)" | tr ',' ' '); do
    [ -n "$key" ] || continue
    if env | grep -qE "^${key}="; then
        fail "${key} must not be exported into a step: $(env | grep -E "^${key}=" | tr '\n' ' ')"
    fi
done
echo "unset in the step env: $(cfg golden_step_env_unset)"
case "$mode" in
    adhoc)
        # Nothing injects CI/GITHUB_ACTIONS into a container the workflow
        # starts itself, on a hosted runner either.
        [ -z "${CI:-}" ] || fail "an ad-hoc docker run must not carry CI, got ${CI}"
        [ -z "${GITHUB_ACTIONS:-}" ] || fail "an ad-hoc docker run must not carry GITHUB_ACTIONS, got ${GITHUB_ACTIONS}"
        echo "CI/GITHUB_ACTIONS: absent (matching GitHub-hosted's ad-hoc containers)"
        ;;
    *)
        [ "${CI:-}" = true ] || fail "CI is ${CI:-<unset>}, expected true"
        [ "${GITHUB_ACTIONS:-}" = true ] || fail "GITHUB_ACTIONS is ${GITHUB_ACTIONS:-<unset>}, expected true"
        echo "CI=true GITHUB_ACTIONS=true"
        ;;
esac

# ── deep recursion (opt-in) ─────────────────────────────────────────────
# pydantic's shape: below the golden's stack a workload that walks more native
# frames than the limit allows dies with SIGSEGV instead of returning.
if [ "$recursion" = 1 ]; then
    cat >/tmp/hosted-runtime-recurse.c <<'EOF'
#include <stdio.h>

static volatile int sink;

/* -O0 keeps every frame real: ~2 KiB of pad per call, 5000 calls is ~10 MiB —
 * past the kernel default, comfortably inside the golden's stack. */
static int descend(int depth) {
  char pad[2048];
  pad[0] = (char)depth;
  if (depth <= 0)
    return pad[0];
  return descend(depth - 1) + pad[0];
}

int main(void) {
  sink = descend(5000);
  printf("deep recursion survived: %d\n", sink);
  return 0;
}
EOF
    gcc -O0 -o /tmp/hosted-runtime-recurse /tmp/hosted-runtime-recurse.c ||
        fail "could not build the deep-recursion probe"
    /tmp/hosted-runtime-recurse ||
        fail "the deep-recursion probe died (exit $?) — a hosted runner survives it"

    python3 -m pip install --quiet --disable-pip-version-check pydantic-core ||
        fail "could not install pydantic-core for the pydantic-shaped probe"
    python3 - <<'PY' || fail "the pydantic-shaped recursion probe did not return a RecursionError"
import pydantic_core as pc
from pydantic_core import core_schema

s = pc.SchemaSerializer(
    core_schema.any_schema(
        serialization=core_schema.plain_serializer_function_ser_schema(
            lambda value: s.to_python(value)
        )
    )
)
for mode in ("python", "json"):
    try:
        if mode == "python":
            s.to_python(42)
        else:
            s.to_json(42)
    except pc.PydanticSerializationError as exc:
        assert "RecursionError" in str(exc), f"unexpected failure: {exc}"
        print(f"pydantic {mode}: {exc}")
    else:
        raise SystemExit(f"PARITY FAIL: pydantic {mode} did not recurse")
print("pydantic shape: RecursionError, no SIGSEGV")
PY
fi

echo "PARITY OK: $(uname -n) uid=$(id -u)"
