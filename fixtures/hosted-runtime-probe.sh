#!/bin/sh
# Hosted-runtime parity assertions — the single source of truth for
# `fixtures/workflows/hosted-runtime-parity.yml` (which runs this in a step, in
# an ad-hoc `docker run` and in a `container:` job) and for the
# `hosted-runtime-parity` CI job (which runs it inside a real preloop guest).
#
# Every value was read back from a real GitHub-hosted runner (workflow_dispatch
# probe on `ubuntu-24.04` and `ubuntu-24.04-arm`, image `20260927.320.1` /
# `20260927.135.1`, kernel `6.17.0-1022-azure`):
#
#   Max stack size   16777216 / unlimited   (`ulimit -Ss` 16384, `-Hs` unlimited)
#   Max open files   65536 / 65536
#   vm.max_map_count 262144, fs.inotify.max_user_watches 655360,
#   fs.inotify.max_user_instances 1280
#   the machine's own name resolves to an address of the machine
#   TERM and COLORTERM absent from the environment; CI=true, GITHUB_ACTIONS=true
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

fail() {
    echo "PARITY FAIL: $*" >&2
    exit 1
}

# ── process limits ──────────────────────────────────────────────────────
# The image's `DefaultLimitSTACK=16M:infinity` and `DefaultLimitNOFILE=65536`.
stack_soft=$(ulimit -Ss)
stack_hard=$(ulimit -Hs)
nofile_soft=$(ulimit -Sn)
nofile_hard=$(ulimit -Hn)
echo "stack  soft=${stack_soft}KiB hard=${stack_hard}   (GitHub-hosted: 16384 / unlimited)"
echo "nofile soft=${nofile_soft} hard=${nofile_hard}      (GitHub-hosted: 65536 / 65536)"
[ "$stack_soft" = 16384 ] || fail "stack soft is ${stack_soft}KiB, expected 16384"
[ "$stack_hard" = unlimited ] || fail "stack hard is ${stack_hard}, expected unlimited"
[ "$nofile_soft" = 65536 ] || fail "nofile soft is ${nofile_soft}, expected 65536"
[ "$nofile_hard" = 65536 ] || fail "nofile hard is ${nofile_hard}, expected 65536"

# ── sysctls ─────────────────────────────────────────────────────────────
# The values `images/ubuntu/scripts/build/configure-environment.sh` writes.
for pair in vm.max_map_count=262144 \
    fs.inotify.max_user_watches=655360 \
    fs.inotify.max_user_instances=1280; do
    key=${pair%%=*}
    want=${pair#*=}
    got=$(cat "/proc/sys/$(printf '%s' "$key" | tr '.' '/')" 2>/dev/null || echo MISSING)
    echo "${key}=${got} (GitHub-hosted: ${want})"
    [ "$got" = "$want" ] || fail "${key} is ${got}, expected ${want}"
done

# ── the machine's own name ──────────────────────────────────────────────
# A hosted runner's own name resolves to an address of the machine (GitHub's
# /etc/hosts maps the VM's name to its interface address). A stale mapping to
# an address the machine does not own is the AgentENV failure mode.
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

# ── the step environment ────────────────────────────────────────────────
# A hosted step's environment has no TERM and no COLORTERM: the `dumb` a bash
# step prints for `$TERM` is bash's own default for an unset TERM, not an
# exported variable.
if env | grep -qE '^(TERM|COLORTERM)='; then
    fail "TERM/COLORTERM must not be exported into a step: $(env | grep -E '^(TERM|COLORTERM)=' | tr '\n' ' ')"
fi
echo "TERM/COLORTERM: absent (matching GitHub-hosted)"
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
# pydantic's shape: at the kernel's 8192 KiB stack a workload that walks more
# than 8 MiB of native frames dies with SIGSEGV instead of returning.
if [ "$recursion" = 1 ]; then
    cat >/tmp/hosted-runtime-recurse.c <<'EOF'
#include <stdio.h>

static volatile int sink;

/* -O0 keeps every frame real: ~2 KiB of pad per call, 5000 calls is ~10 MiB —
 * past 8192 KiB, comfortably inside 16384 KiB. */
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
