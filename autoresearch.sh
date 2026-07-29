#!/usr/bin/env bash
# Preloop comprehensive performance benchmark harness.
#
# Exercises the full stack: server throughput under concurrent load, parser,
# expression evaluator, workspace snapshotting, protocol serialization,
# mutex contention, and cold boot time. Then drives the same submission path
# over real TCP with `oha` and emits a machine-generated HTML dashboard.
#
# Every phase fails closed. A missing binary, a server that never becomes
# ready, an `oha` invocation the installed build rejects, unparsable metric
# output, malformed JSON, or a load run without successful responses aborts the
# harness instead of silently degrading into an empty report.
#
# Artifacts (all under benchmarks/preloop-perf/results/):
#   harness-report.html  machine-generated dashboard for this run
#   metrics.json         every METRIC line, parsed
#   environment.json     host, toolchain, and tool versions for this run
#   oha-submit.json      raw oha output for POST /api/v1/runs
#   oha-poll.json        raw oha output for GET /api/v1/runs
#
# `report.html` and `implementation-report.html` in that directory are the
# curated editorial write-up and are deliberately NOT written by this script.
#
# Primary metric:
#   server_rps       median peak requests/sec under concurrent load (higher is
#                    better). `server_rps_min` / `_max` / `_spread_pct` describe
#                    run-to-run stability; see `preloop-loadtest` for the trial
#                    methodology.
#
# Usage:
#   ./autoresearch.sh              # full run, PRELOOP_BENCH_TRIALS trials
#   ./autoresearch.sh --quick      # single trial, short oha windows
#   ./autoresearch.sh --profile    # additionally profile with samply
#
set -euo pipefail
cd "$(dirname "$0")"

REPO="$(pwd)"
RESULTS_DIR="${REPO}/benchmarks/preloop-perf/results"
LOADTEST_BIN="${REPO}/target/release/preloop-loadtest"
SERVER_BIN="${REPO}/target/release/preloop-server"
HARNESS_REPORT="${RESULTS_DIR}/harness-report.html"
METRICS_FILE="${RESULTS_DIR}/metrics.json"
ENVIRONMENT_FILE="${RESULTS_DIR}/environment.json"
FLAMEGRAPH_SVG="${RESULTS_DIR}/flamegraph.svg"
PROFILE_JSON="${RESULTS_DIR}/profile.json"
OHA_SUBMIT_JSON="${RESULTS_DIR}/oha-submit.json"
OHA_POLL_JSON="${RESULTS_DIR}/oha-poll.json"
AGENT_CI_DATA="${REPO}/goals/preloop-agent-ci-five-repo-benchmark/results/clean-rerun-results.json"

SYSTEM_TOKEN="aksh-system-token"
OHA_PORT="${OHA_PORT:-19999}"
OHA_CONNECTIONS=32
OHA_POLL_CONNECTIONS=16
OHA_SUBMIT_WINDOW=10s
OHA_POLL_WINDOW=5s
# Trials are executed inside preloop-loadtest; the harness only chooses how many.
BENCH_TRIALS="${PRELOOP_BENCH_TRIALS:-3}"
PROFILE=0

log() { echo "[harness] $*" >&2; }
die() { echo "[harness] ERROR: $*" >&2; exit 1; }

# Descriptive-only lookups (CPU model, tool banners). A missing tool is recorded
# as "unavailable" instead of aborting. Never used on a measurement path.
soft() { "$@" 2>/dev/null || echo unavailable; }

for arg in "$@"; do
    case "$arg" in
        --profile) PROFILE=1 ;;
        --quick)
            BENCH_TRIALS=1
            OHA_SUBMIT_WINDOW=3s
            OHA_POLL_WINDOW=2s
            ;;
        -h|--help)
            sed -n '2,32p' "$0" >&2
            exit 0
            ;;
        *) die "unknown argument: $arg (expected --quick, --profile, or --help)" ;;
    esac
done

case "$BENCH_TRIALS" in
    ''|*[!0-9]*) die "PRELOOP_BENCH_TRIALS must be a positive integer, got '${BENCH_TRIALS}'" ;;
    0) die "PRELOOP_BENCH_TRIALS must be >= 1" ;;
esac

mkdir -p "$RESULTS_DIR"

# ── cleanup ──────────────────────────────────────────────────────────────────
# One scratch directory and one background PID, both released on every exit
# path including failures and Ctrl-C.

SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/preloop-perf.XXXXXX")"
SERVER_PID=""

cleanup() {
    local status=$?
    if [ -n "$SERVER_PID" ]; then
        # Idempotent teardown: the server may already be gone.
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$SCRATCH"
    return $status
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ── preflight ────────────────────────────────────────────────────────────────

for tool in cargo curl python3; do
    command -v "$tool" >/dev/null 2>&1 || die "required tool not on PATH: $tool"
done
command -v oha >/dev/null 2>&1 || die "oha is required for the real-TCP load phase; install with: cargo install oha"

# The installed oha decides how JSON is requested: 1.x uses
# `--output-format json`, older builds used a bare `-j`. Probe --help instead of
# guessing, because an unknown flag makes oha exit 2 with no JSON at all.
OHA_HELP="$(oha --help 2>&1)" || die "oha --help failed; oha install is broken"
if printf '%s\n' "$OHA_HELP" | grep -q -- '--output-format'; then
    OHA_JSON_FLAG="--output-format json"
elif printf '%s\n' "$OHA_HELP" | grep -qE '(^|[[:space:]])-j([[:space:],]|$)'; then
    OHA_JSON_FLAG="-j"
else
    die "installed oha ($(soft oha --version)) exposes no JSON output flag; --help lists neither --output-format nor -j"
fi
read -r -a OHA_JSON_ARGS <<<"$OHA_JSON_FLAG"
log "oha JSON flag: ${OHA_JSON_FLAG}"

# ── build ────────────────────────────────────────────────────────────────────

log "Building preloop-loadtest and preloop-server (release)..."
SECONDS=0
cargo build --release -p preloop-loadtest -p aksh-runner-server >&2
build_secs=$SECONDS
log "Build completed in ${build_secs}s"

[ -x "$LOADTEST_BIN" ] || die "loadtest binary not found at $LOADTEST_BIN"
[ -x "$SERVER_BIN" ] || die "server binary not found at $SERVER_BIN"

# ── environment + tool recording ─────────────────────────────────────────────
# Recorded before measuring so a report always states which host, toolchain,
# and tool versions produced its numbers.

log "Recording environment..."
ENV_GIT_COMMIT="$(soft git rev-parse --short HEAD)"
ENV_GIT_DIRTY_FILES="$(git status --porcelain | wc -l | tr -d ' ')"
python3 - "$ENVIRONMENT_FILE" <<PYEOF
import json, sys
environment = {
    "generated_utc": __import__("datetime").datetime.now(
        __import__("datetime").timezone.utc
    ).isoformat(timespec="seconds"),
    "host": {
        "os": "$(soft uname -s)",
        "kernel": "$(soft uname -r)",
        "arch": "$(soft uname -m)",
        "hostname": "$(soft hostname)",
        "cpu": "$(soft sysctl -n machdep.cpu.brand_string)",
        "logical_cpus": "$(getconf _NPROCESSORS_ONLN)",
    },
    "tools": {
        "rustc": "$(soft rustc --version)",
        "cargo": "$(soft cargo --version)",
        "git": "$(soft git --version)",
        "oha": "$(soft oha --version)",
        "python3": "$(soft python3 --version)",
        "samply": "$(soft samply --version)",
    },
    "repo": {
        "commit": "${ENV_GIT_COMMIT}",
        "uncommitted_files": ${ENV_GIT_DIRTY_FILES},
    },
    "settings": {
        "trials": ${BENCH_TRIALS},
        "oha_json_flag": "${OHA_JSON_FLAG}",
        "oha_submit_window": "${OHA_SUBMIT_WINDOW}",
        "oha_submit_connections": ${OHA_CONNECTIONS},
        "oha_poll_window": "${OHA_POLL_WINDOW}",
        "oha_poll_connections": ${OHA_POLL_CONNECTIONS},
        "profile_requested": ${PROFILE},
    },
}
with open(sys.argv[1], "w") as handle:
    json.dump(environment, handle, indent=2, sort_keys=True)
PYEOF
log "Environment: ${ENVIRONMENT_FILE}"

# ── run loadtest ─────────────────────────────────────────────────────────────

METRICS_RAW="${SCRATCH}/metrics.txt"
LOADTEST_RAW="${SCRATCH}/loadtest.log"

log "Running comprehensive loadtest (${BENCH_TRIALS} trial(s))..."
SECONDS=0
if ! PRELOOP_BENCH_TRIALS="$BENCH_TRIALS" "$LOADTEST_BIN" all >"$LOADTEST_RAW" 2>&1; then
    cat "$LOADTEST_RAW" >&2
    die "preloop-loadtest failed; see output above"
fi
loadtest_secs=$SECONDS
cat "$LOADTEST_RAW" >&2
log "Loadtest completed in ${loadtest_secs}s"

grep '^METRIC ' "$LOADTEST_RAW" >"$METRICS_RAW" || die "preloop-loadtest emitted no METRIC lines"
{
    echo "METRIC build_secs=${build_secs}"
    echo "METRIC loadtest_secs=${loadtest_secs}"
} >>"$METRICS_RAW"

# ── profiling (optional) ────────────────────────────────────────────────────
# --profile is an explicit request, so a missing or failing profiler is an
# error rather than a silently skipped step.

if [ "$PROFILE" -eq 1 ]; then
    command -v samply >/dev/null 2>&1 || die "--profile requires samply on PATH (cargo install samply)"
    log "Profiling server-load benchmark with samply..."
    PRELOOP_BENCH_TRIALS=1 samply record --save-only -o "$PROFILE_JSON" -- \
        "$LOADTEST_BIN" server-load >&2 \
        || die "samply record failed (dtrace may need elevated privileges: sudo ./autoresearch.sh --profile)"
    log "Profile saved to ${PROFILE_JSON}"
fi

# ── HTTP load test with oha (real network path) ─────────────────────────────

log "Starting server on 127.0.0.1:${OHA_PORT} for the real-TCP load phase..."
OHA_STATE_DIR="${SCRATCH}/oha-state"
SERVER_LOG="${SCRATCH}/server.log"
mkdir -p "$OHA_STATE_DIR"

AKSH_SYSTEM_TOKEN="$SYSTEM_TOKEN" "$SERVER_BIN" serve \
    --listen "127.0.0.1:${OHA_PORT}" \
    --state-dir "$OHA_STATE_DIR" \
    >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

server_ready=0
for _ in $(seq 1 100); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        break
    fi
    if curl -sf -o /dev/null "http://127.0.0.1:${OHA_PORT}/healthz"; then
        server_ready=1
        break
    fi
    sleep 0.2
done

if [ "$server_ready" -ne 1 ]; then
    cat "$SERVER_LOG" >&2
    die "server never became ready on 127.0.0.1:${OHA_PORT}; see log above"
fi
log "Server ready, running oha..."

SUBMIT_BODY='{"workflow_yaml":"on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n","event":"push","repository":"bench/oha"}'

oha -z "$OHA_SUBMIT_WINDOW" -c "$OHA_CONNECTIONS" --no-tui "${OHA_JSON_ARGS[@]}" \
    -m POST \
    -H "Authorization: Bearer ${SYSTEM_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$SUBMIT_BODY" \
    "http://127.0.0.1:${OHA_PORT}/api/v1/runs" >"$OHA_SUBMIT_JSON" \
    || die "oha submission run failed (exit $?); see ${OHA_SUBMIT_JSON}"

oha -z "$OHA_POLL_WINDOW" -c "$OHA_POLL_CONNECTIONS" --no-tui "${OHA_JSON_ARGS[@]}" \
    -H "Authorization: Bearer ${SYSTEM_TOKEN}" \
    "http://127.0.0.1:${OHA_PORT}/api/v1/runs?limit=20" >"$OHA_POLL_JSON" \
    || die "oha polling run failed (exit $?); see ${OHA_POLL_JSON}"

log "oha complete; validating JSON and response codes..."

# Validate structure and success counts, then emit metrics. Anything short of a
# parsable payload with at least one 2xx and no >=4xx response aborts the run.
oha_metrics() {
    python3 - "$1" "$2" <<'PYEOF'
import json
import sys

path, prefix = sys.argv[1], sys.argv[2]

with open(path) as handle:
    raw = handle.read()
if not raw.strip():
    sys.exit(f"{path}: oha produced no output")
try:
    data = json.loads(raw)
except json.JSONDecodeError as err:
    sys.exit(f"{path}: oha output is not valid JSON: {err}")

summary = data.get("summary")
if not isinstance(summary, dict):
    sys.exit(f"{path}: oha JSON has no 'summary' object")
for key in ("requestsPerSec", "average", "total", "successRate"):
    if not isinstance(summary.get(key), (int, float)):
        sys.exit(f"{path}: oha summary is missing numeric '{key}'")

codes = data.get("statusCodeDistribution")
if not isinstance(codes, dict) or not codes:
    sys.exit(f"{path}: oha JSON has no 'statusCodeDistribution'")
try:
    counted = {int(code): int(count) for code, count in codes.items()}
except (TypeError, ValueError) as err:
    sys.exit(f"{path}: unparsable statusCodeDistribution {codes}: {err}")

errors = data.get("errorDistribution") or {}
succeeded = sum(n for code, n in counted.items() if 200 <= code < 300)
rejected = sum(n for code, n in counted.items() if code >= 400)

if succeeded == 0:
    sys.exit(
        f"{path}: oha recorded 0 successful (2xx) responses; "
        f"codes={counted} errors={errors}"
    )
if rejected:
    sys.exit(f"{path}: oha recorded {rejected} responses >= 400; codes={counted}")

percentiles = data.get("latencyPercentiles") or {}


def ms(value):
    return float(value) * 1000.0


print(f"METRIC {prefix}_rps={summary['requestsPerSec']:.0f}")
print(f"METRIC {prefix}_mean_ms={ms(summary['average']):.2f}")
print(f"METRIC {prefix}_success_responses={succeeded}")
print(f"METRIC {prefix}_success_rate={float(summary['successRate']):.4f}")
# `summary.total` is the wall-clock duration of the load window in seconds, not
# a request count.
print(f"METRIC {prefix}_window_s={float(summary['total']):.2f}")
print(f"METRIC {prefix}_deadline_aborted={int(errors.get('aborted due to deadline', 0))}")
for label in ("p50", "p90", "p95", "p99"):
    if label in percentiles:
        print(f"METRIC {prefix}_{label}_ms={ms(percentiles[label]):.2f}")
PYEOF
}

oha_metrics "$OHA_SUBMIT_JSON" http >>"$METRICS_RAW" \
    || die "oha submission results failed validation"
oha_metrics "$OHA_POLL_JSON" http_poll >>"$METRICS_RAW" \
    || die "oha polling results failed validation"

kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""

# ── emit metrics ─────────────────────────────────────────────────────────────

cat "$METRICS_RAW"

python3 - "$METRICS_RAW" "$METRICS_FILE" <<'PYEOF'
import json
import re
import sys

raw_path, out_path = sys.argv[1], sys.argv[2]
pattern = re.compile(r"^METRIC\s+(\S+)=(\S+)$")
metrics = {}

with open(raw_path) as handle:
    for line in handle:
        line = line.strip()
        if not line:
            continue
        match = pattern.match(line)
        if match is None:
            sys.exit(f"unparsable METRIC line: {line!r}")
        key, value = match.groups()
        try:
            # Integers stay exact: the permutation seed and response counts do
            # not survive a float round trip.
            metrics[key] = int(value)
        except ValueError:
            try:
                metrics[key] = float(value)
            except ValueError:
                metrics[key] = value

if not metrics:
    sys.exit("no metrics were parsed")

with open(out_path, "w") as handle:
    json.dump(metrics, handle, indent=2, sort_keys=True)
print(f"[harness] Metrics saved to {out_path}", file=sys.stderr)
PYEOF

# ── generate HTML report ─────────────────────────────────────────────────────

log "Generating HTML dashboard..."
python3 - "$METRICS_FILE" "$ENVIRONMENT_FILE" "$OHA_SUBMIT_JSON" "$OHA_POLL_JSON" \
    "$AGENT_CI_DATA" "$FLAMEGRAPH_SVG" "$HARNESS_REPORT" <<'PYEOF'
import json
import os
import sys
from datetime import datetime
from html import escape

(
    metrics_path,
    environment_path,
    oha_submit_path,
    oha_poll_path,
    agent_ci_path,
    flamegraph_path,
    report_path,
) = sys.argv[1:8]


def load(path, required=True):
    if not os.path.exists(path):
        if required:
            sys.exit(f"missing required input: {path}")
        return None
    with open(path) as handle:
        return json.load(handle)


metrics = load(metrics_path)
environment = load(environment_path)
oha_submit = load(oha_submit_path)
oha_poll = load(oha_poll_path)
agent_ci = load(agent_ci_path, required=False) or {}
flamegraph_rel = (
    os.path.basename(flamegraph_path) if os.path.exists(flamegraph_path) else None
)

# Warm editorial palette, shared with the curated report.html write-up so the
# machine-generated dashboard reads as part of the same document family.
CSS = """
:root {
  --paper: oklch(96% 0.018 82);
  --paper-deep: oklch(91% 0.026 78);
  --ink: oklch(25% 0.025 55);
  --muted: oklch(49% 0.035 65);
  --accent: oklch(53% 0.16 35);
  --accent-deep: oklch(38% 0.11 35);
  --olive: oklch(48% 0.075 105);
  --signal: oklch(62% 0.13 75);
  --surface: oklch(98% 0.012 84);
  --border: oklch(79% 0.035 73);
  --rule: oklch(69% 0.05 68);
  --mono: 'JetBrains Mono', 'SF Mono', 'Cascadia Code', monospace;
  --serif: 'Alegreya', Georgia, serif;
  --sans: 'Source Sans 3', 'Helvetica Neue', sans-serif;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: var(--sans);
  background: var(--paper); color: var(--ink); line-height: 1.68;
  padding: clamp(1.5rem, 4vw, 4.5rem) clamp(1rem, 5vw, 5rem);
  max-width: 1380px; margin: 0 auto;
}
h1 { font-family: var(--serif); font-size: clamp(2.4rem, 6vw, 4.4rem); font-weight: 600;
     letter-spacing: -0.04em; line-height: 0.98; max-width: 16ch; margin-bottom: 0.6rem; }
h2 { color: var(--accent-deep); font-family: var(--serif); font-size: clamp(1.6rem, 3vw, 2.3rem);
     font-weight: 600; letter-spacing: -0.02em; margin: 3.5rem 0 1rem;
     border-bottom: 1px solid var(--rule); padding-bottom: 0.6rem; }
h3 { font-family: var(--serif); font-size: 1.3rem; font-weight: 600; margin: 2rem 0 0.5rem; }
.subtitle { color: var(--muted); font-size: 1rem; max-width: 78ch; margin-bottom: 0.6rem; }
.subtitle strong { color: var(--ink); font-weight: 600; }
a { color: var(--accent-deep); text-underline-offset: 0.18em; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
        gap: 0.9rem; margin: 1.5rem 0 2rem; }
.card { background: var(--surface); border: 1px solid var(--border);
        border-top: 2px solid var(--accent); border-radius: 2px; padding: 1.3rem 1.4rem 1.1rem; }
.card h4 { margin: 0 0 0.4rem; font-size: 0.72rem; color: var(--muted);
           text-transform: uppercase; letter-spacing: 0.11em; }
.card .value { font-size: 1.8rem; font-weight: 600; color: var(--accent-deep);
               font-family: var(--mono); }
.card .unit { font-size: 0.78rem; color: var(--muted); }
.card .delta { font-size: 0.82rem; margin-top: 0.25rem; color: var(--signal); }
table { width: 100%; border-collapse: collapse; margin: 1.2rem 0; background: var(--surface);
        border: 1px solid var(--border); border-radius: 2px; overflow: hidden; }
th, td { padding: 0.65rem 0.85rem; text-align: left; border-bottom: 1px solid var(--border);
         font-size: 0.92rem; }
th { background: var(--paper-deep); color: var(--accent-deep); font-weight: 600;
     font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.09em; }
td { font-family: var(--mono); }
tr:hover td { background: oklch(94% 0.022 78); }
.pass { color: var(--olive); font-weight: 600; }
.fail { color: var(--accent); font-weight: 600; }
.methodology { background: var(--surface); border: 1px solid var(--border); border-radius: 2px;
               padding: 1.4rem 1.5rem; font-size: 0.94rem; margin: 1.1rem 0; }
.methodology code, .subtitle code, td code, p code {
  background: var(--paper-deep); padding: 0.15em 0.4em; border-radius: 2px;
  font-family: var(--mono); font-size: 0.85em; }
.methodology ul { padding-left: 1.4rem; }
.note { border-left: 3px solid var(--signal); background: var(--surface);
        padding: 0.8rem 1.1rem; margin: 1.1rem 0; font-size: 0.92rem; }
footer { color: var(--muted); font-size: 0.8rem; text-align: center; margin-top: 3.5rem; }
"""


def value(key):
    return metrics.get(key)


def fmt(key, digits=0, missing="&mdash;"):
    raw = value(key)
    if raw is None:
        return missing
    if isinstance(raw, str):
        return escape(raw)
    return f"{raw:,.{digits}f}"


def card(title, key, unit, digits=0):
    spread = value(f"{key}_spread_pct")
    samples = value(f"{key}_samples")
    detail = ""
    if spread is not None and samples is not None:
        detail = (
            f'<div class="delta">spread {spread:.1f}% over '
            f'{int(float(samples))} sample(s)</div>'
        )
    return (
        f'<div class="card"><h4>{escape(title)}</h4>'
        f'<div class="value">{fmt(key, digits)}</div>'
        f'<div class="unit">{escape(unit)}</div>{detail}</div>'
    )


def stat_row(label, key, digits=0, unit=""):
    if value(key) is None:
        return ""
    suffix = f" {escape(unit)}" if unit else ""
    return (
        f"<tr><td>{escape(label)}</td>"
        f"<td>{fmt(key, digits)}{suffix}</td>"
        f"<td>{fmt(f'{key}_min', digits)}</td>"
        f"<td>{fmt(f'{key}_max', digits)}</td>"
        f"<td>{fmt(f'{key}_spread_pct', 1)}%</td>"
        f"<td>{fmt(f'{key}_samples', 0)}</td></tr>"
    )


STAT_HEAD = (
    "<tr><th>Measurement</th><th>Median</th><th>Min</th><th>Max</th>"
    "<th>Spread</th><th>Samples</th></tr>"
)

host = environment["host"]
tools = environment["tools"]
repo = environment["repo"]
settings = environment["settings"]

parts = []
parts.append("<h2>Run Provenance</h2>")
parts.append(
    '<p class="subtitle">Recorded before measuring, so every number below is '
    "attributable to a specific host, toolchain, and tool set.</p>"
)
provenance_rows = [
    (
        "Host",
        f"{escape(host['cpu'])} &middot; {escape(host['logical_cpus'])} logical CPUs",
    ),
    (
        "OS",
        f"{escape(host['os'])} {escape(host['kernel'])} ({escape(host['arch'])})",
    ),
    (
        "Repository",
        f"commit {escape(repo['commit'])}, {repo['uncommitted_files']} uncommitted file(s)",
    ),
    ("Toolchain", f"{escape(tools['rustc'])} &middot; {escape(tools['cargo'])}"),
    (
        "Load generator",
        f"{escape(tools['oha'])} via <code>{escape(settings['oha_json_flag'])}</code>",
    ),
    ("Trials per measurement", str(settings["trials"])),
    (
        "oha windows",
        f"POST {settings['oha_submit_window']} @ c={settings['oha_submit_connections']}, "
        f"GET {settings['oha_poll_window']} @ c={settings['oha_poll_connections']}",
    ),
    ("Benchmark build profile", str(value("bench_profile") or "unknown")),
    ("Permutation seed", str(value("bench_seed"))),
]
parts.append("<table><tr><th>Field</th><th>Value</th></tr>")
for name, detail in provenance_rows:
    parts.append(f"<tr><td>{escape(name)}</td><td>{detail}</td></tr>")
parts.append("</table>")

parts.append("<h2>Summary</h2>")
parts.append('<div class="grid">')
parts.append(card("Server Peak RPS", "server_rps", "requests/sec (in-process)"))
parts.append(card("HTTP RPS (oha)", "http_rps", "submissions/sec over real TCP"))
parts.append(
    card("Sequential Latency", "server_sequential_latency_ms", "ms/request", digits=2)
)
parts.append(card("Cold Boot", "cold_boot_ms", "ms (AppState::new)", digits=1))
parts.append(card("Parser (simple)", "parse_simple_us", "µs/parse", digits=1))
parts.append(card("Expr Eval", "expr_eval_us", "µs/eval", digits=2))
parts.append(card("Mixed Contention", "contention_mixed_rps", "ops/sec @ 32 threads"))
parts.append("</div>")

parts.append("<h2>Server Load (in-process router)</h2>")
parts.append(
    "<p>Requests go straight to the axum router through "
    "<code>tower::ServiceExt::oneshot</code>, so these numbers isolate handler "
    "and state-management cost from kernel networking.</p>"
)
parts.append(f"<table>{STAT_HEAD}")
for concurrency in (4, 16, 64, 128):
    parts.append(
        stat_row(f"Concurrency {concurrency} — RPS", f"server_rps_c{concurrency}")
    )
    parts.append(
        stat_row(
            f"Concurrency {concurrency} — avg latency",
            f"server_avg_ms_c{concurrency}",
            digits=2,
            unit="ms",
        )
    )
parts.append(stat_row("Sequential — RPS", "server_sequential_rps"))
parts.append(stat_row("Matrix 4-shard (c=16) — RPS", "server_matrix_rps"))
parts.append(stat_row("Complex DAG (sequential) — RPS", "server_complex_dag_rps"))
parts.append(stat_row("GET /runs polling — RPS", "server_poll_rps"))
parts.append("</table>")

error_cells = []
for concurrency in (4, 16, 64, 128):
    errors = value(f"server_errors_c{concurrency}")
    if errors is not None:
        state = "pass" if float(errors) == 0 else "fail"
        error_cells.append(
            f"<tr><td>Concurrency {concurrency}</td>"
            f'<td class="{state}">{int(float(errors))}</td></tr>'
        )
if error_cells:
    parts.append("<h3>Rejected submissions</h3>")
    parts.append(
        "<table><tr><th>Sweep level</th><th>Non-2xx responses</th></tr>"
        + "".join(error_cells)
        + "</table>"
    )

parts.append("<h2>Real HTTP Path (oha)</h2>")
submit_summary = oha_submit["summary"]
poll_summary = oha_poll["summary"]
parts.append(
    "<table><tr><th>Metric</th><th>POST /api/v1/runs</th><th>GET /api/v1/runs</th></tr>"
)


def http_row(label, submit_key, poll_key, digits=2):
    return (
        f"<tr><td>{escape(label)}</td><td>{fmt(submit_key, digits)}</td>"
        f"<td>{fmt(poll_key, digits)}</td></tr>"
    )


parts.append(http_row("Requests/sec", "http_rps", "http_poll_rps", digits=0))
parts.append(
    http_row(
        "Successful (2xx) responses",
        "http_success_responses",
        "http_poll_success_responses",
        digits=0,
    )
)
parts.append(http_row("Success rate", "http_success_rate", "http_poll_success_rate", 4))
parts.append(http_row("Mean latency (ms)", "http_mean_ms", "http_poll_mean_ms"))
parts.append(http_row("P50 latency (ms)", "http_p50_ms", "http_poll_p50_ms"))
parts.append(http_row("P95 latency (ms)", "http_p95_ms", "http_poll_p95_ms"))
parts.append(http_row("P99 latency (ms)", "http_p99_ms", "http_poll_p99_ms"))
parts.append(http_row("Load window (s)", "http_window_s", "http_poll_window_s"))
parts.append(
    http_row(
        "Aborted at deadline",
        "http_deadline_aborted",
        "http_poll_deadline_aborted",
        digits=0,
    )
)
parts.append("</table>")
parts.append(
    '<div class="note"><strong>Reading these figures:</strong> '
    "<code>summary.total</code> in oha's JSON is the duration of the load window "
    "in seconds, not a request count, so the request count above is the sum of "
    "oha's 2xx status-code distribution. Requests still in flight when the "
    "<code>-z</code> deadline fires are reported separately as "
    '"aborted at deadline" and are not counted as failures.</div>'
)
parts.append(
    f"<p>Status codes — POST: <code>{escape(json.dumps(oha_submit.get('statusCodeDistribution', {})))}</code>"
    f", GET: <code>{escape(json.dumps(oha_poll.get('statusCodeDistribution', {})))}</code>. "
    f"Raw payloads: <code>{escape(os.path.basename(oha_submit_path))}</code>, "
    f"<code>{escape(os.path.basename(oha_poll_path))}</code>.</p>"
)

parts.append("<h2>Parser, Expressions, Protocol</h2>")
parts.append(f"<table>{STAT_HEAD}")
parts.append(stat_row("Parse simple", "parse_simple_us", 1, "µs"))
parts.append(stat_row("Parse matrix + expand", "parse_matrix_us", 1, "µs"))
parts.append(stat_row("Parse complex DAG + expand", "parse_complex_us", 1, "µs"))
parts.append(stat_row("Expression eval", "expr_eval_us", 2, "µs"))
parts.append(stat_row("Expression eval throughput", "expr_evals_per_sec", 0, "evals/s"))
parts.append(stat_row("Expression validate", "expr_validate_us", 2, "µs"))
parts.append(stat_row("Serialize expanded jobs", "serde_expanded_ser_us", 1, "µs"))
parts.append(stat_row("Deserialize expanded jobs", "serde_expanded_de_us", 1, "µs"))
parts.append(stat_row("AppState::new (cold boot)", "cold_boot_ms", 1, "ms"))
parts.append("</table>")
evaluated = value("expr_evaluated_count")
battery = value("expr_battery_size")
if evaluated is not None and battery is not None:
    parts.append(
        f"<p>Expression preflight: {int(float(evaluated))} of "
        f"{int(float(battery))} battery entries evaluate successfully; the "
        "remainder need filesystem context (<code>hashFiles</code>) and are "
        "reported rather than silently ignored.</p>"
    )

parts.append("<h2>Workspace Snapshots</h2>")
parts.append(
    "<p>Each trial starts from a brand new server state directory, so the cold "
    "figure is genuinely uncached; warm figures are the submissions that follow "
    "within the same trial.</p>"
)
snapshot_rows = []
for label in ("small", "medium", "large", "xlarge"):
    cold = value(f"snapshot_{label}_cold_ms")
    if cold is None:
        continue
    snapshot_rows.append(
        f"<tr><td>{label}</td><td>{fmt(f'snapshot_{label}_files', 0)}</td>"
        f"<td>{fmt(f'snapshot_{label}_cold_ms', 1)}</td>"
        f"<td>{fmt(f'snapshot_{label}_cold_ms_spread_pct', 1)}%</td>"
        f"<td>{fmt(f'snapshot_{label}_warm_ms', 1)}</td>"
        f"<td>{fmt(f'snapshot_{label}_warm_ms_spread_pct', 1)}%</td></tr>"
    )
parts.append(
    "<table><tr><th>Size</th><th>Files</th><th>Cold median (ms)</th>"
    "<th>Cold spread</th><th>Warm median (ms)</th><th>Warm spread</th></tr>"
    + "".join(snapshot_rows)
    + "</table>"
)

parts.append("<h2>Mixed Contention</h2>")
parts.append(
    "<p>32 workers issuing a 70/30 submission/poll mix for 5 seconds against a "
    "server created fresh for each trial.</p>"
)
parts.append(f"<table>{STAT_HEAD}")
parts.append(stat_row("Mixed ops/sec", "contention_mixed_rps"))
parts.append(stat_row("Submissions per trial", "contention_submits"))
parts.append(stat_row("Polls per trial", "contention_polls"))
parts.append("</table>")
contention_errors = value("contention_errors")
if contention_errors is not None:
    state = "pass" if float(contention_errors) == 0 else "fail"
    parts.append(
        f'<p>Rejected requests: <span class="{state}">'
        f"{int(float(contention_errors))}</span></p>"
    )

if agent_ci.get("primary"):
    parts.append("<h2>Preloop vs Agent-CI</h2>")
    parts.append(
        f"<p>Prior five-repo benchmark ({escape(str(agent_ci.get('date', 'undated run')))}). "
        "Wall-clock seconds for a full workflow execution; not produced by this "
        "harness run.</p>"
    )
    parts.append(
        "<table><tr><th>Repository</th><th>System</th><th>Cold (s)</th>"
        "<th>Warm (s)</th><th>Status</th><th>CLI RSS (MiB)</th></tr>"
    )
    for row in agent_ci["primary"]:
        state = "pass" if row.get("cold") == "pass" else "fail"
        rss = row.get("cli_max_rss_mib") or []
        parts.append(
            f"<tr><td>{escape(str(row.get('repository', '')))}</td>"
            f"<td>{escape(str(row.get('system', '')))}</td>"
            f"<td>{row.get('cold_s') if row.get('cold_s') is not None else '&mdash;'}</td>"
            f"<td>{row.get('warm_s') if row.get('warm_s') is not None else '&mdash;'}</td>"
            f'<td class="{state}">{escape(str(row.get("cold")))}/'
            f'{escape(str(row.get("warm")))}</td>'
            f"<td>{escape(' / '.join(f'{v:.1f}' for v in rss)) if rss else '&mdash;'}</td></tr>"
        )
    parts.append("</table>")

if flamegraph_rel:
    parts.append("<h2>Flamegraph</h2>")
    parts.append(
        f'<p>Standalone SVG: <a href="{escape(flamegraph_rel)}">{escape(flamegraph_rel)}</a>. '
        "Linked rather than inlined to keep this dashboard small.</p>"
    )

parts.append("<h2>Methodology</h2>")
parts.append(
    """<div class="methodology">
<h3>Repeatability</h3>
<p>Every quantity is sampled over independent trials. Each trial builds its own
<code>AppState</code> on a fresh temporary state directory, and inside the
concurrency sweep every level gets its own server, so no measurement inherits a
run list or object cache from an earlier phase. Tables report the median with
min, max, and relative spread, so run-to-run noise is visible instead of being
folded into a single headline number.</p>

<h3>Concurrency sweep order</h3>
<p>The sweep order is a seeded permutation that is mirrored on odd trials, so
each level occupies early and late sweep positions equally often across a trial
pair. The previous ascending-only sweep charged all machine warm-up to the
lowest concurrency level and all thermal drift to the highest.</p>

<h3>Failure handling</h3>
<p>The harness fails closed: a missing tool, an <code>oha</code> flag the
installed build rejects, a server that never answers <code>/healthz</code>,
unparsable <code>METRIC</code> output, malformed JSON, zero successful responses,
or any response at or above HTTP 400 aborts the run. No phase degrades into an
empty section.</p>

<h3>Surfaces</h3>
<ul>
<li><strong>Server load:</strong> sequential baseline, concurrency sweep at
4/16/64/128 with 50 requests per worker, matrix and complex-DAG workflows, and a
polling phase seeded to a fixed run-list length.</li>
<li><strong>Real HTTP:</strong> <code>oha</code> against a live server on a
loopback port with an isolated state directory and its own system token.</li>
<li><strong>Snapshots:</strong> deterministic Git repositories from 100 to 10,000
files; cold measured on fresh state, warm on the submissions that follow.</li>
<li><strong>Parser / expressions / protocol:</strong> in-process loops with
<code>std::hint::black_box</code>; the expression battery is preflighted so a
uniformly failing evaluator cannot masquerade as a fast one.</li>
<li><strong>Cold boot:</strong> repeated <code>AppState::new</code> on fresh
temporary directories.</li>
</ul>
</div>"""
)

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Preloop Performance Dashboard — {datetime.now().strftime('%Y-%m-%d %H:%M')}</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Alegreya:wght@500;600;700&family=Source+Sans+3:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
{CSS}
</style>
</head>
<body>
<h1>Preloop Performance Dashboard</h1>
<p class="subtitle">Machine-generated by <code>autoresearch.sh</code> on
{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} &middot;
{escape(host['cpu'])} &middot; {escape(host['os'])} {escape(host['arch'])}</p>
<p class="subtitle">This file is overwritten on every run. The curated
write-up lives in <code>report.html</code> and
<code>implementation-report.html</code> and is not generated by this harness.</p>
{''.join(parts)}
<footer>preloop-loadtest + oha {escape(tools['oha'])} &middot; metrics in
<code>{escape(os.path.basename(metrics_path))}</code> &middot; environment in
<code>{escape(os.path.basename(environment_path))}</code></footer>
</body>
</html>
"""

with open(report_path, "w") as handle:
    handle.write(html)
print(f"[harness] Dashboard written to {report_path}", file=sys.stderr)
PYEOF

log "HTML dashboard: ${HARNESS_REPORT}"
log "Metrics: ${METRICS_FILE}"
log "Done."
