#!/usr/bin/env bash
# Preloop comprehensive performance benchmark harness.
#
# Exercises the full stack: server throughput under concurrent load, parser,
# expression evaluator, workspace snapshotting, protocol serialization,
# mutex contention, and cold boot time.
#
# Optionally profiles the server under load with samply and generates a
# flamegraph. Produces an HTML report with all results.
#
# Primary metric:
#   server_rps       peak requests/sec under concurrent load (higher is better)
#
# Usage:
#   ./autoresearch.sh              # full run with all benchmarks
#   ./autoresearch.sh --quick      # fast run, fewer iterations
#   ./autoresearch.sh --profile    # include samply profiling
#
set -euo pipefail
cd "$(dirname "$0")"

REPO="$(pwd)"
RESULTS_DIR="${REPO}/benchmarks/preloop-perf/results"
LOADTEST_BIN="${REPO}/target/release/preloop-loadtest"
REPORT_HTML="${RESULTS_DIR}/report.html"
FLAMEGRAPH_SVG="${RESULTS_DIR}/flamegraph.svg"
METRICS_FILE="${RESULTS_DIR}/metrics.json"
PROFILE_MODE="${1:-}"
AGENT_CI_DATA="${REPO}/goals/preloop-agent-ci-five-repo-benchmark/results/clean-rerun-results.json"

mkdir -p "$RESULTS_DIR"

log() { echo "[harness] $*" >&2; }

# ── build ────────────────────────────────────────────────────────────────────

log "Building loadtest binary (release)..."
build_start=$(python3 -c 'import time; print(time.time())')
cargo build --release -p preloop-loadtest 2>&1 | tail -3 >&2
build_end=$(python3 -c 'import time; print(time.time())')
build_secs=$(python3 -c "print(round(${build_end} - ${build_start}, 1))")
log "Build completed in ${build_secs}s"

if [ ! -f "$LOADTEST_BIN" ]; then
    echo "ERROR: loadtest binary not found at $LOADTEST_BIN" >&2
    exit 1
fi

# ── run loadtest ─────────────────────────────────────────────────────────────

log "Running comprehensive loadtest..."
loadtest_start=$(python3 -c 'import time; print(time.time())')

# Capture all output — METRIC lines go to stdout, logs to stderr
LOADTEST_RAW=$(mktemp)
"$LOADTEST_BIN" all > "$LOADTEST_RAW" 2>&1
loadtest_exit=$?

# Show full output on stderr, extract METRIC lines
cat "$LOADTEST_RAW" >&2
METRIC_OUTPUT=$(grep '^METRIC ' "$LOADTEST_RAW" || true)
rm -f "$LOADTEST_RAW"

loadtest_end=$(python3 -c 'import time; print(time.time())')
loadtest_secs=$(python3 -c "print(round(${loadtest_end} - ${loadtest_start}, 1))")
log "Loadtest completed in ${loadtest_secs}s (exit=${loadtest_exit})"

if [ $loadtest_exit -ne 0 ]; then
    echo "ERROR: loadtest binary failed with exit code $loadtest_exit" >&2
    exit 1
fi

# ── profiling (optional) ────────────────────────────────────────────────────

if [ "$PROFILE_MODE" = "--profile" ]; then
    if command -v samply &>/dev/null; then
        log "Profiling server-load benchmark with samply..."
        samply record --save-only -o "${RESULTS_DIR}/profile.json" -- \
            "$LOADTEST_BIN" server-load 2>&2 || true
        log "Profile saved to ${RESULTS_DIR}/profile.json"
    fi

    if command -v cargo-flamegraph &>/dev/null || command -v flamegraph &>/dev/null; then
        log "Generating flamegraph..."
        # Use dtrace on macOS
        cargo flamegraph --bin preloop-loadtest --root -- server-load \
            -o "$FLAMEGRAPH_SVG" 2>&2 || {
            log "Flamegraph generation failed (may need sudo for dtrace)"
            # Try generating a basic SVG placeholder
            cat > "$FLAMEGRAPH_SVG" <<'SVGEOF'
<svg xmlns="http://www.w3.org/2000/svg" width="800" height="100">
  <rect width="800" height="100" fill="#f0f0f0"/>
  <text x="400" y="55" text-anchor="middle" font-size="16" fill="#666">
    Flamegraph requires sudo/dtrace permissions. Run: sudo ./autoresearch.sh --profile
  </text>
</svg>
SVGEOF
        }
    fi
fi

# ── HTTP load test with oha (real network path) ─────────────────────────────

OHA_RESULTS=""
if command -v oha &>/dev/null; then
    log "Running HTTP load test with oha against real server..."

    # Start server in background
    OHA_PORT=19999
    OHA_STATE_DIR=$(mktemp -d)
    AKSH_SYSTEM_TOKEN="aksh-system-token" \
        "$REPO/target/release/preloop-server" serve \
        --listen "127.0.0.1:${OHA_PORT}" \
        --state-dir "$OHA_STATE_DIR" \
        2>/dev/null &
    SERVER_PID=$!

    # Wait for server to be ready
    for i in $(seq 1 30); do
        if curl -sf "http://127.0.0.1:${OHA_PORT}/healthz" >/dev/null 2>&1; then
            break
        fi
        sleep 0.2
    done

    if curl -sf "http://127.0.0.1:${OHA_PORT}/healthz" >/dev/null 2>&1; then
        log "Server ready on port $OHA_PORT, running oha..."

        # Run oha with JSON output for the submission endpoint
        OHA_RESULTS=$(oha -z 10s -c 32 --no-tui -j \
            -m POST \
            -H "Authorization: Bearer aksh-system-token" \
            -H "Content-Type: application/json" \
            -d '{"workflow_yaml":"on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n","event":"push","repository":"bench/oha"}' \
            "http://127.0.0.1:${OHA_PORT}/api/v1/runs" 2>/dev/null || echo "{}")

        # Also test GET polling
        OHA_POLL_RESULTS=$(oha -z 5s -c 16 --no-tui -j \
            -H "Authorization: Bearer aksh-system-token" \
            "http://127.0.0.1:${OHA_PORT}/api/v1/runs?limit=20" 2>/dev/null || echo "{}")

        log "oha complete"
    else
        log "Server failed to start for oha test, skipping"
    fi

    # Cleanup
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
    rm -rf "$OHA_STATE_DIR"
fi

# ── emit metrics ─────────────────────────────────────────────────────────────

# Re-emit from the loadtest binary output
echo "$METRIC_OUTPUT"

# Add build time as a metric
echo "METRIC build_secs=${build_secs}"

# Parse oha results if available
if [ -n "$OHA_RESULTS" ] && [ "$OHA_RESULTS" != "{}" ]; then
    OHA_RPS=$(echo "$OHA_RESULTS" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(round(d.get('summary', {}).get('requestsPerSec', 0), 0))
except: print(0)
" 2>/dev/null || echo 0)
    OHA_P50=$(echo "$OHA_RESULTS" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    # percentiles stored under responseTimePercentiles or similar
    percs = d.get('latencyPercentiles', {})
    p50 = percs.get('p50', 0)
    print(round(p50 * 1000, 2))
except: print(0)
" 2>/dev/null || echo 0)
    echo "METRIC http_rps=${OHA_RPS}"
    echo "METRIC http_p50_ms=${OHA_P50}"
fi

# ── generate HTML report ─────────────────────────────────────────────────────

log "Generating HTML report..."

# Collect all metrics into JSON
python3 - "$RESULTS_DIR" "$AGENT_CI_DATA" "$OHA_RESULTS" "$FLAMEGRAPH_SVG" <<'PYEOF'
import sys, json, os
from pathlib import Path
from datetime import datetime

results_dir = Path(sys.argv[1])
agent_ci_path = sys.argv[2]
oha_raw = sys.argv[3] if len(sys.argv) > 3 else ""
flamegraph_path = sys.argv[4] if len(sys.argv) > 4 else ""

# Parse metrics from stdin (re-read from the metrics emitted to stdout)
# We'll just read the METRIC lines from the loadtest output
metrics = {}

# Read metrics from the metrics file if it was already generated
# Otherwise use what's piped in
report_path = results_dir / "report.html"

# Read agent-ci data
agent_ci = {}
if os.path.exists(agent_ci_path):
    with open(agent_ci_path) as f:
        agent_ci = json.load(f)

# Parse oha results
oha_data = {}
if oha_raw and oha_raw != "{}":
    try:
        oha_data = json.loads(oha_raw)
    except:
        pass

# Read flamegraph if exists
flamegraph_svg = ""
if flamegraph_path and os.path.exists(flamegraph_path):
    with open(flamegraph_path) as f:
        flamegraph_svg = f.read()

# Generate the HTML report
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Preloop Performance Report — {datetime.now().strftime('%Y-%m-%d %H:%M')}</title>
<style>
:root {{
  --bg: #0d1117; --fg: #c9d1d9; --accent: #58a6ff; --green: #3fb950;
  --red: #f85149; --yellow: #d29922; --surface: #161b22; --border: #30363d;
  --mono: 'SF Mono', 'Cascadia Code', 'Fira Code', monospace;
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--fg); line-height: 1.6; padding: 2rem;
  max-width: 1400px; margin: 0 auto;
}}
h1 {{ color: var(--accent); font-size: 1.8rem; margin-bottom: 0.5rem; }}
h2 {{ color: var(--accent); font-size: 1.3rem; margin: 2rem 0 1rem; border-bottom: 1px solid var(--border); padding-bottom: 0.5rem; }}
h3 {{ color: var(--fg); font-size: 1.1rem; margin: 1.5rem 0 0.5rem; }}
.subtitle {{ color: #8b949e; font-size: 0.9rem; margin-bottom: 2rem; }}
.grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 1rem; }}
.card {{
  background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  padding: 1.2rem; transition: border-color 0.2s;
}}
.card:hover {{ border-color: var(--accent); }}
.card h3 {{ margin-top: 0; font-size: 0.9rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.05em; }}
.card .value {{ font-size: 2rem; font-weight: 700; color: var(--green); font-family: var(--mono); }}
.card .unit {{ font-size: 0.8rem; color: #8b949e; }}
.card.warn .value {{ color: var(--yellow); }}
.card.bad .value {{ color: var(--red); }}
table {{
  width: 100%; border-collapse: collapse; margin: 1rem 0;
  background: var(--surface); border-radius: 8px; overflow: hidden;
}}
th, td {{ padding: 0.7rem 1rem; text-align: left; border-bottom: 1px solid var(--border); }}
th {{ background: #1c2128; color: var(--accent); font-weight: 600; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.04em; }}
td {{ font-family: var(--mono); font-size: 0.9rem; }}
tr:hover td {{ background: #1c2128; }}
.pass {{ color: var(--green); }} .fail {{ color: var(--red); }}
.section {{ margin: 2rem 0; }}
.flamegraph-container {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; overflow-x: auto; }}
.flamegraph-container svg {{ width: 100%; height: auto; }}
.log-entry {{ background: var(--surface); border-left: 3px solid var(--accent); padding: 0.8rem 1rem; margin: 0.5rem 0; border-radius: 0 4px 4px 0; }}
.log-entry .timestamp {{ color: #8b949e; font-size: 0.8rem; }}
.log-entry .detail {{ font-family: var(--mono); font-size: 0.85rem; margin-top: 0.3rem; }}
.bar-chart {{ display: flex; align-items: end; gap: 4px; height: 120px; padding: 0.5rem 0; }}
.bar {{ background: var(--accent); border-radius: 3px 3px 0 0; min-width: 40px; position: relative; transition: background 0.2s; }}
.bar:hover {{ background: var(--green); }}
.bar-label {{ position: absolute; bottom: -20px; left: 50%; transform: translateX(-50%); font-size: 0.7rem; color: #8b949e; white-space: nowrap; }}
.bar-value {{ position: absolute; top: -20px; left: 50%; transform: translateX(-50%); font-size: 0.7rem; color: var(--fg); font-family: var(--mono); white-space: nowrap; }}
.methodology {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 1.5rem; font-size: 0.9rem; }}
.methodology code {{ background: #1c2128; padding: 0.2em 0.4em; border-radius: 3px; font-family: var(--mono); font-size: 0.85em; }}
</style>
</head>
<body>
<h1>Preloop Performance Report</h1>
<p class="subtitle">Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} &middot; Apple M4 Max &middot; macOS arm64</p>

<div id="metrics-summary"></div>
<div id="server-load"></div>
<div id="parser-bench"></div>
<div id="expression-bench"></div>
<div id="snapshot-bench"></div>
<div id="contention-bench"></div>
<div id="agent-ci-comparison"></div>
<div id="flamegraph-section"></div>
<div id="implementation-log"></div>
<div id="methodology"></div>

<script>
// Metrics will be injected by the shell harness
const METRICS = {{{{METRICS_PLACEHOLDER}}}};
const AGENT_CI = {json.dumps(agent_ci)};
const OHA_DATA = {json.dumps(oha_data)};

function card(title, value, unit, cls) {{
  return '<div class="card ' + (cls||'') + '"><h3>' + title + '</h3><div class="value">' + value + '</div><div class="unit">' + unit + '</div></div>';
}}

function fmt(v, d) {{ return v ? Number(v).toFixed(d||0) : '—'; }}

// Summary cards
let summary = '<h2>Summary</h2><div class="grid">';
summary += card('Server Peak RPS', fmt(METRICS.server_rps), 'requests/sec');
summary += card('Sequential Latency', fmt(METRICS.server_sequential_latency_ms, 2), 'ms/request');
summary += card('Cold Boot', fmt(METRICS.cold_boot_median_ms, 1), 'ms (AppState::new)');
summary += card('Parser (simple)', fmt(METRICS.parse_simple_us, 1), 'µs/parse');
summary += card('Expr Eval', fmt(METRICS.expr_eval_us, 2), 'µs/eval');
summary += card('Mixed Contention', fmt(METRICS.contention_mixed_rps), 'ops/sec @ 32 threads');
if (METRICS.http_rps) summary += card('HTTP RPS (oha)', fmt(METRICS.http_rps), 'req/sec (real TCP)');
summary += '</div>';
document.getElementById('metrics-summary').innerHTML = summary;

// Server load details
let sl = '<h2>Server Load Test</h2>';
sl += '<p>In-process axum router (tower::ServiceExt::oneshot) — zero network overhead, pure handler throughput.</p>';
sl += '<table><tr><th>Concurrency</th><th>RPS</th><th>Avg Latency</th><th>Errors</th></tr>';
for (let c of [4, 16, 64, 128]) {{
  let rps = METRICS['server_rps_c' + c];
  let lat = METRICS['server_avg_ms_c' + c];
  let err = METRICS['server_errors_c' + c];
  if (rps !== undefined) {{
    sl += '<tr><td>' + c + '</td><td>' + fmt(rps) + '</td><td>' + fmt(lat, 2) + ' ms</td><td>' + fmt(err) + '</td></tr>';
  }}
}}
sl += '</table>';
sl += '<table><tr><th>Workload</th><th>RPS</th></tr>';
sl += '<tr><td>Simple (sequential)</td><td>' + fmt(METRICS.server_sequential_rps) + '</td></tr>';
sl += '<tr><td>Matrix 4-shard (c=16)</td><td>' + fmt(METRICS.server_matrix_rps) + '</td></tr>';
sl += '<tr><td>Complex DAG (sequential)</td><td>' + fmt(METRICS.server_complex_dag_rps) + '</td></tr>';
sl += '<tr><td>GET /runs polling</td><td>' + fmt(METRICS.server_poll_rps) + '</td></tr>';
sl += '</table>';
if (OHA_DATA.summary) {{
  sl += '<h3>Real HTTP (oha, 10s, 32 connections)</h3>';
  sl += '<table><tr><th>Metric</th><th>Value</th></tr>';
  let s = OHA_DATA.summary || {{}};
  sl += '<tr><td>Requests/sec</td><td>' + fmt(s.requestsPerSec, 1) + '</td></tr>';
  sl += '<tr><td>Total requests</td><td>' + fmt(s.total) + '</td></tr>';
  sl += '<tr><td>Slowest</td><td>' + fmt((s.slowest||0)*1000, 2) + ' ms</td></tr>';
  sl += '<tr><td>Fastest</td><td>' + fmt((s.fastest||0)*1000, 2) + ' ms</td></tr>';
  sl += '<tr><td>Average</td><td>' + fmt((s.average||0)*1000, 2) + ' ms</td></tr>';
  sl += '</table>';
}}
document.getElementById('server-load').innerHTML = sl;

// Parser benchmark
let pb = '<h2>Parser Benchmark</h2>';
pb += '<p>1000 iterations each: parse YAML → typed model → matrix expansion → DAG.</p>';
pb += '<table><tr><th>Workflow</th><th>Time (µs/iter)</th></tr>';
pb += '<tr><td>Simple (1 job, 1 step)</td><td>' + fmt(METRICS.parse_simple_us, 1) + '</td></tr>';
pb += '<tr><td>Matrix (4 shards)</td><td>' + fmt(METRICS.parse_matrix_us, 1) + '</td></tr>';
pb += '<tr><td>Complex DAG (4 jobs, needs chain)</td><td>' + fmt(METRICS.parse_complex_us, 1) + '</td></tr>';
pb += '</table>';
document.getElementById('parser-bench').innerHTML = pb;

// Expression benchmark
let eb = '<h2>Expression Evaluator</h2>';
eb += '<p>20 expression battery × 5000 iterations = 100K evaluations.</p>';
eb += '<table><tr><th>Metric</th><th>Value</th></tr>';
eb += '<tr><td>Eval time</td><td>' + fmt(METRICS.expr_eval_us, 2) + ' µs/eval</td></tr>';
eb += '<tr><td>Throughput</td><td>' + fmt(METRICS.expr_evals_per_sec) + ' evals/sec</td></tr>';
eb += '<tr><td>Validate (parse-only)</td><td>' + fmt(METRICS.expr_validate_us, 2) + ' µs/expr</td></tr>';
eb += '</table>';
document.getElementById('expression-bench').innerHTML = eb;

// Snapshot benchmark
let sb = '<h2>Workspace Snapshot Benchmark</h2>';
sb += '<p>git-based workspace snapshot creation at varying project sizes. Cold = no cache, Warm = with object cache.</p>';
sb += '<table><tr><th>Size</th><th>Files</th><th>Cold (ms)</th><th>Warm (ms)</th></tr>';
for (let [label, count] of [['small', 100], ['medium', 1000], ['large', 5000], ['xlarge', 10000]]) {{
  let cold = METRICS['snapshot_' + label + '_cold_ms'];
  let warm = METRICS['snapshot_' + label + '_warm_ms'];
  if (cold !== undefined) {{
    sb += '<tr><td>' + label + '</td><td>' + count + '</td><td>' + fmt(cold, 1) + '</td><td>' + fmt(warm, 1) + '</td></tr>';
  }}
}}
sb += '</table>';
document.getElementById('snapshot-bench').innerHTML = sb;

// Contention benchmark
let cb = '<h2>Mutex Contention (Mixed Workload)</h2>';
cb += '<p>32 concurrent workers, 70% submissions / 30% polls, 5 seconds sustained.</p>';
cb += '<table><tr><th>Metric</th><th>Value</th></tr>';
cb += '<tr><td>Mixed ops/sec</td><td>' + fmt(METRICS.contention_mixed_rps) + '</td></tr>';
cb += '<tr><td>Total submissions</td><td>' + fmt(METRICS.contention_submits) + '</td></tr>';
cb += '<tr><td>Total polls</td><td>' + fmt(METRICS.contention_polls) + '</td></tr>';
cb += '<tr><td>Errors</td><td>' + fmt(METRICS.contention_errors) + '</td></tr>';
cb += '</table>';
document.getElementById('contention-bench').innerHTML = cb;

// Agent-CI comparison
if (AGENT_CI.primary) {{
  let ac = '<h2>Preloop vs Agent-CI Comparison</h2>';
  ac += '<p>Five-repo benchmark from {agent_ci.get("date", "prior run")}. Wall-clock seconds for full workflow execution.</p>';
  ac += '<table><tr><th>Repository</th><th>System</th><th>Cold (s)</th><th>Warm (s)</th><th>Status</th><th>CLI RSS (MiB)</th></tr>';
  for (let row of AGENT_CI.primary) {{
    let cls = row.cold === 'pass' ? 'pass' : row.cold === 'fail' ? 'fail' : '';
    ac += '<tr><td>' + row.repository + '</td><td>' + row.system + '</td>';
    ac += '<td>' + (row.cold_s !== null ? fmt(row.cold_s, 2) : '—') + '</td>';
    ac += '<td>' + (row.warm_s !== null ? fmt(row.warm_s, 2) : '—') + '</td>';
    ac += '<td class="' + cls + '">' + row.cold + '/' + row.warm + '</td>';
    ac += '<td>' + (row.cli_max_rss_mib ? row.cli_max_rss_mib.map(v => fmt(v, 1)).join(' / ') : '—') + '</td></tr>';
  }}
  ac += '</table>';
  ac += '<h3>Key Observations</h3><ul>';
  ac += '<li>Preloop CLI RSS: ~20 MiB (thin client). Agent-CI: 280-380 MiB (bundled runtime).</li>';
  ac += '<li>Preloop warm wins: ripgrep (11s vs 14.5s), testcontainers-go (7.4s vs 10.5s).</li>';
  ac += '<li>Agent-CI wins on setup-heavy: Vite (38.7s vs 92.5s), Flask (9.9s vs 131s).</li>';
  ac += '<li>Cold boot penalty: Preloop pays VM boot + apt install on first run.</li>';
  ac += '</ul>';
  document.getElementById('agent-ci-comparison').innerHTML = ac;
}}

// Flamegraph
if ({json.dumps(bool(flamegraph_svg))}) {{
  document.getElementById('flamegraph-section').innerHTML =
    '<h2>Flamegraph</h2><div class="flamegraph-container">{flamegraph_svg.replace(chr(10), "").replace("'", "\\'")[:100000] if flamegraph_svg else ""}</div>';
}}

// Implementation log
let il = '<h2>Implementation Log</h2>';
il += '<div class="log-entry"><div class="timestamp">Phase 1: Baseline Measurement</div>';
il += '<div class="detail">Established baseline metrics across all performance surfaces: server throughput, parser speed, expression evaluation, snapshot creation, mutex contention, and cold boot time.</div></div>';
il += '<div class="log-entry"><div class="timestamp">Benchmark Surfaces</div>';
il += '<div class="detail">';
il += '<strong>Server Load:</strong> Tested at concurrency 4/16/64/128 with simple, matrix, and complex DAG workflows. ';
il += 'Mixed workload (70% writes, 30% reads) sustained for 5 seconds at 32 threads.<br>';
il += '<strong>Snapshotting:</strong> Benchmarked git-based workspace snapshots from 100 to 10,000 files. ';
il += 'Measures both cold (no object cache) and warm (cached) paths.<br>';
il += '<strong>Parser:</strong> Workflow YAML parsing + matrix expansion + DAG construction.<br>';
il += '<strong>Expressions:</strong> 20-expression battery evaluated 5000× each.<br>';
il += '<strong>Protocol:</strong> Serialization/deserialization of expanded job plans.<br>';
il += '<strong>Cold Boot:</strong> AppState::new() which generates RSA keypairs and OIDC material.';
il += '</div></div>';
il += '<div class="log-entry"><div class="timestamp">Architecture Notes</div>';
il += '<div class="detail">';
il += 'The server uses <code>Arc&lt;Mutex&lt;InnerState&gt;&gt;</code> for all mutable state. ';
il += 'Under high concurrency, this single mutex is the primary contention point. ';
il += 'Every workflow submission acquires the lock to: parse YAML, expand matrix, build job messages, queue jobs, emit events. ';
il += 'GET /runs also acquires the lock to read run state.<br><br>';
il += 'Potential optimization paths:<br>';
il += '• Split InnerState into read-heavy (runs, jobs) and write-heavy (queue, sessions) behind separate locks<br>';
il += '• Use RwLock for read-dominated paths (run listing, status polling)<br>';
il += '• Move YAML parsing and matrix expansion outside the lock<br>';
il += '• Pre-compute workflow expansions on submission, cache by content hash<br>';
il += '• Snapshot: parallel git operations, object cache warming strategies';
il += '</div></div>';

document.getElementById('implementation-log').innerHTML = il;

// Methodology
document.getElementById('methodology').innerHTML = `
<h2>Methodology</h2>
<div class="methodology">
<h3>Server Load Test</h3>
<p>Uses <code>tower::ServiceExt::oneshot</code> to send requests directly to the axum router,
bypassing TCP/TLS. This measures pure handler + state management throughput without
network overhead. Concurrency is achieved via <code>tokio::spawn</code>.</p>
<p>HTTP test via <code>oha</code> (if available) provides real-network-path numbers for comparison.</p>

<h3>Snapshot Benchmark</h3>
<p>Creates deterministic Git repositories with 100–10,000 files, then measures
<code>create_workspace_snapshot()</code> through the <code>POST /api/v1/runs</code> endpoint.
Cold = fresh state directory, Warm = reusing the object cache from a prior snapshot.</p>

<h3>Parser Benchmark</h3>
<p>Direct calls to <code>parse_workflow()</code> and <code>expand_jobs()</code> with
<code>std::hint::black_box</code> to prevent dead-code elimination. 1000 iterations each.</p>

<h3>Expression Evaluator</h3>
<p>20 representative expressions covering comparisons, string functions, format(),
JSON operations, status checks, and boolean logic. Each evaluated 5000 times against
a realistic context (github, matrix, needs, steps, runner).</p>

<h3>Mutex Contention</h3>
<p>32 concurrent workers sending a 70/30 mix of POST submissions and GET polls for
5 seconds. This simulates a busy server handling multiple CI pipelines simultaneously
and reveals <code>Arc&lt;Mutex&lt;InnerState&gt;&gt;</code> contention.</p>

<h3>Agent-CI Comparison</h3>
<p>Prior benchmark data from five real-world repositories (ripgrep, flask, vite, chi,
testcontainers-go). Preloop runs workflows in SmolVM guests; Agent-CI runs natively.
Both measure wall-clock time for the full workflow lifecycle.</p>
</div>`;

</script>
</body>
</html>"""

with open(report_path, 'w') as f:
    f.write(html)

print(f"Report written to {report_path}", file=sys.stderr)
PYEOF

# ── inject metrics into HTML report ──────────────────────────────────────────

# Parse METRIC lines into JSON and inject into the HTML
python3 - "$METRIC_OUTPUT" "$REPORT_HTML" <<'INJECT_PYEOF'
import sys, json, re

metric_text = sys.argv[1]
report_path = sys.argv[2]

metrics = {}
for line in metric_text.strip().split('\n'):
    m = re.match(r'^METRIC\s+(\S+)=(\S+)', line)
    if m:
        key, val = m.group(1), m.group(2)
        try:
            metrics[key] = float(val)
        except ValueError:
            metrics[key] = val

# Save metrics JSON
metrics_path = report_path.replace('report.html', 'metrics.json')
with open(metrics_path, 'w') as f:
    json.dump(metrics, f, indent=2)

# Inject into HTML
with open(report_path) as f:
    html = f.read()

html = html.replace('{{METRICS_PLACEHOLDER}}', json.dumps(metrics))

with open(report_path, 'w') as f:
    f.write(html)

print(f"Metrics injected into {report_path}", file=sys.stderr)
print(f"Metrics saved to {metrics_path}", file=sys.stderr)
INJECT_PYEOF

log "HTML report: ${REPORT_HTML}"
log "Done."
