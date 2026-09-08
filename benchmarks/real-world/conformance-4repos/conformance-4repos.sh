#!/usr/bin/env bash
# conformance-4repos.sh — Run the 4-repo conformance campaign.
#
# Cells:
#   B: official runner (v2.336.0) vs local preloop server
#   C: preloop runner (preloop-runner) vs local preloop server
#
# The workflows are the EXACT upstream files (runs-on labels only rewritten),
# executed against a fresh preloop server per cell. Results land in
# benchmarks/real-world/results/conformance-4repos/{repo}/{cell}/run.json
#
# Usage:
#   ./conformance-4repos.sh <repo> <cell>     # one cell
#   ./conformance-4repos.sh all all           # everything (parallel)
set -uo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$ROOT/../../.." && pwd)"
WS_ROOT="${WS_ROOT:-/tmp/conformance-workspaces}"
OUT_ROOT="$REPO_ROOT/benchmarks/real-world/results/conformance-4repos"
OFFICIAL="$HOME/.cache/actions-runner/current"
CLIENT="$REPO_ROOT/target/debug/preloop-runner-client"
SERVER_BIN="$REPO_ROOT/target/debug/preloop-server"
PRELOOP_RUNNER="$REPO_ROOT/target/debug/preloop-runner"
PORT_B="${PORT_B:-9191}"
PORT_C="${PORT_C:-9193}"
if [ -z "${PRELOOP_SYSTEM_TOKEN:-}" ]; then
  export PRELOOP_SYSTEM_TOKEN="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
fi
TOKEN="$PRELOOP_SYSTEM_TOKEN"
mkdir -p "$OUT_ROOT"

# repo -> (workspace dir, workflow path, event, git_ref)
repo_cfg() {
  case "$1" in
    bat)       echo "$WS_ROOT/bat .github/workflows/CICD.yml push refs/heads/master" ;;
    vite)      echo "$WS_ROOT/vite .github/workflows/ci.yml push refs/heads/main" ;;
    uv)        echo "$WS_ROOT/uv .github/workflows/ci.yml pull_request refs/heads/main" ;;
    nextcloud) echo "$WS_ROOT/nextcloud .github/workflows/phpunit-sqlite.yml pull_request refs/heads/master" ;;
  esac
}

# macOS has no GNU `timeout`; the tool shell's builtin does not survive into
# `bash script.sh`. Python is guaranteed present.
run_with_timeout() {
  local secs="$1"; shift
  python3 - "$secs" "$@" <<'PY'
import subprocess, sys
secs = int(sys.argv[1])
try:
    subprocess.run(sys.argv[2:])
except subprocess.TimeoutExpired:
    pass
PY
}

# checkout v6 (used by bat's CICD) stores its git credential in a separate
# config file referenced by `includeIf.gitdir:` entries. Inside the official
# runner the include is not honored (snapshot fetches 401), while the same
# config works standalone. This shim injects the credential via
# `GIT_CONFIG_COUNT` env entries for fetch calls, so the EXACT upstream
# workflow files run unmodified. It sits on PATH before the real git.
make_git_shim() {
  local dir="$1"
  mkdir -p "$dir"
  cat > "$dir/git" <<'EOF'
#!/bin/bash
# preloop campaign shim: inject checkout's snapshot credential into fetches.
if [[ "$*" == *"fetch"* ]]; then
  CREDS=$(/opt/homebrew/bin/git config --local --get-regexp '^includeIf\.' 2>/dev/null | awk '{print $2}' | grep -v /github/ | head -1)
  if [ -n "$CREDS" ] && [ -f "$CREDS" ]; then
    echo "FETCH $(pwd) CREDS=$CREDS" >> /tmp/gitwrap-trace.log
    echo "ENV: $(env | grep -c GIT_CONFIG) GIT_CONFIG_COUNT=${GIT_CONFIG_COUNT:-unset}" >> /tmp/gitwrap-trace.log
    SECTION=$(sed -n 's/^\[http "\(.*\)"\]/\1/p' "$CREDS" | head -1)
    SECTION=${SECTION%/}
    EXTRAHEADER=$(sed -n 's/^[[:space:]]*extraheader = //p' "$CREDS" | head -1)
    if [ -n "$SECTION" ] && [ -n "$EXTRAHEADER" ]; then
      COUNT=${GIT_CONFIG_COUNT:-0}
      export GIT_CONFIG_COUNT=$((COUNT + 1))
      export "GIT_CONFIG_KEY_${COUNT}=http.${SECTION}/.extraheader"
      export "GIT_CONFIG_VALUE_${COUNT}=${EXTRAHEADER}"
      echo "INJECT COUNT=${GIT_CONFIG_COUNT} KEY_${COUNT}=http.${SECTION}/.extraheader" >> /tmp/gitwrap-inject.log
      GIT_TRACE=1 GIT_CURL_VERBOSE=1 /opt/homebrew/bin/git "$@" 2>> /tmp/gitwrap-fetch-trace.log
      echo "FETCH EXIT $?" >> /tmp/gitwrap-fetch-trace.log
      exit 0
    fi
  fi
fi
exec /opt/homebrew/bin/git "$@"
EOF
  chmod +x "$dir/git"
}

start_server() {
  local port="$1" state="$2" log="$3"
  pkill -f "preloop-server serve --listen 127.0.0.1:$port" 2>/dev/null || true
  sleep 0.5
  rm -rf "$state"
  local public_port="${RUNNER_PORT:-$port}"
  PRELOOP_SYSTEM_TOKEN="$TOKEN" PRELOOP_GITHUB_TOKEN="$(gh auth token)" PRELOOP_PUBLIC_URL="http://127.0.0.1:$public_port" \
    RUST_LOG=info,preloop=info "$SERVER_BIN" serve --listen "127.0.0.1:$port" --state-dir "$state" \
    > "$log" 2>&1 &
  local pid=$!
  for _ in $(seq 1 50); do
    curl -sf "http://127.0.0.1:$port/healthz" >/dev/null 2>&1 && { echo "$pid"; return; }
    sleep 0.2
  done
  echo "server failed to start" >&2; tail -5 "$log" >&2; return 1
}

run_status() {
  local port="$1" run_id="$2"
  curl -sf -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$port/api/v1/runs/$run_id" 2>/dev/null \
    | python3 -c "
import json,sys
try:
    r=json.load(sys.stdin)
except Exception:
    print('unknown'); sys.exit(0)
print(r.get('status','unknown'))
" 2>/dev/null || echo "unknown"
}

nonterminal_jobs() {
  local port="$1" run_id="$2"
  curl -sf -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$port/api/v1/runs/$run_id" 2>/dev/null \
    | python3 -c "
import json,sys
try:
    r=json.load(sys.stdin)
    jobs=r.get('jobs',{})
    term={'success','failure','cancelled','skipped'}
    print(sum(1 for s in jobs.values() if s not in term))
except Exception:
    print(0)
" 2>/dev/null | tail -1
}

wait_terminal() {
  local port="$1" run_id="$2" timeout_s="$3"
  local deadline=$(( $(date +%s) + timeout_s ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    local status
    status=$(run_status "$port" "$run_id")
    case "$status" in
      success|failure|cancelled|skipped) echo "$status"; return 0 ;;
    esac
    sleep 10
  done
  echo "timeout"
}

run_cell() {
  local repo="$1" cell="$2"
  read -r ws_dir wf_path event git_ref <<< "$(repo_cfg "$repo")"
  local port
  if [ "$cell" = "official" ]; then port="$PORT_B"; else port="$PORT_C"; fi
  local cell_dir="$OUT_ROOT/$repo/$cell"
  rm -rf "$cell_dir"; mkdir -p "$cell_dir"
  # Stale runner work dirs from aborted runs confuse `actions/checkout`'s
  # clean/reset step (`git reset --hard HEAD` on a no-HEAD repo fails), so
  # every cell starts from a clean slate.
  rm -rf "/tmp/conformance-official-work-$repo-$cell" "/tmp/conformance-runner-$repo-$cell"

  echo "=== [$repo/$cell] starting server on :$port ==="
  local state="/tmp/conformance-state-$repo-$cell"
  local server_pid
  server_pid=$(start_server "$port" "$state" "$cell_dir/server.log") || return 1

  echo "=== [$repo/$cell] submitting $wf_path ($event $git_ref) ==="
  local submit
  submit=$("$CLIENT" --server "http://127.0.0.1:$port" submit -W "$ws_dir/$wf_path" \
    --workspace-root "$ws_dir" --git-ref "$git_ref" --event "$event" 2>&1)
  echo "$submit" | tee "$cell_dir/submit.txt"
  local run_id
  run_id=$(echo "$submit" | python3 -c 'import json,sys; print(json.load(sys.stdin)["run_id"])' 2>/dev/null) || { echo "submit failed: $submit"; kill "$server_pid" 2>/dev/null; return 1; }

  # Runner loop: run one job per invocation until the run is terminal.
  local n=0
  while true; do
    local status
    status=$(run_status "$port" "$run_id")
    case "$status" in
      success|failure|cancelled|skipped) break ;;
      timeout|unknown) echo "[$repo/$cell] run status $status, aborting"; break ;;
    esac
    local pending
    pending=$(nonterminal_jobs "$port" "$run_id" | tail -1)
    case "$pending" in
      ''|*[!0-9]*) pending=0 ;;
    esac
    if [ "$pending" -le 0 ]; then echo "[$repo/$cell] no non-terminal jobs; sleeping"; sleep 5; continue; fi
    n=$((n+1))
    local log="$cell_dir/runner-$n.log"
    echo "[$repo/$cell] runner invocation $n (jobs left: $pending)"
    if [ "$cell" = "official" ]; then
      local shim="/tmp/gitwrap-$repo-$cell"
      make_git_shim "$shim"
      run_with_timeout 1800 bash -c '
        set -o pipefail
        cd "$0"
        export PATH="$5:$PATH"
        RUNNER_PORT="${6:-$2}"
        if [ "$1" -eq 1 ]; then
          ./config.sh remove --token t >/dev/null 2>&1 || true
          USE_DEV_ACTIONS_SERVICE_URL=1 ./config.sh --url "http://127.0.0.1:$RUNNER_PORT" --token t \
            --name "cf-$3-$4" --work "/tmp/conformance-official-work-$3-$4" \
            --unattended --replace --labels self-hosted,Linux,X64
        fi
        USE_DEV_ACTIONS_SERVICE_URL=1 ./run.sh --once
      ' "$OFFICIAL" "$n" "$port" "$repo" "$cell" "$shim" "${RUNNER_PORT:-}" >> "$log" 2>&1 || true
    else
      local root="/tmp/conformance-runner-$repo-$cell"
      if [ "$n" -eq 1 ]; then
        rm -rf "$root"; mkdir -p "$root"
        "$PRELOOP_RUNNER" --runner-root "$root" configure --url "http://127.0.0.1:$port" \
          --token t --name "cf-$repo-$cell" --unattended --replace \
          --labels self-hosted,Linux,X64 >> "$log" 2>&1 || true
      fi
      RUST_LOG=info,preloop=info run_with_timeout 1800 "$PRELOOP_RUNNER" --runner-root "$root" run --once >> "$log" 2>&1 || true
    fi
  done

  local final_status
  final_status=$(run_status "$port" "$run_id")
  echo "[$repo/$cell] final status: $final_status after $n runner invocations"

  # Capture the structured run record.
  curl -sf -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$port/api/v1/runs/$run_id" \
    > "$cell_dir/run.json" 2>/dev/null || echo "{}" > "$cell_dir/run.json"
  kill "$server_pid" 2>/dev/null || true
}

REPO_ARG="${1:-all}"
CELL_ARG="${2:-all}"

if [ "$REPO_ARG" = "all" ]; then REPOS="bat vite uv nextcloud"; else REPOS="$REPO_ARG"; fi
if [ "$CELL_ARG" = "all" ]; then CELLS="official preloop"; else CELLS="$CELL_ARG"; fi

pids=()
for repo in $REPOS; do
  for cell in $CELLS; do
    run_cell "$repo" "$cell" &
    pids+=("$!")
  done
done
for p in "${pids[@]}"; do wait "$p"; done
echo ""
echo "Campaign complete. Results in $OUT_ROOT"
