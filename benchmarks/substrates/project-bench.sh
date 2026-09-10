#!/usr/bin/env bash
# project-bench.sh — run representative open-source CI workloads through
# preloop on a forked golden for AgentENV or SmolVM.
#
# Usage: project-bench.sh <aenv|smolvm> <reps> <outdir>
set -euo pipefail

SUBSTRATE="${1:?substrate}"
REPS="${2:-3}"
OUTDIR="${3:-/tmp/project-bench-$SUBSTRATE}"
REPO=~/preloop-bench
BIN="$REPO/target/release/preloop"
WORK=~/project-bench-workspace
BUNDLE="$HOME/bench-bundle"
PORT=9490
HOSTIP=$(ip -4 -o addr show scope global | awk '{print $4}' | cut -d/ -f1 | head -1)

export PRELOOP_SYSTEM_TOKEN=project-bench-token
export PRELOOP_RUNNER_NAME_PREFIX="project-bench-$SUBSTRATE"
export PRELOOP_URL="http://127.0.0.1:$PORT"
export PRELOOP_HOME="$HOME/.preloop-project-bench-$SUBSTRATE"
export PRELOOP_RUNNER_BUNDLE="$BUNDLE"
export PRELOOP_RUNNER_URL="http://$HOSTIP:$PORT"
export PRELOOP_RUNNER_CPUS=4
export PRELOOP_RUNNER_MEMORY_MB=4096
export PRELOOP_RUNNER_STORAGE_GB=30
export PRELOOP_RUNNER_POOL_SIZE=1
export PRELOOP_USE_FORK=1
export PRELOOP_LOG_FORMAT=json
export RUST_LOG=info
export PRELOOP_AENV_TTL_SECS=7200
export TMPDIR="$PRELOOP_HOME/tmp"

mkdir -p "$OUTDIR"
: > "$OUTDIR/results.jsonl"
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$OUTDIR/driver.log" >&2; }

case "$SUBSTRATE" in
  aenv) export PRELOOP_VM_BACKEND=agentenv ;;
  smolvm) export PRELOOP_VM_BACKEND=smolvm PRELOOP_USE_PACKED_GOLDEN=1 ;;
  *) echo "unknown substrate: $SUBSTRATE" >&2; exit 2 ;;
esac

# Purge only this benchmark's own state: a blanket sweep would delete the
# machines and sandboxes of any other engine on the host. SmolVM machines are
# name-filtered; AgentENV assigns sandbox ids server-side and accepts no
# caller-supplied name, so this benchmark's own sandboxes are read from its
# registry — $PRELOOP_HOME is benchmark-scoped — before the directory is
# wiped. A stale id (sandbox already gone) is ignored.
purge() {
  log "purging benchmark state"
  local registry="$PRELOOP_HOME/agentenv-machines.json"
  if [ -f "$registry" ]; then
    for id in $(jq -r '.machines[]?.sandbox // empty' "$registry" 2>/dev/null); do
      [ -n "$id" ] && aenv delete "$id" >/dev/null 2>&1 || true
    done
  fi
  for m in $(smolvm machine ls --json 2>/dev/null | jq -r '.[].name' | awk '/^project-bench-/{print}' || true); do
    smolvm machine stop --name "$m" >/dev/null 2>&1 || true
    smolvm machine delete --name "$m" -f >/dev/null 2>&1 || true
  done
  rm -rf "$PRELOOP_HOME" "$WORK"
  mkdir -p "$PRELOOP_HOME" "$TMPDIR"
}

start_server() {
  for i in $(seq 1 30); do
    if ! ss -ltn "sport = :$PORT" 2>/dev/null | awk '$1 == "LISTEN" { found=1 } END { exit !found }'; then
      break
    fi
    log "port $PORT still busy; waiting"
    sleep 2
  done
  if ss -ltn "sport = :$PORT" 2>/dev/null | awk '$1 == "LISTEN" { found=1 } END { exit !found }'; then
    log "port $PORT is still in use"
    return 1
  fi
  log "starting preloop serve ($SUBSTRATE)"
  (cd "$REPO" && setsid nohup "$BIN" serve --listen "0.0.0.0:$PORT" > "$OUTDIR/server.log" 2>&1 < /dev/null & echo $! > "$OUTDIR/server.pid")
  for i in $(seq 1 180); do
    if curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
      log "server healthy after ${i}s"
      return 0
    fi
    sleep 1
  done
  log "server never became healthy"
  return 1
}

pool_ready() {
  local deadline=$((SECONDS + 3600))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if python3 - "$OUTDIR/server.log" <<'PY'
import sys
try:
    text = open(sys.argv[1], encoding="utf-8", errors="replace").read()
except OSError:
    raise SystemExit(1)
raise SystemExit(0 if '"message":"on-demand runner pool' in text or '"message":"runner pool ready' in text else 1)
PY
    then
      return 0
    fi
    sleep 2
  done
  return 1
}

stop_server() {
  local pid
  pid=$(cat "$OUTDIR/server.pid" 2>/dev/null || true)
  if [ -n "$pid" ]; then
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 30); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    kill -9 "$pid" 2>/dev/null || true
  fi
}

prepare_project() {
  local name="$1" repo="$2" branch="$3"
  local archive_url="https://github.com/$repo/archive/refs/heads/$branch.tar.gz"
  local tmp top
  log "fetching $name source archive"
  rm -rf "$WORK"
  tmp=$(mktemp -d)
  curl --fail --silent --show-error --location --max-time 300 "$archive_url" -o "$tmp/source.tar.gz"
  log "$name archive downloaded"
  top=$(tar -tzf "$tmp/source.tar.gz" | awk -F/ 'NF { print $1; exit }' || true)
  tar -xzf "$tmp/source.tar.gz" -C "$tmp"
  log "$name archive extracted"
  mv "$tmp/$top" "$WORK"
  rm -rf "$tmp"
  mkdir -p "$WORK/.github"
  if [ -d "$WORK/.github/workflows" ]; then
    mv "$WORK/.github/workflows" "$WORK/.github/upstream-workflows"
  fi
  mkdir -p "$WORK/.github/workflows"
  write_workflow "$name" > "$WORK/.github/workflows/project-bench.yml"
  git -C "$WORK" init -q
  git -C "$WORK" remote add origin "https://github.com/$repo.git"
  git -C "$WORK" config user.email project-bench@local
  git -C "$WORK" config user.name project-bench
  git -C "$WORK" add -A
  git -C "$WORK" commit -qm "add preloop benchmark workflow"
  log "$name workspace committed"
}

write_workflow() {
  case "$1" in
    eslint)
      cat <<'YAML'
name: project-bench-eslint
on: [push, workflow_dispatch]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '24'
      - run: npm install
      - run: npm install
        working-directory: docs
      - run: node Makefile lint
      - run: node Makefile mocha
      - run: npm run lint:rule-types
YAML
      ;;
    typescript)
      cat <<'YAML'
name: project-bench-typescript
on: [push, workflow_dispatch]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: npm ci
      - run: npx hereby build
      - run: npx hereby build:api
      - run: npx hereby test:tsc
YAML
      ;;
    pydantic-core)
      cat <<'YAML'
name: project-bench-pydantic-core
on: [push, workflow_dispatch]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: install uv and dependencies
        run: |
          python3 -m pip install --user uv
          export PATH="$HOME/.local/bin:$PATH"
          uv sync --group testing
      - name: build extension and test Python
        run: |
          export PATH="$HOME/.local/bin:$PATH"
          uv pip install -e .
          uv run pytest
      - run: cargo test
YAML
      ;;
    cargo-mutants)
      cat <<'YAML'
name: project-bench-cargo-mutants
on: [push, workflow_dispatch]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: cargo check --all-targets --all-features
      - run: cargo build --release
      - run: cargo test --workspace
      - run: cargo package --no-verify
YAML
      ;;
    gobgp)
      cat <<'YAML'
name: project-bench-gobgp
on: [push, workflow_dispatch]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: go build ./cmd/gobgp
      - run: go build ./cmd/gobgpd
      - run: go test -race -timeout 240s ./...
YAML
      ;;
    *) echo "unknown project: $1" >&2; return 2 ;;
  esac
}

run_one() {
  local project="$1" rep="$2" start end rc
  start=$(date +%s.%N)
  set +e
  (cd "$WORK" && timeout 3600 "$BIN" run -f .github/workflows/project-bench.yml --no-debug > "$OUTDIR/$project.$rep.out" 2>&1)
  rc=$?
  set -e
  end=$(date +%s.%N)
  python3 - "$SUBSTRATE" "$project" "$rep" "$start" "$end" "$rc" "$OUTDIR/results.jsonl" <<'PY'
import json, sys
substrate, project, rep, start, end, rc, output = sys.argv[1:]
row = {
    "substrate": substrate,
    "project": project,
    "rep": int(rep),
    "seconds": float(end) - float(start),
    "exit": int(rc),
    "conclusion": "success" if rc == "0" else "failure",
}
with open(output, "a", encoding="utf-8") as f:
    f.write(json.dumps(row, separators=(",", ":")) + "\n")
PY
  log "$project rep $rep: ${rc} ($(python3 - "$start" "$end" <<'PY'
import sys
print(f'{float(sys.argv[2])-float(sys.argv[1]):.1f}s')
PY
))"
}
PROJECTS=(
  "eslint|eslint/eslint|main"
  "typescript|microsoft/TypeScript|main"
  "pydantic-core|pydantic/pydantic-core|main"
  "cargo-mutants|sourcefrog/cargo-mutants|main"
  "gobgp|osrg/gobgp|master"
)

purge
# Start with an empty workspace; the engine only needs it after the first run.
mkdir -p "$WORK"
start_server
if ! pool_ready; then
  log "pool never became ready"
  stop_server
  exit 1
fi
log "golden ready; all jobs will use fork mode"

for spec in "${PROJECTS[@]}"; do
  name="${spec%%|*}"
  rest="${spec#*|}"
  repo="${rest%%|*}"
  branch="${rest#*|}"
  prepare_project "$name" "$repo" "$branch"
  for rep in $(seq 1 "$REPS"); do
    run_one "$name" "$rep"
  done
done

stop_server
log "complete: $OUTDIR/results.jsonl"
