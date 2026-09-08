#!/usr/bin/env bash
# New 5-repo conformance campaign — picks 5 repos NOT in previous campaigns.
# Previous: cli, pydantic, serde, valkey, deno, grafana, express, tokio, runc,
#           flask, gin, json-c, nlohmann/json, node-fetch, bat, vite, uv,
#           nextcloud, caddy, bento, agent-ci, openclaw, buzz, qm, runner,
#           ripgrep, click, yq, axios, fd (lightweight wave 2026-08-29)
# New picks (diverse, popular, ubuntu-latest dominated, lightweight):
#   - ajeetdsouza/zoxide (Rust, 1 ubuntu)
#   - junegunn/fzf (Go, 1 ubuntu-24.04)
#   - golangci/golangci-lint (Go, 2 ubuntu)
#   - jqlang/jq (C, 17 cross but ubuntu-latest)
#   - starship/starship (Rust, 5 ubuntu)
set -Eeuo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORKSPACE_ROOT="${CONFORMANCE_WORKSPACE_ROOT:-/tmp/preloop-conformance-new5repos/workspaces}"
OUTPUT_ROOT="${CONFORMANCE_OUTPUT_ROOT:-$ROOT/benchmarks/real-world/results/conformance-new5repos}"
CAMPAIGN_HOME="${CONFORMANCE_HOME:-/tmp/preloop-new5repos-home}"
PORT="${CONFORMANCE_PORT:-9198}"
POLL_SECONDS="${CONFORMANCE_POLL_SECONDS:-10}"
TIMEOUT_SECONDS="${CONFORMANCE_TIMEOUT_SECONDS:-7200}"
POOL_SIZE="${PRELOOP_RUNNER_POOL_SIZE:-1}"
HOST_HOME="${HOME:-}"
SMOLVM_PROCESS_HOME="${CONFORMANCE_SMOLVM_HOME:-$CAMPAIGN_HOME/smolvm-home}"
if [[ -z "${PRELOOP_SYSTEM_TOKEN:-}" ]]; then
  export PRELOOP_SYSTEM_TOKEN="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
fi
export PRELOOP_CLIENT_TIMEOUT_SECONDS="${PRELOOP_CLIENT_TIMEOUT_SECONDS:-3600}"
SERVER_BIN="${PRELOOP_BIN:-$ROOT/target/debug/preloop}"
CLIENT_BIN="${PRELOOP_CLIENT_BIN:-$ROOT/target/debug/preloop-runner-client}"
GUEST_RUNNER_BUNDLE="${PRELOOP_RUNNER_BUNDLE:-$ROOT/target/aarch64-unknown-linux-gnu/debug}"
OFFICIAL_GOLDEN_BASE="ghcr.io/preloopdev/runner-images:ubuntu24-arm64-runner-large-latest@sha256:a58990d6b6f8ca5861f33d77e1d3f0732d7d14261caacd6fba8c8f707c05b40e"
OFFICIAL_GOLDEN_NAME="preloop-ghcr.io-preloopdev-runner-images-ubuntu24-arm64-runner-large-latest-sha256-a58990d6b6f8ca5861f33d77e1d3f0732d7d14261caacd6fba8c8f707c05b40e-aarch64"
OFFICIAL_GOLDEN_ARTIFACT="${PRELOOP_GOLDEN_ARTIFACT:-$HOME/.config/preloop/vms/${OFFICIAL_GOLDEN_NAME}.smolmachine}"
SERVER_PID=""
FAILED_TARGETS=""
if [ "$(uname -s)" = Darwin ]; then
  if [ -z "${DYLD_FALLBACK_LIBRARY_PATH:-}" ]; then
    export DYLD_FALLBACK_LIBRARY_PATH="$HOST_HOME/.smolvm/lib:/opt/homebrew/lib:/usr/lib"
  else
    case ":$DYLD_FALLBACK_LIBRARY_PATH:" in
      *":$HOST_HOME/.smolvm/lib:"*) ;;
      *) export DYLD_FALLBACK_LIBRARY_PATH="$HOST_HOME/.smolvm/lib:$DYLD_FALLBACK_LIBRARY_PATH" ;;
    esac
  fi
fi
usage() {
  cat <<'EOF'
Usage: conformance-new5repos.sh [all|requests|axios|typescript|react|nextjs] [--workflow NAME]
EOF
}
fail() { echo "conformance-new5repos: $*" >&2; exit 1; }
target_cfg() {
  case "$1" in
    zoxide/ci)
      echo "zoxide https://github.com/ajeetdsouza/zoxide.git main .github/workflows/ci.yml push refs/heads/main" ;;
    fzf/ci)
      echo "fzf https://github.com/junegunn/fzf.git master .github/workflows/linux.yml push refs/heads/master" ;;
    golangci-lint/ci)
      echo "golangci-lint https://github.com/golangci/golangci-lint.git main .github/workflows/pr-tests.yml push refs/heads/main" ;;
    jq/ci)
      echo "jq https://github.com/jqlang/jq.git master .github/workflows/ci.yml push refs/heads/master" ;;
    starship/ci)
      echo "starship https://github.com/starship/starship.git main .github/workflows/workflow.yml push refs/heads/main" ;;
    cobra/ci)
      echo "cobra https://github.com/spf13/cobra.git main .github/workflows/test.yml push refs/heads/main" ;;
    *) return 1 ;;
  esac
}
all_targets() {
  echo "zoxide/ci fzf/ci golangci-lint/ci jq/ci starship/ci cobra/ci"
}
repo_targets() {
  case "$1" in
    zoxide) echo "zoxide/ci" ;;
    fzf) echo "fzf/ci" ;;
    golangci-lint) echo "golangci-lint/ci" ;;
    jq) echo "jq/ci" ;;
    starship) echo "starship/ci" ;;
    cobra) echo "cobra/ci" ;;
    *) return 1 ;;
  esac
}
file_size() { stat -f %z "$1" 2>/dev/null || stat -c %s "$1"; }
prepare_golden_home() {
  # Kill any leftover server from a previous interrupted run that still holds
  # the campaign home's socket and mounts. Without this, rm -rf fails with
  # "Directory not empty" and set -e aborts the campaign before start_server.
  pkill -f "preloop.*serve.*--listen.*:$PORT" 2>/dev/null || true
  sleep 1
  # The externals directory may be a mount point; umount first so rm -rf can succeed.
  # Use sudo as the mount may be owned by root.
  sudo umount -f "$CAMPAIGN_HOME/externals/externals" 2>/dev/null || true
  sudo umount -f "$CAMPAIGN_HOME/externals" 2>/dev/null || true
  rm -rf "$CAMPAIGN_HOME" || true
  [ -f "$OFFICIAL_GOLDEN_ARTIFACT" ] || fail "missing 9GB official golden: $OFFICIAL_GOLDEN_ARTIFACT"
  local bytes expected fingerprint
  bytes="$(file_size "$OFFICIAL_GOLDEN_ARTIFACT")"
  [ "$bytes" -ge 8589934592 ] || fail "golden is ${bytes} bytes, not the required ~9GB"
  fingerprint="$(python3 - "$OFFICIAL_GOLDEN_BASE" <<'INNERPY'
import hashlib, json, sys, platform
rosetta = platform.system() == "Darwin" and platform.machine() in ("arm64", "aarch64")
normalized = {"base": sys.argv[1], "toolchains": [], "curated": False, "bake": "", "rosetta_libs": rosetta}
print(hashlib.sha256(json.dumps(normalized, separators=(",", ":")).encode()).hexdigest())
INNERPY
)"
  expected="$CAMPAIGN_HOME/vms/$OFFICIAL_GOLDEN_NAME-$fingerprint"
  mkdir -p "$(dirname "$expected")"
  ln -sfn "$OFFICIAL_GOLDEN_ARTIFACT" "$expected"
  [ -f "$expected" ] || fail "failed to expose official golden at $expected"
}
ensure_binaries() {
  if [ ! -x "$SERVER_BIN" ] || [ ! -x "$CLIENT_BIN" ] || [ ! -x "$GUEST_RUNNER_BUNDLE/preloop-runner" ]; then
    echo "=== building Preloop CLI, client, and Linux guest runner ==="
    cargo build --locked -p preloop-cli -p preloop-runner-client
    cargo zigbuild --locked -p preloop-runner --target aarch64-unknown-linux-gnu
  fi
  [ -x "$SERVER_BIN" ] || fail "missing server binary: $SERVER_BIN"
  [ -x "$CLIENT_BIN" ] || fail "missing client binary: $CLIENT_BIN"
  [ -x "$GUEST_RUNNER_BUNDLE/preloop-runner" ] || fail "missing Linux guest runner: $GUEST_RUNNER_BUNDLE/preloop-runner"
}
clone_repo() {
  local slug="$1" url="$2" branch="$3" dir="$WORKSPACE_ROOT/$1"
  mkdir -p "$WORKSPACE_ROOT"
  if [ ! -d "$dir/.git" ]; then
    echo "=== cloning $slug ($branch) ==="
    git clone "$url" "$dir"
  else
    echo "=== refreshing $slug ($branch) ==="
    git -C "$dir" remote set-url origin "$url"
    git -C "$dir" fetch --prune --tags origin
  fi
  git -C "$dir" fetch --prune --tags origin "$branch"
  git -C "$dir" checkout -B "$branch" "origin/$branch"
  git -C "$dir" reset --hard "origin/$branch"
  git -C "$dir" clean -fdx
}
start_server() {
  local log="$OUTPUT_ROOT/server.log"
  mkdir -p "$OUTPUT_ROOT" "$CAMPAIGN_HOME" "$SMOLVM_PROCESS_HOME"
  rm -f "$log"
  if [ -z "${PRELOOP_GITHUB_TOKEN:-}" ] && command -v gh >/dev/null 2>&1; then
    PRELOOP_GITHUB_TOKEN="$(gh auth token 2>/dev/null || true)"
    export PRELOOP_GITHUB_TOKEN
  fi
  PRELOOP_HOME="$CAMPAIGN_HOME" \
  HOME="$SMOLVM_PROCESS_HOME" \
  SMOLVM_DATA_DIR="${SMOLVM_DATA_DIR:-$CAMPAIGN_HOME/smolvm}" \
  SMOLVM_AGENT_ROOTFS="${SMOLVM_AGENT_ROOTFS:-$HOST_HOME/.smolvm/agent-rootfs}" \
  SMOLVM_LIB_DIR="${SMOLVM_LIB_DIR:-$HOST_HOME/.smolvm/lib}" \
  PRELOOP_RUNNER_BASE_IMAGE="$OFFICIAL_GOLDEN_BASE" \
  PRELOOP_RUNNER_BUNDLE="$GUEST_RUNNER_BUNDLE" \
  PRELOOP_RUNNER_NAME_PREFIX="conformance-new5repos" \
  PRELOOP_USE_PACKED_GOLDEN=1 \
  PRELOOP_RUNNER_POOL_ENABLED=1 \
  PRELOOP_RUNNER_POOL_SIZE="$POOL_SIZE" \
  PRELOOP_USE_FORK=1 \
  PRELOOP_RUNNER_MEMORY_MIB="${PRELOOP_RUNNER_MEMORY_MIB:-8192}" \
  PRELOOP_RUNNER_STORAGE_GB="${PRELOOP_RUNNER_STORAGE_GB:-160}" \
  PRELOOP_RUNNER_LABELS="${PRELOOP_RUNNER_LABELS:-X64,ubuntu-arm64-small,ubuntu-x64-small,ubuntu-x64,ubuntu-latest,ubuntu-22.04,ubuntu-24.04,ubuntu-20.04}" \
  PRELOOP_PUBLIC_URL="http://127.0.0.1:$PORT" \
  RUST_LOG="${RUST_LOG:-info,preloop=info}" \
  "$SERVER_BIN" serve --listen "127.0.0.1:$PORT" >"$log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 120); do
    if curl -fsS --max-time 5 "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
      echo "=== preloop server ready on :$PORT using $OFFICIAL_GOLDEN_ARTIFACT ==="
      return 0
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      tail -50 "$log" >&2 || true
      fail "preloop server exited during startup"
    fi
    sleep 1
  done
  fail "preloop server failed to become ready on :$PORT"
}
cleanup() {
  echo "=== cleaning up campaign (server pid $SERVER_PID) ===" >&2
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM
run_status() {
  local run_id="$1"
  curl -fsS --max-time 15 -H "Authorization: Bearer $PRELOOP_SYSTEM_TOKEN" \
    "http://127.0.0.1:$PORT/api/v1/runs/$run_id" 2>/dev/null | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("status","unknown"))' 2>/dev/null || echo "unknown"
}
run_snapshot() {
  local run_id="$1" output="$2"
  curl -fsS --max-time 15 -H "Authorization: Bearer $PRELOOP_SYSTEM_TOKEN" \
    "http://127.0.0.1:$PORT/api/v1/runs/$run_id" >"$output" 2>/dev/null || printf '%s\n' '{}' >"$output"
}
write_push_payload() {
  local workspace="$1" branch="$2" output="$3"
  python3 - "$workspace" "$branch" >"$output" <<'PY'
import json, subprocess, sys
workspace, branch = sys.argv[1:]
def git(*args):
    return subprocess.check_output(["git", "-C", workspace, *args], text=True).splitlines()
head = git("rev-parse", "HEAD")[0]
try:
    before = git("rev-parse", "HEAD^")[0]
except subprocess.CalledProcessError:
    before = "0" * 40
all_paths = git("ls-files")
paths = all_paths[:25] + all_paths[-25:]
print(json.dumps({
    "action": "synchronize",
    "before": before,
    "after": head,
    "ref": f"refs/heads/{branch}",
    "paths": paths,
    "commits": [{"modified": paths, "added": [], "removed": []}],
    "repository": {"default_branch": branch},
    "pull_request": {"number": 1, "base": {"ref": branch, "sha": before}, "head": {"ref": branch, "sha": head}},
}, separators=(",", ":")))
PY
}
wait_run() {
  local target="$1" run_id="$2" deadline status
  deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      echo "[$target] preloop server exited; cannot poll run $run_id" >&2
      printf '%s\n' server-crashed
      return 0
    fi
    status="$(run_status "$run_id")"
    echo "[$target] status=$status" >&2
    case "$status" in
      success|failure|cancelled|skipped) printf '%s\n' "$status"; return 0 ;;
    esac
    sleep "$POLL_SECONDS"
  done
  printf '%s\n' timeout
}
run_target() {
  local target="$1" workflow_filter="${2:-}"
  local cfg slug url branch workflow event git_ref ws_dir target_dir submit run_id final_status repo_slug
  cfg="$(target_cfg "$target")" || fail "unknown target: $target"
  read -r slug url branch workflow event git_ref <<EOF
$cfg
EOF
  if [ -n "$workflow_filter" ] && [ "$workflow" != "$workflow_filter" ] && [ "${workflow##*/}" != "$workflow_filter" ]; then
    return 0
  fi
  clone_repo "$slug" "$url" "$branch"
  ws_dir="$WORKSPACE_ROOT/$slug"
  [ -f "$ws_dir/$workflow" ] || fail "$target workflow is missing after checkout: $ws_dir/$workflow"
  repo_slug="$(echo "$url" | sed -E 's#^https://github.com/([^/]+/[^/.]+)(\.git)?$#\1#' | sed 's/\.git$//')"
  [ -n "$repo_slug" ] || repo_slug="$slug"
  target_dir="$OUTPUT_ROOT/$target"
  rm -rf "$target_dir"
  mkdir -p "$target_dir"
  write_push_payload "$ws_dir" "$branch" "$target_dir/event.json"
  echo "=== [$target] submitting $workflow ($event $git_ref) ==="
  if ! submit="$("$CLIENT_BIN" --server "http://127.0.0.1:$PORT" submit \
    -W "$ws_dir/$workflow" --workspace-root "$ws_dir" --repository "$repo_slug" \
    --git-ref "$git_ref" --event "$event" --payload "$target_dir/event.json" 2>&1)"; then
    if [ "$event" = "push" ] && [[ "$submit" == *"does not match event"* ]]; then
      event="pull_request"
      echo "[$target] retrying with pull_request trigger"
      submit="$("$CLIENT_BIN" --server "http://127.0.0.1:$PORT" submit \
        -W "$ws_dir/$workflow" --workspace-root "$ws_dir" --repository "$repo_slug" \
        --git-ref "$git_ref" --event "$event" --payload "$target_dir/event.json" 2>&1)" || {
          printf '%s\n' "$submit" | tee "$target_dir/submit.txt"
          fail "[$target] workflow submission failed"
        }
    else
      printf '%s\n' "$submit" | tee "$target_dir/submit.txt"
      fail "[$target] workflow submission failed"
    fi
  fi
  printf '%s\n' "$submit" | tee "$target_dir/submit.txt"
  run_id="$(printf '%s\n' "$submit" | python3 -c 'import json,sys; print(json.load(sys.stdin)["run_id"])')"
  printf '%s\n' "$run_id" >"$target_dir/run-id.txt"
  final_status="$(wait_run "$target" "$run_id")"
  run_snapshot "$run_id" "$target_dir/run.json" || true
  printf '%s\n' "$final_status" >"$target_dir/status.txt" || true
  echo "=== [$target] final status: $final_status ==="
  case "$final_status" in
    success|skipped) ;;
    *) FAILED_TARGETS="${FAILED_TARGETS}${target}=${final_status}\n" ;;
  esac
}
main() {
  local repo="${1:-all}" workflow_filter="" target target_list
  if [ "$repo" = "--help" ] || [ "$repo" = "-h" ]; then usage; return 0; fi
  shift || true
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --workflow) [ "$#" -ge 2 ] || fail "--workflow needs a path"; workflow_filter="$2"; shift 2 ;;
      --help|-h) usage; return 0 ;;
      *) fail "unknown argument: $1" ;;
    esac
  done
  if [ -n "${CONFORMANCE_TARGETS:-}" ]; then
    target_list="$CONFORMANCE_TARGETS"
  elif [ "$repo" = "all" ]; then
    target_list="$(all_targets)"
  else
    target_list="$(repo_targets "$repo")" || fail "unknown repository: $repo"
  fi
  prepare_golden_home
  ensure_binaries
  mkdir -p "$WORKSPACE_ROOT" "$OUTPUT_ROOT"
  start_server
  for target in $target_list; do
    run_target "$target" "$workflow_filter"
  done
  if [ -n "$FAILED_TARGETS" ]; then
    printf 'Campaign failures:\n%b' "$FAILED_TARGETS" >&2
    return 1
  fi
  echo "Campaign complete. Results: $OUTPUT_ROOT"
}
main "$@"
