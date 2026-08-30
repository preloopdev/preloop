#!/usr/bin/env bash
# Run the five-repository conformance campaign against the pinned official
# GitHub-hosted runner golden.
#
# The golden is the 9GB packed artifact produced from the official
# ubuntu24-arm64 runner image.  It is selected by its immutable base-image
# reference and exposed to Preloop through a temporary PRELOOP_HOME symlink;
# no second 9GB copy is made.
#
# Usage:
#   benchmarks/real-world/conformance-5repos.sh [all|grafana|deno|pydantic|valkey|cli]
#   benchmarks/real-world/conformance-5repos.sh cli --workflow test.yml
#
# The default run is sequential.  A single server and a small fork pool keep
# the 9GB golden usable on a laptop while preserving real matrix/job behavior.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORKSPACE_ROOT="${CONFORMANCE_WORKSPACE_ROOT:-/tmp/preloop-conformance-5repos/workspaces}"
OUTPUT_ROOT="${CONFORMANCE_OUTPUT_ROOT:-$ROOT/benchmarks/real-world/results/conformance-5repos}"
# The control socket is Unix-domain based; keep this path short enough for
# macOS SUN_LEN even when the checkout lives in a deep worktree.
CAMPAIGN_HOME="${CONFORMANCE_HOME:-/tmp/preloop-5repos-home}"
PORT="${CONFORMANCE_PORT:-9197}"
POLL_SECONDS="${CONFORMANCE_POLL_SECONDS:-10}"
TIMEOUT_SECONDS="${CONFORMANCE_TIMEOUT_SECONDS:-7200}"
POOL_SIZE="${PRELOOP_RUNNER_POOL_SIZE:-1}"
HOST_HOME="${HOME:-}"
SMOLVM_PROCESS_HOME="${CONFORMANCE_SMOLVM_HOME:-$CAMPAIGN_HOME/smolvm-home}"
export PRELOOP_SYSTEM_TOKEN="${PRELOOP_SYSTEM_TOKEN:-preloop-system-token}"
export PRELOOP_CLIENT_TIMEOUT_SECONDS="${PRELOOP_CLIENT_TIMEOUT_SECONDS:-3600}"

SERVER_BIN="${PRELOOP_BIN:-$ROOT/target/debug/preloop}"
CLIENT_BIN="${PRELOOP_CLIENT_BIN:-$ROOT/target/debug/preloop-runner-client}"
GUEST_RUNNER_BUNDLE="${PRELOOP_RUNNER_BUNDLE:-$ROOT/target/aarch64-unknown-linux-gnu/debug}"

# This is the official ubuntu24 arm64 hosted-image snapshot whose local packed
# artifact is approximately 9GB.  Keep the digest and artifact name together:
# changing either silently selects a different image and invalidates results.
OFFICIAL_GOLDEN_BASE="ghcr.io/preloopdev/runner-images:ubuntu24-arm64-runner-large-latest@sha256:a58990d6b6f8ca5861f33d77e1d3f0732d7d14261caacd6fba8c8f707c05b40e"
OFFICIAL_GOLDEN_NAME="preloop-ghcr.io-preloopdev-runner-images-ubuntu24-arm64-runner-large-latest-sha256-a58990d6b6f8ca5861f33d77e1d3f0732d7d14261caacd6fba8c8f707c05b40e-aarch64"
OFFICIAL_GOLDEN_ARTIFACT="${PRELOOP_GOLDEN_ARTIFACT:-$HOME/.config/preloop/vms/${OFFICIAL_GOLDEN_NAME}.smolmachine}"

SERVER_PID=""
FAILED_TARGETS=""

# The macOS SmolVM release keeps libkrunfw beside the wrapper binary but does
# not encode that directory in the child bootstrap's loader path.  Without
# this export, every VM fails after creation with "Couldn't find or load
# libkrunfw.5.dylib".  Keep an operator-provided loader path intact.
if [ "$(uname -s)" = Darwin ]; then
  SMOLVM_LIB_DIR="${SMOLVM_LIB_DIR:-$HOME/.smolvm/lib}"
  if [ -d "$SMOLVM_LIB_DIR" ]; then
    export DYLD_LIBRARY_PATH="$SMOLVM_LIB_DIR${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
  fi
fi

usage() {
  cat <<'EOF'
Usage: conformance-5repos.sh [all|grafana|deno|pydantic|valkey|cli] [--workflow NAME]

Environment:
  PRELOOP_GOLDEN_ARTIFACT       9GB official .smolmachine artifact
  CONFORMANCE_WORKSPACE_ROOT    clone root (default /tmp/preloop-conformance-5repos/workspaces)
  CONFORMANCE_OUTPUT_ROOT       result root under benchmarks/real-world/results
  CONFORMANCE_TIMEOUT_SECONDS   per-workflow timeout (default 7200)
  PRELOOP_RUNNER_POOL_SIZE      concurrent fork slots (default 2)
  CONFORMANCE_TARGETS           space-separated target IDs (overrides positional repo)
EOF
}

fail() {
  echo "conformance-5repos: $*" >&2
  exit 1
}

# target_cfg returns: repo-slug git-url branch workflow event git-ref.
target_cfg() {
  case "$1" in
    grafana/ci)
      # Grafana replaced ci.yml with the split backend-unit-tests workflow.
      echo "grafana https://github.com/grafana/grafana.git main .github/workflows/backend-unit-tests.yml push refs/heads/main" ;;
    grafana/frontend-metrics)
      # frontend-metrics.yml was renamed to frontend-lint.yml upstream.
      echo "grafana https://github.com/grafana/grafana.git main .github/workflows/frontend-lint.yml push refs/heads/main" ;;
    deno/ci)
      # ci.generated.yml is 36 jobs -> 134 after matrix expansion (each job is
      # well under GitHub's 256-per-job cap; the earlier "408 sections" count
      # was a misread). It is the only deno workflow that triggers on push;
      # the compat-test and pr workflows are schedule/pull_request-only.
      echo "deno https://github.com/denoland/deno.git main .github/workflows/ci.generated.yml push refs/heads/main" ;;
    pydantic/ci)
      echo "pydantic https://github.com/pydantic/pydantic.git main .github/workflows/ci.yml push refs/heads/main" ;;
    pydantic/test)
      # pydantic has no test.yml; its test suite lives in ci.yml (the pydantic/ci
      # target). Use the other large matrix workflow for this target.
      echo "pydantic https://github.com/pydantic/pydantic.git main .github/workflows/third-party.yml push refs/heads/main" ;;
    valkey/ci)
      echo "valkey https://github.com/valkey-io/valkey.git unstable .github/workflows/ci.yml push refs/heads/unstable" ;;
    cli/test)
      # cli has no test.yml; its test suite is go.yml (tests + lint).
      echo "cli https://github.com/cli/cli.git trunk .github/workflows/go.yml push refs/heads/trunk" ;;
    cli/lint)
      echo "cli https://github.com/cli/cli.git trunk .github/workflows/lint.yml push refs/heads/trunk" ;;
    typescript/ci)
      echo "typescript https://github.com/microsoft/TypeScript.git main .github/workflows/ci.yml push refs/heads/main" ;;
    nodejs/test)
      echo "nodejs https://github.com/nodejs/node.git main .github/workflows/test-linux.yml push refs/heads/main" ;;
    react/ci)
      echo "react https://github.com/facebook/react.git main .github/workflows/runtime_build_and_test.yml push refs/heads/main" ;;
    vscode/test)
      # pr.yml is pull_request-only; the script's push->pull_request retry
      # handles it (payload now carries an action).
      echo "vscode https://github.com/microsoft/vscode.git main .github/workflows/pr.yml push refs/heads/main" ;;
    spark/ci)
      # build_and_test.yml is workflow_call-only; build_main.yml is the push
      # entry point that calls it.
      echo "spark https://github.com/apache/spark.git master .github/workflows/build_main.yml push refs/heads/master" ;;
    *) return 1 ;;
  esac
}

all_targets() {
  echo "grafana/ci grafana/frontend-metrics deno/ci pydantic/ci pydantic/test valkey/ci cli/test cli/lint typescript/ci nodejs/test react/ci vscode/test spark/ci"
}

repo_targets() {
  case "$1" in
    grafana) echo "grafana/ci grafana/frontend-metrics" ;;
    deno) echo "deno/ci" ;;
    pydantic) echo "pydantic/ci pydantic/test" ;;
    valkey) echo "valkey/ci" ;;
    cli) echo "cli/test cli/lint" ;;
    typescript) echo "typescript/ci" ;;
    nodejs) echo "nodejs/test" ;;
    react) echo "react/ci" ;;
    vscode) echo "vscode/test" ;;
    spark) echo "spark/ci" ;;
    *) return 1 ;;
  esac
}

file_size() {
  stat -f %z "$1" 2>/dev/null || stat -c %s "$1"
}

prepare_golden_home() {
  # Dedicated temp home: a crashed run leaves VM disks (tens of GiB) that
  # crowd out the next golden unpack. The golden artifact itself lives
  # outside this home (~/.config/preloop/vms), so a clean slate is safe.
  rm -rf "$CAMPAIGN_HOME"
  [ -f "$OFFICIAL_GOLDEN_ARTIFACT" ] || fail "missing 9GB official golden: $OFFICIAL_GOLDEN_ARTIFACT"
  local bytes expected
  bytes="$(file_size "$OFFICIAL_GOLDEN_ARTIFACT")"
  # Reject the small launcher stub or a partial download.  The packed payload
  # used by this campaign is the ~9GB .smolmachine sidecar.
  [ "$bytes" -ge 8589934592 ] || fail "golden is ${bytes} bytes, not the required ~9GB .smolmachine payload"
  # The server probes the packed artifact at
  # <vms>/<stem>-<environment-fingerprint>: artifact_payload() appends the
  # EnvironmentSpec fingerprint (sha256 of the normalized base/toolchains/
  # curated/bake JSON) so bake-content changes invalidate stale packs. A
  # stem-only symlink silently falls through to a base-image pull (tens of
  # GiB) — replicate the fingerprint computation here (the base is custom,
  # so nothing is curated and there are no toolchains).
  fingerprint="$(python3 - "$OFFICIAL_GOLDEN_BASE" <<'INNERPY'
import hashlib, json, sys
import platform
rosetta = platform.system() == "Darwin" and platform.machine() in ("arm64", "aarch64")
normalized = {
    "base": sys.argv[1],
    "toolchains": [],
    "curated": False,
    "bake": "",
    "rosetta_libs": rosetta,
}
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
  # The base reference is intentionally the same string encoded in the local
  # cache filename.  The CLI then consumes the symlinked 9GB artifact instead
  # of downloading or rebuilding a different golden.
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
  PRELOOP_RUNNER_NAME_PREFIX="conformance-5repos" \
  PRELOOP_USE_PACKED_GOLDEN=1 \
  PRELOOP_RUNNER_POOL_ENABLED=1 \
  PRELOOP_RUNNER_POOL_SIZE="$POOL_SIZE" \
  PRELOOP_USE_FORK=1 \
  PRELOOP_RUNNER_MEMORY_MIB="${PRELOOP_RUNNER_MEMORY_MIB:-8192}" \
  PRELOOP_RUNNER_STORAGE_GB="${PRELOOP_RUNNER_STORAGE_GB:-160}" \
  PRELOOP_RUNNER_LABELS="${PRELOOP_RUNNER_LABELS:-X64,ubuntu-arm64-small,ubuntu-x64-small,ubuntu-x64}" \
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
  tail -100 "$log" >&2 || true
  fail "preloop server did not become ready"
}

cleanup() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT
# Signal-specific handlers: the shared cleanup would otherwise resume the
# script after the interrupt. Exit with the conventional statuses (130/143).
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

run_status() {
  # --max-time keeps a stalled status request from masking the deadline; a
  # dead server then surfaces as `unknown` and wait_run fails fast instead.
  curl -fsS --max-time 15 -H "Authorization: Bearer ${PRELOOP_SYSTEM_TOKEN:-preloop-system-token}" \
    "http://127.0.0.1:$PORT/api/v1/runs/$1" 2>/dev/null \
    | python3 -c 'import json,sys; r=json.load(sys.stdin); print(r.get("status", "unknown"))' \
    || echo unknown
}

run_snapshot() {
  local run_id="$1" output="$2"
  curl -fsS --max-time 15 -H "Authorization: Bearer ${PRELOOP_SYSTEM_TOKEN:-preloop-system-token}" \
    "http://127.0.0.1:$PORT/api/v1/runs/$run_id" >"$output" 2>/dev/null || printf '%s\n' '{}' >"$output"
}

write_push_payload() {
  local workspace="$1" branch="$2" output="$3"
  python3 - "$workspace" "$branch" >"$output" <<'PY'
import json
import subprocess
import sys

workspace, branch = sys.argv[1:]

def git(*args):
    return subprocess.check_output(
        ["git", "-C", workspace, *args], text=True
    ).splitlines()

head = git("rev-parse", "HEAD")[0]
try:
    before = git("rev-parse", "HEAD^")[0]
except subprocess.CalledProcessError:
    before = "0" * 40
# A campaign run is intentionally a synthetic "changed everything" event:
# it must exercise the selected workflow even when the current upstream commit
# touched an unrelated path.  The full `ls-files` list is deliberately NOT
# used: the payload is embedded in every job message's github context, so a
# multi-thousand-file list makes run creation take minutes per workflow
# (measured: 3000+ paths -> >120s; 2 paths -> 9s).  Path filters only need a
# non-empty set that is not fully ignored, so a spread sample of the tree is
# enough to make the workflow run.
all_paths = git("ls-files")
paths = all_paths[:25] + all_paths[-25:]

print(json.dumps({
    # pull_request activity type: the server's event matcher requires one of
    # the default PR types (opened/synchronize/reopened) when a workflow
    # declares a pull_request trigger; the push submission ignores it.
    "action": "synchronize",
    "before": before,
    "after": head,
    "ref": f"refs/heads/{branch}",
    # Preloop requires an explicit complete list for path-filter evaluation.
    "paths": paths,
    "commits": [{"modified": paths, "added": [], "removed": []}],
    "repository": {"default_branch": branch},
    "pull_request": {
        "number": 1,
        "base": {"ref": branch, "sha": before},
        "head": {"ref": branch, "sha": head},
    },
}, separators=(",", ":")))
PY
}

wait_run() {
  local target="$1" run_id="$2" deadline status
  deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    # A crashed server would otherwise be retried as `unknown` until the
    # campaign deadline; fail immediately instead.
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
  local cfg slug url branch workflow event git_ref ws_dir target_dir submit run_id final_status
  cfg="$(target_cfg "$target")" || fail "unknown target: $target"
  read -r slug url branch workflow event git_ref <<EOF
$cfg
EOF
  if [ -n "$workflow_filter" ] \
    && [ "$workflow" != "$workflow_filter" ] \
    && [ "${workflow##*/}" != "$workflow_filter" ]; then
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
    # Some upstream workflows intentionally omit push and only accept
    # pull_request.  The same complete changed-file payload is valid for the
    # fallback event, and rejected submissions do not create a run.
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
  # The snapshot curl and status write must never kill the campaign under
  # `set -e`: a transient server hiccup after the run concluded would abort
  # the whole run and lose the recorded result.
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
