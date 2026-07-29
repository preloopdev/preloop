#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
GH_TOKEN_FILE="${GH_TOKEN_FILE:-$MITM_DIR/.cache/gh-token}"
GITHUB_OWNER="${GITHUB_OWNER:-preloopdev}"
GITHUB_REPO="${GITHUB_REPO:-aksh-conformance-sample}"
GITHUB_REF="${GITHUB_REF:-main}"

[[ $(uname -s) == Linux ]] || { echo "record-golden-linux.sh must run on Linux" >&2; exit 1; }
[[ -s "$GH_TOKEN_FILE" ]] || { echo "missing GitHub token file: $GH_TOKEN_FILE" >&2; exit 1; }
command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
command -v gh >/dev/null || { echo "gh is required" >&2; exit 1; }
command -v mitmdump >/dev/null || { echo "mitmdump is required" >&2; exit 1; }
docker info >/dev/null || { echo "Docker daemon is not ready" >&2; exit 1; }

export GH_TOKEN="$(cat "$GH_TOKEN_FILE")"
export GITHUB_OWNER GITHUB_REPO GITHUB_REF
export RUNNER_ALLOW_RUNASROOT=1
export RUNNER_LABELS="self-hosted,mitm,linux,x64"
export MITM_HOST="${MITM_HOST:-172.17.0.1}"
export MITM_LISTEN_HOST="${MITM_LISTEN_HOST:-0.0.0.0}"
export MITM_PORT="${MITM_PORT:-28080}"

cancel_stale_runs() {
  gh run list -R "$GITHUB_OWNER/$GITHUB_REPO" --limit 100 \
    --json databaseId,status \
    --jq '.[] | select(.status == "queued" or .status == "in_progress") | .databaseId' |
    while read -r run_id; do
      [[ -z "$run_id" ]] ||
        gh run cancel "$run_id" -R "$GITHUB_OWNER/$GITHUB_REPO" >/dev/null 2>&1 ||
        true
    done
  sleep 5
}

if [[ $# -eq 0 ]]; then
  mapfile -t scenarios < <(
    for manifest in "$MITM_DIR"/scenarios/*/scenario.toml; do
      basename "$(dirname "$manifest")"
    done | sort
  )
else
  scenarios=("$@")
fi

for scenario in "${scenarios[@]}"; do
  echo "=== Linux golden: $scenario ==="
  cancel_stale_runs
  export GITHUB_RUNNER_TOKEN="$(
    gh api --method POST \
      "repos/$GITHUB_OWNER/$GITHUB_REPO/actions/runners/registration-token" \
      --jq .token
  )"
  "$SCRIPT_DIR/record-golden.sh" --scenario "$scenario" --non-interactive
done
