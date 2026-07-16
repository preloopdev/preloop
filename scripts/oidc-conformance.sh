#!/usr/bin/env bash
# The official runner is required to exercise the authenticated broker flow.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
exec "$REPO/scripts/oidc-conformance-run.sh" "$@"
