#!/bin/sh
set -eu
mkdir -p /github/workspace/.preloop-fidelity
printf 'main|env=%s|arg=%s\n' "${FROM_MANIFEST:-}" "${1:-}" >> /github/workspace/.preloop-fidelity/docker-lifecycle.log
