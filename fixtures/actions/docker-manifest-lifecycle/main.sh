#!/bin/sh
set -eu
mkdir -p /github/workspace/.aksh-fidelity
printf 'main|env=%s|arg=%s\n' "${FROM_MANIFEST:-}" "${1:-}" >> /github/workspace/.aksh-fidelity/docker-lifecycle.log
