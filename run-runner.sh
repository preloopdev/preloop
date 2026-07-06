#!/bin/sh
cd / && /workspace-host/target/release/aksh-runner run --runner-root / >/tmp/runner.log 2>&1
