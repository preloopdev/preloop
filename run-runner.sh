#!/bin/sh
cd / && /workspace-host/target/release/preloop-runner run --runner-root / >/tmp/runner.log 2>&1
