# Aksh Runner vs Official GitHub Actions — Real-World Campaign Report

**Date:** 2026-07-28
**aksh-runner version:** 0.2.0 (protocol-compat 2.335.1)
**Official runner version:** 2.336.0 (from public logs)

## Setup

- aksh-runner cross-compiled for Linux ARM64 (aarch64-unknown-linux-musl)
- Run directly on macOS host (Apple M4 Max)
- Registered against `preloopdev/aksh-conformance-sample`
- Labels: `self-hosted,linux,x64,macOS,ARM64`
- `--no-externals` (no bundled Node.js)

## Results Summary

| Project | Workflow | Official | aksh | aksh Conclusion | Failure Reason |
|---|---|---:|---:|---|---|
| Apache ECharts | ci.yml | ✅ success | ❌ failure | build cancelled | Node 24 missing |
| VS Code | component-fixtures.yml | ✅ success | ❌ failure | checkout failed | Node 24 missing |
| Angular | ci.yml | ✅ success | ❌ failure | checkout failed | Node 24 missing |
| n8n | docker-build-push.yml | ✅ success | ❌ failure | checkout failed | Node 24 missing |
| Apache RocketMQ | maven.yaml | ✅ success | ❌ failure | checkout failed | Node 24 missing |
| Apache Pulsar | pulsar-ci.yaml | ✅ success | ❌ failure | checkout failed | Node 24 missing |
| Cilium | tests-clustermesh-upgrade.yaml | ✅ success | ❌ failure | build cancelled | Node 24 missing |
| Apache Kafka | ci.yml | ✅ success | ❌ failure | build cancelled | Node 24 missing |

## Analysis

### What Worked

1. **Runner registration** — All 8 runners registered successfully with GitHub
2. **Job acquisition** — Runners polled and acquired jobs from the broker
3. **Step execution** — Runner executed `Set up job`, `actions/checkout@v4`, and post-steps
4. **Result reporting** — Runner reported job conclusions back to GitHub
5. **Log upload** — Runner uploaded step logs to GitHub's blob storage

### What Failed

All workflows failed at the same root cause: **Node.js 24 not bundled**.

The `--no-externals` flag skips downloading the official Node.js 24 bundle. GitHub Actions v4+ (`actions/checkout@v4`, `actions/setup-node@v4`, etc.) require Node 24 to run. The host has Node.js v26, which is incompatible.

### Runner Protocol Compliance

The runner successfully completed the full lifecycle:
```
configure → session → message → acquire → execute → report
```

This matches the protocol behavior documented in `docs/runner/conformance-test-log-2026-07-03-live-runner-rust.md`.

### Key Differences from Official Runner

| Aspect | Official Runner | aksh-runner |
|---|---|---|
| Runner version | 2.336.0 | 0.2.0 (protocol-compat 2.335.1) |
| Language | C# (.NET 8) | Rust |
| Node.js bundle | Downloaded automatically | Not bundled (`--no-externals`) |
| OS | Ubuntu 24.04 (GitHub-hosted) | macOS ARM64 (local) |
| Registration | GitHub-hosted runner pool | Self-hosted registration |

## Next Steps

To get meaningful log diffs, the runner needs Node.js 24 bundled:
1. Build with `--no-externals=false` or pre-install Node 24
2. Re-run the workflows
3. Compare full execution logs

## Files

```
benchmarks/real-world/results/aksh-runs/
├── echarts/
│   ├── run.json
│   ├── jobs.json
│   └── logs/
├── vscode/
│   ├── run.json
│   ├── jobs.json
│   └── logs/
├── angular/
├── n8n/
├── rocketmq/
├── pulsar/
├── cilium/
└── kafka/
```
