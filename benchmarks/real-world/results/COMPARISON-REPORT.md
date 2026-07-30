# Aksh Runner vs Official — Real-World Campaign Comparison

**Date:** 2026-07-28
**aksh-runner:** 0.2.0 (protocol-compat 2.335.1), Linux ARM64 cross-compiled
**Official runner:** 2.336.0 (Ubuntu 24.04 GitHub-hosted)

## Campaign Setup

- 8 upstream projects tested in parallel on macOS ARM64 host
- Workflows adapted for `self-hosted` labels with minimal changes
- Runner registered against `preloopdev/aksh-conformance-sample`
- `--no-externals` flag (no bundled Node.js)

## Results

| Project | Official | aksh | Official Steps | aksh Steps | Root Cause |
|---|---|---|---:|---:|---|
| ECharts | ✅ success | ❌ failure | 2 | 0* | Node 24 missing |
| VS Code | ✅ success | ❌ failure | 31 | 8 | Node 24 missing |
| Angular | ✅ success | ❌ failure | 11 | 8 | Node 24 missing |
| n8n | ✅ success | ❌ failure | 6 | 9 | Node 24 missing |
| RocketMQ | ✅ success | ❌ failure | 9 | 7 | Node 24 missing |
| Pulsar | ✅ success | ❌ failure | 8 | 7 | Node 24 missing |
| Cilium | ✅ success | ❌ failure | 10 | 0* | Node 24 missing |
| Kafka | ✅ success | ❌ failure | 12 | 0* | Node 24 missing |

*Zero steps = runner cancelled before execution

## Step-Level Comparison (VS Code as example)

### Official (Ubuntu, full toolchain)
```
 1. Set up job                    → success
 2. Checkout                      → success
 3. Setup Node.js                 → success
 4. Restore node_modules cache    → success
 5. Install dependencies          → skipped
 6. Install build dependencies    → skipped
 7. Install rspack dependencies   → skipped
 8. Save node_modules cache       → skipped
 9. Copy codicons                 → success
10. Install Playwright Chromium   → success
... (31 total steps)
```

### aksh-runner (macOS, no Node 24 bundle)
```
1. Set up job                 → success
2. Run actions/checkout@v4    → failure    ← Node 24 missing
3. Run actions/setup-node@v4  → skipped
4. Install dependencies       → skipped
5. Compile                    → skipped
6. Post Run actions/setup-node@v4 → skipped
7. Post Run actions/checkout@v4   → success
8. Complete job               → success
```

## What Worked

1. **Full runner lifecycle**: configure → session → message → acquire → execute → report
2. **Job acquisition**: Runner polled GitHub broker and acquired jobs
3. **Step execution**: `Set up job` and `Complete job` succeeded
4. **Result reporting**: Job conclusions reported back to GitHub
5. **Log upload**: Step logs uploaded to GitHub blob storage
6. **Parallel execution**: 8 runners processed 8 workflows simultaneously

## What Failed

**Root cause**: `--no-externals` skips Node.js 24 bundle download. GitHub Actions v4+ (`actions/checkout@v4`, `actions/setup-node@v4`) require Node 24. Host has Node.js v26 which is incompatible.

**Error**: `bundled node24 is missing at /private/tmp/aksh-runner-*/_work/externals/node24/bin/node; system Node is v26 but the action requires Node 24`

## Key Differences

| Aspect | Official Runner | aksh-runner |
|---|---|---|
| Language | C# (.NET 8) | Rust |
| Runner version | 2.336.0 | 0.2.0 |
| Node.js | Bundled (auto-download) | Not bundled |
| OS | Ubuntu 24.04 (GitHub-hosted) | macOS ARM64 (local) |
| Registration | GitHub-hosted pool | Self-hosted |
| Step names | Display names (`Checkout`) | Script content (`Run actions/checkout@v4`) |

## Artifacts

```
benchmarks/real-world/results/
├── public-runs/          # Official GitHub runner logs
│   ├── echarts/
│   ├── vscode/
│   ├── angular/
│   ├── n8n/
│   ├── rocketmq/
│   ├── pulsar/
│   ├── cilium/
│   └── kafka/
├── aksh-runs/            # aksh-runner logs
│   ├── echarts/
│   ├── vscode/
│   ├── angular/
│   ├── n8n/
│   ├── rocketmq/
│   ├── pulsar/
│   ├── cilium/
│   └── kafka/
└── aksh-campaign-report.md
```

## Next Steps

1. Bundle Node.js 24 with aksh-runner (remove `--no-externals`)
2. Re-run workflows to get full execution logs
3. Compare step output, timing, and log formatting
4. Run against aksh-server for server-side comparison
