# Conformance: antfu/eslint-config

Workflow-under-test: `.github/workflows/ci.yml` — `lint` plus a `test` matrix
(`node-version: [lts/*]` × `os: [ubuntu-latest, macos-latest,
windows-latest]`, `fail-fast: false`), 4 jobs total. Clone head
`56bf756a1757aea50bea885c6859a21ca5210a63`; oracle log captured from a
github.com run (`eslint-ci.log`) with the same 4 job names.

| | Oracle | Local replay |
|---|---|---|
| Run | github.com | `86c6e183-850b-4874-ad6a-be0b050b9296` |
| Date | 2026-08-06 | 2026-08-06 |
| Runner | hosted (ubuntu/macos/windows) | engine VM pool (arm64 ubuntu) |
| Outcome | success | **all 4 jobs fail at the `setup-node@v7` step** — `lts/*` alias resolution 401s (below) |

## What ran locally

Submission, planning, snapshot checkout (`773c0611…` served from
`/snapshots/86c6e183-…`), job execution, and log upload all worked: the
failure is inside the first step, not the control plane. `fail-fast: false`
matrix semantics observed again (all 4 cells ran to completion independently).

## Divergences found

### 1. `setup-node` cannot resolve the `lts/*` alias without a real GitHub token (DOCUMENTED — engine has no GitHub App configured)

Every job pins `node-version: lts/*`. `setup-node@v7` resolves the alias by
fetching the version manifest from `api.github.com` (via
`@actions/tool-cache`) **using the job's `GITHUB_TOKEN`**. With no GitHub App
configured, Preloop issues a local HMAC JWT as `GITHUB_TOKEN`; the call to
`api.github.com` is rejected with `401 Bad credentials`:

```
Attempt to resolve LTS alias from manifest...
##[error]Bad credentials
##[error]node action exited with code 1
```

This is the *documented* token-scope contract (`docs/github-tokens.md` §3:
"any call it makes to `api.github.com` will fail"), not a new divergence. It
is also the campaign's clearest trip-wire for it: `lts/*` is the most common
node version pin, and every workflow that uses it fails identically without
an App.

Remedy (implemented, needs configuration — `crates/aksh-runner-server/
src/github_app.rs`): register a GitHub App and export `AKSH_GITHUB_APP_ID` +
`AKSH_GITHUB_APP_PEM(_FILE)`. Jobs then receive short-lived installation
tokens and `lts/*` resolves from the real manifest. The `test (lts/*,
ubuntu-latest)` cell additionally shows the step getting *partway* — it
refreshed pnpm to v11.20.0 (`package-manager` input) before dying on the
alias — so everything after version resolution is untested locally.

### 2. macOS/windows cells cannot run on the pool (DOCUMENTED)

`macos-latest`/`windows-latest` jobs are skipped by design (see
`docs/fidelity-gap.md` 1b.4); they would fail at the same setup-node step
anyway.

## Environment differences (documented, not fixed)

- `lts/*` (alias) is the only node pin used; no concrete version exists for
  setup-node to install offline. With an App configured, resolution is a
  normal egress API call.
- The runner executes as root (1b.4/1b.5) — not exercised, no step ran past
  setup-node.
- Pool is arm64 — not exercised.
