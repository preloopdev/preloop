# Preloop Roadmap

**Status snapshot:** current-state descriptions were checked against `public/main`
(`b9ab767f3`) on October 15, 2026. This is an engineering tracker, not a
support contract; verify with the linked docs before planning a deployment.
Protocol-fidelity campaigns and historical gaps live in
[Fidelity Gap & Roadmap](fidelity-gap.md).

## P0 — day-1 blockers


| #   | Feature                                                                                     | Current state                                                                            | Evidence                                                        | Depends on                             |
| --- | ------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------- | -------------------------------------- |
| 1   | Run/log viewer (history, per-step logs, search, annotations, artifacts)                     | CLI-only + minimal public HTML page                                                      | `runs.rs::get_public_run` (≈20-line page); `preloop logs` CLI   | —                                      |
| 2   | Server observability: OTel metrics/traces, `/metrics` for Prometheus, structured access log | Metrics are collected locally; metrics/logs/traces support opt-in OTLP export. `/metrics` is live and protected by the native bearer. Dashboards and structured HTTP access-log output remain missing | `preloop-observability`; `routes.rs`; `http_metrics.rs` | — |
| 3   | Cache correctness + ecosystem payload cache                                                 | Actions-cache uploads/downloads are file-streamed; staged-file commits avoid whole-archive RAM buffering. Configurable server-wide quota and LRU eviction are implemented (10 GiB default; newly committed oversized entries are temporarily protected). Ecosystem payload caching is separate roadmap work | `cache_artifacts.rs`; `blob_store.rs`; `preloop-cache/src/lib.rs` | #2 (instrumentation) |
| 12  | Runner registration tokens the control plane issues and tracks (mint/redeem split)          | Strict GitHub-compatible minting requires the system credential and issues a signed RunnerManage JWT accepted by legacy aliases. Pool provision tokens are a separate ten-minute, one-time credential. Permissive registration is a loopback-only conformance escape; registration JWTs are not one-time redemption records | `oauth.rs`; `auth.rs`; `bootstrap.rs` | — |


## P1 — team scale


| #   | Feature                                                                                             | Current state                                                                        | Evidence                                                    | Depends on                |
| --- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ | ----------------------------------------------------------- | ------------------------- |
| 4   | Multi-user authz (per-user tokens, roles)                                                           | Native operator APIs still use one shared system credential; per-user identities and roles are absent. Runner/job surfaces already use scoped credentials and identity-aware guards | `auth.rs`; `runs.rs` | user/role model for native APIs |
| 5   | Secrets backends (Vault / AWS SM / SOPS)                                                            | Built-in `SecretProvider` supports global/repository/environment configuration secrets, systemd credentials, live updates, and encrypted durable submission-secret run tiers. Vault/AWS SM/SOPS adapters remain missing; the provider seam already exists | `secret_provider.rs`; `secrets_api.rs` | external provider adapters |
| 6   | Retention policies (runs/logs/artifacts) + `preloop backup`/`restore`                               | Retention sweep shipped (90-day default); `preloop backup`/`restore` still missing                     | `retention.rs` (hourly sweep); `docs/setup.md` store docs            | —                         |
| 7   | Fork-PR trust on the data plane: capability URLs | Cache namespaces/read scopes are server-derived; fork-PR cache writes are denied. Cache v2 uses expiring server-signed, operation- and owner-bound upload/download URLs | `results_twirp.rs`; `auth.rs`; `events/trust_tier.rs` | — |
| 13  | Runners on a separate machine from the control plane                                                | Protocol and URL override exist, with authenticated system-token/issued-JWT registration. `PRELOOP_RUNNER_URL` defaults to loopback but accepts an explicit reachable origin. TLS requires configuration; a dedicated runner-only TCP surface remains missing | `oauth.rs`; `auth.rs`; `preloop-cli/src/main.rs`; `bootstrap.rs` | Dedicated runner-only TCP surface |


## P2 — breadth


| #   | Feature                                                    | Current state                                                                                                    | Evidence                                                              | Depends on                |
| --- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- | ------------------------- |
| 8   | macOS/Windows ephemeral VM backends + image disambiguation | Virtualization.framework / QEMU backends designed, not shipped; `macos-13` vs `macos-14/15` collapse to one host | `docs/preloop_vs_others.md`; `docs/fidelity-gap.md` §1b.4             | —                         |
| 9   | Notifications (Slack/email/webhook-out) on run results     | Not implemented                                                                                                  | [INFERENCE: no code or doc references]                                | —                         |
| 10  | Protected-environment approval gates                       | Implemented: GitHub-sourced reviewer rules, authorized Check Run Approve/Reject actions, and a native operator override. Local TOML mode supports at most one shared-credential approval, not distinct-user quorum | `environment_resolver.rs`; `github.rs`; `routes.rs`; `config.rs` | — |
| 11  | Complete multi-node HA (leader election, shared queue and node-aware routing) | Shared Postgres control state, cross-node runner wakeups, and outbox-backed event delivery are implemented. Complete HA still requires node-aware handling for local payload/reservation state and a shared secret provider; authoritative runs/jobs no longer diverge through process-local state | `control/`; `control/wake.rs`; `event_feed.rs`; `secret_provider.rs`; `docs/architecture.md` | Node-aware payloads and shared secrets |
| 14  | Pool agent: microVM pools on hosts other than the control plane | Built-in pool is co-hosted in-process and shares demand, image-selection, preparation, and provision-token state with the server. Label sharing is optional for fixed-image pools; a standalone authenticated remote pool-agent integration is not shipped | `preloop-cli/src/main.rs`; `bootstrap.rs`; `preloop-orchestrator/src/lib.rs`; `runtime_scheduling.rs` | #13 |


## Sequencing

```
#2 (OTel instrumentation) ──┐
                            ├─→ #3a cache correctness
#1 (log viewer) ────────────┘       → #3b payload cache
                                          → #7 fork-PR trust

ServerBuilder + trait seams → #4 authz, #5 secrets
#6 retention/backup → #9 notifications → #10 approvals → #8 macOS/Windows hosts → #11 HA
```

```
#12 issued registration tokens ──→ #13 remote runners ──→ #14 pool agent (multi-host pools)
```

Notes:

- #2 is the keystone: the log viewer's data source, and the first
thing operators ask for. It gates #3.
- #3 is the largest measured win: 62 s of a 92 s warm run was cache handling.
- #4/#5/#7 share the trait-seam refactor; OSS defaults must be
genuinely good (multi-user tokens, Vault optional, fork-PR enforced) — the seams are
not just cloud scaffolding.
- #11 remains unfinished: shared Postgres, cross-node wakeups, and outbox event
  delivery are shipped, but node-local payload/reservation state and shared
  secrets still need explicit coordination.
- #12 must keep a permissive escape for the conformance replays: the goldens send a
real GitHub-issued registration token that this control plane can never have minted,
so strict-by-default needs an explicit opt-out for the harness.
- #13 is mostly #12 plus TLS and pointing `PRELOOP_RUNNER_URL`/`PRELOOP_CONTROL_UPSTREAM`
at a reachable address; the wire protocol already allows it, which is the whole point of
the fidelity work.
- #14 depends on #13. The built-in pool is shipped in-process and shares demand,
  image-selection, preparation, and provision-token state with the server; the
  standalone authenticated remote pool-agent integration is not shipped. Its
  technical blocker is one field: replacing the shared `next_job_runs_on` lock with a
  request ("what labels are queued?") is what lets the orchestrator run anywhere; the
  agent then terminates the control socket locally and proxies to the control plane, so
  guests still never touch the network. Bandwidth is the real constraint — a measured
  cache restore moved 791 MB, which is fine on a LAN and painful across a WAN.

