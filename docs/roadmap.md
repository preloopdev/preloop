# Preloop Roadmap

Protocol-fidelity tracking lives in  
`docs/fidelity-gap.md.`  This doc tracks some fairly high level plans I have.

## P0 — day-1 blockers


| #   | Feature                                                                                     | Current state                                                                            | Evidence                                                        | Depends on                             |
| --- | ------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------- | -------------------------------------- |
| 1   | Run/log viewer (history, per-step logs, search, annotations, artifacts)                     | CLI-only + minimal public HTML page                                                      | `runs.rs::get_public_run` (≈20-line page); `preloop logs` CLI   | —                                      |
| 2   | Server observability: OTel metrics/traces, `/metrics` for Prometheus, structured access log | OTel metrics/traces exported (OTLP) and `/metrics` live; no dashboards or structured access log    | workspace `Cargo.toml`; `preloop-observability` (OTLP export, `preloop.job.queue_depth`); `routes.rs` `/metrics` | —                                      |
| 3   | Cache correctness + ecosystem payload cache                                                 | full-`Vec<u8>` buffering; no quotas/eviction; no payload cache | `preloop-cache/src/lib.rs`                               | #2 (instrumentation) |
| 12  | Runner registration tokens the control plane issues and tracks (mint/redeem split)          | Strict mode requires the system credential for GitHub-compatible registration and all legacy Agent aliases; a pool-issued one-time provision token is the host-side alternative; `PRELOOP_REGISTRATION_POLICY=permissive` is conformance-only | `oauth.rs::github_registration_token`; `auth.rs::require_runner_registration_bearer`; `auth.rs::consume_pending_provision_token` | —                                      |


## P1 — team scale


| #   | Feature                                                                                             | Current state                                                                        | Evidence                                                    | Depends on                |
| --- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ | ----------------------------------------------------------- | ------------------------- |
| 4   | Multi-user authz (per-user tokens, roles)                                                           | Single `PRELOOP_SYSTEM_TOKEN`; 7 middleware guards hardcode it                       | `auth.rs` | trait-seam refactor |
| 5   | Secrets backends (Vault / AWS SM / SOPS)                                                            | Config file + memory + systemd credential only                                       | `docs/setup.md` | trait-seam refactor |
| 6   | Retention policies (runs/logs/artifacts) + `preloop backup`/`restore`                               | Retention sweep shipped (90-day default); `preloop backup`/`restore` still missing                     | `retention.rs` (hourly sweep); `docs/setup.md` store docs            | —                         |
| 7   | Fork-PR trust on the data plane: capability URLs | Cache `repository`/`scope` are server-derived (job token and git ref; reads own-ref + default branch); capability URLs missing | `results_twirp::scoped_cache_key`      | #3                        |
| 13  | Runners on a separate machine from the control plane                                                | Protocol already supports it and the URL-rewrite plumbing exists, but nothing safely authenticates a remote registration, TLS is off by default, and the runner-surface restriction is socket-only | orchestrator `control_upstream`; `configure.rs` "TCP upstream mode" rewrite; `PRELOOP_RUNNER_URL` pinned to loopback at startup | #12                       |


## P2 — breadth


| #   | Feature                                                    | Current state                                                                                                    | Evidence                                                              | Depends on                |
| --- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- | ------------------------- |
| 8   | macOS/Windows ephemeral VM backends + image disambiguation | Virtualization.framework / QEMU backends designed, not shipped; `macos-13` vs `macos-14/15` collapse to one host | `docs/preloop_vs_others.md`; `docs/fidelity-gap.md` §1b.4             | —                         |
| 9   | Notifications (Slack/email/webhook-out) on run results     | Not implemented                                                                                                  | [INFERENCE: no code or doc references]                                | —                         |
| 10  | Protected-environment approval gates                       | `environment` parsed + OIDC-propagated; no reviewer approval flow                                                | `preloop-gha-parser` environment model; [INFERENCE: no approval code] | —                         |
| 11  | Multi-node HA (leader election or shared-bus RunStore)     | Documented limitation: two servers on one DB diverge in memory                                                   | `AGENTS.md` store conventions | RunStore seam |
| 14  | Pool agent: microVM pools on hosts other than the control plane | Orchestrator runs in-process and reads the next job's `runs-on` labels through an `Arc<RwLock<Vec<String>>>` shared with the server, so a pool cannot live in another address space | `ServeConfig::next_job_runs_on` (`bootstrap.rs`); `runtime_scheduling::sync_next_job_labels` (`runtime_scheduling.rs`) | #13                       |


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
- #11 is intentionally last: single-node is the supported topology until the RunStore
seam exists.
- #12 must keep a permissive escape for the conformance replays: the goldens send a
real GitHub-issued registration token that this control plane can never have minted,
so strict-by-default needs an explicit opt-out for the harness.
- #13 is mostly #12 plus TLS and pointing `PRELOOP_RUNNER_URL`/`PRELOOP_CONTROL_UPSTREAM`
at a reachable address; the wire protocol already allows it, which is the whole point of
the fidelity work.
- #14's blocker is one field. Replacing the shared `next_job_runs_on` lock with a
request ("what labels are queued?") is what lets the orchestrator run anywhere; the
agent then terminates the control socket locally and proxies to the control plane, so
guests still never touch the network. Bandwidth is the real constraint — a measured
cache restore moved 791 MB, which is fine on a LAN and painful across a WAN.

