# Preloop Roadmap

Protocol-fidelity tracking lives in  
`docs/fidelity-gap.md.`  This doc tracks some fairly high level plans I have.

## P0 — day-1 blockers


| #   | Feature                                                                                     | Current state                                                                            | Evidence                                                        | Depends on                             |
| --- | ------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------- | -------------------------------------- |
| 1   | Run/log viewer (history, per-step logs, search, annotations, artifacts)                     | CLI-only + minimal public HTML page                                                      | `runs.rs::get_public_run` (≈20-line page); `preloop logs` CLI   | —                                      |
| 2   | Server observability: OTel metrics/traces, `/metrics` for Prometheus, structured access log | `tracing`/`tracing-subscriber` only; no OTel export; `queue_depth` atomic unpublished    | workspace `Cargo.toml`; `bootstrap.rs::ServeConfig.queue_depth` | —                                      |
| 3   | Cache correctness + ecosystem payload cache                                                 | `File name too long` bug; full-`Vec<u8>` buffering; no quotas/eviction; no payload cache | `plans/001` Phases 1–2 (P1, TODO)                               | #2 (instrumentation, plan 001 Phase 0) |
| 12  | Runner registration tokens the control plane issues and tracks (mint/redeem split)          | Any non-empty credential registers a runner over TCP; strict check applies only when the request arrives on the mounted control socket | `oauth.rs::github_registration_token` (`on_socket` gate); `auth.rs::runner_surface_only` layered only on the unix router (`bootstrap.rs`) | —                                      |


## P1 — team scale


| #   | Feature                                                                                             | Current state                                                                        | Evidence                                                    | Depends on                |
| --- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ | ----------------------------------------------------------- | ------------------------- |
| 4   | Multi-user authz (per-user tokens, roles)                                                           | Single `PRELOOP_SYSTEM_TOKEN`; 7 middleware guards hardcode it                       | `auth.rs`; `plans/plans.md` §3.4 (`AuthProvider` seam)      | plans.md Phase 1 refactor |
| 5   | Secrets backends (Vault / AWS SM / SOPS)                                                            | Config file + memory + systemd credential only                                       | `docs/setup.md`; `plans/plans.md` §3.5 (`SecretStore` seam) | plans.md Phase 1 refactor |
| 6   | Retention policies (runs/logs/artifacts) + `preloop backup`/`restore`                               | No enforcement; "store, not cache" with no policy implementation                     | `plans/001` taxonomy; `docs/setup.md` store docs            | —                         |
| 7   | Fork-PR trust on the data plane: server-derived cache namespaces, read/write split, capability URLs | Trust tiers exist for secrets; cache API trusts client-supplied `repository`/`scope` | `results_twirp::scoped_cache_key`; `plans/001` Phase 3      | #3                        |
| 13  | Runners on a separate machine from the control plane                                                | Protocol already supports it and the URL-rewrite plumbing exists, but nothing safely authenticates a remote registration, TLS is off by default, and the runner-surface restriction is socket-only | orchestrator `control_upstream`; `configure.rs` "TCP upstream mode" rewrite; `PRELOOP_RUNNER_URL` pinned to loopback at startup | #12                       |


## P2 — breadth


| #   | Feature                                                    | Current state                                                                                                    | Evidence                                                              | Depends on                |
| --- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- | ------------------------- |
| 8   | macOS/Windows ephemeral VM backends + image disambiguation | Virtualization.framework / QEMU backends designed, not shipped; `macos-13` vs `macos-14/15` collapse to one host | `docs/preloop_vs_others.md`; `docs/fidelity-gap.md` §1b.4             | —                         |
| 9   | Notifications (Slack/email/webhook-out) on run results     | Not implemented                                                                                                  | [INFERENCE: no code or doc references]                                | —                         |
| 10  | Protected-environment approval gates                       | `environment` parsed + OIDC-propagated; no reviewer approval flow                                                | `preloop-gha-parser` environment model; [INFERENCE: no approval code] | —                         |
| 11  | Multi-node HA (leader election or shared-bus RunStore)     | Documented limitation: two servers on one DB diverge in memory                                                   | `AGENTS.md` store conventions; `plans/plans.md` §3.2 (RunStore)       | plans.md Phase 1 refactor |
| 14  | Pool agent: microVM pools on hosts other than the control plane | Orchestrator runs in-process and reads the next job's `runs-on` labels through an `Arc<RwLock<Vec<String>>>` shared with the server, so a pool cannot live in another address space | `ServeConfig::next_job_runs_on` (`bootstrap.rs`); `runtime_scheduling::sync_next_job_labels` (`broker.rs`) | #13                       |


## Sequencing

```
#2 (OTel instrumentation) ──┐
                            ├─→ #3a cache correctness (plan 001 Phase 1)
#1 (log viewer) ────────────┘       → #3b payload cache (plan 001 Phase 2)
                                          → #7 fork-PR trust (plan 001 Phase 3)

plans.md Phase 1 refactor (ServerBuilder + seams) → #4 authz, #5 secrets
#6 retention/backup → #9 notifications → #10 approvals → #8 macOS/Windows hosts → #11 HA
```

```
#12 issued registration tokens ──→ #13 remote runners ──→ #14 pool agent (multi-host pools)
```

Notes:

- #2 is the keystone: it is plan 001 Phase 0, the log viewer's data source, and the first
thing operators ask for. It gates #3.
- #3 is the largest measured win: 62 s of a 92 s warm run was cache handling
(`plans/001` evidence).
- #4/#5/#7 share the trait-seam refactor in `plans/plans.md` Phase 1; OSS defaults must be
genuinely good (multi-user tokens, Vault optional, fork-PR enforced) — the seams are
not just cloud scaffolding.
- #11 is intentionally last: single-node is the supported topology until the RunStore
seam exists.
- #12 is a live hole, not hardening for later: a made-up token against the public
endpoint returns a `RunnerManage` JWT today, and a runner registered with matching
labels receives job messages carrying a minted installation token plus that job's
secrets. Until it lands, network reachability is the only control, so only the
webhook path should ever be published (`docs/self-hosting.md` §6).
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

