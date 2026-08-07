# Preloop Roadmap

Protocol-fidelity tracking lives in  
`docs/fidelity-gap.md.`  This doc tracks some fairly high level plans I have.

## P0 — day-1 blockers


| #   | Feature                                                                                     | Current state                                                                            | Evidence                                                        | Depends on                             |
| --- | ------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------- | -------------------------------------- |
| 1   | Run/log viewer (history, per-step logs, search, annotations, artifacts)                     | CLI-only + minimal public HTML page                                                      | `runs.rs::get_public_run` (≈20-line page); `preloop logs` CLI   | —                                      |
| 2   | Server observability: OTel metrics/traces, `/metrics` for Prometheus, structured access log | `tracing`/`tracing-subscriber` only; no OTel export; `queue_depth` atomic unpublished    | workspace `Cargo.toml`; `bootstrap.rs::ServeConfig.queue_depth` | —                                      |
| 3   | Cache correctness + ecosystem payload cache                                                 | `File name too long` bug; full-`Vec<u8>` buffering; no quotas/eviction; no payload cache | `plans/001` Phases 1–2 (P1, TODO)                               | #2 (instrumentation, plan 001 Phase 0) |


## P1 — team scale


| #   | Feature                                                                                             | Current state                                                                        | Evidence                                                    | Depends on                |
| --- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ | ----------------------------------------------------------- | ------------------------- |
| 4   | Multi-user authz (per-user tokens, roles)                                                           | Single `PRELOOP_SYSTEM_TOKEN`; 7 middleware guards hardcode it                       | `auth.rs`; `plans/plans.md` §3.4 (`AuthProvider` seam)      | plans.md Phase 1 refactor |
| 5   | Secrets backends (Vault / AWS SM / SOPS)                                                            | Config file + memory + systemd credential only                                       | `docs/setup.md`; `plans/plans.md` §3.5 (`SecretStore` seam) | plans.md Phase 1 refactor |
| 6   | Retention policies (runs/logs/artifacts) + `preloop backup`/`restore`                               | No enforcement; "store, not cache" with no policy implementation                     | `plans/001` taxonomy; `docs/setup.md` store docs            | —                         |
| 7   | Fork-PR trust on the data plane: server-derived cache namespaces, read/write split, capability URLs | Trust tiers exist for secrets; cache API trusts client-supplied `repository`/`scope` | `results_twirp::scoped_cache_key`; `plans/001` Phase 3      | #3                        |


## P2 — breadth


| #   | Feature                                                    | Current state                                                                                                    | Evidence                                                              | Depends on                |
| --- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- | ------------------------- |
| 8   | macOS/Windows ephemeral VM backends + image disambiguation | Virtualization.framework / QEMU backends designed, not shipped; `macos-13` vs `macos-14/15` collapse to one host | `docs/preloop_vs_others.md`; `docs/fidelity-gap.md` §1b.4             | —                         |
| 9   | Notifications (Slack/email/webhook-out) on run results     | Not implemented                                                                                                  | [INFERENCE: no code or doc references]                                | —                         |
| 10  | Protected-environment approval gates                       | `environment` parsed + OIDC-propagated; no reviewer approval flow                                                | `preloop-gha-parser` environment model; [INFERENCE: no approval code] | —                         |
| 11  | Multi-node HA (leader election or shared-bus RunStore)     | Documented limitation: two servers on one DB diverge in memory                                                   | `AGENTS.md` store conventions; `plans/plans.md` §3.2 (RunStore)       | plans.md Phase 1 refactor |


## Sequencing

```
#2 (OTel instrumentation) ──┐
                            ├─→ #3a cache correctness (plan 001 Phase 1)
#1 (log viewer) ────────────┘       → #3b payload cache (plan 001 Phase 2)
                                          → #7 fork-PR trust (plan 001 Phase 3)

plans.md Phase 1 refactor (ServerBuilder + seams) → #4 authz, #5 secrets
#6 retention/backup → #9 notifications → #10 approvals → #8 macOS/Windows hosts → #11 HA
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

