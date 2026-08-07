# Preloop Open-Core Split — Plan + Extensibility Audit

Status: proposed. Owner: @bnjoroge.

Goal: ship an OSS control plane usable by individuals/small teams, plus a private
proprietary cloud tier (better schedulers, distributed cache, multi-tenancy,
Firecracker) that **extends** the OSS server by injecting implementations — with
zero forking of OSS source and zero protocol divergence.

---

## 1. Decisions (locked recommendations)

| Question | Decision | Rationale |
|---|---|---|
| Repo topology | **Two repos.** Public `preloop` (OSS) + private `preloop-cloud` embedding it as a **git submodule** | Real proprietary tier needs code + history off GitHub; submodule keeps one working tree |
| OSS the server? | **Yes**, source-available | The server *is* the managed service — the one asset to protect |
| Server license | **FSL-1.1-Apache-2.0** on `preloop-runner-server` | "Free for all except a competing managed service"; auto-converts to Apache-2.0 after 2 yrs |
| Everything else | **MIT** | Max composability (esp. the runner: "any runner works with any server"); MIT inbound embeds into cloud freely |
| Contributions | **DCO on MIT crates; CLA scoped to the server crate from day one** | DCO covers MIT embedding forever; server CLA preserves commercial-license revenue lever; retroactive CLA is painful |
| OSS/proprietary boundary | **The trait seams**, not the `preloop`/`preloop` prefix | Prefixes already don't map to the boundary (server depends on `preloop-socket-activation`) |
| How cloud extends the server | **Injected `Arc<dyn Trait>` impls via a `ServerBuilder`**, never forked files | One protocol implementation → zero wire drift |
| Where cloud scheduler/providers live | **Orchestrator owns providers + placement; server owns protocol + a `Scheduler` seam** | Orchestrator already owns VM lifecycle; server stays the fidelity engine |

### Open inputs still needed
- FSL change window (default 2 yrs) and change license (Apache-2.0 vs MIT).
- Confirm CLA-from-day-one vs DCO-only-defer.
- Public org/repo name (`preloopdev/preloop`).

---

## 2. Target architecture

**Horizontal cut** — share the protocol, inject the brains.

- **Shared OSS (one impl, never diverges):** all `/_apis/...` + `/api/v1/...` routes,
  wire DTOs, session crypto, runner lifecycle, conformance suite.
- **Injectable seams (where cloud differentiates):** `Scheduler`, `RunStore`,
  `CacheStore`/`ArtifactStore`/`BlobStore`, `SecretStore`, `AuthProvider`, plus the
  already-good `VmProvider`/(new) `RunnerProvider` on the orchestrator side.

```
preloop (public)                         preloop-cloud (private, proprietary)
  MIT: protocol/parser/expressions          firecracker VmProvider
       runner/runner-client/cli             fair-share/priority Scheduler
       cache/artifacts/vm/orchestrator      distributed dedup CacheStore
       socket-activation      <—submodule—  sqlx RunStore + billing + tenancy
  FSL: preloop-runner-server                    cloud control-plane bin
       (ServerBuilder + routes + defaults)   (composes ServerBuilder w/ above)
```

**Don't hollow out the core.** OSS ships correct, usable defaults (FIFO+label+
concurrency scheduler; file cache; in-memory store). Cloud differentiates on
**scale + intelligence**, never by crippling the baseline.

---

## 3. Extensibility audit (grounded, current state)

Verdict: **the server has essentially zero dependency injection today.** Only the
`VmProvider` seam (orchestrator side) is production-ready. Every server subsystem is
a concrete struct or free functions over `Arc<Mutex<InnerState>>`. The escape hatch
half-exists (a `Router` is buildable from an `AppState`), but the engine underneath
is sealed (`pub(crate)` fields, hardcoded construction).

### 3.0 Composition entry points — PARTIAL
- `AppState::new()` is `pub` and `routes::app`/`app_with_test_api`/`build_app` build a
  standalone `axum::Router` from an `AppState` (`routes.rs:4,11,18`; `build_app`
  constructs `SharedState` at `routes.rs:27`). So an external crate *can* stand up a
  Router today.
- **Blocker:** all swappable fields on `AppState`/`InnerState` are `pub(crate)`, and
  `AppState::new()` (`state.rs:287`) + `serve()` (`bootstrap.rs:218`) hardcode every
  construction (`CacheStore::new` 288, `ArtifactStore::new` 289, `system_token` from
  env 333, HMAC 335, `github_app` 349, `stored_secrets` 350, `Scheduler::new` 242).
  No `ServerBuilder`, no injection.

### 3.1 Scheduler — NOT INJECTABLE (largest refactor)
- Scheduling is **free functions over `&mut InnerState`**, not a component. No trait.
  (`runtime_scheduling.rs:1-1660`: `promote_ready_jobs`, `take_matching_job`,
  `try_acquire_concurrency`, `under_max_parallel`, `cancel_run_inner`, …)
- Queues live on `InnerState` (`state.rs:548-639`): `queue`/`pending_jobs`/
  `concurrency_blocked` `VecDeque`, `held_runs` `BTreeMap`, `concurrency_groups`.
- Dispatch is a hardcoded O(n) FIFO label scan (`take_matching_job`) called at 3
  sites: `broker.rs:260`, `broker.rs:561`, `distributed_task.rs:66`.
- Promotion/completion entry points: `runs.rs:submit_run_inner` (→`promote_ready_jobs`
  at ~1129), `distributed_task.rs:complete_job_inner` (→`promote_ready_jobs` at ~614).
- **Note:** `scheduling.rs` is a *pure test oracle*, not wired to production — do not
  confuse with runtime dispatch. `scheduler.rs` is the **cron** executor (unrelated).
- **Seam:** `trait Scheduler` with methods `submit_run` / `complete_job` /
  `dispatch_job` / `promote_ready` / `cancel_run` / `acquire_concurrency`; field
  `AppState.scheduler: Arc<dyn Scheduler>`; `DefaultScheduler` delegates to today's
  free functions (zero behavior change). Route the 3 dispatch sites + promotion sites
  through it.

### 3.2 RunStore / persistence — NOT INJECTABLE (broadest refactor)
- All run/job/queue/timeline/log state is in `InnerState` behind one
  `Arc<Mutex<InnerState>>` (`state.rs:469-550`): `runs: BTreeMap<RunId, RunRecord>`,
  the 4 queues, `timeline_records`/`timeline_events`, `logs`/`log_metadata`/
  `live_log_lines`, plus correlation maps.
- Only restart-surviving state today: artifact_v2 registry JSON, OIDC keypair, HMAC
  key, replay logs. Runs are purely in-memory.
- Every handler locks and touches `inner.*` directly. Refactor surface: **~44
  `inner.runs` sites + ~20 queue + ~15 timeline/log** across `runs.rs`, `broker.rs`,
  `distributed_task.rs`, `runtime_scheduling.rs`, `timeline_logs.rs`.
- **Seam:** `trait RunStore` (runs, job state, queue, pending/held/blocked,
  cancellation, timeline, logs, correlation); `InMemoryRunStore` = today's maps (zero
  behavior change); cloud `SqlxRunStore` maps methods to SQL with txn-scoped locking.
- **Sequencing:** this is the widest change. Do it *after* the Scheduler seam, because
  Scheduler methods already take `&mut InnerState` and can adopt `RunStore` internally.

### 3.3 Cache / Artifacts / Blob — NOT INJECTABLE (docs were aspirational)
- **No store traits exist.** `preloop-cache` exposes a concrete `CacheStore`
  (`preloop-cache/src/lib.rs:43`, methods `new/put/get/find_prefix`); `preloop-artifacts`
  a concrete `ArtifactStore` (`preloop-artifacts/src/lib.rs:80`, `new/put/get/list_run`).
  Wired as concrete `AppState` fields (`state.rs:215-216`), constructed at
  `state.rs:287-289`. (`architecture.md` claim of an existing trait is aspirational.)
- A **third, un-abstracted** storage layer: blob handlers write `state_dir/blobs/...`
  and `state_dir/replay/...` via raw `tokio::fs` in axum handlers (`blob_store.rs:52-55,
  174-178, 304-307`; `artifact_twirp.rs:89-93, 118-122`), plus an in-memory
  `artifact_v2_registry`.
- **Seam:** `trait CacheStore` + `trait ArtifactStore` in their crates (concrete impls
  become the file-backed defaults); a new `trait BlobStore` for staged/assembled
  chunks. `AppState` fields become `Arc<dyn ...>`. Cloud injects distributed/dedup.

### 3.4 Auth — NOT INJECTABLE
- No auth trait. 7 middleware guards in `auth.rs` each compare against
  `shared.state.system_token` (single instance credential, `PRELOOP_SYSTEM_TOKEN`, default
  `"preloop-system-token"`, set at `state.rs:307-308`) or verify local JWT claims. Admin
  gate is `token == system_token` at `auth.rs:71`. No tenant namespace. Webhook route
  uses HMAC (`X-Hub-Signature-256`), not bearer.
- **Seam:** `trait AuthProvider { fn authenticate(headers, scope) -> Result<Principal> }`
  wrapping today's logic as `LoopbackAuth`; cloud impl does OAuth/mTLS + per-tenant
  scoping. Field on `AppState`; the 7 guards call through it.

### 3.5 Secrets — NOT INJECTABLE (but cleanly traceable)
- 3 sources merged at submit: submission payload (`WorkflowSubmission.secrets`,
  protocol `lib.rs:180`), `AppState.stored_secrets` from config `[secrets]`
  (`state.rs:350`), and a late-bound GitHub App token minted at broker acquire
  (`broker.rs:793 mint_dispatch_github_token`).
- Flow: `submit_run_inner` merges stored secrets gated by `trust_tier.allows_secrets()`
  (`runs.rs:119-140`) → `build_job_artifacts` exposes via `.expose()` (`runs.rs:582-585`)
  → `job_builder.rs:415-434` emits `VariableValue::secret` + mask hints. `SecretString`
  (`preloop-gha-protocol/src/lib.rs:98`) redacts Debug/Display/Serialize; `.expose()` is
  the only reader.
- **Seam:** `trait SecretStore { async fn resolve(tenant, names) -> SecretMap }`
  defaulting to payload+config; cloud pulls from Vault/AWS SM. Inject at the
  `submit_run_inner` merge point.

### 3.6 VmProvider / RunnerProvider — ALREADY GOOD (Vm) / MISSING (Runner)
- `VmProvider` trait (`preloop-vm/src/lib.rs:219-258`, 13 methods) is well-designed
  and fully injectable via `RunnerPool<P: VmProvider>` (`orchestrator lib.rs:745`).
  `SmolVmProvider` implements it by shelling to the `smolvm` CLI.
- **Firecracker = a new `VmProvider` impl + CLI/config selection. Zero server or
  orchestrator edits.** This seam is done.
- `RunnerProvider` exists **only in docs** — zero `.rs` matches. If wanted, add it in
  the orchestrator; but `VmProvider` already covers the local/cloud VM case.
- Orchestrator lists `preloop-runner-server` as a dep but **never imports it** (unused
  dep; runtime coupling is HTTP). Placement/label-routing belongs here, not the server.

### Audit summary table

| Subsystem | Injectable today? | Blocker | Refactor size |
|---|---|---|---|
| Composition (`ServerBuilder`) | Partial | `pub(crate)` fields; hardcoded `new()`/`serve()` | Small |
| Scheduler | No | free fns over `InnerState`; no trait | **Large** |
| RunStore | No | all state in `InnerState`; ~79 call sites | **Largest** |
| Cache/Artifacts/Blob | No | concrete structs + raw fs handlers | Medium |
| Auth | No | 7 guards hardcode `system_token` | Small-Med |
| Secrets | No | inline merge at submit | Small |
| VmProvider | **Yes** | — | none |
| RunnerProvider | N/A | doc-only | optional |

---

## 4. Phased execution

### Phase 0 — Lock decisions (owner)
Resolve the "Open inputs" in §1.

### Phase 1 — Make the server a framework (load-bearing; unblocks everything)
1. Add `ServerBuilder` accepting `Arc<dyn Scheduler>`, `Arc<dyn RunStore>`,
   `Arc<dyn CacheStore/ArtifactStore/BlobStore>`, `Arc<dyn AuthProvider>`,
   `Arc<dyn SecretStore>`, each defaulting to the current impl. `AppState::new()` and
   `serve()` build via it.
2. Extract the traits (§3.1–3.5). Refactor today's code into the default impls:
   `DefaultScheduler`, `InMemoryRunStore`, file-backed stores, `LoopbackAuth`,
   payload/config `SecretStore`. **Order:** ServerBuilder skeleton → Cache/Artifacts/
   Blob (self-contained) → Auth → Secrets → Scheduler → RunStore (widest, last).
3. Make `Router`/`AppState` fully composable from an external crate (expose what the
   builder needs; keep internals private).
4. Design every trait so a *third party* could implement it (generality keeps the OSS
   split honest — no proprietary-shaped hooks).
- **Verify each step:** `just test-ci` green; `preloop-server` boots on defaults; a
  throwaway alternate `Scheduler`/`RunStore` swaps in via the builder and passes the
  conformance suite (proves no wire drift).

### Phase 2 — Crate reorg + boundary guardrail
- Confirm `preloop-socket-activation` stays OSS (it's a server dep — must be public).
- Add CI checks to `just test-ci`: (a) no OSS crate depends outside the OSS set;
  (b) each OSS crate builds standalone with cloud crates removed from the workspace.

### Phase 3 — Licensing rollout
- Root `LICENSE-MIT` + `LICENSE-FSL` (fill licensor / change date / change license).
- Per-crate `license`: `preloop-runner-server = FSL-1.1-Apache-2.0`, all others `MIT`.
  Drop the workspace-wide `license = "MIT"` default in favor of per-crate values.
- `CONTRIBUTING.md` DCO note (`git commit -s`) + DCO bot; CLA-assistant scoped to the
  server crate path.

### Phase 4 — Two-repo split
- Public `preloop` = current tree minus cloud crates; full `just test-ci` on it alone.
- Private `preloop-cloud` = public as submodule + `preloop-cloud-*` crates; pinned
  submodule commit is the gate for absorbing community fixes.
- **Publish hygiene:** allowlist crates; the repo currently holds `.credentials`,
  `*.pem`, `.preloop/` secrets that must never reach public.

### Phase 5 — Proprietary tier
- `preloop-cloud-store` (sqlx RunStore + billing/tenancy), `-scheduler`
  (fair-share/priority), `-cache` (distributed dedup), `-firecracker` (VmProvider
  primary), `-controlplane` (cloud `main` composing `ServerBuilder`).
- Firecracker is just a second `VmProvider`; cloud runs it primary and still exposes a
  `smolvm`-labeled pool so local workflows run remotely unchanged.

### Phase 6 — Seamless local↔remote CLI
- `preloop context` (like `docker context`): `local` auto-boots `preloop-runner-server` on
  127.0.0.1 + smolvm; `cloud` targets the prod URL + `preloop login` token. Every
  subcommand hits the same `/api/v1/...` + NDJSON surface → identical local/remote.
  "Run remotely" = one context switch, zero workflow edits; label routing picks the host.
- Note: CLI currently hardcodes `SmolVmProvider::default()` (`preloop-cli/src/main.rs:378,689`).

---

## 5. Invariants & risks
- **No wire drift:** one server, injected policy — never a second control plane.
  Conformance suite stays authoritative and must pass with swapped impls.
- **Dependency direction:** OSS never depends up into cloud (CI-enforced, Phase 2).
- **Publish hygiene:** allowlist crates when mirroring; scrub secrets.
- **Trust:** OSS defaults must be genuinely good, or the community revolts.
- **Legal:** FSL is source-available (not OSI-"open source"); expect some purist
  pushback — correct tradeoff for the anti-SaaS goal.

## 6. Critical path
Phase 1 is the only hard prerequisite; it unblocks 4/5/6. Phases 2–3 run alongside it.
**Do not split repos (Phase 4) before the server accepts injected impls** — otherwise
the private repo will be tempted to fork core files (the one thing that forces
copybara/overlay hell). Within Phase 1, the ServerBuilder + Cache/Auth/Secrets seams
are quick wins; Scheduler and especially RunStore are the real work.
