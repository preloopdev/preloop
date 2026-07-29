# Turso's SQLite-Conformance Playbook — Deep Dive & aksh Adaptation

A study of how `tursodatabase/turso` achieves (and closes the gap toward) full
compatibility with official SQLite, and a concrete plan to port the techniques
that fit aksh's goal of being a faithful Rust reimplementation of the GitHub
Actions control plane + runner.

Primary sources (all read for this doc):

- `github.com/tursodatabase/turso` — README, `COMPAT.md`, `AGENTS.md`,
  `CONTRIBUTING.md`, `docs/agent-guides/testing.md`, `sqlite/conformance/Makefile`,
  `testing/differential-oracle/` tree.
- `turso.tech/blog/introducing-limbo-a-complete-rewrite-of-sqlite-in-rust` (Dec 2024)
- `turso.tech/blog/we-will-rewrite-sqlite-and-we-are-going-all-in` (Jan 2025)
- Keles, Chou, Goldstein, Lampropoulos. *DIRT: Database-Integrated Random
  Testing.* DBTest '26 (arXiv:2604.16373)

---

## 1. Why this is the right reference for aksh

Turso is **a from-scratch Rust rewrite of SQLite** (codename "Limbo"), not a
fork. Its stated bar: *"full compatibility [with SQLite] at both file and
language levels … is a requirement for 1.0,"* validated by *"differential
testing against SQLite and ongoing work to pass the full SQLite TCL test
suite."* (`COMPAT.md`)

aksh is structurally the same problem in a different domain:

| | Turso | aksh |
| --- | --- | --- |
| What it rewrites | SQLite (the engine) | GitHub Actions control plane + `actions/runner` |
| The reference oracle | official SQLite (`sqlite3`) | official `actions/runner` v2.335.1 + `ChristopherHX/runner.server` |
| Compatibility surface | SQL dialect, file format, C API, VDBE opcodes | runner protocol (`/_apis`, `/broker`, Twirp), workflow YAML semantics, GitHub Checks UX |
| Reference **source** open? | ✅ yes (SQLite is public domain) | ✅ yes (`actions/runner` is MIT; `ChristopherHX/runner.server` is MIT) |
| Reference **test suite** open? | ❌ **no** — SQLite's TH3/SLT suite is proprietary | ✅ **yes** — 82 C# test files / 842 L0 tests in `actions/runner` |
| Hard part | no access to the reference's tests → confidence from outside | hosted **control plane is closed** (GitHub's backend is not open) → protocol/server behavior has no official test suite |
| Language | Rust, workspace of crates | Rust, workspace of crates |

The last three rows are the crux, and they correct a common misframing. Turso's
blog says it plainly: *"SQLite's test suite is proprietary, meaning that it is
hard to achieve the confidence to make very large changes."* **aksh is NOT in
that boat on the runner side** — `actions/runner` ships its full L0 test suite
under MIT, and aksh has already extracted all 842 tests across 82 files
(`docs/test-coverage.md`, `docs/runner-test-compatibility-plan.md`), classified
each as FULL / PARTIAL / NOT_APPLICABLE / OUTSIDE_RUNNER, and **closed the
entire P0/P1 slice** with live verification against real GitHub Actions
(documented with run IDs). This is a conformance technique Turso *cannot* use,
and aksh should keep doing it as new runner versions land (the `runner-watch
diff` → spec pipeline is the natural hook).

Where the Turso parallel **does** hold — exactly — is the **control plane**.
The official GitHub Actions backend (the server `api.github.com` runs, the
broker, the scheduler, OIDC issuance, cache/artifact stores, the results
service) is a closed hosted service with no public source and no test suite.
`ChristopherHX/runner.server` is a community reimplementation, not the real
backend. So for *server-side / protocol / system* behavior, aksh is in Turso's
situation: the contract must be **discovered** by capturing real traffic (MITM)
and **enforced** by differential testing against the real hosted service. That
is the surface this doc is mostly about, and it is where Turso's playbook
transfers cleanly. (The runner-side L0 tests, by contrast, are a solved layer
aksh already owns.)

A second strategic lesson from Turso's story: they **forked first** (libSQL) and
found deep core contributions stayed rare because *"a fork, still tethered to
its origin and moving cautiously, wasn't bold enough."* The rewrite was what
unlocked momentum. aksh is already on the rewrite path, so the direction is
validated; the job now is to copy the **confidence machinery for the control
plane**, not the fork-vs-rewrite debate.

---

## 2. Turso at a glance: rewrite + virtual machine

Two architectural facts shape everything else:

1. **It's a rewrite, not a fork.** No back-merging from SQLite. Compatibility is
   enforced *outward* by testing, not *inward* by shared code.
2. **It's a virtual machine.** Like SQLite, Turso compiles SQL to bytecode for a
   "VDBE" and runs that bytecode. The VM is general enough that Postgres is now a
   *second frontend* compiling to the same VM — the "LLVM of databases" thesis.
   One core, many frontends; SQLite compatibility is the first and primary
   frontend.

The second fact is the source of Turso's single most powerful conformance
technique (§4.3): because both SQLite and Turso compile to the *same bytecode
IR*, you can compare the bytecode they generate for identical inputs and
localize bugs precisely. aksh's analog is the **protocol trace** (§8.3).

---

## 3. The compatibility contract: `COMPAT.md` as a first-class artifact

### 3.1 The layered compatibility model

`COMPAT.md` (1172 lines) tracks compatibility across **five distinct layers**,
each with its own status column:

1. **Query language** — every SQL statement, `PRAGMA`, expression syntax.
2. **SQL functions** — scalar, math, aggregate, date/time, JSON — each function
   individually.
3. **C API** — every `sqlite3_*` entry point, grouped by concern (connection,
   prepared statements, binding, results, errors, BLOB I/O, WAL, virtual
   tables, serialization, …).
4. **VDBE opcodes** — every bytecode opcode the VM implements.
5. **Extensions & journaling modes** — FTS, vector, UUID, regexp, WAL modes.

Status values are a fixed vocabulary: `✅ Yes`, `🚧 Partial` (with a comment),
`❌ No`, `Not Needed` (deprecated in SQLite), `NotNeeded` (Turso design choice).

### 3.2 The status-matrix discipline

Two rules make the matrix more than a checklist:

- **Every `🚧 Partial` carries an explanatory comment**, often a whole paragraph
  (see the "Same-connection write statements" section: a multi-paragraph
  explanation of *why* Turso returns `SQLITE_BUSY` for a second concurrent
  writer, what SQLite does instead, and the exact error semantics). The gap is
  not just recorded; the *reason and the migration path* are recorded.
- **"Any deviation from SQLite behavior that is not explicitly documented as an
  opt-in extension is considered a bug."** This is the load-bearing sentence.
  It converts the matrix from a wishlist into a contract: undocumented
  divergence is a defect, full stop.

### 3.3 Explicit, published guarantees

`COMPAT.md` opens with four guarantees — short, user-facing, unambiguous:

1. You should always be able to go back to SQLite if you want to.
2. You should be able to access a database created with SQLite in Turso.
3. You need to opt in to any incompatible Turso feature, but even then we
   provide a migration path back to SQLite when possible.
4. We don't support mixed SQLite and Turso in multi-process scenarios.

These are the compatibility *promise*; the matrix is the *evidence*. aksh's
`docs/fidelity-gap.md` has a "Product parity target" with three compatibility
bars (runner protocol, workflow semantics, GitHub integration) — good, but it
could be sharpened into Turso-style one-line guarantees (§8.1).

### 3.4 Turso-specific extensions are documented separately

When Turso adds something SQLite doesn't have (extra PRAGMAs, the `vector` /
`fts` / `uuid` extensions, `libsql_wal_*` API), it goes in a clearly labelled
"Turso-specific" section, never mixed into the SQLite-parity table. This keeps
"compatibility with SQLite" and "our additions" visually and contractually
separate — the same principle aksh's `fidelity-gap.md` §5 states ("upstream
truth + aksh projections; native `/api/v1` is an *additional* surface, never a
replacement for `/_apis`").

---

## 4. The testing pyramid — exactly how they do it

> **Scope note:** §4 is about Turso's techniques for the surface where it has no
> reference test suite (everything). aksh should apply these to its
> **control-plane** surface — the closed hosted backend — where it is in the same
> boat. aksh's **runner** surface already has a stronger technique Turso cannot
> use: direct extraction of the reference's own unit tests (§4.0). Read §4 as
> "what to do for the half of the system that has no official tests"; §4.0 is
> "what to keep doing for the half that does."

### 4.0 aksh-only: extracting the reference's own unit tests (a technique Turso cannot use)

Because `actions/runner` is MIT and ships its L0 test suite, aksh does something
Turso structurally cannot: it mines the official C# tests and mirrors them in
Rust. This is already in production and is aksh's strongest runner-side
conformance asset — it should be understood as a first-class part of the
conformance strategy, not a separate doc.

What exists today (`docs/test-coverage.md`, `docs/runner-test-compatibility-plan.md`):

- **842 official L0 tests** across **82 C# test files** extracted and
  inventoried (e.g. `StepsRunnerL0.cs`, `JobRunnerL0.cs`, `ExecutionContextL0.cs`,
  `ActionManagerL0.cs`, `ContainerOperationProviderL0.cs`, `SetOutputFileCommandL0.cs`,
  `IssueMatcherL0.cs`, …).
- Each test classified: **FULL / PARTIAL / GAP / NOT_APPLICABLE / OUTSIDE_RUNNER**
  with a reason per bucket (e.g. legacy manifest parser → NOT_APPLICABLE because
  aksh uses a single modern parser; `PromptManager` → NOT_APPLICABLE because
  aksh configure is non-interactive; `RunnerConfigUpdater` → NOT_APPLICABLE
  because aksh-runner does not self-update).
- **P0/P1 slice fully closed** and LIVE_VERIFIED against real GitHub Actions
  with run IDs (step execution, file commands/matchers, actions/manifests/
  composite, containers, expressions, listener/config, process/runtime,
  protocol/client DTO). P2 (DAP/background) and P3 (Windows service/self-update)
  are deliberately out of scope.
- **Three verification gates** per bucket: (1) live GitHub run with `aksh-runner`
  against real Actions, (2) local `aksh-conformance runner-e2e` against
  `aksh-runner-server`, (3) `cargo test -p aksh-runner --lib`.

This is the cleanest possible conformance signal: "we run the reference's own
tests, translated, and they pass — and the reference's CI confirms the same
behaviors on hosted runners." Turso has no equivalent because SQLite's tests are
closed.

**How to keep this healthy (the Turso-adjacent part):** the L0 suite moves with
each `actions/runner` release. `runner-watch diff` already shallow-clones two
tags and structural-diffs the C# source → `delta.json` → specs. The natural
extension is to **diff the test files too** (new/removed/changed `*L0.cs`), so
each release produces a "new L0 tests to mirror" worklist alongside the
"new protocol surfaces to implement" worklist. Right now `runner-watch diff`
focuses on `Runner.Listener/Worker/Common/Sdk` source, not the test tree —
adding the test tree to the diff would keep the §4.0 inventory evergreen without
manual re-extraction. This is a small, high-value addition that has no Turso
analog (there's nothing for Turso to extract).

Turso runs **seven distinct testing modalities**, each with a defined location
and use case. From `AGENTS.md` and `docs/agent-guides/testing.md`:

| Type | Location | Purpose |
| --- | --- | --- |
| `.sqltest` | `sqlite/conformance/sqlite-sqltests/` | SQL compatibility — **preferred for new tests** |
| TCL `.test` | `testing/` | Legacy SQLite compat (being phased out, mechanically converted) |
| Upstream golden | `sqlite/conformance/upstream/` | Imported SQLite golden tests, **frozen** |
| Rust integration | `tests/integration/` | API-level regressions, multi-conn orchestration, injected failures |
| Fuzz | `tests/fuzz/` | Minimized fuzz regressions + targeted edge cases |
| Simulator | `testing/simulator/`, `testing/concurrent-simulator/` | Deterministic concurrency + fault injection |
| Differential/stress | `testing/differential-oracle/`, `testing/stress/` | Differential + long-running stress |

Plus three external/auxiliary: **Antithesis** (system-level deterministic
hypervisor), **DIRT** (database-integrated random testing), and **TLA+** /
**Aristo** (formal-ish spec + intent annotations). Each below.

### 4.1 Shared-DSL differential conformance suite (`.sqltest`)

This is the backbone. A `.sqltest` is a declarative, backend-agnostic scenario:

```
@database :memory:
@query
SELECT 1 + 1;
@expected
2
```

The same corpus runs against **multiple backends** via a `sqltest` runner crate
(`testing/sqltest/`), controlled by the conformance `Makefile`:

- `--backend cli` — spawns `tursodb` as a subprocess.
- `--backend rust` — links the native Turso bindings in-process.
- `--backend js` — runs through the Node/WASM bindings.
- `--binary <path>` — point the runner at **any** binary, including the real
  `sqlite3`. The `test-sqlite` target does exactly this: `make test-sqlite`
  runs the *entire* `.sqltest` corpus against official `sqlite3`.
- `--cross-check-binary <path>` — differential mode: run against two binaries
  and compare.
- `--mvcc` — the same corpus also runs in MVCC mode, so conformance is asserted
  in both concurrency models.

The mechanism is the key idea: **one declarative corpus, many engines, diff the
outputs.** The corpus is the shared truth; each backend is an implementation
under test; the real `sqlite3` is one of those backends. Conformance = "the same
test passes against us and against the reference."

There is also a snapshot dimension: `CI=1 make -C sqlite/conformance run-rust`
enables snapshot tests (record-once-expected), while the default
`--snapshot-filter __never__` forces live differential comparison rather than
snapshot capture. Snapshot mode is for CI stability; differential mode is for
active development.

### 4.2 Upstream golden tests, frozen

`sqlite/conformance/upstream/` holds imported upstream SQLite golden tests.
`AGENTS.md` is explicit about the discipline:

> Do not modify these for Turso behavior changes; use them as fixed
> compatibility coverage, and only touch them for intentional upstream sync or
> harness maintenance.

This is the "fixed reference, never edit" half of differential testing: a
corpus you can't game because you've promised not to touch it.

### 4.3 Bytecode-level differential testing (`EXPLAIN`) — the signature technique

Because both SQLite and Turso compile to the VDBE, Turso can compare **the
bytecode itself**, not just query results. From the "rewrite" blog:

> we also routinely fuzz inputs, and then make sure that the generated bytecode
> is the same, for both Limbo and SQLite.

And from `AGENTS.md` core principles:

> **SQLite compatibility.** Compare bytecode with `EXPLAIN`

This is more powerful than result-diffing because it localizes the bug *before*
execution. If `EXPLAIN <query>` produces different opcodes in sqlite3 vs Turso,
the bug is in code generation (the translator/optimizer). If the opcodes are
identical but results differ, the bug is in the VM interpreter or storage. The
triage bisects the system at the IR boundary. (See §5 for the full methodology.)

### 4.4 Quick diff harness — `scripts/diff.sh`

A one-liner to compare `sqlite3` vs `tursodb` output for a single query:

```sh
scripts/diff.sh "SELECT ...";
```

This is the developer-facing tip of the differential pyramid: don't wait for the
full suite, diff one query in one second. The analogue for aksh is a
"diff this workflow's protocol trace against the golden capture" one-liner.

### 4.5 Deterministic Simulation Testing (in-process simulator + fault injection)

Turso's reliability bet, borrowed from TigerBeetle. From the intro blog:

> we are doing it with Deterministic Simulation Testing (DST) built-in from the
> get-go. We have both added DST facilities to the core of the database, and
> partnered with Antithesis.

Properties of the in-process simulator (`testing/simulator/`):

- **Deterministic by seed.** A run is fully reproducible from its seed; a caught
  bug can always be replayed with the same event ordering.
- **Fault injection.** I/O failures, crashes, reorders are injected into the
  simulated execution.
- **The I/O loop is replaced by simulated I/O** — which is both its strength
  (full determinism) and its blind spot (see §4.7).

DST is framed as *"writing our own simulator is akin to writing unit tests"* —
fast, cheap, experimental — with Antithesis as the integration-test layer on
top.

### 4.6 Whopper — concurrent DST

`testing/concurrent-simulator/` ("Whopper") is DST that performs **concurrent
query execution**, not just serial simulation. It prints progress as
`I/U/D/C` (Inserts/Updates/Deletes/integrity Checks) and runs in `fast` or
`chaos` modes:

```sh
./testing/concurrent-simulator/bin/run            # fast sanity
./testing/concurrent-simulator/bin/explore        # chaos exploration
SEED=1234 ./testing/concurrent-simulator/bin/run --mode chaos   # reproduce
```

Reproducibility is preserved via `SEED=`, so a chaotic concurrent run that finds
a bug can be replayed exactly.

### 4.7 Antithesis — system-level deterministic hypervisor (catches what the simulator can't)

The crucial limitation of in-process DST, stated in the intro blog:

> With DST, we believe we can achieve an even higher degree of robustness than
> SQLite … Antithesis does that by providing a deterministic hypervisor that
> runs many fuzzing threads in parallel, allowing us to quickly search the input
> space.

And the canonical example of *why* it's needed:

> they have already helped us find issues in our `io_uring` implementation under
> partial writes. Our own DST framework would not have caught this, since the
> actual I/O loop is replaced by the simulated I/O loop in testing. Partial
> writes are an extremely rare condition, and therefore hard to test in an
> automated fashion.

So the layering is explicit:

- **In-process simulator** → catches logic/ordering bugs, cheap, high volume,
  but **cannot** catch bugs in the real I/O loop because that loop is mocked.
- **Antithesis** → a deterministic hypervisor that runs the *real* binary with
  *real* I/O under a deterministic scheduler, so partial writes, real syscalls,
  real fsync semantics, real network reordering get exercised *and* stay
  reproducible.

This two-tier DST (own simulator + external hypervisor) is the answer to "how do
you test the parts your simulator assumes away?"

### 4.8 DIRT — database-integrated random testing (the most novel idea)

DIRT (DBTest '26 paper) is Turso's answer to a specific pain: off-the-shelf
random testers (SQLancer) generate inputs the system doesn't yet support,
producing ~96.5% false positives on an evolving DB and burying the real bugs.
The fix: **embed the random tester inside the DBMS** so it only generates valid
inputs and evolves with the implementation.

Three core ideas:

1. **Database-integrated.** The generator lives in-tree, sees the real
   implementation surface, and is updated in the same commits that add features.
   False positives drop "by construction" — the generator only emits what
   exists.

2. **Generation Actions (GA) — a DSL for *how* to test, not just *what*.**
   Instead of writing a universally-quantified property ("for all db, p, q:
   `SELECT (p AND q) ≡ SELECT (q AND p)`") and leaving *how* to generate
   satisfying inputs to a generic framework, the developer writes an imperative
   recipe:

   ```
   gen property db =
     t  ← pick db.tables
     c  ← pick t.columns
     v  ← genOf expression c.type
     p  := t.c = v
     q  ← gen expression (t, c)
     ! r1 := SELECT (p AND q)
     ! r2 := SELECT (q AND p)
     ! assert (r1 == r2)
   ```

   `←` binds generation, `:=` is let, `!` marks an interaction with the DB
   (query/assert/**fault injection**). The developer fixes the *how*, so the
   property is satisfiable by construction. This lets DB developers — not
   testing experts — write oracles tailored to the feature they just shipped.

3. **Shadow state, generation-by-execution.** A key-value shadow model of the DB
   is maintained *during generation* (not for differential comparison, just for
   tracking what exists). This avoids querying the DB to ask "what tables
   exist?" — which would assume those queries work, defeating the test. The
   shadow state also gives a canonical invariant: *the shadow state equals the
   DB at every step.*

DIRT reimplements SQLancer's oracles (PQS, NoREC, TLP) as GAs and adds new ones
("deleted rows should not be in the table", "UNION ALL preserves cardinality"),
including a **`ReopenDatabase` fault primitive** that closes and reopens
connections to probe persistent-state logic. Result: 23 confirmed unique bugs,
<1% false positives, vs SQLancer-SQLite's 96.5% FP / 1 bug. The paper's
headline: *"for rapidly evolving databases with many missing features,
database-integrated testing yields more actionable bugs."* That is precisely
aksh's phase of life.

### 4.9 Differential oracle — fuzzer + structured + property-based generation

`testing/differential-oracle/` is the concrete random-differential harness,
three crates:

- `sql_gen` — structured SQL generation.
- `sql_gen_macros` — proc macros for the generators.
- `sql_gen_prop` — property-based generation (proptest-style).
- `fuzzer` — drives generation, runs against both engines, compares.

This is the "fuzz random SQL, run on Turso + SQLite, diff" loop referenced in
the blog. It complements DIRT: DIRT is *integrated* (in-tree, low-FP, dev-driven
properties); the differential oracle is *external-style* (broad generation,
differential against the reference, catches things DIRT's curated generators
don't reach, e.g. exotic literals the paper notes DIRT missed).

### 4.10 Fuzzing + minimized regressions

`fuzz/` (libFuzzer-style targets) and `tests/fuzz/` (minimized regressions kept
as ordinary Rust tests). The workflow: fuzzer finds a crash → minimize → commit
the minimized case as a regression test. `CONTRIBUTING.md` also lists
`cargo-fuzz` over `parse_workflow`/expression lexer as a required fuzz surface
(in aksh's terms: parser + expression evaluator).

### 4.11 Sanitizer stress + unreliable-libc fault injection

Two more fault avenues in `CONTRIBUTING.md`:

- **ThreadSanitizer stress:** `cargo run -Zbuild-std ... -p turso_stress --
  --vfs syscall --nr-threads 4 --nr-iterations 1000` — for multi-threading bugs.
- **Unreliable libc:** build `testing/unreliable-libc` then
  `LD_PRELOAD=./testing/unreliable-libc/unreliable-libc.so cargo run -p
  turso_stress -- --nr-iterations 10000` — injects allocation/IO failures at the
  libc boundary without a hypervisor. A cheap, deterministic-ish fault layer
  below the simulator and above Antithesis.

So the fault-injection stack is itself layered: **unreable-libc LD_PRELOAD**
(cheap, no infra) → **in-process DST simulator** (deterministic, high volume) →
**Antithesis** (real I/O, system-level, reproducible).

### 4.12 TLA+ specs

`tlaplus/sqlite-tx` — a TLA+ specification of SQLite transaction semantics.
Formal modeling of the concurrent state machine that the property tests only
sample. For Turso that's the WAL/transaction protocol.

### 4.13 Aristo — intent annotations with verification

From `CONTRIBUTING.md`: `#[aristo::intent("...")]` annotations capture
invariants a refactor could silently break — properties *invisible from the
signature and not already guarded by a test*. Example on the WAL trait:

```rust
#[aristo::intent(
  "An append-only log that records page-level changes before they are \
   applied to the database, so a system crash can be recovered by \
   replaying the log.",
  verify = "neural", id = "wal_records_changes_before_apply",
)]
pub trait Wal: Debug + Send + Sync { ... }
```

Annotations with `verify = "neural"` are machine-checked by an agent; proofs land
in `.aristo/proofs/`. It's a mechanism for keeping "why this code is correct"
attached to the code, and checking that it *stays* correct across refactors.
Exotic, but directly targets the class of bug that neither tests nor types
catch: a refactor that preserves the signature and breaks the invariant.

---

## 5. The debugging methodology: EXPLAIN-first triage

`CONTRIBUTING.md` codifies a triage rule that's worth quoting whole:

> Turso aims towards SQLite compatibility. If you find a query that has
> different behavior than SQLite, the first step is to check what the generated
> bytecode looks like. … run `EXPLAIN <query>` in `sqlite3`, then in Turso.
> **If the bytecode is different, that's the bug — work towards fixing code
> generation. If the bytecode is the same, but query results are different,
> then the bug is somewhere in the virtual machine interpreter or storage
> layer.**

This is the IR-boundary bisect: the bytecode is the seam between "deciding what
to do" (codegen) and "doing it" (VM/storage). Diff at the seam first, then drill
the side that diverged. It turns a vague "results are wrong" into a precise
"codegen is wrong" or "execution is wrong" in one step.

---

## 6. Process and cultural rules

From `AGENTS.md` "Core Principles" and `CONTRIBUTING.md`:

1. **Correctness paramount.** *"Production DB, not a toy. Crash > corrupt."*
2. **Compare bytecode with `EXPLAIN`.** (The §5 rule.)
3. **Every change needs a test.** *"Must fail without change, pass with it."*
4. **Assert invariants.** *"Don't silently fail. Don't hedge with if-statements."*
5. **Own your regressions.** *"If tests fail after your change, they are your
   regressions. Debug them directly. Never stash/revert to 'check if they fail
   on main' — that wastes time and is categorically banned."*
6. **Validate your hypotheses.** *"If you suspect a given cause for a bug,
   validate it and provide incontrovertible evidence. NEVER make unearned
   assumptions."*
7. **Reproducers must use only user-facing APIs.** *"Manipulating DB internals
   to artificially trigger a condition, or asserting internal state, is a bad
   reproducer."* A reproducer must survive as a regression test after the fix.
8. **Test placement: narrowest existing harness that can express the bug.**
   Prefer extending an existing file/dir over creating a new one; don't invent
   new test formats.
9. **AI-agent contribution rules** (notable for this repo too): keep PRs small
   and focused; include regression tests that *fail without* the change;
   contribute in areas you understand; self-review for the "LLM tics" (removing
   comments, verbose new ones, over-elaborate tests).

aksh's `AGENTS.md` already has "Local CI is mandatory; run `just test-ci`;
dogfood the workflow." Turso's additions worth adopting verbatim: rules 3, 5, 6,
7 — especially the ban on stash/revert-to-check-main and the user-facing-API-only
reproducer rule.

---

## 7. The "shared core, multiple frontends" architecture principle

Turso's VM thesis — one rigorously-tested core, many frontends (SQLite dialect,
Postgres dialect) compiling to it — is the architectural move that *makes*
compatibility a frontend concern rather than a whole-engine concern. aksh's
`fidelity-gap.md` §5 already states the analog: *"Model the AzDO/runner protocol
as the source of truth in `aksh-gha-protocol`; layer aksh extras as read-model
projections / sidecars, never as replacements … native `/api/v1` REST is an
additional ergonomic surface served alongside the runner-compatible `_apis`
surface, both reading the same state."* So aksh is already aligned; the doc
reinforces it: **the runner protocol is the IR; everything else is a frontend or
a projection.**

---

## 8. Mapping to aksh — what exists, what to steal

### 8.0 Code-verified current state (read from the tree, not the docs)

Before mapping, here is what actually exists in code today (verified against the
workspace, not just `docs/`):

- **Protocol-trace IR already exists and is already diffed.** The analog of
  SQLite's VDBE program is `flows.jsonl` — NDJSON, one JSON object per HTTP
  exchange (`flow_index`, method/host/path, redacted headers, base64 + decoded
  JSON request/response bodies, SHA-256, timestamps, `duration_ms`). Goldens
  are captured by mitmproxy between the official `actions/runner` and GitHub
  (`runner-watch record-golden` → `experiments/mitm/bin/record-golden.sh`);
  aksh's side is captured by an axum middleware (`--record-flows` →
  `aksh-runner-server/src/recording.rs`). Both are diffed by
  `runner_watch::compare::render_report` (`crates/runner-watch/src/compare.rs`,
  ~785 LOC, a Rust port of `experiments/mitm/_compare.py`): per normalized
  endpoint — presence, count, mean/p50/p95 ms, status codes, redacted header
  key sets, unified body-JSON diff and body-*schema* diff for request and
  response. The gate (`runner-watch conform`) replays golden flows against aksh
  and fails on official-only endpoints, status mismatch, or schema mismatch.
  **This is not aspirational — it runs.** The gap is that it's a manual/replay
  tool over fixed scenarios, not the mandated first debugging step and not yet
  driven by generated workflows.
- **`aksh-conformance` is a real binary**, not just planned. Working
  subcommands: `expand-fixtures`, `golden` (workflow expansion vs JSON),
  `runner-e2e` (boot server+runner+client, submit, loop to terminal, emit
  verdict), `runner-diff` (render flow-diff report from existing captures),
  `compare-command`, and a hand-rolled `fuzz` (see below). **Stubs:** `record`
  and `replay` print "not yet implemented" — the real recording/replay live in
  `runner-watch` + the MITM scripts, not here.
- **Property tests are broad and use independent oracles.** 21 files, ~33
  `proptest!` blocks across the workspace (not just the 87 concurrency cases),
  with 8 checked-in `proptest-regressions/` dirs. Several already implement
  DIRT-style *independent reference oracles*: `oracle_should_run` (scheduler,
  `scheduling_tests.rs`), `oracle_escape_data`/`oracle_escape_property`
  (independent reimpl of `ActionCommand.EscapeDataMappings`, `commands.rs`),
  the field-by-field `AgentJobRequest` oracle (`azdo_tests.rs`), and
  `official_oracles.rs` (`p0_failure_conditions_oracle`). A live
  `.github/actions/tier2-property-oracle/` Action runs on GitHub-hosted
  runners so the **official runner itself is the oracle** for JS-action
  lifecycle and file-command contracts.
- **A real differential harness already exists — in Python.**
  `benchmarks/real-world/run-concurrency-property-probes.py` + a
  `concurrency-property-cases.json` corpus with `KNOWN_INVARIANTS`
  (`GH-GROUP-01`, `GH-SLOT-01`, `GH-SINGLE-01`, `GH-MAX-01`, `GH-FIFO-01`, …)
  and two modes: `--dry-run` (credential-free, runs in CI) and full
  differential (live GitHub vs aksh). This is a DIRT-adjacent artifact already
  in production use.
- **Fuzz is panic-only, not coverage-guided.** `aksh-conformance fuzz` is a
  hand-rolled LCG that emits skeletal YAML and only asserts `parse_workflow`
  doesn't `panic` (via `catch_unwind`). No `cargo-fuzz`/libfuzzer/AFL, no
  expression-evaluator fuzz, no minimized fuzz corpses.
- **No DST/sanitizers/formal methods in code.** No virtual clock, no
  deterministic scheduler, no fault injection, no TSan/ASan/Miri, no TLA+,
  no Aristo. Fixed proptest seeds (`20250713`, `0xAC710C0DE`, `0xA2D0_2026`,
  …) are the only determinism. All of §8.4–§8.10 is greenfield.
- **aksh is ahead of Turso on one axis: release-diff → implementation.**
  `runner-watch` runs a full `watch → diff → triage → implement → review →
  conform → pr` pipeline (shallow-clone two tags, structural C# source diff →
  `delta.json`, deterministic triage → `.runner-watch/specs/v2.335.1/*.toml`,
  generate implementation/review/PR prompts). The `claude`/`codex` agent steps
  are wired but have only run in `--no-agents`/`--dry-run` deterministic mode.
  Turso has no analog of this; it's an aksh original worth keeping, and the
  natural home for the §8.1 matrix (the specs already cite fidelity-gap rows).
- **And aksh is ahead of Turso on a second, bigger axis: it has the reference's
  own unit tests.** 842 official C# L0 tests across 82 files are extracted,
  classified FULL/PARTIAL/NOT_APPLICABLE/OUTSIDE_RUNNER, and the P0/P1 slice is
  closed and live-verified against real GitHub Actions
  (`docs/runner-test-compatibility-plan.md`). Turso cannot do this — SQLite's
  test suite is proprietary. This covers the **runner** surface. The Turso
  techniques in §4/§8 are therefore needed for the **control-plane** surface
  (the closed hosted backend), not the runner.

The table below maps each Turso technique to an aksh equivalent — **existing
(✅/🚧) or new (❌ to build)**. Rows are scoped to the control-plane surface
unless noted; the runner surface is largely solved by §4.0.

| Turso technique | Turso location | aksh equivalent today (code-verified) | aksh action |
| --- | --- | --- | --- |
| `COMPAT.md` granular status matrix (per-feature ✅/🚧/❌ + comments) | `COMPAT.md` | 🚧 `docs/fidelity-gap.md` — strong prose scorecard by layer, not a per-surface matrix | **Elevate to a per-endpoint / per-message-type / per-lifecycle-state matrix** with the fixed status vocabulary and mandatory comments on every `Partial` (§8.1) |
| "Undocumented divergence is a bug" contract | `COMPAT.md` opening | 🚧 implied by `fidelity-gap.md` "product parity target", not stated as a hard rule | State the one-liner explicitly in the conformance doc |
| 4 published guarantees | `COMPAT.md` Guarantees | 🚧 `fidelity-gap.md` 3 compatibility bars (richer but not one-liners) | Add Turso-style one-line guarantees (§8.1) |
| Shared-DSL differential suite (`.sqltest`) | `sqlite/conformance/sqlite-sqltests/` + `sqltest` runner | ✅ 24 scenario dirs (`experiments/mitm/scenarios/`), 23 golden captures (365 files) in `.runner-watch/golden/v2.335.1/` as `flows.jsonl`; `scenario.toml` driver recipe; `runner-watch conform` runs the gate | **Generalize** to a `.wftest`-style multi-backend runner (`--backend {aksh,official,replay}`, `--cross-check-binary`, `--concurrency-mode`); unstub `aksh-conformance record/replay`; reconcile the conformance rollup (§8.2) |
| Run same corpus against the reference engine | `make test-sqlite` (real `sqlite3`) | ✅ ad hoc: MITM `record.sh --backend official` + `scripts/compare-servers.sh` (smolVM: official vs GitHub vs aksh) + `runner-watch record-golden` | Make "run the whole corpus against the official runner.server + runner" one target, not an occasional experiment |
| Upstream golden, frozen | `sqlite/conformance/upstream/` (never edit) | ✅ `.runner-watch/golden/v2.335.1/` (365 files, mitmproxy-captured from real GitHub) | **Freeze it with the same rule:** never modify for aksh behavior changes; only upstream sync |
| Bytecode diff (`EXPLAIN`) — the signature technique | `AGENTS.md`, fuzz loop | ✅ **already exists** as `flows.jsonl` + `compare::render_report` (~785-LOC Rust port of `_compare.py`): per-normalized-endpoint presence/count/status/header-key/body-schema diffs | **Promote to the mandated first debugging step** (process change, near-zero code) + drive it with generated workflows as a fuzz oracle (§8.3) |
| Quick diff harness | `scripts/diff.sh "SQL"` | 🚧 `just conform` is heavier (full committed replay, not one query) | Add `just diff-trace <workflow>` one-liner over the existing `compare::render_report` |
| In-process DST simulator + fault injection | `testing/simulator/` | ❌ none — 87 concurrency property tests + 21 proptest files/~33 blocks, fixed seeds, but no virtual clock / scheduler / fault injection | **Build `aksh-sim`:** virtual clock, deterministic scheduler, simulated transport, seed-reproducible fault injection over the control-plane ↔ runner polling loop (§8.4) |
| Concurrent DST (Whopper) | `testing/concurrent-simulator/` | ❌ none | Extend `aksh-sim` to N concurrent runners + cancellations + renew races, `fast`/`chaos`, `SEED=` reproducible |
| Antithesis (real-IO deterministic hypervisor) | Antithesis + `Dockerfile.antithesis` | 🚧 non-deterministic only: MITM live captures + `compare-servers.sh` (smolVM) | Name the gap explicitly (simulator can't catch real-IO bugs); keep MITM as smoke; consider a deterministic hypervisor long-term (§8.5) |
| Unreliable-libc `LD_PRELOAD` fault injection | `testing/unreliable-libc/` | ❌ none | Cheap win: fault-injecting proxy/wrapper for the runner's HTTP + filesystem calls (§8.6) |
| **Extract the reference's own unit tests** (Turso *cannot* — SQLite tests are proprietary) | n/a | ✅ **842 C# L0 tests / 82 files extracted, classified, P0/P1 closed & live-verified** (`docs/runner-test-compatibility-plan.md`) | Keep evergreen: add the `*L0.cs` test tree to `runner-watch diff` so each runner release emits a "new tests to mirror" worklist (§4.0) |
| DIRT — integrated random testing + Generation Actions DSL | (paper; in-tree) | 🚧 runner L0 tests cover unit behavior but **not** protocol/concurrency/integration/system; **in-code independent oracles already** (`oracle_should_run`, `oracle_escape_data/property`, `AgentJobRequest` field oracle, `official_oracles.rs` p0 oracle); **Python differential seed-corpus harness** `run-concurrency-property-probes.py` with `KNOWN_INVARIANTS` + dry-run/live modes; **live `tier2-property-oracle` Action** using the official runner as oracle | **Highest-leverage port for the control-plane gap:** generalize the existing oracles + Python harness into an in-tree Rust generator that only emits supported features + a Generation Actions DSL for metamorphic properties (§8.7) |
| Differential oracle (fuzzer + structured + prop gen) | `testing/differential-oracle/` | 🚧 Python concurrency-probes harness is closest; `aksh-conformance fuzz` is a hand-rolled LCG panic-check (not coverage-guided); no expression fuzz | Random-workflow differential harness in Rust: generate → run on aksh + official → diff trace / final state (§8.8) |
| Fuzzing + minimized regressions | `fuzz/`, `tests/fuzz/` | 🚧 `aksh-conformance fuzz` panic-only hand-rolled; 8 `proptest-regressions/` dirs (proptest shrinks, not fuzz corpses); no cargo-fuzz/libfuzzer; no expression fuzz | Add coverage-guided libFuzzer targets for `aksh-gha-parser` + `aksh-gha-expressions`; commit minimized regressions as Rust tests |
| Sanitizer stress | `turso_stress` + TSan | ❌ none (no `just stress`, no sanitizer flags) | `just stress` running runner + server under TSan for multi-thread races |
| TLA+ specs | `tlaplus/sqlite-tx` | ❌ none (no `.tla`, no `tlaplus/`) | **Formalize the broker lease/renew/complete + concurrency-group + `needs`-DAG state machines in TLA+**; property tests then sample the spec (§8.9) |
| Aristo intent annotations | `#[aristo::intent(...)]` | ❌ none | Optional: annotate protocol-boundary invariants ("secrets masked at every egress", "messageId monotonic per session", "job never dispatched before needs complete") (§8.10) |
| EXPLAIN-first triage methodology | `CONTRIBUTING.md` | 🚧 `fidelity-gap.md` is descriptive; the trace-diff *mechanism* exists but isn't the mandated first step | **Adopt the bisect rule verbatim** in `CONTRIBUTING`/`AGENTS` (§8.3) — pure process/doc change on top of existing tooling |
| Process rules (test-first, own regressions, no stash-revert, user-API-only reproducers) | `AGENTS.md` Core Principles | 🚧 `AGENTS.md` has local-CI + dogfood; missing 3/5/6/7 | Adopt rules 3/5/6/7 from §6 |

### 8.1 Elevate `fidelity-gap.md` into a `COMPAT.md`-style matrix

Today `fidelity-gap.md` is a strong prose scorecard organized by layer. The
upgrade is mechanical but high-value:

- **Fixed status vocabulary:** `✅ Yes` / `🚧 Partial` (mandatory comment) / `❌
  No` / `N/A — local by design` (aksh's equivalent of Turso's "Not Needed").
- **Rows = observable contract surfaces**, not code modules: every `/_apis`
  endpoint, every broker message type (the 9 already listed), every Twirp
  results-service route, every `TimelineRecord` state transition, every
  workflow-command (`::set-output::` etc.), every `GITHUB_*` env var, every
  expression function, every matrix/needs/concurrency behavior.
- **Mandatory comment on every `Partial`**, in Turso style: what aksh does,
  what the official runner does, and the migration/reachability path.
- **The contract sentence at the top:** *"Any deviation from official
  `actions/runner` v2.335.1 behavior that is not explicitly documented as an
  opt-in aksh extension is considered a bug."*
- **Turso-style guarantees,** adapted:
  1. You can always point the unmodified official `actions/runner` at aksh.
  2. You can run your existing `.github/workflows/*.yml` unmodified.
  3. Any aksh-specific behavior is opt-in and ships with a path back to
     GitHub-hosted semantics when possible.
  4. aksh does not share a runner with GitHub-hosted Actions for the same job
     (no mixed control plane).

This makes the gap **measurable and machine-tractable** (a conformance gate can
read the matrix) instead of narrative.

### 8.2 Generalize the existing scenario system into a `.wftest` multi-backend runner

aksh already has the bones of the `.sqltest` idea — it just isn't unified.
Today there are three overlapping things: `experiments/mitm/scenarios/NN-name/`
(`scenario.toml` driver + `NN-name.yml` fixture), `.runner-watch/golden/v2.335.1/`
(`flows.jsonl` captures), and `runner-watch conform` (the gate). The
`aksh-conformance` binary has `runner-e2e` (live local run) and `runner-diff`
(report from captures) but its `record`/`replay` subcommands are stubs — the
real recording lives in the MITM scripts and `runner-watch record-golden`, and
the real replay lives in `runner-watch conform`'s `replay_flows_to_aksh`.

The upgrade is to unify these into one declarative corpus + one runner, in the
shape of Turso's conformance `Makefile`:

```
@workflow fixtures/workflows/multi-step.yml
@submit
@expect-trace golden/v2.335.1/06-multi-step
@expect-final-state
  jobs: [build, test]
  build.result: success
  test.result: success
```

Runner flags mirroring Turso's `--backend {cli,rust,js}` + `--binary` +
`--cross-check-binary` + `--mvcc`:

- `--backend aksh` — run against the aksh control plane (in-process or
  subprocess); uses the existing `--record-flows` middleware to capture.
- `--backend official` — run against a real `runner.server` + `actions/runner`
  (today done ad hoc by `record.sh --backend official` / `compare-servers.sh`).
- `--backend replay` — replay a golden capture (no execution; pure wire diff);
  this is what `runner-watch conform`'s `replay_flows_to_aksh` already does.
- `--binary <path>`, `--cross-check-binary <path>` — point at any
  control-plane/runner binary, incl. differential mode.
- `--concurrency-mode {legacy,broker,mvcc}` — the aksh analog of Turso's
  `--mvcc`: the same corpus must pass in every supported concurrency path.

Concretely: (a) fold `scenario.toml` + `flows.jsonl` + `run.json`/`jobs.json`
into one `.wftest` format carrying both `@expect-trace` and `@expect-final-state`
— aksh already has *two* oracle types (wire-trace goldens for 01/06–15, and
result-state goldens `run.json`/`jobs.json`/logs for 30–36/70–74) and the
unified format should carry both; (b) unstub `aksh-conformance record/replay`
by delegating to the existing `runner-watch`/MITM machinery; (c) add
`just conform-all` (whole corpus vs aksh) and `just conform-official` (record
vs the official stack) so "run everything against the reference" is one command.

Also: **reconcile the conformance rollup.** `docs/fidelity-gap.md` claims all 11
golden scenarios pass status+schema; the on-disk
`.runner-watch/conformance-report.md` currently shows 1 scenario matched. The
gate also documents caveats (skipped external-host flows, `oauth2/token` and
`messages` status checks excluded, cache/artifact endpoints 404 until backed).
A unified runner should regenerate the rollup across all golden scenarios and
report the real current number, not inherit a stale claim.

### 8.3 Protocol-trace diff is aksh's `EXPLAIN` — and it already exists

This is the most important correction from reviewing the code: **aksh's analog
of SQLite's VDBE bytecode already exists and is already being diffed.** There is
no bytecode, but there is a deterministic intermediate representation of "what
the system decided to do" — the **protocol trace**, materialized as `flows.jsonl`
(NDJSON, one record per HTTP exchange). The capture is symmetric:

- **Golden (reference):** mitmproxy between the official `actions/runner`
  v2.335.1 and `api.github.com` → `runner-watch record-golden` →
  `.runner-watch/golden/v2.335.1/<scenario>/flows.jsonl`.
- **aksh (under test):** `aksh-server serve --record-flows <path>` flips on
  `recording.rs::record_flows_middleware`, which writes the same NDJSON shape
  for every request.

The diff is `runner_watch::compare::render_report`
(`crates/runner-watch/src/compare.rs`, ~785 LOC, a Rust port of
`experiments/mitm/_compare.py`): it normalizes paths (strip `/runner/server`
prefix, random base segments, GUIDs → `{guid}`, numeric segments → `{n}`),
groups by `"METHOD normalized_path"`, and for each endpoint compares presence,
count, mean/p50/p95 ms, status codes, redacted header key sets, a unified
body-JSON diff, and — crucially — a body-**schema** diff that catches field
additions/omissions without value noise. The `runner-watch conform` gate replays
golden flows against aksh and fails on official-only endpoints, status
mismatch, or schema mismatch.

So the direct analog of the VDBE program holds:

- SQLite compiles a query to a VDBE program; Turso diffs the programs.
- aksh "compiles" a workflow to a protocol trace (`flows.jsonl`); aksh diffs
  the traces.

What's missing is **not the mechanism** but three things Turso does and aksh
doesn't yet:

1. **Make it the mandated first debugging step.** Turso's `CONTRIBUTING.md`
   codifies the IR-boundary bisect; aksh has the tool but not the rule. Adopt it
   verbatim in `CONTRIBUTING.md` / `AGENTS.md`:

   > If a workflow behaves differently than the official runner, the first step
   > is to diff the protocol trace (`just diff-trace <workflow>`, or
   > `runner-watch conform --scenario <s>`). **If the trace diverges, the bug is
   > in the control plane's decision/translation (parser/evaluator/scheduler/
   > broker). If the traces match but final state differs, the bug is in
   > execution, storage, or the NDJSON projection.**

   This bisects the system at the protocol boundary exactly the way `EXPLAIN`
   bisects at the bytecode boundary. Pure process/doc change on existing
   tooling — near-zero code.

2. **Add the one-liner.** `just diff-trace <workflow>` → run the workflow
   against aksh with `--record-flows`, capture the trace, diff against the
   golden trace via `compare::render_report`, print the first divergent
   exchange. aksh's `scripts/diff.sh` equivalent. The pieces — `runner-e2e
   --record-flows`, `runner-diff`, `compare::render_report` — already exist;
   this is a thin Justfile glue recipe.

3. **Drive it with generated workflows, not just the fixed scenarios.** Today
   the trace oracle only runs over the 23 hand-written golden scenarios. Turso
   fuzzes random SQL and asserts bytecode-equality. The aksh analog (§8.7/§8.8)
   is to generate random workflows, run them on aksh + the reference, and assert
   *normalized-trace equality + final-state equality*. The diff engine is
   already there; it just needs generated inputs and a second backend to compare
   against.

One refinement worth noting: aksh's `flows.jsonl` is whole-transport (HTTP),
grouped by normalized endpoint, and the `RunnerJobMessage`/`JobPlan` IR — the
in-process analog of "just the control-plane decision" — is *not* serialized
separately; it shows up as the broker `…/messages` response body and is diffed
there. If finer-grained localization is ever wanted, materializing
`RunnerJobMessage` to its own golden file (one per scenario, diffed field-by-field
like the existing `AgentJobRequest` oracle in `azdo_tests.rs`) would give a
tighter "codegen-level" oracle between the transport-level trace and the
result-state oracle.

### 8.4 A deterministic `aksh-sim` crate (port of the simulator + Whopper)

The control plane + runner polling loop is a textbook DST target: a scheduler,
long-poll message queues, lease/renew/complete timers, cancellations with
hard-kill timers, concurrency groups, and a `needs` DAG — all concurrent state
machines over (simulated) network and time. aksh already has 87 concurrency
property tests, but no **virtual clock** or **deterministic scheduler** with
seed-reproducible fault injection.

Proposed `aksh-sim`:

- **Virtual clock + deterministic executor.** All timers (lease expiry,
  cancel timeout−15s hard-kill, 500ms step-status drain, long-poll timeouts)
  drive off a virtual clock; the scheduler picks the next event deterministically
  from a seed.
- **Simulated transport.** Replace the real HTTP layer between runner and
  control plane with a simulated channel that can reorder, drop, duplicate, and
  stall messages — reproducibly.
- **Fault primitives** (DIRT-style `ReopenDatabase` ports directly): `Restart`,
  `NetworkPartition`, `DropMessage`, `Reorder`, `Duplicate`, `SlowClock`,
  `CrashRunner`, `RebootServer`.
- **Whopper-style concurrency:** N runners, matrix fan-out, concurrent
  cancellations, renew races, `fast`/`chaos` modes, `SEED=` reproducibility.
- **Invariants asserted every step:** `needs` DAG never dispatches before deps;
  `messageId` monotonic per session; a cancelled job settles to `cancelled`;
  no job is both complete and in-flight; concurrency group has ≤1 holder
  (single) or ≤max pending (max).

This is the single biggest reliability investment aksh could make, and it's
cheaper than it sounds because the protocol is already a pull-based queue — the
seams for simulation already exist.

### 8.5 The Antithesis gap — name it explicitly

aksh should write down Turso's caveat as its own: *the in-process simulator
replaces the real IO loop with simulated IO, so it cannot catch bugs in the real
runner↔aksh IO path* — partial HTTP writes, real TLS handshake races, real
fsync/durability on the log/artifact stores, real process-supervision crashes.
Today aksh's MITM live-capture reports are a *non-deterministic* version of this
probe. The recommendation: keep MITM captures as smoke coverage, and treat a
deterministic hypervisor (Antithesis or equivalent) as the long-term tier-3
fault layer. Don't pretend the simulator covers real IO — Turso learned this the
hard way with `io_uring` partial writes.

### 8.6 `LD_PRELOAD`/proxy fault injection (port of unreliable-libc)

A cheap middle tier between the simulator and a hypervisor: a fault-injecting
wrapper around the runner's outbound HTTP calls and filesystem ops (log writes,
artifact uploads, cache commits). Inject failed `write`/`fsync`/`connect`/`read`
at the boundary, deterministically by seed. This catches durability and
retry-path bugs without any hypervisor and complements `aksh-sim` (which mocks
the transport entirely).

### 8.7 DIRT-for-aksh — integrated random workflow testing (highest leverage)

This is the most directly applicable idea for the **control-plane** surface —
the half of aksh that has no official test suite (the hosted backend is closed).
The runner half is already covered by the extracted L0 tests (§4.0). DIRT fills
the gap the L0 tests structurally cannot: protocol sequences, concurrency
races, broker lease/renew/complete interleavings, `needs`-DAG scheduling under
cancellation, and system behavior across server restarts — exactly the
"evolving system with many missing features" regime where DIRT crushed
SQLancer (23 bugs, <1% FP vs 96.5% FP). An external workflow fuzzer would
generate workflows aksh can't parse yet and drown the signal in false positives.
An **in-tree** generator that only emits supported features avoids that by
construction.

The seed already exists in code: independent reference oracles
(`oracle_should_run`, `oracle_escape_data`/`oracle_escape_property`, the
field-by-field `AgentJobRequest` oracle, `official_oracles.rs`'s
`p0_failure_conditions_oracle`), a Python differential seed-corpus harness
(`benchmarks/real-world/run-concurrency-property-probes.py` with
`KNOWN_INVARIANTS` and dry-run/live-GitHub modes), and a live
`tier2-property-oracle` Action that uses the official runner as the oracle.
DIRT-for-aksh is the generalization: lift these into one in-tree Rust generator
with a property DSL, so feature authors write the oracle in the same commit as
the feature.

Proposed `aksh-dirt`:

- **In-tree workflow + integration generator** that reads the actually-supported
  surface (from the §8.1 matrix) and emits only valid workflows: supported
  triggers, supported `uses:` actions, in-scope matrix dimensions, `needs`
  graphs within depth limits. Updated in the same commit that adds a feature.
- **Generation Actions DSL** for aksh metamorphic properties, e.g.:

  ```
  gen property wf =
    dag ← gen_dag max_depth=4
    ! submit wf(dag)
    ! trace1 := capture_trace()
    ! cancel_random_job(dag)
    ! settle()
    ! assert (every cancelled job ⇒ state == cancelled)
    ! assert (no job dispatched before its needs)
  ```

  and differential GAs:

  ```
  gen property wf =
    w ← gen_workflow supported_only=true
    ! a := run aksh(w)
    ! o := run official(w)   # or golden
    ! assert (normalize(a.trace) == normalize(o.trace))
    ! assert (a.final_state == o.final_state)
  ```

- **Shadow state** (DIRT's key trick): maintain a model of runs/jobs/steps
  *during generation* so the generator never has to ask the server "what jobs
  exist?" (which would assume that query works). The shadow state doubles as a
  canonical invariant: *shadow == server at every step*.
- **Fault primitives in the DSL:** `Cancel`, `RebootServer`, `DropMessage`,
  `ReopenConnection` — the last directly porting DIRT's `ReopenDatabase`, which
  found Turso's WAL/header persistence bug. The aksh analog: persistent
  run/job state across a server restart.
- **Oracles to seed it with** (some already implied by existing property tests):
  - Re-running the same workflow yields the same job DAG and matrix expansion.
  - A cancelled job settles to `cancelled`; its `needs`-descendants settle
    correctly (skipped/cancelled per GitHub's rules).
  - Secrets never appear unmasked in any log, NDJSON event, or stored timeline
    record (assert over the full egress surface, not just one channel).
  - `needs` DAG: a job is never dispatched before all its dependencies reach a
    terminal state; `success()/failure()/cancelled()/always()` reflect real
    dependency results.
  - Concurrency: ≤1 holder (single) / ≤max pending (max); `cancel-in-progress`
    actually cancels the in-flight holder.
  - `messageId` monotonic per session; redeliver-until-ack holds.
  - Differential: normalized protocol trace + final state match the official
    runner for the same workflow.

The GA DSL is the force-multiplier: it lets the people who *build* aksh features
write the properties for *their* feature in the same change, the way DIRT's
`ReopenDatabase` primitive was written by the developer who reported the header
bug.

### 8.8 Differential oracle harness (port of `testing/differential-oracle/`)

Complementary to DIRT: a broader, less-curated random generator (`wf_gen` +
`wf_gen_prop`) that runs against both aksh and the reference and diffs —
catching inputs DIRT's supported-only generator won't reach (exotic YAML,
malformed expressions, unusual `uses:`). This is the structured/property-based
fuzz loop the blog describes ("fuzz inputs, then make sure the generated
bytecode is the same"). For aksh the oracle is: *normalized protocol trace
equality + final state equality*, not bytecode equality.

### 8.9 TLA+ for the concurrent state machines

aksh's broker lease/renew/complete lifecycle, concurrency groups
(`cancel-in-progress`, `single`/`max` queue), and `needs` DAG scheduling are
exactly the concurrent protocols Turso models in `tlaplus/sqlite-tx`. The 87
concurrency property tests *sample* these state machines; TLA+ would *specify*
them, so the property tests can be checked against the spec and edge cases
(diamond `needs` + matrix + cancel-in-progress + a mid-flight server reboot)
can be enumerated rather than hoped-for. Highest-value spec targets: the broker
job lease state machine, and concurrency-group acquire/cancel/queue.

### 8.10 Aristo-style intent annotations (optional, lower priority)

The protocol boundary has precisely the invariants Aristo targets — invisible
from signatures, not always guarded by a test, and exactly what a refactor
silently breaks:

- "Secret values are masked at every egress point (logs, NDJSON, timeline,
  stored artifacts)."
- "`messageId` is monotonic per session and messages are redelivered until
  acked."
- "A job is never dispatched before all its `needs` reach a terminal state."
- "The NDJSON feed is a pure projection of timeline + completion state."

Annotating these on the relevant traits/functions and machine-verifying them is
a heavier lift (Aristo is exotic), but it's the right *shape* of defense for
this codebase. File as a later-phase experiment.

---

## 9. Prioritized adoption plan

**P0 — process/doc changes on existing tooling (near-zero new code):**
1. Adopt the trace-diff triage rule (§8.3) in `CONTRIBUTING.md` / `AGENTS.md`.
   The `compare::render_report` mechanism already exists; this is making it the
   mandated first step.
2. Add `just diff-trace <workflow>` one-liner (§8.3) gluing `runner-e2e
   --record-flows` + `compare::render_report`.
3. Freeze `.runner-watch/golden/v2.335.1/` with Turso's "never edit for aksh
   behavior" rule (§4.2).
4. Reconcile the conformance rollup: regenerate
   `.runner-watch/conformance-report.md` across all golden scenarios and report
   the real current pass count instead of the stale "11 pass" / "1 matched"
   discrepancy (§8.2).
5. Elevate `fidelity-gap.md` into a `COMPAT.md`-style matrix with the status
   vocabulary, mandatory comments, the contract sentence, and the 4 guarantees
   (§8.1). This is the artifact that makes the gap measurable.

**P1 — the structural ports:**
6. Unify the scenario system into a `.wftest` multi-backend runner
   (`--backend {aksh,official,replay}`, `--cross-check-binary`,
   `--concurrency-mode`), folding in the 24 existing scenario dirs, the golden
   captures, and the `aksh-conformance record/replay` stubs (§8.2). Make "run
   the whole corpus against the official runner" one target.
7. Build `aksh-sim` (virtual clock + deterministic scheduler + simulated
   transport + fault primitives + Whopper-style concurrent mode), asserting the
   invariants in §8.4 every step. Seed-reproducible. (Greenfield — no virtual
   clock exists today.)
8. Build `aksh-dirt` (in-tree supported-only generator + Generation Actions DSL
   + shadow state + fault primitives), generalizing the existing in-code
   oracles + the Python concurrency-probes harness (§8.7). Highest-leverage new
   investment.

**P2 — depth and formalization:**
9. Differential oracle harness in Rust (`wf_gen` + `wf_gen_prop` +
   coverage-guided `fuzzer`) driving the existing `compare::render_report` with
   generated workflows (§8.8).
10. `LD_PRELOAD`/proxy fault injection for the real runner IO path (§8.6); name
    the Antithesis gap explicitly (§8.5).
11. Coverage-guided libFuzzer targets for `aksh-gha-parser` +
    `aksh-gha-expressions` (replacing the panic-only hand-rolled `fuzz`);
    commit minimized regressions as Rust tests (§4.10). `just stress` under TSan.
12. TLA+ specs for the broker lease and concurrency-group state machines
    (§8.9); cross-check the 87 concurrency property tests against the spec.

**P3 — experimental:**
13. Aristo intent annotations on protocol-boundary invariants (§8.10).
14. Antithesis (or equivalent deterministic hypervisor) as the tier-3 real-IO
    fault layer, once the simulator and DIRT are mature.

---

## 10. The one-sentence takeaway

Turso buys SQLite-level confidence without SQLite's test suite by (a) publishing
a per-feature compatibility contract where undocumented divergence is a bug,
(b) making the shared IR — bytecode for them, **`flows.jsonl` protocol traces
for aksh** — the differential oracle you compare against the reference at every
level from a one-liner `diff` to a fuzz loop, and (c) layering fault finding
from cheap in-process DST up through an external deterministic hypervisor, with
an **in-tree** random tester (DIRT) that evolves with the implementation so
false positives stay near zero while the system is still missing features.

The crucial scoping correction from reviewing aksh's code: this playbook
transfers **to the control plane, not the runner**. aksh's runner surface is
already covered by a technique Turso structurally cannot use — direct extraction
of the reference's own 842 MIT-licensed L0 tests, with the P0/P1 slice closed
and live-verified. The closed hosted GitHub backend has no test suite, and that
is where Turso's playbook applies: the protocol-trace oracle and diff engine
**already exist and run** (`runner-watch conform` + `compare::render_report` +
the `--record-flows` middleware), and independent oracles + a Python
differential harness are already in use. The real work is to (a) sharpen the
contract into a `COMPAT.md`-style matrix, (b) promote the existing trace diff to
the mandated first debugging step and drive it with generated workflows, (c)
keep the L0-test inventory evergreen by adding the test tree to `runner-watch
diff`, and (d) build the two layers aksh genuinely lacks — a deterministic
simulator and an in-tree DIRT generator — on top of the oracles that are already
there, scoped to the control-plane behavior the L0 tests don't reach.
