# ADR: YAML source spans in the workflow parser

Status: **proposed** for the span work (§2–§5). The three independent bugs
this investigation turned up (§6.4) are **fixed and landed**; they needed no
span infrastructure.
Date: 2026-09-12 (fixes and official-runner verification: 2026-09-13)
Scope: `preloop-gha-parser`, `preloop-gha-protocol` (`azdo`), `preloop-runner-server` (`runs.rs` fileTable), `runner-watch` (conformance gate)

Verified against the **official runner v2.337.0** source (latest release,
commit `397b032`, extracted to `/tmp/runner-v2.337.0`). Note `versions.toml`
still pins `runner_version = "2.336.0"`; bumping that pin is a separate change
with golden-recapture consequences and is deliberately not part of this work.

---

## 0. Corrections to the framing

Three claims in the task brief do not survive contact with the tree. They are
corrected here because the plan's phasing depends on where the synthetic
coordinates actually live.

| Claim | Reality |
|---|---|
| `crates/preloop-gha-parser/src/expand.rs::defaults_run_token` | **No such symbol.** `grep 'defaults_run_token'` over `crates/` returns nothing. The `line:1,col:1` synthetic coordinates are in `job_builder.rs` (see §1.3). `AgentJobRequestMessage.defaults` is shipped as `Vec::new()` — `job_builder.rs:637`. |
| `azdo/job.rs` timeoutInMinutes serializer emits `file:1,line:0,col:0` | **No.** `TaskStep.timeout_in_minutes` is `Option<u32>` (`azdo/job.rs:319`) and is serialized as a bare JSON number (`azdo/job.rs:385`). The `file:1,line:0,col:0` literal is on **`continueOnError`** (`azdo/job.rs:372-380`). The `timeoutInMinutes` gap is *worse* than described: GitHub sends a **type-6 TemplateToken** there and we send a scalar (§1.4). |
| Real capture at `crates/preloop-runner/src/worker/job_extension_tests.rs:1507` | **Line does not exist** — that file is 1422 lines, and `"num"` appears nowhere under `crates/`. The token shape quoted in the brief is nevertheless **real**; it lives in the goldens. Verified: `.runner-watch/golden/v2.336.0/103-composite-nested-post/flows.jsonl:23`, acquirejob response, `.steps[0].timeoutInMinutes` = `{"type":6,"file":1,"line":8,"col":26,"num":1}`. |
| `fileTable` is shipped empty | Half true. `job_builder.rs:636` sets `file_table: Vec::new()`, but `runs.rs:2116` overwrites it: `agent_msg.file_table = vec![workflow_path.to_owned()]`. So we ship exactly one entry, always the *caller* workflow path (§1.5). |

Everything below carries a `path:line` citation for current behaviour. Where I
could not verify, §6 says so explicitly.

---

## 1. MAP — where positions are lost

### 1.1 The exact death point

```
crates/preloop-gha-parser/src/yaml.rs:6-16
  6: pub fn parse_workflow(input: &str) -> Result<Workflow, ParserError> {
  7:     let mut value: serde_yaml::Value = serde_yaml::from_str(input)?;   // positions already gone
  8:     normalize_yaml_keys(&mut value);
  9:     let workflow: Workflow = serde_yaml::from_value(value)?;
 10:     workflow.on.validate_event_names()?;
 12:     ... ParserError::EmptyJobs
 14:     crate::eval::validate_workflow_expressions(&workflow)?;
```

`serde_yaml::Value` (0.9) is a position-free tree; marks are lost at line 7,
before the typed model at line 9 ever exists. `parse_action_metadata`
(`yaml.rs:19-23`) is the same shape.

Two structural obstacles sit in this function:

- `normalize_yaml_keys` (`yaml.rs:25-50`) **rebuilds mapping entries** — it
  removes `Value::Bool(true)` and reinserts it as `"on"` (`yaml.rs:28-29`), and
  restringifies every non-string key (`yaml.rs:31-38`). Any span carrier must
  survive that rewrite or the rewrite must move into serde attributes. Note
  `Workflow::on` already carries `alias = "true"` (`models.rs:167`), so the
  `true`-key path is partly redundant today.
- `MatrixValue`'s hand-written `Deserialize` (`models.rs:833-858`) reaches for
  `serde_yaml::Value` *inside* the deserializer and calls `serde_yaml::from_value`
  at `models.rs:854`. Any parser swap must replace this, and any span scheme
  must decide what a span means for the synthesised `Sequence` it fabricates at
  `models.rs:847-850`.

### 1.2 Structs that would need to carry (or be joined to) a span

All in `crates/preloop-gha-parser/src/models.rs`:

| Struct / enum | Line | Notes |
|---|---|---|
| `Workflow` | 159-180 | root; `jobs: IndexMap<String, Job>` at 179 — job **ids** need spans too (rename, go-to-def on `needs:`) |
| `Trigger` | 468-475 | `#[serde(untagged)]` |
| `WorkflowCallTrigger` | 324-334 | |
| `InputDefinition` / `SecretDefinition` / `OutputDefinition` | 338-352 / 398-405 / 409-415 | reusable-workflow surface |
| `Env` | 546-554 | `#[serde(untagged)]` |
| `EnvValue` | 574-583 | `#[serde(untagged)]` |
| `JobContinueOnError` | 589-594 | `#[serde(untagged)]` |
| `Job` | 626-682 | 18 fields; `steps: Vec<Step>` at 660 |
| `JobDefaults` / `DefaultsRun` | 686-690 / 694-701 | |
| `RunsOn` | 706-713 | `#[serde(untagged)]` |
| `Needs` | 758-766 | `#[serde(untagged)]`; the go-to-def target |
| `Strategy` | 780-790 | |
| `DeferredBool` / `DeferredNumber` | 795-800 / 805-810 | `#[serde(untagged)]` |
| `MatrixValue` | 814-819 + manual `Deserialize` 833-858 | |
| `Concurrency` | 873-880 + manual `Deserialize` 882-945 | inner untagged enums at 896-908 |
| `Matrix` | 949-959 | `#[serde(flatten)] axes` at 957 |
| `Step` | 963-994 | **the scanner's primary target**: `run` 972, `with` 981, `env` 978, `if_condition` 984 |
| `ActionMetadata` / `ActionInput` / `ActionOutput` / `ActionRuns` | 998-1012 / 1016-1026 / 1030-1037 / 1042-1093 | `action.yml` path |

**Ten untagged/flatten sites.** This is the single most important constraint on
the parser choice (§2) and the representation choice (§3).

### 1.3 Where the model becomes a plan (positions never existed here)

`crates/preloop-gha-parser/src/expand.rs`:

- `expand_jobs` — 431-463. Matrix loop at 439-458; one `JobPlan` per cell.
- `job_plan_from_job` — 466-560. `JobPlan { .. }` literal at **518-559**. Steps
  are cloned per cell: `job.steps.iter().cloned().map(step_plan)` at **511-516**
  — one YAML step becomes N `StepPlan`s, N = matrix cell count.
- `step_plan` — 1176-1219. `StepPlan { .. }` at **1196-1218**. Job defaults are
  folded in here (`working_directory` 1184-1189, `shell` 1190-1195), so a
  `StepPlan.shell` value may originate at `jobs.<id>.defaults.run.shell`, not at
  the step — the span must record *which*.
- Reusable caller placeholder — `JobPlan { .. }` at **820-880**;
  `workflow_file: Some(path.clone())` at 845.
- `expand_reusable_call` — 1048-1138. Callee plans are rewritten in place:
  id/base_id/needs prefixing 1084-1095, env/inputs/secrets merge 1099-1102,
  `workflow_file` set at **1103**, `if:` conjoined at 1107-1110 (via
  `merge_job_conditions`, 1152-1162 — produces `(outer) && (inner)`, a string
  with **no** source anywhere), name prefixed at 1119.

Target structs (`crates/preloop-gha-protocol/src/lib.rs`):

- `JobPlan` — **367-479**. Already carries provenance fields that a span scheme
  can lean on: `base_id` 371, `matrix_index` 390, `matrix_total` 395,
  `workflow_file` 431, `workflow_ref` 434, `workflow_sha` 437,
  `workflow_repository` 440.
- `StepPlan` — **546-576**. Carries **no** identity at all: no index, no origin.
  This is the gap that blocks per-step span resolution after fan-out.
- `ReusableCallPlan` — 484-499 (`workflow_file` 488, `depth` 498).

### 1.4 Wire token emission (all coordinates synthetic)

`crates/preloop-gha-parser/src/job_builder.rs`:

| Site | Emitted |
|---|---|
| `job_outputs_token` 27-67 | `file:1,line:1,col:1` at 41-43, 48-50, 55, 62-64 |
| `template_string_token` 69-125 | `location()` closure at 70; `file:1,line:1,col:1` at 77-79 and 120-122 |
| `template_token` 127-184 | `location()` at 128; 137-139, 148-150, 158-160 |
| `build_task_step` 844-945 | `displayNameToken` at 882-890: `{"type":1,"lit":n,"col":0,"file":0,"line":0}` |
| message assembly 615-661 | `file_table: Vec::new()` at 636, `defaults: Vec::new()` at 637 |

`crates/preloop-gha-protocol/src/azdo/job.rs`:

| Site | Emitted |
|---|---|
| `TaskStep::serialize` 322-387 | `continueOnError` `{"type":5,"file":1,"line":0,"col":0,"bool":…}` at 372-380; `timeoutInMinutes` as a **bare number** at 385 |
| `TemplateStringMap::serialize` 578-612 | `col:0,file:1,line:0` at 593-597, gated on a `with_loc` bool (call sites `job.rs:351`, `job.rs:358`) |
| `TemplateStringMapPair::serialize` 620-642 | overwrites `file:1,line:0,col:0` at 628-631; key token at 632-636 |

**Ground truth from a real GitHub capture** (I read these values directly out of
the golden, they are not from memory):
`.runner-watch/golden/v2.336.0/103-composite-nested-post/flows.jsonl:23`,
acquirejob response body:

```json
"displayNameToken": {"type": 0, "file": 1, "line": 8, "col": 15, "lit": "Run local composite action"}
".steps[0].timeoutInMinutes": {"type": 6, "file": 1, "line": 8,  "col": 26, "num": 1}
".steps[4].continueOnError":  {"type": 5, "file": 1, "line": 30, "col": 28, "bool": true}
"fileTable": [".github/workflows/103-composite-nested-post.yml"]
```

Census over the 12 v2.336.0 goldens that contain a job message (token = object
with `type` + one of `lit|expr|map|seq|bool|num`):

| shape | count |
|---|---|
| type 0 (`lit`) **with** file/line/col | 149 |
| type 0 (`lit`) **without** coordinates | 40 |
| type 2 (`map`) without coordinates | 40 |
| type 2 (`map`) with coordinates | 20 |
| type 3 (`expr`) with coordinates | 4 |
| type 1 (`seq`) with coordinates | 2 |
| type 6 (`num`) with coordinates | 1 |
| type 5 (`bool`) with coordinates | 1 |

`displayNameToken`: **43/43 are `type: 0`** with `file: 1` and a real line. We
emit `type: 1` (`job_builder.rs:884`). That is an independent fidelity bug,
discovered while gathering evidence for this ADR, and it is *not* fixed by spans.

### 1.5 fileTable

- Field: `azdo/job.rs:59-61`, `#[serde(rename="fileTable", default)] pub file_table: Vec<String>`.
- Serialized unconditionally: `azdo/azdo_tests.rs:269` (`object.insert("fileTable", json!(job.file_table))`) mirrors the production serializer.
- Populated **only** at `runs.rs:2116` — `agent_msg.file_table = vec![workflow_path.to_owned()]`, where `workflow_path` is the `&str` parameter of `build_job_artifacts` (`runs.rs:1886-1899`, param at 1890).
- Consequence: for a reusable-workflow callee job, `JobPlan.workflow_file`
  (set at `expand.rs:1103`) is **ignored**; every token in that job claims
  `file: 1` = the caller's path. Real GitHub indexes callee-origin values into
  additional fileTable slots. This is the "fileTable indices matter" case in the
  brief, and it is already broken independent of spans.

### 1.6 Errors

`ParserError` — `models.rs:13-155`. **No variant carries a line, column, or byte
offset.** The YAML variant wraps the upstream error opaquely
(`models.rs:16`, `Yaml(#[from] serde_yaml::Error)`); `serde_yaml::Error` does
have a `location()`, but nothing in the tree calls it — `grep 'ParserError::Yaml'`
over `crates/` matches only the definition.

`validate_workflow_expressions` — `eval.rs:320-514`. Walk order: `run_name` 321,
workflow `env` 325, workflow `concurrency` 328-335, then per job (337): matrix
338, fail-fast 343, max-parallel 348, name 355, runs-on 359-377, `if` 378,
env 383, concurrency 386-400, continue-on-error 401, container 407,
services 412-426, defaults.run 427-450, outputs 451-457, then per step (459):
name 465, `if` 470, continue-on-error 477, env 488, `with` 491, `run` 496,
working-directory 501. Every arm builds a string:
`ParserError::InvalidExpression(format!("job \`{job_id}\` {step_ref} run: {e}"))`
(`eval.rs:498`), where `step_ref` is `step \`name\`` or `step #<idx>`
(`eval.rs:460-464`). Structured position: none.

Existing tests assert only on substrings of that message
(`lib_tests.rs:1949-1952`, `1997-2000`, `2027-2030`), so enriching the error is
cheap.

**Expression-internal offsets**: `preloop-gha-expressions` has a byte cursor in
its lexer (`lexer.rs:32`, `offset`, advanced at `lexer.rs:187`) but it is private
and never surfaced — `ExpressionError` and the AST carry no offsets. A precise
`${{ }}` column therefore needs *two* pieces: the span of the enclosing YAML
scalar (new) **and** an offset inside the expression string (also new, in the
expressions crate). Phase 1 can ship scalar-level precision only.

---

## 2. PARSER CHOICE

`serde_yaml = "0.9"` at workspace root `Cargo.toml:62`; used by 7 crates
(`preloop-gha-parser`, `preloop-runner-server`, `preloop-runner`, `preloop-cli`,
`preloop-conformance`, `preloop-orchestrator`, and referenced in
`preloop-gha-expressions` comments). It is unmaintained/deprecated; replacement
is forced regardless of spans.

Facts below are from docs.rs as read on 2026-09-12, not from memory.

| Crate | Version read | serde derive? | Per-value line/col? | Verdict |
|---|---|---|---|---|
| **`serde-saphyr`** | 1.2.0 | Yes, direct-to-type (no intermediate `Value`) | **Yes** — `Spanned<T>` with `referenced`/`defined` `Location`, 1-indexed line/column, plus byte offsets for `from_str`/`from_slice` | **Recommended** |
| `serde_yaml_ng` | 0.10.0 | Yes (drop-in serde_yaml fork) | **No.** Public surface lists `Location` only as "the input location that an error occurred" | Fallback if §2's risk is unacceptable |
| `serde_norway` | 0.9.42 | Yes (drop-in serde_yaml fork) | **No** — identical surface to `serde_yaml_ng`, error `Location` only | Fallback |
| `marked-yaml` | 0.8.0 | Yes, plus its own `Spanned<T>` and `Marker` | **Yes** | Rejected — see below |
| `saphyr` | 0.0.12 | **No serde integration** | Yes (`MarkedYaml` / `MarkedYamlOwned` + `Marker`) | Rejected: would require hand-writing every deserializer |
| `yaml-rust2` | 0.13.0 | **No serde integration** | Marks only via the scanner/`Event` stream | Rejected: same reason, plus it is the pre-saphyr lineage |

**Why not `marked-yaml`.** Its documented constraints: top level must be a
mapping or sequence (fine for workflows), mapping keys must be scalars (fine),
and **"Aliases and anchors MAY NOT be used."** No workflow in this repo uses
anchors (`grep` for `&anchor` / `*alias` / `<<:` over `fixtures/workflows` and
`.github/workflows` — no matches), and GitHub Actions itself rejects them, but
`serde_yaml` accepts them today and "drop-in workflows" is a stated project goal
(`AGENTS.md`). Trading a documented capability for spans is the wrong direction
when `serde-saphyr` gives both.

**Why `serde-saphyr`.**

- serde derive survives. `#[derive(Deserialize)]` on the existing 25 model
  structs continues to work; `Spanned<T>` is opt-in per field.
- Spans are a *type-level* opt-in, so §4 can stage adoption field by field
  instead of as one big-bang migration.
- Errors carry `location()` with line/column and byte offset, which alone fixes
  half of `ParserError::Yaml`'s uselessness.
- It is type-driven ("Rust types as schema"), but **not semantically identical
  to the current generic-`Value` path by default**. A throwaway probe over the
  repository's YAML corpus used `strict_booleans: true`: 378 files parsed
  successfully under both parsers; 10 differences were only a trailing newline
  in block scalars, and one was the same newline plus a final `run:` scalar
  difference. A separate scalar probe found that `serde_yaml` parses bare
  `on/yes/no/off/y/n` and `0755` as strings in `serde_json::Value`, while
  `serde-saphyr` with `strict_booleans: true` fixes the boolean cases but still
  parses `0755` as numeric `755`. The raw `Value` fields in this model
  (`models.rs:174,652,657,666,669,681,952-958`) therefore need a compatibility
  adapter or a marked-node conversion; the old claim that the swap simply
  removes the numeric hazard is false.
- MSRV 1.89 ≤ project toolchain 1.97.

The span API itself was also probed. With a block scalar `run: |`, the observed
`Spanned<String>` location is the first content character (line 2, column 3),
not the `|` marker. Plain scalar locations point at the scalar value. This is
usable for diagnostics, but renderers must intentionally underline the scalar
value rather than assume every location is the mapping key.

**Migration risks, concretely.**

1. **The untagged wall is verified, not speculative.** `Spanned<T>` inside
   `#[serde(untagged)]`, `#[serde(tag=…)]`, or `#[serde(flatten)]` deserialises
   successfully but yields `Location::UNKNOWN` (0,0) — serde buffers through
   `ContentDeserializer` and drops the deserializer context. This is documented
   on the [`Spanned` API](https://docs.rs/serde-saphyr/latest/serde_saphyr/struct.Spanned.html)
   and a throwaway probe reproduced it for representative untagged, flattened,
   and internally tagged types. We have ten such sites (§1.2), including `Env`,
   `RunsOn`, `Needs`, `DeferredBool`, and `Matrix`'s `#[serde(flatten)] axes`.
   The documented workaround is to wrap the *whole* enum:
   `Spanned<RunsOn>` works, `RunsOn::Single(Spanned<String>)` does not. The
   design in §3 is built around this constraint, not in spite of it.
2. **No intermediate `Value` tree.** `serde-saphyr` deserialises straight into
   the target type. That breaks two current mechanisms:
   `normalize_yaml_keys` (`yaml.rs:8`, `yaml.rs:25-50`) and `MatrixValue`'s
   `Deserialize` (`models.rs:838-854`). `serde_json::Value` is a supported
   target, so both can be re-expressed, but they are real work, not a
   find-and-replace. The `on:`/`true` normalisation should move onto the
   existing `alias = "true"` (`models.rs:167`) plus a `Trigger` deserializer.
3. **Generic-value compatibility is a measured migration blocker.** With
   `strict_booleans: true`, a throwaway corpus probe found 378 files that
   parsed successfully under both parsers, 10 differences consisting only of
   a trailing block-scalar newline, and one additional `run:` trailing-newline
   difference. A focused scalar probe found that current `serde_yaml` parses
   bare `on/yes/no/off/y/n` and `0755` as strings in `serde_json::Value`;
   `serde-saphyr` with `strict_booleans: true` fixes the boolean cases but still
   parses `0755` as numeric `755`. The raw `Value` fields in this model
   (`models.rs:174,652,657,666,669,681,952-958`) therefore need a compatibility
   adapter or a marked-node conversion. Phase 0 must use an explicit
   allowlist for the newline difference and must fail on unapproved scalar
   changes; it must not assert byte-for-byte `Workflow` equality prematurely.
4. **Ecosystem age.** 1.2.0, single-maintainer, `granit-parser` (a saphyr fork)
   underneath. Mitigation: the swap is confined to `yaml.rs` +
   `models.rs`'s two manual deserializers; the other six crates use YAML for
   ad-hoc `Value` poking (`preloop-cli/src/github_setup.rs:766`,
   `preloop-runner-server/src/oidc.rs:360`,
   `preloop-orchestrator/src/environment.rs:53`,
   `preloop-conformance/src/main.rs:455`,
   `preloop-runner/src/worker/handlers/factory.rs:77`) and can move to any of
   the three candidates independently.

**Recommendation.** Keep `serde-saphyr` as the span-capable target for
`preloop-gha-parser`, but require the compatibility adapter in phase 0 before
accepting the migration. If that adapter cannot preserve the current raw
`Value` semantics without a fragile fork, use `serde_yaml_ng` as the
short-term deprecation fix and put the span front-end behind a separate marked
tree; that is safer for drop-in workflows but defers the direct `Spanned<T>`
ergonomics and likely means a second migration.

`ParserError::Yaml(#[from] serde_yaml::Error)` (`models.rs:16`) is public API,
but nothing in `crates/` matches on it, so changing the inner type is a
non-event for callers inside the workspace.

---

## 3. REPRESENTATION

### Option A — side table, JSON-Pointer-like path → span

`BTreeMap<String, Span>` keyed by `"jobs.build.steps[0].timeout-minutes"`.

- **Blast radius:** zero on `models.rs`, `expand.rs`, `job_builder.rs`, and the
  ~81 `parse_workflow(` call sites in `lib_tests.rs` (verified: `lib_tests.rs`
  contains **zero** `Step {` / `Job {` / `StepPlan {` / `JobPlan {` struct
  literals — every test parses YAML). Best-in-class here.
- **Ergonomics:** poor for consumers and worse for producers. The table must be
  built by a second traversal that mirrors serde's field naming, including
  `rename`/`alias` (`models.rs:164,167,631,637,640,699,785,788,986,992`). Any
  drift between the traversal and the derive is a silently wrong span. String
  keys cost an allocation and a `BTreeMap` lookup per query.
- **Memory:** worst. ~40-60 bytes per entry for the key alone.
- **Matrix / reusable survival:** the *table* survives (it describes source, not
  plans), but nothing in `StepPlan` (`protocol/lib.rs:546-576`) says which path
  it came from. You would have to reconstruct `jobs.<base_id>.steps[i]` from
  `JobPlan.base_id` + the step's index — and `base_id` is rewritten during
  reusable inlining to `"<caller>/<inner>"` (`expand.rs:1085`), so the
  reconstruction needs a second de-prefixing rule. Fragile.

### Option B — `Spanned<T>` on every model field

- **Blast radius:** very large. Every read site becomes `.value`: `expand.rs`
  reads `job.steps`, `job.defaults`, `job.strategy.*`, `job.env`, `job.runs_on`,
  `job.needs`, `job.permissions`, `job.outputs`, and `step.*` in
  `job_plan_from_job` (466-560) and `step_plan` (1176-1219); `eval.rs:320-514`
  touches essentially every field once. `dag.rs`, `trigger.rs`, `matrix_expand.rs`
  follow.
- **Ergonomics:** best for LSP — hover a field, you have its span.
- **Memory:** two `Location`s per wrapped field. On a 200-step monorepo workflow
  with ~15 spanned fields per step that is real, but not pathological.
- **Matrix / reusable survival:** *only if the spans propagate into `StepPlan`*,
  which today holds plain `String`/`BTreeMap` (`protocol/lib.rs:546-576`). Making
  `StepPlan` spanned means the plan is no longer cheaply serialisable to the
  store and every runner-side consumer changes. That is a much bigger cutover
  than the parser.
- **Blocked by §2 risk 1:** the ten untagged/flatten sites would silently
  produce `(0,0)`. `Step.env` is `Env` (untagged, `models.rs:546`),
  `Step.continue_on_error` is `DeferredBool` (untagged, `models.rs:795`),
  `Job.runs_on` is `RunsOn` (untagged). Per-field `Spanned` inside those is
  exactly the pattern the crate documents as broken.

### Option C — **recommended**: arena + stable node ids, `Spanned<T>` only at enum boundaries

Three pieces:

```rust
// preloop-gha-parser::span
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct FileId(pub u16);          // index into the workflow's file table
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct NodeId(pub u32);          // index into SpanArena.spans
#[derive(Copy, Clone)]
pub struct Span { pub file: FileId, pub line: u32, pub col: u32, pub len: u32 }

pub struct SpanArena {
    spans: Vec<Span>,                // NodeId -> Span
    files: Vec<String>,              // FileId -> workflow path (== wire fileTable)
    by_path: HashMap<Box<str>, NodeId>, // optional reverse index, LSP only
}
```

1. **`SpanArena` per parsed workflow**, returned alongside `Workflow` from a new
   `parse_workflow_spanned(path, input) -> (Workflow, SpanArena)`. The existing
   `parse_workflow` keeps its signature and drops the arena, so no caller
   changes on day one.
2. **`Spanned<T>` used sparingly at the parse boundary**, wrapping *whole*
   untagged enums (`Spanned<RunsOn>`, `Spanned<Env>`, `Spanned<DeferredBool>`)
   per the documented workaround, and individual scalars where the type is a
   plain `String`/`Option<String>` (`Step::run` `models.rs:972`, `Step::uses`
   975, `Step::if_condition` 984, `Job::if_condition` 638, job-id keys). The
   deserializer immediately interns each location into the arena and stores only
   a `NodeId` on the model. Model fields therefore grow by **4 bytes**, not by
   two `Location`s.
3. **One `origin: Option<NodeId>` on `StepPlan` and `JobPlan`.** Add
   `StepPlan.origin: Option<NodeId>` (`protocol/lib.rs:546-576`, `#[serde(default,
   skip_serializing_if="Option::is_none")]`) and `JobPlan.origin` similarly. Set
   in `step_plan` (`expand.rs:1196-1218`) and `job_plan_from_job`
   (`expand.rs:518-559`).

Judged on the same four axes:

- **Blast radius:** two new fields on the plan structs, two new fields per
  spanned model field, one new module. Struct-literal churn is bounded: workspace-wide
  there are exactly **2** `StepPlan {` literals (`protocol/lib.rs`,
  `expand.rs`) and **6** `JobPlan {` literals (2 in `protocol/lib.rs`, 2 in
  `dag.rs`, 2 in `expand.rs`). The 20 `Step {` literals in the workspace are the
  *runner's* own `Step` (`preloop-runner/src/worker/job_extension.rs` and its
  tests), not the parser's. `lib_tests.rs`: zero literals to fix.
- **Ergonomics:** an LSP or scanner holds `(Workflow, SpanArena)`; resolving a
  finding is `arena.spans[node.0]` — one index, no allocation, no string keys.
  The optional `by_path` index gives the JSON-Pointer view for free when a tool
  wants it, without making paths the storage format.
- **Memory:** 16 bytes per span in a flat `Vec`, plus 4 bytes per annotated
  field. Strictly better than A, better than B.
- **Matrix survival:** free. `expand.rs:511-516` clones steps per matrix cell;
  a `Copy` `NodeId` rides along, and all N `StepPlan`s correctly point at the
  *one* source step. That is the right semantics: a scanner finding on
  `run:` in a 12-cell matrix is one finding at one location, not twelve.
- **Reusable-workflow survival:** each parsed file gets its own arena. Inlining
  (`expand_reusable_call`, `expand.rs:1048-1138`) must merge the callee arena
  into the caller's, remapping `FileId` and offsetting `NodeId`s — a mechanical
  `Vec` concat plus `+base` on the ids it rewrote. Because `FileId` **is** the
  wire `fileTable` index, this is also exactly the data structure phase 3 needs
  to fix `runs.rs:2116`. Values with no source (the conjoined `if:` from
  `merge_job_conditions`, `expand.rs:1152-1162`; job defaults folded into a step
  at `expand.rs:1184-1195`) get `origin: None` or the *defaults'* node — the
  representation can express both, which A and B cannot without extra rules.

**Decision: Option C.**

---

## 4. PHASING

### Phase 0 — parser swap, no spans *(ships alone; no wire change)*

Replace `serde_yaml` with `serde-saphyr` in `preloop-gha-parser` only, but do
not treat that as a mechanical rename. Rewrite `normalize_yaml_keys`
(`yaml.rs:25-50`) and `MatrixValue::deserialize` (`models.rs:833-858`) and
add the generic-`Value` compatibility adapter required by §2 risk 3. Change
`ParserError::Yaml`'s inner type (`models.rs:16`).

The compatibility gate must parse every file under `fixtures/workflows/` and
`.github/workflows/` with both parsers. It may allow only the ten observed
block-scalar trailing-newline differences (and the one observed final `run:`
newline difference); it must reject the measured `0755` retyping and any new
boolean/scalar drift. This is a semantic differential test, not a claim that
the two raw `serde_json::Value` trees are identical.

*Value on its own:* clears an unmaintained dependency. Nothing else changes.

### Phase 1 — `SpanArena` + spans in `ParserError` *(no wire change)*

Add the `span` module, `parse_workflow_spanned`, and `Spanned<T>` at the ~12
highest-value fields. Add `span: Option<Span>` to `ParserError::InvalidExpression`
and to the YAML variant. Thread the arena into
`validate_workflow_expressions` (`eval.rs:320-514`) so each of its ~25
`format!` sites (`eval.rs:340,345,352,357,362,368,374,380,384,389,394,403,409,417,423,432,444,454,467,473,483,489,493,498,505`)
attaches the node it was validating.

*Value on its own:* `preloop run` on a broken workflow prints
`file:line:col` instead of `job \`build\` step #2 run: …`. Existing tests
(`lib_tests.rs:1949,1997,2027`) assert substrings and keep passing.

### Phase 2 — `origin` on plans; scanner + LSP *(no wire change)*

Add `StepPlan.origin` / `JobPlan.origin`; populate at `expand.rs:1196-1218` and
`expand.rs:518-559`; merge arenas in `expand_reusable_call` (`expand.rs:1083-1120`).
Build the consumers (§5).

*Value on its own:* `preloop lint` with zizmor-class findings, and the data an
LSP needs for diagnostics/hover/go-to-def/rename. Still zero wire impact.

### Phase 3 — real coordinates on the wire *(wire change; gated)*

Thread `SpanArena` into `build_agent_job_message_with_normalized_context`
(`job_builder.rs:298-662`) — note its current signature takes `&JobPlan` and has
no path back to the source, so this phase needs a new parameter or an arena
handle on the plan. Replace the synthetic literals (§1.4). Populate
`file_table` from `SpanArena.files` and stop clobbering it at `runs.rs:2116`.

Bundle the two independent fidelity fixes found here, because they touch the
same serializers: `displayNameToken` `type: 1` → `type: 0`
(`job_builder.rs:884`), and `timeoutInMinutes` as a type-6 token instead of a
bare number (`azdo/job.rs:385`).

**Everything through phase 2 ships without touching the wire format.**

---

## 5. WORKED EXAMPLE — security scanner: untrusted input into `run:`

Input, `.github/workflows/pr.yml`:

```yaml
1  on: pull_request_target
2  jobs:
3    build:
4      runs-on: ubuntu-latest
5      strategy:
6        matrix:
7          os: [ubuntu-latest, macos-14]
8      steps:
9        - name: Greet
10         run: echo "Hello ${{ github.event.pull_request.title }}"
```

**Call path, with the structs traversed at each hop:**

1. `parse_workflow_spanned(".github/workflows/pr.yml", src)` — new entry beside
   `yaml.rs:6`. `serde_saphyr::from_str` fills `Workflow` (`models.rs:159-180`).
   `Step::run` (`models.rs:972`) is declared `Spanned<Option<String>>`; its
   deserializer interns `Span { file: FileId(0), line: 10, col: 14, len: 47 }`
   into `SpanArena` as `NodeId(37)` and stores `37` on the field.
   `SpanArena.files[0] = ".github/workflows/pr.yml"`.
   *Why `Spanned` works here:* `Step` is a plain struct, not untagged — §2 risk 1
   does not bite. `Step::env` (`Env`, untagged, `models.rs:978`) would be wrapped
   as `Spanned<Env>` at the enum boundary instead.

2. `expand_jobs` (`expand.rs:431-463`) iterates the 2 matrix cells (loop at 439).
   Each cell calls `job_plan_from_job` (466), which clones the steps at
   `expand.rs:511-516` and calls `step_plan` (1176). `step_plan`'s
   `StepPlan { .. }` (1196-1218) copies `origin: step.run.node` — a `Copy` u32.
   Result: `JobPlan("build (ubuntu-latest)")` and
   `JobPlan("build (macos-14)")`, both with `steps[0].origin == NodeId(37)`.
   **One source location, two plans** — deduplication at report time is a
   `HashSet<NodeId>`, not a heuristic.

3. Scanner (new, `preloop-gha-parser::lint` or a `preloop-lint` crate) walks the
   `Workflow`, not the plans — it wants source semantics, not expansion. For each
   `Step` with `run: Some(_)`:
   - `preloop_gha_expressions::collect_contexts` (`expressions/src/lib.rs:118`)
     over each `${{ }}` in the scalar → `{"github"}`.
   - Check the accessor path against a taint list
     (`github.event.*.title`, `.body`, `head_ref`, …).
   - Check the trigger: `Workflow::on` (`models.rs:168`) is
     `pull_request_target` → privileged context.
   - Emit `Finding { node: NodeId(37), rule: "template-injection", severity: High }`.

4. Rendering: `arena.spans[37]` → `(file 0, line 10, col 14)`;
   `arena.files[0]` → the path. Column precision inside the scalar needs the
   expression-crate offset work noted in §1.6; until then the span points at the
   scalar start, which is already actionable.

**User-visible output** (`preloop lint .github/workflows/pr.yml`):

```
error[template-injection]: untrusted `github.event.pull_request.title` is
interpolated into a `run:` script
  --> .github/workflows/pr.yml:10:14
   |
10 |         run: echo "Hello ${{ github.event.pull_request.title }}"
   |                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ expands to attacker-controlled text
   |
   = note: workflow trigger `pull_request_target` (line 1) runs with the base
           repository's secrets and a write-scoped GITHUB_TOKEN
   = note: this step expands to 2 matrix jobs; the finding is reported once
   = help: bind the value to an env var and reference "$TITLE" in the script

1 error
```

Exit non-zero, so it drops into `just test-ci` next to the existing `zizmor`
recipe (`justfile:52-53`) — the difference being that this one runs on the user's
own workflows through preloop's parser, with no Python/`uvx` dependency.

---

## 6. RISKS

### 6.1 Conformance impact of changing emitted coordinates

**Verified: `just conform` will not notice coordinate *value* changes.**

- `just conform` → `benchmarks/conformance/run.sh:122-123` → `run.sh:74-75`
  runs `runner-watch conform … --skip-cargo-test`, with **no**
  `--value-gate-strict`.
- Without that flag the policy is `ValueGate::Off` (`main.rs:1881-1893`,
  else-branch at 1892), and `Off` short-circuits the value gate
  (`compare.rs:576-580`).
- The remaining gates are *schema* gates. `to_schema_value`
  (`compare.rs:256-280`) erases every scalar to its type name, so
  `"line": 0` and `"line": 8` both become `"number"`.
- `schema_drops_fields` (`compare.rs:382-394`) tolerates additions on the
  candidate side — asserted directly at `compare.rs:1375-1376`. So *adding*
  `file`/`line`/`col` where we currently omit them is invisible to the gate.

**This is a fidelity improvement with no golden-diff cost, with two caveats:**

1. Changing `timeoutInMinutes` from `number` to `object` is a **type change**,
   which `schema_drops_fields` does flag (`compare.rs:393`, asserted at
   `compare.rs:1378-1379`) — but only for endpoints whose key contains
   `"/broker/{n}/acquirejob"` (`compare.rs:481`, gate at 563-569). The
   endpoint key construction is verified at `compare.rs:193-199`:
   `format!("{method} {}", normalize_path(path))`. The golden path
   `.runner-watch/golden/v2.336.0/103-composite-nested-post/flows.jsonl:23`
   is `/190/acquirejob`, which normalizes to `POST /{n}/acquirejob`
   (`compare.rs:66-90`), so it does **not** contain the `/broker/` prefix.
   Therefore the default response-schema gate does not fire for this golden
   endpoint. This is a gate-coverage bug, not an unresolved question.
2. Anyone running `runner-watch conform --value-gate-strict` (`main.rs:141`,
   `1881-1890`) *would* see every coordinate change. That mode is not part of
   `just conform` today.

### 6.2 Tests that pin synthetic coordinates (these break in phase 3)

The `azdo_tests.rs` wire oracle (`expected_template_token`,
`expected_template_map`) hard-coded `file: 1`. The §6.4 fixes made it take the
step's `file_id`, so it now asserts the *emitted* index rather than a
constant, and the `arb_literal_step` strategy generates ids in `1..=3` so the
round-trip covers callee-indexed steps. Its `line: 0` / `col: 0` are still
synthetic and will need revisiting in phase 3.

Still pinned to synthetic or placeholder values:

- `azdo_tests.rs` — `timeoutInMinutes` asserted as `Value::Null` and
  round-tripped as `Option<u32>`; becomes a type-6 token in phase 3.
- Two proptest regression seeds encode the pre-fix `TaskStep` shape, including
  `display_name_token: Some(Object {"lit": …, "type": Number(1)})`:
  `crates/preloop-gha-protocol/proptest-regressions/azdo_tests.txt:7` and
  `proptest-regressions/azdo/azdo_tests.txt:7-8`. These are replay seeds, not
  assertions — they still load, but the quoted shape is now stale.

Per the project's testing bar, anything pinning a *synthetic* coordinate is
pinning an implementation artefact and should be deleted rather than re-pinned
to a new synthetic value; assertions about token *shape* (type tag, key
presence) should track the real GitHub shape evidenced in §1.4.

### 6.3 Remaining unverified items

The three uncertainties called out in the original handoff are resolved:

- **Schema-gate matching:** resolved from source, then **fixed** (§6.4).
  `group_flows` constructs `METHOD <normalized path>` (`compare.rs:193-199`);
  the golden `/190/acquirejob`
  (`.runner-watch/golden/v2.336.0/103-composite-nested-post/flows.jsonl:23`)
  becomes `POST /{n}/acquirejob`, which never contained the old
  `/broker/{n}/acquirejob` substring, so the only schema gate in the default
  policy was inert on every committed golden.
- **`serde-saphyr` model boundary:** the isolated copy of the complete
  `preloop-gha-parser` compiled with a direct `serde-saphyr` workflow/action
  frontend and its parser library tests passed: **154 passed, 0 failed**.
  The probe retained `serde_yaml` only for the existing `MatrixValue`
  implementation and unused normalization helpers, so this resolves derive,
  untagged, flattened, tagged-enum, and action-metadata compatibility but not
  the final no-`serde_yaml` dependency cutover.
- **Scalar-resolution drift:** measured. With `strict_booleans: true`, 378
  repository YAML files parsed under both parsers; 10 differed only by a
  trailing block-scalar newline, and one additional file differed by a final
  `run:` newline. The focused generic-`Value` probe showed
  `serde_yaml`: `on/yes/no/off/y/n/0755` → strings; `serde-saphyr` strict:
  the boolean spellings → strings but `0755` → numeric `755`. This is now a
  compatibility requirement in phase 0, not an unknown.

Two risks remain open:
1. **Final no-`serde_yaml` cutover has not been compiled.** The isolated probe
   retained it for `MatrixValue` and the dead `normalize_yaml_keys` helpers.
   Replace those paths and compile before removing the workspace dependency.
2. **Runtime cost of `serde-saphyr` vs `serde_yaml` on this corpus.** No
   benchmark was run. Parse time is on the submission path (`runs.rs`), so
   phase 0 should carry a before/after measurement.

**Resolved:** the official runner *does* consume token coordinates.
`TemplateContext.Error` routes every template error through
`GetErrorPrefix(fileId, line, column)`
(`/tmp/runner-v2.337.0/src/Sdk/DTObjectTemplating/ObjectTemplating/TemplateContext.cs:154-168,203-228`),
which renders `"{fileName} (Line: {line}, Col: {col}): {message}"` after
resolving the name through `GetFileName` (`:193-196`). Coordinates are
therefore diagnostic, not load-bearing for evaluation — but `fileId` is a
1-based index (`GetFileId` returns `count + 1`, `:180-191`) and `GetFileName`
indexes `FileNames[fileId - 1]`, so an out-of-range id like the `file: 0` we
used to emit is a latent fault on the error path, not merely cosmetic.

### 6.4 Fixes landed alongside this ADR

Three independent defects surfaced during the investigation. None needed span
infrastructure, so they were fixed directly rather than folded into phase 3.

1. **`displayNameToken` used the wrong token type.** We emitted `type: 1` with
   a `lit` (`job_builder.rs`). `TokenType` is authoritative at
   `/tmp/runner-v2.337.0/src/Sdk/DTObjectTemplating/ObjectTemplating/Tokens/TokenType.cs:7-9`:
   `String = 0`, `Sequence = 1`. `TemplateTokenJsonConverter.ReadJson`
   (`Tokens/TemplateTokenJsonConverter.cs:66-108`) switches on `type`, so 1
   built a `SequenceToken` — which has no `lit` — and
   `ActionRunner.GenerateDisplayName`'s `as ScalarToken` cast
   (`src/Runner.Worker/ActionRunner.cs:374-376`, null-guarded at `:421-424`)
   then dropped the step name. All 43 `displayNameToken`s across the goldens
   are `type: 0`. Fixed, plus `file: 0` → a valid 1-based id.
2. **`fileTable` discarded callee provenance.** `runs.rs` overwrote it with the
   caller path alone, throwing away `JobPlan.workflow_file` that
   `expand.rs:1103` had already resolved. Real GitHub ships
   `[caller.yml, owner/repo/path.yml@sha]` and indexes per job: in
   `.runner-watch/golden/v2.337.0/gh-official/205-reusable-workflow-chain/`
   the callee job `ci / build` carries `file: 2` on 8 tokens while the
   caller-defined `post` job carries `file: 1`, against the same table. Now
   modelled by `job_builder::job_file_id` and `TaskStep.file_id`; a
   reusable-call *placeholder* stays on 1 because its `workflow_file` names the
   callee it is about, not the file its own tokens came from.
3. **The conformance response-schema gate was dead.** Fixed as described above;
   the regression test asserts a dropped acquirejob field is caught on the
   golden path shape, and fails against the old substring with `failures: []`.
