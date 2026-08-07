# actionlint vs aksh's parser vs the official runner

A deep comparison of three parsers for GitHub Actions workflows:

- [rhysd/actionlint](https://github.com/rhysd/actionlint) — Go static linter
- aksh's own parser — `aksh-gha-parser` + `aksh-gha-expressions` (Rust, ~9k LOC)
- the official `actions/runner` v2.336.0 (C#, commit `98aabcd`)

All three implement the same GitHub expression grammar. They differ radically in
*why* they parse.

> Updated 2026-08-05. Sources: actionlint `main` (Go 1.25, `go.yaml.in/yaml/v4`),
> `/tmp/runner-v2.336.0`, and the aksh crates.

## The framing: four layers

| Layer | actionlint | aksh | Official runner |
|---|---|---|---|
| YAML → AST | ✅ custom, strict, positions | ✅ serde-based, lenient, no positions | ⚠️ server-side (inaccessible); runner parses only **reusable workflows** + **action.yml** |
| Semantic validation | ✅ full (types, contexts, structure) | ⚠️ subset (expression syntax, allowed contexts, DAG/matrix) | ⚠️ schema-only (`workflow-v1.0.json`, `action_yaml.json`) |
| Static analysis / linting | ✅ 16+2 rule families | ❌ | ❌ (explicitly none) |
| Runtime execution | ❌ by design | ✅ | ✅ |

## Architecture

### actionlint

```
workflow YAML
  └─ go.yaml.in/yaml/v4 ──► raw yaml.Node tree
       └─ parser.resolveAliases()          (recursive/unused anchors)
       └─ parser.parse()                   (strict typed Workflow AST, Pos on every node)
            └─ 16+ Rule instances over the Visitor (pass.go)
                 └─ errors: file:line:col + snippet + ^~~~ (error.go)
```

- Custom YAML validation on top of `go.yaml.in/yaml/v4` — not a generic parser:
  unknown keys rejected per section, duplicate keys (case-insensitive), typed
  scalars (`!bool`/`!int`/`!float` tags or `${{ }}`), required sections, empty
  mappings/sequences, YAML merge key `<<` rejected, anchors resolved.
- Own expression lexer (`expr_lexer.go`) + recursive-descent parser
  (`expr_parser.go`): `||` < `&&` < comparisons < `!` < postfix (`.prop`, `.*`,
  `[idx]`); single-quoted strings; keywords `null`/`true`/`false`.
- Full expression type system (`expr_type.go`, `expr_sema.go`): `ObjectType`
  strict/loose/mapped, `ArrayType` with deref flag, `Merge()` narrowing,
  `Assignable()`; builtin function signatures incl. `format()` placeholder
  validation, `fromJSON()` typed returns, `case()` arity; **stricter than the
  runner** (template eval rejects objects/arrays/null; only `any`/`number` →
  string coercion).
- Context-availability per workflow key, **generated from GitHub docs**
  (`availability.go`, weekly CI).
- Untrusted-input analysis (`expr_insecure.go`): `github.event.*.body` etc.,
  including through `.*` filters, exempting `contains`/`startsWith`/`endsWith`.
- Script analysis: detect shell from `shell:`/`defaults.run.shell` + OS from
  `runs-on:`, extract `run:` text, replace `${{ }}` with underscores, run
  external shellcheck/pyflakes, map positions back.
- Output: human/JSON/SARIF, exit codes 0/1/2/3, reviewdog/pre-commit/VS Code
  integrations.

### aksh

```
workflow YAML
  └─ serde_yaml ──► Value ── normalize keys (bool true → "on")
       └─ serde typed deserialize ──► Workflow model (models.rs)
            └─ validate_workflow_expressions (eval.rs)
            └─ validate_job_plans (dag.rs: cycles, unknown needs)
            └─ matrix expansion + trigger matching
```

- Lenient YAML: unknown keys ignored, duplicates last-wins, no anchor
  handling, no positions (errors are message strings).
- Expression engine: custom lexer + Pratt parser (`lexer.rs`, `expr_parser.rs`),
  AST `Literal | Path | UnaryNot | Binary | Call | MemberAccess`; functions
  `always/success/failure/cancelled/contains/startsWith/endsWith/format/
  fromJSON/join/hashFiles/toJSON/case` (lazy `case()`), abstract equality,
  numeric coercion, `hashFiles` = SHA-256 over the workspace.
- Parse-time validation: expression syntax + known function names +
  per-field allowed contexts (hand-maintained `CTX_*` constants in `eval.rs`)
  + needs-DAG cycles + empty jobs.
- Runtime: matrix expansion (include/exclude), needs DAG scheduling,
  concurrency, reusable-workflow expansion with input coercion, wire protocol.

### Official runner

- **Regular jobs**: the server parses the workflow, expands matrix, resolves
  needs/concurrency, and sends `AgentJobRequestMessage` (JSON with
  pre-parsed `TemplateToken` trees — no raw YAML). The runner deserializes it
  (`Worker.cs:72`) and executes.
- **Reusable workflows**: the runner parses YAML itself —
  `WorkflowTemplateParser` → `YamlObjectReader` (YamlDotNet) → validated
  against `workflow-v1.0.json` (2,918-line JSON Schema) → `WorkflowTemplate`.
- **Action manifests**: `ActionManifestManager` parses `action.yml` against
  `action_yaml.json`, evaluating expressions in inputs/outputs/container args.
- **Expressions**: dual engine in v2.336 — legacy
  `GitHub.DistributedTask.Expressions2` (shunting-yard) and new
  `GitHub.Actions.Expressions`, run side-by-side behind
  `PipelineTemplateEvaluatorWrapper` with mismatch telemetry (a migration
  canary). Runtime limits: `MaxDepth=50`, `MaxLength=21000`, 1 MB evaluation
  memory cap; wildcard via `FilteredArray`; lazy `format()`; `hashFiles()`
  spawns Node with a 120s timeout; step `if:` re-evaluated on cancellation;
  default condition `success()`.
- **No static analysis of any kind.** Schema validation surfaces only when a
  reusable workflow is evaluated at runtime.

## What actionlint does that aksh doesn't

| # | Capability | actionlint evidence | aksh status |
|---|---|---|---|
| 1 | Position-tracked diagnostics (line/col, snippet, `^~~~`), JSON/SARIF output, exit codes | `error.go`, `command.go` | ❌ plain `ParserError` strings |
| 2 | Strict YAML: unknown keys, case-insensitive duplicates, typed scalars, required sections, merge-key rejection, anchor resolution | `parse.go` | ❌ serde-lenient |
| 3 | Expression **type checking**: `format()` placeholder/arity, `fromJSON()` typed objects, `case()` arity, property access on wrong type, null derefs, unknown step IDs | `expr_type.go`, `expr_sema.go` | ⚠️ names + allowed contexts only |
| 4 | Context availability per workflow key, generated from docs | `availability.go` | ⚠️ hand-maintained `CTX_*` subset |
| 5 | Rule families: matrix dups, credentials, shell names, runner labels, webhook events + cron syntax, needs cycles, action refs/outdated versions, env-var names, ID uniqueness, glob syntax, permissions, workflow_call cross-validation, deprecated commands, constant `if:`, untrusted inputs | `rule_*.go` | ⚠️ needs cycles only |
| 6 | Script analysis (shellcheck/pyflakes) with `${{ }}` masking and position mapping | `rule_shellcheck.go`, `rule_pyflakes.go` | ❌ |
| 7 | Tooling: config ignore patterns, reviewdog/pre-commit/VS Code, problem matchers | `config.go`, `command.go` | ❌ |

## What the official runner does that the others don't

| Capability | Evidence | aksh status |
|---|---|---|
| Schema-driven validation (`workflow-v1.0.json`, `action_yaml.json`) encoding types + allowed contexts | `src/Sdk/WorkflowParser/workflow-v1.0.json` | ⚠️ typed structs instead of a schema file |
| Expression runtime caps: depth 50, length 21000, 1 MB memory | `ExpressionConstants.cs`, `ExpressionNode.cs` | ❌ no caps observed |
| Lazy `format()` segments, `FilteredArray` wildcard semantics | `Sdk/Operators/Index.cs` | ✅ semantics implemented |
| Default `success()` step condition; `if:` re-evaluation on cancellation | `StepsRunner.cs` | ✅ (`conditions.rs`) |
| Dual-engine differential canary with telemetry | `PipelineTemplateEvaluatorWrapper.cs` | ⚠️ wire-level differential harness instead |
| Command parsing: `##[legacy]` + `::v2::` escape tables, `add-matcher`/`remove-matcher`, file commands with heredoc | `ActionCommand.cs`, `ActionCommandManager.cs`, `FileCommandManager.cs` | ✅ per aksh matrix (heredoc parity to verify) |
| Action metadata parsing + runtime input validation | `ActionManifestManager.cs` | ✅ `parse_action_metadata` |

## What aksh does that actionlint doesn't (by design)

- Executes: real context values (`steps.*.outputs`, `job.status`, `runner.os`),
  matrix expansion with include/exclude semantics, needs DAG scheduling,
  concurrency groups, reusable-workflow expansion with input coercion and
  depth limits, trigger matching (branch/path filters).
- `hashFiles()` against a real filesystem (SHA-256 over the workspace, sorted).
- The full wire protocol: `AgentJobRequestMessage` construction, broker
  lifecycle, timeline, masking, OIDC.
- `format()` with the real `{{`/`}}` escape state machine; lazy `case()`.

## The honest summary

- **actionlint** = the *spec police*: strict YAML, an expression type system,
  a context matrix, script linting, injection analysis. The official stack
  also lacks most of this — GitHub's server is lenient and most of these
  surface as runtime failures there too.
- **aksh** = the *executor*: everything actionlint refuses to do, plus most of
  the runner's runtime semantics — but its parse-time validation is the
  weakest of the three.
- **Official** = the *reference*: schema-driven where it parses,
  runtime-lenient everywhere else, with the deepest runtime-semantics polish
  (caps, lazy evaluation, cancellation re-evaluation) — the closest model for
  aksh's runtime fidelity work.

## Cheapest high-value gaps for aksh

In priority order:

1. **Line/column in `ParserError`** — wrap serde errors with source positions;
   this alone changes failure UX from "what" to "where".
2. **Parse-time argument validation for `format()`/`fromJSON()`/`case()`** —
   the expression AST already exists (`validate_function_calls`); the checks
   are ~50 LOC each and mirror actionlint's `expr_sema.go`.
3. **Unknown-key warnings** for the top-level `on:`/`jobs:`/`env:` typos —
   aksh currently ignores them silently.
4. **Expression depth/length caps** mirroring the runner
   (`MaxDepth=50`, `MaxLength=21000`) to avoid pathological inputs.
5. **Deprecation warnings** for `::set-output::` / `::set-env::` at parse time.

Items 1+2 close most of the user-facing gap with actionlint without adding a
linting product.
