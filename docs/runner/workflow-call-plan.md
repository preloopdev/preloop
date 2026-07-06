# `workflow_call` (Reusable Workflows) Implementation Plan

## 1. How the Official Runner Does It

The official implementation splits across **server-side** (WorkflowParser — parses YAML and expands the job graph) and **runner-side** (Worker — executes jobs that arrive with `contextData.inputs` already populated). The runner itself is unaware of reusable workflows; the server flattens them into individual job messages.

### 1.1 Data Model

**`Runner.Server/WorkflowParser/ReusableWorkflowJob.cs`**

The core type representing a `uses: ...` job in the workflow:

```
Id                  — string, the job key from the caller YAML
Name                — display name
Needs               — List<string>, dependency job IDs
If                  — condition expression
Ref                 — string, the workflow reference (e.g. "./.github/workflows/x.yml" or "owner/repo/.github/workflows/x.yml@v1")
Permissions         — token permissions block
InputDefinitions    — OrderedDictionary<string, InputDefinition>, from callee's `on.workflow_call.inputs`
InputValues         — OrderedDictionary<string, TemplateToken>, from caller's `with:`
SecretDefinitions   — OrderedDictionary<string, SecretDefinition>, from callee's `on.workflow_call.secrets`
SecretValues        — OrderedDictionary<string, TemplateToken>, from caller's `secrets:`
InheritSecrets      — bool, `secrets: inherit`
Outputs             — OrderedDictionary<string, OutputDefinition>, from callee's `on.workflow_call.outputs`
Defaults            — callee workflow `defaults:`
Env                 — callee workflow `env:`
Concurrency         — caller's `concurrency:` on the `uses:` job
EmbeddedConcurrency — callee's workflow-level `concurrency:`
Strategy            — caller's `strategy:` (matrix applied to the entire reusable call)
Jobs                — List<Job|ReusableWorkflowJob>, the callee's expanded job list (recursive)
```

### 1.2 Loading and Parsing

**`Runner.Server/WorkflowParser/Conversion/ReusableWorkflowsLoader.cs`**

`LoadRecursive()` (line 131–193):

1. **Depth check** (line 136): compares current depth against `MaxNestedReusableWorkflowsDepth` (hardcoded to 4 in the official runner). Exceeding this throws a validation error.

2. **Qualify the ref** (line 147, `FullyQualifyWorkflowRef()` at line 200–220): converts `./path` to `{owner}/{repo}/{path}@{sha}`. For remote refs (`owner/repo/path@ref`), resolves the ref to a commit SHA via the GitHub API.

3. **Parse called YAML** (line 151): `m_loader.ParseWorkflow()` parses the called workflow YAML into a template token tree.

4. **Convert to referenced workflow** (line 164): calls `WorkflowTemplateConverter.ConvertToReferencedWorkflow()` which extracts:
   - `on: workflow_call` trigger → input/secret/output definitions
   - `jobs:` → the callee's job list
   - `defaults:`, `env:`, `permissions:`, `concurrency:`

5. **Recurse** (line 180–193): for each job in the called workflow that is itself a `uses:` (nested reusable), recurse with depth+1.

### 1.3 Input/Secret/Output Validation

**`Runner.Server/WorkflowParser/Conversion/WorkflowTemplateConverter.cs`**

**`ConvertToOnTrigger()`** (line 258–325):
- Validates that the called workflow has `on: workflow_call`
- Extracts `inputs:` with their type (string/number/boolean), required flag, default value
- Extracts `secrets:` with required flag
- Extracts `outputs:` with `value:` expression and optional `description:`

**`ConvertToWorkflowJobInputs()`** (line 2091–2168):
- Iterates caller's `with:` entries
- Matches each against callee's `inputs:` definitions (case-insensitive)
- Validates type compatibility: `type: boolean` must receive a boolean-coercible value, `type: number` must receive a numeric value, `type: string` accepts anything
- Applies default values for inputs not provided by the caller
- Error if a required input without default is not provided

**`ValidateWorkflowJobSecrets()`** (line 2170–2222):
- If `secrets: inherit`, all parent secrets are forwarded — no validation needed
- Otherwise, iterates caller's `secrets:` entries, matches against callee's `secrets:` definitions (case-insensitive)
- Error if a required secret is not provided
- Error if a secret is provided that isn't declared by the callee (unless callee has no secret definitions at all)

**`ConvertToWorkflowJobOutputs()`** (line 2064–2087):
- Converts callee's `on.workflow_call.outputs` to a string map
- Each output has a `value:` expression that can reference `${{ jobs.<job_id>.outputs.<name> }}`

### 1.4 Runtime Evaluation

**`Runner.Server/WorkflowParser/WorkflowTemplateEvaluator.cs`**

**`EvaluateWorkflowJobInputs()`** (line 236–269):
- Called at job dispatch time (when the server is about to send the job to a runner)
- Evaluates each input expression in the caller's context (has access to `github.*`, `needs.*`, `vars.*`, `secrets.*`)
- Type coerces the result: boolean inputs → bool, number inputs → number, string inputs → string
- Produces the `inputs` context data that gets injected into the job message

**`EvaluateWorkflowJobOutputs()`** (line 271–296):
- Called after all jobs in the reusable workflow complete
- Evaluates each output's `value:` expression in a context that has `jobs.<id>.outputs` from the completed jobs
- Produces the output values that become available via `needs.<caller_job_id>.outputs.<name>` in downstream jobs

**`EvaluateWorkflowJobSecrets()`** (line 837–862):
- Evaluates secret expressions from the caller's `secrets:` mapping
- If `InheritSecrets`, passes all parent secrets through unchanged
- Otherwise, evaluates each secret value expression and builds the secrets map

### 1.5 Runner-Side Handling

**`Runner.Worker/JobExtension.cs`** (line 382–401):
- The runner receives a normal job message — it doesn't know about reusable workflows
- It reads `system.workflowFileFullPath` variable and logs `Uses: <ref>`
- It reads `contextData.inputs` and injects them into the expression context as `inputs.*`
- Steps execute normally with `inputs.*` available in expressions

**`Runner.Worker/Handlers/JobContext.cs`** (line 86–132):
- Exposes `job.workflow_ref` (e.g. `owner/repo/.github/workflows/x.yml@refs/heads/main`)
- Exposes `job.workflow_sha` (the resolved commit SHA)
- Exposes `job.workflow_repository` (e.g. `owner/repo`)
- These are populated from variables in the job message, not computed by the runner

**`Runner.Worker/GitHubContext.cs`** (line 45–46):
- `github.workflow_ref` and `github.workflow_sha` are separate from `job.*` — they refer to the *triggering* workflow, not the called one

### 1.6 Key Behaviors

1. **Flattening**: The server expands reusable workflow calls into individual jobs. A caller with `uses: ./.github/workflows/x.yml` where x.yml has 3 jobs produces 3 expanded jobs with IDs `caller_job/inner_job_1`, `caller_job/inner_job_2`, `caller_job/inner_job_3`.

2. **Needs rewriting**: Inner job `needs` are prefixed with the caller job ID. If inner job `B` needs inner job `A`, the expanded `needs` becomes `caller_job/A`.

3. **Outer needs**: The caller job's `needs` become prerequisites for ALL expanded inner jobs. If `call-job` needs `build`, then `call-job/test1`, `call-job/test2` all need `build`.

4. **Output propagation**: After all inner jobs complete, the server evaluates the callee's `on.workflow_call.outputs` expressions (which can reference `jobs.<id>.outputs`). These become the caller job's outputs, accessible as `needs.call-job.outputs.<name>` by downstream jobs.

5. **Secret scoping**: Without `secrets: inherit`, the callee only sees secrets explicitly passed via `secrets:`. With `secrets: inherit`, all parent secrets are available.

6. **Input type coercion**: Inputs are coerced to their declared type at evaluation time. A `type: boolean` input receiving `"true"` string is coerced to `true`. A `type: number` input receiving `"42"` is coerced to `42`.

7. **Case insensitivity**: Input and secret names are matched case-insensitively between caller and callee.

8. **Max nesting depth**: 4 levels of `uses:` → `uses:` → `uses:` → `uses:`. Deeper nesting is rejected at parse time.

---

## 2. Current aksh State

### What exists (`crates/aksh-gha-parser/src/lib.rs:655-717`)

`expand_jobs_with_reusables()`:
- ✅ Detects local reusable workflow refs (`./` or `.github/`)
- ✅ Parses called workflow YAML
- ✅ Prefixes inner job IDs with `caller_id/`
- ✅ Rewrites inner `needs` with caller prefix
- ✅ Merges caller env and global env into inner jobs
- ✅ Sets `secrets_inherit` flag from `secrets: inherit`

### What's missing

| Gap | Severity | Description |
|---|---|---|
| Input validation | P0 | Caller `with:` not validated against callee `on.workflow_call.inputs` |
| Input injection | P0 | `inputs` context not populated in expanded job plans |
| Output definitions | P0 | Callee `on.workflow_call.outputs` not parsed |
| Output propagation | P0 | Caller job outputs not computed from inner job results |
| Secret validation | P1 | Caller `secrets:` not validated against callee `on.workflow_call.secrets` |
| Secret scoping | P1 | Secrets not filtered to only declared ones (unless `inherit`) |
| Type coercion | P1 | Input types (string/number/boolean) not enforced |
| Remote refs | P1 | `owner/repo/.github/workflows/x.yml@ref` not supported |
| Nesting depth | P1 | No max depth check (should be 4) |
| Outer needs | P1 | Caller's own `needs` not propagated to inner jobs |
| Case insensitivity | P2 | Input/secret name matching not case-insensitive |
| `job.workflow_ref` | P2 | Context fields not set for reusable workflow jobs |
| `workflow_call` trigger validation | P2 | Called workflow not validated to have `on: workflow_call` |
| Default values | P2 | Input defaults from callee definition not applied |
| Strategy on caller | P2 | Matrix on the `uses:` job not applied (matrix of reusable calls) |

---

## 3. Implementation Plan

### Phase 1: Parser — Input/Output/Secret Model

**Files**: `crates/aksh-gha-parser/src/lib.rs`

1. **Add `WorkflowCallTrigger` struct** to the `Workflow` model:
   ```rust
   pub struct WorkflowCallTrigger {
       pub inputs: BTreeMap<String, InputDefinition>,
       pub secrets: BTreeMap<String, SecretDefinition>,
       pub outputs: BTreeMap<String, OutputDefinition>,
   }

   pub struct InputDefinition {
       pub input_type: InputType, // String | Number | Boolean
       pub required: bool,
       pub default: Option<Value>,
       pub description: Option<String>,
   }

   pub enum InputType { String, Number, Boolean }

   pub struct SecretDefinition {
       pub required: bool,
       pub description: Option<String>,
   }

   pub struct OutputDefinition {
       pub value: String,           // expression like "${{ jobs.test.outputs.y }}"
       pub description: Option<String>,
   }
   ```

2. **Parse `on.workflow_call`** in `parse_workflow()` or a new helper:
   - Extract `inputs:`, `secrets:`, `outputs:` from the trigger
   - Validate types are valid enum values

3. **Add `workflow_call_trigger` field to `Workflow`**:
   ```rust
   pub workflow_call: Option<WorkflowCallTrigger>,
   ```

### Phase 2: Parser — Expand with Validation

**Files**: `crates/aksh-gha-parser/src/lib.rs`

Rewrite `expand_jobs_with_reusables()`:

1. **Validate `on: workflow_call`**: the called workflow must have this trigger. Error if missing.

2. **Depth tracking**: add `depth` parameter (default 0), error if `depth >= 4`.

3. **Input validation and defaults**:
   - For each callee input definition:
     - If caller provides `with.<name>` (case-insensitive match) → use caller's value
     - Else if definition has `default:` → use default
     - Else if `required: true` → error
     - Else → use type zero-value (empty string / 0 / false)
   - Error if caller provides an input not declared by callee

4. **Secret validation**:
   - If `secrets: inherit` → mark `secrets_inherit = true`, skip validation
   - Otherwise, for each callee secret definition:
     - If caller provides `secrets.<name>` (case-insensitive) → ok
     - Else if `required: true` → error
   - Error if caller provides a secret not declared by callee

5. **Inject inputs into expanded jobs**: add an `inputs` field to `JobPlan`:
   ```rust
   pub inputs: BTreeMap<String, serde_json::Value>,
   ```
   Populated from the validated/defaulted input values.

6. **Store output definitions**: add a `reusable_outputs` field to some tracking structure (or return alongside the plans) so the server can evaluate outputs after inner jobs complete:
   ```rust
   pub struct ReusableCallMetadata {
       pub caller_job_id: String,
       pub output_definitions: BTreeMap<String, String>, // name → value expression
       pub inner_job_ids: Vec<JobId>,
   }
   ```

7. **Propagate caller's `needs`**: if the `uses:` job has `needs: [build]`, all expanded inner jobs should also depend on `build`:
   ```rust
   for inner_plan in &mut called_plans {
       for outer_need in &job.needs.ids() {
           if !inner_plan.needs.contains(outer_need) {
               inner_plan.needs.push(outer_need.clone());
           }
       }
   }
   ```

8. **Recurse with depth**: when an inner job is itself a `uses:`, recurse with `depth + 1`.

### Phase 3: Protocol — Wire the Inputs

**Files**: `crates/aksh-gha-protocol/src/lib.rs`

1. **Add `inputs` to `JobPlan`**:
   ```rust
   #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
   pub inputs: BTreeMap<String, serde_json::Value>,
   ```

2. **Add `workflow_file` to `JobPlan`** (for `job.workflow_ref`):
   ```rust
   #[serde(default, skip_serializing_if = "Option::is_none")]
   pub workflow_file: Option<String>,
   ```

### Phase 4: Server — Inject Inputs into Job Message

**Files**: `crates/aksh-runner-server/src/lib.rs`

When building the `AgentJobRequestMessage` for an expanded reusable job:

1. **Populate `contextData.inputs`**: inject the validated inputs as a typed-dict in `contextData`:
   ```rust
   if !plan.inputs.is_empty() {
       let inputs_map = plan.inputs.iter().map(|(k, v)| {
           // Build AzDO typed-dict entries
       }).collect();
       context_data["inputs"] = inputs_map;
   }
   ```

2. **Set `system.workflowFileFullPath`** variable to the called workflow path.

3. **Set `job.workflow_ref`**, `job.workflow_sha`, `job.workflow_repository`** in contextData.

### Phase 5: Server — Output Propagation

**Files**: `crates/aksh-runner-server/src/lib.rs`

This is the hardest server-side change. When all inner jobs of a reusable call complete:

1. **Track reusable call metadata**: when queueing expanded inner jobs, store the `ReusableCallMetadata` (caller job ID, output definitions, inner job IDs).

2. **Detect completion**: when a job completes, check if it's the last inner job of a reusable call.

3. **Evaluate output expressions**: when all inner jobs are done, evaluate each output definition's `value:` expression with a context containing `jobs.<inner_id>.outputs.<name>` from completed jobs.

4. **Store as caller outputs**: the evaluated outputs become available as `needs.<caller_job_id>.outputs.<name>` for downstream jobs.

5. **Trigger downstream**: resume any jobs that had `needs: [<caller_job_id>]`.

### Phase 6: Runner — Input Context (minimal)

**Files**: `crates/aksh-runner/src/worker/job_extension.rs`, `crates/aksh-runner/src/worker/contexts.rs`

The runner already handles `contextData.inputs` via `decode_typed_value()`. Verify:

1. `inputs` context is decoded from `contextData` and available in expression evaluation
2. `${{ inputs.x }}` resolves correctly in step scripts
3. `job.workflow_ref`, `job.workflow_sha` context fields are populated from variables

### Phase 7: Remote Workflow Refs

**Files**: `crates/aksh-gha-parser/src/lib.rs`, `crates/aksh-runner-server/src/lib.rs`

Support `owner/repo/.github/workflows/x.yml@ref`:

1. **Parse remote ref**: extract owner, repo, path, and ref from the `uses` string
2. **Download workflow YAML**: fetch from GitHub API (`GET /repos/{owner}/{repo}/contents/{path}?ref={ref}`) or codeload tarball
3. **Resolve ref to SHA**: `GET /repos/{owner}/{repo}/git/ref/{ref}` → commit SHA for `job.workflow_sha`
4. **Feed into the same expansion pipeline** as local refs

---

## 4. Test Matrix

### Unit tests (parser)

| Test | Fixture | What it validates |
|---|---|---|
| Basic call with defaults | `called_complex.yml` | Inputs with defaults applied, types coerced |
| Explicit inputs | `test_node16_called_complex.yml` | `with:` values override defaults |
| Required secret | `called_with_required_secret.yml` | Error if required secret missing |
| Secrets inherit | `inherit_secrets/` | All parent secrets forwarded |
| Case insensitive | `reusablesCaseInsensitive/` | `inputs.Me` matches definition `me` |
| Vars propagation | `inherit_vars/` | `vars.*` available in called workflow |
| Dynamic runs-on | `called_template_runs_on.yml` | `${{ inputs.runner }}` in `runs-on` |
| Nesting depth | (new) | Error at depth 5 |
| Missing required input | (new) | Error when required input with no default is omitted |
| Unknown input | (new) | Error when caller provides undeclared input |
| Output definitions | `called_complex.yml` | `outputs.x.value` parsed correctly |
| Outer needs propagated | (new) | Inner jobs get caller's `needs` as prerequisites |

### E2E tests (server + runner)

| Test | What it validates |
|---|---|
| Caller → callee, `${{ inputs.x }}` in step | Input arrives in runner context |
| Callee outputs → `needs.call.outputs.x` in downstream job | Full output propagation pipeline |
| `secrets: inherit` → `${{ secrets.MY_SECRET }}` in callee | Secret forwarding |
| Explicit secrets → only declared secrets visible | Secret scoping |
| Nested reusable (2 levels) | Recursive expansion works |
| Matrix on `uses:` job | Matrix expansion of the entire reusable call |
| `workflow_call` trigger missing | Error: called workflow doesn't declare `on: workflow_call` |

---

## 5. Acceptance Criteria

1. `cargo test --workspace --quiet` passes with ≥15 new tests covering the matrix above
2. All fixtures in `fixtures/upstream-workflows/` that involve reusable workflows parse without panics
3. Local E2E: submit `test_node16_called_complex.yml` to aksh-runner-server, verify:
   - 4 jobs expanded: `test`, `test2/test`, `test3/test`, `test4`
   - `test2/test` gets default inputs (x="Hello World", y=235, z=true)
   - `test3/test` gets explicit inputs matching defaults
   - `test4` receives `needs.test3.outputs.x` = "Hello World", `.y` = 235, `.z` = true
   - All jobs succeed (exit 0)
4. `inherit_secrets/main.yml` expands to 3 jobs, callee jobs have `secrets_inherit = true`
5. `reusablesCaseInsensitive` — case-insensitive input matching works
6. Nesting beyond depth 4 produces a clear error, not a stack overflow

---

## 6. Ordering and Dependencies

```
Phase 1 (parser model)
  ↓
Phase 2 (parser expansion) ← depends on Phase 1
  ↓
Phase 3 (protocol wire) ← depends on Phase 2
  ↓
Phase 4 (server input injection) ← depends on Phase 3
  ↓
Phase 5 (server output propagation) ← depends on Phase 4, hardest phase
  ↓
Phase 6 (runner verification) ← depends on Phase 4
  ↓
Phase 7 (remote refs) ← independent of 5/6, depends on Phase 2
```

Phases 1–4 are ~60% of the work. Phase 5 (output propagation) is ~25% — it requires the server to track which jobs belong to which reusable call and evaluate expressions after completion. Phase 7 (remote refs) is ~15% and can be deferred if local-only reusable workflows are sufficient for initial validation.

---

## 7. Risk Areas

1. **Output evaluation timing**: The server must defer output expression evaluation until ALL inner jobs complete. If one inner job fails and `fail-fast` cancels siblings, the outputs may be partial. The official runner evaluates outputs only from successfully completed jobs.

2. **Expression context for outputs**: The output `value:` expressions run in a context with `jobs.<id>.outputs` and `inputs`. The expression engine must handle `fromJSON()` on job outputs (see `called_complex.yml` line 17: `${{fromJSON(jobs.test.outputs.y)}}`).

3. **Needs graph correctness**: Prefixing inner job IDs and rewriting needs must handle diamond dependencies. If caller A needs caller B, and both expand to inner jobs, the dependency graph must remain a DAG.

4. **Matrix on `uses:` job**: If the caller applies `strategy.matrix` to a `uses:` job, each matrix leg expands independently. A 3×2 matrix on a reusable with 2 inner jobs produces 12 expanded jobs.

5. **Concurrency**: The caller's `concurrency:` applies to the entire reusable invocation. The callee's workflow-level `concurrency:` (`EmbeddedConcurrency`) is separate and must also be enforced by the server.
