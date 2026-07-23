# CLI, Failure Debugging, and Reproducibility

## Product promise

Preloop should not stop at “run GitHub Actions locally.” Its distinctive loop is:

> Run CI before pushing. When it fails, open the machine, rewind the failing step, fork experiments, and share an exact redacted reproduction.

The CLI should expose CI concepts — workflow, run, job, step, failure, checkpoint, and capsule — rather than generic VM internals. smolvm is the execution substrate, but users should not need to manage vsock ports, disk overlays, or VMM processes.

## CLI principles

1. **The common path is one command.** `preloop run` discovers and runs the relevant GitHub Actions workflows.
2. **Defaults are safe and explainable.** Network, secrets, source mounts, caches, and retained state must be visible in the run plan.
3. **Every destructive action is explicit.** Reaping a preserved failure, deleting a capsule, or clearing cache is never an accidental side effect of inspection.
4. **Human and machine interfaces agree.** Commands support structured JSON/NDJSON without maintaining a second semantic API.
5. **A run remains addressable.** `current`, `last`, and `last-failed` are conveniences; stable run/job/checkpoint IDs are the durable interface.
6. **Local and remote execution use the same workflow model.** Placement changes; workflow semantics do not.
7. **Unsupported fidelity is reported, not silently approximated.** `plan`, `run`, and `compare` surface compatibility warnings.

## Initial command surface

### Run workflows

```text
preloop run
preloop run -f .github/workflows/ci.yml
preloop run -f ci.yml
preloop run -f ci.yml --job test
preloop run -f ci.yml --matrix os=ubuntu-latest
preloop run -f ci.yml --event pull_request --base main
preloop run --affected
preloop run --preserve-on-failure
preloop watch
```

Workflow selection:

- `-f <path>` — workflow file path, required when targeting a specific workflow. A bare filename like `ci.yml` resolves inside `.github/workflows/`; a full path is used as-is.
- `--job <job>` — select a single job by its job ID (the YAML key under `jobs:`). Requires `-f`. Includes the dependency closure required by `needs:`.

When `-f` is omitted, `preloop run` discovers and runs all workflows in `.github/workflows/` whose triggers match the current repository state and selected event.

Expected behavior:

- `-f` bypasses workflow discovery but does not bypass job semantics (matrix, DAG, and conditions still apply).
- `--matrix` narrows a matrix dimension and fails clearly if the axis or value does not exist.
- `--event pull_request --base main` constructs the event context and candidate source state without modifying the checked-out branch.
- `--preserve-on-failure` retains the failed job VM under a bounded retention policy.
- `watch` reruns safely selected workflows/jobs after source changes; it must explain why each unit was selected.

### Inspect and control runs

```text
preloop status
preloop run list
preloop run inspect <run>
preloop run logs <run> [--job <job>] [--step <step>]
preloop run cancel <run>
preloop run delete <run>
```

Convenience aliases:

```text
preloop logs                 # preloop run logs current-or-last
preloop cancel               # preloop run cancel current
```

`run inspect` should show:

- workflow, event, source revision, and dirty patch digest;
- expanded job DAG and matrix cells;
- local/remote placement and VM/image identity;
- timeline, conclusions, annotations, outputs, and artifacts;
- active network, secret, filesystem, cache, and retention policies;
- checkpoints and whether the failed VM is still available;
- fidelity warnings and reproducibility status.

### Plan and explain

```text
preloop plan
preloop plan ci --event pull_request --base main
preloop plan ci --json
preloop affected --base origin/main
preloop affected --explain
preloop affected --why <job>
preloop affected --why-not <job>
preloop compare github --run <github-run-id>
```

`plan` performs trigger matching, expression/context evaluation where appropriate, matrix expansion, and job-DAG construction without executing jobs. Secret values must never appear in plan output.

`affected` begins conservatively with workflow path filters, job dependencies, repository package/workspace dependencies, and explicit project rules. It must not claim arbitrary step-level reuse until Preloop can prove all relevant inputs.

### Debug a failed run

```text
preloop shell last-failed
preloop exec last-failed -- env
preloop sync last-failed
preloop timeline last-failed
preloop rewind last-failed --before "Integration tests"
preloop resume last-failed --from "Integration tests"
preloop fork last-failed --name investigate-timeout
```

Semantics:

- `shell` attaches to the preserved VM through the smolvm guest-agent control channel.
- `exec` runs a non-interactive command in the same environment.
- `sync` applies the current source delta through an explicit workspace synchronization operation; it does not silently overwrite generated guest files.
- `timeline` lists steps and available checkpoints.
- `rewind` creates or selects a new execution branch from a checkpoint. It never mutates the original failure evidence.
- `resume` reruns from the selected boundary and clearly marks the result as an investigative run, not a clean CI verdict.
- `fork` creates an independent copy-on-write investigation branch.

`preloop debug` may exist as an alias for `preloop shell last-failed`, but `shell`, `rewind`, and `fork` should remain distinct operations.

### Failure capsules

```text
preloop capsule create <run-or-job>
preloop capsule inspect <capsule>
preloop capsule run <capsule>
preloop capsule push <capsule>
preloop capsule pull <reference>
preloop capsule delete <capsule>
```

A capsule is a portable, content-addressed reproduction manifest plus the VM state and blobs required to recreate a job. It is not merely a log archive.

Default capsule contents:

- VM pack/overlay identity and required content-addressed blobs;
- normalized job message and workflow source;
- source commit, source-tree digest, and dirty patch digest;
- action, runner, toolchain, and base-image versions/digests;
- non-secret environment metadata;
- job timeline, logs, annotations, outputs, and failure signature;
- cache inputs used by the run and their immutable digests;
- policy manifest for network, filesystem, secrets, and capabilities;
- secret names/references, never plaintext values.

Capsule creation must fail closed if redaction cannot be established. `capsule inspect` should report portability limitations before transfer, including host architecture, unavailable secret references, external service dependencies, and missing blobs.

### Diagnose CI behavior

```text
preloop flake <run> --step <step> --repeat 100 --parallel 12
preloop bisect --good origin/main --bad HEAD --workflow ci --job integration
preloop profile <run> --critical-path
preloop audit <workflow>
preloop audit <run> --explain-network
preloop merge-preview --base main
```

These are differentiating features, not requirements for the first executable alpha:

- **Flake laboratory:** fork repeatable attempts from the same pre-step checkpoint and classify failure signatures.
- **CI bisect:** reuse warm setup state while binary-searching source revisions.
- **Critical-path profile:** separate VM boot, action resolution, dependency download, execution, cache transfer, and DAG idle time.
- **Hermeticity audit:** report observed egress, filesystem capabilities, secret availability, and unpinned dependencies; generate a candidate policy for review.
- **Merge preview:** construct a synthetic candidate merge state and run workflows triggered by `merge_group`; it does not replace GitHub merge-queue orchestration.

### Secrets, variables, images, and workers

```text
preloop secret set <name>
preloop secret list
preloop secret delete <name>

preloop variable set <name> <value>
preloop variable list
preloop variable delete <name>

preloop image list
preloop image pull <image>
preloop image prune

preloop worker start
preloop worker status
preloop worker stop
```

Secrets are reference-based. Listing commands return names and metadata only. Untrusted jobs receive no real secrets unless a reviewed policy explicitly grants them.

The worker commands are for Preloop capacity, not generic VM management. Local-to-remote spillover should preserve the same run and job IDs:

```text
preloop run --remote
preloop run --strategy spillover
preloop run --strategy fastest
```

## Flagship user journey

```text
# Simulate the pull-request workflow locally and retain failures.
preloop run --event pull_request --base main --preserve-on-failure

# Inspect the exact failed environment.
preloop shell last-failed

# Rewind without destroying the original evidence.
preloop rewind last-failed --before "Integration tests"
preloop sync last-failed
preloop resume last-failed --from "Integration tests"

# Test whether it is flaky from an identical checkpoint.
preloop flake last-failed --step "Integration tests" --repeat 50

# Produce a redacted, shareable reproduction.
preloop capsule create last-failed
preloop capsule push last-failed
```

A resumed or modified investigation must not be reported as a clean verification. Before Preloop produces a final “fixed” verdict, it runs the relevant workflow from a clean base image/pack with a clean policy and no investigative mutations.

## Reproducibility model

Reproducibility is not “the same source commit ran twice.” A CI run is reproducible only when Preloop can identify or reconstruct every input that may affect observable behavior.

A microVM supplies isolation and a controllable filesystem boundary; it does not, by itself, make execution reproducible. The result is better modeled as a function of all material inputs:

```text
result = f(
  source,
  workflow,
  event,
  runner,
  VM image,
  actions,
  dependencies,
  cache,
  policy,
  network,
  secrets,
  external state
)
```

If an input is known only by a mutable name, replaying that name later may resolve to different bytes. A weak record looks like:

```text
commit: HEAD
image: ubuntu-latest
action: actions/checkout@v4
cache: cargo-linux-v3
```

A reproducible record resolves those names to immutable identities:

```text
source tree:
  sha256:19ab...

dirty patch:
  sha256:b33f...

workflow:
  sha256:4c01...

runner image:
  OCI manifest sha256:89de...

actions/checkout@v4:
  Git commit: 11bd719...
  archive sha256:c291...

restored cache:
  key: cargo-linux-v3
  object sha256:50aa...
```

Preloop therefore needs both an immutable input manifest and content-addressed storage for the bytes referenced by that manifest. A replay retrieves objects by digest. If an exact object is unavailable, replay stops with a precise missing-input error rather than silently resolving the mutable name again.

### Reproducibility levels

Preloop should report a level rather than a misleading boolean:

| Level | Meaning |
|---|---|
| `recorded` | Run metadata and declared inputs were captured, but mutable external dependencies may remain. |
| `replayable` | Required VM state and content-addressed blobs are available; the job can be restarted with equivalent declared inputs. |
| `hermetic` | Undeclared host access is absent, network is disabled or replayed/fully pinned, and all material inputs are content-addressed. |
| `verified` | A replay from a clean environment produced the expected conclusion and declared output digests. |

A run may be useful and replayable without being perfectly deterministic. Preloop must state which conditions prevent a stronger level.

### Input manifest

Every run should produce an immutable manifest with at least:

1. **Source identity**
   - repository identity;
   - Git commit/tree digest;
   - submodule revisions;
   - dirty working-tree patch digest and, when permitted, encrypted/content-addressed patch blob;
   - synthetic merge commit/tree for pull-request or merge-group previews.
2. **Workflow identity**
   - workflow file digest;
   - normalized expanded job description;
   - event payload digest;
   - evaluated matrix cell, contexts, inputs, variables, and permission policy;
   - secret reference IDs and versions, not secret values.
3. **Execution identity**
   - Preloop, aksh, runner, smolvm, guest-agent, and protocol versions;
   - host and guest architecture;
   - VM base pack/rootfs and writable overlay parent digests;
   - CPU/memory configuration and relevant emulation mode such as Rosetta;
   - clock, timezone, locale, and random-seed policy where controlled.
4. **Dependency identity**
   - resolved GitHub Action commit SHAs and downloaded archive digests;
   - OCI image manifest/config/layer digests;
   - toolchain archives and package-cache object digests;
   - cache key plus exact restored cache object digest;
   - service image digests and startup configuration.
5. **Capability identity**
   - network policy and observed destinations;
   - filesystem mounts, paths, access modes, and source snapshot digests;
   - forwarded sockets/capabilities;
   - secret grants and token scopes;
   - external service declarations.
6. **Result identity**
   - step/job conclusions and exit status;
   - normalized logs and annotations;
   - declared outputs;
   - artifact names, sizes, and content digests;
   - resulting checkpoint/overlay digest;
   - failure signature when applicable.

The manifest itself is canonically encoded and hashed. Any mutable run record points to this immutable manifest digest.

The manifest is the shared backbone for replay, capsules, clean verification, remote handoff, CI bisect, flake experiments, cache provenance, artifact attestation, and GitHub-versus-local comparison:

```text
RunRecord
  id: run_01...
  state: completed
  manifest_digest: sha256:7fd2...

Manifest sha256:7fd2...
  +-- source snapshot sha256:19ab...
  +-- dirty patch sha256:b33f...
  +-- workflow and event sha256:4c01...
  +-- VM base sha256:89de...
  +-- action archives
  |     +-- sha256:c291...
  |     +-- sha256:721a...
  +-- restored cache sha256:50aa...
  +-- policy and capability manifest
  +-- result metadata
  +-- artifact digests
```

The run record may change state while work is executing, but it must not become the sole historical description of what ran. Final execution evidence points to immutable manifests and immutable blobs.

### Content-addressed storage

Mutable names such as `ubuntu-latest`, `actions/checkout@v4`, a cache key, or `main` are convenient user references, not reproducible identities. Preloop resolves them once and records immutable identities:

```text
ubuntu-latest
  -> OCI manifest sha256:...

actions/checkout@v4
  -> Git commit 11bd719...
  -> archive sha256:...

cargo-linux-v3
  -> cache object sha256:...
```

Blobs are stored by digest. A replay either retrieves the exact blob or stops with a precise missing-input error. It must not silently fetch a newer object under the same mutable name.

### Source and workspace capture

A local run commonly includes uncommitted changes. Recording only `HEAD` would produce a false reproduction. Preloop should:

1. record `HEAD` and the Git tree digest;
2. construct a canonical patch or source snapshot for tracked modifications;
3. separately account for untracked files according to explicit include/ignore rules;
4. hash the effective guest workspace after synchronization;
5. reject or downgrade reproducibility when files are omitted but may affect the job.

The host workspace should not remain a mutable read/write input during a reproducible run. A deterministic source snapshot or staged copy is safer than observing a directory that can change while the job is running.

### Network and external state

Network access is the largest reproducibility leak. A URL does not identify the bytes returned from it, and remote APIs may change behavior without changing URLs.

Preloop should support three modes:

- **Offline:** no network; every required object must already exist by digest.
- **Pinned egress:** only reviewed destinations are reachable; downloaded objects are hashed and retained, but arbitrary API responses can still make the run merely `recorded` or `replayable`.
- **Recorded/replayed egress:** selected dependency downloads are served from a content-addressed proxy. General TLS/API traffic is not claimed reproducible unless Preloop can safely capture and replay it.

Do not present an allowlist alone as hermeticity. It limits authority but does not make remote responses immutable.

### Secrets and tokens

Plaintext secrets must never be written into manifests, capsules, logs, checkpoints intended for sharing, or cache objects. Reproduction records:

- logical secret name;
- secret-store provider;
- immutable version ID where supported;
- scope and recipient job/step;
- whether the secret was actually requested or merely available.

A replay requiring a secret resolves it again through the local/team secret broker. If the exact version is unavailable, Preloop reports that the replay is not cryptographically identical. Short-lived GitHub/OIDC tokens are regenerated with equivalent claims and scopes; token bytes and timestamps are expected to differ and are normalized as volatile protocol fields.

Before exporting a capsule, Preloop should scan:

- filesystem changes since the base checkpoint;
- logs and annotations;
- environment snapshots;
- shell history and temporary files;
- action and package-manager credential files;
- cache writes and artifacts.

If safe redaction cannot be proven, export is blocked rather than producing a partially scrubbed capsule.

### Time, randomness, and concurrency

Not every workflow can be deterministic. Time, random numbers, process scheduling, race conditions, external services, and CPU architecture can affect output. Preloop should record these conditions and control them only where doing so does not break GitHub Actions semantics.

Safe initial behavior:

- record start/finish timestamps but normalize them for semantic comparisons;
- record timezone and locale;
- pass through explicit seeds and record them;
- record CPU count, architecture, memory limit, and runner image;
- preserve matrix/job scheduling history;
- label nondeterministic external dependencies explicitly.

Freezing time or replacing randomness should be opt-in diagnostic behavior, never the default CI execution mode.

### Clean verification

Interactive debugging changes the machine and may alter source, environment, services, or caches. Therefore:

1. the original failed checkpoint is immutable;
2. each rewind/fork creates an investigation branch;
3. investigative runs are visibly marked and cannot satisfy a required check;
4. after a candidate fix passes, Preloop executes the relevant workflow from a clean base;
5. the clean run uses the recorded/pinned inputs and reviewed policy;
6. Preloop compares conclusions and declared output/artifact digests;
7. only the clean run may receive `verified` status.

This separates fast iteration from trustworthy evidence.

### Reproducibility report

```text
$ preloop run inspect last --reproducibility

Level: replayable
Manifest: sha256:7fd2...

Pinned:
  source tree                 sha256:19ab...
  workflow                    sha256:4c01...
  runner image                sha256:89de...
  7 GitHub actions            commit + archive digest
  restored cache              sha256:50aa...

Volatile or external:
  api.stripe.com response     not recorded
  OIDC token                  regenerated with equivalent claims
  wall clock                  recorded, not frozen

To reach hermetic:
  deny api.stripe.com or provide a replay fixture

Clean replay:
  not yet run
```

The report should always explain both the achieved level and the blockers to the next level.

### First implementation boundary

Do not begin with arbitrary TLS recording, frozen clocks, or a claim of universally deterministic execution. The first useful and honest slice is:

1. Capture the exact source state, including tracked modifications, staged changes, relevant untracked files, and submodule revisions.
2. Hash the effective workspace after synchronization into the guest.
3. Record workflow and event-payload digests plus the expanded job identity.
4. Record Preloop, aksh, runner, smolvm, guest-agent, protocol, host-architecture, and guest-architecture versions.
5. Resolve GitHub Actions to immutable commits and archive digests.
6. Resolve OCI images to manifest, config, and layer digests.
7. Record the exact restored cache object, not only its lookup key.
8. Record network, mount, token, secret, and capability policies and any observed external dependencies.
9. Keep the original failed checkpoint immutable; every rewind or fork creates an investigation branch.
10. Execute a clean verification after interactive debugging before reporting the fix as verified.

This is sufficient to produce truthful `recorded` and, when all referenced blobs are retained, `replayable` runs. Hermetic execution, dependency-response replay, and signed attestations can be layered on the same manifest without changing the identity model.

## Delivery order

### Local alpha

1. `run`, `plan`, `status`, `logs`, and `cancel`.
2. One smolvm microVM per job through the Preloop provider/orchestrator seam.
3. Preserve failed VM with bounded retention.
4. `shell`, `exec`, and clean reap.
5. Immutable input manifest with source/workflow/runner/image/action/cache digests.
6. Explicit clean verification.

### Differentiating release

1. Checkpoints around selected steps and failed-state preservation.
2. Immutable rewind/fork investigation branches.
3. Local capsules with redaction gate.
4. Conservative affected-job selection with explanations.
5. Merge-group preview.
6. Flake laboratory.

### Team and remote release

1. Remote workers and local-to-remote spillover.
2. Shared encrypted/content-addressed capsule storage.
3. Team secret broker with versioned references.
4. Failure-signature history.
5. Hermeticity audit and policy generation.
6. Signed reproducibility attestations and clean replay verification.

## Explicit non-goals for the first release

- Generic VM management.
- A replacement workflow language.
- A local replacement for GitHub merge-queue orchestration.
- Transparent reuse of arbitrary workflow steps without complete input evidence.
- Claims of deterministic execution merely because a VM was used.
- Exporting secret-bearing VM state through best-effort redaction.
- Requiring Firecracker or a remote control plane for the local product.
