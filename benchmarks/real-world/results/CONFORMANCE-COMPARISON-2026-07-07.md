# Conformance Comparison: aksh-runner vs Official C# Runner

**Control Plane:** GitHub  
**VMs:** smolvm ARM64 4CPU/8GB  
**Date:** 2026-07-07  

## Results

### BOTH PASS (8) — aksh-runner matches official behavior

| Scenario | What it tests |
|----------|---------------|
| 83-local-node-action | `uses: ./path` with node20 action |
| 87-multiline-output | Heredoc syntax in GITHUB_OUTPUT |
| 88-state-and-post | GITHUB_STATE + post step lifecycle |
| 93-empty-null-values | Empty strings, unset vars, -z/-n |
| 94-action-pinning | Tag vs SHA vs branch resolution |
| 95-nested-composite | Deep composite output propagation |
| 96-env-inheritance | workflow > job > step override ordering |
| 100-tool-cache | runner.tool_cache + setup-node |

### BOTH FAIL (4) — test YAML bugs, not runner issues

| Scenario | Root Cause |
|----------|------------|
| 82-reusable-workflow | Secrets not declared in caller workflow_call |
| 86-environment-deployments | Hardcoded wrong env name in assertion (staging vs production) |
| 89-workflow-inputs | `gh workflow run` doesn't pass typed inputs without -f flags |
| 90-shell-exit-behavior | Pipefail test assertions wrong (`false \| true` returns 1 with pipefail) |

### AKSH-ONLY FAILURE (1) — real gap

| Scenario | Gap | Severity |
|----------|-----|----------|
| 85-permissions-scoping | Job-level `permissions:` not applied to GITHUB_TOKEN | P2 |

Official runner passes all 3 jobs (full, read-only, no permissions). aksh-runner fails because job-level permissions are not implemented.

### OFFICIAL-ONLY FAILURES (2) — needs investigation

| Scenario | Official Failure | aksh Behavior |
|----------|-----------------|---------------|
| 91-large-output | 50KB output var or 1000-iteration loop fails | Passes — may process output differently |
| 92-unicode-special-chars | `Invalid value. Matching delimiter not found 'EOF'` — printf hex escapes break GITHUB_OUTPUT heredoc parsing | Passes — more lenient output file parsing |

These need deeper investigation: aksh may be more lenient in output parsing (accepting malformed heredoc delimiters), which could mask bugs.

### INCONCLUSIVE (6) — infrastructure limitations

| Scenario | Reason |
|----------|--------|
| 80-custom-shells | 3 jobs, not enough runners connected |
| 81-step-timeout | Run cancelled before 120s timeout triggered |
| 84-concurrency-groups | Skipped (inherently tests cancellation) |
| 97-artifact-cross-job | 2 jobs, second job queued with no runner |
| 98-outcome-vs-conclusion | Official "fails" by design — test has intentional failure step |
| 99-workspace-defaults | Official: success. aksh: not tested |

## Summary

- **Tested:** 15 scenarios with both runners
- **Match:** 12/15 (80%) identical behavior
- **Test bugs:** 4 workflows need YAML fixes
- **Real gaps:** 1 (permissions scoping, P2)
- **Interesting:** 2 cases where aksh passes but official fails

## Run IDs

Batch 1 (official): 28898391333, 28898395135, 28898399134, 28898402861
Batch 2 (official): 28898511712, 28898515348, 28898519251, 28898523050
Batch 3 (official): 28898566813, 28898571117, 28898575465, 28898579992
Batch 4 (official): 28898911605, 28898915542, 28898919306, 28898922835
Batch 5 (official): 28899246990
Multi-job 80: 28899549572
Multi-job 81: 28899843432
Multi-job 97: 28900137433
