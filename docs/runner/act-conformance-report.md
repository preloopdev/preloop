# nektos/act Compatibility Report for Conformance Workflows

| Workflow File | Status | Detail / Error |
|---|---|---|
| `fixtures/upstream-workflows/actions_artifacts_v4/.github/workflows/artifactv4.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/actions_checkout_v1/.github/workflows/test.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/cache-save-restore-order-tests/.github/workflows/a.yml` | ❌ FAIL | Error: reference not found | *DRYRUN* [a.yml/testa] Unable to resolve v4: reference not found |
| `fixtures/upstream-workflows/cache-save-restore-order-tests/.github/workflows/b.yml` | ❌ FAIL | Error: reference not found | *DRYRUN* [b.yml/testa] Unable to resolve v4: reference not found |
| `fixtures/upstream-workflows/case-insensitive-keys-matrix/.github/workflows/test.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/case_insensitive_needs/.github/workflows/case-insensitive-needs.yml` | ❌ FAIL | "2026-07-04T20:55:13-04:00" msg="unable to build dependency graph for case-insensitive-needs.yml (case-insensitive-needs.yml)" | "2026-07-04T20:55:13-04:00" msg="unable to build dependency graph for case-insensitive-needs.yml (case-insensitive-needs.yml)" |
| `fixtures/upstream-workflows/db-disposed-issue/.github/workflows/expressions-in-environment-name.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/db-disposed-issue/.github/workflows/expressions-in-environment.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/db-disposed-issue/.github/workflows/has-advanced-status-functions.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/db-disposed-issue/.github/workflows/has-inputs-in-concurrency.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/db-disposed-issue/.github/workflows/has-inputs-in-job-concurrency.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/db-disposed-issue/.github/workflows/has-recursive-needsctx.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/inherit_secrets/.github/workflows/main.yml` | ❌ FAIL | Error: stat /Users/bnjoroge/container-support/.github/workflows/x.yml: no such file or directory |
| `fixtures/upstream-workflows/inherit_secrets/.github/workflows/x.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/inherit_secrets/.github/workflows/y.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/inherit_vars/.github/workflows/main.yml` | ❌ FAIL | Error: stat /Users/bnjoroge/container-support/.github/workflows/x.yml: no such file or directory |
| `fixtures/upstream-workflows/inherit_vars/.github/workflows/x.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/linux-container-i386/.github/workflows/exec-node-js-action.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/linux-container-problem-matcher-test1/.github/workflows/exec-action.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/matrix-eq-test/.github/workflows/matrix-eq-test.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/matrix-partial-test/.github/workflows/matrix-partial-test.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/matrix-selector/.github/workflows/test.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/oidc-provider/.github/workflows/oidc.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/reusable-workflows-secrets-inherit-with-required-secrets/.github/workflows/main.yml` | ❌ FAIL | Error: stat /Users/bnjoroge/container-support/.github/workflows/required-environment-secrets.yml: no such file or directory |
| `fixtures/upstream-workflows/reusable-workflows-secrets-inherit-with-required-secrets/.github/workflows/required-environment-secrets.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/reusablesCaseInsensitive/.github/workflows/main.yml` | ❌ FAIL | Error: stat /Users/bnjoroge/container-support/.github/workflows/reusable.yml: no such file or directory |
| `fixtures/upstream-workflows/reusablesConsistentWorkflowName/.github/workflows/main-custom-name-run-name.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/reusablesConsistentWorkflowName/.github/workflows/main-custom-name.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/reusablesConsistentWorkflowName/.github/workflows/main.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/windows-add-path/.github/workflows/test.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/windows-container-test1/.github/workflows/exec-action.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/windows-container-test2/.github/workflows/exec-action.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/windows-container-test3-invalid-problem-matcher/.github/workflows/exec-action.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/workflow_dispatch/.github/workflows/dispatch.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/workflow_ref_and_job_workflow_ref/.github/workflows/assert.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/workflow_ref_and_job_workflow_ref/.github/workflows/test.yml` | ❌ FAIL | Error: stat /Users/bnjoroge/container-support/.github/workflows/assert.yml: no such file or directory |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 10.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 10.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 11.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 11.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 12.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 12.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 13.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 13.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 14.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 14.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 15.yml` | ✅ PASS |  |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 2.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 2.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 3.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 3.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 4.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 4.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 5.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 5.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 6.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 6.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 7.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 7.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 8.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 8.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy 9.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy 9.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors copy.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors copy.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |
| `fixtures/upstream-workflows/workflowerrors/.github/workflows/test-errors.yml` | ❌ FAIL | Error: workflow is not valid. 'test-errors.yml': Line: 4 Column 5: Failed to match job-factory: Line: 6 Column 7: Failed to match run-step: Line: 6 Column 7: Unknown Property ru n | Line: 6 Column 7: Failed to match regular-step: Line: 6 Column 7: Unknown Property ru n |