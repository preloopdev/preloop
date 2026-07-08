# Runner Conformance Comparison Report

Generated from conformance JSONL data.
Official scenarios: 19, Aksh scenarios: 21

## Summary Matrix

| # | Scenario | Official | Aksh | Match | Issues |
|---|---|---|---|---|---|
| 80 | Custom Shells | (empty) | failure | ⏳ | incomplete-run |
| 81 | Step Timeout | (empty) | failure | ⏳ | incomplete-run |
| 82 | Reusable Workflow | failure | cancelled | ❌ | conclusion-mismatch, job-conclusion-mismatch, no-aksh-steps |
| 83 | Local Node Action | (empty) | success | ⏳ | duplicate-steps, incomplete-run, step-display-name |
| 84 | Concurrency Groups | N/A | cancelled | ⏳ | no-official-data |
| 85 | Permissions Scoping | (empty) | (empty) | ✅ |  |
| 86 | Environment Deployments | (empty) | (empty) | ✅ |  |
| 87 | Multiline Output | success | (empty) | ⏳ | incomplete-run, no-aksh-steps |
| 88 | State and Post Step | success | (empty) | ⏳ | incomplete-run, no-aksh-steps |
| 89 | Workflow Inputs | failure | (empty) | ⏳ | incomplete-run, no-aksh-steps |
| 90 | Shell Exit Behavior | failure | failure | ✅ | duplicate-steps |
| 91 | Large Output | failure | success | ❌ | conclusion-mismatch, duplicate-steps, job-conclusion-mismatc |
| 92 | Unicode Special Chars | failure | success | ❌ | conclusion-mismatch, duplicate-steps, job-conclusion-mismatc |
| 93 | Empty/Null Values | success | success | ✅ | duplicate-steps, step-display-name |
| 94 | Action Pinning | success | success | ✅ | duplicate-steps, step-display-name |
| 95 | Nested Composite | success | (empty) | ⏳ | incomplete-run, no-aksh-steps |
| 96 | Env Inheritance | success | cancelled | ❌ | conclusion-mismatch, duplicate-steps, job-conclusion-mismatc |
| 97 | Artifact Cross-Job | N/A | (empty) | ⏳ | no-official-data |
| 98 | Outcome vs Conclusion | failure | failure | ✅ | duplicate-steps, step-conclusion |
| 99 | Workspace Defaults | (empty) | (empty) | ✅ |  |
| 100 | Tool Cache | success | success | ✅ | duplicate-steps |

**Totals**: 8 matching, 4 mismatched, 9 incomplete/missing

## Detailed Comparison

### 80 — Custom Shells

- Official run: 28899549572
- Aksh run: 28917121192
- Conclusions: official=(empty), aksh=failure

- Official runner did not complete

### 81 — Step Timeout

- Official run: 28899843432
- Aksh run: 28917123635
- Conclusions: official=(empty), aksh=failure

- Official runner did not complete

### 82 — Reusable Workflow

- Official run: 28898915542
- Aksh run: 28917126105
- Conclusions: official=failure, aksh=cancelled

- Conclusion: official=failure, aksh=cancelled
- Job 'call-reusable / reusable-job': official=failure, aksh=cancelled
- Job 'call-reusable / reusable-job': official has 6 steps, aksh has none (did not run?)

### 83 — Local Node Action

- Official run: 28898919306
- Aksh run: 28895382623
- Conclusions: official=(empty), aksh=success

- Official runner did not complete
- Aksh has 2 duplicate step entries
- Step 2 name: official='Run actions/checkout@v4' vs aksh='actions/checkout@v4'
- Step 3 name: official='Create test-actions directory structure' vs aksh='Run mkdir -p test-actions/node-action'
- Step 4 name: official='Create action.yml' vs aksh='Run cat > test-actions/node-action/action.yml <<'EOF''
- Step 5 name: official='Create index.js' vs aksh='Run cat > test-actions/node-action/index.js <<'EOF''
- Step 6 name: official='Post Run actions/checkout@v4' vs aksh='Post actions/checkout@v4'

### 84 — Concurrency Groups

- Aksh run: 28917485908
- Conclusions: official=N/A, aksh=cancelled

### 87 — Multiline Output

- Official run: 28898391333
- Aksh run: 28895532930
- Conclusions: official=success, aksh=(empty)

- Aksh runner did not complete
- Job 'test-multiline-output': official has 5 steps, aksh has none (did not run?)

### 88 — State and Post Step

- Official run: 28898395135
- Aksh run: 28895537410
- Conclusions: official=success, aksh=(empty)

- Aksh runner did not complete
- Job 'test-state-and-post': official has 6 steps, aksh has none (did not run?)

### 89 — Workflow Inputs

- Official run: 28898399134
- Aksh run: 28895541774
- Conclusions: official=failure, aksh=(empty)

- Aksh runner did not complete
- Job 'test-workflow-inputs': official has 9 steps, aksh has none (did not run?)

### 90 — Shell Exit Behavior

- Official run: 28898402861
- Aksh run: 28917463920
- Conclusions: official=failure, aksh=failure

- Aksh has 2 duplicate step entries

### 91 — Large Output

- Official run: 28898511712
- Aksh run: 28895802641
- Conclusions: official=failure, aksh=success

- Conclusion: official=failure, aksh=success
- Job 'test-large-output': official=failure, aksh=success
- Aksh has 2 duplicate step entries
- Step 2 name: official='Generate large stdout output' vs aksh='Run echo "Generating 10000 lines of output (100KB+)..."'
- Step 3 name: official='Generate large output variable' vs aksh='Run echo "Creating 50KB output variable..."'
- Step 3 'Generate large output variable': official=failure, aksh=success
- Step 4 name: official='Verify large output variable retrieval' vs aksh='Run OUTPUT='''
- Step 5 name: official='Test step log size limits' vs aksh='Run echo "Testing if runner handles large logs without truncation..."'
- Step 5 'Test step log size limits': official=skipped, aksh=success
- Step 6 name: official='Verify runner didn't crash' vs aksh='Run echo "✓ Runner survived large output operations"'

### 92 — Unicode Special Chars

- Official run: 28898515348
- Aksh run: 28895806286
- Conclusions: official=failure, aksh=success

- Conclusion: official=failure, aksh=success
- Job 'test-unicode-special-chars': official=failure, aksh=success
- Aksh has 2 duplicate step entries
- Step 2 name: official='Set environment with unicode and special chars' vs aksh='Run echo "EMOJI_VAR=$EMOJI_VAR"'
- Step 3 name: official='Test unicode in output variables' vs aksh='Run echo 'emoji_output<<EOF' >> $GITHUB_OUTPUT'
- Step 4 name: official='Retrieve and verify unicode outputs' vs aksh='Run ${{ format('EMOJI=''{0}'''
- Step 5 name: official='Test file paths with spaces and unicode' vs aksh='Run mkdir -p "test dir with spaces"'
- Step 6 name: official='Test env var with newlines' vs aksh='Run echo "Multiline env var:"'
- Step 7 name: official='Test hex escape sequences' vs aksh='Run echo 'hex_output<<EOF' >> $GITHUB_OUTPUT'
- Step 7 'Test hex escape sequences': official=failure, aksh=success
- Step 8 name: official='Verify special character round-trip' vs aksh='Run ${{ format('HEX=''{0}'''
- Step 8 'Verify special character round-trip': official=skipped, aksh=success

### 93 — Empty/Null Values

- Official run: 28898519251
- Aksh run: 28895810084
- Conclusions: official=success, aksh=success

- Aksh has 2 duplicate step entries
- Step 2 name: official='Set empty string output' vs aksh='Run echo 'empty_var=' >> $GITHUB_OUTPUT'
- Step 3 name: official='Verify empty string output' vs aksh='Run EMPTY='''
- Step 4 name: official='Test empty string comparison' vs aksh='Run EMPTY='''
- Step 5 name: official='Test unset env var reference' vs aksh='Run # Unset env var should be empty/null'
- Step 6 name: official='Test step output that is never set' vs aksh='Run # Intentionally don't set never_set_var'
- Step 7 name: official='Access undefined step output' vs aksh='Run ${{ format('UNDEFINED=''{0}'''
- Step 8 name: official='Test empty string in matrix (simulated)' vs aksh='Run # Simulate matrix with empty value'
- Step 9 name: official='Test empty string vs null handling' vs aksh='Run if [[ "$EXPLICIT_EMPTY" == "" ]]; then'
- Step 10 name: official='Test default value with empty' vs aksh='Run EMPTY_VAR=""'
- Step 11 name: official='Test empty string in conditions' vs aksh='Run EMPTY=""'
- Step 12 name: official='Test null output field access' vs aksh='Run # Set one output but not another'
- Step 13 name: official='Access defined and undefined outputs' vs aksh='Run DEFINED='has_value''

### 94 — Action Pinning

- Official run: 28898523050
- Aksh run: 28895813858
- Conclusions: official=success, aksh=success

- Aksh has 2 duplicate step entries
- Step 2 name: official='Test checkout with tag' vs aksh='actions/checkout@v4'
- Step 3 name: official='Echo checkout tag version' vs aksh='Run echo "Tested tag-based action resolution (v4)"'
- Step 4 name: official='Test checkout with SHA pin' vs aksh='actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683'
- Step 5 name: official='Echo checkout SHA version' vs aksh='Run echo "Tested SHA-pinned action resolution"'
- Step 6 name: official='Test checkout with branch' vs aksh='actions/checkout@main'
- Step 7 name: official='Echo checkout branch version' vs aksh='Run echo "Tested branch-based action resolution (main)"'
- Step 8 name: official='Verify all three resolution methods succeeded' vs aksh='Run echo "All action pinning methods completed successfully"'
- Step 9 name: official='Post Test checkout with branch' vs aksh='Post actions/checkout@main'
- Step 10 name: official='Post Test checkout with SHA pin' vs aksh='Post actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683'
- Step 11 name: official='Post Test checkout with tag' vs aksh='Post actions/checkout@v4'

### 95 — Nested Composite

- Official run: 28898566813
- Aksh run: 28895666108
- Conclusions: official=success, aksh=(empty)

- Aksh runner did not complete
- Job 'test-nested-composite': official has 8 steps, aksh has none (did not run?)

### 96 — Env Inheritance

- Official run: 28898571117
- Aksh run: 28917468833
- Conclusions: official=success, aksh=cancelled

- Conclusion: official=success, aksh=cancelled
- Job 'test-env-job-1': official=success, aksh=cancelled
- Job 'test-env-job-1': official has 4 steps, aksh has none (did not run?)
- Aksh has 2 duplicate step entries

### 97 — Artifact Cross-Job

- Aksh run: 28895674600
- Conclusions: official=N/A, aksh=(empty)

### 98 — Outcome vs Conclusion

- Official run: 28898575465
- Aksh run: 28917466595
- Conclusions: official=failure, aksh=failure

- Aksh has 2 duplicate step entries
- Step 3 'Verify success step': official=success, aksh=failure
- Step 4 'Failing step with continue-on-error': official=success, aksh=skipped
- Step 5 'Verify failing step with continue-on-error': official=success, aksh=skipped
- Step 6 'Failing step without continue-on-error': official=failure, aksh=skipped
- Step 8 'Complete job': official=success, aksh=failure

### 100 — Tool Cache

- Official run: 28898911605
- Aksh run: 28917118947
- Conclusions: official=success, aksh=success

- Aksh has 2 duplicate step entries

## Issue Categories

| Issue Type | Count |
|---|---:|
| step-display-name | 39 |
| duplicate-steps | 9 |
| step-conclusion | 9 |
| incomplete-run | 7 |
| no-aksh-steps | 6 |
| conclusion-mismatch | 4 |
| job-conclusion-mismatch | 4 |
| no-official-data | 2 |

## Issue Type Reference

| Issue | Severity | Description |
|---|---|---|
| conclusion-mismatch | 🔴 Critical | Job passed on one runner but failed on the other |
| job-conclusion-mismatch | 🔴 Critical | Individual job conclusion differs |
| step-conclusion | 🟠 High | Step passed/failed differently |
| step-count | 🟡 Medium | Different number of steps executed |
| step-display-name | 🔵 Low | Step name shown differently (display only) |
| step-name-mismatch | 🟡 Medium | Step name differs in a meaningful way |
| duplicate-steps | 🟡 Medium | Aksh reports duplicate step entries |
| incomplete-run | ⚪ Info | One runner did not complete the workflow |
| no-aksh-data | ⚪ Info | Aksh has no data for this scenario |
| no-aksh-steps | 🟠 High | Aksh job has no step data (runner didn't execute) |
