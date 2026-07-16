# Log Content Comparison Report

### Log Content Comparison: 07-step-failure

| Metric | Official | Aksh |
|---|---|---|
| Lines | 26 | 12 |
| Steps | 4 | 4 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✗ |
| Annotations | ✓ | ✓ |
| Secret masking | ✗ | ✗ |

**Issues:**

- 🔴 Aksh log significantly smaller: 12 vs 26 lines (ratio=0.46)
- 🔴 Aksh logs missing ##[group] markers
- 🔴 Aksh logs missing ##[endgroup] markers
- 🟡 Command ##[endgroup] used 3x in official, 0x in aksh
- 🟡 Command ##[group] used 3x in official, 0x in aksh
- 🟡 Step 'Run echo ran-on-failure': aksh has 1 lines vs official 5 (ratio=0.20)
- 🟡 ##[error] count: official=1, aksh=2

### Log Content Comparison: 10-uses-checkout

| Metric | Official | Aksh |
|---|---|---|
| Lines | 55 | 39 |
| Steps | 5 | 4 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✗ |
| Annotations | ✓ | ✗ |
| Secret masking | ✓ | ✗ |

**Issues:**

- 🟡 Step count: official=5, aksh=4
- 🟡 Step only in official: 'Post Run actions/checkout@v4'
- 🔴 Aksh logs missing ##[group] markers
- 🔴 Aksh logs missing ##[endgroup] markers
- 🔴 Aksh logs missing secret masking (***) 
- 🟡 Aksh logs missing annotations
- 🟡 Command ##[endgroup] used 4x in official, 0x in aksh
- 🟡 Command ##[group] used 4x in official, 0x in aksh
- 🟡 Command ##[warning] used 1x in official, 0x in aksh
- 🟡 Step 'Run echo checked-out': aksh has 1 lines vs official 5 (ratio=0.20)
- 🟡 ##[warning] count: official=1, aksh=0

### Log Content Comparison: 11-cache-roundtrip

| Metric | Official | Aksh |
|---|---|---|
| Lines | 49 | 66 |
| Steps | 6 | 5 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✗ |
| Annotations | ✓ | ✓ |
| Secret masking | ✗ | ✗ |

**Issues:**

- 🟡 Step count: official=6, aksh=5
- 🟡 Step only in official: 'Run mkdir -p .cache-dir && date > .cache-dir/stamp'
- 🔴 Aksh logs missing ##[group] markers
- 🔴 Aksh logs missing ##[endgroup] markers
- 🟡 Command ##[endgroup] used 4x in official, 0x in aksh
- 🟡 Command ##[group] used 4x in official, 0x in aksh
- 🟡 Step 'Run cat .cache-dir/stamp': aksh has 1 lines vs official 5 (ratio=0.20)
- 🟡 ##[warning] count: official=1, aksh=2

### Log Content Comparison: 12-artifact

| Metric | Official | Aksh |
|---|---|---|
| Lines | 74 | 83 |
| Steps | 6 | 5 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✗ |
| Annotations | ✓ | ✗ |
| Secret masking | ✗ | ✗ |

**Issues:**

- 🟡 Step count: official=6, aksh=5
- 🟡 Step only in official: 'Run echo hi > out.txt'
- 🔴 Aksh logs missing ##[group] markers
- 🔴 Aksh logs missing ##[endgroup] markers
- 🟡 Aksh logs missing annotations
- 🟡 Command ##[endgroup] used 5x in official, 0x in aksh
- 🟡 Command ##[group] used 5x in official, 0x in aksh
- 🟡 Command ##[warning] used 1x in official, 0x in aksh
- 🟡 Step 'Run cat dl/out.txt': aksh has 1 lines vs official 5 (ratio=0.20)
- 🟡 ##[warning] count: official=1, aksh=0

### Log Content Comparison: 13-composite-action

| Metric | Official | Aksh |
|---|---|---|
| Lines | 60 | 39 |
| Steps | 5 | 4 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✗ |
| Annotations | ✓ | ✗ |
| Secret masking | ✓ | ✗ |

**Issues:**

- 🟡 Step count: official=5, aksh=4
- 🟡 Step only in official: 'Post Run actions/checkout@v4'
- 🔴 Aksh logs missing ##[group] markers
- 🔴 Aksh logs missing ##[endgroup] markers
- 🔴 Aksh logs missing secret masking (***) 
- 🟡 Aksh logs missing annotations
- 🟡 Command ##[endgroup] used 5x in official, 0x in aksh
- 🟡 Command ##[group] used 5x in official, 0x in aksh
- 🟡 Command ##[warning] used 1x in official, 0x in aksh
- 🟡 Step 'Run ./.github/actions/greet': aksh has 1 lines vs official 10 (ratio=0.10)
- 🟡 ##[warning] count: official=1, aksh=0

### Log Content Comparison: 14-annotations

| Metric | Official | Aksh |
|---|---|---|
| Lines | 25 | 13 |
| Steps | 3 | 3 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✗ |
| Annotations | ✓ | ✓ |
| Secret masking | ✗ | ✗ |

**Issues:**

- 🔴 Aksh log significantly smaller: 13 vs 25 lines (ratio=0.52)
- 🔴 Aksh logs missing ##[group] markers
- 🔴 Aksh logs missing ##[endgroup] markers
- 🟡 Command ##[endgroup] used 2x in official, 0x in aksh
- 🟡 Command ##[group] used 2x in official, 0x in aksh
- 🟡 Command ##[warning] used 1x in official, 0x in aksh
- 🟡 ##[warning] count: official=1, aksh=0

### Log Content Comparison: 15-oidc-id-token

| Metric | Official | Aksh |
|---|---|---|
| Lines | 22 | 12 |
| Steps | 3 | 3 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✗ |
| Annotations | ✓ | ✓ |
| Secret masking | ✓ | ✗ |

**Issues:**

- 🔴 Aksh log significantly smaller: 12 vs 22 lines (ratio=0.55)
- 🟡 Step only in official: 'Run curl -sS -H "Authorization: ***" \'
- 🔵 Step only in aksh: 'Run curl -sS -H "Authorization: Bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \'
- 🔴 Aksh logs missing ##[group] markers
- 🔴 Aksh logs missing ##[endgroup] markers
- 🔴 Aksh logs missing secret masking (***) 
- 🟡 Command ##[endgroup] used 2x in official, 0x in aksh
- 🟡 Command ##[group] used 2x in official, 0x in aksh
- 🟡 ##[error] count: official=1, aksh=2

### Log Content Comparison: 91-large-output

| Metric | Official | Aksh |
|---|---|---|
| Lines | 10075 | 10075 |
| Steps | 6 | 6 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✓ |
| Annotations | ✓ | ✓ |
| Secret masking | ✗ | ✗ |

✅ No log content issues found.

### Log Content Comparison: 92-unicode-special-chars

| Metric | Official | Aksh |
|---|---|---|
| Lines | 126 | 0 |
| Steps | 8 | 0 |
| Timestamps | ✓ | ✗ |
| Groups | ✓ | ✗ |
| Annotations | ✓ | ✗ |
| Secret masking | ✗ | ✗ |

**Issues:**

- 🟡 Step count: official=8, aksh=0
- 🟡 Step only in official: 'Complete job'
- 🟡 Step only in official: 'Retrieve and verify unicode outputs'
- 🟡 Step only in official: 'Set environment with unicode and special chars'
- 🟡 Step only in official: 'Set up job'
- 🟡 Step only in official: 'Test env var with newlines'
- 🟡 Step only in official: 'Test file paths with spaces and unicode'
- 🟡 Step only in official: 'Test hex escape sequences'
- 🟡 Step only in official: 'Test unicode in output variables'
- 🔴 Aksh logs missing timestamps
- 🔴 Aksh logs missing ##[group] markers
- 🔴 Aksh logs missing ##[endgroup] markers
- 🟡 Aksh logs missing annotations
- 🟡 Command ##[endgroup] used 7x in official, 0x in aksh
- 🟡 Command ##[error] used 2x in official, 0x in aksh
- 🟡 Command ##[group] used 7x in official, 0x in aksh
- 🟡 ##[error] count: official=2, aksh=0

### Log Content Comparison: 93-empty-null-values

| Metric | Official | Aksh |
|---|---|---|
| Lines | 176 | 175 |
| Steps | 14 | 13 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✓ |
| Annotations | ✗ | ✗ |
| Secret masking | ✗ | ✗ |

**Issues:**

- 🟡 Step count: official=14, aksh=13
- 🟡 Step only in official: 'Access defined and undefined outputs'
- 🟡 Step only in official: 'Access undefined step output'
- 🟡 Step only in official: 'Set empty string output'
- 🟡 Step only in official: 'Test default value with empty'
- 🟡 Step only in official: 'Test empty string comparison'
- 🟡 Step only in official: 'Test empty string in conditions'
- 🟡 Step only in official: 'Test empty string in matrix (simulated)'
- 🟡 Step only in official: 'Test empty string vs null handling'
- 🟡 Step only in official: 'Test null output field access'
- 🟡 Step only in official: 'Test step output that is never set'
- 🟡 Step only in official: 'Test unset env var reference'
- 🟡 Step only in official: 'Verify empty string output'
- 🔵 Step only in aksh: 'Run # Intentionally don't set never_set_var'
- 🔵 Step only in aksh: 'Run # Set one output but not another'
- 🔵 Step only in aksh: 'Run # Simulate matrix with empty value'
- 🔵 Step only in aksh: 'Run # Unset env var should be empty/null'
- 🔵 Step only in aksh: 'Run ${{ format('DEFINED=''{0}'''
- 🔵 Step only in aksh: 'Run ${{ format('EMPTY=''{0}'''
- 🔵 Step only in aksh: 'Run ${{ format('UNDEFINED=''{0}'''
- 🔵 Step only in aksh: 'Run EMPTY=""'
- 🔵 Step only in aksh: 'Run EMPTY_VAR=""'
- 🔵 Step only in aksh: 'Run echo 'empty_var=' >> $GITHUB_OUTPUT'
- 🔵 Step only in aksh: 'Run if [[ "$EXPLICIT_EMPTY" == "" ]]; then'

---
**Total**: 27 scenarios compared, 104 issues found