# Log Content Comparison Report

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
**Total**: 3 scenarios compared, 41 issues found