# Log Content Comparison Report

### Log Content Comparison: 101-dynamic-matrix-dataflow

| Metric | Official | Aksh |
|---|---|---|
| Lines | 184 | 150 |
| Steps | 6 | 6 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✓ |
| Annotations | ✗ | ✗ |
| Secret masking | ✗ | ✗ |

✅ No log content issues found.

### Log Content Comparison: 102-failure-needs-conditions

| Metric | Official | Aksh |
|---|---|---|
| Lines | 95 | 91 |
| Steps | 9 | 9 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✓ |
| Annotations | ✓ | ✓ |
| Secret masking | ✗ | ✗ |

✅ No log content issues found.

### Log Content Comparison: 103-cancellation-background-post

| Metric | Official | Aksh |
|---|---|---|
| Lines | 42 | 40 |
| Steps | 5 | 5 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✓ |
| Annotations | ✗ | ✗ |
| Secret masking | ✗ | ✗ |

✅ No log content issues found.

### Log Content Comparison: 104-nested-lifecycle

| Metric | Official | Aksh |
|---|---|---|
| Lines | 71 | 56 |
| Steps | 8 | 8 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✓ |
| Annotations | ✗ | ✗ |
| Secret masking | ✗ | ✗ |

✅ No log content issues found.

### Log Content Comparison: 105-command-logs-annotations

| Metric | Official | Aksh |
|---|---|---|
| Lines | 65 | 61 |
| Steps | 5 | 5 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✓ |
| Annotations | ✓ | ✓ |
| Secret masking | ✓ | ✓ |

✅ No log content issues found.

### Log Content Comparison: 106-cache-artifact-pipeline

| Metric | Official | Aksh |
|---|---|---|
| Lines | 16 | 28 |
| Steps | 1 | 4 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✓ |
| Annotations | ✓ | ✓ |
| Secret masking | ✗ | ✗ |

**Issues:**

- 🟡 Step count: official=1, aksh=4
- 🔵 Step only in aksh: 'Complete job'
- 🔵 Step only in aksh: 'Create unusual files'
- 🔵 Step only in aksh: 'Restore cache'

### Log Content Comparison: 107-remote-action-resolution

| Metric | Official | Aksh |
|---|---|---|
| Lines | 16 | 73 |
| Steps | 1 | 5 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✓ |
| Annotations | ✓ | ✓ |
| Secret masking | ✗ | ✗ |

**Issues:**

- 🟡 Aksh log significantly larger: 73 vs 16 lines (ratio=4.56)
- 🟡 Step count: official=1, aksh=5
- 🔵 Step only in aksh: 'Checkout explicit secondary repository'
- 🔵 Step only in aksh: 'Checkout pinned action source'
- 🔵 Step only in aksh: 'Complete job'
- 🔵 Step only in aksh: 'Execute pinned JavaScript action'

### Log Content Comparison: 108-environment-shell-filesystem

| Metric | Official | Aksh |
|---|---|---|
| Lines | 56 | 45 |
| Steps | 5 | 5 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✓ |
| Annotations | ✗ | ✗ |
| Secret masking | ✗ | ✗ |

✅ No log content issues found.

### Log Content Comparison: 109-dag-matrix-scheduler

| Metric | Official | Aksh |
|---|---|---|
| Lines | 179 | 168 |
| Steps | 7 | 7 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✓ |
| Annotations | ✓ | ✓ |
| Secret masking | ✗ | ✗ |

**Issues:**

- 🟡 Step only in official: 'Run echo "AKSH_ORACLE: final root=success build=success test=success package=success"'
- 🔵 Step only in aksh: 'Run ${{ format('echo "AKSH_ORACLE: final root={0} build={1} test={2} package={3}"', …'

### Log Content Comparison: 110-synthetic-workspace-checkout

| Metric | Official | Aksh |
|---|---|---|
| Lines | 59 | 38 |
| Steps | 5 | 4 |
| Timestamps | ✓ | ✓ |
| Groups | ✓ | ✓ |
| Annotations | ✓ | ✓ |
| Secret masking | ✓ | ✗ |

**Issues:**

- 🟡 Step count: official=5, aksh=4
- 🟡 Step only in official: 'Post Primary checkout'
- 🔴 Aksh logs missing secret masking (***) 
- 🟡 Command ##[warning] used 1x in official, 0x in aksh
- 🟡 ##[error] count: official=1, aksh=2
- 🟡 ##[warning] count: official=1, aksh=0

---
**Total**: 10 scenarios compared, 18 issues found