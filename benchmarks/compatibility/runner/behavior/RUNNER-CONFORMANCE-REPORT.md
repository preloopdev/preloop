# Runner Compatibility Conformance

Profile: **deep**

The official runner and `preloop-runner` are compared against the same GitHub workflow runs.

- Expected workflows: 10
- Official records: 10
- Aksh records: 10
- Verdict: **FAIL**

## Differences

- 103 103-cancellation-background-post job 'cancellable': steps differ official=[('Set up job', 'success'), ('Start background worker', 'cancelled'), ('Long foreground step', 'skipped'), ('Cancel-only step', 'success'), ('Always cleanup', 'success'), ('Complete job', 'success')] aksh=[('Set up job', 'success'), ('Start background worker', 'failure'), ('Long foreground step', 'skipped'), ('Cancel-only step', 'success'), ('Always cleanup', 'success'), ('Complete job', 'success')]
- 106 106-cache-artifact-pipeline job 'create': steps differ official=[('Set up job', 'failure')] aksh=[('Set up job', 'success'), ('Create unusual files', 'success'), ('Restore cache', 'failure'), ('Record cache state', 'skipped'), ('Upload artifact', 'skipped'), ('Complete job', 'success')]
- 107 107-remote-action-resolution job 'actions': steps differ official=[('Set up job', 'failure')] aksh=[('Set up job', 'success'), ('Checkout pinned action source', 'success'), ('Checkout explicit secondary repository', 'success'), ('Execute pinned JavaScript action', 'failure'), ('Verify downloaded trees', 'skipped'), ('Post Checkout pinned action source', 'success'), ('Complete job', 'success')]
- 109 109-dag-matrix-scheduler job 'final': steps differ official=[('Set up job', 'success'), ('Run echo "AKSH_ORACLE: final root=success build=success test=success package=success"', 'success'), ('Complete job', 'success')] aksh=[('Set up job', 'success'), ('Run ${{ format(\'echo "AKSH_ORACLE: final root={0} build={1} test={2} package={3}"\', …', 'success'), ('Complete job', 'success')]
