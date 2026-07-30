# Preloop versus Agent CI benchmark

Benchmark the current Preloop implementation against Agent CI by running real CI workflows from five diversified public repositories pinned to immutable commits. Measure the complete pipeline—not just workflow duration—including submission, parsing, scheduling, VM boot or resume, runner registration, checkout and action preparation, workload execution, result delivery, teardown, resource usage, reliability, and Agent CI/controller overhead.

Shared acceptance facts: [facts.md](facts.md)

Execution plan: [plan.md](plan.md)

Early measured results: [results/early-results.md](results/early-results.md)

## Done condition

The goal is complete when:

- five repositories have passed a pinned-commit preflight and are represented in the benchmark manifest;
- both Preloop and Agent CI have run the same workloads with three cold and five warm repetitions per repository;
- every planned run has a correlated raw event stream, normalized phase timings, outcome/fidelity result, resource data or an explicit unavailable marker, and secret-redaction validation;
- cold and warm results are analyzed separately;
- the report compares latency distributions, throughput, reliability, resource cost, phase overhead, and workflow fidelity; and
- a clean checkout can reproduce the manifest validation, dry run, and report generation from stored artifacts without repository credentials.

The Agent CI endpoint/authentication details and the SmolVM runner-pool startup issue are explicit prerequisites. A partial run must not be presented as the completed comparison.

> Goal setup note: the repository did not have the Plannotator CLI installed, so the interview and facts artifacts were recorded manually from the confirmed scope and documented recommendations. Plan gating remains an external review step.
