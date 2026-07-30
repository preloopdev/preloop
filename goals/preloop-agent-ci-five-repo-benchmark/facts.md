# Benchmark facts

- The benchmark uses five public repositories pinned to immutable commits.
- The repositories cover Rust, Python, JavaScript or TypeScript, Go, and a container-heavy or monorepo-style CI workload.
- Each system runs the same pinned workflow inputs, with documented adaptations only where a platform requires them.
- Each repository receives three cold-start runs and five warm runs on each system.
- The benchmark records timestamps for submission, parsing, scheduling, VM boot or resume, runner registration, job acquisition, checkout, action preparation, workload steps, result delivery, and teardown.
- The benchmark records CPU, memory, storage, network transfer, VM count, and peak resource usage where each platform exposes them.
- Every run records success or failure, failing phase, retry count, cancellation state, and whether the produced result matches the expected CI outcome.
- Agent CI orchestration and controller overhead are measured separately from workload execution time.
- Each benchmark record links the repository commit, workflow file, system configuration, runner image, tool versions, raw events, and summarized timings.
- The final report compares cold latency, warm latency, phase distributions, throughput, reliability, resource cost, and fidelity across Preloop and Agent CI.
- Agent CI credentials, repository tokens, and other secrets stay outside the repository and are redacted from benchmark artifacts.
