# aksh diagram suite

## Brief

Audience: maintainers, contributors, infrastructure engineers, and users evaluating aksh/Preloop. The suite explains the repository at four zoom levels: product boundary, control-plane compilation and scheduling, runner execution, and supporting data/security/verification mechanisms. Each SVG answers one mechanism-level question and has an editable `.excalidraw` companion.

| Diagram | Mechanism explained | Teaching claim |
|---|---|---|
| `00-system-map` | End-to-end project topology | The runner-compatible protocol is the source of truth; native REST/NDJSON is an additive projection over the same state. |
| `01-workflow-compilation` | YAML-to-job-message lowering | Parsing, trigger matching, matrix/reusable expansion, DAG validation, and expression evaluation happen before concrete `AgentJobRequestMessage` values enter the queue. |
| `02-scheduling-broker` | Dependency/concurrency gating and leases | A job becomes runnable only after `needs` and concurrency gates; broker acquire/renew/complete gives a matching runner a time-bounded lease. |
| `03-runner-lifecycle` | Listener/Worker process isolation | A long-lived Listener owns sessions and cancellation while a disposable Worker child executes one job and reports directly. |
| `04-step-execution-reporting` | Per-job step engine | Pre/main/post execution is sequential; status and live logs leave through separate background channels, with a final completion drain. |
| `05-results-data-plane` | Logs, cache, artifacts, and local storage | Runner-native AzDO/Twirp/blob protocols terminate in file-backed stores and live native projections. |
| `06-security-identity` | Trust and credential boundaries | Webhook HMAC, runner RSA/OAuth/AES, `SecretString` masking, and OIDC RS256/JWKS protect different hops. |
| `07-preloop-vm-lifecycle` | Optional disposable VM capacity | `RunnerProvider` converts queue demand into forked run-once VMs without changing the runner protocol. |
| `08-protocol-conformance` | Wire-fidelity verification | Official-runner captures are normalized, replayed against aksh, and gated on status and schema equivalence. |
| `09-dap-debugging` | Step-level workflow debugging | A DAP client controls the runner’s paused step loop through an adaptive WebSocket/raw-TCP bridge and masked synthetic context view. |

## Visual language

- Warm off-white canvas: `#FAF7F0`.
- Dark charcoal structure and request arrows: `#2D2A26`.
- One primary accent: blue `#2563EB`, always paired with labels or structural emphasis.
- Warning/failure color: red `#C2413A`, always paired with failure/cancellation text.
- Solid arrow: request or command.
- Dashed arrow: background work, periodic work, replication, or persistence.
- Red arrow: failure, rejection, timeout, or cancellation.
- Rounded rectangles represent bounded processes/services; ellipses represent entry/terminal points; diamonds represent decisions.

## Assumptions

1. The modern broker + Twirp path is the primary runtime path; AzDO endpoints remain runner-compatible and are shown where they clarify the shared protocol surface.
2. `actions/runner` compatibility and the checked conformance golden baseline both target v2.336.0.
3. “Official runner” and `aksh-runner` are alternative clients of the same control plane; diagrams do not imply that both execute the same job simultaneously.
4. Default run/queue/session state is in memory. Cache, artifact, action, blob, and OIDC key material are file-backed under `.aksh/`; deployment-specific backend traits may replace those defaults.
5. The Preloop VM diagram describes the optional product/orchestrator layer present in this workspace. aksh itself remains execution-agnostic and works with external runners.
6. The VM lifecycle abstracts substrate-specific boot/fork details; the invariant is create/configure/run-once/destroy through `RunnerProvider`.
7. The DAP diagram combines the local server-proxy and upstream-compatible transport concepts at the protocol boundary; exact relay choice is deployment/configuration dependent.
8. IDs, tokens, timestamps, URLs, and GUIDs in conformance replay are volatile and normalized; status codes and checked schemas remain the fidelity gate.
9. Cloudflare publication uses an unauthenticated temporary preview deployment because this workstation has no authenticated Cloudflare account. The generated URL is therefore a preview link, not a permanent production domain.
