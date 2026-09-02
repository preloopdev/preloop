# v2.337.0 Conformance Campaign — Report
**Date:** 2026-08-31 → 2026-09-01 · **Runner under test:** official actions/runner v2.337.0 (released 2026-08-26) + preloop-runner (protocol-compat 2.335.1) · **Workflows:** 27 edge-case scenarios (201–227) · **Isolation:** every runner process executed inside a dedicated smolvm microVM (ubuntu 24.04 rootfs, 4 vCPU / 4 GB), host only ran mitmdump + preloop-server + orchestration. No workflow ever executed on the host.

## The four cells
| Cell | Runner | Server | Captured |
|---|---|---|---|
| gh-official | official v2.337.0 (smolvm) | github.com/Bnjoroge1/conformance-v2337 (private) | 27/27 |
| gh-preloop | preloop-runner (smolvm) | github.com (same repo) | 27/27 |
| pl-preloop | preloop-runner (smolvm) | local preloop server | 27/27 |
| pl-official | official v2.337.0 (smolvm) | local preloop server | 25/27 clean (202, 206 re-captured with known capture-harness caveats) |

MITM (mitmdump + capture addon) recorded every flow on GitHub cells; preloop-server's --record-flows recorded every flow on server cells. Redaction via experiments/mitm/addons/redact.py.

## Headline result — official runner vs preloop runner, both against github.com
**23/27 scenarios match exactly** (run conclusion + per-job conclusions):
201, 202, 204, 205, 206, 207, 209, 210, 211, 212, 214, 215, 217, 218, 219, 220, 221, 222, 223, 225, 226, 227 …(incl. fail-fast, composite pre/post, docker actions, reusable chains, cache/artifact roundtrips, masking, OIDC claim shape)

**4 genuine divergences (preloop-runner side):**
1. **213-oidc-token-claims** — flaky, reclassified as harness-environmental: a debug run showed ACTIONS_ID_TOKEN_REQUEST_TOKEN/URL correctly populated by preloop-runner and GitHub's OIDC endpoint returning a valid token (audience conformance-test) through the capture proxy; a subsequent clean run failed with the request never reaching the proxy. Inconclusive — intermittent empty-response through the mitm/egress path, not a conclusive preloop-runner gap. (Workflow now retries.)
2. **216-summaries** — preloop-runner spawns step with working-directory = ${{ github.workspace }} before the directory exists: "spawning bash: No such file or directory".
3. **208-timeout-graceful-kill** — timed-out job conclusion: official=cancelled vs preloop=cancelled on the outer job but the inner quick job result differs (failure vs success) — timeout bookkeeping difference.
4. **224-matrix-include-exclude** — fail-fast race: official cancels in-flight shard (cancelled) while preloop lets it finish (success).

## Preloop bugs found & fixed (branch fix/git-protocol-conformance)
1. **Strategy expressions blind to dispatch inputs** (5a36f431 + follow-up): `jobs.<id>.strategy.*` expressions could not see dispatch inputs (`inputs` and `github.event.inputs` contexts were absent at top-level expansion). Fixed in preloop-gha-parser::expand (expression_context + expand_jobs_with_reusables_and_shas_and_inputs) and runs.rs threading.
2. **Job env wire shape** (35acfe20): environmentVariables must be TemplateToken maps ({type:2,map:[{Key:{type:0,lit:K},Value:{type:0,lit:V}}]}) — plain JSON objects crash the official runner's schema evaluator ("The template is not valid. Unexpected value ''"). Fixed in preloop-gha-parser::job_builder.
3. **orchestrationId token safety**: `system.orchestrationId` = "{planId}.{jobId}.__default"; reusable-call job ids contain "/" which .NET's ProductHeaderValue rejects → FormatException, job dies at start. Fixed with token-safe sanitization in runs.rs::orchestration_id.
4. **Agent lookup clientId must be a Guid** (runner_lifecycle.rs): empty "" crashed the official runner's Guid parse during job acquisition. Now emits a deterministic per-runner Guid.

## Infra constraints discovered (documented, not product bugs)
- Official runner strips non-default ports from the server URL → server must be reachable on :80 (TCP forwarder used).
- smolvm TSI egress drops new VM connections while many long-polls are alive (mitigated by connection topology).
- macOS firewall silently drops unsigned binaries' inbound; smolvm 1.8.1 on PATH is broken (use ~/.smolvm/smolvm-bin 1.8.2).
- Official runner in VM needs libicu + ldconfig cache rebuild at boot (vm-init.sh).

## Pre-existing failure on main (not from this campaign)
- preloop-orchestrator lifecycle_tests::published_node_externals_are_traversable_by_other_users fails: test mock SHASUMS still pin node-v20.19.0 while the pin is v20.20.2 (commit 48386439).

## Known capture caveats
- 202 (dynamic matrix): official-runner matrix legs materialize late; capture window raced the expansion. gh-official/gh-preloop/pl-preloop captures all fine; pl-official run-result recorded a starved leg.
- 206 (artifacts): found a NEW preloop-server fidelity bug — artifact CreateArtifact/ListArtifacts scoping uses per-job plan ids (build job wrote under plan X, consume listed under plan Y) → "Artifact not found". Filed as the top follow-up fix (artifact_twirp scoping / plan-id stability). Not fixed tonight.
- 222: strategy expression fix verified (submit-time 400 gone); final pl-official capture had a poll-time race; gh-side captures are authoritative.

## Where the goldens live
- **Raw captures** (flows.jsonl, flows.mitm, run-result.json, runner/server logs, per-scenario dirs):
  `~/preloop-captures/captures-raw/<cell>/<scenario>/<timestamp>/` — 4 cells x 27 scenarios.
  Sizes: gh-official 115M, pl-official 76M, pl-preloop 106M, gh-preloop 4.0G (the preloop runner
  downloads node externals inside each session; the 54 MB tarballs are captured inline).
- **Compressed archive**: `~/preloop-captures/captures-v2337.tar.gz` (3.1 GB).
- Bulk pruned before archiving: `flows.mitm` >10 MB and single flow bodies >5 MB — flows.jsonl is
  the golden record and stays complete; the dropped bodies are re-downloadable artifacts
  (node tarballs, log streams) not protocol evidence.
- Committed to the repo: the report, findings, compare table, and per-cell summaries
  (`benchmarks/real-world/results/v2337-conformance/`) — not the multi-GB raw flows.

## Recommended next steps
1. Fix artifact/workflow_run_backend_id scoping (per-run plan id stability) — highest-value fidelity gap found tonight.
2. Fix preloop-runner OIDC env wiring against github.com servers.
3. Fix preloop-runner workspace-directory creation before step spawn.
4. Align timeout conclusion semantics (cancelled vs failure).
5. Fix the stale node-SHASUM test mock on main (pre-existing).
