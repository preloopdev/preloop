# Conformance: cli/cli `go.yml` (Unit and Integration Tests)

- Date: 2026-08-06
- Oracle: GitHub run [31109178954](https://github.com/cli/cli/actions/runs/31109178954) (trunk, success)
- Local: aksh server + two aksh runners — one linux-labeled, one macOS-labeled (native host) — on port 9134
- Server: `preloop-server serve --listen 127.0.0.1:9134`, `AKSH_LOCAL_WORKSPACE=/tmp/conf-repos/cli`

## Result

| Job | GitHub | aksh | Delta |
|---|---|---|---|
| integration-tests (ubuntu-latest) | success | **success** | none |
| integration-tests (macos-latest) | success | **success** | none |
| build (ubuntu-latest) | success | invalid | runner root deleted mid-run by the operator (see Findings 3) |
| build (macos-latest) | success | invalid | same |
| build / integration-tests (windows-latest) | success | failure (unhostable) | hardware: no windows runner |

## Step-by-step (integration-tests, ubuntu-latest)

GitHub: `Set up job → Check out code → Set up Go → Build executable → Run attestation command set integration tests → Post Set up Go → Post Check out code → Complete job`

aksh: identical order, all steps succeeded. The `attestation` integration test
set exercises `gh` against a local HTTP fixture server — it ran against the
aksh control plane's endpoints.

## Findings

1. **Multi-OS matrix scheduling works.** One server, two runners with
   different label sets: ubuntu legs went to the linux runner, macOS legs to
   the native macOS runner, concurrently. Each leg's step sequence matched
   GitHub.
2. **Windows legs conclude loudly when unhostable** (server's
   `unhostable_platform` fix): the run reports `failure` with the reason in
   the job event rather than queueing forever or, worse, reporting green.
3. **Operator error produced invalid build legs.** The `build` legs ran after
   the operator deleted their runner root out from under the live run; steps
   failed with `Unable to read current working directory` and a missing
   `action.yml` in the deleted `_work`. Not a server defect; kept here so the
   run's `failure` verdict is not mistaken for a protocol finding.
4. **A runner that declares `linux` and one that declares `macos` coexist
   cleanly** — the OS guard (hosted-image stand-in) routes jobs by the
   declared OS and never lets a linux-labeled runner take a macOS job.

## Repro commands

Same skeleton as `go-github-tests.yml.md`, with one runner per OS:

```sh
preloop-runner --runner-root <linux-root> configure --url http://127.0.0.1:9134 --token dummy-token \
  --name cli-linux --work _work --unattended --replace --labels self-hosted,linux,ubuntu-latest
preloop-runner --runner-root <mac-root> configure --url http://127.0.0.1:9134 --token dummy-token \
  --name cli-macos --work _work --unattended --replace --labels self-hosted,macos,macos-latest,ARM64
# submit .github/workflows/go.yml, then run both runners in drain loops
```
