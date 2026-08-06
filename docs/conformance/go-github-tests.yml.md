# Conformance: google/go-github `tests.yml`

- Date: 2026-08-06
- Oracle: GitHub run [31106455098](https://github.com/google/go-github/actions/runs/31106455098) (master, success)
- Local: aksh server + aksh runner (v2.335.1 externals contract), single linux-labeled runner on a macOS host
- Server: `preloop-server serve --listen 127.0.0.1:9133 --state-dir /tmp/conf-repos/state/go-github`, `AKSH_LOCAL_WORKSPACE=/tmp/conf-repos/go-github`
- Runner: `preloop-runner configure --labels self-hosted,linux,ubuntu-latest`, then `run --once` in a drain loop

## Result

| Job | GitHub | aksh | Delta |
|---|---|---|---|
| test (stable, ubuntu-latest) | success | **success** | none |
| test (oldstable, ubuntu-latest) | success | **success** | none |
| test (stable, windows-latest) | success | failure (unhostable) | hardware: no windows runner; aksh fails loudly with a reason instead of queueing forever |

Run summary differs only because of the windows leg: GitHub has a hosted
windows fleet; this host does not. The failure is the server's
`unhostable_platform` conclusion (added in the review fix pass), which is loud
and carries the reason in the job event.

## Step-by-step (stable, ubuntu-latest)

GitHub: `Set up job → checkout → setup-go → Run go test → Ensure integration tests build → Upload coverage to Codecov → Post setup-go → Post checkout → Complete job`

aksh: identical order, all steps succeeded, including the coverage upload
(`codecov-action` reached the network and posted). Local checkout ran against
the server's workspace snapshot (13s, shallow), not a GitHub clone.

## Findings

1. **Matrix expansion matches GitHub.** Leg names `test (stable, ubuntu-latest,
   true)` (the `update-coverage` include) and `test (oldstable,
   ubuntu-latest)` match GitHub's ids byte-for-byte, including declaration
   order (`stable` before `oldstable`).
2. **fail-fast honored.** When the earlier contaminated run failed its stable
   leg, the oldstable leg concluded `cancelled` — the default `fail-fast`
   behavior.
3. **Snapshot staging embeds nested git repos as gitlinks.** A directory with a
   `.git` anywhere under the workspace lands in the snapshot tree as a gitlink
   without a `.gitmodules` entry; `actions/checkout`'s `git submodule foreach`
   then dies with `No url found for submodule path '…'`. Reproduced by
   accidentally placing a runner root under the workspace. GitHub-hosted repos
   never contain a second repo at a random path, so this is a server-side
   robustness gap: either exclude nested `.git` trees from staging or convert
   stray gitlinks to plain directories.
4. **`actions/checkout` works against the snapshot** with the server's origin
   rewrite (`insteadOf` redirect to `/snapshots/<run>`), including the auth
   cleanup (`Removing auth`) pass.

## Repro commands

```sh
cargo build --release -p aksh-runner-server -p aksh-runner -p aksh-runner-client
# server on 9133 with AKSH_LOCAL_WORKSPACE=<clone> (runner root OUTSIDE the workspace)
preloop-runner --runner-root <outside> configure --url http://127.0.0.1:9133 --token dummy-token \
  --name conf-go-github --work _work --unattended --replace --labels self-hosted,linux,ubuntu-latest
curl -s -X POST -H "Authorization: Bearer aksh-system-token" -H "Content-Type: application/json" \
  -d "$(python3 -c 'import json;print(json.dumps({"workflow_yaml":open("tests.yml").read(),"event":"push","payload":{},"repository":"google/go-github","git_ref":"refs/heads/master","vars":{},"inputs":{},"secrets":{},"reusable_workflows":{},"reusable_workflow_shas":{}}))')" \
  http://127.0.0.1:9133/api/v1/runs
# loop: preloop-runner run --once until jobs are terminal
```
