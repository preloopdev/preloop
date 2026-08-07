# Runner user: who your steps run as

By default, **steps in Preloop VMs run as a dedicated `runner` account (uid
1001) — not root** — matching GitHub's hosted runner user-session contract.
This page covers why, how it works, every knob, and what to expect in each
circumstance.

## Why

GitHub-hosted runners execute steps as the `runner` user (uid 1001 on the
Ubuntu images) inside a systemd session: `USER=runner`, `HOME=/home/runner`,
`XDG_RUNTIME_DIR=/run/user/1001`. Real-world workflows and test suites
depend on that contract:

- steps that check `id -u` or `env_var('USER')` (e.g. `just`'s test suite)
- tools that refuse to run as root (npm's `unsafe-perm` posture, `git`
  ownership checks, home-directory caches)
- file ownership: artifacts written by root can't be read by the next job
  that runs as the checkout owner
- `runtime_directory()`-style helpers reading `XDG_RUNTIME_DIR`

Preloop VMs historically booted the runner as root and then *fabricated*
`USER=root` in the step environment — parity by assertion, which also
leaked into host-run runners (a laptop runner reported `USER=root` too).
The runner now actually drops privileges, and the environment reports the
real account.

## The mechanism

The control plane and VM provisioning stay root; **only the runner process
(and therefore its steps) drops privileges**. At provisioning time the
orchestrator wraps `configure` and `run` in a guest bootstrap that:

1. creates the account if missing (`useradd -m -u <uid> <user>`)
2. provisions its runtime dir (`/run/user/<uid>`, chowned)
3. chowns the runner root (`/var/lib/preloop-runner`, which contains
   `_work`)
4. opens the control bridge socket to the account
   (`chmod 777 /run/preloop-control`)
5. grants the docker group (`usermod -aG docker <user>`) so
   `container:` / `services:` jobs work
6. drops privileges with `setpriv --reuid --regid --init-groups
   --clear-groups` and exports `PRELOOP_RUNNER_USER` / `PRELOOP_RUNNER_UID`
   / `HOME` for the step-environment contract

The step environment then derives `USER` / `LOGNAME` /
`XDG_RUNTIME_DIR=/run/user/<uid>` from the orchestrator override. Explicit
job/step values always win.

## The knobs

| Env var | Effect |
|---|---|
| `PRELOOP_RUNNER_USER` | Account name for guest runners (default `runner`). `root` restores root behavior; empty disables switching |
| `PRELOOP_RUNNER_UID` | UID for the account (default `1001` — GitHub's hosted `runner` uid; was `1000` before the May 2025 image change) |

```sh
preloop serve                                   # steps run as runner/1001
PRELOOP_RUNNER_USER=root preloop serve          # legacy root behavior
PRELOOP_RUNNER_USER=ci PRELOOP_RUNNER_UID=2000  # custom account
PRELOOP_RUNNER_USER= preloop serve              # no switching (guest root)
```

## What to expect in each circumstance

### VM pool (default)

Every provisioned VM creates the account at configure time. Steps run as
`runner` with `HOME=/home/runner`, `USER=runner`, `LOGNAME=runner`,
`XDG_RUNTIME_DIR=/run/user/1001`. Container and service jobs work because
the account is in the `docker` group.

### Fork pool

Identical — fork slots are provisioned through the same code path, so the
account exists in each fork before the runner starts.

### Host-run runner (`preloop-runner configure` + `run` on a machine)

No user switching happens — the runner runs as the account that launched
it, and **steps report that real account** (`USER`/`LOGNAME` from the
process environment). Before this fix, a laptop runner reported the
fabricated `USER=root`; now it reports the actual user (verified: `uid=502
USER=bnjoroge` on a macOS host run). Set `PRELOOP_RUNNER_USER` yourself if
you want a host-run runner to advertise a different account.

### Custom account

`PRELOOP_RUNNER_USER=ci PRELOOP_RUNNER_UID=2000` — the bootstrap creates
`ci` with uid 2000, chowns the runner root and runtime dir to it, and steps
see `USER=ci`, `XDG_RUNTIME_DIR=/run/user/2000`. Choose uids that don't
collide with other accounts you mount into the VM.

### Root restoration

`PRELOOP_RUNNER_USER=root` — the wrapper is skipped entirely; steps run as
root with `XDG_RUNTIME_DIR=/run/user/0` (the pre-fix behavior, minus the
fabrication: `USER=root` is now the *truth*, not an assertion).

### Debug sessions

`preloop shell` / `preloop debug` attach to the VM's control shell, which
stays root (that is the operator's surface). The *job* still runs as the
runner account.

## Behavior changes to know about

- **`HOME` is `/home/runner`, not `/root`.** Workflows that write to `~/`
  (tool caches, dotfiles) now land in the runner's home. The golden bakes
  toolchains system-wide, so this rarely matters, but check workflows that
  read `~/.config` or `~/.cache` directly.
- **Workspace ownership is consistent.** The runner root (including
  `_work`) is chowned to the runner account, so checkout and artifacts have
  a single owner — no root-owned files left behind for the next job.
- **`git` ownership checks stop complaining.** `safe.directory` friction
  disappears because the checkout owner is the step user.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Step reports uid 0 | `PRELOOP_RUNNER_USER` unset/empty on an old engine, or `=root` | Set the env; the engine must be the build with user-switching support |
| `useradd: user exists` noise | Account already created by a prior provision | Harmless — the bootstrap guards with `getent passwd` first |
| `Permission denied` on `/run/preloop-control` | The bridge dir appeared after the bootstrap's `chmod` (race) | Restart the pool; the chmod is applied before the runner starts |
| `Permission denied` on `/var/run/docker.sock` | The `docker` group didn't exist when the bootstrap ran (docker installed after) | Ensure docker is baked in the golden; the `getent group docker` guard skips `usermod` when missing |
| Workspace files owned by root after a custom-mount job | Custom volumes mounted over the runner root | Chown the mount target to the runner uid, or set `PRELOOP_RUNNER_USER=root` for that pool |
| Steps disagree about `HOME` with container jobs | Containers keep their own image user (hosted parity) | Not a bug — the host contract doesn't apply inside containers |

## Verification

Quick check that the contract holds on your pool — run this workflow and
inspect the step log:

```yaml
on: push
jobs:
  who:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo "uid=$(id -u) user=$(id -un)"
          echo "USER=$USER LOGNAME=$LOGNAME HOME=$HOME XDG=$XDG_RUNTIME_DIR"
```

Expected on a VM/fork pool: `uid=1001 user=runner`, `USER=runner`,
`HOME=/home/runner`, `XDG=/run/user/1001`. On a host-run runner: the
launching account's values. `id -u` must never print `0` unless
`PRELOOP_RUNNER_USER=root` was set deliberately.

## Design notes

- Only the runner process drops privileges; provisioning, the control
  bridge, and the VM shell remain root — the surface that needs root keeps
  it.
- The account is created per VM at provision time, so a golden never needs
  rebuilding for user changes — the knob is live on the next provision.
- The environment contract has a strict precedence: explicit job/step
  values > orchestrator override > process identity.
