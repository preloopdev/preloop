# Conformance: psf/requests `lint.yml`

- Date: 2026-08-06
- Oracle: recent GitHub run of `lint.yml` on push (success)
- Local: aksh server + linux-labeled aksh runner on a macOS host, port 9131
- Workflow: `checkout → Set up Python → Run pre-commit`

## Result

| Step | GitHub | aksh | Delta |
|---|---|---|---|
| Checkout | success | **success** | none (snapshot rewrite; 13s shallow fetch) |
| Set up Python | success | **failure** | root cause below |
| Run pre-commit | success | skipped (upstream failed) | consequence |

## Root cause: the privilege model

`actions/setup-python@v7` on macOS downloads `python-3.14.7-macos11.pkg` and
installs it with `sudo installer`. The aksh runner on this host runs as an
unprivileged user with no passwordless sudo, so the install fails and the step
exits 1.

GitHub-hosted macOS runners run as the unprivileged `runner` user **with
passwordless sudo** — that is exactly why the same workflow succeeds there.
This is the same root-user divergence found independently in the CLI dogfood
pass (gin's EACCES test): aksh's VM image runs jobs as root, which is *more*
privileged than GitHub, but the host-side self-hosted path runs as a plain
user with *no* sudo. Neither matches GitHub's `runner` + passwordless sudo
contract.

Two distinct gaps, one fix direction:
1. **VM image** (smolvm Linux guests): jobs run as root; GitHub runs them as
   uid 1001 `runner` with passwordless sudo. Tools that refuse root (bazel,
   `pip --user`, many test suites) diverge.
2. **Host self-hosted path**: no sudo configured for the job user, so
   installers that need elevation fail where GitHub-hosted succeeds.

Fix: provision a non-root `runner` user (uid 1001) with passwordless sudo in
the VM image, and run host-side workers as a user with passwordless sudo.

## Notes

- The failure is loud and correctly attributed: `Set up Python` failed, `Run
  pre-commit` was skipped (`condition not met`), job `failure`, run `failure`.
- Nothing in the failure is a wire/protocol divergence — step order, names,
  and conclusion semantics matched GitHub.
