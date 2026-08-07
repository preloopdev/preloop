# Security Policy

## Supported versions

Only the latest release is supported. Releases are tagged from `main`; the
`main` branch is the compatibility target for protocol fidelity with the
official `actions/runner` (pinned in `versions.toml`).

## Reporting a vulnerability

Do **not** open a public issue for security vulnerabilities. Report privately
via GitHub's private vulnerability reporting:

https://github.com/preloopdev/aksh/security/advisories/new

Please include:

- the affected version(s) and platform
- a minimal reproduction (workflow, event, and command line)
- whether the issue touches the runner protocol interface
  (`/_apis/...`, broker, Twirp results/artifact services)

You should receive an acknowledgment within 3 business days. Fixes land as
soon as a reproduction is confirmed; coordinated disclosure is welcome.

## Scope

In scope: the server control plane, the runner, workflow parsing/execution,
and the protocol implementations. Out of scope: the `experiments/`
directory (internal tooling), and anything explicitly marked as a mock.

## Disclosure

Security fixes are announced in the release notes for the version that fixes
them. If you reported the issue, you may choose to be credited.
