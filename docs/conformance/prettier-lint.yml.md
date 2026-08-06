# Conformance: prettier/prettier `lint.yml`

- Date: 2026-08-06
- Oracle: recent GitHub run of `lint.yml` on push (success)
- Local: aksh server + linux-labeled aksh runner on a macOS host, port 9132
- Workflow steps (local): Checkout → Setup Node.js → Install Dependencies → Check Dependencies → Check JSDoc Types → (post steps)

## Result

| Step | GitHub | aksh | Delta |
|---|---|---|---|
| Checkout | success | **success** | none |
| Setup Node.js | success | **success** | none (tarball toolcache install, no sudo needed) |
| Install Dependencies (`yarn install --immutable`) | success | **success** | required `yarn` on the host (installed) |
| Check Dependencies | success | **success** | none |
| Check JSDoc Types | success | **failure** (exit 2) | toolchain divergence, see below |
| Post Checkout | success | success | none |

## Finding 1: setup-node is fine; the host image is not the GitHub image

`actions/setup-node` extracts a Node tarball into the toolcache — no elevation,
no system mutation — so it works on a bare host. The failure was earlier
runs' `yarn` missing (`Unable to locate executable file: yarn`): GitHub-hosted
images ship yarn; a bare host does not. Host-environment divergence, not a
protocol one.

## Finding 2: `Check JSDoc Types` diverges at the toolchain level

The step (a TypeScript type-check over prettier's codebase, run with the
repo's pinned toolchain) exits 2 locally while passing on GitHub. The step
sequence, echo, and conclusion semantics are faithful; the divergence is in
what the type-checker sees — host Node v26 vs the runner image's Node,
registry-fresh dependencies vs the image's cache. Worth re-running on a
GitHub-shaped image (non-root `runner` user, Node 22/24, yarn) before calling
it a real divergence; the plumbing around the step behaved identically.

## Notes

- Checkout against the snapshot succeeded on a repo with a large working tree
  (~1,000 files), and `Post Checkout` auth cleanup passed.
- The run concluded `failure` exactly as GitHub would for a failed step:
  dependents skipped, summary correct.
