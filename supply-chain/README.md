# Supply-chain policy

This directory holds preloop's supply-chain policy:

| Path | What it is |
| --- | --- |
| `config.toml`, `audits.toml`, `imports.lock` | [cargo-vet](https://mozilla.github.io/cargo-vet/) state: who audited which crate, and what we trust |
| `guard-policy-files.sh` | the isolation guard CI runs on every pull request |
| `tests/` | tests for the guard (`bash supply-chain/tests/guard-policy-files.test.sh`) |
| `node-baseline.json` | Node externals baseline for `scripts/audit-node-externals.sh` |

The rest of the policy lives at the repository root and in `.github/`:
`Cargo.lock`, `deny.toml`, `.cargo/audit.toml`,
`.github/workflows/supply-chain.yml`.

## The rule

**A pull request may not mix dependency/policy files with other files.**

Dependency/policy files are:

```
deny.toml
.cargo/audit.toml
.github/workflows/supply-chain.yml
supply-chain/**                      (this directory, including this file)
any Cargo.toml                       (workspace root, crates/*, benchmarks/*,
                                      a standalone crate, any member added later)
any Cargo.lock                       (the workspace lockfile, or a standalone
                                      crate's own)
```

Any combination of those may ship together in one PR — lockfile, audit policy,
vet state, and the manifests that declare a dependency all in one diff, which
keeps the supply-chain CI to a single run. The PR fails only when one of them
is mixed with **anything else**: source, tests, docs, `CHANGELOG.md`, another
workflow, the `justfile`, or a script.

There is no exception and no override: the verdict follows from the diff alone,
and it is recomputed on every push to the PR.

### The split

Two shapes only:

1. **Dependency/policy PR** — any combination of the paths above, nothing else.
2. **Code PR** — no dependency/policy file at all. The ordinary PR.

### Ordering: dependency/policy first, then code

Code that needs a new dependency lands in **two PRs, in this order**:

1. Open the dependency/policy PR with the new dependency in the lockfile and
   the manifest (plus any `deny.toml`, `.cargo/audit.toml`, or
   `supply-chain/` update that comes with it). Get it reviewed and merged on its
   own.
2. Rebase the code branch on `main` and open the code PR. It now touches no
   dependency/policy file, and its CI tests the dependency that is actually on
   `main`.

Never do both in one branch. A breaking upgrade that needs source changes works
the same way: the lockfile bump first, the source migration second.

## Running the guard locally

```sh
just supply-chain-guard                     # origin/main vs HEAD
supply-chain/guard-policy-files.sh origin/main HEAD
supply-chain/guard-policy-files.sh "$base_sha" "$head_sha"
```

The guard diffs `git merge-base <base> <head>` against `<head>`; that is the
whole input. Nothing is fetched, so fetch the base first (`git fetch origin
main`) when it is missing. Exit status:

| Status | Meaning |
| --- | --- |
| `0` | sanctioned shape (`Dependency/policy PR detected: OK`, or no such file touched) |
| `1` | isolation violation — the output names both file sets and the split |
| `2` | the guard could not decide (bad arguments, unresolvable revision, no merge base). Fails closed |

The tests cover every allowed combination, each path mixed with one non-policy
file, and the merge-base behaviour:

```sh
bash supply-chain/tests/guard-policy-files.test.sh
```

## What CI does

`.github/workflows/supply-chain.yml`, job `supply-chain`, step
`Guard policy files`, on `pull_request`:

1. checks out full history and drops any PR-shipped `.cargo/config*`;
2. fetches the PR's base and head SHAs with `git fetch --no-tags origin
   "$base" "$pr_head"`, and fails the step if the fetch fails;
3. runs `supply-chain/guard-policy-files.sh "$base" "$pr_head"`, which decides
   from the merge base, not from the synthetic merge commit the checkout left
   in `HEAD`.

The rest of the workflow (`cargo vet --locked`, `cargo audit`, `cargo deny
check all`, the Node externals audit, Node pin parity) runs on `main` pushes,
the weekly schedule, and pull requests. `just supply-chain` runs the Rust side
of that gate locally.
