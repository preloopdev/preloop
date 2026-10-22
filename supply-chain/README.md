# Supply-chain policy

This directory holds preloop's supply-chain policy:

| Path | What it is |
| --- | --- |
| `config.toml`, `audits.toml`, `imports.lock` | [cargo-vet](https://mozilla.github.io/cargo-vet/) state: who audited which crate, and what we trust |
| `guard-policy-files.sh` | the isolation guard CI runs on every pull request |
| `tests/` | tests for the guard (`bash supply-chain/tests/guard-policy-files.test.sh`) |
| `node-baseline.json` | Node externals baseline for `scripts/audit-node-externals.sh` |

The rest of the strict policy lives at the repository root and in `.github/`:
`deny.toml`, `.cargo/audit.toml`, `.github/workflows/supply-chain.yml`.

## The rule

**A pull request may not mix dependency/policy files with other files.**

Two kinds of file count.

The strict policy files always count:

```
deny.toml
.cargo/audit.toml
.github/workflows/supply-chain.yml
supply-chain/**                      (this directory, including this file)
```

`Cargo.toml` and `Cargo.lock` count only when the pull request changes a
third-party lock entry. A third-party entry is a `[[package]]` block in a
`Cargo.lock` that has a `source` line (a registry or git dependency); a
workspace member's block has no `source`.

For every `Cargo.lock` path in the diff, the guard compares a normalized view
at the merge base against the head:

- **kept:** every `[[package]]` block that has a `source` line, byte for byte
  (including `dependencies` and `checksum`), plus every line outside package
  blocks (the `version = N` header, `[[patch.unused]]`, and so on);
- **dropped:** first-party blocks (no `source`) and blank lines.

A lockfile missing at a revision contributes an empty view. Any difference
makes the PR a **dependency PR**: every `Cargo.toml` and `Cargo.lock` in the
diff is then a dependency file, and mixing one with other files fails.

If no third-party entry changed, `Cargo.toml` and `Cargo.lock` are first-party
edits and are classified like any other file. Adding or renaming a workspace
crate, wiring a path dependency, and adding a `[[test]]` target may ship with
their code.

The guard diffs with `--no-renames`, so a renamed lockfile or policy file shows
both its old and its new path and cannot move out of the set.

Any combination of dependency files may ship together in one PR — lockfile,
audit policy, vet state, and the manifests that declare a dependency all in one
diff, which keeps the supply-chain CI to a single run. The PR fails only when
one of them is mixed with **anything else**: source, tests, docs,
`CHANGELOG.md`, another workflow, the `justfile`, or a script.

There is no exception and no override: the verdict follows from the diff alone,
and it is recomputed on every push to the PR.

### The split

Two shapes:

1. **Dependency PR** — a third-party lock entry changed. The PR may contain any
   combination of the files above, and nothing else.
2. **Code PR** — no strict policy file and no third-party lock change. It may
   edit manifests and lockfiles as first-party files (a new crate, a path
   dependency, a test target), and is otherwise the ordinary PR.

A first-party manifest or lockfile edit does not need its own PR; it ships with
the code it describes.

### Ordering: third-party dependencies first, then code

Code that needs a new third-party dependency lands in **two PRs, in this
order**:

1. Open the dependency PR with the new dependency in the lockfile and the
   manifest (plus any `deny.toml`, `.cargo/audit.toml`, or `supply-chain/`
   update that comes with it). Get it reviewed and merged on its own.
2. Rebase the code branch on `main` and open the code PR. Its lockfile and
   manifests then contain no third-party change, and its CI tests the
   dependency that is actually on `main`.

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
| `0` | sanctioned shape (`Dependency/policy PR detected: OK`; first-party manifests/lockfiles classified as ordinary files; or nothing such touched) |
| `1` | isolation violation — the output names both file sets and the split |
| `2` | the guard could not decide (bad arguments, unresolvable revision, no merge base). Fails closed |

The tests cover the strict policy set, first-party manifest and lockfile edits,
third-party bumps, additions and dependency-list changes, renames, new
lockfiles, each path mixed with one non-policy file, and the merge-base
behaviour:

```sh
bash supply-chain/tests/guard-policy-files.test.sh
```

## What CI does

`.github/workflows/supply-chain.yml`, job `supply-chain`, on every push and
pull request:

1. checks out full history, drops any PR-shipped `.cargo/config*`, and runs
   `supply-chain/tests/guard-policy-files.test.sh`, so a change to the guard
   meets its own cases before anything is judged by it. This step (not a
   `justfile` recipe, not `ci.yml`) is the suite's only CI entry point: both of
   those files are outside the policy set, so a local recipe for it arrives in
   its own PR;
2. on `pull_request`, fetches the PR's base and head SHAs with `git fetch
   --no-tags origin "$base" "$pr_head"`, and fails the step if the fetch fails;
3. runs `supply-chain/guard-policy-files.sh "$base" "$pr_head"`, which decides
   from the merge base, not from the synthetic merge commit the checkout left
   in `HEAD`.

The guard runs from the pull request's own checkout, like every step in this
workflow: it is a policy gate, not a sandbox. A diff that weakens the guard or
this workflow is a policy diff like any other, and it is reviewed as one
(`.github/CODEOWNERS` owns `/.github/workflows/`, the default rule owns
`supply-chain/`), which is what enforces the rule; nothing inside the checkout
can enforce it against itself.

The rest of the workflow (`cargo vet --locked`, `cargo audit`, `cargo deny
check all`, the Node externals audit, Node pin parity) runs on `main` pushes,
the weekly schedule, and pull requests. `just supply-chain` runs the Rust side
of that gate locally.
