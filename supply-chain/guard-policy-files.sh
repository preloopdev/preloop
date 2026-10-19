#!/usr/bin/env bash
#
# Supply-chain dependency/policy isolation guard.
#
#   supply-chain/guard-policy-files.sh <base-rev> <head-rev>
#
# A pull request may not mix dependency/policy files with anything else. The
# lockfile, the audit policy, the vet state, and the manifests that declare a
# dependency may ship together -- that is one reviewable PR -- but they may not
# ride along with source, docs, or any other change.
#
# In CI (.github/workflows/supply-chain.yml, step "Guard policy files") the job
# has already fetched both revisions and calls:
#
#   supply-chain/guard-policy-files.sh "$base" "$pr_head"
#
# Locally, against the merge base of origin/main and the current branch:
#
#   just supply-chain-guard
#   # equivalent, and what that recipe runs:
#   supply-chain/guard-policy-files.sh origin/main HEAD
#
# The guard diffs the merge base against the head, never the two tips: with
# two tips, a change that exists on both sides cancels out of the diff and a
# policy touch can hide. See the test in supply-chain/tests/ for that case.
#
# Exit status:
#   0  sanctioned shape
#   1  isolation violation (the PR must be split)
#   2  the guard could not decide: bad usage, unresolvable revision, no merge
#      base. Also a failure -- the guard fails closed.
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: supply-chain/guard-policy-files.sh <base-rev> <head-rev>

  base-rev  the pull request's base, e.g. origin/main
  head-rev  the pull request's head, e.g. HEAD or a SHA

Both revisions must already be present locally (git fetch --no-tags origin
<ref>); the guard never fetches, so it cannot hang or half-decide.
EOF
}

# Dependency/policy files, in one place: keep these in sync with the failure
# message and with supply-chain/README.md.
#
# The audit policy and the vet state.
is_policy_file() {
  case "$1" in
    deny.toml | .cargo/audit.toml | .github/workflows/supply-chain.yml | supply-chain/*)
      return 0
      ;;
  esac
  # Any lockfile, wherever it sits: the workspace root's, and a standalone
  # crate's own (e.g. packaging/crates-io/preloop/Cargo.lock).
  [[ "$1" =~ (^|/)Cargo\.lock$ ]]
}

# Any manifest, wherever it sits: the workspace root, any workspace member
# (crates/*, benchmarks/*), and a standalone crate's own manifest. A dependency
# bump rewrites whichever manifest declares it, so the shape has to cover all of
# them and any member added later.
is_dependency_manifest() {
  [[ "$1" =~ (^|/)Cargo\.toml$ ]]
}

is_dependency_or_policy_file() {
  is_policy_file "$1" || is_dependency_manifest "$1"
}

# print_files <indent> <file>...
print_files() {
  local indent="$1"
  shift
  local file
  for file in "$@"; do
    printf '%s- %s\n' "$indent" "$file"
  done
}

if [[ $# -ne 2 || -z "${1:-}" || -z "${2:-}" ]]; then
  usage
  exit 2
fi

base="$1"
head="$2"

check_rev() {
  # check_rev <label> <rev>
  if ! git rev-parse --verify --quiet "$2^{commit}" >/dev/null; then
    echo "ERROR: cannot resolve $1 revision '$2'." >&2
    echo "Fetch it first, then re-run (the guard diffs what it is given, it does not fetch)." >&2
    exit 2
  fi
}

check_rev base "$base"
check_rev head "$head"

if ! merge_base="$(git merge-base "$base" "$head" 2>&1)"; then
  echo "ERROR: no merge base between '$base' and '$head': $merge_base" >&2
  echo "The guard cannot tell what this branch changes, so it fails closed." >&2
  exit 2
fi

diff_files="$(git diff --name-only "$merge_base" "$head")"

# Classify every changed path into one of two arrays. Appending to an array is
# amortized O(1), so a PR with tens of thousands of paths costs linear work (a
# scalar `list="${list}${file}\n"` rebuilds the whole string per path, which is
# quadratic over an attacker-controlled diff).
dependency_or_policy_files=()
other_files=()
while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  if is_dependency_or_policy_file "$file"; then
    dependency_or_policy_files+=("$file")
  else
    other_files+=("$file")
  fi
done <<<"$diff_files"

# One rule: dependency/policy files may ship with each other and with nothing
# else. Both allowed combinations pass; the only failure is a mix.
if [[ "${#dependency_or_policy_files[@]}" -eq 0 ]]; then
  echo "Policy file guard: OK (no policy or manifest files modified)"
  exit 0
fi
if [[ "${#other_files[@]}" -eq 0 ]]; then
  echo "Dependency/policy PR detected: OK"
  print_files "    " "${dependency_or_policy_files[@]}"
  exit 0
fi

cat >&2 <<EOF
ERROR: this PR mixes dependency/policy files with other files.

  dependency/policy files changed:
$(print_files "    " "${dependency_or_policy_files[@]}")
  other files changed:
$(print_files "    " "${other_files[@]}")

What to do: split it. Put the dependency/policy changes in their own PR and
merge that first, then rebase this branch on main and open the code PR -- the
code PR then touches no policy or manifest file.

The dependency/policy PR may contain any combination of:

  deny.toml  .cargo/audit.toml  .github/workflows/supply-chain.yml  supply-chain/**
  any Cargo.toml  any Cargo.lock

...and must not contain anything else -- no source, tests, docs, changelog,
other workflows, justfile, or scripts. Grouping the lockfile, the policy files
and the manifests in one PR is fine, and keeps the supply-chain CI to one run.

There is no exception to this rule. The check re-runs on every push to the PR
and decides from what the branch contains, so a green run here means the split
is already done.

  run it locally with: supply-chain/guard-policy-files.sh <base> <head>
  policy: supply-chain/README.md
EOF
exit 1
