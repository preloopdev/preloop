#!/usr/bin/env bash
#
# Supply-chain dependency/policy isolation guard.
#
#   supply-chain/guard-policy-files.sh <base-rev> <head-rev>
#
# A pull request may not mix dependency/policy files with anything else.
#
# The strict policy files always count:
#
#   deny.toml  .cargo/audit.toml  .github/workflows/supply-chain.yml  supply-chain/**
#
# Cargo.toml and Cargo.lock count only when the pull request changes a
# third-party lock entry: a [[package]] block in a Cargo.lock that has a
# `source` line (registry or git). For every lockfile path in the diff the
# guard compares a normalized view at the merge base against the head:
#
#   keeps   every [[package]] block that has a `source` line, byte for byte
#           (including `dependencies` and `checksum`), plus every line outside
#           package blocks (the `version = N` header, [[patch.unused]], ...)
#   drops   first-party blocks (no `source`) and blank lines
#
# A lockfile missing at a revision contributes an empty view. Any difference
# makes the pull request a dependency PR: every manifest and lockfile in the
# diff then counts as a dependency file, and mixing one with anything else
# fails. With no third-party change, a manifest or lockfile edit is a
# first-party edit and is classified like any other file -- adding or renaming
# a workspace crate, wiring a path dependency, or adding a [[test]] target may
# ship with its code.
#
# The diff is taken with --no-renames so that a renamed lockfile or policy file
# shows both its old and its new path and cannot slip past the set.
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

# Strict policy files, in one place: keep these in sync with the failure
# message and with supply-chain/README.md. These are the files that may never
# ride along with code, whatever else the diff contains.
#
# The audit policy, the vet state, and the workflow that runs this guard.
is_policy_file() {
  case "$1" in
    deny.toml | .cargo/audit.toml | .github/workflows/supply-chain.yml | supply-chain/*)
      return 0
      ;;
  esac
  return 1
}

# Any manifest, wherever it sits: the workspace root, any workspace member
# (crates/*, benchmarks/*), and a standalone crate's own manifest.
is_manifest() {
  [[ "$1" =~ (^|/)Cargo\.toml$ ]]
}

# Any lockfile, wherever it sits: the workspace root's, and a standalone
# crate's own (e.g. packaging/crates-io/preloop/Cargo.lock).
is_lockfile() {
  [[ "$1" =~ (^|/)Cargo\.lock$ ]]
}

is_manifest_or_lockfile() {
  is_manifest "$1" || is_lockfile "$1"
}

# lock_view <rev> <path>: the third-party view of one lockfile at one revision.
#
# A [[package]] block is third-party when it carries a `source` line; registry
# and git entries both have one, workspace members do not. The view keeps such
# blocks byte for byte and every line outside package blocks, and drops
# first-party blocks and blank lines. A lockfile that does not exist at <rev>
# yields no output at all.
lock_view() {
  local rev="$1" path="$2"
  if ! git cat-file -e "$rev:$path" 2>/dev/null; then
    return 0
  fi
  git show "$rev:$path" | awk '
    function emit_block() {
      if (in_package && has_source) {
        printf "%s", block
      }
      block = ""
      has_source = 0
      in_package = 0
    }
    /^\[\[package\]\][[:space:]]*$/ {
      emit_block()
      in_package = 1
      block = $0 "\n"
      next
    }
    /^\[/ {
      emit_block()
      if ($0 != "") {
        print
      }
      next
    }
    {
      if ($0 == "") {
        next
      }
      if (in_package) {
        block = block $0 "\n"
        if ($0 ~ /^source[[:space:]]*=/) {
          has_source = 1
        }
      } else {
        print
      }
    }
    END { emit_block() }
  '
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

# --no-renames: a rename is a deletion plus an addition, so both paths are in
# the list and a renamed lockfile or policy file cannot hide behind its new
# name.
diff_files="$(git diff --no-renames --name-only "$merge_base" "$head")"

# Decide whether any third-party lock entry changed. First pass over the diff:
# for each lockfile path, compare its normalized view at the merge base and at
# the head. One differing lockfile is enough to classify every manifest and
# lockfile in the diff as a dependency file.
third_party_changed=false
while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  is_lockfile "$file" || continue
  if [[ "$(lock_view "$merge_base" "$file")" != "$(lock_view "$head" "$file")" ]]; then
    third_party_changed=true
  fi
done <<<"$diff_files"

# Classify every changed path into the dependency/policy set or the other set,
# recording first-party manifest/lockfile edits separately for the OK message.
# Appending to an array is amortized O(1), so a PR with tens of thousands of
# paths costs linear work (a scalar `list="${list}${file}\n"` rebuilds the
# whole string per path, which is quadratic over an attacker-controlled diff).
dependency_or_policy_files=()
first_party_files=()
other_files=()
while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  if is_policy_file "$file"; then
    dependency_or_policy_files+=("$file")
  elif is_manifest_or_lockfile "$file"; then
    if [[ "$third_party_changed" == true ]]; then
      dependency_or_policy_files+=("$file")
    else
      # No third-party entry moved, so this is a first-party edit: a new or
      # renamed member, a path dependency, a test target. It is ordinary code.
      first_party_files+=("$file")
      other_files+=("$file")
    fi
  else
    other_files+=("$file")
  fi
done <<<"$diff_files"

# One rule: dependency/policy files may ship with each other and with nothing
# else. Both allowed combinations pass; the only failure is a mix.
if [[ "${#dependency_or_policy_files[@]}" -eq 0 ]]; then
  if [[ "${#first_party_files[@]}" -eq 0 ]]; then
    echo "Policy file guard: OK (no policy or manifest files modified)"
  else
    echo "Policy file guard: OK (manifests/lockfiles classified as first-party: no third-party lock entry changed)"
    print_files "    " "${first_party_files[@]}"
  fi
  exit 0
fi
if [[ "${#other_files[@]}" -eq 0 ]]; then
  echo "Dependency/policy PR detected: OK"
  print_files "    " "${dependency_or_policy_files[@]}"
  if [[ "$third_party_changed" == true ]]; then
    echo "  reason: third-party lock entries changed"
  fi
  exit 0
fi

cat >&2 <<EOF
ERROR: this PR mixes dependency/policy files with other files.

  dependency/policy files changed:
$(print_files "    " "${dependency_or_policy_files[@]}")
  other files changed:
$(print_files "    " "${other_files[@]}")
EOF

if [[ "$third_party_changed" == true ]]; then
  cat >&2 <<'EOF'

A third-party lock entry changed (a Cargo.lock [[package]] block with a
`source` line, seen through the merge-base/head comparison), so every manifest
and lockfile in this diff is a dependency file and may not ship with code.
EOF
else
  cat >&2 <<'EOF'

No third-party lock entry changed, so the manifests and lockfiles here are
first-party edits and may ship with code. The strict policy files above still
may not.
EOF
fi

cat >&2 <<'EOF'

What to do: split it. Put the dependency/policy changes in their own PR and
merge that first, then rebase this branch on main and open the code PR -- the
code PR then touches no policy file and no third-party lock entry.

The dependency/policy PR may contain any combination of:

  deny.toml  .cargo/audit.toml  .github/workflows/supply-chain.yml  supply-chain/**

...and must not contain anything else -- no source, tests, docs, changelog,
other workflows, justfile, or scripts. Cargo.toml and Cargo.lock join that set
only when a third-party lock entry changes; otherwise they are ordinary files
and may ship with the code that uses them.

There is no exception to this rule. The check re-runs on every push to the PR
and decides from what the branch contains, so a green run here means the split
is already done.

  run it locally with: supply-chain/guard-policy-files.sh <base> <head>
  policy: supply-chain/README.md
EOF
exit 1
