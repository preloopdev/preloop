#!/usr/bin/env bash
#
# Tests for supply-chain/guard-policy-files.sh.
#
#   bash supply-chain/tests/guard-policy-files.test.sh
#
# Each case builds a scratch repository with `git init`, commits a base and a
# head, and runs the guard inside that repository. No network, no fixtures
# in-tree.
set -uo pipefail

guard="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/guard-policy-files.sh"
if [[ ! -x "$guard" ]]; then
  echo "guard script not executable: $guard" >&2
  exit 1
fi

scratch="$(mktemp -d "${TMPDIR:-/tmp}/guard-policy-files-test.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT

failures=0
case_no=0

ok() {
  printf 'ok   %s\n' "$1"
}

bad() {
  printf 'FAIL %s\n' "$1"
  failures=$((failures + 1))
}

# seed <dir> <path> [content]: write a path, creating its parent directories.
seed() {
  local dir="$1" path="$2" content="${3:-seed}"
  mkdir -p "$dir/$(dirname "$path")"
  printf '%s\n' "$content" >"$dir/$path"
}

# new_repo <dir>: a repository with a base commit that already contains every
# path the cases touch, so the head commits modify rather than add.
new_repo() {
  local dir="$1"
  mkdir -p "$dir"
  git -C "$dir" init -q
  git -C "$dir" config user.email guard-test@example.com
  git -C "$dir" config user.name guard-test
  git -C "$dir" config commit.gpgsign false

  seed "$dir" README.md seed
  seed "$dir" CHANGELOG.md seed
  seed "$dir" justfile seed
  seed "$dir" Cargo.lock 'version = 3'
  seed "$dir" Cargo.toml '[workspace]'
  seed "$dir" crates/demo/Cargo.toml 'name = "demo"'
  seed "$dir" crates/demo/src/lib.rs '// demo'
  seed "$dir" benchmarks/preloop-perf/Cargo.toml 'name = "preloop-perf"'
  seed "$dir" benchmarks/preloop-perf/src/main.rs 'fn main() {}'
  seed "$dir" packaging/crates-io/preloop/Cargo.toml 'name = "preloop"'
  seed "$dir" packaging/crates-io/preloop/Cargo.lock 'version = 3'
  seed "$dir" packaging/crates-io/preloop/src/lib.rs '// packaging shim'
  seed "$dir" deny.toml 'bans = {}'
  seed "$dir" .cargo/audit.toml '[audit]'
  seed "$dir" .github/workflows/supply-chain.yml 'name: supply-chain'
  seed "$dir" .github/workflows/ci.yml 'name: ci'
  seed "$dir" supply-chain/config.toml '[cargo-vet]'
  seed "$dir" scripts/audit-node-externals.sh 'true'

  git -C "$dir" add -A
  git -C "$dir" commit -qm base
}

# modify <dir> <path> [content]: append, so the path shows as modified.
modify() {
  local dir="$1" path="$2" content="${3:-changed}"
  mkdir -p "$dir/$(dirname "$path")"
  printf '%s\n' "$content" >>"$dir/$path"
}

# run_guard <dir> [guard args...]: run the guard inside <dir>; sets $out, $status.
run_guard() {
  local dir="$1"
  shift
  out="$(cd "$dir" && "$guard" "$@" 2>&1)"
  status=$?
}

# expect <label> <expected-status> <dir> <base> <head> [output substring...]
expect() {
  local label="$1" want="$2" dir="$3" base="$4" head="$5"
  shift 5
  case_no=$((case_no + 1))
  run_guard "$dir" "$base" "$head"
  local problem=""
  if [[ "$status" -ne "$want" ]]; then
    problem="exit $status, wanted $want"
  fi
  local needle
  for needle in ${@+"$@"}; do
    # shellcheck disable=SC2076 # deliberate literal substring match
    if [[ "$out" != *"$needle"* ]]; then
      problem="${problem:+$problem; }missing output: $needle"
    fi
  done
  if [[ -n "$problem" ]]; then
    bad "$label ($problem)
--- output
$out
---"
  else
    ok "$label"
  fi
}

# case_start <name>: fresh repository, base recorded in $base.
base=""
case_start() {
  dir="$scratch/$1"
  new_repo "$dir"
  base="$(git -C "$dir" rev-parse HEAD)"
}

# commit_change <dir> <message>: commit the modifications; sets $head.
commit_change() {
  local dir="$1" message="$2"
  (cd "$dir" && git add -A && git commit -qm "$message")
  head="$(git -C "$dir" rev-parse HEAD)"
}

# --- PASS: nothing in the dependency/policy set is touched -------------------
case_start nothing
modify "$dir" crates/demo/src/lib.rs
commit_change "$dir" source
expect "source only -> OK" 0 "$dir" "$base" "$head" \
  "Policy file guard: OK (no policy or manifest files modified)"

case_start other-workflow
modify "$dir" .github/workflows/ci.yml '# comment'
commit_change "$dir" ci
expect ".github/workflows/ci.yml alone -> OK" 0 "$dir" "$base" "$head" \
  "Policy file guard: OK (no policy or manifest files modified)"

case_start scripts
modify "$dir" scripts/audit-node-externals.sh 'echo hi'
commit_change "$dir" scripts
expect "scripts/ alone -> OK" 0 "$dir" "$base" "$head" \
  "Policy file guard: OK (no policy or manifest files modified)"

case_start empty-diff
expect "no files changed -> OK" 0 "$dir" "$base" "$base" \
  "Policy file guard: OK (no policy or manifest files modified)"

# --- PASS: dependency/policy files, and only those ---------------------------
case_start deny-only
modify "$dir" deny.toml 'unknown-registry = "deny"'
commit_change "$dir" policy
expect "deny.toml only -> OK" 0 "$dir" "$base" "$head" \
  "Dependency/policy PR detected: OK" "- deny.toml"

case_start audit-only
modify "$dir" .cargo/audit.toml 'ignore = ["RUSTSEC-0000-0000"]'
commit_change "$dir" audit
expect ".cargo/audit.toml only -> OK" 0 "$dir" "$base" "$head" \
  "Dependency/policy PR detected: OK" "- .cargo/audit.toml"

case_start lock-only
modify "$dir" Cargo.lock 'new = "1.0.0"'
commit_change "$dir" lock
expect "Cargo.lock only -> OK" 0 "$dir" "$base" "$head" \
  "Dependency/policy PR detected: OK" "- Cargo.lock"

case_start manifest-only
modify "$dir" crates/demo/Cargo.toml 'new = "1.0.0"'
commit_change "$dir" manifest
expect "crates/demo/Cargo.toml alone -> OK" 0 "$dir" "$base" "$head" \
  "Dependency/policy PR detected: OK" "- crates/demo/Cargo.toml"

case_start guard-self
seed "$dir" supply-chain/guard-policy-files.sh '#!/usr/bin/env bash'
seed "$dir" supply-chain/README.md 'policy'
seed "$dir" supply-chain/tests/guard.test.sh '#!/usr/bin/env bash'
seed "$dir" .github/workflows/supply-chain.yml 'name: supply-chain # edited'
commit_change "$dir" guard
expect "supply-chain/** + its workflow -> OK" 0 "$dir" "$base" "$head" \
  "Dependency/policy PR detected: OK" "- supply-chain/README.md" \
  "- .github/workflows/supply-chain.yml"

# The consolidation shape: lockfile + every policy file + manifests at once.
case_start combined
modify "$dir" Cargo.lock 'new = "1.0.0"'
modify "$dir" deny.toml 'unknown-registry = "deny"'
modify "$dir" .cargo/audit.toml 'ignore = ["RUSTSEC-0000-0000"]'
modify "$dir" .github/workflows/supply-chain.yml '# edited'
modify "$dir" supply-chain/audits.toml '[[audits]]'
modify "$dir" supply-chain/config.toml '[policy]'
modify "$dir" Cargo.toml 'new = "1.0.0"'
modify "$dir" crates/demo/Cargo.toml 'new = "1.0.0"'
commit_change "$dir" combined
expect "lockfile + policy + manifests together -> OK" 0 "$dir" "$base" "$head" \
  "Dependency/policy PR detected: OK" "- Cargo.lock" "- deny.toml" \
  "- Cargo.toml" "- crates/demo/Cargo.toml"

# --- PASS: manifests and lockfiles anywhere in the tree ----------------------
case_start benchmarks-manifest
modify "$dir" Cargo.lock 'preloop-perf = "0.1.0"'
modify "$dir" benchmarks/preloop-perf/Cargo.toml 'criterion = "0.5"'
commit_change "$dir" bench-deps
expect "Cargo.lock + benchmarks/preloop-perf/Cargo.toml -> OK" 0 "$dir" "$base" "$head" \
  "Dependency/policy PR detected: OK" "- benchmarks/preloop-perf/Cargo.toml"

case_start nested-lockfile
modify "$dir" packaging/crates-io/preloop/Cargo.lock 'standalone = "1.0.0"'
modify "$dir" packaging/crates-io/preloop/Cargo.toml 'standalone = "1.0.0"'
commit_change "$dir" packaging-deps
expect "nested lockfile + its manifest -> OK" 0 "$dir" "$base" "$head" \
  "Dependency/policy PR detected: OK" "- packaging/crates-io/preloop/Cargo.lock"

case_start deep-manifest
modify "$dir" Cargo.lock 'new = "1.0.0"'
modify "$dir" crates/demo/nested/Cargo.toml 'new = "1.0.0"'
commit_change "$dir" nested-manifest
expect "Cargo.lock + crates/demo/nested/Cargo.toml -> OK (any manifest)" 0 "$dir" "$base" "$head" \
  "Dependency/policy PR detected: OK" "- crates/demo/nested/Cargo.toml"

# --- FAIL: a manifest or lockfile plus anything else -------------------------
case_start packaging-manifest-and-doc
modify "$dir" packaging/crates-io/preloop/Cargo.toml 'publish = true'
modify "$dir" README.md 'docs'
commit_change "$dir" packaging-docs
expect "packaging manifest + README.md -> FAIL" 1 "$dir" "$base" "$head" \
  "ERROR: this PR mixes dependency/policy files with other files" \
  "- packaging/crates-io/preloop/Cargo.toml" "- README.md"

case_start nested-lockfile-and-source
modify "$dir" packaging/crates-io/preloop/Cargo.lock 'standalone = "1.0.0"'
modify "$dir" packaging/crates-io/preloop/src/lib.rs '// edit'
commit_change "$dir" packaging-lock-src
expect "nested lockfile + source -> FAIL" 1 "$dir" "$base" "$head" \
  "ERROR: this PR mixes dependency/policy files with other files" \
  "- packaging/crates-io/preloop/Cargo.lock" "- packaging/crates-io/preloop/src/lib.rs"

case_start benchmarks-manifest-and-source
modify "$dir" benchmarks/preloop-perf/Cargo.toml 'criterion = "0.5"'
modify "$dir" benchmarks/preloop-perf/src/main.rs 'fn main() { /* bench */ }'
commit_change "$dir" bench-manifest-src
expect "benchmarks manifest + its source -> FAIL" 1 "$dir" "$base" "$head" \
  "ERROR: this PR mixes dependency/policy files with other files" \
  "- benchmarks/preloop-perf/src/main.rs"

# --- FAIL: a dependency/policy file plus anything else -----------------------
case_start lock-and-source
modify "$dir" Cargo.lock 'new = "1.0.0"'
modify "$dir" crates/demo/Cargo.toml 'new = "1.0.0"'
modify "$dir" crates/demo/src/lib.rs 'use new;'
commit_change "$dir" deps-and-code
expect "Cargo.lock + manifest + source -> FAIL" 1 "$dir" "$base" "$head" \
  "ERROR: this PR mixes dependency/policy files with other files" \
  "crates/demo/src/lib.rs" "split it" "merge that first"

case_start combined-and-doc
modify "$dir" Cargo.lock 'new = "1.0.0"'
modify "$dir" deny.toml 'unknown-registry = "deny"'
modify "$dir" supply-chain/audits.toml '[[audits]]'
modify "$dir" Cargo.toml 'new = "1.0.0"'
modify "$dir" crates/demo/Cargo.toml 'new = "1.0.0"'
modify "$dir" README.md 'docs'
commit_change "$dir" combined-docs
expect "combined set + README.md -> FAIL" 1 "$dir" "$base" "$head" \
  "ERROR: this PR mixes dependency/policy files with other files" \
  "- README.md" "- Cargo.lock" "- deny.toml" "- supply-chain/audits.toml"

case_start deny-and-justfile
modify "$dir" deny.toml 'unknown-registry = "deny"'
modify "$dir" justfile 'supply-chain-guard:'
commit_change "$dir" policy-and-justfile
expect "deny.toml + justfile -> FAIL" 1 "$dir" "$base" "$head" \
  "ERROR: this PR mixes dependency/policy files with other files" \
  "- justfile" "- deny.toml"

case_start policy-workflow-and-other-workflow
modify "$dir" .github/workflows/supply-chain.yml '# edited'
modify "$dir" .github/workflows/ci.yml '# edited'
commit_change "$dir" workflows
expect "supply-chain.yml + ci.yml -> FAIL" 1 "$dir" "$base" "$head" \
  "ERROR: this PR mixes dependency/policy files with other files" \
  "- .github/workflows/ci.yml"

case_start changelog
modify "$dir" Cargo.lock 'new = "1.0.0"'
modify "$dir" CHANGELOG.md 'entry'
commit_change "$dir" changelog
expect "Cargo.lock + CHANGELOG.md -> FAIL" 1 "$dir" "$base" "$head" \
  "ERROR: this PR mixes dependency/policy files with other files" \
  "- CHANGELOG.md"

# --- the failure message offers no escape hatch ------------------------------
case_no=$((case_no + 1))
case "$out" in
  *label* | *Label* | *exception:* | *"dependency migration"*)
    bad "failure message must not offer a label or exception path"
    ;;
  *)
    ok "failure message offers no label or exception path"
    ;;
esac

# --- the failure message keeps both headings on their own lines --------------
case_no=$((case_no + 1))
if grep -q '^  dependency/policy files changed:$' <<<"$out" &&
  grep -q '^  other files changed:$' <<<"$out"; then
  ok "failure message lists both sets under their own headings"
else
  bad "failure message must list both sets under their own headings
--- output
$out
---"
fi

# --- an identical change on both tips is still caught ------------------------
# Both tips end up with the same trees, so a tip-to-tip diff is empty; only a
# merge-base diff sees the dependency touch. The workflow checks out a synthetic
# merge commit, which is where this shape comes from.
case_start identical-on-both-tips
modify "$dir" Cargo.lock 'new = "1.0.0"'
modify "$dir" crates/demo/src/lib.rs 'use new;'
commit_change "$dir" topic
topic="$head"
git -C "$dir" checkout -q "$base"
git -C "$dir" checkout -q -b mainline
modify "$dir" Cargo.lock 'new = "1.0.0"'
modify "$dir" crates/demo/src/lib.rs 'use new;'
commit_change "$dir" mainline
mainline="$head"

if [[ -n "$(git -C "$dir" diff --name-only "$mainline" "$topic")" ]]; then
  case_no=$((case_no + 1))
  bad "fixture broken: the two tips were expected to be identical"
else
  expect "identical change on both tips -> FAIL (merge-base diff)" 1 \
    "$dir" "$mainline" "$topic" \
    "ERROR: this PR mixes dependency/policy files with other files" \
    "- Cargo.lock"
fi

# --- a policy-only branch over a moved main is still OK ----------------------
case_start moved-base
git -C "$dir" checkout -q -b topic
modify "$dir" deny.toml 'unknown-registry = "deny"'
commit_change "$dir" policy
topic="$head"
git -C "$dir" checkout -q "$base"
git -C "$dir" checkout -q -b mainline
modify "$dir" crates/demo/src/lib.rs '// mainline work'
commit_change "$dir" mainline
mainline="$head"
expect "policy-only branch over a moved main -> OK" 0 "$dir" "$mainline" "$topic" \
  "Dependency/policy PR detected: OK" "- deny.toml"

# --- fail-closed paths ------------------------------------------------------
case_start fail-closed
case_no=$((case_no + 1))
run_guard "$dir" "origin/main" "HEAD"
if [[ "$status" -eq 2 && "$out" == *"cannot resolve base revision"* ]]; then
  ok "unresolvable revision -> exit 2 (fails closed)"
else
  bad "unresolvable revision -> exit 2 (fails closed), got exit $status
--- output
$out
---"
fi

case_no=$((case_no + 1))
run_guard "$dir" "origin/main"
if [[ "$status" -eq 2 && "$out" == *"usage:"* ]]; then
  ok "missing argument -> exit 2 with usage"
else
  bad "missing argument -> exit 2 with usage, got exit $status
--- output
$out
---"
fi

case_no=$((case_no + 1))
new_repo "$scratch/no-merge-base"
dir="$scratch/no-merge-base"
root="$(git -C "$dir" rev-parse HEAD)"
git -C "$dir" checkout -q --orphan unrelated
git -C "$dir" rm -rqf .
seed "$dir" other.txt other
git -C "$dir" add -A
git -C "$dir" commit -qm 'unrelated root'
orphan="$(git -C "$dir" rev-parse HEAD)"
run_guard "$dir" "$root" "$orphan"
if [[ "$status" -eq 2 && "$out" == *"no merge base"* ]]; then
  ok "no merge base -> exit 2 (fails closed)"
else
  bad "no merge base -> exit 2 (fails closed), got exit $status
--- output
$out
---"
fi

printf '\n%d case(s), %d failure(s)\n' "$case_no" "$failures"
[[ "$failures" -eq 0 ]]
