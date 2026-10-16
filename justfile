server := "http://127.0.0.1:9090"
repo := "preloopdev/preloop"

build:
    cargo build --locked --release -p preloop-runner-server

build-all:
    cargo build --locked --release --workspace

#preloop

# Build the macOS CLI/server and the ARM64 Linux runner used inside SmolVM.
build-preloop:
    cargo zigbuild -p preloop-runner --target aarch64-unknown-linux-gnu
    cargo build -p preloop-cli -p preloop-runner-server

# Run a workflow locally. A failed step pauses for `preloop debug`.
preloop-run WF="fixtures/workflows/failing.yml": build-preloop
    ./target/debug/preloop run -f "{{WF}}"

# Submit a workflow and return immediately, keeping a failed VM for
# `just preloop-shell`. Nothing is attached, so it preserves instead of pausing.
preloop-run-detached WF="fixtures/workflows/failing.yml": build-preloop
    ./target/debug/preloop run -f "{{WF}}" --detach --preserve-on-failure

# Open the most recently preserved failed runner VM.
preloop-shell: build-preloop
    ./target/debug/preloop shell


check:
    cargo check --locked --workspace

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

clippy:
    cargo clippy --locked --workspace --all-targets -- -D warnings


test:
    cargo test --locked --workspace --quiet

test-properties-full:
    PROPTEST_CASES=10000 cargo test --locked -p preloop-runner-server --quiet
    PROPTEST_CASES=10000 cargo test --locked -p preloop-runner-server --quiet -- --ignored

# Control-database migration gate (refinery over migrations/{sqlite,postgres}):
# fresh init, populated-baseline upgrade, refusal/rollback and adoption, on
# SQLite always and on PostgreSQL when PRELOOP_TEST_POSTGRES_URL names a
# server (the control-plane workflow sets it in every matrix leg). Set
# PRELOOP_TEST_REQUIRE_POSTGRES=1 to make a missing server a failure instead
# of a skip.
test-control-migrations:
    cargo test --locked -p preloop-runner-server --quiet control::migrate_runner

# Apply pending control-database migrations (creates a fresh store). Run
# before `just serve` on a new PRELOOP_HOME; `just serve` does it for you.
store-migrate:
    PRELOOP_HOME="${PRELOOP_HOME:-$PWD/.preloop}" cargo run -p preloop-cli -- store migrate

store-status:
    PRELOOP_HOME="${PRELOOP_HOME:-$PWD/.preloop}" cargo run -p preloop-cli -- store status

# Security linter for GitHub Actions workflows
zizmor:
    uvx zizmor .github/workflows/

test-ci: fmt-check clippy zizmor
    PROPTEST_CASES=8 cargo test --locked --workspace --quiet
    just test-control-migrations
    just conform
    @echo CI: all checks passed

# Supply-chain gate: cargo vet (supply-chain/audits.toml), cargo audit (RustSec),
# cargo deny check all (licenses/bans/sources), and Node externals OSV/SBOM audit.
# Local runs are on trusted code (report-only for Node externals). CI gate is
# .github/workflows/supply-chain.yml (runs-on: ubuntu-latest; triggers: pull_request,
# push to main, weekly schedule) — it drops any PR-shipped .cargo/config*, rejects
# mixed policy PRs, runs the Node externals OSV+SBOM audit, and enforces upstream
# pin parity.
supply-chain:
    cargo vet
    cargo audit
    cargo deny check all
    ./scripts/audit-node-externals.sh --report-only

#lint (ast-grep structural rules)

sg-scan:
    ast-grep scan

sg-scan-strict:
    ast-grep scan --error

#dogfood (e2e against preloop with real runner) 

dogfood: build
    ./scripts/preloop-e2e-bench.sh

# Preloop end-to-end performance benchmark (see benchmarks/preloop-perf/).
bench-preloop:
    ./autoresearch.sh

# Same harness, single trial and short load windows.
bench-preloop-quick:
    ./autoresearch.sh --quick

# e2e redirect (one-time setup) 

# Local runner-client commands discover the persisted token in this same
# engine home. Set PRELOOP_SYSTEM_TOKEN explicitly for a different server.

serve: store-migrate
    PRELOOP_HOME="${PRELOOP_HOME:-$PWD/.preloop}" PRELOOP_LOCAL_WORKSPACE="${PRELOOP_LOCAL_WORKSPACE:-$PWD}" cargo run --release -p preloop-runner-server -- serve --listen 127.0.0.1:9090

serve-dev: store-migrate
    PRELOOP_HOME="${PRELOOP_HOME:-$PWD/.preloop}" PRELOOP_LOCAL_WORKSPACE="${PRELOOP_LOCAL_WORKSPACE:-$PWD}" cargo run --release -p preloop-runner-server -- serve --listen 127.0.0.1:9090 --enable-test-api --test-api-token dev-token

#submit

submit-ci:
    PRELOOP_HOME="${PRELOOP_HOME:-$PWD/.preloop}" cargo run -p preloop-runner-client -- --server {{server}} submit -W .github/workflows/ci.yml --repository {{repo}}

submit-dogfood:
    PRELOOP_HOME="${PRELOOP_HOME:-$PWD/.preloop}" cargo run -p preloop-runner-client -- --server {{server}} submit -W fixtures/workflows/dogfood.yml

#runner 

build-runner:
    cargo build --release -p preloop-runner

runner-e2e WF:
    cargo run -p preloop-conformance -- runner-e2e --runner-bin target/release/preloop-runner --workflow {{WF}}

# Replay every committed official-runner flow and fail on protocol drift.
conform:
    bash ./benchmarks/conformance/run.sh

# Replay every committed official-runner flow against the current server.
conform-server-light:
    bash ./benchmarks/conformance/run.sh

# Build the current server, run live official-runner GitHub/server comparisons,
# and fail on conclusion, job, step, or flow-count differences.
conform-server-deep:
    cargo zigbuild -p preloop-runner-server --release --target aarch64-unknown-linux-musl
    bash ./scripts/conform-server-deep.sh

# Run the five-repository campaign against the pinned 9GB official runner
# golden. Override PRELOOP_GOLDEN_ARTIFACT when the cache lives elsewhere.
conform-5repos:
    bash ./benchmarks/real-world/conformance-5repos.sh

# Compare workflow/job status responses from the official and preloop runners.
conform-runner-light:
    python3 benchmarks/real-world/runner-conformance.py --mode light

# Run the numbered runner corpus on smolVM with both runner implementations,
# then fail on workflow, job, or step-level differences.
conform-runner-deep:
    bash ./benchmarks/real-world/batch-conformance.sh both '10[1-9]-*' '110-*'
    python3 benchmarks/real-world/runner-conformance.py --mode deep

#release

# Promote the [Unreleased] changelog section into a dated release entry
# (VERSION as X.Y.Z or vX.Y.Z). Fill in the section body before tagging: the
# release workflow fails tags without a matching entry in CHANGELOG.md.
changelog-release VERSION:
    python3 scripts/changelog-release.py "{{VERSION}}"

# --- preloop-dev SmolVM: per-worktree dev VMs forked from a frozen golden ---
# Agents: run `just dev-vm-check` first; if it fails, use the host toolchain.
# Optional env: PRELOOP_DEV_GATEWAY_URL + PRELOOP_DEV_GATEWAY_TOKEN_FILE route the
# VM's OMP through the host's auth gateway (credentials never enter the VM).
# See scripts/dev-vm/common.sh.

# Succeeds only if the preloop-dev golden exists on this host and can be forked.
dev-vm-check:
    scripts/dev-vm/check.sh

# Fork the golden for this worktree (idempotent).
dev-vm-up:
    scripts/dev-vm/up.sh

# Run a command in this worktree's dev VM, in /workspace.
[positional-arguments]
dev-vm-exec +CMD:
    scripts/dev-vm/exec.sh "$@"

# Interactive shell (or `just dev-vm-shell omp`) in this worktree's dev VM.
[positional-arguments]
dev-vm-shell *CMD:
    scripts/dev-vm/shell.sh "$@"

# Delete this worktree's dev VM; the worktree is untouched.
dev-vm-down:
    scripts/dev-vm/down.sh

# List dev VMs forked from the golden.
dev-vm-ls:
    scripts/dev-vm/ls.sh

# Publish guest ports (HOST:GUEST) to the host; restarts the VM. Servers in the
# guest must bind 0.0.0.0. Prefer PRELOOP_DEV_PORTS="18080:8080" with dev-vm-up.
[positional-arguments]
dev-vm-publish +PORTS:
    scripts/dev-vm/publish.sh "$@"

# Re-sync the VM's OMP providers with the host gateway (after a new login/key).
dev-vm-omp-sync:
    scripts/dev-vm/omp-sync.sh
