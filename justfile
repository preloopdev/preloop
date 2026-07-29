server := "http://127.0.0.1:9090"
repo := "preloopdev/aksh"

build:
    cargo build --release -p aksh-runner-server

build-all:
    cargo build --release --workspace

#preloop

# Build the macOS CLI/server and the ARM64 Linux runner used inside SmolVM.
build-preloop:
    cargo zigbuild -p aksh-runner --target aarch64-unknown-linux-gnu
    cargo build -p preloop-cli -p aksh-runner-server

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
    cargo check --workspace

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

clippy:
    cargo clippy --workspace --all-targets -- -D warnings


test:
    cargo test --workspace --quiet

test-properties-full:
    PROPTEST_CASES=10000 cargo test -p aksh-runner-server --quiet
    PROPTEST_CASES=10000 cargo test -p aksh-runner-server --quiet -- --ignored

test-ci: fmt-check clippy
    PROPTEST_CASES=8 cargo test --workspace --quiet
    just conform
    @echo CI: all checks passed

#lint (ast-grep structural rules)

sg-scan:
    sg scan

sg-scan-strict:
    sg scan --error

#dogfood (e2e against aksh with real runner) 

dogfood: build
    ./scripts/aksh-e2e-bench.sh

# Preloop end-to-end performance benchmark (see benchmarks/preloop-perf/).
bench-preloop:
    ./autoresearch.sh

# Same harness, single trial and short load windows.
bench-preloop-quick:
    ./autoresearch.sh --quick

# e2e redirect (one-time setup) 

e2e-setup:
    sudo ./scripts/e2e-setup.sh

e2e-status:
    ./scripts/e2e-setup.sh --status

e2e-teardown:
    sudo ./scripts/e2e-setup.sh --teardown

#serve

serve:
    AKSH_LOCAL_WORKSPACE="${AKSH_LOCAL_WORKSPACE:-$PWD}" cargo run --release -p aksh-runner-server -- serve --listen 127.0.0.1:9090

serve-dev:
    AKSH_LOCAL_WORKSPACE="${AKSH_LOCAL_WORKSPACE:-$PWD}" cargo run --release -p aksh-runner-server -- serve --listen 127.0.0.1:9090 --enable-test-api --test-api-token dev-token

#submit 

submit-ci:
    cargo run -p aksh-runner-client -- --server {{server}} submit -W .github/workflows/ci.yml --repository {{repo}}

submit-dogfood:
    cargo run -p aksh-runner-client -- --server {{server}} submit -W fixtures/workflows/dogfood.yml

#runner 

build-runner:
    cargo build --release -p aksh-runner

runner-e2e WF:
    cargo run -p aksh-conformance -- runner-e2e --runner-bin target/release/preloop-runner --workflow {{WF}}

# Replay every committed official-runner flow and fail on protocol drift.
conform:
    bash ./benchmarks/conformance/run.sh

# Replay every committed official-runner flow against the current server.
conform-server-light:
    bash ./benchmarks/conformance/run.sh

# Build the current server, run live official-runner GitHub/server comparisons,
# and fail on conclusion, job, step, or flow-count differences.
conform-server-deep:
    cargo zigbuild -p aksh-runner-server --release --target aarch64-unknown-linux-musl
    bash ./scripts/conform-server-deep.sh

# Compare workflow/job status responses from the official and aksh runners.
conform-runner-light:
    python3 benchmarks/real-world/runner-conformance.py --mode light

# Run the numbered runner corpus on smolVM with both runner implementations,
# then fail on workflow, job, or step-level differences.
conform-runner-deep:
    bash ./benchmarks/real-world/batch-conformance.sh both '10[1-9]-*' '110-*'
    python3 benchmarks/real-world/runner-conformance.py --mode deep
