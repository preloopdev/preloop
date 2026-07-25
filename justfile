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

# Run a workflow locally with failed VMs preserved for `just preloop-shell`.
preloop-run WF="fixtures/workflows/failing.yml": build-preloop
    ./target/debug/preloop run -f "{{WF}}" --preserve-on-failure

# Submit a workflow and return immediately.
preloop-run-detached WF="fixtures/workflows/failing.yml": build-preloop
    ./target/debug/preloop run -f "{{WF}}" --preserve-on-failure --detach

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

conform-runner S:
    cargo run -p aksh-conformance -- runner-diff --scenario {{S}} --target github

conform-local S:
    cargo run -p aksh-conformance -- runner-diff --scenario {{S}} --target aksh

conform-smoke: build-runner build
    cargo run -p aksh-conformance -- runner-e2e --runner-bin target/release/preloop-runner --workflow crates/aksh-conformance/fixtures/hello-world.yml --record-flows /tmp/smoke-flows.jsonl

conform-ci: build-all
    cargo run --release -p aksh-runner-server -- serve --listen 127.0.0.1:9090 & \
    SERVER_PID=$! ; \
    sleep 2 ; \
    cargo run -p runner-watch -- conform --runner v2.335.1 --aksh-url http://127.0.0.1:9090 ; \
    STATUS=$? ; \
    kill $SERVER_PID ; \
    exit $STATUS
