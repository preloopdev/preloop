server := "http://127.0.0.1:9090"
repo := "preloopdev/aksh"



build:
    cargo build --release -p aksh-runner-server

build-all:
    cargo build --release --workspace


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

test-ci: fmt-check clippy test
    @echo CI: all checks passed

#lint (ast-grep structural rules)

sg-scan:
    sg scan

sg-scan-strict:
    sg scan --error

#dogfood (e2e against aksh with real runner) 

dogfood: build
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

#submit 

submit-ci:
    cargo run -p aksh-runner-client -- --server {{server}} submit -W .github/workflows/ci.yml --repository {{repo}}

submit-dogfood:
    cargo run -p aksh-runner-client -- --server {{server}} submit -W fixtures/workflows/dogfood.yml

#runner 

build-runner:
    cargo build --release -p aksh-runner

runner-e2e WF:
    cargo run -p aksh-conformance -- runner-e2e --runner-bin target/release/aksh-runner --workflow {{WF}}

conform-runner S:
    cargo run -p aksh-conformance -- runner-diff --scenario {{S}} --target github

conform-local S:
    cargo run -p aksh-conformance -- runner-diff --scenario {{S}} --target aksh

conform-smoke: build-runner build
    cargo run -p aksh-conformance -- runner-e2e --runner-bin target/release/aksh-runner --workflow crates/aksh-conformance/fixtures/hello-world.yml --record-flows /tmp/smoke-flows.jsonl

conform-ci: build-all
    cargo run --release -p aksh-runner-server -- serve --listen 127.0.0.1:9090 & \
    SERVER_PID=$! ; \
    sleep 2 ; \
    cargo run -p runner-watch -- conform --runner v2.335.1 --aksh-url http://127.0.0.1:9090 ; \
    STATUS=$? ; \
    kill $SERVER_PID ; \
    exit $STATUS
