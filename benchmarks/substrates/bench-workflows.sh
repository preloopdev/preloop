#!/usr/bin/env bash
# Create the benchmark workspace: a real git repo with the workflow corpus.
set -euo pipefail
WORK=~/bench-workspace
mkdir -p "$WORK/.github/workflows" "$WORK/src"
cd "$WORK"

cat > .github/workflows/bench-hello.yml <<'YAML'
name: bench-hello
on: [push, workflow_dispatch]
jobs:
  hello:
    runs-on: ubuntu-latest
    steps:
      - run: echo "hello from $(hostname)"
      - run: uname -r; nproc; free -m | sed -n 2p
YAML

cat > .github/workflows/bench-io.yml <<'YAML'
name: bench-io
on: [push, workflow_dispatch]
jobs:
  io:
    runs-on: ubuntu-latest
    steps:
      - name: report the filesystem under test
        run: stat -fc '%T' . ; df -h . | tail -1
      - name: sequential write 1 GiB
        run: dd if=/dev/zero of=./seq bs=1M count=1024 conv=fsync
      - name: sequential read 1 GiB
        run: dd if=./seq of=/dev/null bs=1M
      - name: 20k small files
        run: |
          mkdir -p many && cd many
          for i in $(seq 1 20000); do echo "$i" > "f$i"; done
          sync
          ls | wc -l
      - name: tar roundtrip
        run: tar -C many -cf many.tar . && rm -rf many && mkdir many2 && tar -C many2 -xf many.tar && ls many2 | wc -l
      - name: clean up
        run: rm -rf seq many.tar many2
YAML

cat > .github/workflows/bench-checkout-rust.yml <<'YAML'
name: bench-checkout-rust
on: [push, workflow_dispatch]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: cargo build
        run: |
          export PATH="/usr/local/cargo/bin:$HOME/.cargo/bin:$PATH"
          cargo --version
          cd bench-crate && cargo build --release 2>&1 | tail -5
YAML

cat > .github/workflows/bench-node.yml <<'YAML'
name: bench-node
on: [push, workflow_dispatch]
jobs:
  node:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: npm install
        run: |
          export PATH="/usr/local/go/bin:$PATH"
          node --version
          cd bench-node && npm install --no-audit --no-fund 2>&1 | tail -3
          ls node_modules | wc -l
YAML

cat > .github/workflows/bench-matrix8.yml <<'YAML'
name: bench-matrix8
on: [push, workflow_dispatch]
jobs:
  fan:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        shard: [1, 2, 3, 4, 5, 6, 7, 8]
    steps:
      - run: echo "shard ${{ matrix.shard }} on $(hostname)"
      - run: dd if=/dev/zero of=./x bs=1M count=64 conv=fsync && rm -f ./x
YAML

# A small Rust crate: real compile work, no network beyond crates.io-free deps.
mkdir -p bench-crate/src
cat > bench-crate/Cargo.toml <<'TOML'
[package]
name = "bench-crate"
version = "0.1.0"
edition = "2021"

[dependencies]
TOML
cat > bench-crate/src/main.rs <<'RS'
/// Enough monomorphization to make the compiler do real work without a
/// network fetch, so the measurement stays on the substrate.
fn collatz(mut n: u64) -> u32 {
    let mut steps = 0;
    while n != 1 {
        n = if n % 2 == 0 { n / 2 } else { 3 * n + 1 };
        steps += 1;
    }
    steps
}

fn main() {
    let worst = (1..200_000u64).map(|n| (collatz(n), n)).max().unwrap();
    println!("worst: {worst:?}");
}
RS

mkdir -p bench-node
cat > bench-node/package.json <<'JSON'
{
  "name": "bench-node",
  "version": "1.0.0",
  "private": true,
  "dependencies": {
    "express": "4.19.2",
    "lodash": "4.17.21",
    "chalk": "4.1.2"
  }
}
JSON

git init -q 2>/dev/null || true
git config user.email bench@local
git config user.name bench
git add -A
git commit -qm "benchmark corpus" 2>/dev/null || git commit -qm "update corpus"
echo "workspace ready: $WORK"
ls .github/workflows
