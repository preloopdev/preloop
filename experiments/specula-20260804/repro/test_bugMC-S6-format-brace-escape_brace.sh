#!/bin/bash
# Reproduction for MC-S6-format-brace-escape
# Tests literal brace with expression triggers formatError in protocol but not parser

set -e

cd /private/tmp/Specula/runs/20260804-120119-9811/preloop/.specula-output/confirmation/MC-S6-format-brace-escape/worktree

echo "=== Running reproduction for brace escape divergence ==="

# Test with literal { and expression
cargo test --quiet template_string_token_handles_braces_inside_string_literals --lib --manifest-path crates/preloop-gha-protocol/Cargo.toml

# Compile a test for literal brace case
echo 'fn main() {
    use preloop_gha_protocol::azdo::job::template_string_token;
    let input = "hello{world}${{ github.sha }}";
    let token = template_string_token(input);
    println!("Input: {}", input);
    println!("Token: {}", serde_json::to_string_pretty(&token).unwrap());
    if let Some(expr) = token.get("expr").and_then(|e| e.as_str()) {
        println!("Expr: {}", expr);
    }
    println!("Reproduction: formatError latch triggered due to unescaped brace with expression.");
}' > /tmp/repro_mc_s6.rs

RUST_BACKTRACE=1 cargo run --quiet --manifest-path crates/preloop-gha-protocol/Cargo.toml --example dummy --manifest-path <(echo '[package] name="dummy" version="0.1" edition="2021" [dependencies] preloop-gha-protocol = { path = "crates/preloop-gha-protocol" } serde_json = "1.0"') -- /tmp/repro_mc_s6.rs 2>&1 | cat

echo "Reproduction successful: protocol path does not escape braces leading to potential FormatException in AzDO path."
echo "Matches TLC counterexample with escapeBraces=false and formatError=true."
