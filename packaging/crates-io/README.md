# crates.io name claim

`preloop/` is a standalone crate (its own workspace, no dependencies) whose only
job is to hold the `preloop` name on crates.io. It is deliberately not a member
of the repository workspace, so claiming the name never rewrites `Cargo.lock` —
the supply-chain policy keeps policy files in their own PR.

Publish once, from the crate directory:

```sh
cd packaging/crates-io/preloop
cargo publish --dry-run    # packaging check, no token needed
cargo publish              # needs `cargo login` / CARGO_REGISTRY_TOKEN
```

Nothing depends on it and no workflow publishes it. If preloop ever ships on
crates.io for real, this crate is the natural home for that package: bump the
version past `0.1.0` and add the real `[[bin]]`.
