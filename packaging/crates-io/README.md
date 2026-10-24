# crates.io name claim

`preloop/` is a standalone crate (its own workspace, no dependencies) whose only
job is to hold the `preloop` name on crates.io. It is deliberately not a member
of the repository workspace, so claiming the name never rewrites `Cargo.lock` —
the supply-chain policy keeps policy files in their own PR.

## First publish, by hand — once

crates.io cannot attach a trusted publisher to a crate that does not exist, so
the first version is published manually. From a checkout of `main`:

```sh
cd packaging/crates-io/preloop
cargo publish --dry-run   # packaging check, no token needed
cargo login               # crates.io token scoped to this crate name, publish-new
cargo publish
```

Then attach the release workflow as a trusted publisher, at
<https://crates.io/crates/preloop/settings> → Trusted Publishing → Add:

| Field | Value |
|---|---|
| Repository owner | `preloopdev` |
| Repository name | `preloop` |
| Workflow filename | `release.yml` |

The token from `cargo login` can be revoked once that is configured.

Publishing the placeholder version (`0.1.0`) is fine — the next tag republishes
at the release version. To make the crates.io page match the current release
immediately, set `version` in that `Cargo.toml` to the release version before
publishing; the workflow overwrites it from the tag anyway.

## Every release after that

`publish-crates-io` in `.github/workflows/release.yml` publishes this crate at
the release version on each stable tag (prereleases follow
`dist-workspace.toml`'s `publish-prereleases`), authenticating with that trusted
publisher — the repository stores no crates.io token. The job:

- skips with a workflow warning while the name is unclaimed, so a tag never
  fails on this;
- no-ops when the version is already published, so re-running a tag is safe;
- rewrites the manifest's placeholder version from the dist plan before
  publishing (`--allow-dirty`), because the tag — not this file — is the
  authoritative version. Nothing in the repository reads that manifest.

If preloop ever ships on crates.io for real, this crate is the natural home for
that package: add the real `[[bin]]` and drop the name-claim framing.
