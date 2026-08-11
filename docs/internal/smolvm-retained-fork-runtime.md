# Temporary retained-checkpoint SmolVM runtime

SmolVM PR [#888](https://github.com/smol-machines/smolvm/pull/888) fixes the
plain-fork path so a paused golden retains its RAM checkpoint for later clones.
Until that change is included in an official release, Preloop defaults to the
`preloopdev/smolvm` fork at version `1.7.4` and enables
`PRELOOP_SMOLVM_RETAINED_FORKS`.

When SmolVM publishes the official release containing the fix:

1. Change `SMOLVM_REPOSITORY` in `crates/preloop-cli/src/update.rs` back to
   `smol-machines/smolvm`.
2. Update `SMOLVM_VERSION` to the official fixed version.
3. Keep the per-golden fork mutex; it serializes the short fork operations.
4. Remove the temporary `PRELOOP_SMOLVM_RELEASE_REPOSITORY` and
   `PRELOOP_SMOLVM_RELEASE_VERSION` defaults.
5. Retain the old-runtime escape hatch only if compatibility with older
   SmolVM installations is still required.
