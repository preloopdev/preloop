# SmolVM packed-machine socket fix

A focused handoff for upstreaming one correctness fix to SmolVM.

Verified against:

- SmolVM v1.6.13
- upstream commit `a31810ebdaed21336562c1b956f33817490bd357`
- `src/cli/machine.rs` in both versions

## Bug

`smolvm machine create` accepts `--mount-socket` and `--expose-socket` when
`--from <artifact.smolmachine>` is used, but the packed-machine branch silently
discards both options.

Normal image creation does this:

```rust
params.published_sockets =
    parse_published_sockets(&self.expose_socket, &self.mount_socket)?;
```

Packed creation in `CreateCmd::run_from_smolmachine` currently does this:

```rust
published_sockets: Vec::new(),
```

As a result, packed-machine creation succeeds, but the socket relay is absent at
boot. The failure appears later when a guest or host process tries to connect.

Source:

- v1.6.13:
  <https://github.com/smol-machines/smolvm/blob/v1.6.13/src/cli/machine.rs>
- current source inspected:
  <https://github.com/smol-machines/smolvm/blob/a31810ebdaed21336562c1b956f33817490bd357/src/cli/machine.rs>

## Minimal fix

Change only `CreateCmd::run_from_smolmachine` in `src/cli/machine.rs`.

Parse the local CLI flags with the existing helper:

```rust
let published_sockets =
    parse_published_sockets(&self.expose_socket, &self.mount_socket)?;
```

Then pass the result into `CreateVmParams`:

```rust
let params = vm_common::CreateVmParams {
    // existing fields
    published_sockets,
    // existing fields
};
```

Conceptual diff:

```diff
 fn run_from_smolmachine(&self, sidecar_path: &Path) -> smolvm::Result<()> {
+    let published_sockets =
+        parse_published_sockets(&self.expose_socket, &self.mount_socket)?;
     // existing artifact validation and manifest loading

     let params = vm_common::CreateVmParams {
         // existing fields
-        published_sockets: Vec::new(),
+        published_sockets,
         // existing fields
     };
 }
```

Place parsing before expensive artifact extraction. Reuse
`parse_published_sockets`; do not duplicate its validation.

## Security invariant

A portable `.smolmachine` must not carry authority over paths on the importing
host.

This patch is safe because it honors only socket mappings explicitly supplied
by the local user during `machine create`. It must not read socket mappings from
the artifact manifest.

Do not weaken egress restrictions or replace the Unix-socket relay with broad
host TCP access.

## Tests

Keep the test change focused.

Add one regression test at the existing packed-create test layer:

1. Create or reuse the smallest existing `.smolmachine` fixture.
2. Invoke packed creation with one `--mount-socket` and one `--expose-socket`.
3. Assert the resulting VM record contains both `PublishedSocketConfig` values,
   with the correct directions and exact host/guest paths.
4. Assert packed creation without either flag still stores an empty list.

If the existing integration harness already boots VMs and tests published
sockets, add one packed-machine round-trip case there. Do not build a new test
framework solely for this fix.

Existing `parse_published_sockets` tests already cover malformed syntax and
separator validation. Do not duplicate all parser tests for the packed branch;
the regression is that the parsed result was discarded.

## Acceptance criteria

- `machine create --from artifact.smolmachine --mount-socket H:G` persists the
  mapping.
- `machine create --from artifact.smolmachine --expose-socket G:H` persists the
  mapping.
- omitted flags still produce no published sockets.
- normal `--image` creation is unchanged.
- socket paths come only from the local CLI invocation, never from the artifact.

## Explicit non-goals

Do not include any of these in this PR:

- changes to `machine fork`;
- socket mutation in `machine update`;
- new socket relay code;
- pack manifest format changes;
- egress-policy changes;
- pack extraction caching;
- preloop golden reuse;
- cold-start optimization;
- unrelated cleanup or refactoring.

The existing relay and persistence pipeline already works. The packed branch
only needs to stop throwing away its input.

## Upstream issue draft

### Title

`machine create --from` silently ignores `--mount-socket` and `--expose-socket`

### Body

```markdown
## Summary

`smolvm machine create --from <artifact.smolmachine>` accepts
`--mount-socket` and `--expose-socket`, creates the machine successfully, but
silently discards both options. The failure appears only when a process tries to
connect.

## Affected versions

- Reproduced downstream with SmolVM 1.6.13 on macOS/arm64.
- Still present at commit
  `a31810ebdaed21336562c1b956f33817490bd357`.

## Cause

Normal creation in `src/cli/machine.rs` parses the flags:

```rust
params.published_sockets =
    parse_published_sockets(&self.expose_socket, &self.mount_socket)?;
```

`CreateCmd::run_from_smolmachine` instead constructs:

```rust
published_sockets: Vec::new(),
```

The same packed branch is used for local `--from` artifacts and pack references
resolved from a registry.

## Expected behavior

Socket mappings explicitly supplied by the local user should be validated and
persisted for packed creation exactly as they are for normal image creation.
The portable artifact itself must not supply host paths.

## Suggested fix

Reuse the existing parser in `run_from_smolmachine` and pass its result into
`CreateVmParams`:

```rust
let published_sockets =
    parse_published_sockets(&self.expose_socket, &self.mount_socket)?;

let params = vm_common::CreateVmParams {
    // ...
    published_sockets,
    // ...
};
```

## Regression test

Create from the smallest existing pack fixture with one mount and one expose
mapping, then assert that the resulting VM record contains both
`PublishedSocketConfig` entries with their exact directions and paths. Also
assert that omitting both flags leaves the list empty.
```

## Separate cold-start note

Do not mix cold-start work into this upstream patch.

The socket fix makes packed machines correct, but local measurements showed that
first `create --from` extraction remained expensive: 18.1 seconds for a small
pack. An already extracted VM started in 0.28 seconds. Pack extraction caching
and golden reuse therefore belong in separate issues with separate benchmarks.
