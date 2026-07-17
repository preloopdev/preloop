# Plan 010: Match the official OIDC discovery, JWKS path, and JWT header shape

> **Executor instructions**: Follow step by step; verify each step; STOP on a stop
> condition; update `plans/README.md` when done.
>
> **Drift check (run first)**: `git diff --stat 3505476..HEAD -- crates/aksh-runner-server/src/oidc.rs crates/aksh-runner-server/src/lib.rs`
> Compare "Current state" excerpts before proceeding; mismatch = STOP.

## Status

- **Priority**: P3
- **Effort**: S
- **Risk**: MED (changing `kid`/key material can invalidate cached verifier keys)
- **Depends on**: none
- **Category**: protocol
- **Planned at**: commit `3505476`, 2026-07-16

## Why this matters

aksh mints RS256 id-tokens with correct claims (verified clean by audit), but the
discovery/JWKS/header *surface* diverges from the official runner, so strict OIDC
verifiers can fail discovery or key selection:

1. **JWKS path**: aksh serves `/.well-known/jwks.json` (and `/oidc/.well-known/jwks.json`)
   but not the official `/.well-known/jwks`; discovery advertises the `.json` URI.
   Official discovery advertises `jwks_uri = .../.well-known/jwks` and serves that path
   (`Runner.Server/Controllers/OidcController.cs:19-39`).
2. **Discovery metadata**: aksh advertises `subject_types_supported: ["public"]` and
   omits `scopes_supported`. Official advertises `subject_types_supported:
   ["public","pairwise"]` and `scopes_supported: ["openid"]`.
3. **JWT header**: aksh's header is `{alg, typ, kid}` with `kid` = RFC 7638 thumbprint.
   The official captured token header (`.runner-watch/golden/v2.335.1/15-oidc-id-token/flows.jsonl:23`)
   is `{alg: RS256, kid: <uuid-style>, typ: JWT, x5t: <thumbprint>}` — it carries an
   `x5t` and a server-assigned `kid`.

This is P3 because a lenient verifier that just fetches JWKS and validates RS256 works
today; it matters for consumers that follow discovery strictly or select keys by `x5t`.

## Current state

- `crates/aksh-runner-server/src/oidc.rs:78-85` — `sign_jwt` header is `{alg, typ, kid}`.
- `crates/aksh-runner-server/src/oidc.rs:88-95` — `compute_kid` = RFC 7638 thumbprint.
- `crates/aksh-runner-server/src/oidc.rs:62-75` — `jwks()` emits one key `{kty,kid,alg,use,n,e}`.
- `crates/aksh-runner-server/src/oidc.rs:102-125` — `discovery_document`:
  `subject_types_supported: ["public"]`, no `scopes_supported`.
- `crates/aksh-runner-server/src/lib.rs:584-592` — routes `/.well-known/jwks.json` and
  `/oidc/.well-known/jwks.json` (search: `well-known/jwks`).

Official reference: `Runner.Server/Controllers/OidcController.cs:19-39`;
golden header at `.runner-watch/golden/v2.335.1/15-oidc-id-token/flows.jsonl:23`.

## Commands you will need

| Purpose | Command | Expected |
|---------|---------|----------|
| Check | `cargo check -p aksh-runner-server` | exit 0 |
| Tests | `cargo test -p aksh-runner-server --quiet oidc` | all pass |
| Clippy | `cargo clippy -p aksh-runner-server --all-targets -- -D warnings` | exit 0 |

## Scope

**In scope**: `crates/aksh-runner-server/src/oidc.rs`, the JWKS/discovery routes in
`lib.rs`, and the OIDC tests (inline + `scripts/oidc-conformance*.sh` fixtures if they
assert paths).

**Out of scope**: claim construction/semantics (audited clean — do NOT change `sub`,
`aud`, `nbf`/`exp` window, or claim families), RS256 signing in `crypto.rs`.

## Steps

### Step 1: Serve `/.well-known/jwks` and advertise it

Add routes for `/.well-known/jwks` and `/oidc/.well-known/jwks` pointing at the same
handler as the `.json` variants (keep the `.json` aliases for backward compat). Change
`discovery_document`'s `jwks_uri` argument (at the call site in `lib.rs`) to the
non-`.json` path.

**Verify**: `curl`-style test through the router asserting `GET /.well-known/jwks`
returns the JWKS document; add to the oidc test module.

### Step 2: Complete discovery metadata

In `discovery_document`, set `subject_types_supported: ["public","pairwise"]` and add
`scopes_supported: ["openid"]`, matching `OidcController.cs:19-39`.

**Verify**: oidc discovery test asserts both fields.

### Step 3: Add `x5t` to the JWT header and JWKS

The official `x5t` is the base64url SHA-1 thumbprint of the signing certificate. aksh
has no X.509 cert (it uses a raw RSA keypair), so the faithful analog is the base64url
SHA-1 of the key's DER `SubjectPublicKeyInfo` (or reuse the same thumbprint basis as
`kid`). Add `x5t` to both the JWT header (`sign_jwt`) and the JWKS key entry (`x5t`),
so verifiers selecting by `x5t` find the key. Keep `kid` present and consistent between
header and JWKS. Decide with the maintainer whether to switch `kid` to a stable
server-assigned id (official style) or keep the thumbprint `kid` — either works as long
as header `kid`/`x5t` match the JWKS entry (see STOP conditions).

**Verify**: a test decodes a freshly minted token's header and asserts `alg`,`typ`,`kid`,
`x5t` are present and that `kid`/`x5t` equal the JWKS entry's values.

## Test plan

- oidc tests: `GET /.well-known/jwks` (no `.json`) returns keys; discovery advertises
  that URI, `subject_types_supported` includes `pairwise`, `scopes_supported` = `["openid"]`;
  minted token header has `x5t` and matching `kid`. Model after existing oidc tests in
  `oidc.rs` and the `scripts/oidc-conformance.sh` flow.
- Verification: `cargo test -p aksh-runner-server --quiet oidc` → all pass.

## Done criteria

- [ ] `cargo check -p aksh-runner-server` exits 0
- [ ] `cargo clippy -p aksh-runner-server --all-targets -- -D warnings` exits 0
- [ ] `GET /.well-known/jwks` and `/oidc/.well-known/jwks` both return the JWKS document
- [ ] Discovery advertises the non-`.json` `jwks_uri`, `["public","pairwise"]`, and `["openid"]`
- [ ] Minted token header carries `x5t`; `kid`/`x5t` match the JWKS entry
- [ ] `plans/README.md` row updated

## STOP conditions

- If changing `kid` to a server-assigned id would invalidate tokens already trusted by
  a running verifier during rollover, keep the thumbprint `kid` and only ADD `x5t` —
  do not silently rotate key identifiers.
- If the golden `x5t` is genuinely a cert thumbprint aksh cannot reproduce without an
  X.509 cert, document that `x5t` is a public-key thumbprint analog and stop before
  inventing a certificate.

## Maintenance notes

- Key rotation strategy should keep JWKS and header `kid`/`x5t` in lockstep; a reviewer
  should confirm a token minted after rotation still validates against the served JWKS.
