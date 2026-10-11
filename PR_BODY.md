# PR-K: Transform-aware masking, layered (round-3 §9)

## Finding

Round-3 report §9, 2 of 3 reproduced against `879329a0`:

1. `::add-mask::pentest-mask-9f3k2`, then echoing the secret raw and base64-encoded:
   the stored log showed `raw=***` but the **base64 form in plaintext** — one
   `base64 -d` recovers it. Root cause: `JobContext::new` registered base64
   variants for *initial* secrets, but `add_mask` registered only the raw value
   (plus trimmed lines); percent/hex/case variants were never masked anywhere.
2. Proxy credentials raw in setup logs: `steps_runner.rs` printed the proxy URL
   verbatim **with userinfo** — any deployment with an authenticated egress
   proxy leaks it to every log reader.

(The third sub-item — annotation title/file — was not reproduced end-to-end;
residual risk only, out of scope.)

## Fix, in four layers

1. **Transform-aware masking** (`crates/preloop-gha-protocol/src/masking.rs`):
   new `secret_variants(secret)` generates base64 (standard + URL-safe, padded
   and unpadded), percent-encoding, hex (lower + upper); new canonical
   `mask_secrets_transform_aware` / `mask_secrets_preserving_lines_transform_aware`
   expand variants and match ASCII case-insensitively. The exact `mask_secrets`
   is kept for the DAP transport path that needs byte-exact behavior.
   All runner log paths (`contexts.rs`, `live_logs.rs`) and `mask_annotations`
   now use the transform-aware functions.
2. **Server-side pass before persistence**: every bearerless signed-URL
   `PUT /replay/results/*` (step logs, job logs, summaries) is re-masked
   transform-aware before bytes hit disk (`blob_store.rs`); fails closed —
   500 and the upload is dropped if secret resolution fails. Also wired into
   the AzDO `append_log` and `console_log` live-wrapper ingestion (fail-open
   there: transient feed, and the runner already masks).
3. **Never emit credentials**: new dependency-free `strip_url_userinfo(url)`;
   the proxy setup line now prints the host without userinfo (extracted as
   `proxy_setup_line` in `steps_runner.rs`, "don't print, not mask after
   printing"). Same treatment audited onto the broker-migration URL log, the
   action-tarball download URL, and live-log feed URL warnings. Reviewed and
   deliberately left: node-externals URLs (content-addressed, no creds),
   completion-report URLs, server `redact_store_url`, OTLP debug output.
4. **Chunk boundaries**: new `StreamingMasker` — a trailing-overlap buffer
   (`push(chunk)` masks the reassembled window and withholds possible secret
   heads; `finish()` flushes the tail). Wired into `append_log` per log key
   behind a per-key mutex; flushed on `finish_job`/`finish_job_plan` and
   dropped on memory-pressure eviction.

## Regression tests

- `preloop-gha-protocol` (13 new unit tests): variant generation (b64 std/urlsafe
  ±pad, percent, hex lower/upper, dedupe, case-insensitive), `StreamingMasker`
  split-across-chunks at every split offset, `strip_url_userinfo`.
- `preloop-runner` (4 new): `::add-mask::` then base64/upper echo → all masked;
  proxy setup line strips userinfo; annotations masked transform-aware.
- `preloop-runner-server` integration
  (`replay_step_log_upload_is_masked_before_persist`): submit a run carrying a
  secret, `PUT` **unmasked** bytes (raw + base64) to a minted signed step-log
  URL through the real router → asserts the persisted file is
  `raw=*** b64=***`.

## Repro verification (live server, 2026-10-11)

Patched server on `127.0.0.1:19111` (state dir under the worktree,
`preloop store migrate` first, `/readyz` 200):

1. Submitted a run with secret `MASK_ME=pentest-mask-9f3k2` via
   `POST /api/v1/runs` → run `aa3fce07-b73c-4196-b7cb-03c81d67a8bd`.
2. Minted a signed upload ticket (HMAC-SHA256 over `replay-blob\n<exp>\n<path>`
   with the server's key) and `PUT` the bytes
   `raw=pentest-mask-9f3k2 b64=cGVudGVzdC1tYXNrLTlmM2sy` — **unmasked**, as a
   masking-buggy runner would send — to
   `/replay/results/<run>/job-1/step-abc.txt` → HTTP 201.
3. Read the persisted file: **`raw=*** b64=***`** — the server-side pass
   caught both the raw and base64 forms before they hit disk.

Full VM job execution was not possible on this machine (no `/dev/kvm`;
the golden-image download stalled), so the runner half is verified by the
unit/integration tests above rather than a live `::add-mask::` workflow run.
The proxy-userinfo fix is verified by the `proxy_setup_line` regression test
(the setup line is emitted by the in-VM runner; the audit confirmed it is the
only proxy-URL emission point).

## Test results

- `cargo test -p preloop-gha-protocol --lib`: **103 passed**
- runner targeted tests (contexts/live_logs/commands/steps_runner): **32 passed**
- `preloop-runner-server` lib (timeline_logs/memory_caps/blob_store/live_logs): **24 passed**
- new server integration test `replay_step_log_upload_is_masked_before_persist`: **passed**
- `cargo clippy --all-targets` on the three touched crates: **clean**
- `cargo fmt --check`: **clean**

## Notes

- During verification, `cargo check` once reported a clean build on stale cache
  while a later test build surfaced a real E0382 (moved `Arc` in the
  `finish_job` flush calls); fixed by cloning the `Arc`, and all subsequent
  verification was done from clean rebuilds.
- Diag-zip uploads (`/twirp-blob/diag/`) are compressed blobs — no plaintext
  masking is possible server-side; the runner masks `_diag` content at write
  time via the same log pipeline. Out of scope for this pass.
- Withheld streaming tails are flushed at job finish; if `finish_job` never
  arrives (runner crash), at most the bounded overlap tail stays out of the
  lossy in-memory preview. The durable runner-uploaded logs are unaffected.
