# Capture format (`flows.jsonl`, `capture_format: 1`)

One JSON object per line, one line per HTTP exchange. Produced by
`addons/capture.py` (mitmproxy) and `bin/pull-app-deliveries.py` (GitHub App
delivery history); consumed by `runner-watch conform`, `runner-watch
flows-diff`, and external projects (e.g. a GitHub emulator's contract tests).

Additions are backwards compatible; renaming or removing a field bumps
`capture_format`. Records written before `capture_format` existed are v1
without the `capture_format`/`direction` keys.

| Field | Type | Meaning |
|---|---|---|
| `capture_format` | int | Layout version (1) |
| `direction` | `"outbound"` \| `"inbound"` | Outbound: the captured client called the service. Inbound: the service called the captured receiver (webhooks). Set by `MITM_CAPTURE_DIRECTION` |
| `flow_index` | int | 1-based order within the capture |
| `ts_request`, `ts_response` | float epoch \| ISO string \| null | Timing (deliveries carry GitHub's `delivered_at`) |
| `duration_ms` | float \| null | |
| `method`, `scheme`, `host`, `path` | string | `path` includes the query string |
| `request_headers`, `response_headers` | `[[name, value], ...]` | Redacted |
| `request_body_b64`, `response_body_b64` | string | Redacted bytes, base64 |
| `request_body_json`, `response_body_json` | any \| null | Parsed JSON when the content type is JSON, redacted |
| `request_body_sha256`, `response_body_sha256` | string | Of the redacted bytes (addon only) |
| `status` | int \| null | null when the exchange failed before a response |
| `github_delivery` | object | Delivery-history records only: `id`, `guid`, `event`, `action`, `redelivery`, `status`, `delivered_at`, `installation_id`, `repository_id` |

Bodies over 256 KiB or non-UTF-8 are also written beside the file as
`flow.<index>.req.bin` / `flow.<index>.resp.bin`.

## Capture knobs (addon)

| Env | Default | Effect |
|---|---|---|
| `MITM_CAPTURE_DIR` | unset (capture off) | Output directory |
| `MITM_CAPTURE_HOSTS` | all | Comma-separated host allowlist; `.github.com` matches the domain and subdomains |
| `MITM_CAPTURE_DIRECTION` | `outbound` | Stamped on every record |
| `MITM_REWRITE_LOCAL` | `1` | Redirect `localhost`/`*.local` to `BACKEND_PORT` (runner captures). Set `0` for engine-side or reverse-proxy captures |

## Recipes

Engine → GitHub (outbound), e.g. preloop's App/check-run traffic:

```sh
MITM_CAPTURE_DIR=cap/engine MITM_CAPTURE_HOSTS=.github.com,.githubusercontent.com \
MITM_REWRITE_LOCAL=0 mitmdump -p 8888 -s addons/capture.py
# engine: HTTPS_PROXY=http://127.0.0.1:8888, mitm CA in the system trust store;
# git: GIT_SSL_CAINFO=~/.mitmproxy/mitmproxy-ca-cert.pem
```

GitHub → engine webhooks (inbound), live:

```sh
MITM_CAPTURE_DIR=cap/hooks MITM_CAPTURE_DIRECTION=inbound MITM_REWRITE_LOCAL=0 \
mitmdump --mode reverse:http://127.0.0.1:9090 -p 9091 -s addons/capture.py
```

GitHub → engine webhooks, from GitHub's own history (no proxy):

```sh
bin/pull-app-deliveries.py --app-id "$APP_ID" --pem app.pem --out cap/deliveries
```

Carve a service's traffic out of existing captures:

```sh
bin/extract-hosts.py --hosts api.github.com,github.com,codeload.github.com \
  --out corpus/ ../../.runner-watch/golden/v2.336.0
```

Gate a candidate (e.g. an emulator replay) against a reference:

```sh
runner-watch flows-diff --left corpus/push --right replay/push --schema-gate '*' --json
```
