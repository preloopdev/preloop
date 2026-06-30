## Runner sync: actions/runner v2.335.1

### Changes (low tier)

| ID | Category | Description | Spec |
|---|---|---|---|
| disable-stdout-multiline-log-prefixing | nit | Runner reads an env var controlling multiline stdout log prefixing. | .runner-watch/specs/v2.335.1/disable-stdout-multiline-log-prefixing.toml |
| node20-deprecation-warning | nit | Runner emits Node 20 deprecation warning annotations for affected JavaScript actions. | .runner-watch/specs/v2.335.1/node20-deprecation-warning.toml |
| server-enforced-runner-settings | nit | Server can enforce selected runner settings. | .runner-watch/specs/v2.335.1/server-enforced-runner-settings.toml |

### Conformance
See `.runner-watch/conformance-report.md`.

### Review log
See `.runner-watch/review.toml`.

### Upstream references
Generated from deterministic source diff artifacts in `.runner-watch/delta.json`.