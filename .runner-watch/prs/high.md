## Runner sync: actions/runner v2.335.1

### Changes (high tier)

| ID | Category | Description | Spec |
|---|---|---|---|
| batch-action-resolution | feature | Runner can resolve action downloads in batches and optionally use bearer tokens for codeload. | .runner-watch/specs/v2.335.1/batch-action-resolution.toml |
| dap-debugger-endpoint | feature | Runner can expose a DAP debugger integration. | .runner-watch/specs/v2.335.1/dap-debugger-endpoint.toml |
| request-ack | concern | Runner sends an explicit acknowledgement after receiving a job request. | .runner-watch/specs/v2.335.1/request-ack.toml |
| runner-version-deprecated | concern | Server can tell the runner its version is deprecated. | .runner-watch/specs/v2.335.1/runner-version-deprecated.toml |
| send-job-level-annotations | feature | Runner can send job-level annotations in timeline updates. | .runner-watch/specs/v2.335.1/send-job-level-annotations.toml |
| use-bearer-token-for-codeload | feature | Runner can resolve action downloads in batches and optionally use bearer tokens for codeload. | .runner-watch/specs/v2.335.1/use-bearer-token-for-codeload.toml |
| use-runner-admin-flow | concern | Runner v2 admin flow discovers auth_url_v2 and BrokerUrl values. | .runner-watch/specs/v2.335.1/use-runner-admin-flow.toml |
| v2-admin-broker-connection | concern | Runner v2 admin flow discovers auth_url_v2 and BrokerUrl values. | .runner-watch/specs/v2.335.1/v2-admin-broker-connection.toml |

### Conformance
See `.runner-watch/conformance-report.md`.

### Review log
See `.runner-watch/review.toml`.

### Upstream references
Generated from deterministic source diff artifacts in `.runner-watch/delta.json`.