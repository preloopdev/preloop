# Terminal UI

`preloop tui` is a ratatui client for the engine and Sandbox Service. It is a
client, not a second control plane: engine mutations use the existing native
bearer API and sandbox operations use the existing Sandbox Service bearer/CA
configuration.

## Start

```sh
preloop tui
preloop tui --context cpane
preloop tui --run <run-id>
preloop tui --read-only
```

The default context resolves `PRELOOP_URL` and engine credentials with the same
precedence as the other native CLI commands. Sandbox credentials use
`PRELOOP_SANDBOX_URL`, `PRELOOP_SANDBOX_TOKEN`, and `PRELOOP_SANDBOX_CA`, then
local files under `$PRELOOP_HOME/state` only for the local loopback service.
Remote contexts must provide their own token and CA paths in
`$PRELOOP_HOME/tui.toml`.

## Contexts and stack configuration

```toml
[[context]]
name = "cpane"
engine_url = "https://cpane.example:9090"
engine_token_file = "/path/to/cpane-token"
sandbox_url = "https://cpane.example:9191"
sandbox_token_file = "/path/to/sandbox-token"
sandbox_ca_file = "/path/to/sandbox-ca.pem"

[stack]
repos = ["preloopdev/preloop", "preloopdev/plp-priv"]
author = "Bnjoroge"
extra_edges = [["preloopdev/plp-priv:sandbox-service-base", "preloopdev/preloop:#619"]]
```

The stack view calls `gh api graphql` using the user's existing GitHub CLI
session. It does not store GitHub tokens in `tui.toml`.

## Views

- **Overview**: queue, pool, VM, webhook and detector summaries.
- **Runs**: run → job → step → log drill-down. `events.ndjson` applies
  `job_status` and `step_status` updates live; `live-log` SSE lines are routed
  by the step GUID and the selected step's elapsed time ticks while running.
- **Fleet**, **Debug**, **Webhooks**, **Sandboxes**, and **Stack**: operational
  views with actions gated by confirmation and `--read-only`.
- **Setup**: press `s` or choose `setup wizard` from `:`. The real `preloop
  init` flow runs through the TUI prompt bridge; it does not duplicate setup
  policy or write configuration before the wizard completes.

`q` exits, `:` opens the command palette, `?` shows help, and `C` switches
contexts. Destructive actions require a confirmation modal; sandbox releases
require the sandbox id.

## API limitations

The TUI polls Sandbox Service inventory because the current Sandbox contract
has no caller-facing `Watch` RPC. It joins runner and sandbox names where the
pool provides the same machine name; lease-scoped actions remain disabled for
redacted inventory rows unless the TUI owns the lease. Engine pool bindings are
available through `GET /api/v1/pool/bindings`.
