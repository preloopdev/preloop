# Security Policy and Trust Tiers

## Goal

Preloop must be safe enough to run code produced by autonomous agents and, later, untrusted PRs in self-hosted and managed CI.

Security must be explicit, configurable, and visible in run metadata. It cannot be a hidden collection of defaults.

## Baseline posture

Default for untrusted or agent-generated code:

```text
No host Docker socket.
No SSH-agent forwarding.
No real GITHUB_TOKEN.
No cloud credentials.
No host home mount.
Read-only source.
Bounded writable layers.
Egress allowlist or off.
Action pinning warnings/errors.
Secrets masked before logs reach agents.
Cache writes quarantined or disabled.
```

## Trust tiers

| Tier | Example | Secrets | Network | Cache writes | Source | Docker |
|---|---|---|---|---|---|---|
| `local-dev` | human local repo | opt-in | proxy/allowlist | allowed | live mount | private in-guest |
| `agent-local` | Codex loop | fake by default | allowlist | quarantine | live or snapshot | private restricted |
| `trusted-branch` | main branch CI | scoped | allowlist | write-on-success | commit snapshot | private |
| `internal-pr` | same-org PR | limited | allowlist | quarantine | merge/ref snapshot | private restricted |
| `untrusted-fork-pr` | public fork | none/fake | off or narrow | disabled | merge/ref snapshot | private restricted |
| `deployment` | release job | approved scoped | explicit allowlist | write-on-success | signed/tagged snapshot | private |
| `managed-untrusted` | SaaS tenant code | none/fake unless approved | strict proxy | tenant-isolated/quarantine | snapshot | private restricted |

Every run should carry a trust tier.

## Policy file

Example `.github/preloop.toml`:

```toml
[default]
trust = "agent-local"
final_verification = "strict-clean"

[filesystem]
host_mount = "readonly"
upper = "hybrid"
upper_limit = "8GiB"
scratch_limit = "32GiB"
deny_symlink_escape = true
case_sensitivity_check = true

[network]
mode = "allowlist"
allow = ["github.com", "api.github.com", "registry.npmjs.org"]
block_cloud_metadata = true

[secrets]
mode = "brokered"
deny_plain_env = true
github_token = "fake"
oidc = "disabled"

[actions]
allow_unpinned = false
allow_docker_actions = "warn"
allow_private_actions = "policy"

[cache]
read = true
write = "on-success"
quarantine_failed_attempts = true
```

## Secret model

Secrets should never be dumped wholesale into the VM environment by default.

Preferred model:

- fake/local `GITHUB_TOKEN` by default,
- scoped token only through explicit policy,
- brokered per-request secrets where possible,
- no OIDC unless brokered and policy-approved,
- no cloud provider credentials unless approved for a trusted tier,
- secret values stored in redaction-safe types,
- logs/artifacts/repro bundles masked before external display.

Secret type rule:

```rust
SecretString must not expose raw values through Debug, Display, Serialize, logs, errors, or telemetry.
Raw access requires explicit expose() at a protocol boundary.
```

## Network policy

Network events should be classified:

```json
{
  "type": "preloop.network.denied",
  "job_id": "test",
  "step_id": "npm_install",
  "host": "unknown.example.com",
  "reason": "not_in_allowlist",
  "trust_tier": "agent-local"
}
```

Modes:

| Mode | Use |
|---|---|
| `off` | untrusted fork, deterministic replay |
| `allowlist` | agent default |
| `proxy` | policy, logging, masking |
| `tsi` | fast local mode only if policy can be enforced |
| `virtio-net` | private Docker/services |

Cloud metadata IPs should be blocked by default.

## Action supply-chain policy

Warn or fail on:

- `uses: owner/action@main`,
- mutable tags without pinning,
- third-party actions in untrusted PRs,
- private actions in untrusted PRs,
- Docker actions pulling mutable tags,
- `curl | bash`,
- workflows that request broad token permissions,
- `pull_request_target` with untrusted checkout.

## Cache poisoning controls

Untrusted jobs must not write caches used by trusted jobs.

Controls:

- trust-tier cache namespaces,
- write-on-success policy,
- failed-run quarantine,
- manual promotion where needed,
- lockfile/source digest provenance,
- cache metadata audit trail.

## Threat table

| Threat | Example | Control |
|---|---|---|
| Host secret theft | script reads `~/.ssh` | no home mount, path allowlist, fake tokens |
| Symlink escape | repo symlink points outside root | preflight scanner, mount isolation |
| Network exfiltration | postinstall uploads env | egress allowlist/proxy, no secrets |
| Cache poisoning | failed PR writes malicious dependency cache | trust namespaces, quarantine |
| Disk exhaustion | agent writes huge files | quotas on upper/scratch/cache/logs |
| Host Docker escape | job controls host daemon | no host Docker socket |
| VMM-proxy escape | guest abuses proxied resource | host jail, per-VM UID, seccomp, cgroups |
| Secret log leakage | action echoes token | masking pipeline before log/artifact exposure |

## Security acceptance tests

Build a malicious workflow corpus:

- symlink escape attempt,
- read `~/.ssh` attempt,
- read cloud credential paths,
- echo fake and real-looking secrets,
- network exfiltration to blocked host,
- metadata service request,
- disk fill,
- cache poisoning attempt,
- Docker socket mount attempt,
- privileged container attempt,
- artifact path traversal,
- `pull_request_target` unsafe checkout.

These tests should run before self-hosted beta and managed private beta.
