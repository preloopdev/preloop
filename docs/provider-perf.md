# CI Provider Performance Techniques

How companies like Blacksmith, Depot, WarpBuild, and Namespace deliver faster CI
than GitHub-hosted runners — and how each technique maps to aksh.

---

## The Core Insight: Environment Variable Injection

The official runner reads a fixed set of environment variables from the job
message to know where to send cache writes, artifact uploads, log streams, and
tool lookups. Every variable is injected by the control plane server before the
job starts. The workflow YAML never sees them. Actions running inside the job
treat them as opaque URLs and tokens.

Whoever controls the server controls these variables. That is the entire
business model.

---

## Variables and What Providers Do With Them

| Variable | Injected from | Provider technique |
|---|---|---|
| `ACTIONS_CACHE_URL` | `SystemVssConnection.data.CacheServerUrl` | Replace with local cache server |
| `ACTIONS_RESULTS_URL` | `SystemVssConnection.data.ResultsServiceUrl` | Replace with local artifact server |
| `ACTIONS_RUNTIME_URL` | `SystemVssConnection.url` | Replace with local results/log server |
| `RUNNER_TOOL_CACHE` | Runner host directory | Pre-populate with common runtimes |
| `RUNNER_TEMP` | Runner host temp dir | Mount tmpfs/ramdisk |
| `GITHUB_WORKSPACE` | Runner working directory | Mount fast NVMe |
| `GITHUB_SERVER_URL` | `github.server_url` context | Local Git mirror (public repos only) |
| `GITHUB_API_URL` | `github.api_url` context | Local API proxy |
| `ACTIONS_RUNNER_HOOK_JOB_STARTED` | OS environment on runner host | Pre-warm workspace, snapshot VM |
| `ACTIONS_RUNNER_HOOK_JOB_COMPLETED` | OS environment on runner host | Cleanup, metrics, snapshot |

aksh currently sets all of these correctly **except**
`ACTIONS_RUNNER_HOOK_JOB_STARTED` and `ACTIONS_RUNNER_HOOK_JOB_COMPLETED`,
which the official runner reads from the OS environment but aksh does not
implement.

---

## Why Cache is Faster: Geography, Not GitHub

GitHub's cache backend (Azure Blob Storage) is genuinely fast when the runner
is co-located with it. GitHub-hosted runners run in the same Azure region as the
cache server, so cache restores are sub-datacenter hops.

The performance gap appears when you run self-hosted runners. The runner is in
your AWS/GCP/on-prem infrastructure but `ACTIONS_CACHE_URL` still points to
Azure. Every cache read and write crosses the public internet.

### Latency comparison for a 500MB cache restore

| Setup | Latency | Throughput | Total time |
|---|---|---|---|
| GitHub-hosted runner → GitHub cache | <1ms | ~10 Gbps (internal) | ~2-4s |
| Self-hosted in AWS → GitHub cache | 40-80ms | ~100-500 Mbps (internet) | ~15-40s |
| Self-hosted + local cache server | <1ms | ~10 Gbps (LAN) | ~1-2s |

Providers collapse the network distance to near zero by running the runner and
the cache server in the same physical location.

### Cache size limits

GitHub caps cache storage at 10GB per repository with a 7-day TTL. Exceeding
this causes automatic eviction of older caches, reducing hit rates. Custom
providers offer hundreds of GB with configurable TTLs.

---

## The Git Mirror Technique (and Why Most Orgs Skip It)

Pointing `GITHUB_SERVER_URL`, `GITHUB_API_URL`, and `GITHUB_GRAPHQL_URL` at a
local Git mirror makes `actions/checkout` clone from LAN instead of GitHub.
The action constructs its clone URL as:

```
git clone https://x-access-token:${GITHUB_TOKEN}@${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}.git
```

**The catch:** to serve clones from a local mirror, you must pull the source
code onto the provider's infrastructure first. For private repos this requires
a GitHub App token or deploy key with read access. Most security-conscious
organizations are not comfortable with their source code living on a third-party
provider's disks, even briefly.

Providers handle this in one of three ways:

1. **In-VPC runners (Blacksmith, WarpBuild):** VMs run inside the customer's
   own cloud account. Code never touches provider infrastructure. Clone goes
   directly from GitHub into a VM the customer owns.
2. **Cache-only (Depot):** Only opaque content-addressed cache blobs live on
   provider infrastructure, not source code. Cache keys are hashes so the
   provider sees no plaintext content.
3. **Self-managed (ARC, Preloop):** Customer runs the control plane and runners
   entirely inside their own infrastructure. Nothing leaves their network.

The git mirror speedup is effectively off the table for private repos at most
enterprise customers.

---

## `RUNNER_TOOL_CACHE`: The Highest ROI Technique

Every `actions/setup-*` action (setup-node, setup-python, setup-go, setup-java)
checks `RUNNER_TOOL_CACHE` first. If the requested tool version is already
present, it skips the download entirely and adds the existing directory to
`$PATH`. This takes ~50ms. A cold download takes 10-60 seconds depending on
the tool and version.

Providers pre-populate `RUNNER_TOOL_CACHE` with every common tool version on
their VM images before the job starts. The directory ships baked into the disk
image so no download happens at all. This is the single highest-ROI optimization
available because it requires no server-side protocol changes and is completely
transparent to the workflow.

---

## Job Hooks: The Missing aksh Feature

The official runner reads two paths from the OS environment of the runner host
before starting any job:

```
ACTIONS_RUNNER_HOOK_JOB_STARTED=/path/to/script.sh
ACTIONS_RUNNER_HOOK_JOB_COMPLETED=/path/to/script.sh
```

It executes these scripts before and after every job, at the runner level.
The workflow YAML never sees them. Providers use them for:

- **Pre-job:** Clone the repo into the workspace so `actions/checkout` becomes
  a local `git pull` instead of a full network clone.
- **Pre-job:** Take a VM snapshot so failed jobs can roll back without
  re-provisioning.
- **Pre-job:** Pre-pull Docker images the job will need.
- **Post-job:** Clean up orphaned containers, export metrics, write billing
  records.

aksh does not implement these hooks. The official runner reads them in
`JobExtension.cs`:

```csharp
var startedHookPath = Environment.GetEnvironmentVariable("ACTIONS_RUNNER_HOOK_JOB_STARTED");
if (!string.IsNullOrEmpty(startedHookPath))
{
    var hookProvider = HostContext.GetService<IJobHookProvider>();
    var jobHookData = new JobHookData(ActionRunStage.Pre, startedHookPath);
    preJobSteps.Add(new JobExtensionRunner(...));
}
```

This is a concrete compatibility gap. Any workflow that depends on these hooks
(e.g. a repo that has set these env vars on its self-hosted runner host to
pre-warm the workspace) will silently skip the hooks when running on aksh.

---

## What aksh Already Does

aksh correctly replaces all service URLs in the job message before the runner
sees them. In `broker_acquire_job` (`crates/aksh-runner-server/src/lib.rs`):

```rust
endpoint.data.insert("CacheServerUrl".to_owned(), public_base_url());
endpoint.data.insert("ResultsServiceUrl".to_owned(), public_base_url());
endpoint.data.insert("FeedStreamUrl".to_owned(),
    format!("{}/ws/live-logs/{}", websocket_base_url(), message.job_id));
```

So from day one, aksh redirects cache, artifact, and live-log traffic to itself
rather than GitHub's Azure backend. This is the same mechanism every commercial
CI provider uses.
