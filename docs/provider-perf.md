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

aksh implements all of these, including the job start/completed hooks, which
are injected as synthetic script steps before and after the user steps respectively.

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

## Job Hooks: Supported natively in aksh

The official runner reads two paths from the OS environment of the runner host:

```
ACTIONS_RUNNER_HOOK_JOB_STARTED=/path/to/script.sh
ACTIONS_RUNNER_HOOK_JOB_COMPLETED=/path/to/script.sh
```

aksh implements this natively during step ordering (`crates/aksh-runner/src/worker/job_runner.rs`).
If these variables are present in the runner host OS environment, aksh generates
synthetic script steps and inserts them at the start and end of the step execution queue.

Providers utilize these hooks for:
- **Pre-job:** Cloning the repository into the workspace so `actions/checkout` can just do a local `git pull`.
- **Pre-job:** Instantly snapshotting the VM to enable time-travel rollbacks on failure.
- **Pre-job:** Pre-pulling heavy Docker images specified by steps.
- **Post-job:** Cleaning up orphaned containers, writing telemetry, and calculating billing metrics.
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

---

## Advanced Control-Plane Value-Add Techniques

By controlling the backend scheduler and emulating the Twirp protocols, a managed CI provider can layer advanced features directly on top of the unmodified official runner:

### 1. Network & Dependency Sandboxing (The "Token Firewall")
- **Mechanism:** `aksh` intercepts the `GITHUB_API_URL` environment variable, routing all API calls from the runner through a local proxy. 
- **Feature:** Enforce granular policies (least-privilege tokens). If the runner is compromised by a malicious pull request dependency, `aksh` blocks any calls attempting to write to the repository or delete tags, while still permitting standard check-run status posts.

### 2. Zero-Wait Cache Pre-fetching
- **Mechanism:** Since the control plane knows which job is queued, it knows the expected cache keys before the runner even boots.
- **Feature:** Begin streaming the `.tar.zst` cache archive to the runner host's staging area in the background during VM bootstrap. When the runner evaluates the restore step, the files are already sitting on the local SSD, reducing restore wait times from seconds to milliseconds.

### 3. OIDC Cloud Federation Brokerage
- **Mechanism:** `aksh` serves the OIDC token endpoints (`/.../oidctoken`) and acts as the JWT signing authority.
- **Feature:** Negotiate credentials dynamically with cloud providers (AWS IAM Roles Anywhere, GCP, HashiCorp Vault) based on job metadata, injecting AWS credentials directly into the job context without requiring the developer to configure cloud trust policies with GitHub.

### 4. Interactive Debugging & Time-Travel VM snapshots
- **Mechanism:** Control plane coordinates with the VM/container host during job execution.
- **Feature:** On step failure, `aksh` suspends the run, holds the VM alive, and launches an interactive shell session in the web browser. If running on isolated microVMs, `aksh` can take snapshots at the end of each step, allowing developers to restart execution from any prior step's exact state.

### 5. Dynamic VM Sizing
- **Mechanism:** The server analyzes the CPU and memory history of previous runs.
- **Feature:** If a job historically uses very few resources, the server schedules it on a smaller, cheaper VM. If a job historically runs out of memory, the server automatically upgrades it to a larger VM size, preventing out-of-memory crashes.

### 6. Zero Trust Secret Management
- **Mechanism:** Bypass GitHub's secrets database entirely.
- **Feature:** Integrate directly with a company's private HashiCorp Vault. When a job starts, our server pulls the secret from Vault and injects it into the job payload. The secrets never touch GitHub's cloud, keeping the credentials secure.

### 7. Real-Time Budget Enforcer
- **Mechanism:** Track resource consumption at the queue layer.
- **Feature:** Track CPU minutes and storage costs per developer, per team, or per repository. If a developer triggers a massive, expensive matrix run that exceeds their team's weekly budget, the server can pause the run and request approval from a team lead.

### 8. Smart Code Warning Blamer
- **Mechanism:** Match log parser outputs with git blame metadata.
- **Feature:** When compiler warnings or test errors are outputted, the server parses the warnings, runs a git blame command on the affected file, and notifies the exact developer who introduced the warning.

### 9. Cross-Repository Cache Sharing
- **Mechanism:** Write custom lookup logic in the CacheService Twirp database.
- **Feature:** If repository B requests a cache key that does not exist, the server can check if repository A has a cache with the identical lockfile hash and serve that instead, saving compile time across the whole company.

### 10. Smart Step Skipping
- **Mechanism:** Reuse previous build artifacts.
- **Feature:** If a test fails and the developer fixes it, the server detects that only the test files changed. It dispatches a job that skips compilation, pulls the pre-built artifact from the previous run's database, and starts the runner directly on the test step.

### 11. Pre-fetching Git Dependencies
- **Mechanism:** The server parses the workflow file and identifies dependency lockfiles.
- **Feature:** If the server has seen this lockfile before, the VM host pre-populates the dependency directories (like the Cargo or npm cache folders) directly onto the VM filesystem before dispatching the job.

### 12. Canary VM Image Rollouts
- **Mechanism:** Control job routing dynamically.
- **Feature:** When rolling out a new VM image, route a small percentage of jobs to the new image, monitor the failure rate, and automatically roll back the image update if failures spike, all without the developer knowing.

### 13. Collaborative Terminal Debugging
- **Mechanism:** Host-coordinated WebSocket terminal sessions.
- **Feature:** Multiple developers can log into the same suspended runner VM through their web browsers, share the same terminal screen, and work together to fix the environment.

### 14. Dynamic Matrix Sharding
- **Mechanism:** Split tests based on test count dynamically before dispatching.
- **Feature:** Count the tests in the suite before dispatching. If the test suite has doubled since the last run, dynamically split the tests across 8 runners instead of 4 to keep the total execution time constant.

### 15. Git LFS Network Caching
- **Mechanism:** Local LFS caching proxy on the control plane.
- **Feature:** Intercept LFS file downloads and serve them from the local SSD storage over the LAN, reducing large file download times from minutes to seconds.

---

## Strategic Trade-offs & Napkin Math

If you are building a managed CI service, you face a major architectural fork:

### Option A: Reimplement the GHA Control Plane (`aksh` approach)
*You host the scheduler, broker, and Twirp services. Unmodified runners talk only to you.*
* **Initial CapEx:** High ($800k+ engineering over 1 year).
* **OpEx Maintenance:** High ($400k+/year). You are chasing GitHub's undocumented internal runner API. If GitHub releases a runner version with new required location fields or padding requirements, your customers' builds will fail until you reverse-engineer the change.
* **SLA Risk:** 100% on you. If your database/scheduler crashes, your customers cannot run builds.
* **The Payoff:** **Enterprise/Air-Gapped Market Access.** You can package your control plane and sell it to banks, defense contractors, and healthcare companies that are legally barred from sending code, secrets, or logs to `github.com`. These contracts routinely sell for $100k-$250k/year ARR.

### Option B: Compute-Only Hosting + Caching Proxy (Warp/Depot approach)
*Runners talk directly to github.com. You manage the VM hosting and optimize disk/caching at the VM boundary.*
* **Initial CapEx:** Low ($150k over 3 months).
* **OpEx Maintenance:** Low ($100k/year). GitHub maintains the API contract; your VM agent only listens to standard signals.
* **SLA Risk:** Low. If GitHub Actions has an outage, it is GitHub's fault, not yours.
* **The Payoff:** **Developer SaaS Scale.** High-speed, high-performance compute that is easy to adopt for public-cloud developers.

### Napkin Math: The Egress Bottleneck
Assume a provider running 1,000 active concurrent runner VMs with a 5GB average cache size and 50 builds per day per VM.
Total cache data transferred per month: 

  1,000 VMs * 50 builds/day * 5 GB * 30 days = 7.5 PB/month

* **Without Local Caching (Egress to Azure/S3):** If your compute runs in AWS and your cache is in GitHub's Azure storage, at standard egress rates ($0.05/GB), the network transfer cost would be:

  7.5 PB * 1,000,000 GB/PB * $0.05/GB = $375,000/month

* **With Local Caching:** By routing cache traffic locally (Option A or Option B with a proxy), egress fees are eliminated ($0), saving hundreds of thousands of dollars monthly at scale.
