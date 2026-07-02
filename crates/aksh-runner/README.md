# aksh-runner — Rust GitHub Actions Runner

`aksh-runner` is a native Rust reimplementation of the client-side execution component (`Runner.Listener` + `Runner.Worker`) of the official GitHub Actions runner.

---

## Why Rewrite? (Rust vs. C#/.NET)

The official runner is written in C# and runs on the .NET Runtime. Reimplementing the runner in Rust compiles it directly to native machine code, providing significant performance and operational benefits:

### 1. Instant Startup (Cold Start)
- **Rust**: Starts in **$<5\text{ ms}$**.
- **C#**: Takes **$\sim 200\text{ ms}$** to boot up the bundled Common Language Runtime (CLR) Virtual Machine and Just-In-Time (JIT) compile assemblies.
- **Benefit**: Essential for on-demand execution inside ephemeral containers or microVMs (like Preloop), where start-up latency blocks task dispatch.

### 2. Zero-VM Memory Footprint
- **Rust**: Runs in **$<10\text{ MiB}$** of RAM because there is no VM engine, JIT compiler cache, or Garbage Collector (GC) heap.
- **C#**: Requires **$30\text{--}80\text{ MiB}$** of RAM just to idle.
- **Benefit**: Multiplies runner density on a single host. You can run 5x to 10x more parallel Rust runners on the same memory footprint.

### 3. Single-Binary Deployment
- **Rust**: Distributed as a single compiled executable (~5.6 MiB).
- **C#**: Distributed as a self-contained directory (~400 MiB) containing the execution shims, assemblies, over 150 system DLLs, and bundled runtimes.
- **Benefit**: Simplifies packaging, mounting, and updating runners in containerized or air-gapped environments.

### 4. Deterministic Execution
- **Rust**: Memory reclamation is compiled-in using ownership rules; no Garbage Collector background threads or execution pauses.
- **Benefit**: Eliminates CPU spikes and latency jitter during time-critical tasks like log uploads, timeline updates, or process synchronization.
