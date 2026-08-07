# Phase 1 Investigation for CR-2

## Step 1: Code audit
Cited locations:
- crates/preloop-runner/src/listener/broker_listener.rs:623-630 (spawn_job on RunnerJobRequest overlap)
- crates/preloop-runner/src/listener/job_dispatcher.rs:297-303 (kill() only does child.kill(), no PG in some paths)
- crates/preloop-runner/src/listener/broker_listener.rs:493-518 (dedup/parse/ack ordering)
- crates/preloop-runner/src/process.rs:104 (group_spawn)

Call chain: public broker listener loop -> classify_message -> RunnerJobRequest overlap cancel (broker_listener:586) or RunnerShutdown (737: return Ok(()) bypassing active_job.shutdown_gracefully) -> job_dispatcher::cancel/shutdown_gracefully/kill -> process::invoke group_spawn or direct kill.

Trigger scenario: 1. Start runner with active long-running job that spawns background step group (setsid or double-fork). 2. Send RunnerShutdown or overlap new-job or timeout. 3. Listener returns Ok on shutdown without worker graceful, kill only hits worker PID not separate step PGs; dedup before full parse can leave unacked; different clocks (server vs tokio Instant). Reachable via normal broker messages / ctrl-c / lease loss. Safeguards: job_extension kill_orphan_processes (tracking_id), but not always called on all paths; stream drain grace in process.rs.

## Step 2: Developer-knowledge search
- git log shows commit f8c217dc adding "force-fail crashed workers, graceful shutdown" — addresses some but not all sequencing (overlap vs RunnerShutdown vs listener kill).
- Tests in process.rs:425 (cancel sigint), job_dispatcher.rs:898 (shutdown_gracefully), job_extension for orphans.
- Comments note historical orphan-process cleanup mirroring JobExtension.cs FinalizeJob, and "escaped the process group while retaining a pipe".
- No TODOs explicitly calling out the 3-source mismatch or S4 gap.
- No direct developer comments on the exact sequencing gaps in cited lines.

## Step 3: Known-status / precedent
Searched git history, commits mentioning orphan/cancel/kill/shutdown (f8c217dc and earlier). No exact match for "cancellation to process-tree kill sequencing gaps" or the specific 3 mismatched treatments + dedup-before-parse in upstream issues or closed PRs (no remote but local log clean). No prior Specula dataset match for this mechanism. Thus NEW. Not code-review × already-reported, so proceed to Phase 2 reproduction.

(Recorded facts only; no verdict here.)
