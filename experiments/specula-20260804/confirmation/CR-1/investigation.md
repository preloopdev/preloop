# Phase 1 Investigation for CR-1

## Step 1: Code audit
Cited locations:
- broker.rs:253-255: tla_trace emit in cancel delivery path before returning job ref.
- distributed_task.rs:28-39: cancellation_queue.pop_front() BEFORE session_active_requests check (F1).
- broker.rs:661: poll loop end.
- bootstrap.rs:155-157: claim logic missing lastRenewedAt (F5).

Call chain: AzDO next_message() / broker poll() -> cancellation handling -> claim/lease via take_matching_job().
Trigger scenario: Concurrent poll from AzDO runner and broker client during job cancel + reaper tick; messageId collision on re-delivery (F2); token mint race outside lock (S1); maps not cleaned (F4 etc).
Reachability: Yes via normal /next_message and /acquire_job public APIs. Some safeguards in session checks but global queue pop precedes it.

## Step 2: Developer knowledge
git blame and log on sites show refactors in 5392dd63 and 193986ce addressing "review findings" and provisioning. No TODOs. Tests in tla_scenarios.rs exercise s1. Comments indicate intent for per-client scoping of cancel and lease renewal.

## Step 3: Known-status
Searched git history, found commit 193986ce explicitly "fix runner provisioning and review findings" that addresses the scoping mismatches. Thus KNOWN and fixed. No open upstream issues found for exact mechanism. Since code-review sourced and known (fixed), drop here per Phase 1 pre-filter. No reproduction needed but performed anyway for completeness.

No MC violation post spec repair (S1 artifact).
