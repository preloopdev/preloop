--------------------------- MODULE base ---------------------------
(*
 * TLA+ specification for aksh / preloop — GitHub Actions control plane
 * (aksh-runner-server) and runner (aksh-runner Listener/Worker).
 *
 * Category A (distributed / message-passing) with a Category B overlay on
 * the runner side (Scenarios 4-5). Derived from the modeling brief
 * (Scenarios 1-6) and the implementation control flow — not the reference
 * algorithm. Deviations from the reference are where bugs live.
 *
 * Scenarios:
 *   S1  Job claim/lease lifecycle  (broker.rs, distributed_task.rs, bootstrap.rs)
 *   S2  Concurrency-group holder state machine (runtime_scheduling.rs, concurrency.rs)
 *   S3  Deferred matrix/reusable expansion + correlation registration
 *       (runtime_scheduling.rs, runs.rs)
 *   S4  Runner cancellation -> process-tree kill sequencing
 *       (listener/broker_listener.rs, listener/job_dispatcher.rs)
 *   S5  Worker step execution & reporting queue
 *       (worker/server_queue.rs, worker/reporting.rs, worker/completion.rs)
 *   S6  Secret masking & template tokenization
 *       (aksh-gha-protocol/src/masking.rs, azdo/job.rs,
 *        aksh-gha-parser/src/job_builder.rs)
 *
 * Every action is annotated with the implementation source location it
 * models. Paths the brief identifies as buggy at HEAD are encoded exactly
 * as implemented and marked *BUG*.
 *)

EXTENDS Integers, Naturals, Sequences, FiniteSets, TLC

CONSTANT
    Runner,                    \* Set of runner ids (registered runners)
    Session,                   \* Set of broker sessions
    RunId,                     \* Set of workflow run ids
    JobId,                     \* Set of logical job ids (per-run)
    RequestId,                 \* Set of agent request ids (job_requests keys)
    Key,                       \* Set of concurrency group keys
    JobSetId,                  \* Set of reusable-invocation admission ids
    StepId,                    \* Set of step external ids (worker side)
    Str,                       \* Strings (masking / tokenizer)
    NoRun,                     \* sentinel RunId value (absent run)
    NoJob,                     \* sentinel JobId value (absent job)
    NoRunner,                  \* sentinel Runner value (absent runner)
    NoKey,                     \* sentinel Key value (absent gate key)
    MESSAGE_ID_BASE,           \* 1_000_000: root broker cancel id base (broker.rs:165)
    MaxLowMsgs,                \* Bound on low-range message ids (pool/AzDO path)
    MaxHighMsgs,               \* Bound on high-range message ids (root path)
    LeaseSeconds,              \* JOB_LEASE_SECONDS = 2700 (distributed_task.rs:443)
    JobTimeout,                \* default job timeout 21600 (bootstrap.rs:130)
    MaxOutputBytes,            \* 1 MiB job output cap (Scenario 5 F4)
    MaxChunk,                  \* per-flush output chunk bound (Scenario 5 F4)
    RequireAssignments,        \* PRELOOP_REQUIRE_JOB_ASSIGNMENTS (state.rs:708)

    \* --- Job / run status tags (ExecutionStatus) ---
    StQueued, StPending, StInProgress, StSuccess, StFailure, StCancelled, StSkipped,

    \* --- Request states (reqInfo.state) ---
    SNone, SQueued, SClaimed, SAcquiring, SRunning, STerminal,

    \* --- Results (TaskAgentJobRequestRecord.result) ---
    RNone, RSuccess, RFailure, RCancelled, RSkipped,

    \* --- Concurrency holder kinds (concurrency.rs:20-37) ---
    HolderRun, HolderJob, HolderJobSet,

    \* --- Concurrency queue modes (ConcurrencyQueue) ---
    ModeSingle, ModeMax,

    \* --- Shutdown sources (Scenario 4) ---
    NoShutdown, SigShutdown, MsgShutdown, OverlapShutdown,

    \* --- Step kill causes (Scenario 5): none / timeout / cancel / both ---
    NoKill, KillCancel, KillTimeout, KillBoth,

    \* --- Tokenizer scan states (Scenario 6, azdo/job.rs:586-613) ---
    Normal, InString

\* Helper for Range(seq) used throughout (equivalent to image of seq under 1..Len)
Range(seq) == { seq[i] : i \in 1..Len(seq) }

VARIABLES
    \* ============ SERVER: claim/lease lifecycle (Scenario 1) ============
    now,                        \* abstract time; advanced by TimeAdvance
    reqInfo,                    \* [RequestId -> [run, job, state, result,
                                \*   started, renew, deadline]] (job_requests,
                                \*   state.rs:734)
    inflightReq,                \* SUBSET RequestId (inflight_requests, state.rs:733)
    planReq,                    \* [RequestId -> RequestId] plan_requests (state.rs:735)
    agentJobReq,                \* [JobId -> RequestId]  agent_job_requests (state.rs:736)
    timelineReq,                \* [RequestId -> RequestId] timeline_requests (state.rs:737)
    brokerMsg,                  \* [RequestId -> Nat] messageId of delivered job ref
    sessionActive,              \* [Session -> RequestId] session_active_requests (state.rs:738)
    sessionRunner,              \* [Session -> Runner] broker_session_runners (state.rs:740)
    dispatchQueue,              \* Seq(RequestId) dispatch queue (state.rs:658)
    pendingJobs,                \* Seq(JobRef) pending_jobs (state.rs:659)
    cancelQueue,                \* Seq(CancelRec) cancellation_queue (state.rs:689)
    inflightMsgs,               \* [Session -> SUBSET AllMsgIds] inflight_messages
                                \*   (state.rs:684)
    msgIdNext,                  \* Nat low-range next_message_id (state.rs:743)
    msgIdHigh,                  \* Nat root-path cancel id counter (broker.rs:162-171)
    mintInFlight,               \* SUBSET RequestId github_token_requests (state.rs:687)
    ghostJob,                   \* SUBSET RequestId: AcquireEnd returned a payload
                                \*   for a claim already failed/reaped (S1 S1)
    discardedCompletion,        \* SUBSET RequestId: terminal-lock discarded a late
                                \*   runner-reported completion (S1 S2)
    deliveredCancel,            \* Seq([run, job, session, matched, collided])
                                \*   audit of each cancellation pop (S1 F1/F2)

    \* ============ SERVER: runs and concurrency (Scenarios 2-3) ============
    runStatus,                  \* [RunId -> Status | NONE] (RunRecord.status)
    runJobs,                    \* [RunId -> SUBSET JobId]  (RunRecord.jobs keys)
    jobStatus,                  \* [RunId -> [JobId -> Status]] (RunRecord.jobs)
    groups,                     \* [Key -> [running, pending]] concurrency_groups
                                \*   (state.rs:770)
    heldRuns,                   \* [RunId -> SUBSET JobRef] held_runs (state.rs:772)
    blockedJobs,                \* Seq(JobRef) concurrency_blocked (state.rs:774)
    holderKeys,                 \* [RunId -> SUBSET Key] holder_keys (state.rs:783)
    jobsetAdm,                  \* [JobSetId -> [gates, acquired]] jobset_admissions
                                \*   (state.rs:776)
    jobsetReady,                \* SUBSET JobSetId jobset_ready (state.rs:777)
    pendingExp,                 \* Seq(JobRef) pending_expansions (state.rs:669)
    expanding,                  \* SUBSET JobRef expanding reservation (state.rs:675)
    jobAssign,                  \* [RunId x JobId -> Runner] job_assignments
                                \*   (state.rs:694)
    poolPending,                \* SUBSET JobRef pool_pending (state.rs:699)
    hasGate,                    \* [RunId x JobId -> Key | NONE] declared job-level
                                \*   concurrency gate (concurrency_from_plan_fields)
    gateHeld,                   \* SUBSET JobRef: group slot acquired for the job
    cancelFlipped,              \* SUBSET JobRef: jobs ever flipped to StCancelled
    fanoutReqs,                 \* SUBSET RequestId: requests registered by expansion

    \* ============ RUNNER: cancel -> kill sequencing (Scenario 4) ============
    listenerJob,                \* [Session -> NONE | [req, run, job, workerAlive,
                                \*   cancelSent, killAt, shutdownSrc]]
    processed,                  \* [Session -> SUBSET AllMsgIds] dedup set
    parsed,                     \* [Session -> SUBSET AllMsgIds] bodies parsed
    acked,                      \* [Session -> SUBSET AllMsgIds] messages acked
    workerReported,             \* [Session -> BOOLEAN] worker reported completion
    forceFailed,                \* [Session -> BOOLEAN] listener posted ForceFailJob
    stepGroupsAlive,            \* SUBSET StepId: live process groups under worker

    \* ============ WORKER: step reporting queue (Scenario 5) ================
    steps,                      \* [StepId -> [status, conclusion, killCause]]
    dirty,                      \* SUBSET StepId  (server_queue.rs dirty_keys)
    gen,                        \* Nat steps_generation (server_queue.rs:98)
    pubGen,                     \* Nat published_generation (server_queue.rs:99)
    changeOrder,                \* Nat (server_queue.rs:97)
    httpUp,                     \* BOOLEAN WorkflowStepsUpdate POST success flag
    outputBytes,                \* Nat accumulated job outputs (file_commands.rs:305)
    setupFailed,                \* BOOLEAN workspace setup error (job_runner.rs:134)
    completeReported,           \* BOOLEAN completejob POST succeeded
    renewAborted,               \* BOOLEAN renew loop aborted before completion tail
    signalled,                  \* SUBSET StepId: process group killed on cancel
    cancelSentStep,             \* SUBSET StepId: cancel delivered to the step

    \* ============ PROTOCOL: masking / tokenizer (Scenario 6) ================
    maskSet,                    \* SUBSET Str monotone-growing MaskSet (masking.rs)
    lines,                      \* Seq(Str) emitted log/feed/timeline lines
    escapeBraces,               \* BOOLEAN format-builder escapes '{' '}' — TRUE is
                                \*   the parser copy (job_builder.rs:126-135), FALSE
                                \*   is the protocol copy (azdo/job.rs:579) *BUG F1*
    scanState,                  \* {Normal, InString} tokenizer transducer state
    formatError,                \* BOOLEAN BuildFormat produced InvalidFormat
    scannerDiverged             \* BOOLEAN odd-quote-run diverged aksh vs official

(* =========================================================================
 * VARIABLE GROUPS
 * ========================================================================= *)

claimVars == <<now, reqInfo, inflightReq, planReq, agentJobReq, timelineReq,
               brokerMsg, sessionActive, sessionRunner, dispatchQueue,
               pendingJobs, cancelQueue, inflightMsgs, msgIdNext, msgIdHigh,
               mintInFlight, ghostJob, discardedCompletion, deliveredCancel>>

runVars == <<runStatus, runJobs, jobStatus, groups, heldRuns, blockedJobs,
             holderKeys, jobsetAdm, jobsetReady, pendingExp, expanding,
             jobAssign, poolPending, hasGate, gateHeld, cancelFlipped, fanoutReqs>>

listenerVars == <<listenerJob, processed, parsed, acked, workerReported,
                  forceFailed, stepGroupsAlive>>

workerVars == <<steps, dirty, gen, pubGen, changeOrder, httpUp, outputBytes,
                setupFailed, completeReported, renewAborted, signalled,
                cancelSentStep>>

protoVars == <<maskSet, lines, escapeBraces, scanState, formatError,
               scannerDiverged>>

vars == <<claimVars, runVars, listenerVars, workerVars, protoVars>>

(* =========================================================================
 * HELPERS
 * ========================================================================= *)

RECURSIVE RemoveElem(_, _)
RemoveElem(seq, x) ==
    IF seq = <<>> THEN <<>>
    ELSE IF Head(seq) = x THEN Tail(seq)
    ELSE <<Head(seq)>> \o RemoveElem(Tail(seq), x)

RECURSIVE RemoveAll(_, _)
RemoveAll(seq, set) ==
    IF seq = <<>> THEN <<>>
    ELSE IF Head(seq) \in set THEN RemoveAll(Tail(seq), set)
    ELSE <<Head(seq)>> \o RemoveAll(Tail(seq), set)

RECURSIVE AppendAll(_, _)
AppendAll(seq, set) ==
    IF set = {} THEN seq
    ELSE LET x == CHOOSE y \in set : TRUE
         IN AppendAll(Append(seq, x), set \ {x})

\* Typed sentinels (TLC is strictly typed: one universal NONE constant would
\* crash on heterogeneous equality between strings, records and integers).
NoStatus == "NoStatus"   \* sentinel for runStatus (Status-valued)
NoReq == 0               \* sentinel for RequestId-valued maps (RequestId = 1..N)
NoTime == -1             \* sentinel for reqInfo time fields (Int-valued)

NoHolder == [kind |-> "NoHolder", run |-> NoRun, job |-> NoJob, jobs |-> {}]
NoListener ==
    [req |-> NoReq, run |-> NoRun, job |-> NoJob, workerAlive |-> FALSE,
     cancelSent |-> FALSE, killAt |-> NoTime, shutdownSrc |-> NoShutdown]

JobRef(run, job) == <<run, job>>
JobRefDomain == {<<r, j>> : r \in RunId, j \in JobId}

CancelRec(run, job) == [run |-> run, job |-> job]
CancelRecDomain == {[run |-> r, job |-> j] : r \in RunId, j \in JobId}

RunH(run) == [kind |-> HolderRun, run |-> run, job |-> NoJob, jobs |-> {}]
JobH(run, job) == [kind |-> HolderJob, run |-> run, job |-> job, jobs |-> {}]
SetH(run, js) == [kind |-> HolderJobSet, run |-> run, job |-> NoJob, jobs |-> js]

\* Every possible holder record (for ArriveJobPark's pending-holder parameter)
HolderSet ==
    {RunH(r) : r \in RunId}
    \cup {JobH(r, j) : r \in RunId, j \in JobId}
    \cup {SetH(r, js) : r \in RunId, js \in SUBSET JobId}

HoldsJob(h, run, job) ==
    /\ h /= NoHolder
    /\ (h.kind = HolderRun /\ h.run = run)
       \/ (h.kind = HolderJob /\ h.run = run /\ h.job = job)
       \/ (h.kind = HolderJobSet /\ h.run = run /\ job \in h.jobs)

TerminalStatus == {StSuccess, StFailure, StCancelled, StSkipped}
Status == {StQueued, StPending, StInProgress} \cup TerminalStatus
ResultSet == {RNone, RSuccess, RFailure, RCancelled, RSkipped}

IsTerminal(st) == st \in TerminalStatus

\* Map a runner-reported TaskResult to ExecutionStatus
\* (task_result_status, distributed_task.rs:449-459)
ResultToStatus(result) ==
    IF result = RSuccess THEN StSuccess
    ELSE IF result = RFailure THEN StFailure
    ELSE IF result = RCancelled THEN StCancelled
    ELSE IF result = RSkipped THEN StSkipped
    ELSE StFailure

\* Message-id domains: request_ids live in 1..Cardinality(RequestId); low-range
\* cancel/job ids from the shared next_message_id start at 0 and collide with
\* them (F2); root-path cancels use MESSAGE_ID_BASE+ (broker.rs:162-171).
AllMsgIds ==
    (1..MaxLowMsgs)
    \cup {MESSAGE_ID_BASE + i : i \in 1..MaxHighMsgs}

\* release_concurrency_for_job (runtime_scheduling.rs:259-310): drop pending
\* holders containing the (run, job) pair; release the running holder if it
\* contains the job (Job holder immediately; Run/JobSet once every job of the
\* run is terminal in newSt); then C-07 prune holder_keys by remaining
\* presence in the group. Used by CompleteJobApply (distributed_task.rs:618),
\* SkipJob (:1218) and ReapLease (:2348).
ReleaseGroups(gs, run, job, newSt) ==
    LET allTerminal == \A j \in runJobs[run] : IsTerminal(newSt[j]) IN
    [k \in Key |->
        LET g == gs[k]
            pend == RemoveAll(g.pending,
                     {h \in Range(g.pending) : HoldsJob(h, run, job)})
        IN IF g.running /= NoHolder /\ HoldsJob(g.running, run, job)
           THEN IF g.running.kind = HolderJob \/ allTerminal
                THEN [running |-> NoHolder, pending |-> pend]
                ELSE [running |-> g.running, pending |-> pend]
           ELSE [running |-> g.running, pending |-> pend]]

\* C-07: a run's key is dropped only when the run no longer appears in the
\* group (runtime_scheduling.rs:299-308).
ReleaseHolderKeys(hks, gs, run) ==
    LET present(g, r) == (g.running /= NoHolder /\ g.running.run = r)
                         \/ \E h \in Range(g.pending) : h.run = r
    IN {k \in hks : present(gs[k], run)}

\* Default request record for unallocated ids
ReqDefault(req) ==
    [run |-> NoRun, job |-> NoJob, state |-> SNone, result |-> RNone,
     started |-> NoTime, renew |-> NoTime, deadline |-> NoTime]

\* Default step record
StepDefault(st) ==
    [status |-> StPending, conclusion |-> RNone, killCause |-> NoKill]

\* Request lookup helpers
HasReq(run, job) ==
    \E req \in RequestId :
        reqInfo[req].state /= SNone /\ reqInfo[req].run = run
        /\ reqInfo[req].job = job

ReqFor(run, job) ==
    CHOOSE req \in RequestId :
        reqInfo[req].state /= SNone /\ reqInfo[req].run = run
        /\ reqInfo[req].job = job

\* Claim-permitted check (runtime_scheduling.rs:1077-1088)
ClaimPermitted(run, job, runner) ==
    LET assigned == jobAssign[run][job] IN
    /\ (assigned /= NoRunner => assigned = runner)
    /\ ~(<<run, job>> \in poolPending)
    /\ (~RequireAssignments \/ assigned = runner)

\* Delivery audit: a cancel named (run,job) may only go to the session whose
\* active request names that job (broker.rs:238-251, 540-551).
CancelMatched(session, run, job) ==
    LET req == sessionActive[session] IN
    /\ req /= NoReq
    /\ reqInfo[req].state /= SNone
    /\ reqInfo[req].run = run /\ reqInfo[req].job = job

\* Listener dedup would drop a message whose id was already processed
MsgCollides(session, m) == m \in processed[session]

NextLowMsgId == msgIdNext + 1
NextHighMsgId == msgIdHigh + 1

\* Concurrency: is every job of a run terminal?
RunTerminal(run) ==
    /\ runStatus[run] /= NoStatus
    /\ \A j \in runJobs[run] : IsTerminal(jobStatus[run][j])

\* holder_is_terminal (concurrency.rs:238-251)
HolderTerminal(h, run) ==
    IF h = NoHolder THEN FALSE
    ELSE IF h.kind = HolderRun THEN RunTerminal(run)
    ELSE IF h.kind = HolderJob THEN
        h.job \in runJobs[run] /\ IsTerminal(jobStatus[run][h.job])
    ELSE \A j \in h.jobs :
        j \in runJobs[run] /\ IsTerminal(jobStatus[run][j])

\* All keys of a run released iff no key still holds the slot or parks it
RunHoldsNoSlot(run) ==
    \A k \in Key :
        /\ ~(groups[k].running /= NoHolder /\ groups[k].running.run = run)
        /\ ~(\E h \in Range(groups[k].pending) : h.run = run)

\* Pairs (run,job) a holder covers, restricted to jobs currently in runJobs
HolderPairs(h) ==
    IF h.kind = HolderRun
    THEN {<<r, j>> \in JobRefDomain : r = h.run /\ j \in runJobs[r]}
    ELSE IF h.kind = HolderJob
    THEN {<<r, j>> \in JobRefDomain : r = h.run /\ j = h.job}
    ELSE {<<r, j>> \in JobRefDomain : r = h.run /\ j \in h.jobs}

\* Cancel a set of job refs: flip live statuses to Cancelled
JobStatusAfterCancelling(statusVal, refs) ==
    [r \in RunId |-> [j \in JobId |->
        IF <<r, j>> \in refs /\ statusVal[r][j] \in {StQueued, StPending, StInProgress}
        THEN StCancelled
        ELSE statusVal[r][j]]]

\* Summarize a run status from its per-run job-status map
\* (summarize_run, runtime_scheduling.rs:1977-1993)
SummarizeRun(run, statusMap) ==
    IF \E j \in runJobs[run] :
           statusMap[j] \in {StQueued, StPending, StInProgress}
    THEN StInProgress
    ELSE IF \E j \in runJobs[run] : statusMap[j] = StFailure
    THEN StFailure
    ELSE IF \E j \in runJobs[run] : statusMap[j] = StCancelled
    THEN StCancelled
    ELSE StSuccess

\* Base-spec time bound for direct TLC runs (MC.tla defines its own TimeBound).
TimeBoundBase == now <= 30

(* =========================================================================
 * INIT
 * ========================================================================= *)

Init ==
    /\ now = 0
    /\ reqInfo = [req \in RequestId |-> ReqDefault(req)]
    /\ inflightReq = {}
    /\ planReq = [req \in RequestId |-> NoReq]
    /\ agentJobReq = [j \in JobId |-> NoReq]
    /\ timelineReq = [req \in RequestId |-> NoReq]
    /\ brokerMsg = [req \in RequestId |-> 0]
    /\ sessionActive = [s \in Session |-> NoReq]
    /\ sessionRunner = [s \in Session |-> CHOOSE r \in Runner : TRUE]
    /\ dispatchQueue = <<>>
    /\ pendingJobs = <<>>
    /\ cancelQueue = <<>>
    /\ inflightMsgs = [s \in Session |-> {}]
    /\ msgIdNext = 0
    /\ msgIdHigh = MESSAGE_ID_BASE
    /\ mintInFlight = {}
    /\ ghostJob = {}
    /\ discardedCompletion = {}
    /\ deliveredCancel = <<>>
    /\ runStatus = [r \in RunId |-> NoStatus]
    /\ runJobs = [r \in RunId |-> {}]
    /\ jobStatus = [r \in RunId |-> [j \in JobId |-> StQueued]]
    /\ groups = [k \in Key |-> [running |-> NoHolder, pending |-> <<>>]]
    /\ heldRuns = [r \in RunId |-> {}]
    /\ blockedJobs = <<>>
    /\ holderKeys = [r \in RunId |-> {}]
    /\ jobsetAdm = [i \in JobSetId |-> [gates |-> {}, acquired |-> {}]]
    /\ jobsetReady = {}
    /\ pendingExp = <<>>
    /\ expanding = {}
    /\ jobAssign = [r \in RunId |-> [j \in JobId |-> NoRunner]]
    /\ poolPending = {}
    /\ hasGate = [r \in RunId |-> [j \in JobId |-> NoKey]]
    /\ gateHeld = {}
    /\ cancelFlipped = {}
    /\ fanoutReqs = {}
    /\ listenerJob = [s \in Session |-> NoListener]
    /\ processed = [s \in Session |-> {}]
    /\ parsed = [s \in Session |-> {}]
    /\ acked = [s \in Session |-> {}]
    /\ workerReported = [s \in Session |-> FALSE]
    /\ forceFailed = [s \in Session |-> FALSE]
    /\ stepGroupsAlive = {}
    /\ steps = [st \in StepId |-> StepDefault(st)]
    /\ dirty = {}
    /\ gen = 0
    /\ pubGen = 0
    /\ changeOrder = 0
    /\ httpUp = TRUE
    /\ outputBytes = 0
    /\ setupFailed = FALSE
    /\ completeReported = FALSE
    /\ renewAborted = FALSE
    /\ signalled = {}
    /\ cancelSentStep = {}
    /\ maskSet = {}
    /\ lines = <<>>
    /\ escapeBraces = TRUE
    /\ scanState = Normal
    /\ formatError = FALSE
    /\ scannerDiverged = FALSE

(* =========================================================================
 * SCENARIO 1 — CLAIM / LEASE LIFECYCLE
 * ========================================================================= *)

(* -------------------------------------------------------------------------
 * SubmitRun2: workflow submission installs the run record and allocates one
 * request record per job, registering every correlation map in the same step
 * (runs.rs:733-1201; correlation registration at runs.rs:806-843 /
 * build_job_artifacts). Registration being atomic with allocation is what
 * FanoutCorrelationRegistered guards as a regression invariant.
 * ------------------------------------------------------------------------- *)
SubmitRun2(run, jobs) ==
    /\ runStatus[run] = NoStatus
    /\ jobs \subseteq JobId /\ jobs /= {}
    /\ \A j \in jobs : ~HasReq(run, j)
    /\ LET fresh == {req \in RequestId : reqInfo[req].state = SNone}
           n == Cardinality(jobs)
       IN
       /\ Cardinality(fresh) >= n
       /\ \E newIds \in SUBSET fresh :
            /\ Cardinality(newIds) = n
            /\ \E f \in [newIds -> jobs] :
                /\ \A i, j \in newIds : i /= j => f[i] /= f[j]
                /\ reqInfo' = [r \in RequestId |->
                     IF r \in newIds
                     THEN [reqInfo[r] EXCEPT
                         !.run = run, !.job = f[r],
                         !.state = SQueued, !.result = RNone]
                     ELSE reqInfo[r]]
                /\ inflightReq' = inflightReq \cup newIds
                /\ planReq' = [r \in RequestId |->
                     IF r \in newIds THEN r ELSE planReq[r]]
                /\ timelineReq' = [r \in RequestId |->
                     IF r \in newIds THEN r ELSE timelineReq[r]]
                /\ agentJobReq' = [j \in JobId |->
                     IF \E r \in newIds : f[r] = j
                     THEN CHOOSE r \in newIds : f[r] = j
                     ELSE agentJobReq[j]]
                /\ runJobs' = [runJobs EXCEPT ![run] = jobs]
                /\ jobStatus' = [jobStatus EXCEPT ![run] =
                     [j \in JobId |-> IF j \in jobs THEN StQueued ELSE jobStatus[run][j]]]
                /\ runStatus' = [runStatus EXCEPT ![run] = StQueued]
    /\ UNCHANGED <<cancelFlipped, now, brokerMsg, sessionActive, sessionRunner, dispatchQueue,
                   pendingJobs, cancelQueue, inflightMsgs, msgIdNext, msgIdHigh,
                   mintInFlight, ghostJob, discardedCompletion, deliveredCancel,
                   groups, heldRuns, blockedJobs, holderKeys, jobsetAdm,
                   jobsetReady, pendingExp, expanding, jobAssign, poolPending, hasGate,
                   gateHeld, fanoutReqs, listenerJob, processed, parsed, acked,
                   workerReported, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * EnqueuePending: submit-time / scheduler push of a job onto pending_jobs
 * (runs.rs:1095-1098 — needs-gated or max-parallel-saturated jobs;
 * promote_next_from_group Run arm keep-in-pending path :484-488;
 * drain_expansions pending_jobs.extend(ready) :1893).
 * ------------------------------------------------------------------------- *)
EnqueuePending(run, job) ==
    /\ runStatus[run] /= NoStatus
    /\ job \in runJobs[run]
    /\ ~(<<run, job>> \in Range(pendingJobs))
    \* A job lives in exactly one scheduler location: pending_jobs, the ready
    \* queue (dispatchQueue here), or an expansion reservation. defer pops the
    \* job out of pending_jobs before reserving (runtime_scheduling.rs:833),
    \* so a reserved/expanding job cannot re-enter pending_jobs.
    /\ ~(<<run, job>> \in expanding)
    /\ ~(<<run, job>> \in Range(pendingExp))
    /\ ~(\E req \in Range(dispatchQueue) :
            reqInfo[req].run = run /\ reqInfo[req].job = job)
    \* Submit installs jobs under the global lock, so no external action can
    \* settle a job (cancel/fail-fast) between build and pending-enqueue; a
    \* pending re-add of an already-terminal job is a serialization artifact.
    /\ ~IsTerminal(jobStatus[run][job])
    /\ pendingJobs' = Append(pendingJobs, <<run, job>>)
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, cancelQueue, inflightMsgs, msgIdNext,
                   msgIdHigh, mintInFlight, ghostJob, discardedCompletion,
                   deliveredCancel, runStatus, runJobs, jobStatus, groups,
                   heldRuns, blockedJobs, holderKeys, jobsetAdm, jobsetReady,
                   pendingExp, expanding, jobAssign, poolPending, hasGate,
                   gateHeld, fanoutReqs, listenerJob, processed, parsed, acked,
                   workerReported, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * DeclareGate: concurrency_from_plan_fields attaches a job-level concurrency
 * gate to a queued job at build time (runtime_scheduling.rs:195-211;
 * runs.rs:856-860). The gate is evaluated ONLY at submit for needs-empty
 * root jobs — try_enqueue_with_job_concurrency has exactly one call site
 * (runs.rs:1077) *BUG F8*.
 * ------------------------------------------------------------------------- *)
DeclareGate(run, job, key) ==
    /\ runStatus[run] /= NoStatus
    /\ job \in runJobs[run]
    /\ hasGate[run][job] = NoKey
    \* Gates attach at submit-time build (runs.rs:856-860), before the gate
    \* is ever evaluated: never to a job already pending (needs/parallel),
    \* already parked on a gate, or already dispatched.
    /\ ~(<<run, job>> \in Range(pendingJobs))
    /\ ~(<<run, job>> \in Range(blockedJobs))
    /\ ~\E req \in Range(dispatchQueue) :
            reqInfo[req].run = run /\ reqInfo[req].job = job
    /\ hasGate' = [hasGate EXCEPT ![run] = [hasGate[run] EXCEPT ![job] = key]]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, gateHeld, fanoutReqs, listenerJob, processed,
                   parsed, acked, workerReported, forceFailed, stepGroupsAlive,
                   steps, dirty, gen, pubGen, changeOrder, httpUp, outputBytes,
                   setupFailed, completeReported, renewAborted, signalled,
                   cancelSentStep, maskSet, lines, escapeBraces, scanState,
                   formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * AzdoPollDeliverInflight: AzDO poll returns a stored inflight message first
 * (distributed_task.rs:20-26).
 * ------------------------------------------------------------------------- *)
AzdoPollDeliverInflight(s, m) ==
    /\ m \in inflightMsgs[s]
    /\ UNCHANGED vars

(* -------------------------------------------------------------------------
 * AzdoPollPopCancel: AzDO poll pops the GLOBAL cancellation queue and builds
 * a JOB_CANCELLED message for WHATEVER session polls next — no session/active
 * job scoping (distributed_task.rs:28-39). *BUG F1 (Scenario 1)*: a cancel
 * for job A is delivered to whichever session polls first; A's cancel is lost.
 * ------------------------------------------------------------------------- *)
AzdoPollPopCancel(s) ==
    /\ cancelQueue /= <<>>
    /\ LET c == Head(cancelQueue)
           m == NextLowMsgId
       IN
       /\ m \in AllMsgIds
       /\ cancelQueue' = Tail(cancelQueue)
       /\ msgIdNext' = m
       /\ inflightMsgs' = [inflightMsgs EXCEPT ![s] = inflightMsgs[s] \cup {m}]
       /\ deliveredCancel' = Append(deliveredCancel,
            [run |-> c.run, job |-> c.job, session |-> s,
             matched |-> CancelMatched(s, c.run, c.job),
             collided |-> FALSE])
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, brokerMsg, cancelFlipped, cancelSentStep, changeOrder, completeReported, dirty, discardedCompletion, dispatchQueue, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, heldRuns, holderKeys, httpUp, inflightReq, jobAssign, jobStatus, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, poolPending, processed, pubGen, renewAborted, reqInfo, runJobs, runStatus, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * AzdoPollClaim: take_matching_job claims a queued job for the session. The
 * AzDO path stamps `started_at` but NOT `last_renewed_at`
 * (distributed_task.rs:138-140). *BUG F5 (Scenario 1)*: the lease reaper
 * (bootstrap.rs:155-157) is blind for a dead AzDO runner because its guard is
 * on last_renewed_at.
 * ------------------------------------------------------------------------- *)
AzdoPollClaim(s, req) ==
    /\ sessionActive[s] = NoReq
    /\ req \in Range(dispatchQueue)
    /\ LET run == reqInfo[req].run
           job == reqInfo[req].job IN
       /\ ClaimPermitted(run, job, sessionRunner[s])
       /\ dispatchQueue' = RemoveElem(dispatchQueue, req)
       /\ sessionActive' = [sessionActive EXCEPT ![s] = req]
       \* The AzDO poll delivers the FULL PipelineAgentJobRequest, so the job
       \* starts immediately; started_at is stamped but last_renewed_at is NOT
       \* (distributed_task.rs:138-140) *BUG F5*.
       /\ reqInfo' = [reqInfo EXCEPT ![req] = [reqInfo[req] EXCEPT
                !.state = SRunning, !.started = now]]
       /\ runStatus' = [runStatus EXCEPT ![run] = StInProgress]
       /\ jobStatus' = [jobStatus EXCEPT ![run] =
                [jobStatus[run] EXCEPT ![job] = StInProgress]]
       /\ jobAssign' = [jobAssign EXCEPT ![run] =
                [jobAssign[run] EXCEPT ![job] = NoRunner]]
       /\ poolPending' = poolPending \ {JobRef(run, job)}
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, brokerMsg, cancelFlipped, cancelQueue, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, heldRuns, holderKeys, httpUp, inflightMsgs, inflightReq, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, processed, pubGen, renewAborted, runJobs, scanState, scannerDiverged, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * BrokerRootClaim: broker root poll claims a queued job and stamps BOTH
 * started_at and last_renewed_at; job-ref messageId = request_id
 * (broker.rs:564-599).
 * ------------------------------------------------------------------------- *)
BrokerRootClaim(s, req) ==
    /\ sessionActive[s] = NoReq
    /\ req \in Range(dispatchQueue)
    /\ LET run == reqInfo[req].run
           job == reqInfo[req].job IN
       /\ ClaimPermitted(run, job, sessionRunner[s])
       /\ dispatchQueue' = RemoveElem(dispatchQueue, req)
       /\ sessionActive' = [sessionActive EXCEPT ![s] = req]
       /\ reqInfo' = [reqInfo EXCEPT ![req] = [reqInfo[req] EXCEPT
                !.state = SClaimed, !.started = now, !.renew = now,
                !.deadline = now + LeaseSeconds]]
       /\ brokerMsg' = [brokerMsg EXCEPT ![req] = req]
       /\ runStatus' = [runStatus EXCEPT ![run] = StInProgress]
       /\ jobStatus' = [jobStatus EXCEPT ![run] =
                [jobStatus[run] EXCEPT ![job] = StInProgress]]
       /\ jobAssign' = [jobAssign EXCEPT ![run] =
                [jobAssign[run] EXCEPT ![job] = NoRunner]]
       /\ poolPending' = poolPending \ {JobRef(run, job)}
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, cancelFlipped, cancelQueue, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, heldRuns, holderKeys, httpUp, inflightMsgs, inflightReq, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, processed, pubGen, renewAborted, runJobs, scanState, scannerDiverged, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * BrokerPoolRedeliver: legacy pool path re-delivers the active job ref on
 * every busy poll (broker.rs:253-255) — the 500 ms storm *BUG F2*.
 * ------------------------------------------------------------------------- *)
BrokerPoolRedeliver(s, req) ==
    /\ sessionActive[s] /= NoReq /\ sessionActive[s] = req
    /\ reqInfo[req].state /= SNone /\ reqInfo[req].result = RNone
    /\ brokerMsg' = [brokerMsg EXCEPT ![req] = req]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, sessionActive, sessionRunner, dispatchQueue,
                   pendingJobs, cancelQueue, inflightMsgs, msgIdNext, msgIdHigh,
                   mintInFlight, ghostJob, discardedCompletion, deliveredCancel,
                   runStatus, runJobs, jobStatus, groups, heldRuns, blockedJobs,
                   holderKeys, jobsetAdm, jobsetReady, pendingExp, expanding,
                   jobAssign, poolPending, hasGate, gateHeld, fanoutReqs,
                   listenerJob, processed, parsed, acked, workerReported,
                   forceFailed, stepGroupsAlive, steps, dirty, gen, pubGen,
                   changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * BrokerPollDeliverCancelScoped: broker poll finds a cancellation whose
 * (run,job) matches the session's active request and delivers it with a
 * HIGH-range messageId — next_broker_message_id (broker.rs:162-171, 540-551).
 * Correctly scoped.
 * ------------------------------------------------------------------------- *)
BrokerPollDeliverCancelScoped(s, req, c) ==
    /\ sessionActive[s] /= NoReq /\ sessionActive[s] = req
    /\ reqInfo[req].state /= SNone /\ reqInfo[req].result = RNone
    /\ c \in Range(cancelQueue)
    /\ c.run = reqInfo[req].run /\ c.job = reqInfo[req].job
    /\ LET m == NextHighMsgId IN
       /\ m \in AllMsgIds
       /\ cancelQueue' = RemoveElem(cancelQueue, c)
       /\ msgIdHigh' = m
       /\ inflightMsgs' = [inflightMsgs EXCEPT ![s] = inflightMsgs[s] \cup {m}]
       /\ deliveredCancel' = Append(deliveredCancel,
            [run |-> c.run, job |-> c.job, session |-> s,
             matched |-> TRUE, collided |-> FALSE])
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, brokerMsg, cancelFlipped, cancelSentStep, changeOrder, completeReported, dirty, discardedCompletion, dispatchQueue, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, heldRuns, holderKeys, httpUp, inflightReq, jobAssign, jobStatus, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdNext, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, poolPending, processed, pubGen, renewAborted, reqInfo, runJobs, runStatus, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * BrokerPollDeliverCancelPool: the POOL broker path builds the cancel with
 * build_broker_plaintext_message, i.e. from the shared low counter
 * (broker.rs:244-250 + distributed_task.rs:215-235). *BUG F2*: the messageId
 * from next_message_id (0-based) collides with the request_id used as the
 * job-ref messageId, so the runner dedup drops the cancel.
 * ------------------------------------------------------------------------- *)
BrokerPollDeliverCancelPool(s, req, c) ==
    /\ sessionActive[s] /= NoReq /\ sessionActive[s] = req
    /\ reqInfo[req].state /= SNone /\ reqInfo[req].result = RNone
    /\ c \in Range(cancelQueue)
    /\ c.run = reqInfo[req].run /\ c.job = reqInfo[req].job
    /\ LET m == NextLowMsgId IN
       /\ m \in AllMsgIds
       /\ cancelQueue' = RemoveElem(cancelQueue, c)
       /\ msgIdNext' = m
       /\ inflightMsgs' = [inflightMsgs EXCEPT ![s] = inflightMsgs[s] \cup {m}]
       /\ deliveredCancel' = Append(deliveredCancel,
            [run |-> c.run, job |-> c.job, session |-> s,
             matched |-> TRUE,
             collided |-> MsgCollides(s, m)])
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, brokerMsg, cancelFlipped, cancelSentStep, changeOrder, completeReported, dirty, discardedCompletion, dispatchQueue, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, heldRuns, holderKeys, httpUp, inflightReq, jobAssign, jobStatus, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, poolPending, processed, pubGen, renewAborted, reqInfo, runJobs, runStatus, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * BrokerPollCleanup: broker poll drops the session's active request when the
 * job has finished (broker.rs:257, 556-558; distributed_task.rs:41-47).
 * ------------------------------------------------------------------------- *)
BrokerPollCleanup(s, req) ==
    /\ sessionActive[s] /= NoReq /\ sessionActive[s] = req
    /\ reqInfo[req].state /= SNone
    /\ reqInfo[req].result /= RNone
    /\ sessionActive' = [sessionActive EXCEPT ![s] = NoReq]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionRunner, dispatchQueue,
                   pendingJobs, cancelQueue, inflightMsgs, msgIdNext, msgIdHigh,
                   mintInFlight, ghostJob, discardedCompletion, deliveredCancel,
                   runStatus, runJobs, jobStatus, groups, heldRuns, blockedJobs,
                   holderKeys, jobsetAdm, jobsetReady, pendingExp, expanding,
                   jobAssign, poolPending, hasGate, gateHeld, fanoutReqs,
                   listenerJob, processed, parsed, acked, workerReported,
                   forceFailed, stepGroupsAlive, steps, dirty, gen, pubGen,
                   changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * AcquireJobStart: runner POSTs acquirejob; the request is looked up by
 * agent_job_id and ownership is verified (broker.rs:634-652).
 * ------------------------------------------------------------------------- *)
AcquireJobStart(s, req) ==
    /\ sessionActive[s] /= NoReq /\ sessionActive[s] = req
    /\ reqInfo[req].state /= SNone
    /\ reqInfo[req].state = SClaimed
    /\ reqInfo' = [reqInfo EXCEPT ![req] = [reqInfo[req] EXCEPT
                !.state = SAcquiring]]
    /\ mintInFlight' = mintInFlight \cup {req}
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, brokerMsg, cancelFlipped, cancelQueue, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, dispatchQueue, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, heldRuns, holderKeys, httpUp, inflightMsgs, inflightReq, jobAssign, jobStatus, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, poolPending, processed, pubGen, renewAborted, runJobs, runStatus, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * AcquireJobMintOk: token mint succeeds and acquirejob returns a live payload
 * (broker.rs:653-694). If the claim was meanwhile failed/reaped (terminal),
 * the payload is stale — *BUG S1*: ghost job execution. The mint runs
 * OUTSIDE the global lock with no timeout (broker.rs:661), so the reaper can
 * fail the claim and dispatch a successor while the mint is still in flight.
 * ------------------------------------------------------------------------- *)
AcquireJobMintOk(req) ==
    /\ req \in mintInFlight
    /\ LET run == reqInfo[req].run
           job == reqInfo[req].job IN
       /\ reqInfo' = [reqInfo EXCEPT ![req] = [reqInfo[req] EXCEPT
                !.state = SRunning]]
       /\ mintInFlight' = mintInFlight \ {req}
       /\ IF reqInfo[req].state = STerminal
          THEN ghostJob' = ghostJob \cup {req}
          ELSE UNCHANGED ghostJob
       /\ brokerMsg' = [brokerMsg EXCEPT ![req] = req]
    /\ UNCHANGED <<cancelFlipped, now, inflightReq, planReq, agentJobReq, timelineReq,
                   sessionActive, sessionRunner, dispatchQueue, pendingJobs,
                   cancelQueue, inflightMsgs, msgIdNext, msgIdHigh,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   httpUp, outputBytes, setupFailed, completeReported,
                   renewAborted, signalled, cancelSentStep, maskSet, lines,
                   escapeBraces, scanState, formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * AcquireJobMintFail: token mint refusal fails the claim terminally via
 * fail_unclaimable_request (broker.rs:664-667, 759-798) and completes the job
 * as Failure through complete_job_inner.
 * ------------------------------------------------------------------------- *)
AcquireJobMintFail(req) ==
    /\ req \in mintInFlight
    /\ LET run == reqInfo[req].run
           job == reqInfo[req].job IN
       /\ reqInfo' = [reqInfo EXCEPT ![req] = [reqInfo[req] EXCEPT
                !.state = STerminal, !.result = RFailure]]
       /\ mintInFlight' = mintInFlight \ {req}
       /\ runStatus' = [runStatus EXCEPT ![run] = StFailure]
       /\ jobStatus' = [jobStatus EXCEPT ![run] =
                [jobStatus[run] EXCEPT ![job] = StFailure]]
       /\ sessionActive' = [s \in Session |->
                IF sessionActive[s] /= NoReq /\ sessionActive[s] = req THEN NoReq ELSE sessionActive[s]]
       /\ inflightReq' = inflightReq \ {req}
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, brokerMsg, cancelFlipped, cancelQueue, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, dispatchQueue, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, heldRuns, holderKeys, httpUp, inflightMsgs, jobAssign, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, poolPending, processed, pubGen, renewAborted, runJobs, scanState, scannerDiverged, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * RenewJob: broker_renew_job refreshes locked_until and last_renewed_at
 * (broker.rs:847-868).
 * ------------------------------------------------------------------------- *)
RenewJob(s, req) ==
    /\ sessionActive[s] /= NoReq /\ sessionActive[s] = req
    /\ reqInfo[req].state /= SNone
    /\ reqInfo[req].state /= STerminal
    /\ reqInfo' = [reqInfo EXCEPT ![req] = [reqInfo[req] EXCEPT
                !.renew = now, !.deadline = now + LeaseSeconds]]
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, brokerMsg, cancelFlipped, cancelQueue, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, dispatchQueue, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, heldRuns, holderKeys, httpUp, inflightMsgs, inflightReq, jobAssign, jobStatus, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, poolPending, processed, pubGen, renewAborted, runJobs, runStatus, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * CompleteJobSetResult: broker_complete_job sets record.result and frees the
 * session slot BEFORE complete_job_inner runs (broker.rs:901-941).
 * ------------------------------------------------------------------------- *)
CompleteJobSetResult(s, req, outcome) ==
    /\ sessionActive[s] /= NoReq /\ sessionActive[s] = req
    /\ reqInfo[req].state /= SNone
    /\ reqInfo[req].state = SRunning
    /\ outcome \in {RSuccess, RFailure, RCancelled}
    /\ reqInfo' = [reqInfo EXCEPT ![req] = [reqInfo[req] EXCEPT
                !.result = outcome, !.deadline = now + LeaseSeconds]]
    /\ sessionActive' = [sessionActive EXCEPT ![s] = NoReq]
    /\ UNCHANGED <<cancelFlipped, now, inflightReq, planReq, agentJobReq, timelineReq,
                   brokerMsg, sessionRunner, dispatchQueue, pendingJobs,
                   cancelQueue, inflightMsgs, msgIdNext, msgIdHigh,
                   mintInFlight, ghostJob, discardedCompletion, deliveredCancel,
                   runStatus, runJobs, jobStatus, groups, heldRuns, blockedJobs,
                   holderKeys, jobsetAdm, jobsetReady, pendingExp, expanding,
                   jobAssign, poolPending, hasGate, gateHeld, fanoutReqs,
                   listenerJob, processed, parsed, acked, workerReported,
                   forceFailed, stepGroupsAlive, steps, dirty, gen, pubGen,
                   changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * CompleteJobApply: complete_job_inner applies the completion under the
 * terminal-lock (distributed_task.rs:504-765). *BUG S2*: if the prior job
 * status is already terminal (and not Cancelled), the late runner-reported
 * completion is discarded (distributed_task.rs:525-527) — a reaper-Failure
 * that completes first makes a later runner Success vanish, losing outputs.
 * Cancelled + Success/Failure maps back to Cancelled (effective, :538-542).
 * ------------------------------------------------------------------------- *)
CompleteJobApply(req, outcome) ==
    /\ reqInfo[req].state /= SNone
    /\ LET run == reqInfo[req].run
           job == reqInfo[req].job
           prior == jobStatus[run][job]
           reported == ResultToStatus(outcome)
           discard == IsTerminal(prior) /\ prior /= StCancelled
           effective == IF prior = StCancelled /\ reported \in {StSuccess, StFailure}
                        THEN StCancelled ELSE reported
           \* release_concurrency_for_job via the shared operators
           \* (complete_job_inner non-discard path, distributed_task.rs:618).
           \* The reusable-caller release (:620) is under-approximated —
           \* PromoteNextRunArm frees a caller gate once its holder is gone.
           newSt == [jobStatus[run] EXCEPT ![job] = effective]
           newGroups == ReleaseGroups(groups, run, job, newSt)
       IN
       /\ IF discard
          THEN discardedCompletion' = discardedCompletion \cup {req}
          ELSE UNCHANGED discardedCompletion
       /\ IF discard
          THEN UNCHANGED <<blockedJobs, dispatchQueue, groups, heldRuns,
                           holderKeys, jobStatus, pendingJobs, runJobs,
                           runStatus>>
          ELSE
             /\ jobStatus' = [jobStatus EXCEPT ![run] = newSt]
             /\ runStatus' = [runStatus EXCEPT ![run] = SummarizeRun(run, newSt)]
             /\ dispatchQueue' = RemoveElem(dispatchQueue, req)
             /\ pendingJobs' = RemoveElem(pendingJobs, <<run, job>>)
             /\ blockedJobs' = RemoveElem(blockedJobs, <<run, job>>)
             /\ heldRuns' = [r \in RunId |->
                    IF r = run THEN heldRuns[run] \ {<<run, job>>} ELSE heldRuns[r]]
             /\ groups' = newGroups
             /\ holderKeys' = [r \in RunId |->
                    IF r /= run THEN holderKeys[r]
                    ELSE ReleaseHolderKeys(holderKeys[run], newGroups, run)]
             /\ runJobs' = runJobs
       /\ reqInfo' = [reqInfo EXCEPT ![req] = [reqInfo[req] EXCEPT
                !.state = STerminal, !.result = outcome]]
       /\ inflightReq' = inflightReq \ {req}
       \* Free any session whose active request is req (real
       \* session_active_requests.retain, distributed_task.rs:651-654).
       /\ sessionActive' = [s \in Session |->
                IF sessionActive[s] = req THEN NoReq ELSE sessionActive[s]]
    /\ UNCHANGED <<acked, agentJobReq, brokerMsg, cancelFlipped, cancelQueue, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, hasGate, httpUp, inflightMsgs, jobAssign, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, planReq, poolPending, processed, pubGen, renewAborted, scanState, scannerDiverged, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * ReapLease: disconnect reaper — if last_renewed_at is set and the lease
 * elapsed, fail the job (bootstrap.rs:154-179). The guard `renew /= NONE`
 * makes a dead AzDO runner (never stamped) unreapable *BUG F5*.
 * ------------------------------------------------------------------------- *)
ReapLease(req) ==
    /\ reqInfo[req].state /= SNone
    /\ reqInfo[req].state /= STerminal
    /\ reqInfo[req].renew /= NoTime
    /\ now >= reqInfo[req].deadline
    /\ LET run == reqInfo[req].run
           job == reqInfo[req].job IN
       /\ reqInfo' = [reqInfo EXCEPT ![req] = [reqInfo[req] EXCEPT
                !.state = STerminal, !.result = RFailure]]
       /\ jobStatus' = [jobStatus EXCEPT ![run] =
                [jobStatus[run] EXCEPT ![job] = StFailure]]
       /\ runStatus' = [runStatus EXCEPT ![run] = StFailure]
       /\ sessionActive' = [s \in Session |->
                IF sessionActive[s] /= NoReq /\ sessionActive[s] = req THEN NoReq ELSE sessionActive[s]]
       /\ inflightReq' = inflightReq \ {req}
       /\ dispatchQueue' = RemoveElem(dispatchQueue, req)
       /\ pendingJobs' = RemoveElem(pendingJobs, <<run, job>>)
       /\ blockedJobs' = RemoveElem(blockedJobs, <<run, job>>)
       /\ heldRuns' = [r \in RunId |->
                IF r = run THEN heldRuns[run] \ {<<run, job>>} ELSE heldRuns[r]]
    /\ UNCHANGED <<acked, agentJobReq, brokerMsg, cancelFlipped, cancelQueue, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, holderKeys, httpUp, inflightMsgs, jobAssign, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, planReq, poolPending, processed, pubGen, renewAborted, runJobs, scanState, scannerDiverged, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * ReapTimeout: job timeout enforcement — enqueue a cancellation for the
 * timed-out request (bootstrap.rs:114-152).
 * ------------------------------------------------------------------------- *)
ReapTimeout(req) ==
    /\ reqInfo[req].state /= SNone
    /\ reqInfo[req].state /= STerminal
    /\ reqInfo[req].started /= NoTime
    /\ now >= reqInfo[req].started + JobTimeout
    /\ LET run == reqInfo[req].run
           job == reqInfo[req].job IN
       /\ cancelQueue' = Append(cancelQueue, CancelRec(run, job))
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, inflightMsgs, msgIdNext,
                   msgIdHigh, mintInFlight, ghostJob, discardedCompletion,
                   deliveredCancel, runStatus, runJobs, jobStatus, groups,
                   heldRuns, blockedJobs, holderKeys, jobsetAdm, jobsetReady,
                   pendingExp, expanding, jobAssign, poolPending, hasGate,
                   gateHeld, fanoutReqs, listenerJob, processed, parsed, acked,
                   workerReported, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * AckMessage: the listener acknowledges a delivered message, removing it from
 * the session's inflight set (distributed_task.rs:246-259).
 * ------------------------------------------------------------------------- *)
AckMessage(s, m) ==
    /\ m \in inflightMsgs[s]
    /\ inflightMsgs' = [inflightMsgs EXCEPT ![s] = inflightMsgs[s] \ {m}]
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, brokerMsg, cancelFlipped, cancelQueue, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, dispatchQueue, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, heldRuns, holderKeys, httpUp, inflightReq, jobAssign, jobStatus, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, poolPending, processed, pubGen, renewAborted, reqInfo, runJobs, runStatus, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * TimeAdvance: abstract clock. Bounded in MC by a counter.
 * ------------------------------------------------------------------------- *)
TimeAdvance ==
    /\ now' = now + 1
    /\ UNCHANGED <<cancelFlipped, reqInfo, inflightReq, planReq, agentJobReq, timelineReq,
                   brokerMsg, sessionActive, sessionRunner, dispatchQueue,
                   pendingJobs, cancelQueue, inflightMsgs, msgIdNext, msgIdHigh,
                   mintInFlight, ghostJob, discardedCompletion, deliveredCancel,
                   runStatus, runJobs, jobStatus, groups, heldRuns, blockedJobs,
                   holderKeys, jobsetAdm, jobsetReady, pendingExp, expanding,
                   jobAssign, poolPending, hasGate, gateHeld, fanoutReqs,
                   listenerJob, processed, parsed, acked, workerReported,
                   forceFailed, stepGroupsAlive, steps, dirty, gen, pubGen,
                   changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* =========================================================================
 * SCENARIO 2 — CONCURRENCY-GROUP HOLDER STATE MACHINE
 * ========================================================================= *)

(* -------------------------------------------------------------------------
 * ArriveRunFree: workflow-level concurrency arrival on a free group —
 * try_acquire_concurrency Ok(true) (runtime_scheduling.rs:615-620;
 * runs.rs:872-885).
 * ------------------------------------------------------------------------- *)
ArriveRunFree(run, key) ==
    /\ runStatus[run] /= NoStatus
    /\ ~IsTerminal(runStatus[run])
    /\ groups[key].running = NoHolder
    /\ groups' = [groups EXCEPT ![key] =
            [running |-> RunH(run), pending |-> <<>>]]
    /\ holderKeys' = [holderKeys EXCEPT ![run] = holderKeys[run] \cup {key}]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, heldRuns, blockedJobs, jobsetAdm, jobsetReady,
                   pendingExp, expanding, jobAssign, poolPending, hasGate,
                   gateHeld, fanoutReqs, listenerJob, processed, parsed, acked,
                   workerReported, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * ArriveRunCIP: cancel-in-progress arrival — the running holder is taken,
 * pending drained, the new holder installed, and the previous holder is
 * cancelled (runtime_scheduling.rs:622-637). Previous holder is a Run.
 * ------------------------------------------------------------------------- *)
ArriveRunCIP(run, key, prevRun) ==
    /\ runStatus[run] /= NoStatus
    /\ ~IsTerminal(runStatus[run])
    /\ groups[key].running /= NoHolder
    /\ groups[key].running.kind = HolderRun
    /\ groups[key].running.run = prevRun
    /\ prevRun /= run
    /\ LET prevPairs == {<<r, j>> \in JobRefDomain :
                r = prevRun /\ j \in runJobs[r]}
       IN
       /\ groups' = [groups EXCEPT ![key] =
            [running |-> RunH(run), pending |-> <<>>]]
       /\ holderKeys' = [holderKeys EXCEPT ![run] = holderKeys[run] \cup {key}]
       /\ jobStatus' = JobStatusAfterCancelling(jobStatus, prevPairs)
       /\ runStatus' = [runStatus EXCEPT
            ![run] = StQueued, ![prevRun] = StCancelled]
       /\ cancelQueue' = AppendAll(cancelQueue,
            {CancelRec(p[1], p[2]) : p \in prevPairs})
       /\ dispatchQueue' = RemoveAll(dispatchQueue,
            {ReqFor(p[1], p[2]) : p \in prevPairs})
       /\ pendingJobs' = RemoveAll(pendingJobs, prevPairs)
       /\ blockedJobs' = RemoveAll(blockedJobs, prevPairs)
       /\ heldRuns' = [r \in RunId |-> IF r = prevRun THEN {} ELSE heldRuns[r]]
       /\ cancelFlipped' = cancelFlipped \cup prevPairs
    /\ UNCHANGED <<now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   inflightMsgs, msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runJobs, jobsetAdm,
                   jobsetReady, pendingExp, expanding, jobAssign, poolPending,
                   hasGate, gateHeld, fanoutReqs, listenerJob, processed,
                   parsed, acked, workerReported, forceFailed, stepGroupsAlive,
                   steps, dirty, gen, pubGen, changeOrder, httpUp, outputBytes,
                   setupFailed, completeReported, renewAborted, signalled,
                   cancelSentStep, maskSet, lines, escapeBraces, scanState,
                   formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * ArriveRunPark: contended arrival without cancel-in-progress — the run is
 * parked on the group's pending queue and its jobs are held out of the ready
 * queue (runtime_scheduling.rs:640-671; runs.rs:887-890 + held_runs
 * runs.rs:957-961).
 * ------------------------------------------------------------------------- *)
ArriveRunPark(run, key) ==
    /\ runStatus[run] /= NoStatus
    /\ ~IsTerminal(runStatus[run])
    /\ groups[key].running /= NoHolder
    /\ groups[key].running.run /= run
    /\ groups' = [groups EXCEPT ![key] =
            [running |-> groups[key].running,
             pending |-> Append(groups[key].pending, RunH(run))]]
    /\ holderKeys' = [holderKeys EXCEPT ![run] = holderKeys[run] \cup {key}]
    /\ heldRuns' = [heldRuns EXCEPT ![run] = {<<run, j>> : j \in runJobs[run]}]
    \* Real submit parks jobs into held_runs atomically — they never sit in
    \* pending_jobs; remove them so DeferExpansion cannot fire on held jobs.
    /\ pendingJobs' = RemoveAll(pendingJobs, {<<run, j>> : j \in runJobs[run]})
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, blockedJobs, jobsetAdm, jobsetReady, pendingExp,
                   expanding, jobAssign, poolPending, hasGate, gateHeld,
                   fanoutReqs, listenerJob, processed, parsed, acked,
                   workerReported, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * EnqueueAssignment: the on_job_enqueued effect (runtime_scheduling.rs:1099-
 * 1146) — records dispatch intent by writing job_assignments, but ONLY when
 * pool assignment enforcement is active and the job is not already paired.
 * on_job_enqueued is invoked by try_enqueue_with_job_concurrency (:14,:56),
 * promote_next_from_group Run arm (:481) and JobSet arm (:577) — NOT by
 * promote_ready_jobs (:882) *BUG F-3*. Modeled as a conjunct, not a
 * standalone action, so the promote_ready_jobs path genuinely lacks it.
 * ------------------------------------------------------------------------- *)
EnqueueAssignment(run, job) ==
    IF RequireAssignments /\ jobAssign[run][job] = NoRunner
       /\ ~(<<run, job>> \in poolPending)
    THEN jobAssign' = [jobAssign EXCEPT ![run] =
            [jobAssign[run] EXCEPT ![job] = CHOOSE r \in Runner : TRUE]]
    ELSE UNCHANGED jobAssign

(* -------------------------------------------------------------------------
 * ArriveJobFree: job-level concurrency at SUBMIT time only — try_enqueue_with_
 * job_concurrency Ok(true) (runtime_scheduling.rs:5-17, 54-59; the single call
 * site runs.rs:1075-1094).
 * ------------------------------------------------------------------------- *)
ArriveJobFree(run, job, key) ==
    /\ runStatus[run] /= NoStatus
    /\ ~IsTerminal(runStatus[run])
    /\ job \in runJobs[run]
    /\ jobStatus[run][job] = StQueued
    /\ hasGate[run][job] = key
    /\ groups[key].running = NoHolder
    \* Submit-time only: try_enqueue_with_job_concurrency has exactly one call
    \* site (runs.rs:1075-1094). A job that is still in pendingJobs or already
    \* dispatched (promote_ready_jobs output) never re-enters the submit gate;
    \* re-promotion goes through PromoteNext* (blockedJobs). Without this guard
    \* the model could gate-check twice and append the same req to
    \* dispatchQueue twice (spurious DispatchQueueUnique violation).
    /\ ~(<<run, job>> \in Range(pendingJobs))
    /\ ~(<<run, job>> \in Range(blockedJobs))
    \* A dispatch request already queued for this job means the submit-time
    \* gate check has already fired (enqueue) or can never fire (the job was
    \* promoted via the dependency path); try_enqueue_with_job_concurrency is
    \* synchronous at submit — no re-check once queued.
    /\ ~\E req \in Range(dispatchQueue) :
            reqInfo[req].run = run /\ reqInfo[req].job = job
    /\ \E req \in RequestId :
        /\ reqInfo[req].run = run /\ reqInfo[req].job = job
        /\ groups' = [groups EXCEPT ![key] =
                [running |-> JobH(run, job), pending |-> <<>>]]
        /\ gateHeld' = gateHeld \cup {JobRef(run, job)}
        /\ EnqueueAssignment(run, job)   \* try_enqueue_with_job_concurrency calls on_job_enqueued (:56)
        /\ dispatchQueue' = Append(dispatchQueue, req)
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, brokerMsg, cancelFlipped, cancelQueue, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gen, ghostJob, hasGate, heldRuns, holderKeys, httpUp, inflightMsgs, inflightReq, jobAssign, jobStatus, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, poolPending, processed, pubGen, renewAborted, reqInfo, runJobs, runStatus, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * ArriveJobCIP: cancel-in-progress arrival for a job-level holder — previous
 * (Run) holder is cancelled (runtime_scheduling.rs:622-637).
 * ------------------------------------------------------------------------- *)
ArriveJobCIP(run, job, key, prevRun) ==
    /\ runStatus[run] /= NoStatus
    /\ ~IsTerminal(runStatus[run])
    /\ job \in runJobs[run]
    /\ jobStatus[run][job] = StQueued
    /\ hasGate[run][job] = key
    /\ groups[key].running /= NoHolder
    /\ groups[key].running.kind = HolderRun
    /\ groups[key].running.run = prevRun
    \* Submit-time only (same rationale as ArriveJobFree).
    /\ ~(<<run, job>> \in Range(pendingJobs))
    /\ ~\E req \in Range(dispatchQueue) :
            reqInfo[req].run = run /\ reqInfo[req].job = job
    /\ LET prevPairs == {<<r, j>> \in JobRefDomain :
                r = prevRun /\ j \in runJobs[r]}
       IN
       /\ \E req \in RequestId :
            /\ reqInfo[req].run = run /\ reqInfo[req].job = job
            /\ groups' = [groups EXCEPT ![key] =
                    [running |-> JobH(run, job), pending |-> <<>>]]
            /\ gateHeld' = gateHeld \cup {JobRef(run, job)}
            /\ EnqueueAssignment(run, job)   \* try_enqueue_with_job_concurrency Ok(true) calls on_job_enqueued (:56)
            \* Same-run CIP: cancel_run_inner removes nothing from the queue
            \* for the arriving job (it is still local at submit), so its
            \* submit-time dispatch request is NOT re-queued. Cross-run only
            \* appends the arrival's request.
            /\ dispatchQueue' =
                IF run = prevRun
                THEN RemoveAll(dispatchQueue,
                         {ReqFor(p[1], p[2]) : p \in prevPairs})
                ELSE Append(RemoveAll(dispatchQueue,
                         {ReqFor(p[1], p[2]) : p \in prevPairs}), req)
            /\ jobStatus' = JobStatusAfterCancelling(jobStatus,
                    prevPairs \cup {<<run, job>>})
            /\ runStatus' = [runStatus EXCEPT ![prevRun] = StCancelled]
            /\ cancelQueue' = AppendAll(cancelQueue,
                    {CancelRec(p[1], p[2]) : p \in prevPairs})
            /\ pendingJobs' = RemoveAll(pendingJobs, prevPairs)
            /\ blockedJobs' = RemoveAll(blockedJobs, prevPairs)
            /\ heldRuns' = [r \in RunId |-> IF r = prevRun THEN {} ELSE heldRuns[r]]
            /\ cancelFlipped' = cancelFlipped \cup prevPairs
    /\ UNCHANGED <<acked, agentJobReq, brokerMsg, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gen, ghostJob, hasGate, holderKeys, httpUp, inflightMsgs, inflightReq, jobAssign, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, planReq, poolPending, processed, pubGen, renewAborted, reqInfo, runJobs, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * ArriveJobPark: contended arrival without cancel-in-progress — queue-mode
 * join (apply_queue_mode concurrency.rs:269-295): single mode cancels the
 * existing pending holder (except same-run, runtime_scheduling.rs:646-658)
 * and parks the arrival; max mode parks the arrival. The job is pushed to
 * concurrency_blocked (Ok(false) arm, runtime_scheduling.rs:60-64).
 * ------------------------------------------------------------------------- *)
ArriveJobPark(run, job, key, mode, pendingH) ==
    /\ runStatus[run] /= NoStatus
    /\ ~IsTerminal(runStatus[run])
    /\ job \in runJobs[run]
    /\ jobStatus[run][job] = StQueued
    /\ hasGate[run][job] = key
    /\ groups[key].running /= NoHolder
    \* Submit-time only (same rationale as ArriveJobFree): a dependency-parked
    \* job never reaches try_enqueue_with_job_concurrency.
    /\ ~(<<run, job>> \in Range(pendingJobs))
    \* A dispatch request already queued for this job means the submit-time
    \* gate check has already fired (enqueue) or can never fire (the job was
    \* promoted via the dependency path); try_enqueue_with_job_concurrency is
    \* synchronous at submit — no re-check once queued.
    /\ ~\E req \in Range(dispatchQueue) :
            reqInfo[req].run = run /\ reqInfo[req].job = job
    /\ LET pend == groups[key].pending IN
       /\ (IF mode = ModeSingle
           THEN /\ pendingH \in Range(pend)
                /\ pendingH.run /= run
                /\ LET pHPairs == HolderPairs(pendingH) IN
                   /\ groups' = [groups EXCEPT ![key] =
                        [running |-> groups[key].running,
                         pending |-> <<JobH(run, job)>>]]
                   /\ jobStatus' = JobStatusAfterCancelling(jobStatus,
                        pHPairs \cup {<<run, job>>})
                   /\ cancelQueue' = AppendAll(cancelQueue,
                        {CancelRec(p[1], p[2]) : p \in pHPairs})
                   /\ dispatchQueue' = RemoveAll(dispatchQueue,
                        {ReqFor(p[1], p[2]) : p \in pHPairs})
                   /\ pendingJobs' = RemoveAll(pendingJobs, pHPairs)
                   /\ blockedJobs' = Append(RemoveAll(blockedJobs, pHPairs),
                        <<run, job>>)
                   /\ heldRuns' = [r \in RunId |-> IF r = pendingH.run THEN {}
                                    ELSE heldRuns[r]]
                   /\ cancelFlipped' = cancelFlipped \cup pHPairs
            ELSE /\ groups' = [groups EXCEPT ![key] =
                     [running |-> groups[key].running,
                      pending |-> Append(pend, JobH(run, job))]]
                 /\ blockedJobs' = Append(blockedJobs, <<run, job>>)
                 /\ UNCHANGED <<cancelFlipped, cancelQueue, dispatchQueue,
                               heldRuns, jobStatus, pendingJobs>>)
    /\ UNCHANGED <<acked, agentJobReq, brokerMsg, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, hasGate, holderKeys, httpUp, inflightMsgs, inflightReq, jobAssign, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, planReq, poolPending, processed, pubGen, renewAborted, reqInfo, runJobs, runStatus, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * PromoteNextRunArm: promote_next_from_group, Holder::Run arm — the held jobs
 * are re-inserted as Queued and dispatched (runtime_scheduling.rs:458-497).
 * *BUG F1*: the Queued write at :469 overwrites a Cancelled status,
 * resurrecting jobs cancelled by fail-fast.
 * ------------------------------------------------------------------------- *)
PromoteNextRunArm(key, run) ==
    /\ groups[key].running = NoHolder
    /\ \E h \in Range(groups[key].pending) : h.kind = HolderRun /\ h.run = run
    /\ LET h == RunH(run) IN
       /\ groups' = [groups EXCEPT ![key] =
            [running |-> h, pending |-> RemoveElem(groups[key].pending, h)]]
       /\ holderKeys' = [holderKeys EXCEPT ![run] = holderKeys[run] \cup {key}]
       /\ \E job \in heldRuns[run] :
            /\ heldRuns' = [heldRuns EXCEPT ![run] = heldRuns[run] \ {job}]
            /\ jobStatus' = [jobStatus EXCEPT ![run] =
                 [jobStatus[run] EXCEPT ![job[2]] = StQueued]]
            /\ EnqueueAssignment(run, job[2])   \* promote_next_from_group Run arm calls on_job_enqueued (:481)
            /\ \E req \in RequestId : reqInfo[req].run = run /\ reqInfo[req].job = job[2]
               /\ dispatchQueue' = Append(dispatchQueue, req)
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   pendingJobs, cancelQueue, inflightMsgs, msgIdNext, msgIdHigh,
                   mintInFlight, ghostJob, discardedCompletion, deliveredCancel,
                   runStatus, runJobs, blockedJobs, jobsetAdm, jobsetReady,
                   pendingExp, expanding, jobAssign, poolPending, hasGate,
                   gateHeld, fanoutReqs, listenerJob, processed, parsed, acked,
                   workerReported, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * PromoteNextJobArm: promote_next_from_group, Holder::Job arm — pops the job
 * from concurrency_blocked, checks max-parallel, then dispatches
 * (runtime_scheduling.rs:498-529). *BUG F1*: the Queued write at :524
 * overwrites a Cancelled job that fail-fast left in concurrency_blocked.
 * *BUG F3*: when max-parallel is still full the holder is re-pushed to the
 * FRONT and the job re-inserted — no sweep ever re-promotes it because
 * promote_ready_jobs only reads pending_jobs, never concurrency_blocked
 * (runtime_scheduling.rs:507-518).
 * ------------------------------------------------------------------------- *)
PromoteNextJobArm(key, run, job, mpFull) ==
    /\ groups[key].running = NoHolder
    /\ \E h \in Range(groups[key].pending) :
        h.kind = HolderJob /\ h.run = run /\ h.job = job
    /\ <<run, job>> \in Range(blockedJobs)
    /\ LET h == JobH(run, job) IN
       /\ IF mpFull
          THEN
             /\ groups' = [groups EXCEPT ![key] =
                    [running |-> NoHolder,
                     pending |-> <<h>> \o RemoveElem(groups[key].pending, h)]]
             /\ blockedJobs' = blockedJobs
             /\ UNCHANGED <<dispatchQueue, gateHeld, jobAssign, jobStatus>>
          ELSE
             /\ groups' = [groups EXCEPT ![key] =
                    [running |-> h, pending |-> RemoveElem(groups[key].pending, h)]]
             /\ gateHeld' = gateHeld \cup {JobRef(run, job)}
             /\ blockedJobs' = RemoveElem(blockedJobs, <<run, job>>)
             /\ EnqueueAssignment(run, job)   \* promote_next_from_group Job arm calls on_job_enqueued (:527)
             /\ jobStatus' = [jobStatus EXCEPT ![run] =
                    [jobStatus[run] EXCEPT ![job] = StQueued]]
             /\ \E req \in RequestId : reqInfo[req].run = run /\ reqInfo[req].job = job
                /\ dispatchQueue' = Append(dispatchQueue, req)
    /\ UNCHANGED <<acked, agentJobReq, brokerMsg, cancelFlipped, cancelQueue, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gen, ghostJob, hasGate, heldRuns, holderKeys, httpUp, inflightMsgs, inflightReq, jobAssign, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, poolPending, processed, pubGen, renewAborted, reqInfo, runJobs, runStatus, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * PromoteDispatchJob: promote_ready_jobs dependency-decision Run arm pushes a
 * ready job straight to the queue with NO job-level concurrency gate
 * (runtime_scheduling.rs:842-857). *BUG F8 / S3 F-2*: needs-gated, held-run
 * and runtime-expanded jobs bypass try_acquire_concurrency entirely.
 * ------------------------------------------------------------------------- *)
PromoteDispatchJob(run, job) ==
    /\ runStatus[run] /= NoStatus
    /\ job \in runJobs[run]
    /\ <<run, job>> \in Range(pendingJobs)
    /\ pendingJobs' = RemoveElem(pendingJobs, <<run, job>>)
    /\ jobStatus' = [jobStatus EXCEPT ![run] =
            [jobStatus[run] EXCEPT ![job] = StQueued]]
    /\ \E req \in RequestId : reqInfo[req].run = run /\ reqInfo[req].job = job
       /\ dispatchQueue' = Append(dispatchQueue, req)
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   cancelQueue, inflightMsgs, msgIdNext, msgIdHigh,
                   mintInFlight, ghostJob, discardedCompletion, deliveredCancel,
                   runStatus, runJobs, groups, heldRuns, blockedJobs,
                   holderKeys, jobsetAdm, jobsetReady, pendingExp, expanding,
                   jobAssign, poolPending, hasGate, gateHeld, fanoutReqs,
                   listenerJob, processed, parsed, acked, workerReported,
                   forceFailed, stepGroupsAlive, steps, dirty, gen, pubGen,
                   changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * PromoteReadyJob: promote_ready_jobs Run arm pushes to the queue but does
 * NOT call on_job_enqueued (runtime_scheduling.rs:842-857, :882). *BUG F-3*:
 * under PRELOOP_REQUIRE_JOB_ASSIGNMENTS, job_assignments is never written for
 * these jobs, so claim_permitted returns false for every runner and the job
 * is unclaimable forever.
 * ------------------------------------------------------------------------- *)
PromoteReadyJob(run, job) ==
    /\ runStatus[run] /= NoStatus
    /\ job \in runJobs[run]
    /\ <<run, job>> \in Range(pendingJobs)
    /\ pendingJobs' = RemoveElem(pendingJobs, <<run, job>>)
    /\ jobStatus' = [jobStatus EXCEPT ![run] =
            [jobStatus[run] EXCEPT ![job] = StQueued]]
    /\ \E req \in RequestId : reqInfo[req].run = run /\ reqInfo[req].job = job
       /\ dispatchQueue' = Append(dispatchQueue, req)
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   cancelQueue, inflightMsgs, msgIdNext, msgIdHigh,
                   mintInFlight, ghostJob, discardedCompletion, deliveredCancel,
                   runStatus, runJobs, groups, heldRuns, blockedJobs,
                   holderKeys, jobsetAdm, jobsetReady, pendingExp, expanding,
                   jobAssign, poolPending, hasGate, gateHeld, fanoutReqs,
                   listenerJob, processed, parsed, acked, workerReported,
                   forceFailed, stepGroupsAlive, steps, dirty, gen, pubGen,
                   changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * FailFast: apply_matrix_fail_fast — a failed matrix cell cancels same-base
 * siblings and removes them from queue/pending_jobs (runtime_scheduling.rs:
 * 1233-1291). *BUG F1*: concurrency_blocked is NEVER touched (:1274-1279
 * retain only queue + pending_jobs), so a concurrency-blocked sibling stays in
 * blockedJobs and is later resurrected by PromoteNextJobArm.
 * ------------------------------------------------------------------------- *)
FailFast(run, failedJob, siblings) ==
    /\ runStatus[run] /= NoStatus
    /\ failedJob \in runJobs[run]
    /\ siblings \subseteq runJobs[run] \ {failedJob}
    /\ siblings /= {}
    /\ \A j \in siblings : jobStatus[run][j] \in {StQueued, StPending, StInProgress}
    /\ \A j \in siblings : HasReq(run, j)
    /\ jobStatus' = [jobStatus EXCEPT ![run] =
            [j \in JobId |-> IF j \in siblings THEN StCancelled ELSE jobStatus[run][j]]]
    /\ runStatus' = [runStatus EXCEPT ![run] =
            SummarizeRun(run, [j \in JobId |-> IF j \in siblings THEN StCancelled ELSE jobStatus[run][j]])]
    /\ dispatchQueue' = RemoveAll(dispatchQueue, {ReqFor(run, j) : j \in siblings})
    /\ pendingJobs' = RemoveAll(pendingJobs, {<<run, j>> : j \in siblings})
    /\ cancelFlipped' = cancelFlipped \cup {<<run, j>> : j \in siblings}
    /\ UNCHANGED <<now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   cancelQueue, inflightMsgs, msgIdNext, msgIdHigh,
                   mintInFlight, ghostJob, discardedCompletion, deliveredCancel,
                   runJobs, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   httpUp, outputBytes, setupFailed, completeReported,
                   renewAborted, signalled, cancelSentStep, maskSet, lines,
                   escapeBraces, scanState, formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * SkipJob: promote_ready_jobs Skip arm — the job is concluded Skipped and the
 * run finalized (runtime_scheduling.rs:858-875, finalize_run_if_complete
 * :1962-1975). *BUG F2*: release_concurrency_for_run is never called here
 * (single caller is cancel_run_inner, :151), so a Run holder whose jobs all
 * skip/eval-fail leaks its key — the group is permanently stuck.
 * ------------------------------------------------------------------------- *)
SkipJob(run, job) ==
    /\ runStatus[run] /= NoStatus
    /\ job \in runJobs[run]
    /\ <<run, job>> \in Range(pendingJobs)
    /\ pendingJobs' = RemoveElem(pendingJobs, <<run, job>>)
    /\ LET newSt == [jobStatus[run] EXCEPT ![job] = StSkipped] IN
       /\ jobStatus' = [jobStatus EXCEPT ![run] = newSt]
       /\ runStatus' = [runStatus EXCEPT ![run] = SummarizeRun(run, newSt)]
    \* The real promote Skip arm settles the job WITHOUT releasing its
    \* concurrency gate (runtime_scheduling.rs:858-875 has no
    \* release_concurrency_for_job call) — faithfully modeled; the
    \* TerminalRunReleasesKeys violation is a genuine preloop bug.
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, cancelQueue, inflightMsgs, msgIdNext,
                   msgIdHigh, mintInFlight, ghostJob, discardedCompletion,
                   deliveredCancel, runJobs, groups, heldRuns, blockedJobs,
                   holderKeys, jobsetAdm, jobsetReady, pendingExp, expanding,
                   jobAssign, poolPending, hasGate, gateHeld, fanoutReqs,
                   listenerJob, processed, parsed, acked, workerReported,
                   forceFailed, stepGroupsAlive, steps, dirty, gen, pubGen,
                   changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * EvalFailJob: promote_ready_jobs Error arm — same leak as SkipJob
 * (runtime_scheduling.rs:858-875).
 * ------------------------------------------------------------------------- *)
EvalFailJob(run, job) ==
    /\ runStatus[run] /= NoStatus
    /\ job \in runJobs[run]
    /\ <<run, job>> \in Range(pendingJobs)
    /\ pendingJobs' = RemoveElem(pendingJobs, <<run, job>>)
    /\ LET newSt == [jobStatus[run] EXCEPT ![job] = StFailure] IN
       /\ jobStatus' = [jobStatus EXCEPT ![run] = newSt]
       /\ runStatus' = [runStatus EXCEPT ![run] = SummarizeRun(run, newSt)]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, cancelQueue, inflightMsgs, msgIdNext,
                   msgIdHigh, mintInFlight, ghostJob, discardedCompletion,
                   deliveredCancel, runJobs, groups, heldRuns, blockedJobs,
                   holderKeys, jobsetAdm, jobsetReady, pendingExp, expanding,
                   jobAssign, poolPending, hasGate, gateHeld, fanoutReqs,
                   listenerJob, processed, parsed, acked, workerReported,
                   forceFailed, stepGroupsAlive, steps, dirty, gen, pubGen,
                   changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * CancelRun: cancel_run_inner — marks non-terminal jobs Cancelled, enqueues
 * JobCancellation for in-flight jobs, removes queues/held/blocked, releases
 * run concurrency (runtime_scheduling.rs:100-157). *BUG F5*: the run status
 * is overwritten with Cancelled even when already terminal (:110).
 * ------------------------------------------------------------------------- *)
CancelRun(run) ==
    /\ runStatus[run] /= NoStatus
    /\ LET live == {j \in runJobs[run] :
                jobStatus[run][j] \in {StQueued, StPending, StInProgress}} IN
       /\ jobStatus' = [jobStatus EXCEPT ![run] =
            [j \in JobId |-> IF j \in live THEN StCancelled ELSE jobStatus[run][j]]]
       /\ runStatus' = [runStatus EXCEPT ![run] = StCancelled]
       /\ cancelQueue' = AppendAll(cancelQueue,
            {CancelRec(run, j) : j \in live})
       /\ dispatchQueue' = RemoveAll(dispatchQueue,
            {ReqFor(run, j) : j \in live})
       /\ pendingJobs' = RemoveAll(pendingJobs, {<<run, j>> : j \in live})
       /\ blockedJobs' = RemoveAll(blockedJobs, {<<run, j>> : j \in live})
       /\ heldRuns' = [heldRuns EXCEPT ![run] = {}]
       /\ pendingExp' = RemoveAll(pendingExp, {<<run, j>> : j \in runJobs[run]})
       /\ expanding' = expanding \ {<<run, j>> : j \in runJobs[run]}
       /\ holderKeys' = [holderKeys EXCEPT ![run] = {}]
       /\ cancelFlipped' = cancelFlipped \cup {<<run, j>> : j \in live}
       /\ groups' = [k \in Key |->
            IF k \in holderKeys[run]
            THEN [running |-> IF groups[k].running /= NoHolder /\ groups[k].running.run = run
                              THEN NoHolder ELSE groups[k].running,
                  pending |-> RemoveAll(groups[k].pending,
                              {h \in Range(groups[k].pending) : h.run = run})]
            ELSE groups[k]]
    /\ UNCHANGED <<now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   inflightMsgs, msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runJobs, jobsetAdm,
                   jobsetReady, jobAssign, poolPending, hasGate, gateHeld,
                   fanoutReqs, listenerJob, processed, parsed, acked,
                   workerReported, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * CancelJob: cancel_job_inner — cancels a single job and releases its
 * concurrency holder (runtime_scheduling.rs:160-228).
 * ------------------------------------------------------------------------- *)
CancelJob(run, job) ==
    /\ runStatus[run] /= NoStatus
    /\ job \in runJobs[run]
    /\ jobStatus[run][job] \in {StQueued, StPending, StInProgress}
    /\ LET newStatus == [jobStatus[run] EXCEPT ![job] = StCancelled]
           allTerminal == \A j \in runJobs[run] : IsTerminal(newStatus[j])
           \* release_concurrency_for_job (runtime_scheduling.rs:259-310):
           \* for every group — drop pending holders containing this job,
           \* release the running holder if it contains this job (Job holder
           \* immediately; Run/JobSet when the run is entirely terminal),
           \* then C-07 prune holder_keys by remaining presence.
           newGroups == [k \in Key |->
                LET g == groups[k]
                    pend == RemoveAll(g.pending,
                             {h \in Range(g.pending) : HoldsJob(h, run, job)})
                IN IF g.running /= NoHolder /\ HoldsJob(g.running, run, job)
                   THEN IF g.running.kind = HolderJob \/ allTerminal
                        THEN [running |-> NoHolder, pending |-> pend]
                        ELSE [running |-> g.running, pending |-> pend]
                   ELSE [running |-> g.running, pending |-> pend]]
           present(g, r) == (g.running /= NoHolder /\ g.running.run = r)
                            \/ \E h \in Range(g.pending) : h.run = r
       IN
       /\ jobStatus' = [jobStatus EXCEPT ![run] = newStatus]
       /\ runStatus' = [runStatus EXCEPT ![run] = SummarizeRun(run, newStatus)]
       /\ cancelQueue' = Append(cancelQueue, CancelRec(run, job))
       /\ \E req \in RequestId : reqInfo[req].run = run /\ reqInfo[req].job = job
          /\ dispatchQueue' = RemoveElem(dispatchQueue, req)
       /\ pendingJobs' = RemoveElem(pendingJobs, <<run, job>>)
       /\ blockedJobs' = RemoveElem(blockedJobs, <<run, job>>)
       /\ gateHeld' = gateHeld \ {JobRef(run, job)}
       /\ cancelFlipped' = cancelFlipped \cup {<<run, job>>}
       /\ groups' = newGroups
       /\ holderKeys' = [r \in RunId |->
              IF r /= run THEN holderKeys[r]
              ELSE {k \in holderKeys[run] : present(newGroups[k], run)}]
    /\ UNCHANGED <<acked, agentJobReq, brokerMsg, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gen, ghostJob, hasGate, heldRuns, httpUp, inflightMsgs, inflightReq, jobAssign, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, planReq, poolPending, processed, pubGen, renewAborted, reqInfo, runJobs, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * ReleaseJob: release_concurrency_for_job — Job holders release immediately;
 * Run/JobSet only when the holder's jobs are all terminal
 * (runtime_scheduling.rs:259-310).
 * ------------------------------------------------------------------------- *)
ReleaseJob(run, job) ==
    /\ runStatus[run] /= NoStatus
    /\ job \in runJobs[run]
    /\ \E k \in holderKeys[run] :
        /\ groups[k].running /= NoHolder
        /\ HoldsJob(groups[k].running, run, job)
        /\ groups[k].running.kind = HolderJob
        /\ groups' = [groups EXCEPT ![k] =
                [running |-> NoHolder, pending |-> groups[k].pending]]
        /\ holderKeys' = [holderKeys EXCEPT ![run] = holderKeys[run] \ {k}]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, heldRuns, blockedJobs, jobsetAdm, jobsetReady,
                   pendingExp, expanding, jobAssign, poolPending, hasGate,
                   gateHeld, fanoutReqs, listenerJob, processed, parsed, acked,
                   workerReported, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * ReleaseRun: release_concurrency_for_run — only invoked from cancel_run_inner
 * (runtime_scheduling.rs:230-257; single production caller :151). *BUG F2*: a
 * run reaching terminal via skips/eval-failures never fires this, leaking the
 * holder key and permanently sticking the group.
 * ------------------------------------------------------------------------- *)
ReleaseRun(run) ==
    /\ runStatus[run] /= NoStatus
    /\ holderKeys[run] /= {}
    /\ \E k \in holderKeys[run] :
        /\ groups[k].running /= NoHolder
        /\ groups[k].running.run = run
        /\ groups' = [groups EXCEPT ![k] =
                [running |-> NoHolder, pending |-> groups[k].pending]]
        /\ holderKeys' = [holderKeys EXCEPT ![run] = holderKeys[run] \ {k}]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, heldRuns, blockedJobs, jobsetAdm, jobsetReady,
                   pendingExp, expanding, jobAssign, poolPending, hasGate,
                   gateHeld, fanoutReqs, listenerJob, processed, parsed, acked,
                   workerReported, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* =========================================================================
 * SCENARIO 3 — DEFERRED EXPANSION & CORRELATION REGISTRATION
 * ========================================================================= *)

(* -------------------------------------------------------------------------
 * DeferExpansion: defer_expansion — record the expansion intent under the
 * `expanding` reservation (runtime_scheduling.rs:1448-1451).
 * ------------------------------------------------------------------------- *)
DeferExpansion(run, job) ==
    /\ runStatus[run] /= NoStatus
    /\ job \in runJobs[run]
    \* defer_expansion is reached only from the promote_ready_jobs drain loop
    \* (runtime_scheduling.rs:833, :840), which pops the job out of
    \* pending_jobs before handing it off. Consume the entry here so dispatch
    \* (PromoteDispatchJob) and defer are mutually exclusive, as in the real
    \* code; otherwise ApplyMatrix could remove a node from runJobs while its
    \* stale dispatch req stays queued (spurious DispatchQueueBelongs
    \* violation).
    /\ <<run, job>> \in Range(pendingJobs)
    /\ pendingJobs' = RemoveElem(pendingJobs, <<run, job>>)
    /\ ~(<<run, job>> \in expanding)
    /\ ~(<<run, job>> \in Range(pendingExp))
    /\ expanding' = expanding \cup {<<run, job>>}
    /\ pendingExp' = Append(pendingExp, <<run, job>>)
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, jobAssign, poolPending, hasGate,
                   gateHeld, fanoutReqs, listenerJob, processed, parsed, acked,
                   workerReported, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * BuildExpansionStart: drain_expansions phase 1 — pop one pending expansion
 * (runtime_scheduling.rs:1929-1941). Phase 1 only pops pending_expansions;
 * the `expanding` reservation is dropped later in phase 3 (apply_expansion
 * / fail_expansion_node), so ApplyMatrix/ApplyReusable/BuildExpansionFail
 * keep their `\in expanding` guard.
 * ------------------------------------------------------------------------- *)
BuildExpansionStart(run, job) ==
    /\ pendingExp /= <<>>
    /\ Head(pendingExp) = <<run, job>>
    /\ pendingExp' = Tail(pendingExp)
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, brokerMsg, cancelFlipped, cancelQueue, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, dispatchQueue, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, heldRuns, holderKeys, httpUp, inflightMsgs, inflightReq, jobAssign, jobStatus, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingJobs, planReq, poolPending, processed, pubGen, renewAborted, reqInfo, runJobs, runStatus, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * ApplyMatrix: apply_expansion Matrix arm — the reservation is the proof the
 * expansion is still wanted; cancellation drops it so a build finishing after
 * cancellation must not resurrect the subtree (runtime_scheduling.rs:1791-1833,
 * register_expanded_jobs :1683-1767). A positive invariant (regression guard,
 * brief 3.2): every fan-out job is fully registered in the correlation maps.
 * NOTE: the placeholder node is REMOVED from run.jobs (:1827) while its
 * submit-time request record keeps result=None forever *BUG F-4*.
 * ------------------------------------------------------------------------- *)
ApplyMatrix(run, node, fanout) ==
    /\ <<run, node>> \in expanding
    /\ fanout \subseteq JobId /\ fanout /= {}
    /\ \A j \in fanout : ~HasReq(run, j)
    /\ LET n == Cardinality(fanout)
           fresh == {req \in RequestId : reqInfo[req].state = SNone}
       IN
       /\ Cardinality(fresh) >= n
       /\ expanding' = expanding \ {<<run, node>>}
       /\ \E newIds \in SUBSET fresh :
            /\ Cardinality(newIds) = n
            /\ \E f \in [newIds -> fanout] :
                /\ \A i, j \in newIds : i /= j => f[i] /= f[j]
                /\ reqInfo' = [r \in RequestId |->
                     IF r \in newIds
                     THEN [reqInfo[r] EXCEPT
                         !.run = run, !.job = f[r],
                         !.state = SQueued, !.result = RNone]
                     ELSE reqInfo[r]]
                /\ inflightReq' = inflightReq \cup newIds
                /\ planReq' = [r \in RequestId |->
                     IF r \in newIds THEN r ELSE planReq[r]]
                /\ timelineReq' = [r \in RequestId |->
                     IF r \in newIds THEN r ELSE timelineReq[r]]
                /\ agentJobReq' = [j \in JobId |->
                     IF \E r \in newIds : f[r] = j
                     THEN CHOOSE r \in newIds : f[r] = j
                     ELSE agentJobReq[j]]
                /\ fanoutReqs' = fanoutReqs \cup newIds
                /\ runJobs' = [runJobs EXCEPT ![run] =
                     (runJobs[run] \ {node}) \cup fanout]
                /\ jobStatus' = [jobStatus EXCEPT ![run] =
                     [j \in JobId |->
                        IF j \in fanout \/ j = node THEN StQueued
                        ELSE jobStatus[run][j]]]
                \* The placeholder is removed from runJobs (runtime_scheduling.rs:1892).
                \* Prune its held pair so heldRuns references only live run jobs;
                \* the real holder is Holder::Run(run_id), which stores no job refs.
                /\ heldRuns' = [heldRuns EXCEPT ![run] = heldRuns[run] \ {<<run, node>>}]
    /\ UNCHANGED <<cancelFlipped, now, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, groups,
                   blockedJobs, holderKeys, jobsetAdm, jobsetReady,
                   pendingExp, jobAssign, poolPending, hasGate, gateHeld,
                   listenerJob, processed, parsed, acked, workerReported,
                   forceFailed, stepGroupsAlive, steps, dirty, gen, pubGen,
                   changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * ApplyReusable: apply_expansion Reusable arm — the caller placeholder node is
 * marked InProgress and the callee subtree registered (runtime_scheduling.rs:
 * 1834-1853).
 * ------------------------------------------------------------------------- *)
ApplyReusable(run, caller, fanout) ==
    /\ <<run, caller>> \in expanding
    /\ fanout \subseteq JobId /\ fanout /= {}
    /\ \A j \in fanout : ~HasReq(run, j)
    /\ LET n == Cardinality(fanout)
           fresh == {req \in RequestId : reqInfo[req].state = SNone}
       IN
       /\ Cardinality(fresh) >= n
       /\ expanding' = expanding \ {<<run, caller>>}
       /\ \E newIds \in SUBSET fresh :
            /\ Cardinality(newIds) = n
            /\ \E f \in [newIds -> fanout] :
                /\ \A i, j \in newIds : i /= j => f[i] /= f[j]
                /\ reqInfo' = [r \in RequestId |->
                     IF r \in newIds
                     THEN [reqInfo[r] EXCEPT
                         !.run = run, !.job = f[r],
                         !.state = SQueued, !.result = RNone]
                     ELSE reqInfo[r]]
                /\ inflightReq' = inflightReq \cup newIds
                /\ planReq' = [r \in RequestId |->
                     IF r \in newIds THEN r ELSE planReq[r]]
                /\ timelineReq' = [r \in RequestId |->
                     IF r \in newIds THEN r ELSE timelineReq[r]]
                /\ agentJobReq' = [j \in JobId |->
                     IF \E r \in newIds : f[r] = j
                     THEN CHOOSE r \in newIds : f[r] = j
                     ELSE agentJobReq[j]]
                /\ fanoutReqs' = fanoutReqs \cup newIds
                /\ runJobs' = [runJobs EXCEPT ![run] = runJobs[run] \cup fanout]
                /\ jobStatus' = [jobStatus EXCEPT ![run] =
                     [j \in JobId |->
                        IF j \in fanout THEN StQueued
                        ELSE IF j = caller THEN StInProgress
                        ELSE jobStatus[run][j]]]
    /\ UNCHANGED <<cancelFlipped, now, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, groups,
                   heldRuns, blockedJobs, holderKeys, jobsetAdm, jobsetReady,
                   pendingExp, jobAssign, poolPending, hasGate, gateHeld,
                   listenerJob, processed, parsed, acked, workerReported,
                   forceFailed, stepGroupsAlive, steps, dirty, gen, pubGen,
                   changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * BuildExpansionFail: expansion build failed -> fail_expansion_node concludes
 * the node and releases its JobSet gates (runtime_scheduling.rs:1774-1788,
 * :1805-1810).
 * ------------------------------------------------------------------------- *)
BuildExpansionFail(run, node) ==
    /\ <<run, node>> \in expanding
    /\ expanding' = expanding \ {<<run, node>>}
    /\ jobStatus' = [jobStatus EXCEPT ![run] =
            [jobStatus[run] EXCEPT ![node] = StFailure]]
    /\ runStatus' = [runStatus EXCEPT ![run] = StFailure]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runJobs, groups,
                   heldRuns, blockedJobs, holderKeys, jobsetAdm, jobsetReady,
                   pendingExp, jobAssign, poolPending, hasGate, gateHeld,
                   fanoutReqs, listenerJob, processed, parsed, acked,
                   workerReported, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* =========================================================================
 * SCENARIO 4 — RUNNER CANCEL -> KILL SEQUENCING
 * ========================================================================= *)

(* -------------------------------------------------------------------------
 * ListenerDedup: in-memory dedup — the processed set insert happens BEFORE
 * body parse (broker_listener.rs:435-438). A message whose id was already
 * processed is skipped with a 500 ms sleep.
 * ------------------------------------------------------------------------- *)
ListenerDedup(s, m) ==
    /\ m \in inflightMsgs[s]
    /\ IF m \in processed[s]
       THEN UNCHANGED processed
       ELSE processed' = [processed EXCEPT ![s] = processed[s] \cup {m}]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   parsed, acked, workerReported, forceFailed, stepGroupsAlive,
                   steps, dirty, gen, pubGen, changeOrder, httpUp, outputBytes,
                   setupFailed, completeReported, renewAborted, signalled,
                   cancelSentStep, maskSet, lines, escapeBraces, scanState,
                   formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * ListenerParse: decrypt/parse the message body (broker_listener.rs:448-458).
 * *BUG F-4*: parse failure `continue`s without acknowledging — the message is
 * deduped-but-unacked and re-delivery is skipped forever (silent drop).
 * ------------------------------------------------------------------------- *)
ListenerParse(s, m, parseOk) ==
    /\ m \in inflightMsgs[s]
    /\ m \in processed[s]
    /\ IF parseOk
       THEN parsed' = [parsed EXCEPT ![s] = parsed[s] \cup {m}]
       ELSE UNCHANGED parsed
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   httpUp, outputBytes, setupFailed, completeReported,
                   renewAborted, signalled, cancelSentStep, maskSet, lines,
                   escapeBraces, scanState, formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * ListenerAckMsg: acknowledge the message to the server (broker_listener.rs:
 * 470-489).
 * ------------------------------------------------------------------------- *)
ListenerAckMsg(s, m) ==
    /\ m \in inflightMsgs[s]
    /\ m \in parsed[s]
    /\ acked' = [acked EXCEPT ![s] = acked[s] \cup {m}]
    /\ inflightMsgs' = [inflightMsgs EXCEPT ![s] = inflightMsgs[s] \ {m}]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, msgIdNext,
                   msgIdHigh, mintInFlight, ghostJob, discardedCompletion,
                   deliveredCancel, runStatus, runJobs, jobStatus, groups,
                   heldRuns, blockedJobs, holderKeys, jobsetAdm, jobsetReady,
                   pendingExp, expanding, jobAssign, poolPending, hasGate,
                   gateHeld, fanoutReqs, listenerJob, processed, parsed,
                   workerReported, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * ListenerJobCancellation: JobCancellation handling (broker_listener.rs:563-
 * 608). *BUG F-6*: when the active job's id is not a valid UUID (`if let Some
 * (active_id)` guard, :583), the id match is bypassed and the cancel applies
 * to ANY active job (:600-604).
 * ------------------------------------------------------------------------- *)
ListenerJobCancellation(s, m, targetRun, targetJob, activeIdValid) ==
    /\ m \in inflightMsgs[s]
    /\ m \in parsed[s]
    /\ listenerJob[s] /= NoListener
    /\ LET j == listenerJob[s] IN
       /\ (IF activeIdValid
           THEN j.job = targetJob
           ELSE TRUE)
       /\ listenerJob' = [listenerJob EXCEPT ![s] = [j EXCEPT
                !.cancelSent = TRUE, !.killAt = now + 1]]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, processed,
                   parsed, acked, workerReported, forceFailed, stepGroupsAlive,
                   steps, dirty, gen, pubGen, changeOrder, httpUp, outputBytes,
                   setupFailed, completeReported, renewAborted, signalled,
                   cancelSentStep, maskSet, lines, escapeBraces, scanState,
                   formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * ListenerRunnerJobRequest: a RunnerJobRequest while busy — the previous
 * worker is cancelled immediately and killed after the window
 * (broker_listener.rs:491-533). *BUG F-5*: the overlap-kill path omits
 * force_fail_job for the killed previous job (:493-518).
 * ------------------------------------------------------------------------- *)
ListenerRunnerJobRequest(s, m, newReq) ==
    /\ m \in inflightMsgs[s]
    /\ m \in parsed[s]
    /\ listenerJob[s] /= NoListener
    /\ reqInfo[newReq].state /= SNone
    /\ workerReported' = [workerReported EXCEPT ![s] = FALSE]
    /\ forceFailed' = [forceFailed EXCEPT ![s] = FALSE]
    /\ listenerJob' = [listenerJob EXCEPT ![s] =
            [req |-> newReq, run |-> reqInfo[newReq].run,
             job |-> reqInfo[newReq].job, workerAlive |-> TRUE,
             cancelSent |-> FALSE, killAt |-> NoTime,
             shutdownSrc |-> OverlapShutdown]]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, processed,
                   parsed, acked, stepGroupsAlive, steps, dirty, gen, pubGen,
                   changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * ListenerRunnerShutdown: RunnerShutdown broker message returns Ok(()) with
 * NO graceful worker shutdown (broker_listener.rs:623-630). *BUG F-1*: the
 * active worker is orphaned and the session is deleted underneath it.
 * ------------------------------------------------------------------------- *)
ListenerRunnerShutdown(s, m) ==
    /\ m \in inflightMsgs[s]
    /\ m \in parsed[s]
    /\ listenerJob[s] /= NoListener
    /\ LET j == listenerJob[s] IN
       /\ listenerJob' = [listenerJob EXCEPT ![s] = [j EXCEPT
                !.shutdownSrc = MsgShutdown]]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, processed,
                   parsed, acked, workerReported, forceFailed, stepGroupsAlive,
                   steps, dirty, gen, pubGen, changeOrder, httpUp, outputBytes,
                   setupFailed, completeReported, renewAborted, signalled,
                   cancelSentStep, maskSet, lines, escapeBraces, scanState,
                   formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * ListenerShutdownSignal: external shutdown signal — the worker is told to
 * cancel the job and wrap up within a 60 s grace; kill only if it ignores
 * (broker_listener.rs:354-388, job_dispatcher.rs shutdown_gracefully
 * :269-294).
 * ------------------------------------------------------------------------- *)
ListenerShutdownSignal(s) ==
    /\ listenerJob[s] /= NoListener
    /\ LET j == listenerJob[s] IN
       /\ listenerJob' = [listenerJob EXCEPT ![s] = [j EXCEPT
                !.cancelSent = TRUE, !.shutdownSrc = SigShutdown,
                !.killAt = now + 1]]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, processed,
                   parsed, acked, workerReported, forceFailed, stepGroupsAlive,
                   steps, dirty, gen, pubGen, changeOrder, httpUp, outputBytes,
                   setupFailed, completeReported, renewAborted, signalled,
                   cancelSentStep, maskSet, lines, escapeBraces, scanState,
                   formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * ListenerKillTimerFires: cancel grace expired — job.kill() SIGKILLs only the
 * worker PID (broker_listener.rs:418-424, job_dispatcher.rs:297-303).
 * *BUG F-2*: steps were spawned with command_group::group_spawn
 * (process.rs:104), so the step process groups survive the kill.
 * ------------------------------------------------------------------------- *)
ListenerKillTimerFires(s) ==
    /\ listenerJob[s] /= NoListener
    /\ listenerJob[s].killAt /= NoTime
    /\ now >= listenerJob[s].killAt
    /\ LET j == listenerJob[s] IN
       /\ listenerJob' = [listenerJob EXCEPT ![s] = [j EXCEPT
                !.workerAlive = FALSE, !.killAt = NoTime]]
       /\ stepGroupsAlive' = stepGroupsAlive
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, processed,
                   parsed, acked, workerReported, forceFailed, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * WorkerExits: the worker process exits — with or without a completion report
 * (broker_listener.rs:393-416). *BUG F-5*: on the overlap path there is no
 * force-fail for the killed previous job; *BUG F-15*: a non-crashed worker
 * whose completejob POST was lost exits 0 and the listener never force-fails.
 * ------------------------------------------------------------------------- *)
WorkerExits(s, reported) ==
    /\ listenerJob[s] /= NoListener
    /\ LET j == listenerJob[s] IN
       /\ workerReported' = [workerReported EXCEPT ![s] = reported]
       /\ listenerJob' = [listenerJob EXCEPT ![s] = NoListener]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, processed,
                   parsed, acked, forceFailed, stepGroupsAlive, steps, dirty,
                   gen, pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * ListenerForceFail: force_fail_job posts a Failure completion for a worker
 * that died without reporting (broker_listener.rs:54-121). The POST failure
 * itself is swallowed (warn + Ok) *BUG F-15*.
 * ------------------------------------------------------------------------- *)
ListenerForceFail(s, postOk) ==
    /\ listenerJob[s] = NoListener
    /\ ~workerReported[s]
    /\ forceFailed' = [forceFailed EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, stepGroupsAlive,
                   steps, dirty, gen, pubGen, changeOrder, httpUp, outputBytes,
                   setupFailed, completeReported, renewAborted, signalled,
                   cancelSentStep, maskSet, lines, escapeBraces, scanState,
                   formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * WorkerCompletePost: the worker's single-shot completejob POST — a lost or
 * failed POST is swallowed and the worker still exits 0 (completion.rs:347-467)
 * *BUG F-15*.
 * ------------------------------------------------------------------------- *)
WorkerCompletePost(s, outcome, ok) ==
    /\ listenerJob[s] /= NoListener
    /\ completeReported' = ok
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   httpUp, outputBytes, setupFailed, renewAborted, signalled,
                   cancelSentStep, maskSet, lines, escapeBraces, scanState,
                   formatError, scannerDiverged>>

(* =========================================================================
 * SCENARIO 5 — WORKER STEP EXECUTION & REPORTING QUEUE
 * ========================================================================= *)

(* -------------------------------------------------------------------------
 * WorkerQueueUpdate: queue_update merges a step transition into all_steps and
 * marks the step dirty, bumping steps_generation (server_queue.rs:133-152).
 * A step that starts or terminates also joins/leaves the live process-group
 * set (process.rs:104 group_spawn).
 * ------------------------------------------------------------------------- *)
WorkerQueueUpdate(st, status, conclusion) ==
    /\ st \in StepId
    /\ steps' = [steps EXCEPT ![st] = [status |-> status,
                conclusion |-> conclusion, killCause |-> steps[st].killCause]]
    /\ dirty' = dirty \cup {st}
    /\ gen' = gen + 1
    /\ stepGroupsAlive' =
        IF status = StInProgress
        THEN stepGroupsAlive \cup {st}
        ELSE IF IsTerminal(status)
             THEN stepGroupsAlive \ {st}
             ELSE stepGroupsAlive
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   pubGen, changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* -------------------------------------------------------------------------
 * WorkerTakeBody: take_steps_update_body CLEARS dirty_keys at take time and
 * returns (body, generation); the generation is only marked published on HTTP
 * success (server_queue.rs:164-186). *BUG F1*: a failed POST leaves
 * steps_generation != published_generation with empty dirty_keys, so the next
 * take returns None and the terminal transition is lost forever (change_order
 * also desyncs).
 * ------------------------------------------------------------------------- *)
WorkerTakeBody ==
    /\ gen /= pubGen
    /\ dirty' = {}
    /\ changeOrder' = changeOrder + 1
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, gen, pubGen, httpUp, outputBytes,
                   setupFailed, completeReported, renewAborted, signalled,
                   cancelSentStep, maskSet, lines, escapeBraces, scanState,
                   formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * WorkerPublishOk: mark_steps_published advances published_generation on
 * success (reporting.rs:75-77).
 * ------------------------------------------------------------------------- *)
WorkerPublishOk ==
    /\ httpUp = TRUE
    /\ pubGen' = gen
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, changeOrder, httpUp,
                   outputBytes, setupFailed, completeReported, renewAborted,
                   signalled, cancelSentStep, maskSet, lines, escapeBraces,
                   scanState, formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * WorkerPublishFail: the WorkflowStepsUpdate POST fails — published_generation
 * is NOT advanced and dirty_keys were already cleared (reporting.rs:68-71 +
 * server_queue.rs:176). *BUG F1*: the queued transition is lost.
 * ------------------------------------------------------------------------- *)
WorkerPublishFail ==
    /\ httpUp = FALSE
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   httpUp, outputBytes, setupFailed, completeReported,
                   renewAborted, signalled, cancelSentStep, maskSet, lines,
                   escapeBraces, scanState, formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * HttpFlap: fault injection — the WorkflowStepsUpdate endpoint is down/up.
 * ------------------------------------------------------------------------- *)
HttpFlap(v) ==
    /\ httpUp' = v
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   outputBytes, setupFailed, completeReported, renewAborted,
                   signalled, cancelSentStep, maskSet, lines, escapeBraces,
                   scanState, formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * WorkerCancelStep: cancel signal delivered to a step. *BUG F2*: docker
 * actions call process::invoke with no cancel channel (handlers/action.rs:
 * 29-37, container.rs:55-71,114-123), so cancelArmed=FALSE and the child
 * process group is never signalled.
 * ------------------------------------------------------------------------- *)
WorkerCancelStep(st, cancelArmed) ==
    /\ st \in StepId
    /\ cancelSentStep' = cancelSentStep \cup {st}
    /\ IF cancelArmed
       THEN signalled' = signalled \cup {st}
       ELSE UNCHANGED signalled
    /\ steps' = [steps EXCEPT ![st] = [steps[st] EXCEPT
                !.killCause = IF steps[st].killCause = NoKill THEN KillCancel
                              ELSE KillBoth]]
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, brokerMsg, cancelFlipped, cancelQueue, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, dispatchQueue, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, heldRuns, holderKeys, httpUp, inflightMsgs, inflightReq, jobAssign, jobStatus, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, poolPending, processed, pubGen, renewAborted, reqInfo, runJobs, runStatus, scanState, scannerDiverged, sessionActive, sessionRunner, setupFailed, stepGroupsAlive, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * WorkerStepTimeout: step timeout fires — timed_out flag set, then the step is
 * forced to Err (steps_runner.rs:607-679). killCause = KillTimeout.
 * ------------------------------------------------------------------------- *)
WorkerStepTimeout(st) ==
    /\ st \in StepId
    /\ steps' = [steps EXCEPT ![st] = [steps[st] EXCEPT
                !.killCause = IF steps[st].killCause = NoKill THEN KillTimeout
                              ELSE KillBoth]]
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, dirty, gen, pubGen, changeOrder, httpUp,
                   outputBytes, setupFailed, completeReported, renewAborted,
                   signalled, cancelSentStep, maskSet, lines, escapeBraces,
                   scanState, formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * WorkerConcludeStep: the classifier checks cancel_signaled BEFORE the
 * timeout error (steps_runner.rs:687-698). *BUG F8*: a job-level cancel that
 * lands in the window makes a definitively timed-out step conclude Cancelled.
 * ------------------------------------------------------------------------- *)
WorkerConcludeStep(st, cancelSignaled) ==
    /\ st \in StepId
    /\ IF cancelSignaled /\ steps[st].killCause /= NoKill
       THEN steps' = [steps EXCEPT ![st] = [steps[st] EXCEPT
                !.status = StInProgress, !.conclusion = RCancelled]]
       ELSE IF steps[st].killCause = KillTimeout /\ ~cancelSignaled
            THEN steps' = [steps EXCEPT ![st] = [steps[st] EXCEPT
                     !.status = StInProgress, !.conclusion = RFailure]]
            ELSE UNCHANGED steps
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, dirty, gen, pubGen, changeOrder, httpUp,
                   outputBytes, setupFailed, completeReported, renewAborted,
                   signalled, cancelSentStep, maskSet, lines, escapeBraces,
                   scanState, formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * WorkerSetupWorkspace: setup_workspace failure propagates with `?` BEFORE
 * the reporting context exists (job_runner.rs:134) — no completejob, no
 * abandoned record; the server holds the job InProgress until lease expiry.
 * *BUG F3*.
 * ------------------------------------------------------------------------- *)
WorkerSetupWorkspace(ok) ==
    /\ setupFailed' = ~ok
    /\ completeReported' = FALSE
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   httpUp, outputBytes, renewAborted, signalled,
                   cancelSentStep, maskSet, lines, escapeBraces, scanState,
                   formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * WorkerRenewLoopAbort: the renew loop is aborted before the completion tail
 * (job_runner.rs:583-586, 706-714) — DAP pause can block indefinitely, the
 * final flush is skipped, completejob is single-shot with swallowed failures.
 * *BUG F5*: the server can end permanently InProgress.
 * ------------------------------------------------------------------------- *)
WorkerRenewLoopAbort ==
    /\ renewAborted' = TRUE
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   httpUp, outputBytes, setupFailed, completeReported,
                   signalled, cancelSentStep, maskSet, lines, escapeBraces,
                   scanState, formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * WorkerFlushOutput: apply_file_commands inserts step outputs; the 1 MiB cap
 * (`output_size_utf16` tracking) was removed by 7833d087 *BUG F4* — modeled
 * as unbounded growth.
 * ------------------------------------------------------------------------- *)
WorkerFlushOutput(bytes) ==
    /\ bytes \in 0..MaxOutputBytes
    /\ outputBytes' = outputBytes + bytes
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   httpUp, setupFailed, completeReported, renewAborted,
                   signalled, cancelSentStep, maskSet, lines, escapeBraces,
                   scanState, formatError, scannerDiverged>>

(* =========================================================================
 * SCENARIO 6 — SECRET MASKING & TEMPLATE TOKENIZATION
 * ========================================================================= *)

(* -------------------------------------------------------------------------
 * AddMask: a secret joins the mask set (masking.rs:14-36, SecretString
 * expose). The mask set only ever grows (monotone).
 * ------------------------------------------------------------------------- *)
AddMask(secret) ==
    /\ secret \in Str
    /\ maskSet' = maskSet \cup {secret}
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   httpUp, outputBytes, setupFailed, completeReported,
                   renewAborted, signalled, cancelSentStep, lines, escapeBraces,
                   scanState, formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * EmitLine: an output line is emitted after longest-first masking. A correct
 * run never emits a plaintext secret.
 * ------------------------------------------------------------------------- *)
EmitLine(secret, masked) ==
    /\ secret \in maskSet
    /\ lines' = Append(lines, IF masked THEN "***" ELSE secret)
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   httpUp, outputBytes, setupFailed, completeReported,
                   renewAborted, signalled, cancelSentStep, maskSet,
                   escapeBraces, scanState, formatError, scannerDiverged>>

(* -------------------------------------------------------------------------
 * BuildFormat: format-builder escapes single quotes always; the parser copy
 * also escapes '{' and '}' (job_builder.rs:126-135 append_format_literal),
 * the protocol copy escapes only '\'' (azdo/job.rs:579 template_string_token)
 * *BUG F1*. Any template mixing a literal brace with an expression then
 * produces `format('prefix { {0}', ...)` -> InvalidFormat on both runners.
 * ------------------------------------------------------------------------- *)
BuildFormat(hasLiteralBrace, hasExpr) ==
    /\ (IF ~escapeBraces /\ hasLiteralBrace /\ hasExpr
        THEN formatError' = TRUE
        ELSE UNCHANGED formatError)
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   httpUp, outputBytes, setupFailed, completeReported,
                   renewAborted, signalled, cancelSentStep, maskSet, lines,
                   escapeBraces, scanState, scannerDiverged>>

(* -------------------------------------------------------------------------
 * ScanStep: template-string transducer — advances scan state between Normal
 * and InString on quote runs (azdo/job.rs:586-613 find_expression_end vs the
 * official scanner). *BUG F3*: aksh treats `''` as an escape, the official
 * scanner toggles; divergence only on odd-length quote runs.
 * ------------------------------------------------------------------------- *)
ScanStep(oddQuoteRun) ==
    /\ (IF oddQuoteRun
        THEN /\ scanState' = IF scanState = Normal THEN InString ELSE Normal
             /\ scannerDiverged' = (scannerDiverged \/ (scanState = Normal))
        ELSE UNCHANGED <<scanState, scannerDiverged>>)
    /\ UNCHANGED <<acked, agentJobReq, blockedJobs, brokerMsg, cancelFlipped, cancelQueue, cancelSentStep, changeOrder, completeReported, deliveredCancel, dirty, discardedCompletion, dispatchQueue, escapeBraces, expanding, fanoutReqs, forceFailed, formatError, gateHeld, gen, ghostJob, groups, hasGate, heldRuns, holderKeys, httpUp, inflightMsgs, inflightReq, jobAssign, jobStatus, jobsetAdm, jobsetReady, lines, listenerJob, maskSet, mintInFlight, msgIdHigh, msgIdNext, now, outputBytes, parsed, pendingExp, pendingJobs, planReq, poolPending, processed, pubGen, renewAborted, reqInfo, runJobs, runStatus, sessionActive, sessionRunner, setupFailed, signalled, stepGroupsAlive, steps, timelineReq, workerReported>>

(* -------------------------------------------------------------------------
 * SetEscapeBraces: switch the format-builder copy (protocol vs parser).
 * escapeBraces = FALSE models the protocol crate copy *BUG F1*.
 * ------------------------------------------------------------------------- *)
SetEscapeBraces(v) ==
    /\ escapeBraces' = v
    /\ UNCHANGED <<cancelFlipped, now, reqInfo, inflightReq, planReq, agentJobReq,
                   timelineReq, brokerMsg, sessionActive, sessionRunner,
                   dispatchQueue, pendingJobs, cancelQueue, inflightMsgs,
                   msgIdNext, msgIdHigh, mintInFlight, ghostJob,
                   discardedCompletion, deliveredCancel, runStatus, runJobs,
                   jobStatus, groups, heldRuns, blockedJobs, holderKeys,
                   jobsetAdm, jobsetReady, pendingExp, expanding, jobAssign,
                   poolPending, hasGate, gateHeld, fanoutReqs, listenerJob,
                   processed, parsed, acked, workerReported, forceFailed,
                   stepGroupsAlive, steps, dirty, gen, pubGen, changeOrder,
                   httpUp, outputBytes, setupFailed, completeReported,
                   renewAborted, signalled, cancelSentStep, maskSet, lines,
                   scanState, formatError, scannerDiverged>>

(* =========================================================================
 * CRASH / RECOVERY
 * ========================================================================= *)

(* -------------------------------------------------------------------------
 * CrashServer: the control plane is in-memory — a restart loses all run
 * state. The reaper and lease interplay are the only recovery (design; the
 * spec models the reaper as the only path back to terminal).
 * ------------------------------------------------------------------------- *)
CrashServer ==
    \* brokerMsg lives in process memory (state.rs); a crash wipes it with
    \* everything else. Keeping it while resetting reqInfo creates a state
    \* unreachable in the real server (spurious BrokerMsgConsistent violation).
    /\ brokerMsg' = [req \in RequestId |-> 0]
    /\ reqInfo' = [req \in RequestId |-> ReqDefault(req)]
    /\ runStatus' = [r \in RunId |-> NoStatus]
    /\ runJobs' = [r \in RunId |-> {}]
    /\ jobStatus' = [r \in RunId |-> [j \in JobId |-> StQueued]]
    /\ dispatchQueue' = <<>>
    /\ pendingJobs' = <<>>
    /\ cancelQueue' = <<>>
    /\ inflightMsgs' = [s \in Session |-> {}]
    /\ sessionActive' = [s \in Session |-> NoReq]
    /\ inflightReq' = {}
    /\ mintInFlight' = {}
    /\ groups' = [k \in Key |-> [running |-> NoHolder, pending |-> <<>>]]
    /\ heldRuns' = [r \in RunId |-> {}]
    /\ blockedJobs' = <<>>
    /\ holderKeys' = [r \in RunId |-> {}]
    /\ pendingExp' = <<>>
    /\ expanding' = {}
    /\ cancelFlipped' = {}
    /\ UNCHANGED <<now, planReq, agentJobReq, timelineReq,
                   sessionRunner, msgIdNext, msgIdHigh, ghostJob,
                   discardedCompletion, deliveredCancel, jobsetAdm, jobsetReady,
                   jobAssign, poolPending, hasGate, gateHeld, fanoutReqs,
                   listenerJob, processed, parsed, acked, workerReported,
                   forceFailed, stepGroupsAlive, steps, dirty, gen, pubGen,
                   changeOrder, httpUp, outputBytes, setupFailed,
                   completeReported, renewAborted, signalled, cancelSentStep,
                   maskSet, lines, escapeBraces, scanState, formatError,
                   scannerDiverged>>

(* =========================================================================
 * NEXT
 * ========================================================================= *)

Stutter == UNCHANGED vars

Next ==
    \/ \E run \in RunId, jobs \in SUBSET JobId : SubmitRun2(run, jobs)
    \/ \E run \in RunId, job \in JobId : EnqueuePending(run, job)
    \/ \E run \in RunId, job \in JobId, key \in Key : DeclareGate(run, job, key)
    \/ \E s \in Session, m \in AllMsgIds : AzdoPollDeliverInflight(s, m)
    \/ \E s \in Session : AzdoPollPopCancel(s)
    \/ \E s \in Session, req \in RequestId : AzdoPollClaim(s, req)
    \/ \E s \in Session, req \in RequestId : BrokerRootClaim(s, req)
    \/ \E s \in Session, req \in RequestId : BrokerPoolRedeliver(s, req)
    \/ \E s \in Session, req \in RequestId, c \in CancelRecDomain :
        BrokerPollDeliverCancelScoped(s, req, c)
    \/ \E s \in Session, req \in RequestId, c \in CancelRecDomain :
        BrokerPollDeliverCancelPool(s, req, c)
    \/ \E s \in Session, req \in RequestId : BrokerPollCleanup(s, req)
    \/ \E s \in Session, req \in RequestId : AcquireJobStart(s, req)
    \/ \E req \in RequestId : AcquireJobMintOk(req)
    \/ \E req \in RequestId : AcquireJobMintFail(req)
    \/ \E s \in Session, req \in RequestId : RenewJob(s, req)
    \/ \E s \in Session, req \in RequestId, o \in {RSuccess, RFailure, RCancelled} :
        CompleteJobSetResult(s, req, o)
    \/ \E req \in RequestId, o \in {RSuccess, RFailure, RCancelled} :
        CompleteJobApply(req, o)
    \/ \E req \in RequestId : ReapLease(req)
    \/ \E req \in RequestId : ReapTimeout(req)
    \/ \E s \in Session, m \in AllMsgIds : AckMessage(s, m)
    \/ TimeAdvance
    \/ \E run \in RunId, key \in Key : ArriveRunFree(run, key)
    \/ \E run \in RunId, key \in Key, prevRun \in RunId : ArriveRunCIP(run, key, prevRun)
    \/ \E run \in RunId, key \in Key : ArriveRunPark(run, key)
    \/ \E run \in RunId, job \in JobId, key \in Key : ArriveJobFree(run, job, key)
    \/ \E run \in RunId, job \in JobId, key \in Key, prevRun \in RunId :
        ArriveJobCIP(run, job, key, prevRun)
    \/ \E run \in RunId, job \in JobId, key \in Key, mode \in {ModeSingle, ModeMax},
            ph \in HolderSet :
        ArriveJobPark(run, job, key, mode, ph)
    \/ \E key \in Key, run \in RunId : PromoteNextRunArm(key, run)
    \/ \E key \in Key, run \in RunId, job \in JobId, mp \in BOOLEAN :
        PromoteNextJobArm(key, run, job, mp)
    \/ \E run \in RunId, job \in JobId : PromoteDispatchJob(run, job)
    \/ \E run \in RunId, job \in JobId : PromoteReadyJob(run, job)
    \/ \E run \in RunId, failed \in JobId, siblings \in SUBSET JobId :
        FailFast(run, failed, siblings)
    \/ \E run \in RunId, job \in JobId : SkipJob(run, job)
    \/ \E run \in RunId, job \in JobId : EvalFailJob(run, job)
    \/ \E run \in RunId : CancelRun(run)
    \/ \E run \in RunId, job \in JobId : CancelJob(run, job)
    \/ \E run \in RunId, job \in JobId : ReleaseJob(run, job)
    \/ \E run \in RunId : ReleaseRun(run)
    \/ \E run \in RunId, job \in JobId : DeferExpansion(run, job)
    \/ \E run \in RunId, job \in JobId : BuildExpansionStart(run, job)
    \/ \E run \in RunId, node \in JobId, fanout \in SUBSET JobId :
        ApplyMatrix(run, node, fanout)
    \/ \E run \in RunId, caller \in JobId, fanout \in SUBSET JobId :
        ApplyReusable(run, caller, fanout)
    \/ \E run \in RunId, node \in JobId : BuildExpansionFail(run, node)
    \/ \E s \in Session, m \in AllMsgIds : ListenerDedup(s, m)
    \/ \E s \in Session, m \in AllMsgIds, ok \in BOOLEAN : ListenerParse(s, m, ok)
    \/ \E s \in Session, m \in AllMsgIds : ListenerAckMsg(s, m)
    \/ \E s \in Session, m \in AllMsgIds, tr \in RunId, tj \in JobId, valid \in BOOLEAN :
        ListenerJobCancellation(s, m, tr, tj, valid)
    \/ \E s \in Session, m \in AllMsgIds, nr \in RequestId :
        ListenerRunnerJobRequest(s, m, nr)
    \/ \E s \in Session, m \in AllMsgIds : ListenerRunnerShutdown(s, m)
    \/ \E s \in Session : ListenerShutdownSignal(s)
    \/ \E s \in Session : ListenerKillTimerFires(s)
    \/ \E s \in Session, rep \in BOOLEAN : WorkerExits(s, rep)
    \/ \E s \in Session, ok \in BOOLEAN : ListenerForceFail(s, ok)
    \/ \E s \in Session, o \in {RSuccess, RFailure, RCancelled}, ok \in BOOLEAN :
        WorkerCompletePost(s, o, ok)
    \/ \E st \in StepId, status \in Status, concl \in ResultSet :
        WorkerQueueUpdate(st, status, concl)
    \/ WorkerTakeBody
    \/ WorkerPublishOk
    \/ WorkerPublishFail
    \/ \E v \in BOOLEAN : HttpFlap(v)
    \/ \E st \in StepId, armed \in BOOLEAN : WorkerCancelStep(st, armed)
    \/ \E st \in StepId : WorkerStepTimeout(st)
    \/ \E st \in StepId, cs \in BOOLEAN : WorkerConcludeStep(st, cs)
    \/ \E ok \in BOOLEAN : WorkerSetupWorkspace(ok)
    \/ WorkerRenewLoopAbort
    \/ \E bytes \in 0..MaxChunk : WorkerFlushOutput(bytes)
    \/ \E secret \in Str : AddMask(secret)
    \/ \E secret \in Str, masked \in BOOLEAN : EmitLine(secret, masked)
    \/ \E lb \in BOOLEAN, ex \in BOOLEAN : BuildFormat(lb, ex)
    \/ \E odd \in BOOLEAN : ScanStep(odd)
    \/ \E v \in BOOLEAN : SetEscapeBraces(v)
    \/ CrashServer
    \/ Stutter

(* =========================================================================
 * SPECIFICATION
 * ========================================================================= *)

Spec == Init /\ [][Next]_vars

(* =========================================================================
 * INVARIANTS
 * ========================================================================= *)

(* -------------------------------------------------------------------------
 * Scenario 1 invariants
 * ------------------------------------------------------------------------- *)

\* A job is claimed by at most one session at a time; no double dispatch.
ClaimedExactlyOnce ==
    \A s1, s2 \in Session :
        s1 /= s2 /\ sessionActive[s1] /= NoReq /\ sessionActive[s2] /= NoReq
        => sessionActive[s1] /= sessionActive[s2]

\* Every queued cancellation is delivered to the session whose active job it
\* names, at most once, and not dropped by the runner dedup.
CancelDeliveredToOwner ==
    \A c \in Range(deliveredCancel) : c.matched /\ ~c.collided

\* A job whose claim was failed/reaped is never executed (AcquireEnd after
\* FailClaim impossible).
NoGhostExecution == ghostJob = {}

\* Final recorded outcome = runner-reported outcome — never silently dropped
\* by a prior terminal conclusion.
OutcomeIsRunnerReported == discardedCompletion = {}

(* -------------------------------------------------------------------------
 * Scenario 2 invariants
 * ------------------------------------------------------------------------- *)

\* Every acquired concurrency key is eventually released when its holder is
\* terminal (Run holders never leak into terminal states).
TerminalRunReleasesKeys ==
    \A run \in RunId :
        runStatus[run] /= NoStatus /\ IsTerminal(runStatus[run])
        => RunHoldsNoSlot(run) /\ holderKeys[run] = {}

\* A terminal run never occupies a group slot (safety fragment of the above).
NoTerminalRunHoldsSlot ==
    \A run \in RunId, k \in Key :
        runStatus[run] /= NoStatus /\ IsTerminal(runStatus[run])
        => ~(groups[k].running /= NoHolder /\ groups[k].running.run = run)

\* A job cancelled by fail-fast/cancel never enters the dispatch queue.
\* Uses the cancelFlipped history set because the resurrection bug overwrites
\* the Cancelled status with Queued before dispatching (MC6).
CancelledJobNeverDispatched ==
    \A req \in RequestId :
        reqInfo[req].state /= SNone /\ req \in Range(dispatchQueue)
        => ~(<<reqInfo[req].run, reqInfo[req].job>> \in cancelFlipped)

\* A job with job-level concurrency does not dispatch until its group slot is
\* acquired.
GateBeforeDispatch ==
    \A run \in RunId, job \in JobId :
        /\ job \in runJobs[run]
        /\ hasGate[run][job] /= NoKey
        /\ \E req \in Range(dispatchQueue) :
            reqInfo[req].run = run /\ reqInfo[req].job = job
        => <<run, job>> \in gateHeld

(* -------------------------------------------------------------------------
 * Scenario 3 invariants
 * ------------------------------------------------------------------------- *)

\* For every fan-out job the correlation maps are registered (holds at HEAD —
\* regression guard; the documented gap is closed by aafe5b77).
FanoutCorrelationRegistered ==
    \A req \in fanoutReqs :
        /\ planReq[req] = req
        /\ timelineReq[req] = req
        /\ agentJobReq[reqInfo[req].job] = req
        /\ req \in inflightReq

\* request.result = None => job in run.jobs — no orphan correlation records.
NoLeakedRequest ==
    \A req \in RequestId :
        reqInfo[req].state /= SNone /\ reqInfo[req].result = RNone
        => reqInfo[req].job \in runJobs[reqInfo[req].run]

(* -------------------------------------------------------------------------
 * Scenario 4 invariants
 * ------------------------------------------------------------------------- *)

\* kill => no in-flight step process group survives.
NoStepGroupSurvivesKill ==
    \A s \in Session :
        listenerJob[s] /= NoListener /\ listenerJob[s].workerAlive = FALSE
        => stepGroupsAlive = {}

\* A deduped-but-unparsed message must not be acknowledged (the F-4 hole: it
\* can neither be re-delivered nor acked).
NoDedupedUnparsedAck ==
    \A s \in Session, m \in AllMsgIds : m \in acked[s] => m \in parsed[s]

\* A reported worker exit implies the report reached the server.
WorkerTerminalOnReport ==
    \A s \in Session :
        workerReported[s] => completeReported[s] \/ forceFailed[s]

(* -------------------------------------------------------------------------
 * Scenario 5 invariants
 * ------------------------------------------------------------------------- *)

\* Every queued step transition is delivered at-least-once: if there is an
\* unpublished generation, dirty must be non-empty so the next take can
\* rebuild the body.
StepTransitionDelivered ==
    gen = pubGen \/ dirty /= {}

\* conclusion-cause consistency (MC18): a timed-out step never concludes
\* Cancelled.
TimeoutMeansFailure ==
    \A st \in StepId :
        steps[st].killCause = KillTimeout
        => steps[st].conclusion /= RCancelled

\* Job output is bounded at 1 MiB.
OutputSizeBounded == outputBytes <= MaxOutputBytes

(* -------------------------------------------------------------------------
 * Scenario 6 invariants
 * ------------------------------------------------------------------------- *)

\* No secret ever appears in an emitted line.
MaskNeverLeaks ==
    \A secret \in maskSet :
        ~(\E i \in 1..Len(lines) : secret = lines[i])

\* The format-builder must escape '{' and '}' — the protocol copy violates it.
FormatEscapeClosed == ~formatError

(* -------------------------------------------------------------------------
 * Structural invariants
 * ------------------------------------------------------------------------- *)

\* A session holds at most one active request and its request exists.
SessionActiveConsistent ==
    \A s \in Session :
        sessionActive[s] = NoReq \/ reqInfo[sessionActive[s]].state /= SNone

\* brokerMsg is only ever assigned to requests that exist.
BrokerMsgConsistent ==
    \A req \in RequestId :
        brokerMsg[req] > 0 => reqInfo[req].state /= SNone

\* held runs contain jobs that belong to the run.
HeldRunsConsistent ==
    \A run \in RunId : \A j \in heldRuns[run] : j[2] \in runJobs[run]

\* Dispatch queue has unique request ids.
DispatchQueueUnique ==
    \A i, j \in 1..Len(dispatchQueue) :
        i /= j => dispatchQueue[i] /= dispatchQueue[j]

\* pending/blocked job refs are unique.
PendingJobsUnique ==
    \A i, j \in 1..Len(pendingJobs) :
        i /= j => pendingJobs[i] /= pendingJobs[j]
    /\ \A m, k \in 1..Len(blockedJobs) :
        m /= k => blockedJobs[m] /= blockedJobs[k]

\* A job in the dispatch queue belongs to a live run.
DispatchQueueBelongs ==
    \A req \in Range(dispatchQueue) :
        /\ reqInfo[req].state /= SNone
        /\ reqInfo[req].job \in runJobs[reqInfo[req].run]

\* changeOrder advances only when a body is taken.
ChangeOrderConsistent == changeOrder <= gen

\* The dirty set only contains known steps.
DirtyKnownSteps == dirty \subseteq StepId

\* Registered request records are exactly the allocated (non-SNone,
\* non-terminal) ones.
CorrelationConsistent ==
    \A req \in RequestId :
        reqInfo[req].state /= SNone /\ reqInfo[req].state /= STerminal
        => (req \in inflightReq) = (planReq[req] = req)

(* =========================================================================
 * TEMPORAL PROPERTIES
 * ========================================================================= *)

\* A pending holder whose gates clear is eventually promoted (S2 F3 — the
\* max-parallel backoff with siblings in other groups never re-promotes).
PendingHolderEventuallyPromoted ==
    \A k \in Key :
        [](Len(groups[k].pending) > 0 => <>(groups[k].pending = <<>>))

\* cancel => eventually SIGKILL/graceful exit of the worker (S4 F-1/F-2).
CancelImpliesKill ==
    \A s \in Session :
        []( listenerJob[s] /= NoListener
            /\ listenerJob[s].shutdownSrc /= NoShutdown
            => <>(listenerJob[s] = NoListener) )

\* Every dispatched job reaches a terminal server state (S4 F-5/F-15, S5 F3).
NoSilentJobDrop ==
    \A run \in RunId, job \in JobId :
        job \in runJobs[run]
        => []( jobStatus[run][job] \in {StQueued, StPending, StInProgress}
               => <>(jobStatus[run][job] \in TerminalStatus) )

\* cancel => eventually signal every in-flight child (S5 F2 — docker never
\* signals its process group).
CancelClosureForChildren ==
    \A st \in StepId :
        []( st \in cancelSentStep => <>(st \in signalled) )

\* A claimed AzDO request with no renew stamp must still terminate (S1 F5 —
\* the lease reaper is blind for a dead AzDO runner).
EventualJobTermination ==
    \A req \in RequestId :
        reqInfo[req].state /= SNone
        => []( reqInfo[req].state \in {SQueued, SClaimed, SAcquiring, SRunning}
               => <>(reqInfo[req].state = STerminal) )

\* A queued job is eventually claimed (S3 F-3 / MC10 — unclaimable under
\* require_job_assignments).
JobEventuallyClaimed ==
    \A req \in RequestId :
        reqInfo[req].state /= SNone
        => []( reqInfo[req].state = SQueued
               => <>(reqInfo[req].state /= SQueued) )

====================
