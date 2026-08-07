--------------------------- MODULE MC ---------------------------
(*
 * Model checking specification for aksh / preloop.
 *
 * Wraps the base spec with counter-bounded fault-injection actions.
 * Deterministic / reactive actions pass through unbounded.
 *
 * Counter-bounded fault-injection actions:
 *   - SubmitRun2            (client submission, nondeterministic)
 *   - ArriveRunFree/CIP/Park, ArriveJobFree/CIP/Park
 *                           (concurrency-group arrivals, client nondeterminism)
 *   - CancelRun / CancelJob (user cancellation API, nondeterministic)
 *   - TimeAdvance           (abstract clock, Scenario 1/4 timers)
 *   - HttpFlap              (WorkflowStepsUpdate POST failure, S5 F1)
 *   - ListenerShutdownSignal (external shutdown, S4 F-1)
 *   - WorkerExits-unreported (worker crash, S4 F-2/F-5/F-15)
 *   - ListenerParse-fail     (dedup-before-parse hole, S4 F-4)
 *   - WorkerCompletePost-fail (lost completejob POST, S4 F-15 / S5 F5)
 *   - SetEscapeBraces(FALSE) (protocol-copy format builder, S6 F1)
 *   - EmitLine-unmasked      (masking failure, S6)
 *   - CrashServer            (in-memory control-plane restart, S1/S3)
 *   - WorkerFlushOutput      (job output growth, S5 F4)
 *   - AddMask / BuildFormat / ScanStep (protocol inputs, S6)
 *
 * Unbounded (deterministic / reactive):
 *   - claims, renews, completions, reaper, acks, delivery paths
 *   - promotions, release/cancel holders, skip/eval-fail, fail-fast
 *   - expansion apply/build, listener message pipeline (non-fault arms)
 *   - step queue take/publish (non-fault arms)
 *)

EXTENDS base

\* Access original (un-overridden) operator definitions.
b == INSTANCE base

\* ============================================================================
\* CONSTRAINT CONSTANTS
\* ============================================================================

CONSTANT
    MaxSubmitLimit,       \* Max workflow submissions
    MaxArriveLimit,       \* Max concurrency arrivals
    MaxCancelLimit,       \* Max cancel API calls
    MaxTimeLimit,         \* Max abstract clock ticks
    MaxHttpFailLimit,     \* Max WorkflowStepsUpdate POST failures
    MaxShutdownLimit,     \* Max external shutdown signals
    MaxWorkerCrashLimit,  \* Max worker exits without a report
    MaxParseFailLimit,    \* Max message parse failures
    MaxPostFailLimit,     \* Max completejob POST losses
    MaxEscapeSwitchLimit, \* Max protocol-copy format-builder switches (S6)
    MaxMaskFailLimit,     \* Max unmasked line emissions (S6)
    MaxOutputLimit,       \* Max output flushes (S5 F4)
    MaxInputLimit,        \* Max tokenizer / format inputs (S6)
    MaxCrashLimit,        \* Max control-plane crashes
    MaxCancelQueue,       \* Max cancellation_queue length
    MaxDispatchQueue,     \* Max dispatch queue length
    MaxPendingJobs        \* Max pending_jobs length

\* ============================================================================
\* CONSTRAINT VARIABLES
\* ============================================================================

VARIABLE faultCounters

faultVars == <<faultCounters>>

\* ============================================================================
\* COUNTER-BOUNDED FAULT-INJECTION ACTIONS
\* ============================================================================

MCSubmitRun2(run, jobs) ==
    /\ faultCounters.submit < MaxSubmitLimit
    /\ b!SubmitRun2(run, jobs)
    /\ faultCounters' = [faultCounters EXCEPT !.submit = @ + 1]

MCArriveRunFree(run, key) ==
    /\ faultCounters.arrive < MaxArriveLimit
    /\ b!ArriveRunFree(run, key)
    /\ faultCounters' = [faultCounters EXCEPT !.arrive = @ + 1]

MCArriveRunCIP(run, key, prevRun) ==
    /\ faultCounters.arrive < MaxArriveLimit
    /\ b!ArriveRunCIP(run, key, prevRun)
    /\ faultCounters' = [faultCounters EXCEPT !.arrive = @ + 1]

MCArriveRunPark(run, key) ==
    /\ faultCounters.arrive < MaxArriveLimit
    /\ b!ArriveRunPark(run, key)
    /\ faultCounters' = [faultCounters EXCEPT !.arrive = @ + 1]

MCArriveJobFree(run, job, key) ==
    /\ faultCounters.arrive < MaxArriveLimit
    /\ b!ArriveJobFree(run, job, key)
    /\ faultCounters' = [faultCounters EXCEPT !.arrive = @ + 1]

MCArriveJobCIP(run, job, key, prevRun) ==
    /\ faultCounters.arrive < MaxArriveLimit
    /\ b!ArriveJobCIP(run, job, key, prevRun)
    /\ faultCounters' = [faultCounters EXCEPT !.arrive = @ + 1]

MCArriveJobPark(run, job, key, mode, ph) ==
    /\ faultCounters.arrive < MaxArriveLimit
    /\ b!ArriveJobPark(run, job, key, mode, ph)
    /\ faultCounters' = [faultCounters EXCEPT !.arrive = @ + 1]

MCCancelRun(run) ==
    /\ faultCounters.cancel < MaxCancelLimit
    /\ b!CancelRun(run)
    /\ faultCounters' = [faultCounters EXCEPT !.cancel = @ + 1]

MCCancelJob(run, job) ==
    /\ faultCounters.cancel < MaxCancelLimit
    /\ b!CancelJob(run, job)
    /\ faultCounters' = [faultCounters EXCEPT !.cancel = @ + 1]

MCTimeAdvance ==
    /\ faultCounters.time < MaxTimeLimit
    /\ b!TimeAdvance
    /\ faultCounters' = [faultCounters EXCEPT !.time = @ + 1]

MCHttpFlap(v) ==
    /\ faultCounters.httpFail < MaxHttpFailLimit
    /\ b!HttpFlap(v)
    /\ faultCounters' = [faultCounters EXCEPT !.httpFail = @ + 1]

MCListenerShutdownSignal(s) ==
    /\ faultCounters.shutdown < MaxShutdownLimit
    /\ b!ListenerShutdownSignal(s)
    /\ faultCounters' = [faultCounters EXCEPT !.shutdown = @ + 1]

MCWorkerExitsUnreported(s) ==
    /\ faultCounters.workerCrash < MaxWorkerCrashLimit
    /\ b!WorkerExits(s, FALSE)
    /\ faultCounters' = [faultCounters EXCEPT !.workerCrash = @ + 1]

MCListenerParseFail(s, m) ==
    /\ faultCounters.parseFail < MaxParseFailLimit
    /\ b!ListenerParse(s, m, FALSE)
    /\ faultCounters' = [faultCounters EXCEPT !.parseFail = @ + 1]

MCWorkerCompletePostFail(s, o) ==
    /\ faultCounters.postFail < MaxPostFailLimit
    /\ b!WorkerCompletePost(s, o, FALSE)
    /\ faultCounters' = [faultCounters EXCEPT !.postFail = @ + 1]

MCSetEscapeBracesFalse ==
    /\ faultCounters.escapeSwitch < MaxEscapeSwitchLimit
    /\ b!SetEscapeBraces(FALSE)
    /\ faultCounters' = [faultCounters EXCEPT !.escapeSwitch = @ + 1]

MCEmitLineUnmasked(secret) ==
    /\ faultCounters.maskFail < MaxMaskFailLimit
    /\ b!EmitLine(secret, FALSE)
    /\ faultCounters' = [faultCounters EXCEPT !.maskFail = @ + 1]

MCCrashServer ==
    /\ faultCounters.crash < MaxCrashLimit
    /\ b!CrashServer
    /\ faultCounters' = [faultCounters EXCEPT !.crash = @ + 1]

MCWorkerFlushOutput(bytes) ==
    /\ faultCounters.output < MaxOutputLimit
    /\ b!WorkerFlushOutput(bytes)
    /\ faultCounters' = [faultCounters EXCEPT !.output = @ + 1]

\* S6 protocol inputs (nondeterministic template/token streams)
MCAddMask(secret) ==
    /\ faultCounters.input < MaxInputLimit
    /\ b!AddMask(secret)
    /\ faultCounters' = [faultCounters EXCEPT !.input = @ + 1]

MCBuildFormat(lb, ex) ==
    /\ faultCounters.input < MaxInputLimit
    /\ b!BuildFormat(lb, ex)
    /\ faultCounters' = [faultCounters EXCEPT !.input = @ + 1]

MCScanStep(odd) ==
    /\ faultCounters.input < MaxInputLimit
    /\ b!ScanStep(odd)
    /\ faultCounters' = [faultCounters EXCEPT !.input = @ + 1]

\* ============================================================================
\* UNBOUNDED (DETERMINISTIC / REACTIVE) ACTIONS
\* ============================================================================

\* These actions are NOT bounded: they react to existing state. Bounding them
\* would prune valid state space (mc-spec-pattern.md: "don't bound the normal
\* reactive steps that make the bug reachable").

MCEnqueuePending(run, job) ==
    /\ b!EnqueuePending(run, job)
    /\ UNCHANGED faultVars

MCDeclareGate(run, job, key) ==
    /\ b!DeclareGate(run, job, key)
    /\ UNCHANGED faultVars

MCAzdoPollDeliverInflight(s, m) ==
    /\ b!AzdoPollDeliverInflight(s, m)
    /\ UNCHANGED faultVars

MCAzdoPollPopCancel(s) ==
    /\ b!AzdoPollPopCancel(s)
    /\ UNCHANGED faultVars

MCAzdoPollClaim(s, req) ==
    /\ b!AzdoPollClaim(s, req)
    /\ UNCHANGED faultVars

MCBrokerRootClaim(s, req) ==
    /\ b!BrokerRootClaim(s, req)
    /\ UNCHANGED faultVars

MCBrokerPoolRedeliver(s, req) ==
    /\ b!BrokerPoolRedeliver(s, req)
    /\ UNCHANGED faultVars

MCBrokerPollDeliverCancelScoped(s, req, c) ==
    /\ b!BrokerPollDeliverCancelScoped(s, req, c)
    /\ UNCHANGED faultVars

MCBrokerPollDeliverCancelPool(s, req, c) ==
    /\ b!BrokerPollDeliverCancelPool(s, req, c)
    /\ UNCHANGED faultVars

MCBrokerPollCleanup(s, req) ==
    /\ b!BrokerPollCleanup(s, req)
    /\ UNCHANGED faultVars

MCAcquireJobStart(s, req) ==
    /\ b!AcquireJobStart(s, req)
    /\ UNCHANGED faultVars

MCAcquireJobMintOk(req) ==
    /\ b!AcquireJobMintOk(req)
    /\ UNCHANGED faultVars

MCAcquireJobMintFail(req) ==
    /\ b!AcquireJobMintFail(req)
    /\ UNCHANGED faultVars

MCRenewJob(s, req) ==
    /\ b!RenewJob(s, req)
    /\ UNCHANGED faultVars

MCCompleteJobSetResult(s, req, o) ==
    /\ b!CompleteJobSetResult(s, req, o)
    /\ UNCHANGED faultVars

MCCompleteJobApply(req, o) ==
    /\ b!CompleteJobApply(req, o)
    /\ UNCHANGED faultVars

MCReapLease(req) ==
    /\ b!ReapLease(req)
    /\ UNCHANGED faultVars

MCReapTimeout(req) ==
    /\ b!ReapTimeout(req)
    /\ UNCHANGED faultVars

MCAckMessage(s, m) ==
    /\ b!AckMessage(s, m)
    /\ UNCHANGED faultVars

MCPromoteNextRunArm(key, run) ==
    /\ b!PromoteNextRunArm(key, run)
    /\ UNCHANGED faultVars

MCPromoteNextJobArm(key, run, job, mp) ==
    /\ b!PromoteNextJobArm(key, run, job, mp)
    /\ UNCHANGED faultVars

MCPromoteDispatchJob(run, job) ==
    /\ b!PromoteDispatchJob(run, job)
    /\ UNCHANGED faultVars

MCPromoteReadyJob(run, job) ==
    /\ b!PromoteReadyJob(run, job)
    /\ UNCHANGED faultVars

\* NOTE: on_job_enqueued is NOT a standalone action — it is inlined into
\* ArriveJobFree / PromoteNextRunArm / PromoteNextJobArm as EnqueueAssignment.
\* promote_ready_jobs (PromoteDispatchJob / PromoteReadyJob) lacks it *BUG F-3*.

MCFailFast(run, failed, siblings) ==
    /\ b!FailFast(run, failed, siblings)
    /\ UNCHANGED faultVars

MCSkipJob(run, job) ==
    /\ b!SkipJob(run, job)
    /\ UNCHANGED faultVars

MCEvalFailJob(run, job) ==
    /\ b!EvalFailJob(run, job)
    /\ UNCHANGED faultVars

MCReleaseJob(run, job) ==
    /\ b!ReleaseJob(run, job)
    /\ UNCHANGED faultVars

MCReleaseRun(run) ==
    /\ b!ReleaseRun(run)
    /\ UNCHANGED faultVars

MCDeferExpansion(run, job) ==
    /\ b!DeferExpansion(run, job)
    /\ UNCHANGED faultVars

MCBuildExpansionStart(run, job) ==
    /\ b!BuildExpansionStart(run, job)
    /\ UNCHANGED faultVars

MCApplyMatrix(run, node, fanout) ==
    /\ b!ApplyMatrix(run, node, fanout)
    /\ UNCHANGED faultVars

MCApplyReusable(run, caller, fanout) ==
    /\ b!ApplyReusable(run, caller, fanout)
    /\ UNCHANGED faultVars

MCBuildExpansionFail(run, node) ==
    /\ b!BuildExpansionFail(run, node)
    /\ UNCHANGED faultVars

MCListenerDedup(s, m) ==
    /\ b!ListenerDedup(s, m)
    /\ UNCHANGED faultVars

MCListenerParseOk(s, m) ==
    /\ b!ListenerParse(s, m, TRUE)
    /\ UNCHANGED faultVars

MCListenerAckMsg(s, m) ==
    /\ b!ListenerAckMsg(s, m)
    /\ UNCHANGED faultVars

MCListenerJobCancellation(s, m, tr, tj, valid) ==
    /\ b!ListenerJobCancellation(s, m, tr, tj, valid)
    /\ UNCHANGED faultVars

MCListenerRunnerJobRequest(s, m, nr) ==
    /\ b!ListenerRunnerJobRequest(s, m, nr)
    /\ UNCHANGED faultVars

MCListenerRunnerShutdown(s, m) ==
    /\ b!ListenerRunnerShutdown(s, m)
    /\ UNCHANGED faultVars

MCListenerKillTimerFires(s) ==
    /\ b!ListenerKillTimerFires(s)
    /\ UNCHANGED faultVars

MCWorkerExitsReported(s) ==
    /\ b!WorkerExits(s, TRUE)
    /\ UNCHANGED faultVars

MCListenerForceFail(s, ok) ==
    /\ b!ListenerForceFail(s, ok)
    /\ UNCHANGED faultVars

MCWorkerCompletePostOk(s, o) ==
    /\ b!WorkerCompletePost(s, o, TRUE)
    /\ UNCHANGED faultVars

MCWorkerQueueUpdate(st, status, concl) ==
    /\ b!WorkerQueueUpdate(st, status, concl)
    /\ UNCHANGED faultVars

MCWorkerTakeBody ==
    /\ b!WorkerTakeBody
    /\ UNCHANGED faultVars

MCWorkerPublishOk ==
    /\ b!WorkerPublishOk
    /\ UNCHANGED faultVars

MCWorkerPublishFail ==
    /\ b!WorkerPublishFail
    /\ UNCHANGED faultVars

MCWorkerCancelStep(st, armed) ==
    /\ b!WorkerCancelStep(st, armed)
    /\ UNCHANGED faultVars

MCWorkerStepTimeout(st) ==
    /\ b!WorkerStepTimeout(st)
    /\ UNCHANGED faultVars

MCWorkerConcludeStep(st, cs) ==
    /\ b!WorkerConcludeStep(st, cs)
    /\ UNCHANGED faultVars

MCWorkerSetupWorkspace(ok) ==
    /\ b!WorkerSetupWorkspace(ok)
    /\ UNCHANGED faultVars

MCWorkerRenewLoopAbort ==
    /\ b!WorkerRenewLoopAbort
    /\ UNCHANGED faultVars

MCEmitLineMasked(secret) ==
    /\ b!EmitLine(secret, TRUE)
    /\ UNCHANGED faultVars

MCScanStepNormal(odd) ==
    /\ b!ScanStep(odd)
    /\ UNCHANGED faultVars

MCSetEscapeBracesTrue ==
    /\ b!SetEscapeBraces(TRUE)
    /\ UNCHANGED faultVars

MCStutter ==
    /\ b!Stutter
    /\ UNCHANGED faultVars

\* ============================================================================
\* INITIALIZATION
\* ============================================================================

MCInit ==
    /\ Init
    /\ faultCounters = [
         submit |-> 0, arrive |-> 0, cancel |-> 0, time |-> 0,
         httpFail |-> 0, shutdown |-> 0, workerCrash |-> 0,
         parseFail |-> 0, postFail |-> 0, escapeSwitch |-> 0,
         maskFail |-> 0, output |-> 0, input |-> 0, crash |-> 0]

\* ============================================================================
\* NEXT STATE RELATION
\* ============================================================================

MCNext ==
    \* --- Submission / gating (bounded client behavior) ---
    \/ \E run \in RunId, jobs \in SUBSET JobId : MCSubmitRun2(run, jobs)
    \/ \E run \in RunId, job \in JobId : MCEnqueuePending(run, job)
    \/ \E run \in RunId, job \in JobId, key \in Key : MCDeclareGate(run, job, key)
    \* --- Concurrency arrivals (bounded client behavior) ---
    \/ \E run \in RunId, key \in Key : MCArriveRunFree(run, key)
    \/ \E run \in RunId, key \in Key, prevRun \in RunId : MCArriveRunCIP(run, key, prevRun)
    \/ \E run \in RunId, key \in Key : MCArriveRunPark(run, key)
    \/ \E run \in RunId, job \in JobId, key \in Key : MCArriveJobFree(run, job, key)
    \/ \E run \in RunId, job \in JobId, key \in Key, prevRun \in RunId :
        MCArriveJobCIP(run, job, key, prevRun)
    \/ \E run \in RunId, job \in JobId, key \in Key, mode \in {ModeSingle, ModeMax},
            ph \in HolderSet :
        MCArriveJobPark(run, job, key, mode, ph)
    \* --- Cancels (bounded client behavior) ---
    \/ \E run \in RunId : MCCancelRun(run)
    \/ \E run \in RunId, job \in JobId : MCCancelJob(run, job)
    \* --- Scheduler promotions (unbounded, reactive) ---
    \/ \E key \in Key, run \in RunId : MCPromoteNextRunArm(key, run)
    \/ \E key \in Key, run \in RunId, job \in JobId, mp \in BOOLEAN :
        MCPromoteNextJobArm(key, run, job, mp)
    \/ \E run \in RunId, job \in JobId : MCPromoteDispatchJob(run, job)
    \/ \E run \in RunId, job \in JobId : MCPromoteReadyJob(run, job)
    \/ \E run \in RunId, failed \in JobId, siblings \in SUBSET JobId :
        MCFailFast(run, failed, siblings)
    \/ \E run \in RunId, job \in JobId : MCSkipJob(run, job)
    \/ \E run \in RunId, job \in JobId : MCEvalFailJob(run, job)
    \/ \E run \in RunId, job \in JobId : MCReleaseJob(run, job)
    \/ \E run \in RunId : MCReleaseRun(run)
    \* --- Deferred expansion (unbounded, reactive) ---
    \/ \E run \in RunId, job \in JobId : MCDeferExpansion(run, job)
    \/ \E run \in RunId, job \in JobId : MCBuildExpansionStart(run, job)
    \/ \E run \in RunId, node \in JobId, fanout \in SUBSET JobId : MCApplyMatrix(run, node, fanout)
    \/ \E run \in RunId, caller \in JobId, fanout \in SUBSET JobId : MCApplyReusable(run, caller, fanout)
    \/ \E run \in RunId, node \in JobId : MCBuildExpansionFail(run, node)
    \* --- Claim/lease lifecycle (unbounded, reactive) ---
    \/ \E s \in Session, m \in AllMsgIds : MCAzdoPollDeliverInflight(s, m)
    \/ \E s \in Session : MCAzdoPollPopCancel(s)
    \/ \E s \in Session, req \in RequestId : MCAzdoPollClaim(s, req)
    \/ \E s \in Session, req \in RequestId : MCBrokerRootClaim(s, req)
    \/ \E s \in Session, req \in RequestId : MCBrokerPoolRedeliver(s, req)
    \/ \E s \in Session, req \in RequestId, c \in CancelRecDomain :
        MCBrokerPollDeliverCancelScoped(s, req, c)
    \/ \E s \in Session, req \in RequestId, c \in CancelRecDomain :
        MCBrokerPollDeliverCancelPool(s, req, c)
    \/ \E s \in Session, req \in RequestId : MCBrokerPollCleanup(s, req)
    \/ \E s \in Session, req \in RequestId : MCAcquireJobStart(s, req)
    \/ \E req \in RequestId : MCAcquireJobMintOk(req)
    \/ \E req \in RequestId : MCAcquireJobMintFail(req)
    \/ \E s \in Session, req \in RequestId : MCRenewJob(s, req)
    \/ \E s \in Session, req \in RequestId, o \in {RSuccess, RFailure, RCancelled} :
        MCCompleteJobSetResult(s, req, o)
    \/ \E req \in RequestId, o \in {RSuccess, RFailure, RCancelled} :
        MCCompleteJobApply(req, o)
    \/ \E req \in RequestId : MCReapLease(req)
    \/ \E req \in RequestId : MCReapTimeout(req)
    \/ \E s \in Session, m \in AllMsgIds : MCAckMessage(s, m)
    \* --- Runner listener pipeline (unbounded, reactive; fault arms bounded) ---
    \/ \E s \in Session, m \in AllMsgIds : MCListenerDedup(s, m)
    \/ \E s \in Session, m \in AllMsgIds : MCListenerParseOk(s, m)
    \/ \E s \in Session, m \in AllMsgIds : MCListenerAckMsg(s, m)
    \/ \E s \in Session, m \in AllMsgIds, tr \in RunId, tj \in JobId, valid \in BOOLEAN :
        MCListenerJobCancellation(s, m, tr, tj, valid)
    \/ \E s \in Session, m \in AllMsgIds, nr \in RequestId :
        MCListenerRunnerJobRequest(s, m, nr)
    \/ \E s \in Session, m \in AllMsgIds : MCListenerRunnerShutdown(s, m)
    \/ \E s \in Session : MCListenerKillTimerFires(s)
    \/ \E s \in Session : MCWorkerExitsReported(s)
    \/ \E s \in Session, ok \in BOOLEAN : MCListenerForceFail(s, ok)
    \/ \E s \in Session, o \in {RSuccess, RFailure, RCancelled} :
        MCWorkerCompletePostOk(s, o)
    \* --- Worker step queue (unbounded, reactive) ---
    \/ \E st \in StepId, status \in Status, concl \in ResultSet :
        MCWorkerQueueUpdate(st, status, concl)
    \/ MCWorkerTakeBody
    \/ MCWorkerPublishOk
    \/ MCWorkerPublishFail
    \/ \E st \in StepId, armed \in BOOLEAN : MCWorkerCancelStep(st, armed)
    \/ \E st \in StepId : MCWorkerStepTimeout(st)
    \/ \E st \in StepId, cs \in BOOLEAN : MCWorkerConcludeStep(st, cs)
    \/ \E ok \in BOOLEAN : MCWorkerSetupWorkspace(ok)
    \/ MCWorkerRenewLoopAbort
    \* --- Protocol (bounded inputs; masked arm unbounded) ---
    \/ \E secret \in Str : MCEmitLineMasked(secret)
    \/ \E odd \in BOOLEAN : MCScanStepNormal(odd)
    \/ MCSetEscapeBracesTrue
    \* --- Time (bounded) ---
    \/ MCTimeAdvance
    \* --- Fault injection (bounded) ---
    \/ \E s \in Session, m \in AllMsgIds : MCListenerParseFail(s, m)
    \/ \E s \in Session : MCWorkerExitsUnreported(s)
    \/ \E s \in Session, o \in {RSuccess, RFailure, RCancelled} :
        MCWorkerCompletePostFail(s, o)
    \/ \E s \in Session : MCListenerShutdownSignal(s)
    \/ \E v \in BOOLEAN : MCHttpFlap(v)
    \/ \E secret \in Str : MCEmitLineUnmasked(secret)
    \/ MCSetEscapeBracesFalse
    \/ \E secret \in Str : MCAddMask(secret)
    \/ \E lb \in BOOLEAN, ex \in BOOLEAN : MCBuildFormat(lb, ex)
    \/ \E odd \in BOOLEAN : MCScanStep(odd)
    \/ \E bytes \in 0..MaxChunk : MCWorkerFlushOutput(bytes)
    \/ MCCrashServer
    \/ MCStutter

\* ============================================================================
\* SPECIFICATIONS
\* ============================================================================

mc_vars == <<vars, faultVars>>

MCSpec == MCInit /\ [][MCNext]_mc_vars
MCSpecFair == MCInit /\ [][MCNext]_mc_vars /\ WF_mc_vars(MCNext)

\* ============================================================================
\* SYMMETRY AND VIEW
\* ============================================================================

\* Sessions are interchangeable (broker sessions of identical runners).
Symmetry == Permutations(Session)

\* Exclude fault counters from the view (they don't affect protocol behavior).
ModelView == <<vars>>

\* ============================================================================
\* STATE SPACE PRUNING
\* ============================================================================

TimeBound == now <= MaxTimeLimit + 30

QueueConstraint ==
    /\ Len(cancelQueue) <= MaxCancelQueue
    /\ Len(dispatchQueue) <= MaxDispatchQueue
    /\ Len(pendingJobs) <= MaxPendingJobs

\* ============================================================================
\* MC-LEVEL STRUCTURAL INVARIANTS
\* ============================================================================

\* A delivered cancel either went to the right session or is recorded as a
\* violation candidate — sanity for the audit trail.
DeliveredCancelAuditConsistent ==
    \A c \in Range(deliveredCancel) : c.session \in Session

\* Fault counters never exceed their limits by construction.
FaultCountersBounded ==
    /\ faultCounters.submit <= MaxSubmitLimit
    /\ faultCounters.time <= MaxTimeLimit
    /\ faultCounters.cancel <= MaxCancelLimit

====================
