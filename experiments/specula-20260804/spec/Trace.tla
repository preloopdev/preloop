--------------------------- MODULE Trace ---------------------------
(*
 * Trace validation specification for preloop / preloop.
 *
 * Replays implementation traces against the base spec to verify that the
 * base spec can reproduce every observed state transition.
 *
 * Category A: single-file linear trace. Trace format: NDJSON with tag =
 * "trace" and event records containing:
 *   - event.name:     base action name (one trace event per spec action)
 *   - event.session:  session id (for session-scoped actions)
 *   - event.state:    post-action state snapshot (see instrumentation-spec.md)
 *
 * Every wrapper calls the full base action and validates the post-state
 * fields the harness captures (instrumentation-spec.md is the single source
 * of truth for the field mapping). The cursor `l` walks TraceLog; a wrapper
 * that fires advances l. Silent actions cover un-instrumented clock ticks
 * and stuttering.
 *)

EXTENDS base, Json, IOUtils, Sequences, TLC

\* Access original (un-overridden) operator definitions.
b == INSTANCE base

\* ============================================================================
\* TRACE LOADING
\* ============================================================================

\* Read the JSON file path from the environment or use the default.
JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON
    ELSE "../traces/trace.ndjson"

\* Load NDJSON, filter to trace events only.
TraceLog == TLCEval(
    LET all == ndJsonDeserialize(JsonFile)
    IN SelectSeq(all, LAMBDA x :
        /\ "tag" \in DOMAIN x
        /\ x.tag = "trace"
        /\ "event" \in DOMAIN x))

ASSUME Len(TraceLog) > 0

\* ============================================================================
\* TRACE CURSOR
\* ============================================================================

VARIABLE l

traceVars == <<l>>

logline == TraceLog[l]

snap == logline.event.state

\* ============================================================================
\* SERVER EXTRACTION FROM TRACE
\* ============================================================================

TraceSession == TLCEval(
    UNION {IF "session" \in DOMAIN TraceLog[k].event
           THEN {TraceLog[k].event.session} ELSE {} : k \in 1..Len(TraceLog)})

ASSUME TraceSession \subseteq Session

\* ============================================================================
\* EVENT PREDICATES
\* ============================================================================

IsEvent(name) ==
    /\ l <= Len(TraceLog)
    /\ logline.event.name = name

IsNodeEvent(name, s) ==
    /\ IsEvent(name)
    /\ logline.event.session = s

\* ============================================================================
\* STATE MAPPING
\* ============================================================================

TraceResult(v) ==
    IF v = "" \/ v = "null" \/ v = "nil" THEN RNone ELSE v

TraceStatus(v) ==
    IF v = "Queued" THEN StQueued
    ELSE IF v = "Pending" THEN StPending
    ELSE IF v = "InProgress" THEN StInProgress
    ELSE IF v = "Success" THEN StSuccess
    ELSE IF v = "Failure" THEN StFailure
    ELSE IF v = "Cancelled" THEN StCancelled
    ELSE IF v = "Skipped" THEN StSkipped
    ELSE StQueued

TraceReqState(v) ==
    IF v = "Queued" THEN SQueued
    ELSE IF v = "Claimed" THEN SClaimed
    ELSE IF v = "Acquiring" THEN SAcquiring
    ELSE IF v = "Running" THEN SRunning
    ELSE IF v = "Terminal" THEN STerminal
    ELSE SNone

TraceResultOf(v) ==
    IF v = "None" THEN RNone
    ELSE IF v = "Success" THEN RSuccess
    ELSE IF v = "Failure" THEN RFailure
    ELSE IF v = "Cancelled" THEN RCancelled
    ELSE IF v = "Skipped" THEN RSkipped
    ELSE RNone

TraceBool(v) == IF v = "true" \/ v = 1 THEN TRUE ELSE FALSE

\* JSON arrays parse as TLA+ sequences; convert to sets.
TraceSet(v) == {v[i] : i \in 1..Len(v)}

\* ============================================================================
\* COMMON POST-STATE VALIDATION
\* ============================================================================

\* Every event snapshot carries the abstract time `now`; validate it.
ValidateNow == now' = snap.now

\* Validate the request record fields the harness captures at every server
\* lifecycle event (see instrumentation-spec.md Section 1).
ValidateReq(req) ==
    /\ now' = snap.now
    /\ reqInfo'[req].state = TraceReqState(snap.reqState)
    \* reqResult is captured only by terminal events; non-terminal snapshots
    \* omit it (instrumentation-spec.md Section 1).
    /\ IF "reqResult" \in DOMAIN snap
      THEN reqInfo'[req].result = TraceResultOf(snap.reqResult)
      ELSE TRUE
    /\ (IF snap.reqStarted = "" THEN reqInfo'[req].started = NoTime
        ELSE reqInfo'[req].started = snap.reqStarted)

ValidateSession(s) ==
    /\ (IF snap.sessionActive = "" THEN sessionActive'[s] = NoReq
        ELSE sessionActive'[s] = snap.sessionActive)

ValidateJob(run, job) ==
    /\ jobStatus'[run][job] = TraceStatus(snap.jobSt)
    /\ runStatus'[run] = TraceStatus(snap.runSt)

\* ============================================================================
\* ACTION WRAPPERS
\* ============================================================================

\* ---------------------------------------------------------------------------
\* SubmitRun2
\* ---------------------------------------------------------------------------
SubmitRun2IfLogged ==
    \E run \in RunId, jobs \in SUBSET JobId :
        /\ IsEvent("SubmitRun2")
        /\ snap.run = run
        /\ TraceSet(snap.jobs) = jobs
        /\ b!SubmitRun2(run, jobs)
        /\ runJobs'[run] = jobs
        /\ runStatus'[run] = StQueued
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* EnqueuePending
\* ---------------------------------------------------------------------------
EnqueuePendingIfLogged ==
    \E run \in RunId, job \in JobId :
        /\ IsEvent("EnqueuePending")
        /\ snap.run = run /\ snap.job = job
        /\ b!EnqueuePending(run, job)
        /\ <<run, job>> \in Range(pendingJobs')
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* DeclareGate
\* ---------------------------------------------------------------------------
DeclareGateIfLogged ==
    \E run \in RunId, job \in JobId, key \in Key :
        /\ IsEvent("DeclareGate")
        /\ snap.run = run /\ snap.job = job /\ snap.key = key
        /\ b!DeclareGate(run, job, key)
        /\ hasGate'[run][job] = key
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* AzdoPollClaim (AzDO claim stamps started only — F5)
\* ---------------------------------------------------------------------------
AzdoPollClaimIfLogged ==
    \E s \in Session, req \in RequestId :
        /\ IsNodeEvent("AzdoPollClaim", s)
        \* The harness numbers requests from its own counter; the model binds
        \* them by (run, job) identity instead of matching raw req numbers.
        /\ snap.run = reqInfo[req].run /\ snap.job = reqInfo[req].job
        /\ b!AzdoPollClaim(s, req)
        \* AzdoPollClaim contract (instrumentation-spec.md Section 4): reqState
        \* + reqStarted only; result stays None and sessionActive[s] = req.
        /\ now' = snap.now
        /\ reqInfo'[req].state = TraceReqState(snap.reqState)
        /\ sessionActive'[s] = req
        /\ reqInfo'[req].started = snap.reqStarted
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* BrokerRootClaim (broker claim stamps started + renew)
\* ---------------------------------------------------------------------------
BrokerRootClaimIfLogged ==
    \E s \in Session, req \in RequestId :
        /\ IsNodeEvent("BrokerRootClaim", s)
        /\ snap.req = req
        /\ b!BrokerRootClaim(s, req)
        /\ ValidateReq(req)
        /\ sessionActive'[s] = req
        /\ reqInfo'[req].renew = snap.reqRenew
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* AcquireJobStart
\* ---------------------------------------------------------------------------
AcquireJobStartIfLogged ==
    \E s \in Session, req \in RequestId :
        /\ IsNodeEvent("AcquireJobStart", s)
        /\ snap.req = req
        /\ b!AcquireJobStart(s, req)
        /\ reqInfo'[req].state = SAcquiring
        /\ req \in mintInFlight'
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* AcquireJobMintOk
\* ---------------------------------------------------------------------------
AcquireJobMintOkIfLogged ==
    \E req \in RequestId :
        /\ IsEvent("AcquireJobMintOk")
        /\ snap.req = req
        /\ b!AcquireJobMintOk(req)
        /\ reqInfo'[req].state = SRunning
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* AcquireJobMintFail
\* ---------------------------------------------------------------------------
AcquireJobMintFailIfLogged ==
    \E req \in RequestId :
        /\ IsEvent("AcquireJobMintFail")
        /\ snap.req = req
        /\ b!AcquireJobMintFail(req)
        /\ reqInfo'[req].state = STerminal
        /\ reqInfo'[req].result = RFailure
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* RenewJob
\* ---------------------------------------------------------------------------
RenewJobIfLogged ==
    \E s \in Session, req \in RequestId :
        /\ IsNodeEvent("RenewJob", s)
        /\ snap.req = req
        /\ b!RenewJob(s, req)
        /\ reqInfo'[req].renew = snap.reqRenew
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* CompleteJobSetResult
\* ---------------------------------------------------------------------------
CompleteJobSetResultIfLogged ==
    \E s \in Session, req \in RequestId, o \in {RSuccess, RFailure, RCancelled} :
        /\ IsNodeEvent("CompleteJobSetResult", s)
        /\ snap.req = req
        /\ b!CompleteJobSetResult(s, req, o)
        /\ reqInfo'[req].result = TraceResultOf(snap.reqResult)
        /\ sessionActive'[s] = NoReq
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* CompleteJobApply
\* ---------------------------------------------------------------------------
CompleteJobApplyIfLogged ==
    \E req \in RequestId, o \in {RSuccess, RFailure, RCancelled} :
        /\ IsEvent("CompleteJobApply")
        \* Identity binding: complete events are matched by their (run, job),
        \* not the harness's raw request number (see AzdoPollClaim note).
        /\ snap.run = reqInfo[req].run /\ snap.job = reqInfo[req].job
        /\ b!CompleteJobApply(req, o)
        /\ reqInfo'[req].state = STerminal
        /\ jobStatus'[reqInfo'[req].run][reqInfo'[req].job] =
            TraceStatus(snap.jobSt)
        /\ runStatus'[reqInfo'[req].run] = TraceStatus(snap.runSt)
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* ReapLease
\* ---------------------------------------------------------------------------
ReapLeaseIfLogged ==
    \E req \in RequestId :
        /\ IsEvent("ReapLease")
        /\ snap.req = req
        /\ b!ReapLease(req)
        /\ reqInfo'[req].state = STerminal
        /\ reqInfo'[req].result = RFailure
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* ReapTimeout
\* ---------------------------------------------------------------------------
ReapTimeoutIfLogged ==
    \E req \in RequestId :
        /\ IsEvent("ReapTimeout")
        /\ snap.req = req
        /\ b!ReapTimeout(req)
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* AzdoPollPopCancel (global pop — F1)
\* ---------------------------------------------------------------------------
AzdoPollPopCancelIfLogged ==
    \E s \in Session :
        /\ IsNodeEvent("AzdoPollPopCancel", s)
        /\ b!AzdoPollPopCancel(s)
        /\ Len(cancelQueue') = Len(cancelQueue) - 1
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* BrokerPollDeliverCancelScoped
\* ---------------------------------------------------------------------------
BrokerPollDeliverCancelScopedIfLogged ==
    \E s \in Session, req \in RequestId, c \in CancelRecDomain :
        /\ IsNodeEvent("BrokerPollDeliverCancelScoped", s)
        /\ snap.req = req
        /\ b!BrokerPollDeliverCancelScoped(s, req, c)
        /\ Len(cancelQueue') = Len(cancelQueue) - 1
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* BrokerPollDeliverCancelPool
\* ---------------------------------------------------------------------------
BrokerPollDeliverCancelPoolIfLogged ==
    \E s \in Session, req \in RequestId, c \in CancelRecDomain :
        /\ IsNodeEvent("BrokerPollDeliverCancelPool", s)
        /\ snap.req = req
        /\ b!BrokerPollDeliverCancelPool(s, req, c)
        /\ Len(cancelQueue') = Len(cancelQueue) - 1
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* CancelRun / CancelJob
\* ---------------------------------------------------------------------------
CancelRunIfLogged ==
    \E run \in RunId :
        /\ IsEvent("CancelRun")
        /\ snap.run = run
        /\ b!CancelRun(run)
        /\ runStatus'[run] = StCancelled
        /\ now' = snap.now
        /\ l' = l + 1

CancelJobIfLogged ==
    \E run \in RunId, job \in JobId :
        /\ IsEvent("CancelJob")
        /\ snap.run = run /\ snap.job = job
        /\ b!CancelJob(run, job)
        /\ jobStatus'[run][job] = StCancelled
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* Concurrency arrivals (S2)
\* ---------------------------------------------------------------------------
ArriveRunFreeIfLogged ==
    \E run \in RunId, key \in Key :
        /\ IsEvent("ArriveRunFree")
        /\ snap.run = run /\ snap.key = key
        /\ b!ArriveRunFree(run, key)
        /\ groups'[key].running.run = run
        /\ now' = snap.now
        /\ l' = l + 1

ArriveRunParkIfLogged ==
    \E run \in RunId, key \in Key :
        /\ IsEvent("ArriveRunPark")
        /\ snap.run = run /\ snap.key = key
        /\ b!ArriveRunPark(run, key)
        /\ \E h \in Range(groups'[key].pending) : h.run = run
        /\ now' = snap.now
        /\ l' = l + 1

ArriveJobFreeIfLogged ==
    \E run \in RunId, job \in JobId, key \in Key :
        /\ IsEvent("ArriveJobFree")
        /\ snap.run = run /\ snap.job = job /\ snap.key = key
        /\ b!ArriveJobFree(run, job, key)
        /\ <<run, job>> \in gateHeld'
        /\ now' = snap.now
        /\ l' = l + 1

ArriveJobParkIfLogged ==
    \E run \in RunId, job \in JobId, key \in Key, mode \in {ModeSingle, ModeMax},
            ph \in HolderSet :
        /\ IsEvent("ArriveJobPark")
        /\ snap.run = run /\ snap.job = job /\ snap.key = key
        /\ b!ArriveJobPark(run, job, key, mode, ph)
        /\ <<run, job>> \in Range(blockedJobs')
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* Promotions (S2)
\* ---------------------------------------------------------------------------
PromoteNextRunArmIfLogged ==
    \E key \in Key, run \in RunId :
        /\ IsEvent("PromoteNextRunArm")
        /\ snap.key = key /\ snap.run = run
        /\ b!PromoteNextRunArm(key, run)
        /\ groups'[key].running /= NoHolder
        /\ now' = snap.now
        /\ l' = l + 1

PromoteNextJobArmIfLogged ==
    \E key \in Key, run \in RunId, job \in JobId, mp \in BOOLEAN :
        /\ IsEvent("PromoteNextJobArm")
        /\ snap.key = key /\ snap.run = run /\ snap.job = job
        /\ b!PromoteNextJobArm(key, run, job, mp)
        /\ now' = snap.now
        /\ l' = l + 1

PromoteDispatchJobIfLogged ==
    \E run \in RunId, job \in JobId :
        /\ IsEvent("PromoteDispatchJob")
        /\ snap.run = run /\ snap.job = job
        /\ b!PromoteDispatchJob(run, job)
        /\ \E req \in Range(dispatchQueue') :
            reqInfo'[req].run = run /\ reqInfo'[req].job = job
        /\ now' = snap.now
        /\ l' = l + 1

PromoteReadyJobIfLogged ==
    \E run \in RunId, job \in JobId :
        /\ IsEvent("PromoteReadyJob")
        /\ snap.run = run /\ snap.job = job
        /\ b!PromoteReadyJob(run, job)
        /\ \E req \in Range(dispatchQueue') :
            reqInfo'[req].run = run /\ reqInfo'[req].job = job
        /\ now' = snap.now
        /\ l' = l + 1

FailFastIfLogged ==
    \E run \in RunId, failed \in JobId, siblings \in SUBSET JobId :
        /\ IsEvent("FailFast")
        /\ snap.run = run /\ snap.failed = failed
        /\ siblings = TraceSet(snap.siblings)
        /\ b!FailFast(run, failed, siblings)
        /\ \A j \in siblings : jobStatus'[run][j] = StCancelled
        /\ now' = snap.now
        /\ l' = l + 1

SkipJobIfLogged ==
    \E run \in RunId, job \in JobId :
        /\ IsEvent("SkipJob")
        /\ snap.run = run /\ snap.job = job
        /\ b!SkipJob(run, job)
        /\ jobStatus'[run][job] = StSkipped
        /\ now' = snap.now
        /\ l' = l + 1

EvalFailJobIfLogged ==
    \E run \in RunId, job \in JobId :
        /\ IsEvent("EvalFailJob")
        /\ snap.run = run /\ snap.job = job
        /\ b!EvalFailJob(run, job)
        /\ jobStatus'[run][job] = StFailure
        /\ now' = snap.now
        /\ l' = l + 1

ReleaseJobIfLogged ==
    \E run \in RunId, job \in JobId :
        /\ IsEvent("ReleaseJob")
        /\ snap.run = run /\ snap.job = job
        /\ b!ReleaseJob(run, job)
        /\ now' = snap.now
        /\ l' = l + 1

ReleaseRunIfLogged ==
    \E run \in RunId :
        /\ IsEvent("ReleaseRun")
        /\ snap.run = run
        /\ b!ReleaseRun(run)
        /\ holderKeys'[run] = {}
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* Deferred expansion (S3)
\* ---------------------------------------------------------------------------
DeferExpansionIfLogged ==
    \E run \in RunId, job \in JobId :
        /\ IsEvent("DeferExpansion")
        /\ snap.run = run /\ snap.job = job
        /\ b!DeferExpansion(run, job)
        /\ <<run, job>> \in expanding'
        /\ now' = snap.now
        /\ l' = l + 1

ApplyMatrixIfLogged ==
    \E run \in RunId, node \in JobId, fanout \in SUBSET JobId :
        /\ IsEvent("ApplyMatrix")
        /\ snap.run = run /\ snap.node = node
        /\ fanout = TraceSet(snap.fanout)
        /\ b!ApplyMatrix(run, node, fanout)
        /\ fanout \subseteq runJobs'[run]
        /\ \A j \in fanout : HasReq(run, j)
        /\ now' = snap.now
        /\ l' = l + 1

ApplyReusableIfLogged ==
    \E run \in RunId, caller \in JobId, fanout \in SUBSET JobId :
        /\ IsEvent("ApplyReusable")
        /\ snap.run = run /\ snap.caller = caller
        /\ fanout = snap.fanout
        /\ b!ApplyReusable(run, caller, fanout)
        /\ fanout \subseteq runJobs'[run]
        /\ now' = snap.now
        /\ l' = l + 1

BuildExpansionFailIfLogged ==
    \E run \in RunId, node \in JobId :
        /\ IsEvent("BuildExpansionFail")
        /\ snap.run = run /\ snap.node = node
        /\ b!BuildExpansionFail(run, node)
        /\ jobStatus'[run][node] = StFailure
        /\ now' = snap.now
        /\ l' = l + 1

BuildExpansionStartIfLogged ==
    \E run \in RunId, job \in JobId :
        /\ IsEvent("BuildExpansionStart")
        /\ snap.run = run /\ snap.job = job
        /\ b!BuildExpansionStart(run, job)
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* Runner listener pipeline (S4)
\* ---------------------------------------------------------------------------
ListenerDedupIfLogged ==
    \E s \in Session, m \in AllMsgIds :
        /\ IsNodeEvent("ListenerDedup", s)
        /\ snap.msg = m
        /\ b!ListenerDedup(s, m)
        /\ processed'[s] = TraceSet(snap.processed)
        /\ now' = snap.now
        /\ l' = l + 1

ListenerParseIfLogged ==
    \E s \in Session, m \in AllMsgIds, ok \in BOOLEAN :
        /\ IsNodeEvent("ListenerParse", s)
        /\ snap.msg = m
        /\ b!ListenerParse(s, m, ok)
        /\ parsed'[s] = TraceSet(snap.parsed)
        /\ now' = snap.now
        /\ l' = l + 1

ListenerAckMsgIfLogged ==
    \E s \in Session, m \in AllMsgIds :
        /\ IsNodeEvent("ListenerAckMsg", s)
        /\ snap.msg = m
        /\ b!ListenerAckMsg(s, m)
        /\ m \in acked'[s]
        /\ ~(m \in inflightMsgs'[s])
        /\ now' = snap.now
        /\ l' = l + 1

ListenerJobCancellationIfLogged ==
    \E s \in Session, m \in AllMsgIds, tr \in RunId, tj \in JobId, valid \in BOOLEAN :
        /\ IsNodeEvent("ListenerJobCancellation", s)
        /\ snap.msg = m
        /\ b!ListenerJobCancellation(s, m, tr, tj, valid)
        /\ listenerJob'[s] /= NoListener
        /\ listenerJob'[s].cancelSent = TRUE
        /\ now' = snap.now
        /\ l' = l + 1

ListenerRunnerJobRequestIfLogged ==
    \E s \in Session, m \in AllMsgIds, nr \in RequestId :
        /\ IsNodeEvent("ListenerRunnerJobRequest", s)
        /\ snap.msg = m /\ snap.req = nr
        /\ b!ListenerRunnerJobRequest(s, m, nr)
        /\ listenerJob'[s].req = nr
        /\ listenerJob'[s].shutdownSrc = OverlapShutdown
        /\ now' = snap.now
        /\ l' = l + 1

ListenerRunnerShutdownIfLogged ==
    \E s \in Session, m \in AllMsgIds :
        /\ IsNodeEvent("ListenerRunnerShutdown", s)
        /\ snap.msg = m
        /\ b!ListenerRunnerShutdown(s, m)
        /\ listenerJob'[s].shutdownSrc = MsgShutdown
        /\ now' = snap.now
        /\ l' = l + 1

ListenerShutdownSignalIfLogged ==
    \E s \in Session :
        /\ IsNodeEvent("ListenerShutdownSignal", s)
        /\ b!ListenerShutdownSignal(s)
        /\ listenerJob'[s].shutdownSrc = SigShutdown
        /\ listenerJob'[s].cancelSent = TRUE
        /\ now' = snap.now
        /\ l' = l + 1

ListenerKillTimerFiresIfLogged ==
    \E s \in Session :
        /\ IsNodeEvent("ListenerKillTimerFires", s)
        /\ b!ListenerKillTimerFires(s)
        /\ listenerJob'[s].workerAlive = FALSE
        /\ now' = snap.now
        /\ l' = l + 1

WorkerExitsIfLogged ==
    \E s \in Session, rep \in BOOLEAN :
        /\ IsNodeEvent("WorkerExits", s)
        /\ b!WorkerExits(s, rep)
        /\ workerReported'[s] = TraceBool(snap.workerReported)
        /\ listenerJob'[s] = NoListener
        /\ now' = snap.now
        /\ l' = l + 1

ListenerForceFailIfLogged ==
    \E s \in Session, ok \in BOOLEAN :
        /\ IsNodeEvent("ListenerForceFail", s)
        /\ b!ListenerForceFail(s, ok)
        /\ forceFailed'[s] = TRUE
        /\ now' = snap.now
        /\ l' = l + 1

WorkerCompletePostIfLogged ==
    \E s \in Session, o \in {RSuccess, RFailure, RCancelled}, ok \in BOOLEAN :
        /\ IsNodeEvent("WorkerCompletePost", s)
        /\ b!WorkerCompletePost(s, o, ok)
        /\ completeReported' = TraceBool(snap.completeReported)
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* Worker step queue (S5)
\* ---------------------------------------------------------------------------
WorkerQueueUpdateIfLogged ==
    \E st \in StepId, status \in Status, concl \in ResultSet :
        /\ IsEvent("WorkerQueueUpdate")
        /\ snap.step = st
        /\ b!WorkerQueueUpdate(st, status, concl)
        /\ steps'[st].status = TraceStatus(snap.stepSt)
        /\ steps'[st].conclusion = TraceResultOf(snap.stepConcl)
        /\ gen' = snap.gen
        /\ st \in dirty'
        /\ now' = snap.now
        /\ l' = l + 1

WorkerTakeBodyIfLogged ==
    /\ IsEvent("WorkerTakeBody")
    /\ b!WorkerTakeBody
    /\ dirty' = {}
    /\ changeOrder' = snap.changeOrder
    /\ now' = snap.now
    /\ l' = l + 1

WorkerPublishOkIfLogged ==
    /\ IsEvent("WorkerPublishOk")
    /\ b!WorkerPublishOk
    /\ pubGen' = snap.pubGen
    /\ now' = snap.now
    /\ l' = l + 1

WorkerPublishFailIfLogged ==
    /\ IsEvent("WorkerPublishFail")
    /\ b!WorkerPublishFail
    /\ pubGen' = snap.pubGen
    /\ httpUp' = FALSE
    /\ now' = snap.now
    /\ l' = l + 1

HttpFlapIfLogged ==
    \E v \in BOOLEAN :
        /\ IsEvent("HttpFlap")
        /\ b!HttpFlap(v)
        /\ httpUp' = TraceBool(snap.httpUp)
        /\ now' = snap.now
        /\ l' = l + 1

WorkerCancelStepIfLogged ==
    \E st \in StepId, armed \in BOOLEAN :
        /\ IsEvent("WorkerCancelStep")
        /\ snap.step = st
        /\ b!WorkerCancelStep(st, armed)
        /\ st \in cancelSentStep'
        /\ now' = snap.now
        /\ l' = l + 1

WorkerStepTimeoutIfLogged ==
    \E st \in StepId :
        /\ IsEvent("WorkerStepTimeout")
        /\ snap.step = st
        /\ b!WorkerStepTimeout(st)
        /\ steps'[st].killCause = KillTimeout
        /\ now' = snap.now
        /\ l' = l + 1

WorkerConcludeStepIfLogged ==
    \E st \in StepId, cs \in BOOLEAN :
        /\ IsEvent("WorkerConcludeStep")
        /\ snap.step = st
        /\ b!WorkerConcludeStep(st, cs)
        /\ steps'[st].conclusion = TraceResultOf(snap.stepConcl)
        /\ now' = snap.now
        /\ l' = l + 1

WorkerSetupWorkspaceIfLogged ==
    \E ok \in BOOLEAN :
        /\ IsEvent("WorkerSetupWorkspace")
        /\ b!WorkerSetupWorkspace(ok)
        /\ setupFailed' = TraceBool(snap.setupFailed)
        /\ now' = snap.now
        /\ l' = l + 1

WorkerRenewLoopAbortIfLogged ==
    /\ IsEvent("WorkerRenewLoopAbort")
    /\ b!WorkerRenewLoopAbort
    /\ renewAborted' = TRUE
    /\ now' = snap.now
    /\ l' = l + 1

WorkerFlushOutputIfLogged ==
    \E bytes \in 0..MaxChunk :
        /\ IsEvent("WorkerFlushOutput")
        /\ b!WorkerFlushOutput(bytes)
        /\ outputBytes' = snap.outputBytes
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* Protocol (S6)
\* ---------------------------------------------------------------------------
AddMaskIfLogged ==
    \E secret \in Str :
        /\ IsEvent("AddMask")
        /\ b!AddMask(secret)
        /\ secret \in maskSet'
        /\ now' = snap.now
        /\ l' = l + 1

EmitLineIfLogged ==
    \E secret \in Str, masked \in BOOLEAN :
        /\ IsEvent("EmitLine")
        /\ b!EmitLine(secret, masked)
        /\ Len(lines') = Len(lines) + 1
        /\ now' = snap.now
        /\ l' = l + 1

BuildFormatIfLogged ==
    \E lb \in BOOLEAN, ex \in BOOLEAN :
        /\ IsEvent("BuildFormat")
        /\ b!BuildFormat(lb, ex)
        /\ formatError' = TraceBool(snap.formatError)
        /\ now' = snap.now
        /\ l' = l + 1

ScanStepIfLogged ==
    \E odd \in BOOLEAN :
        /\ IsEvent("ScanStep")
        /\ b!ScanStep(odd)
        /\ scanState' = snap.scanState
        /\ now' = snap.now
        /\ l' = l + 1

SetEscapeBracesIfLogged ==
    \E v \in BOOLEAN :
        /\ IsEvent("SetEscapeBraces")
        /\ b!SetEscapeBraces(v)
        /\ escapeBraces' = TraceBool(snap.escapeBraces)
        /\ now' = snap.now
        /\ l' = l + 1

\* ---------------------------------------------------------------------------
\* TimeAdvance
\* ---------------------------------------------------------------------------
TimeAdvanceIfLogged ==
    /\ IsEvent("TimeAdvance")
    /\ b!TimeAdvance
    /\ now' = snap.now
    /\ l' = l + 1

\* ============================================================================
\* SILENT ACTIONS
\* ============================================================================

\* Un-instrumented clock ticks: advance now toward the next event's recorded
\* time without overshooting it (tightly constrained — no state-space blowup).
SilentTimeAdvance ==
    /\ l <= Len(TraceLog)
    /\ now < logline.event.state.now
    /\ b!TimeAdvance
    /\ l' = l


\* ============================================================================
\* TRACE NEXT
\* ============================================================================

TraceNext ==
    \/ SubmitRun2IfLogged
    \/ EnqueuePendingIfLogged
    \/ DeclareGateIfLogged
    \/ AzdoPollClaimIfLogged
    \/ BrokerRootClaimIfLogged
    \/ AcquireJobStartIfLogged
    \/ AcquireJobMintOkIfLogged
    \/ AcquireJobMintFailIfLogged
    \/ RenewJobIfLogged
    \/ CompleteJobSetResultIfLogged
    \/ CompleteJobApplyIfLogged
    \/ ReapLeaseIfLogged
    \/ ReapTimeoutIfLogged
    \/ AzdoPollPopCancelIfLogged
    \/ BrokerPollDeliverCancelScopedIfLogged
    \/ BrokerPollDeliverCancelPoolIfLogged
    \/ CancelRunIfLogged
    \/ CancelJobIfLogged
    \/ ArriveRunFreeIfLogged
    \/ ArriveRunParkIfLogged
    \/ ArriveJobFreeIfLogged
    \/ ArriveJobParkIfLogged
    \/ PromoteNextRunArmIfLogged
    \/ PromoteNextJobArmIfLogged
    \/ PromoteDispatchJobIfLogged
    \/ PromoteReadyJobIfLogged
    \/ FailFastIfLogged
    \/ SkipJobIfLogged
    \/ EvalFailJobIfLogged
    \/ ReleaseJobIfLogged
    \/ ReleaseRunIfLogged
    \/ DeferExpansionIfLogged
    \/ ApplyMatrixIfLogged
    \/ ApplyReusableIfLogged
    \/ BuildExpansionFailIfLogged
    \/ BuildExpansionStartIfLogged
    \/ ListenerDedupIfLogged
    \/ ListenerParseIfLogged
    \/ ListenerAckMsgIfLogged
    \/ ListenerJobCancellationIfLogged
    \/ ListenerRunnerJobRequestIfLogged
    \/ ListenerRunnerShutdownIfLogged
    \/ ListenerShutdownSignalIfLogged
    \/ ListenerKillTimerFiresIfLogged
    \/ WorkerExitsIfLogged
    \/ ListenerForceFailIfLogged
    \/ WorkerCompletePostIfLogged
    \/ WorkerQueueUpdateIfLogged
    \/ WorkerTakeBodyIfLogged
    \/ WorkerPublishOkIfLogged
    \/ WorkerPublishFailIfLogged
    \/ HttpFlapIfLogged
    \/ WorkerCancelStepIfLogged
    \/ WorkerStepTimeoutIfLogged
    \/ WorkerConcludeStepIfLogged
    \/ WorkerSetupWorkspaceIfLogged
    \/ WorkerRenewLoopAbortIfLogged
    \/ WorkerFlushOutputIfLogged
    \/ AddMaskIfLogged
    \/ EmitLineIfLogged
    \/ BuildFormatIfLogged
    \/ ScanStepIfLogged
    \/ SetEscapeBracesIfLogged
    \/ TimeAdvanceIfLogged
    \/ SilentTimeAdvance

\* ============================================================================
\* SPECIFICATION
\* ============================================================================

TraceVars == <<vars, l>>

TraceInit ==
    /\ Init
    /\ l = 1

TraceSpec == TraceInit /\ [][TraceNext]_TraceVars /\ WF_TraceVars(TraceNext)

\* ============================================================================
\* TRACE COMPLETION (must be a PROPERTY in Trace.cfg)
\* ============================================================================

TraceMatched == <>(l > Len(TraceLog))

\* ============================================================================
\* VIEW / ALIAS (referenced by Trace.cfg; must be module-level operators)
\* ============================================================================

TraceView == <<vars, l>>

TraceAlias == <<l>>

====================
