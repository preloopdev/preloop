---- MODULE MC_TTrace_1785902609 ----
EXTENDS Sequences, TLCExt, MC, Toolbox, Naturals, TLC, MC_TEConstants

_expression ==
    LET MC_TEExpression == INSTANCE MC_TEExpression
    IN MC_TEExpression!expression
----

_trace ==
    LET MC_TETrace == INSTANCE MC_TETrace
    IN MC_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        hasGate = ((run1 :> (job1 :> nokey @@ job2 :> nokey @@ job3 :> nokey) @@ run2 :> (job1 :> nokey @@ job2 :> nokey @@ job3 :> nokey)))
        /\
        cancelSentStep = ({})
        /\
        expanding = ({})
        /\
        stepGroupsAlive = ({})
        /\
        workerReported = ((s1 :> FALSE @@ s2 :> FALSE))
        /\
        completeReported = (FALSE)
        /\
        faultCounters = ([submit |-> 1, arrive |-> 1, cancel |-> 0, time |-> 0, httpFail |-> 0, shutdown |-> 0, workerCrash |-> 0, parseFail |-> 0, postFail |-> 0, escapeSwitch |-> 0, maskFail |-> 0, crash |-> 0, output |-> 0, input |-> 0])
        /\
        renewAborted = (FALSE)
        /\
        escapeBraces = (TRUE)
        /\
        signalled = ({})
        /\
        jobAssign = ((run1 :> (job1 :> norunner @@ job2 :> norunner @@ job3 :> norunner) @@ run2 :> (job1 :> norunner @@ job2 :> norunner @@ job3 :> norunner)))
        /\
        scanState = ("Normal")
        /\
        msgIdHigh = (1000000)
        /\
        msgIdNext = (0)
        /\
        outputBytes = (0)
        /\
        steps = ((st1 :> [status |-> "StPending", conclusion |-> "RNone", killCause |-> "NoKill"] @@ st2 :> [status |-> "StPending", conclusion |-> "RNone", killCause |-> "NoKill"]))
        /\
        pubGen = (0)
        /\
        sessionRunner = ((s1 :> r1 @@ s2 :> r1))
        /\
        planReq = (<<1, 0, 0, 0, 0, 0, 0, 0>>)
        /\
        pendingExp = (<<>>)
        /\
        cancelQueue = (<<>>)
        /\
        parsed = ((s1 :> {} @@ s2 :> {}))
        /\
        ghostJob = ({})
        /\
        dispatchQueue = (<<>>)
        /\
        scannerDiverged = (FALSE)
        /\
        jobsetReady = ({})
        /\
        poolPending = ({})
        /\
        deliveredCancel = (<<>>)
        /\
        jobStatus = ((run1 :> (job1 :> "StSkipped" @@ job2 :> "StQueued" @@ job3 :> "StQueued") @@ run2 :> (job1 :> "StQueued" @@ job2 :> "StQueued" @@ job3 :> "StQueued")))
        /\
        maskSet = ({})
        /\
        listenerJob = ((s1 :> [run |-> norun, job |-> nojob, req |-> 0, workerAlive |-> FALSE, cancelSent |-> FALSE, killAt |-> -1, shutdownSrc |-> "NoShutdown"] @@ s2 :> [run |-> norun, job |-> nojob, req |-> 0, workerAlive |-> FALSE, cancelSent |-> FALSE, killAt |-> -1, shutdownSrc |-> "NoShutdown"]))
        /\
        inflightMsgs = ((s1 :> {} @@ s2 :> {}))
        /\
        inflightReq = ({1})
        /\
        cancelFlipped = ({})
        /\
        mintInFlight = ({})
        /\
        setupFailed = (FALSE)
        /\
        brokerMsg = (<<0, 0, 0, 0, 0, 0, 0, 0>>)
        /\
        formatError = (FALSE)
        /\
        gen = (0)
        /\
        httpUp = (TRUE)
        /\
        discardedCompletion = ({})
        /\
        now = (0)
        /\
        reqInfo = (<<[run |-> run1, job |-> job1, result |-> "RNone", state |-> "SQueued", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1]>>)
        /\
        timelineReq = (<<1, 0, 0, 0, 0, 0, 0, 0>>)
        /\
        gateHeld = ({})
        /\
        pendingJobs = (<<>>)
        /\
        lines = (<<>>)
        /\
        forceFailed = ((s1 :> FALSE @@ s2 :> FALSE))
        /\
        sessionActive = ((s1 :> 0 @@ s2 :> 0))
        /\
        agentJobReq = ((job1 :> 1 @@ job2 :> 0 @@ job3 :> 0))
        /\
        heldRuns = ((run1 :> {} @@ run2 :> {}))
        /\
        runStatus = ((run1 :> "StSuccess" @@ run2 :> "NoStatus"))
        /\
        runJobs = ((run1 :> {job1} @@ run2 :> {}))
        /\
        dirty = ({})
        /\
        blockedJobs = (<<>>)
        /\
        fanoutReqs = ({})
        /\
        groups = ((g1 :> [pending |-> <<>>, running |-> [run |-> run1, jobs |-> {}, job |-> nojob, kind |-> "HolderRun"]] @@ g2 :> [pending |-> <<>>, running |-> [run |-> norun, jobs |-> {}, job |-> nojob, kind |-> "NoHolder"]]))
        /\
        acked = ((s1 :> {} @@ s2 :> {}))
        /\
        processed = ((s1 :> {} @@ s2 :> {}))
        /\
        holderKeys = ((run1 :> {g1} @@ run2 :> {}))
        /\
        jobsetAdm = ((js1 :> [gates |-> {}, acquired |-> {}]))
        /\
        changeOrder = (0)
    )
----

_init ==
    /\ brokerMsg = _TETrace[1].brokerMsg
    /\ gateHeld = _TETrace[1].gateHeld
    /\ listenerJob = _TETrace[1].listenerJob
    /\ planReq = _TETrace[1].planReq
    /\ scannerDiverged = _TETrace[1].scannerDiverged
    /\ forceFailed = _TETrace[1].forceFailed
    /\ msgIdNext = _TETrace[1].msgIdNext
    /\ blockedJobs = _TETrace[1].blockedJobs
    /\ pendingJobs = _TETrace[1].pendingJobs
    /\ lines = _TETrace[1].lines
    /\ now = _TETrace[1].now
    /\ pendingExp = _TETrace[1].pendingExp
    /\ ghostJob = _TETrace[1].ghostJob
    /\ stepGroupsAlive = _TETrace[1].stepGroupsAlive
    /\ setupFailed = _TETrace[1].setupFailed
    /\ sessionActive = _TETrace[1].sessionActive
    /\ mintInFlight = _TETrace[1].mintInFlight
    /\ signalled = _TETrace[1].signalled
    /\ scanState = _TETrace[1].scanState
    /\ maskSet = _TETrace[1].maskSet
    /\ completeReported = _TETrace[1].completeReported
    /\ agentJobReq = _TETrace[1].agentJobReq
    /\ changeOrder = _TETrace[1].changeOrder
    /\ timelineReq = _TETrace[1].timelineReq
    /\ holderKeys = _TETrace[1].holderKeys
    /\ workerReported = _TETrace[1].workerReported
    /\ jobsetReady = _TETrace[1].jobsetReady
    /\ heldRuns = _TETrace[1].heldRuns
    /\ inflightReq = _TETrace[1].inflightReq
    /\ httpUp = _TETrace[1].httpUp
    /\ hasGate = _TETrace[1].hasGate
    /\ runStatus = _TETrace[1].runStatus
    /\ deliveredCancel = _TETrace[1].deliveredCancel
    /\ runJobs = _TETrace[1].runJobs
    /\ cancelSentStep = _TETrace[1].cancelSentStep
    /\ fanoutReqs = _TETrace[1].fanoutReqs
    /\ cancelFlipped = _TETrace[1].cancelFlipped
    /\ jobStatus = _TETrace[1].jobStatus
    /\ gen = _TETrace[1].gen
    /\ faultCounters = _TETrace[1].faultCounters
    /\ escapeBraces = _TETrace[1].escapeBraces
    /\ renewAborted = _TETrace[1].renewAborted
    /\ poolPending = _TETrace[1].poolPending
    /\ jobsetAdm = _TETrace[1].jobsetAdm
    /\ sessionRunner = _TETrace[1].sessionRunner
    /\ jobAssign = _TETrace[1].jobAssign
    /\ processed = _TETrace[1].processed
    /\ steps = _TETrace[1].steps
    /\ pubGen = _TETrace[1].pubGen
    /\ acked = _TETrace[1].acked
    /\ formatError = _TETrace[1].formatError
    /\ dirty = _TETrace[1].dirty
    /\ outputBytes = _TETrace[1].outputBytes
    /\ msgIdHigh = _TETrace[1].msgIdHigh
    /\ cancelQueue = _TETrace[1].cancelQueue
    /\ expanding = _TETrace[1].expanding
    /\ reqInfo = _TETrace[1].reqInfo
    /\ inflightMsgs = _TETrace[1].inflightMsgs
    /\ discardedCompletion = _TETrace[1].discardedCompletion
    /\ dispatchQueue = _TETrace[1].dispatchQueue
    /\ groups = _TETrace[1].groups
    /\ parsed = _TETrace[1].parsed
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ brokerMsg  = _TETrace[i].brokerMsg
        /\ brokerMsg' = _TETrace[j].brokerMsg
        /\ gateHeld  = _TETrace[i].gateHeld
        /\ gateHeld' = _TETrace[j].gateHeld
        /\ listenerJob  = _TETrace[i].listenerJob
        /\ listenerJob' = _TETrace[j].listenerJob
        /\ planReq  = _TETrace[i].planReq
        /\ planReq' = _TETrace[j].planReq
        /\ scannerDiverged  = _TETrace[i].scannerDiverged
        /\ scannerDiverged' = _TETrace[j].scannerDiverged
        /\ forceFailed  = _TETrace[i].forceFailed
        /\ forceFailed' = _TETrace[j].forceFailed
        /\ msgIdNext  = _TETrace[i].msgIdNext
        /\ msgIdNext' = _TETrace[j].msgIdNext
        /\ blockedJobs  = _TETrace[i].blockedJobs
        /\ blockedJobs' = _TETrace[j].blockedJobs
        /\ pendingJobs  = _TETrace[i].pendingJobs
        /\ pendingJobs' = _TETrace[j].pendingJobs
        /\ lines  = _TETrace[i].lines
        /\ lines' = _TETrace[j].lines
        /\ now  = _TETrace[i].now
        /\ now' = _TETrace[j].now
        /\ pendingExp  = _TETrace[i].pendingExp
        /\ pendingExp' = _TETrace[j].pendingExp
        /\ ghostJob  = _TETrace[i].ghostJob
        /\ ghostJob' = _TETrace[j].ghostJob
        /\ stepGroupsAlive  = _TETrace[i].stepGroupsAlive
        /\ stepGroupsAlive' = _TETrace[j].stepGroupsAlive
        /\ setupFailed  = _TETrace[i].setupFailed
        /\ setupFailed' = _TETrace[j].setupFailed
        /\ sessionActive  = _TETrace[i].sessionActive
        /\ sessionActive' = _TETrace[j].sessionActive
        /\ mintInFlight  = _TETrace[i].mintInFlight
        /\ mintInFlight' = _TETrace[j].mintInFlight
        /\ signalled  = _TETrace[i].signalled
        /\ signalled' = _TETrace[j].signalled
        /\ scanState  = _TETrace[i].scanState
        /\ scanState' = _TETrace[j].scanState
        /\ maskSet  = _TETrace[i].maskSet
        /\ maskSet' = _TETrace[j].maskSet
        /\ completeReported  = _TETrace[i].completeReported
        /\ completeReported' = _TETrace[j].completeReported
        /\ agentJobReq  = _TETrace[i].agentJobReq
        /\ agentJobReq' = _TETrace[j].agentJobReq
        /\ changeOrder  = _TETrace[i].changeOrder
        /\ changeOrder' = _TETrace[j].changeOrder
        /\ timelineReq  = _TETrace[i].timelineReq
        /\ timelineReq' = _TETrace[j].timelineReq
        /\ holderKeys  = _TETrace[i].holderKeys
        /\ holderKeys' = _TETrace[j].holderKeys
        /\ workerReported  = _TETrace[i].workerReported
        /\ workerReported' = _TETrace[j].workerReported
        /\ jobsetReady  = _TETrace[i].jobsetReady
        /\ jobsetReady' = _TETrace[j].jobsetReady
        /\ heldRuns  = _TETrace[i].heldRuns
        /\ heldRuns' = _TETrace[j].heldRuns
        /\ inflightReq  = _TETrace[i].inflightReq
        /\ inflightReq' = _TETrace[j].inflightReq
        /\ httpUp  = _TETrace[i].httpUp
        /\ httpUp' = _TETrace[j].httpUp
        /\ hasGate  = _TETrace[i].hasGate
        /\ hasGate' = _TETrace[j].hasGate
        /\ runStatus  = _TETrace[i].runStatus
        /\ runStatus' = _TETrace[j].runStatus
        /\ deliveredCancel  = _TETrace[i].deliveredCancel
        /\ deliveredCancel' = _TETrace[j].deliveredCancel
        /\ runJobs  = _TETrace[i].runJobs
        /\ runJobs' = _TETrace[j].runJobs
        /\ cancelSentStep  = _TETrace[i].cancelSentStep
        /\ cancelSentStep' = _TETrace[j].cancelSentStep
        /\ fanoutReqs  = _TETrace[i].fanoutReqs
        /\ fanoutReqs' = _TETrace[j].fanoutReqs
        /\ cancelFlipped  = _TETrace[i].cancelFlipped
        /\ cancelFlipped' = _TETrace[j].cancelFlipped
        /\ jobStatus  = _TETrace[i].jobStatus
        /\ jobStatus' = _TETrace[j].jobStatus
        /\ gen  = _TETrace[i].gen
        /\ gen' = _TETrace[j].gen
        /\ faultCounters  = _TETrace[i].faultCounters
        /\ faultCounters' = _TETrace[j].faultCounters
        /\ escapeBraces  = _TETrace[i].escapeBraces
        /\ escapeBraces' = _TETrace[j].escapeBraces
        /\ renewAborted  = _TETrace[i].renewAborted
        /\ renewAborted' = _TETrace[j].renewAborted
        /\ poolPending  = _TETrace[i].poolPending
        /\ poolPending' = _TETrace[j].poolPending
        /\ jobsetAdm  = _TETrace[i].jobsetAdm
        /\ jobsetAdm' = _TETrace[j].jobsetAdm
        /\ sessionRunner  = _TETrace[i].sessionRunner
        /\ sessionRunner' = _TETrace[j].sessionRunner
        /\ jobAssign  = _TETrace[i].jobAssign
        /\ jobAssign' = _TETrace[j].jobAssign
        /\ processed  = _TETrace[i].processed
        /\ processed' = _TETrace[j].processed
        /\ steps  = _TETrace[i].steps
        /\ steps' = _TETrace[j].steps
        /\ pubGen  = _TETrace[i].pubGen
        /\ pubGen' = _TETrace[j].pubGen
        /\ acked  = _TETrace[i].acked
        /\ acked' = _TETrace[j].acked
        /\ formatError  = _TETrace[i].formatError
        /\ formatError' = _TETrace[j].formatError
        /\ dirty  = _TETrace[i].dirty
        /\ dirty' = _TETrace[j].dirty
        /\ outputBytes  = _TETrace[i].outputBytes
        /\ outputBytes' = _TETrace[j].outputBytes
        /\ msgIdHigh  = _TETrace[i].msgIdHigh
        /\ msgIdHigh' = _TETrace[j].msgIdHigh
        /\ cancelQueue  = _TETrace[i].cancelQueue
        /\ cancelQueue' = _TETrace[j].cancelQueue
        /\ expanding  = _TETrace[i].expanding
        /\ expanding' = _TETrace[j].expanding
        /\ reqInfo  = _TETrace[i].reqInfo
        /\ reqInfo' = _TETrace[j].reqInfo
        /\ inflightMsgs  = _TETrace[i].inflightMsgs
        /\ inflightMsgs' = _TETrace[j].inflightMsgs
        /\ discardedCompletion  = _TETrace[i].discardedCompletion
        /\ discardedCompletion' = _TETrace[j].discardedCompletion
        /\ dispatchQueue  = _TETrace[i].dispatchQueue
        /\ dispatchQueue' = _TETrace[j].dispatchQueue
        /\ groups  = _TETrace[i].groups
        /\ groups' = _TETrace[j].groups
        /\ parsed  = _TETrace[i].parsed
        /\ parsed' = _TETrace[j].parsed

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("MC_TTrace_1785902609.json", _TETrace)

=============================================================================

 Note that you can extract this module `MC_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `MC_TEExpression.tla` file takes precedence 
  over the module `MC_TEExpression` below).

---- MODULE MC_TEExpression ----
EXTENDS Sequences, TLCExt, MC, Toolbox, Naturals, TLC, MC_TEConstants

expression == 
    [
        \* To hide variables of the `MC` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        brokerMsg |-> brokerMsg
        ,gateHeld |-> gateHeld
        ,listenerJob |-> listenerJob
        ,planReq |-> planReq
        ,scannerDiverged |-> scannerDiverged
        ,forceFailed |-> forceFailed
        ,msgIdNext |-> msgIdNext
        ,blockedJobs |-> blockedJobs
        ,pendingJobs |-> pendingJobs
        ,lines |-> lines
        ,now |-> now
        ,pendingExp |-> pendingExp
        ,ghostJob |-> ghostJob
        ,stepGroupsAlive |-> stepGroupsAlive
        ,setupFailed |-> setupFailed
        ,sessionActive |-> sessionActive
        ,mintInFlight |-> mintInFlight
        ,signalled |-> signalled
        ,scanState |-> scanState
        ,maskSet |-> maskSet
        ,completeReported |-> completeReported
        ,agentJobReq |-> agentJobReq
        ,changeOrder |-> changeOrder
        ,timelineReq |-> timelineReq
        ,holderKeys |-> holderKeys
        ,workerReported |-> workerReported
        ,jobsetReady |-> jobsetReady
        ,heldRuns |-> heldRuns
        ,inflightReq |-> inflightReq
        ,httpUp |-> httpUp
        ,hasGate |-> hasGate
        ,runStatus |-> runStatus
        ,deliveredCancel |-> deliveredCancel
        ,runJobs |-> runJobs
        ,cancelSentStep |-> cancelSentStep
        ,fanoutReqs |-> fanoutReqs
        ,cancelFlipped |-> cancelFlipped
        ,jobStatus |-> jobStatus
        ,gen |-> gen
        ,faultCounters |-> faultCounters
        ,escapeBraces |-> escapeBraces
        ,renewAborted |-> renewAborted
        ,poolPending |-> poolPending
        ,jobsetAdm |-> jobsetAdm
        ,sessionRunner |-> sessionRunner
        ,jobAssign |-> jobAssign
        ,processed |-> processed
        ,steps |-> steps
        ,pubGen |-> pubGen
        ,acked |-> acked
        ,formatError |-> formatError
        ,dirty |-> dirty
        ,outputBytes |-> outputBytes
        ,msgIdHigh |-> msgIdHigh
        ,cancelQueue |-> cancelQueue
        ,expanding |-> expanding
        ,reqInfo |-> reqInfo
        ,inflightMsgs |-> inflightMsgs
        ,discardedCompletion |-> discardedCompletion
        ,dispatchQueue |-> dispatchQueue
        ,groups |-> groups
        ,parsed |-> parsed
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_brokerMsgUnchanged |-> brokerMsg = brokerMsg'
        
        \* Format the `brokerMsg` variable as Json value.
        \* ,_brokerMsgJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(brokerMsg)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_brokerMsgModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].brokerMsg # _TETrace[s-1].brokerMsg
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE MC_TETrace ----
\*EXTENDS IOUtils, MC, TLC, MC_TEConstants
\*
\*trace == IODeserialize("MC_TTrace_1785902609.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE MC_TETrace ----
EXTENDS MC, TLC, MC_TEConstants

trace == 
    <<
    ([hasGate |-> (run1 :> (job1 :> nokey @@ job2 :> nokey @@ job3 :> nokey) @@ run2 :> (job1 :> nokey @@ job2 :> nokey @@ job3 :> nokey)),cancelSentStep |-> {},expanding |-> {},stepGroupsAlive |-> {},workerReported |-> (s1 :> FALSE @@ s2 :> FALSE),completeReported |-> FALSE,faultCounters |-> [submit |-> 0, arrive |-> 0, cancel |-> 0, time |-> 0, httpFail |-> 0, shutdown |-> 0, workerCrash |-> 0, parseFail |-> 0, postFail |-> 0, escapeSwitch |-> 0, maskFail |-> 0, crash |-> 0, output |-> 0, input |-> 0],renewAborted |-> FALSE,escapeBraces |-> TRUE,signalled |-> {},jobAssign |-> (run1 :> (job1 :> norunner @@ job2 :> norunner @@ job3 :> norunner) @@ run2 :> (job1 :> norunner @@ job2 :> norunner @@ job3 :> norunner)),scanState |-> "Normal",msgIdHigh |-> 1000000,msgIdNext |-> 0,outputBytes |-> 0,steps |-> (st1 :> [status |-> "StPending", conclusion |-> "RNone", killCause |-> "NoKill"] @@ st2 :> [status |-> "StPending", conclusion |-> "RNone", killCause |-> "NoKill"]),pubGen |-> 0,sessionRunner |-> (s1 :> r1 @@ s2 :> r1),planReq |-> <<0, 0, 0, 0, 0, 0, 0, 0>>,pendingExp |-> <<>>,cancelQueue |-> <<>>,parsed |-> (s1 :> {} @@ s2 :> {}),ghostJob |-> {},dispatchQueue |-> <<>>,scannerDiverged |-> FALSE,jobsetReady |-> {},poolPending |-> {},deliveredCancel |-> <<>>,jobStatus |-> (run1 :> (job1 :> "StQueued" @@ job2 :> "StQueued" @@ job3 :> "StQueued") @@ run2 :> (job1 :> "StQueued" @@ job2 :> "StQueued" @@ job3 :> "StQueued")),maskSet |-> {},listenerJob |-> (s1 :> [run |-> norun, job |-> nojob, req |-> 0, workerAlive |-> FALSE, cancelSent |-> FALSE, killAt |-> -1, shutdownSrc |-> "NoShutdown"] @@ s2 :> [run |-> norun, job |-> nojob, req |-> 0, workerAlive |-> FALSE, cancelSent |-> FALSE, killAt |-> -1, shutdownSrc |-> "NoShutdown"]),inflightMsgs |-> (s1 :> {} @@ s2 :> {}),inflightReq |-> {},cancelFlipped |-> {},mintInFlight |-> {},setupFailed |-> FALSE,brokerMsg |-> <<0, 0, 0, 0, 0, 0, 0, 0>>,formatError |-> FALSE,gen |-> 0,httpUp |-> TRUE,discardedCompletion |-> {},now |-> 0,reqInfo |-> <<[run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1]>>,timelineReq |-> <<0, 0, 0, 0, 0, 0, 0, 0>>,gateHeld |-> {},pendingJobs |-> <<>>,lines |-> <<>>,forceFailed |-> (s1 :> FALSE @@ s2 :> FALSE),sessionActive |-> (s1 :> 0 @@ s2 :> 0),agentJobReq |-> (job1 :> 0 @@ job2 :> 0 @@ job3 :> 0),heldRuns |-> (run1 :> {} @@ run2 :> {}),runStatus |-> (run1 :> "NoStatus" @@ run2 :> "NoStatus"),runJobs |-> (run1 :> {} @@ run2 :> {}),dirty |-> {},blockedJobs |-> <<>>,fanoutReqs |-> {},groups |-> (g1 :> [pending |-> <<>>, running |-> [run |-> norun, jobs |-> {}, job |-> nojob, kind |-> "NoHolder"]] @@ g2 :> [pending |-> <<>>, running |-> [run |-> norun, jobs |-> {}, job |-> nojob, kind |-> "NoHolder"]]),acked |-> (s1 :> {} @@ s2 :> {}),processed |-> (s1 :> {} @@ s2 :> {}),holderKeys |-> (run1 :> {} @@ run2 :> {}),jobsetAdm |-> (js1 :> [gates |-> {}, acquired |-> {}]),changeOrder |-> 0]),
    ([hasGate |-> (run1 :> (job1 :> nokey @@ job2 :> nokey @@ job3 :> nokey) @@ run2 :> (job1 :> nokey @@ job2 :> nokey @@ job3 :> nokey)),cancelSentStep |-> {},expanding |-> {},stepGroupsAlive |-> {},workerReported |-> (s1 :> FALSE @@ s2 :> FALSE),completeReported |-> FALSE,faultCounters |-> [submit |-> 1, arrive |-> 0, cancel |-> 0, time |-> 0, httpFail |-> 0, shutdown |-> 0, workerCrash |-> 0, parseFail |-> 0, postFail |-> 0, escapeSwitch |-> 0, maskFail |-> 0, crash |-> 0, output |-> 0, input |-> 0],renewAborted |-> FALSE,escapeBraces |-> TRUE,signalled |-> {},jobAssign |-> (run1 :> (job1 :> norunner @@ job2 :> norunner @@ job3 :> norunner) @@ run2 :> (job1 :> norunner @@ job2 :> norunner @@ job3 :> norunner)),scanState |-> "Normal",msgIdHigh |-> 1000000,msgIdNext |-> 0,outputBytes |-> 0,steps |-> (st1 :> [status |-> "StPending", conclusion |-> "RNone", killCause |-> "NoKill"] @@ st2 :> [status |-> "StPending", conclusion |-> "RNone", killCause |-> "NoKill"]),pubGen |-> 0,sessionRunner |-> (s1 :> r1 @@ s2 :> r1),planReq |-> <<1, 0, 0, 0, 0, 0, 0, 0>>,pendingExp |-> <<>>,cancelQueue |-> <<>>,parsed |-> (s1 :> {} @@ s2 :> {}),ghostJob |-> {},dispatchQueue |-> <<>>,scannerDiverged |-> FALSE,jobsetReady |-> {},poolPending |-> {},deliveredCancel |-> <<>>,jobStatus |-> (run1 :> (job1 :> "StQueued" @@ job2 :> "StQueued" @@ job3 :> "StQueued") @@ run2 :> (job1 :> "StQueued" @@ job2 :> "StQueued" @@ job3 :> "StQueued")),maskSet |-> {},listenerJob |-> (s1 :> [run |-> norun, job |-> nojob, req |-> 0, workerAlive |-> FALSE, cancelSent |-> FALSE, killAt |-> -1, shutdownSrc |-> "NoShutdown"] @@ s2 :> [run |-> norun, job |-> nojob, req |-> 0, workerAlive |-> FALSE, cancelSent |-> FALSE, killAt |-> -1, shutdownSrc |-> "NoShutdown"]),inflightMsgs |-> (s1 :> {} @@ s2 :> {}),inflightReq |-> {1},cancelFlipped |-> {},mintInFlight |-> {},setupFailed |-> FALSE,brokerMsg |-> <<0, 0, 0, 0, 0, 0, 0, 0>>,formatError |-> FALSE,gen |-> 0,httpUp |-> TRUE,discardedCompletion |-> {},now |-> 0,reqInfo |-> <<[run |-> run1, job |-> job1, result |-> "RNone", state |-> "SQueued", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1]>>,timelineReq |-> <<1, 0, 0, 0, 0, 0, 0, 0>>,gateHeld |-> {},pendingJobs |-> <<>>,lines |-> <<>>,forceFailed |-> (s1 :> FALSE @@ s2 :> FALSE),sessionActive |-> (s1 :> 0 @@ s2 :> 0),agentJobReq |-> (job1 :> 1 @@ job2 :> 0 @@ job3 :> 0),heldRuns |-> (run1 :> {} @@ run2 :> {}),runStatus |-> (run1 :> "StQueued" @@ run2 :> "NoStatus"),runJobs |-> (run1 :> {job1} @@ run2 :> {}),dirty |-> {},blockedJobs |-> <<>>,fanoutReqs |-> {},groups |-> (g1 :> [pending |-> <<>>, running |-> [run |-> norun, jobs |-> {}, job |-> nojob, kind |-> "NoHolder"]] @@ g2 :> [pending |-> <<>>, running |-> [run |-> norun, jobs |-> {}, job |-> nojob, kind |-> "NoHolder"]]),acked |-> (s1 :> {} @@ s2 :> {}),processed |-> (s1 :> {} @@ s2 :> {}),holderKeys |-> (run1 :> {} @@ run2 :> {}),jobsetAdm |-> (js1 :> [gates |-> {}, acquired |-> {}]),changeOrder |-> 0]),
    ([hasGate |-> (run1 :> (job1 :> nokey @@ job2 :> nokey @@ job3 :> nokey) @@ run2 :> (job1 :> nokey @@ job2 :> nokey @@ job3 :> nokey)),cancelSentStep |-> {},expanding |-> {},stepGroupsAlive |-> {},workerReported |-> (s1 :> FALSE @@ s2 :> FALSE),completeReported |-> FALSE,faultCounters |-> [submit |-> 1, arrive |-> 0, cancel |-> 0, time |-> 0, httpFail |-> 0, shutdown |-> 0, workerCrash |-> 0, parseFail |-> 0, postFail |-> 0, escapeSwitch |-> 0, maskFail |-> 0, crash |-> 0, output |-> 0, input |-> 0],renewAborted |-> FALSE,escapeBraces |-> TRUE,signalled |-> {},jobAssign |-> (run1 :> (job1 :> norunner @@ job2 :> norunner @@ job3 :> norunner) @@ run2 :> (job1 :> norunner @@ job2 :> norunner @@ job3 :> norunner)),scanState |-> "Normal",msgIdHigh |-> 1000000,msgIdNext |-> 0,outputBytes |-> 0,steps |-> (st1 :> [status |-> "StPending", conclusion |-> "RNone", killCause |-> "NoKill"] @@ st2 :> [status |-> "StPending", conclusion |-> "RNone", killCause |-> "NoKill"]),pubGen |-> 0,sessionRunner |-> (s1 :> r1 @@ s2 :> r1),planReq |-> <<1, 0, 0, 0, 0, 0, 0, 0>>,pendingExp |-> <<>>,cancelQueue |-> <<>>,parsed |-> (s1 :> {} @@ s2 :> {}),ghostJob |-> {},dispatchQueue |-> <<>>,scannerDiverged |-> FALSE,jobsetReady |-> {},poolPending |-> {},deliveredCancel |-> <<>>,jobStatus |-> (run1 :> (job1 :> "StQueued" @@ job2 :> "StQueued" @@ job3 :> "StQueued") @@ run2 :> (job1 :> "StQueued" @@ job2 :> "StQueued" @@ job3 :> "StQueued")),maskSet |-> {},listenerJob |-> (s1 :> [run |-> norun, job |-> nojob, req |-> 0, workerAlive |-> FALSE, cancelSent |-> FALSE, killAt |-> -1, shutdownSrc |-> "NoShutdown"] @@ s2 :> [run |-> norun, job |-> nojob, req |-> 0, workerAlive |-> FALSE, cancelSent |-> FALSE, killAt |-> -1, shutdownSrc |-> "NoShutdown"]),inflightMsgs |-> (s1 :> {} @@ s2 :> {}),inflightReq |-> {1},cancelFlipped |-> {},mintInFlight |-> {},setupFailed |-> FALSE,brokerMsg |-> <<0, 0, 0, 0, 0, 0, 0, 0>>,formatError |-> FALSE,gen |-> 0,httpUp |-> TRUE,discardedCompletion |-> {},now |-> 0,reqInfo |-> <<[run |-> run1, job |-> job1, result |-> "RNone", state |-> "SQueued", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1]>>,timelineReq |-> <<1, 0, 0, 0, 0, 0, 0, 0>>,gateHeld |-> {},pendingJobs |-> <<<<run1, job1>>>>,lines |-> <<>>,forceFailed |-> (s1 :> FALSE @@ s2 :> FALSE),sessionActive |-> (s1 :> 0 @@ s2 :> 0),agentJobReq |-> (job1 :> 1 @@ job2 :> 0 @@ job3 :> 0),heldRuns |-> (run1 :> {} @@ run2 :> {}),runStatus |-> (run1 :> "StQueued" @@ run2 :> "NoStatus"),runJobs |-> (run1 :> {job1} @@ run2 :> {}),dirty |-> {},blockedJobs |-> <<>>,fanoutReqs |-> {},groups |-> (g1 :> [pending |-> <<>>, running |-> [run |-> norun, jobs |-> {}, job |-> nojob, kind |-> "NoHolder"]] @@ g2 :> [pending |-> <<>>, running |-> [run |-> norun, jobs |-> {}, job |-> nojob, kind |-> "NoHolder"]]),acked |-> (s1 :> {} @@ s2 :> {}),processed |-> (s1 :> {} @@ s2 :> {}),holderKeys |-> (run1 :> {} @@ run2 :> {}),jobsetAdm |-> (js1 :> [gates |-> {}, acquired |-> {}]),changeOrder |-> 0]),
    ([hasGate |-> (run1 :> (job1 :> nokey @@ job2 :> nokey @@ job3 :> nokey) @@ run2 :> (job1 :> nokey @@ job2 :> nokey @@ job3 :> nokey)),cancelSentStep |-> {},expanding |-> {},stepGroupsAlive |-> {},workerReported |-> (s1 :> FALSE @@ s2 :> FALSE),completeReported |-> FALSE,faultCounters |-> [submit |-> 1, arrive |-> 1, cancel |-> 0, time |-> 0, httpFail |-> 0, shutdown |-> 0, workerCrash |-> 0, parseFail |-> 0, postFail |-> 0, escapeSwitch |-> 0, maskFail |-> 0, crash |-> 0, output |-> 0, input |-> 0],renewAborted |-> FALSE,escapeBraces |-> TRUE,signalled |-> {},jobAssign |-> (run1 :> (job1 :> norunner @@ job2 :> norunner @@ job3 :> norunner) @@ run2 :> (job1 :> norunner @@ job2 :> norunner @@ job3 :> norunner)),scanState |-> "Normal",msgIdHigh |-> 1000000,msgIdNext |-> 0,outputBytes |-> 0,steps |-> (st1 :> [status |-> "StPending", conclusion |-> "RNone", killCause |-> "NoKill"] @@ st2 :> [status |-> "StPending", conclusion |-> "RNone", killCause |-> "NoKill"]),pubGen |-> 0,sessionRunner |-> (s1 :> r1 @@ s2 :> r1),planReq |-> <<1, 0, 0, 0, 0, 0, 0, 0>>,pendingExp |-> <<>>,cancelQueue |-> <<>>,parsed |-> (s1 :> {} @@ s2 :> {}),ghostJob |-> {},dispatchQueue |-> <<>>,scannerDiverged |-> FALSE,jobsetReady |-> {},poolPending |-> {},deliveredCancel |-> <<>>,jobStatus |-> (run1 :> (job1 :> "StQueued" @@ job2 :> "StQueued" @@ job3 :> "StQueued") @@ run2 :> (job1 :> "StQueued" @@ job2 :> "StQueued" @@ job3 :> "StQueued")),maskSet |-> {},listenerJob |-> (s1 :> [run |-> norun, job |-> nojob, req |-> 0, workerAlive |-> FALSE, cancelSent |-> FALSE, killAt |-> -1, shutdownSrc |-> "NoShutdown"] @@ s2 :> [run |-> norun, job |-> nojob, req |-> 0, workerAlive |-> FALSE, cancelSent |-> FALSE, killAt |-> -1, shutdownSrc |-> "NoShutdown"]),inflightMsgs |-> (s1 :> {} @@ s2 :> {}),inflightReq |-> {1},cancelFlipped |-> {},mintInFlight |-> {},setupFailed |-> FALSE,brokerMsg |-> <<0, 0, 0, 0, 0, 0, 0, 0>>,formatError |-> FALSE,gen |-> 0,httpUp |-> TRUE,discardedCompletion |-> {},now |-> 0,reqInfo |-> <<[run |-> run1, job |-> job1, result |-> "RNone", state |-> "SQueued", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1]>>,timelineReq |-> <<1, 0, 0, 0, 0, 0, 0, 0>>,gateHeld |-> {},pendingJobs |-> <<<<run1, job1>>>>,lines |-> <<>>,forceFailed |-> (s1 :> FALSE @@ s2 :> FALSE),sessionActive |-> (s1 :> 0 @@ s2 :> 0),agentJobReq |-> (job1 :> 1 @@ job2 :> 0 @@ job3 :> 0),heldRuns |-> (run1 :> {} @@ run2 :> {}),runStatus |-> (run1 :> "StQueued" @@ run2 :> "NoStatus"),runJobs |-> (run1 :> {job1} @@ run2 :> {}),dirty |-> {},blockedJobs |-> <<>>,fanoutReqs |-> {},groups |-> (g1 :> [pending |-> <<>>, running |-> [run |-> run1, jobs |-> {}, job |-> nojob, kind |-> "HolderRun"]] @@ g2 :> [pending |-> <<>>, running |-> [run |-> norun, jobs |-> {}, job |-> nojob, kind |-> "NoHolder"]]),acked |-> (s1 :> {} @@ s2 :> {}),processed |-> (s1 :> {} @@ s2 :> {}),holderKeys |-> (run1 :> {g1} @@ run2 :> {}),jobsetAdm |-> (js1 :> [gates |-> {}, acquired |-> {}]),changeOrder |-> 0]),
    ([hasGate |-> (run1 :> (job1 :> nokey @@ job2 :> nokey @@ job3 :> nokey) @@ run2 :> (job1 :> nokey @@ job2 :> nokey @@ job3 :> nokey)),cancelSentStep |-> {},expanding |-> {},stepGroupsAlive |-> {},workerReported |-> (s1 :> FALSE @@ s2 :> FALSE),completeReported |-> FALSE,faultCounters |-> [submit |-> 1, arrive |-> 1, cancel |-> 0, time |-> 0, httpFail |-> 0, shutdown |-> 0, workerCrash |-> 0, parseFail |-> 0, postFail |-> 0, escapeSwitch |-> 0, maskFail |-> 0, crash |-> 0, output |-> 0, input |-> 0],renewAborted |-> FALSE,escapeBraces |-> TRUE,signalled |-> {},jobAssign |-> (run1 :> (job1 :> norunner @@ job2 :> norunner @@ job3 :> norunner) @@ run2 :> (job1 :> norunner @@ job2 :> norunner @@ job3 :> norunner)),scanState |-> "Normal",msgIdHigh |-> 1000000,msgIdNext |-> 0,outputBytes |-> 0,steps |-> (st1 :> [status |-> "StPending", conclusion |-> "RNone", killCause |-> "NoKill"] @@ st2 :> [status |-> "StPending", conclusion |-> "RNone", killCause |-> "NoKill"]),pubGen |-> 0,sessionRunner |-> (s1 :> r1 @@ s2 :> r1),planReq |-> <<1, 0, 0, 0, 0, 0, 0, 0>>,pendingExp |-> <<>>,cancelQueue |-> <<>>,parsed |-> (s1 :> {} @@ s2 :> {}),ghostJob |-> {},dispatchQueue |-> <<>>,scannerDiverged |-> FALSE,jobsetReady |-> {},poolPending |-> {},deliveredCancel |-> <<>>,jobStatus |-> (run1 :> (job1 :> "StSkipped" @@ job2 :> "StQueued" @@ job3 :> "StQueued") @@ run2 :> (job1 :> "StQueued" @@ job2 :> "StQueued" @@ job3 :> "StQueued")),maskSet |-> {},listenerJob |-> (s1 :> [run |-> norun, job |-> nojob, req |-> 0, workerAlive |-> FALSE, cancelSent |-> FALSE, killAt |-> -1, shutdownSrc |-> "NoShutdown"] @@ s2 :> [run |-> norun, job |-> nojob, req |-> 0, workerAlive |-> FALSE, cancelSent |-> FALSE, killAt |-> -1, shutdownSrc |-> "NoShutdown"]),inflightMsgs |-> (s1 :> {} @@ s2 :> {}),inflightReq |-> {1},cancelFlipped |-> {},mintInFlight |-> {},setupFailed |-> FALSE,brokerMsg |-> <<0, 0, 0, 0, 0, 0, 0, 0>>,formatError |-> FALSE,gen |-> 0,httpUp |-> TRUE,discardedCompletion |-> {},now |-> 0,reqInfo |-> <<[run |-> run1, job |-> job1, result |-> "RNone", state |-> "SQueued", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1], [run |-> norun, job |-> nojob, result |-> "RNone", state |-> "SNone", started |-> -1, renew |-> -1, deadline |-> -1]>>,timelineReq |-> <<1, 0, 0, 0, 0, 0, 0, 0>>,gateHeld |-> {},pendingJobs |-> <<>>,lines |-> <<>>,forceFailed |-> (s1 :> FALSE @@ s2 :> FALSE),sessionActive |-> (s1 :> 0 @@ s2 :> 0),agentJobReq |-> (job1 :> 1 @@ job2 :> 0 @@ job3 :> 0),heldRuns |-> (run1 :> {} @@ run2 :> {}),runStatus |-> (run1 :> "StSuccess" @@ run2 :> "NoStatus"),runJobs |-> (run1 :> {job1} @@ run2 :> {}),dirty |-> {},blockedJobs |-> <<>>,fanoutReqs |-> {},groups |-> (g1 :> [pending |-> <<>>, running |-> [run |-> run1, jobs |-> {}, job |-> nojob, kind |-> "HolderRun"]] @@ g2 :> [pending |-> <<>>, running |-> [run |-> norun, jobs |-> {}, job |-> nojob, kind |-> "NoHolder"]]),acked |-> (s1 :> {} @@ s2 :> {}),processed |-> (s1 :> {} @@ s2 :> {}),holderKeys |-> (run1 :> {g1} @@ run2 :> {}),jobsetAdm |-> (js1 :> [gates |-> {}, acquired |-> {}]),changeOrder |-> 0])
    >>
----


=============================================================================

---- MODULE MC_TEConstants ----
EXTENDS MC

CONSTANTS r1, r2, s1, s2, run1, run2, job1, job2, job3, g1, g2, js1, st1, st2, norun, nojob, norunner, nokey

=============================================================================

---- CONFIG MC_TTrace_1785902609 ----
CONSTANTS
    Runner = { r1 , r2 }
    Session = { s1 , s2 }
    RunId = { run1 , run2 }
    JobId = { job1 , job2 , job3 }
    RequestId = { 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 }
    Key = { g1 , g2 }
    JobSetId = { js1 }
    StepId = { st1 , st2 }
    Str = { "s1" , "s2" }
    NoRun = norun
    NoJob = nojob
    NoRunner = norunner
    NoKey = nokey
    MESSAGE_ID_BASE = 1000000
    MaxLowMsgs = 12
    MaxHighMsgs = 4
    LeaseSeconds = 3
    JobTimeout = 4
    MaxOutputBytes = 1048576
    MaxChunk = 4
    RequireAssignments = FALSE
    StQueued = "StQueued"
    StPending = "StPending"
    StInProgress = "StInProgress"
    StSuccess = "StSuccess"
    StFailure = "StFailure"
    StCancelled = "StCancelled"
    StSkipped = "StSkipped"
    SNone = "SNone"
    SQueued = "SQueued"
    SClaimed = "SClaimed"
    SAcquiring = "SAcquiring"
    SRunning = "SRunning"
    STerminal = "STerminal"
    RNone = "RNone"
    RSuccess = "RSuccess"
    RFailure = "RFailure"
    RCancelled = "RCancelled"
    RSkipped = "RSkipped"
    HolderRun = "HolderRun"
    HolderJob = "HolderJob"
    HolderJobSet = "HolderJobSet"
    ModeSingle = "ModeSingle"
    ModeMax = "ModeMax"
    NoShutdown = "NoShutdown"
    SigShutdown = "SigShutdown"
    MsgShutdown = "MsgShutdown"
    OverlapShutdown = "OverlapShutdown"
    NoKill = "NoKill"
    KillCancel = "KillCancel"
    KillTimeout = "KillTimeout"
    KillBoth = "KillBoth"
    Normal = "Normal"
    InString = "InString"
    MaxSubmitLimit = 1
    MaxArriveLimit = 2
    MaxCancelLimit = 1
    MaxTimeLimit = 0
    MaxHttpFailLimit = 0
    MaxShutdownLimit = 0
    MaxWorkerCrashLimit = 0
    MaxParseFailLimit = 0
    MaxPostFailLimit = 0
    MaxEscapeSwitchLimit = 0
    MaxMaskFailLimit = 0
    MaxOutputLimit = 0
    MaxInputLimit = 0
    MaxCrashLimit = 0
    MaxCancelQueue = 4
    MaxDispatchQueue = 4
    MaxPendingJobs = 4
    r2 = r2
    g2 = g2
    js1 = js1
    run1 = run1
    norun = norun
    run2 = run2
    r1 = r1
    job2 = job2
    nojob = nojob
    job3 = job3
    norunner = norunner
    st2 = st2
    job1 = job1
    st1 = st1
    s2 = s2
    s1 = s1
    g1 = g1
    nokey = nokey

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Wed Aug 05 00:03:31 EDT 2026