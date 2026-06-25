# libkrun Runner.Listener Conformance Plan

The integration harness is intentionally explicit because a real `Runner.Listener`
inside a libkrun microVM is the final compatibility gate.

1. Build or download the modified `Runner.Listener` from the pinned
   `ChristopherHX/runner.server` reference.
2. Boot a Preloop libkrun Linux microVM with the listener binary, a mounted
   workspace, and the host Preloop server URL.
3. Configure the listener against `/runner/server` using an ignored local token.
4. Submit the same workflow fixture to upstream `Runner.Server` and Preloop.
5. Compare expanded jobs, contexts, logs, annotations, cache hits/misses,
   artifact upload/download, outputs, failure states, cancellation, and reruns.
6. Record any deliberate Preloop differences in `docs/reference/runner-server.md`
   before accepting the fixture.

