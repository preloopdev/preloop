# Provider Runner.Listener Conformance Plan

The integration harness is explicitly scoped because a real `Runner.Listener`
inside a provider host (container, libkrun microVM, etc.) is the final
compatibility gate.

1. Build or download the official `actions/runner` `Runner.Listener` binary.
2. Boot a provider host with the listener binary, a mounted workspace, and
   the aksh control plane URL. For Preloop: use a libkrun Linux microVM.
3. Configure the listener against aksh using an ignored local token.
4. Submit the same workflow fixture to upstream `runner.server` and aksh.
5. Compare expanded jobs, contexts, logs, annotations, cache hits/misses,
   artifact upload/download, outputs, failure states, cancellation, and reruns.
6. Record any deliberate aksh differences in `docs/reference/runner-server.md`
   before accepting the fixture.
