# 13 — Establish deterministic contract and fault gates

**What to build:** Make the installed facade pass one authoritative sync/async contract suite and deterministic fault matrix covering every public method, exception, lifecycle transition, mutation outcome, and cancellation boundary.

**Blocked by:** 09 — Deliver the complete Python error and recovery contract; 10 — Deliver cancellation, lost open state, and recovery events; 12 — Complete public typing and distribution artifacts

**Status:** ready-for-agent

- [ ] Every public method has matching sync/async success and failure coverage through the installed facade.
- [ ] Fault barriers cover before-send and after-send/before-response for every first-release mutation.
- [ ] Partial writes, commit-verifier changes, stale identity, lease/session loss, callbacks, and pNFS data-path failures assert structured outcomes.
- [ ] Correctness tests use deterministic synchronization, never automatic retry or enlarged timeout to mask failure.
- [ ] Failure artifacts contain sanitized protocol, environment, seed, and barrier-phase context sufficient for reproduction.
