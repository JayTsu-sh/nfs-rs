# 01 — Establish structured operation outcomes

**What to build:** Make the Rust core report enough structured truth for every first-release Python operation to distinguish work that was not sent, safe retry, definite failure, and uncertain mutation without parsing strings or guessing from method names.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] Every first-release NFSv3, NFSv4.0, and NFSv4.1 operation has an explicit operation class and structured outcome.
- [ ] Modifying operations preserve sent state, recovery action, and authoritative completed bytes where applicable.
- [ ] Unclassified sent mutations fail conservatively as uncertain and cannot be retried as safe.
- [ ] Existing Rust callers remain compatible and deterministic tests cover all outcome transitions.
