# 02 — Establish the connected-client core and deterministic test seam

**What to build:** Provide one lifecycle-owning connected-client core shared by future sync and async adapters, including its open-resource registry, continuing operations, cleanup result, recovery-event storage, and a deterministic protocol injection seam.

**Blocked by:** 01 — Establish structured operation outcomes

**Status:** completed

- [x] The connected client has explicit ready, closing, and closed behavior with one shared terminal cleanup result.
- [x] Open resources register through opaque keys and are drained in deterministic registration order.
- [x] Continuing work remains core-owned after a waiter disappears.
- [x] A test-only fake can stop operations at explicit lifecycle and transport barriers without sleeps.
- [x] Rust tests prove concurrent close, rejection of new work, cleanup ordering, and registry convergence.

Implemented by `b8a506b`, `b2600cc`, and `bef0d4f`. Final Standards and Spec reviews reported 0 findings.
