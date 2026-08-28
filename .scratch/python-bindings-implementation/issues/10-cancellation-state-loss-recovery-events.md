# 10 — Deliver cancellation, lost open state, and recovery events

**What to build:** Preserve native asyncio cancellation while allowing core-owned sent work and cleanup to settle safely, invalidating files whose identity or state is lost, and exposing later uncertain outcomes through bounded redacted recovery events.

**Blocked by:** 02 — Establish the connected-client core and deterministic test seam; 09 — Deliver the complete Python error and recovery contract

**Status:** completed

- [x] Cancelling open, read, write, flush, file close, or client close follows the documented ownership and cleanup contract.
- [x] A file survives recovery only when remote identity and open state are proven; otherwise unsafe operations reject with guidance while close remains available.
- [x] Snapshot and atomic-drain accessors expose immutable redacted events without duplicating normally awaited errors.
- [x] Queue overflow increments a visible dropped-event count and memory remains bounded.
- [x] Deterministic barriers prove outcome, state, and cleanup behavior without timing sleeps.
