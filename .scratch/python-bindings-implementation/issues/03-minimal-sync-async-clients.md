# 03 — Deliver minimal installable sync and async clients

**What to build:** Let a Python user install a development artifact, connect synchronously or through asyncio, inspect the selected version and health, and close deterministically through explicit calls or context managers.

**Blocked by:** 02 — Establish the connected-client core and deterministic test seam

**Status:** ready-for-agent

- [ ] The stable facade exposes only class-level sync and async connection factories with matching validated options.
- [ ] Sync calls use a client-owned bounded runtime and release the GIL; async calls use the process bridge and bind to their creating loop.
- [ ] Version and health are immutable local snapshots with redacted representations.
- [ ] Explicit and context-manager close are deterministic, idempotent, and behaviorally equivalent across client kinds.
- [ ] An installed development artifact passes sync and async connect/inspect/close tests.
