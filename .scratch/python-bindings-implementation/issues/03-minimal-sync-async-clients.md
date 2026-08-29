# 03 — Deliver minimal installable sync and async clients

**What to build:** Let a Python user install a development artifact, connect synchronously or through asyncio, inspect the selected version and health, and close deterministically through explicit calls or context managers.

**Blocked by:** 02 — Establish the connected-client core and deterministic test seam

**Status:** completed

- [x] The stable facade exposes only class-level sync and async connection factories with matching validated options.
- [x] Sync calls use a client-owned bounded runtime and release the GIL; async calls use the process bridge and bind to their creating loop.
- [x] Version and health are immutable local snapshots with redacted representations.
- [x] Explicit and context-manager close are deterministic, idempotent, and behaviorally equivalent across client kinds.
- [x] An installed development artifact passes sync and async connect/inspect/close tests.

Implemented by `cb0983b`, `666c52a`, `4bcf3c0`, `1d869db`, and `4a8c0f4`. The installed wheel suite passes 16 tests; Rust passes 508 tests and strict Clippy. Final Standards and Spec reviews reported 0 findings.
