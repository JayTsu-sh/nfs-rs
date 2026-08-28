# 05 — Deliver read-only open files

**What to build:** Let users open existing files for binary reading and safely use ordinary and positional reads, writable-buffer reads, seeking, position inspection, context managers, and standard synchronous I/O composition.

**Blocked by:** 03 — Deliver minimal installable sync and async clients; 04 — Deliver paths, metadata, and directory browsing

**Status:** completed

- [x] Read-only open validates mode, acquires and registers state before returning, and cannot orphan state on cancellation.
- [x] Read, readinto, positional variants, seek, tell, and close have sync/async parity and negotiated chunking.
- [x] Relative operations serialize while positional reads do not alter logical position and may overlap.
- [x] Synchronous files implement the required raw-I/O contract and async files provide equivalent async behavior.
- [x] Buffer targets are never retained across detached or suspended network work.
