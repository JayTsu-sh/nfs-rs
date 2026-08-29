# 06 — Deliver writes, position, and durability

**What to build:** Let users create, truncate, update, and append files with Python-compatible complete writes, safe buffer snapshots, deterministic position rules, dirty-range tracking, authoritative flush, and terminal close behavior.

**Blocked by:** 01 — Establish structured operation outcomes; 05 — Deliver read-only open files

**Status:** completed

- [x] All selected binary write/update/append modes enforce permissions locally and document non-transactional open effects.
- [x] High-level writes hide server partial writes, preserve confirmed byte counts, and snapshot input before detach or suspension.
- [x] Relative and positional writes follow the specified position and concurrency rules; append resolves current EOF per ordinary write.
- [x] Flush clears dirty state only after authoritative commit and handles verifier change or uncertainty safely.
- [x] File close completes all cleanup, stores one immutable failure report, and never performs network work on repeated terminal close.
