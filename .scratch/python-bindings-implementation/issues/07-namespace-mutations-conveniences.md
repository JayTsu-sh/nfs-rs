# 07 — Deliver namespace mutations and facade conveniences

**What to build:** Let Python users create and remove directories and entries, rename and link objects, manage symlinks, and use common whole-file conveniences while retaining explicit non-transactional and uncertain-mutation behavior.

**Blocked by:** 01 — Establish structured operation outcomes; 04 — Deliver paths, metadata, and directory browsing

**Status:** completed

- [x] Sync and async namespace primitives have matching arguments, results, and structured failures.
- [x] Parent creation, existing-directory handling, and removal semantics suppress only their documented conditions.
- [x] Symlink target text preserves caller POSIX meaning while lookup paths remain normalized.
- [x] Touch, read-all, and write-all conveniences compose public primitives and do not bypass lifecycle or outcome rules.
- [x] Before-send and after-send mutation cases never trigger blind Adapter retry.
