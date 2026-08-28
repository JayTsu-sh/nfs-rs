# 04 — Deliver paths, metadata, and directory browsing

**What to build:** Let Python users safely address export-relative POSIX paths, inspect objects, test existence, stream directories, collect directory names, and discover exports without hidden per-entry metadata calls.

**Blocked by:** 03 — Deliver minimal installable sync and async clients

**Status:** completed

- [x] String and path-like inputs normalize consistently on every host while bytes, NUL, and root escape fail locally.
- [x] Sync and async stat, exists, scandir, and listdir have matching values and errors.
- [x] Directory iteration is streaming and each entry carries complete immutable metadata without hidden stat work.
- [x] Export discovery works without a connected client and uses compatible connection validation.
- [x] Public-path and directory behavior passes deterministic facade and real-server tests.

Implemented by `0ff49c0` and `9b0c744`. The final installed wheel suite passes 38 tests, Rust passes 508 tests and strict Clippy, and final Standards and Spec reviews reported 0 findings. Real-server path tests are enabled by `NFS_RS_PYTHON_REAL_URL`.
