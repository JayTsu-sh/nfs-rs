# 04 — Deliver paths, metadata, and directory browsing

**What to build:** Let Python users safely address export-relative POSIX paths, inspect objects, test existence, stream directories, collect directory names, and discover exports without hidden per-entry metadata calls.

**Blocked by:** 03 — Deliver minimal installable sync and async clients

**Status:** ready-for-agent

- [ ] String and path-like inputs normalize consistently on every host while bytes, NUL, and root escape fail locally.
- [ ] Sync and async stat, exists, scandir, and listdir have matching values and errors.
- [ ] Directory iteration is streaming and each entry carries complete immutable metadata without hidden stat work.
- [ ] Export discovery works without a connected client and uses compatible connection validation.
- [ ] Public-path and directory behavior passes deterministic facade and real-server tests.
