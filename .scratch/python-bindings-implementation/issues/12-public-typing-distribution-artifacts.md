# 12 — Complete public typing and distribution artifacts

**What to build:** Produce the stable typed facade and cleanly installable Linux artifacts promised to users, with no accidental private API or unexpected native dependency.

**Blocked by:** 08 — Deliver metadata mutation, xattrs, and filesystem information; 09 — Deliver the complete Python error and recovery contract; 10 — Deliver cancellation, lost open state, and recovery events; 11 — Prove concurrency, responsiveness, and resource convergence

**Status:** completed

- [x] Public exports, stubs, typing marker, immutable value shapes, enums, and exception MRO match runtime behavior on boundary Python versions.
- [x] The preferred distribution and stable import naming policy is implemented with version coupling to the Rust release.
- [x] Audited abi3 wheels build for manylinux x86_64 and aarch64 without unexpected external libraries.
- [x] A complete Linux/glibc source distribution builds and passes sync/async smoke tests in a clean environment.
- [x] Extension-load failures preserve their cause and add actionable platform and ABI context.
