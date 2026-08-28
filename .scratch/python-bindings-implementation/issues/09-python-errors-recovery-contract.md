# 09 — Deliver the complete Python error and recovery contract

**What to build:** Give every public operation familiar Python filesystem exceptions that also preserve exhaustive protocol identity, operation outcome, completed bytes, and stable recovery guidance without string parsing or hidden retries.

**Blocked by:** 05 — Deliver read-only open files; 06 — Deliver writes, position, and durability; 07 — Deliver namespace mutations and facade conveniences; 08 — Deliver metadata mutation, xattrs, and filesystem information

**Status:** ready-for-agent

- [ ] The public exception hierarchy has tested built-in inheritance, serialization, immutable fields, and safe filename context.
- [ ] Every NFSv3/v4 status and Rust error category has an exhaustive centralized mapping.
- [ ] Safe-to-retry, definite failure, and uncertain outcomes expose stable recovery actions and authoritative completed bytes.
- [ ] Uncertain file position, lost open state, closed resources, mode violations, and aggregate close errors use dedicated public classes.
- [ ] Sync and async facade tests prove identical classification and no post-seam retry.
