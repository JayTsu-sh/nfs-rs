# 08 — Deliver metadata mutation, xattrs, and filesystem information

**What to build:** Let Python users update ordinary metadata, check access, manipulate supported extended attributes, and inspect filesystem space, capabilities, and negotiated I/O limits through immutable protocol-neutral values.

**Blocked by:** 04 — Deliver paths, metadata, and directory browsing

**Status:** ready-for-agent

- [ ] Sync and async chmod, chown, nanosecond utime, path truncate, and access have semantic parity.
- [ ] Supported xattr operations round-trip values and unsupported protocol paths raise the structured unsupported category.
- [ ] Filesystem and I/O information is immutable, protocol neutral, and omits raw handles and channel state.
- [ ] Capabilities report unsupported features honestly rather than silently emulating them.
- [ ] Deterministic and real-server tests cover success, permission, unsupported, and protocol failure cases.
