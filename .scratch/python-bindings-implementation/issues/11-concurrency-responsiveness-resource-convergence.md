# 11 — Prove concurrency, responsiveness, and resource convergence

**What to build:** Make shared synchronous clients and loop-bound asynchronous clients remain responsive and leak-free under supported concurrency, cancellation, positional I/O, and close races.

**Blocked by:** 06 — Deliver writes, position, and durability; 10 — Deliver cancellation, lost open state, and recovery events

**Status:** ready-for-agent

- [ ] Python heartbeat tests prove blocking sync operations release the GIL and allowed work overlaps.
- [ ] Async event-loop tests prove same-loop concurrency, bounded lag, and deterministic cross-loop/thread rejection.
- [ ] Stress coverage exercises 32 sync threads, 128 async tasks, multiple clients/files, positional same-file work, and close races.
- [ ] Repeated connect, I/O, cancellation, and close cycles drain registries, tasks, sockets, file objects, runtimes, and continuing cleanup.
- [ ] Hangs, deadlocks, timeouts, leaked work, and linearly growing RSS fail deterministically.
