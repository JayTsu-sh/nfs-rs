# 14 — Validate final artifacts across real protocols and performance gates

**What to build:** Prove that the exact wheel and source artifacts behave correctly and within accepted responsiveness, memory, and performance bounds on the required real NFS and architecture matrix.

**Blocked by:** 11 — Prove concurrency, responsiveness, and resource convergence; 12 — Complete public typing and distribution artifacts; 13 — Establish deterministic contract and fault gates

**Status:** completed

- [x] Common public scenarios pass on required NFSv3, exact NFSv4.0, NFSv4.1, and NetApp pNFS fixtures with unique run directories and cleanup.
- [x] Required unavailable server capability fails closed; negotiation fallback is tested separately.
- [x] x86_64 validates final wheel and source artifacts and aarch64 validates final-wheel installation plus real-protocol smoke behavior.
- [x] Buffer memory, RSS plateau, GIL heartbeat, event-loop lag, throughput, and latency gates meet the accepted baseline policy.
- [x] Five comparable performance runs yield at least four valid samples and a regression over ten percent blocks release.
