# 15 — Deliver immutable-artifact release and user documentation

**What to build:** Let maintainers publish one version-coupled, previously verified set of Rust and Python artifacts through a protected reproducible pipeline, with documentation that lets users install, connect, recover safely, and understand first-release limits.

**Blocked by:** 14 — Validate final artifacts across real protocols and performance gates

**Status:** ready-for-agent

- [ ] One version tag validates versions, builds all artifacts once, and publishes only the exact tested bytes through protected trusted publishing.
- [ ] Test registry preflight, dependency audits, checksums, available provenance, and partial-publication recovery use the same immutable artifacts.
- [ ] Documentation includes sync and async workflows, typing, binary I/O composition, reserved ports, chunked large transfers, cancellation, uncertain outcomes, and recovery events.
- [ ] Metadata accurately states supported Python/platform/protocol scope, experimental exact NFSv4.0 status, and absence of Kerberos/RPCSEC_GSS.
- [ ] A release cannot pass by skipping a required server, retrying correctness tests, quarantining flakes, or rebuilding after verification.
