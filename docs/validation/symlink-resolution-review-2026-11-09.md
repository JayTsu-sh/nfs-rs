# Symlink resolution pre-merge review — 2026-11-09

Scope: the complete branch diff from `main@d46bb833` through the symlink feature
`b789528` and encoding correction `d31d603`. This includes the four pending
metadata/replay corrections recorded in the 0.12.0 changelog. Documentation and
release metadata are updated together for 0.12.0.

## Standards

The independent Standards review found no actionable violations. Architecture
review confirmed that traversal, limits and resolution states belong to the shared
`path_resolution` module; protocol engines retain wire decoding, replay eligibility
and session/sequence responsibility. Attribute presence and ACL selection remain
centralized in shared NFSv4 metadata code. Python adapters handle overall deadlines,
cancellation and translation, and the facade/stubs expose the same result contract.

## Spec

One P1 was found and resolved: READLINK in NFSv3 and NFSv4.1/v4.2 replaced invalid
UTF-8 bytes with U+FFFD. Following the replacement text could incorrectly report a
missing target or resolve a different filename. The protocol READLINK String
boundaries now reject unrepresentable targets with a typed encoding error, matching
NFSv4.0; unrelated directory decoding is unchanged.

Two actual TCP RPC/COMPOUND regressions failed against the previous implementation
with `Ok("�")` and passed after the correction. They cover ordinary UTF-8,
non-ASCII text, a legitimate U+FFFD name, invalid bytes and overlong encoding.
The installed-wheel tests also verify Python error classification and context.
Both review axes rechecked the correction and reported no remaining findings.

## Functional evidence

Before the encoding correction, the ordinary current-source wheel from `b789528`
was compared with kernel NFS observations in isolated lab directories:

| Environment | Protocols | Server/protocol combinations |
|---|---|---:|
| Linux source | NFSv3, NFSv4.0, NFSv4.1 | 3 |
| AFX, four independent data LIFs | NFSv4.1, NFSv4.2 | 8 |

All 440 synchronous/asynchronous semantic checks and 176 concurrent asynchronous
queries passed. Eleven permission checks preserved typed errors. Cases included
ordinary files/directories, final and intermediate links, relative targets,
symlink-aware `..`, trailing slashes, missing targets, non-directory components,
absolute/out-of-mount targets, self/mutual cycles and the 40/41-link boundary.
Successful final paths, types and file sizes matched kernel observations.

AFX preparation and cleanup passed on its data-network executor. The initial Linux
cleanup could not remove kernel-created nested fixtures under the runner identity;
their contents were removed with the matching NFS identity, repository cleanup was
rerun successfully, and all run-scoped lab directories were verified absent.
Temporary executor tools and credential copies were removed. Raw logs and JSON
remain outside the checkout.

The corrected source passed the new wire regressions, clippy with Python bindings,
424 installed-wheel Python tests (one separate physical fixture module skipped),
stubtest and documentation/governance checks. The pre-merge lab wheel is a diagnostic
manylinux_2_34 build, not a publishable release artifact. Malformed target bytes were
checked with deterministic production wire decoders; they were not injected into
the physical server fixtures. No new physical network-fault result is claimed here.

## Release boundary

The tag-triggered immutable release workflow must validate the final 0.12.0 crate,
manylinux2014 wheel and source distribution, including its required physical-lab
and performance gates, before uploading those exact artifacts. The pre-merge checks
above supplement that workflow and do not replace its evidence or prove registry
publication. Existing tags and published artifacts remain immutable.
