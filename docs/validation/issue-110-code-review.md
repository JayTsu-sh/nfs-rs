# Issue #110: pre-release code review

Review baseline: `c393c94330a1375ca58a3f4e6c4d8a1cce16366e`.
Scope: owned single-response reads, the memory diagnostic and lab runner,
regressions, evidence documents, and preparation of patch release 0.11.2.
The user authorized review, merge to GitHub `main`, and publication to both
crates.io and PyPI. Standards and Spec were reviewed independently.

## Standards

The initial review identified one P2: sending SIGTERM only to the lab wrapper
could run cleanup before the still-running controller/readers had stopped.
An isolated stub fixture reproduced subsequent child I/O after cleanup and an
incorrect success status. No physical NFS server was contacted by this review.

The wrapper now handles INT/TERM explicitly, terminates and waits for the
controller before deleting storage, and preserves an interruption exit status.
During preparation it waits for the helper to establish ownership; a failed
prepare does not authorize deleting another run's directory. Six stub
regressions cover TERM, INT, normal completion, controller failure, failed
preparation and interruption during preparation. The old implementation failed
two and timed out on one; the corrected implementation passed all six.

Independent re-review passed: six runner tests, six report/barrier tests,
four Rust CLI integration tests, shell syntax and whitespace checks. Cargo,
lockfile, changelog, dynamic Python version, tagged documentation links and
mirrored guides agree on 0.11.2. No remaining actionable standards or code-smell
findings were reported.

## Spec

The implemented behavior matches the approved recommendations 2 and 3:
single owned responses avoid a payload copy; multiple pieces preserve
contiguous, ordered results, short reads, EOF, errors and resource lifetime.
The diagnostic shares pattern backing across protocols, separates fresh-process
reads from writes, covers QD1/QD8 and synchronized readers, and reports RSS
separately from live and cumulative allocations.

The reviewer independently re-analyzed the 520 non-smoke process reports and
verified report counts, binary hashes and the maximum whole-live tail range of
5128 bytes. No additional physical workloads were run by the reviewer.
No missing requirements, scope creep or incorrect implementation were found.
The signal cleanup fix and 0.11.2 metadata received a follow-up review; all six
runner tests and four Rust CLI integration tests passed.

The original long-running RSS residency is not claimed resolved. Supplemental
memory measurements do not replace accepted per-environment release performance
gates. Artifact validation, registry publication and release completion remain
the responsibility of the tag-triggered immutable release workflow.

Standards: one P2 found and resolved, zero remaining; Spec: zero findings.

After the review fix and version update, local verification passed formatting,
Clippy, 739 Rust all-target tests, 48 doctests and the reliability-results check.
The newly built 0.11.2 test-support wheel passed 398 installed-adapter Python
tests and stubtest; the one Python physical-URL test and 30 opt-in Rust lab
tests were not claimed executed by this local gate. Final release artifacts and
the full physical release gate are validated separately by the release workflow.
