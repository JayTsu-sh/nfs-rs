# Bounded retry validation — 2026-11-02

The change implements the fixed policy described in
[bounded retries](../bounded-retries.md) against main at
`8f8734e72c6df34426c43967332142c6d476a83a`. There is no retry-off mode or new
connection option. The shared scheduler lives in `src/retry.rs`; protocol
engines retain responsibility for replay safety and request identities.

## Reproductions and protocol coverage

Before the correction, the new NFSv3 READ regression returned JUKEBOX after
one request, and the v4.0 READ regression returned DELAY after one request.
Both regressions now succeed within one public read call. Existing successful
and NOENT paths remain single-request controls.

Local TCP fixtures exercise production RPC/XDR paths for:

- NFSv3 READ, GETATTR and native NFSACL GETACL/SETACL: JUKEBOX then
  success, with fresh RPC XIDs.
- NFSv4.0 anonymous and already-OPEN READ: DELAY/GRACE then success.
- NFSv4.1 and v4.2 MDS and independent DS request paths: DELAY on SEQUENCE
  retains slot/sequence identity; DELAY after SEQUENCE success advances the
  sequence. RPC XIDs are fresh on reissue and each fixture checks its exact
  minor version on the wire.
- DS WRITE in minor versions 1 and 2: DELAY/GRACE then success, preserving
  write data and using a fresh sequence after the rejected operation.
- Completed ALLOCATE followed by GETATTR DELAY: no mutation replay;
  Uncertain/VerifyThenResume is returned.
- Mismatched busy-response opcodes in v4.0/v4.1/v4.2: rejected without
  issuing another request; malformed v4.0 mutation replies retain uncertainty.
- Cancellation during NFSv3 read backoff: no subsequent READ is sent.
- Public pNFS read routing: cold LAYOUTGET/GETDEVICEINFO exhaustion and
  cancelled DS backoff return without layout refresh or MDS fallback.
- Public pNFS WRITE routing: wholly busy-rejected batches retain ServerBusy;
  short success followed by exhaustion remains Uncertain without fallback.
- Mixed session recovery and DELAY retain the structured busy exhaustion and
  its independent retry count. Nested aggregate uncertainty overrides busy.

Shared-policy tests use Tokio's paused clock to verify 16-reissue exhaustion,
equal-jitter bounds and cap, elapsed-time exhaustion, nested budget sharing,
shorter Python deadlines, inheritance across detached tasks, cancelled Python
read backoff and separately bounded temporary-OPEN cleanup. The longer private
clock window in the retry-count test isolates the retry bound from the elapsed
bound; it is not a configurable production policy.

The new shared-policy tests replace the old v4.1-only jitter tests. No
reliability mapping, benchmark threshold or lab acceptance gate was weakened.

## Deterministic gates

- Rust formatting and clippy with Python bindings: passed.
- Rust all-target tests: 716 passed; 30 physical-lab tests ignored in that command.
- Python-bindings library tests: 685 passed.
- Rust documentation tests: 47 passed.
- Dedicated venv with a freshly built and installed current-source test-support
  wheel: 392 Python tests passed, one separate real-server fixture module skipped.
- Installed-wheel stubtest: passed with the existing allowlist.
- Concrete reliability mappings: passed; prior partial/unmapped coverage stays explicit.

Installed-wheel tests cover both sync/async native busy-error translation,
original numeric/symbolic status and cause, retry reason/count/elapsed fields,
immutability, pickle/context preservation and the updated exception hierarchy.

## Boundaries

Owner-seqid operations in v4.0 remain in their owner state machines rather
than being blindly replayed with cached seqids. Successful modifications,
transport uncertainty and partial COMPOUND results are not treated as definite
busy rejections. See the policy document for the exact safe-operation boundary.

This validation did not replay the reported 132-node data-mover workload,
induce physical storage overload, or benchmark throughput. TCP fixtures prove
client behavior for explicit JUKEBOX/DELAY/GRACE responses; they do not establish
why a particular storage appliance returned those statuses. No AFX settings or
lab exports were changed for this retry task. These local checks do not publish
release artifacts; publication is separately gated by the immutable release
workflow. Applications must adopt the updated library for behavior to change.
Raw build/reproduction logs remain outside the checkout under `/tmp`.

## Independent code review

Standards: three initial P2 findings (cancelled pNFS fallback, nested uncertainty
classification and v4.0 response/request matching), plus a header-offset defect
introduced during correction, were fixed and independently re-reviewed. Final
result: PASS, no remaining actionable findings. Final gate follow-up also
keeps NFSv4.0 NULL outside COMPOUND parsing and boxes nested WRITE futures
to avoid worker-stack overflow without changing retry or ownership semantics.

Spec: four initial findings, worst P1 (cold pNFS exhaustion swallowed by
fallback), covered cold metadata, cancellation, mixed recovery counters and
NFSv3 ACL coverage. Re-review also found public DS-WRITE aggregation losing
structured exhaustion. All were fixed with regression coverage. Final result:
PASS, no remaining substantive findings.

Version 0.10.0 also updates the Rust/Python package documentation to state
NFSv4.2 support, exact version selection and AFX-dependent optional operation
limits. The historical AFX reports describe their own builds and do not claim
to validate this change under a physical JUKEBOX/DELAY load.
