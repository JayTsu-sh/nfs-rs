# NFSv4.1 Migration Reliability — P0/P1 Specification

Status: Draft  
Created: 2026-07-31  
Target library: `nfs-rs`  
Primary consumer: long-running file migration tools  
Normative protocol: RFC 5661 (NFSv4.1), RFC 5531 (ONC RPC), RFC 4506 (XDR)

## 1. Goal

Make the NFSv4.1 client safe for long-running migration workloads under lost replies,
connection failure, session loss, callback replay, and pNFS WRITE failure. A completed
migration must never silently accept an uncertain write result or corrupted destination
file.

The work is divided into:

- **P0 — migration correctness:** fore-channel replay, lease renewal, uncertain outcomes,
  state invalidation, and deterministic recovery boundaries.
- **P1 — resilient advanced operation:** connection/backchannel recovery, callback replay,
  negotiated channel limits, pNFS WRITE failure handling, lab capability discovery, and
  production-grade diagnostics.

Every independently implementable problem MUST have a GitHub Issue before implementation.
Every production-code modification MUST be covered by both a deterministic CI test and a
nightly-lab end-to-end scenario.

A parent tracking Issue MUST be created before any child work begins. It is the canonical
progress view for this specification and MUST link every P0/P1 child Issue, dependency,
pull request, CI run, nightly run, review result, and blocked lab capability.

## 2. Background and current state

The current client establishes an NFSv4.1 session with
`EXCHANGE_ID -> CREATE_SESSION -> RECLAIM_COMPLETE`, prepends `SEQUENCE` to normal
COMPOUNDs, multiplexes fore-channel replies and backchannel calls on one TCP connection,
and supports pNFS file layouts.

The following reliability gaps are in scope:

- Normal requests use `sa_cachethis=false` without a complete
  `NFS4ERR_RETRY_UNCACHED_REP` policy.
- An RPC timeout can leave the server-side execution result unknown while the local slot
  remains reusable.
- Lease renewal advances its slot after any RPC reply without validating the decoded
  `SEQUENCE` result.
- Session replacement can leave old OPEN, LOCK, layout, and DS state locally usable.
- TCP reconnect does not re-establish connection-to-session binding.
- The callback handler captures the original session ID and does not implement callback
  reply replay.
- CB_COMPOUND currently does not consistently echo the request tag or propagate the final
  operation status to the compound status.
- Negotiated `maxoperations`, target-highest-slot changes, and echoed SEQUENCE identity
  are not fully enforced.
- pNFS WRITE fallback and layout recovery are not proven under DS/MDS failure.
- The current nightly health check proves NFSv4.1 availability only. It does not prove
  pNFS, independent DS access, callback delivery, or fault-injection authority.

The existing lab consists of a controller, source, destination, and a worker described as
supporting fault injection. The repository does not currently expose a fault-injection
interface, and the actual pNFS capability cannot be established from repository contents
alone.

## 3. Safety model for migration

### 3.1 Operation classes

Every NFSv4.1 request MUST be assigned one of these classes:

1. **Read-only/idempotent:** for example NULL, LOOKUP, GETATTR, GETFH, ACCESS, READ,
   READDIR, READLINK.
2. **Replay-sensitive modifying:** for example CREATE, WRITE, SETATTR, REMOVE, RENAME,
   LINK, COMMIT, OPEN, CLOSE, LOCK, LOCKU, DELEGRETURN, LAYOUTCOMMIT, LAYOUTRETURN.
3. **Session/control:** for example SEQUENCE-only renewal, BIND_CONN_TO_SESSION,
   DESTROY_SESSION, EXCHANGE_ID, CREATE_SESSION, RECLAIM_COMPLETE.

The classification MUST be explicit in code and exhaustively tested. Adding a new
operation without assigning a class MUST fail CI.

### 3.2 Uncertain outcome

An **uncertain outcome** means the client cannot prove whether a replay-sensitive operation
was executed by the server. Examples include:

- the request was fully transmitted but no authoritative reply was received;
- `NFS4ERR_RETRY_UNCACHED_REP` was received for a request whose full result is required;
- a connection or session failed after transmission but before result validation;
- DS data may have been written but the corresponding layout commit outcome is unknown.

The library MUST return a structured error that preserves:

- operation or compound tag;
- operation class;
- file handle identity as a non-sensitive stable digest, not raw bytes;
- offset and length when applicable;
- session and slot identifiers when known;
- whether the caller must reopen, remount, verify, or restart the file;
- the underlying RPC/NFS error.

The library MUST NOT report success, automatically advance the migration checkpoint, or
blindly replay with stale state after an uncertain outcome.

### 3.3 Migration recovery contract

For an uncertain modifying result, stale session state, or verifier change, the safe
library contract is:

1. stop issuing dependent operations for the affected file;
2. invalidate affected OPEN/layout/DS state;
3. re-establish connection and session as necessary;
4. require the consumer to reopen the file;
5. resume only from a consumer-verified checkpoint, or restart the temporary destination
   file;
6. verify final size and content checksum before publishing the final filename.

Transparent full NFS state reclaim is not required by this specification.

## 4. Requirements

### R1. Issue-first traceability

- **Current:** Protocol fixes can be implemented without a dedicated tracking record.
- **Target:** Every independently testable reliability problem has a GitHub Issue before
  production implementation begins.
- **Required Issue content:**
  - observable problem and migration impact;
  - P0 or P1 severity;
  - exact RFC sections;
  - current and target state transitions;
  - explicit non-goals;
  - compatibility and rollback;
  - CI test cases;
  - nightly-lab scenarios and required capabilities;
  - code-review checklist;
  - dependencies on other Issues.
- **Acceptance:** Every implementing PR links at least one conforming Issue. Every changed
  production behavior maps to an Issue acceptance criterion, one CI test, one nightly
  scenario, and a review disposition.

### R1.1 Parent tracking Issue and progress states

- One parent Issue titled `[tracking] NFSv4.1 migration reliability P0/P1` MUST own the
  overall progress checklist.
- The parent MUST list every child Issue from Section 5 and show:
  - priority and owner;
  - current state;
  - dependencies and blockers;
  - linked PR;
  - mapped requirement IDs and test IDs;
  - latest CI and nightly disposition;
  - open Critical/High review finding count.
- Child Issue state MUST use this controlled lifecycle:
  `proposed -> specified -> ready -> in-progress -> in-review -> ci-passed ->
  nightly-passed -> done`, with `blocked` as an explicit side state carrying a reason.
- An Issue MUST NOT move to `ready` until its dependencies, acceptance criteria, CI cases,
  nightly scenarios, and review checklist are defined.
- An Issue MUST NOT move to `done` until its PR is merged, required CI and nightly evidence
  is linked, cleanup is verified, and Critical/High findings are zero.
- The parent Issue MUST be updated in the same PR or closure action that changes a child
  Issue's completion status.
- **Acceptance:** The parent checklist and all child states agree with repository evidence;
  no completed requirement lacks a closed child Issue, and no `done` Issue lacks a merged
  PR plus green CI/nightly evidence.

### R2. Operation-aware SEQUENCE reply caching

- **Current:** Normal compounds unconditionally request `sa_cachethis=false`.
- **Target:** Replay-sensitive modifying compounds request a cached reply when the
  negotiated cache size can hold the required response. Read-only compounds MAY avoid
  full reply caching.
- The decision MUST be based on operation class, not a free boolean chosen at individual
  call sites.
- When a reply cannot be cached because of negotiated limits, the caller MUST receive a
  defined preflight failure or an explicit uncertain-outcome policy; the client MUST NOT
  silently downgrade exactly-once expectations.
- **Acceptance:** A deterministic server harness executes a modifying operation, drops its
  first reply, receives the retransmission with the same slot/sequence ID, and returns the
  cached result. The operation is observed exactly once.

### R3. Fore-channel replay and uncertain results

- **Current:** RPC retry changes XID but can reuse a slot after an unresolved timeout, and
  `RETRY_UNCACHED_REP` has no recovery policy.
- **Target:** Retransmissions retain the original session ID, slot ID, sequence ID, and
  encoded compound until the request reaches a terminal protocol state.
- The slot MUST NOT be reused for a different request while the prior execution remains
  ambiguous.
- `NFS4ERR_RETRY_UNCACHED_REP` and `NFS4ERR_SEQ_FALSE_RETRY` MUST have explicit,
  operation-class-aware handling.
- A modifying operation with no authoritative result MUST return the structured uncertain
  outcome described in Section 3.2.
- **Acceptance:** CI and nightly cases cover lost request, lost reply, duplicate reply,
  delayed stale reply, reconnect during request, retry exhaustion, RETRY_UNCACHED_REP, and
  SEQ_FALSE_RETRY without duplicate namespace mutation or silent success.

### R4. Correct lease renewal

- **Current:** Renewal advances the local slot after any successfully received RPC frame.
- **Target:** Renewal decodes CB/RPC framing and COMPOUND, verifies that result zero is
  `OP_SEQUENCE`, validates the echoed session/slot/sequence, and advances only on
  `NFS4_OK`.
- BADSESSION, DEADSESSION, SEQ_MISORDERED, BADSLOT, and transport failure MUST enter the
  same recovery coordinator used by foreground requests.
- Only one recovery attempt for a session generation MAY run at a time.
- **Acceptance:** For every non-OK SEQUENCE result, a CI state-machine test proves the
  local sequence ID does not advance. A nightly long-running mount spans at least three
  lease renewal intervals and remains usable before and after an injected session fault.

### R5. Session-generation state fencing

- **Current:** Replacing `SessionHolder` can leave state created under the old session
  generation in `StateManager` and `LayoutManager`.
- **Target:** Every session, OPEN state, lock state, layout, DS connection, and in-flight
  request is associated with a monotonically increasing local session generation.
- State from an older generation MUST be rejected before wire transmission.
- Session recovery MUST invalidate or quarantine old OPEN, LOCK, layout, dirty range, and
  DS state before the mount is made available again.
- Concurrent recovery calls MUST converge on one active generation.
- **Acceptance:** CI races 64 concurrent operations with one session replacement and
  proves no old-generation stateid is encoded after the new generation is published.
  Nightly invalidates the session during active migration, verifies a controlled failure,
  reopens the file, resumes from a verified checkpoint, and validates checksum.

### R6. Connection/session binding recovery

- **Current:** RPC reconnect replaces TCP state without reissuing
  `BIND_CONN_TO_SESSION`.
- **Target:** A new TCP connection MUST NOT be declared NFSv4.1-ready until its fore and
  requested backchannel directions are associated with the active session.
- A connection generation change MUST notify the session recovery coordinator.
- Rebinding MUST be idempotent and safe when multiple failed requests observe the same
  reconnect.
- Failure to restore a required backchannel MUST produce a degraded or failed health state,
  not an informational-only log.
- **Acceptance:** CI proves exactly one effective rebind per connection generation under
  concurrency. Nightly resets the TCP connection during I/O, confirms rebind from
  observable evidence, then verifies continued callback delivery and file checksum.

### R7. Backchannel session context and replay

- **Current:** The callback handler captures the initial session ID and records only the
  next expected callback sequence ID.
- **Target:** Callback context follows the active session generation and negotiated
  backchannel attributes.
- Each callback slot MUST retain the last completed request identity and encoded reply
  required to answer a legal replay.
- A repeated CB_SEQUENCE for the most recently completed request MUST return the cached
  reply without executing CB_RECALL or CB_LAYOUTRECALL twice.
- A false retry, invalid session, out-of-range slot, or misordered sequence MUST return the
  RFC-defined error and MUST NOT enqueue recall work.
- Callback slot storage MUST be bounded by negotiated backchannel limits.
- **Acceptance:** CI covers first request, exact replay, next sequence, false retry,
  misorder, old/new session IDs, minimum and maximum slot, maximum+1 slot, and concurrent
  duplicate callback delivery. Nightly drops a callback reply, observes retransmission,
  and proves one recall side effect.

### R8. Correct CB_COMPOUND response semantics

- **Current:** Callback request tag is discarded and top-level status can be emitted as
  NFS4_OK even when an operation failed.
- **Target:** The response echoes the exact opaque tag bytes, stops after the first failed
  operation, and sets top-level status to the status of the last executed operation.
- Truncated or invalid RPC/XDR MUST receive a deterministic protocol/RPC error when enough
  request context exists; it MUST NOT become a successful short response.
- Callback minor version, RPC program/version/procedure, credential flavor, op order, and
  `CB_SEQUENCE` position MUST be validated.
- **Acceptance:** Golden-wire CI tests cover empty and non-UTF-8 opaque tags, zero ops,
  successful multi-op compounds, each supported operation error, unknown op, truncated
  field, excessive array length, and wrong program/version/minor version. Nightly confirms
  a real server accepts replies after callback recall and continues the session.

### R9. Negotiated channel and slot limits

- **Current:** Some CREATE_SESSION values are decoded but not enforced, and
  `target_highest_slotid` is not used to resize active slot availability.
- **Target:** The client enforces negotiated maximum request size, cached response size,
  response size, operations per compound, and requests per channel.
- SEQUENCE success MUST validate echoed session ID, sequence ID, and slot ID before local
  advancement.
- Slot availability MUST respond safely to target-highest-slot shrink and growth without
  revoking an in-flight slot.
- Arithmetic involving negotiated values MUST use checked conversions and bounded
  allocations.
- **Acceptance:** CI covers negotiated values 0, 1, configured maximum, maximum-1,
  maximum+1, shrink with high slots in flight, subsequent growth, malformed echoes, and
  integer overflow attempts. Nightly records negotiated values and runs a compound and
  concurrency level at their effective limits.

### R10. pNFS WRITE integrity and fallback

- **Current:** pNFS WRITE exists, but DS/MDS failure, ambiguous layout commit, callback
  recall, and recovery are not comprehensively proven.
- **Target:** pNFS WRITE preserves a single, auditable state transition across:
  - layout acquisition and device discovery;
  - DS session establishment;
  - stripe planning;
  - partial multi-stripe completion;
  - DS timeout or disconnect;
  - LAYOUTCOMMIT;
  - CB_LAYOUTRECALL;
  - LAYOUTRETURN;
  - COMMIT/CLOSE;
  - fallback to MDS.
- Automatic MDS fallback is allowed only before any ambiguous DS mutation, or after the
  library proves the fallback write safely overwrites the complete affected range with
  identical data.
- If DS data may have changed and visibility at the MDS is unknown, the operation MUST
  return an uncertain outcome and require file-level verification/restart.
- Dirty ranges MUST survive recoverable transient failures and MUST be removed only after
  authoritative commit/return or explicit invalidation.
- Layout recall and foreground close MUST serialize so that LAYOUTCOMMIT precedes required
  LAYOUTRETURN and CLOSE.
- **Acceptance:** CI uses a deterministic fake MDS/DS topology to cover failure before the
  first DS write, after one stripe, after all DS writes but before LAYOUTCOMMIT, during
  LAYOUTCOMMIT, during recall, and during CLOSE. Nightly repeats these cases against a real
  pNFS server and verifies destination size plus full-file checksum.

### R11. Nightly advanced-capability discovery

- **Current:** `health-check.sh` proves only that NFSv3 and NFSv4.1 are advertised.
- **Target:** Nightly emits a machine-readable capability artifact before advanced tests.
- The probe MUST independently establish:
  - NFSv4.1 session creation;
  - `EXCHGID4_FLAG_USE_PNFS_MDS`;
  - successful file-layout LAYOUTGET;
  - GETDEVICEINFO returning at least one reachable DS;
  - whether the DS endpoint is operationally independent of the MDS;
  - successful DS WRITE followed by LAYOUTCOMMIT and checksum verification;
  - working backchannel callback delivery;
  - authorized connection reset, DS isolation, MDS service restart/session invalidation,
    and callback-reply loss injection.
- Capability probing MUST be non-destructive outside the run-specific export directory.
- Missing required capability MUST fail the advanced nightly job as
  `BLOCKED_CAPABILITY`; it MUST NOT be converted into success by test skip.
- **Acceptance:** The artifact is retained for every nightly run and contains server
  identity, capability booleans, negotiated channel limits, MDS/DS addresses with
  redaction policy, and probe evidence. A deliberately disabled capability makes the
  advanced job fail with the corresponding capability name.

### R12. Controlled nightly fault injection

- **Current:** The worker is documented as supporting fault injection, but no repository
  interface or cleanup contract exists.
- **Target:** Repository-owned scripts provide allow-listed, run-ID-scoped fault actions:
  - drop the next matching RPC reply;
  - reset the client-to-MDS TCP connection;
  - isolate one DS endpoint;
  - restore the DS endpoint;
  - restart/invalidate the NFSv4.1 service/session using a lab-approved action;
  - delay or drop one callback reply.
- Every action MUST have an idempotent cleanup action.
- Cleanup MUST run from an `always()` workflow step and MUST verify restoration rather than
  assume it.
- Fault commands MUST reject non-lab hosts, unvalidated run IDs, broad network targets, and
  actions outside the allow list.
- **Acceptance:** CI shell tests validate command construction using a fake SSH endpoint.
  Nightly injects and restores every action, then runs a clean post-fault smoke migration.

### R13. CI coverage for every modification

- **Current:** CI runs formatting, check, clippy, and tests, but there is no traceability
  rule requiring a regression test for each reliability change.
- **Target:** Every production-code modification under this specification adds or updates
  a deterministic test that fails against the pre-fix behavior.
- CI MUST include:
  - unit state-machine tests;
  - XDR/golden packet tests;
  - an in-process scripted NFSv4.1 MDS/DS/callback fault server;
  - concurrency tests with deterministic barriers rather than timing-only sleeps;
  - property tests for slot/sequence transitions and bounded decoding;
  - shell tests for lab orchestration;
  - unchanged NFSv3 and public API regression tests.
- Tests MUST NOT require access to the private lab or public network.
- Flaky retries are not an acceptable substitute for deterministic synchronization.
- **Acceptance:** Each Issue contains a CI test ID, each PR reports its red-before/green-after
  evidence, and a traceability check fails when a changed production requirement has no
  mapped CI test.

### R14. Nightly E2E coverage for every modification

- **Current:** Nightly exercises normal CRUD against NFSv3 and NFSv4.1 only.
- **Target:** Every production behavior changed under this specification maps to at least
  one nightly scenario exercising the real wire path.
- A low-level modification MAY share a scenario with related changes, but the scenario
  MUST contain an assertion capable of detecting regression of each mapped behavior.
- Nightly MUST retain:
  - capability artifact;
  - scenario-to-Issue/test mapping;
  - structured client log;
  - fault timeline;
  - negotiated session/layout summary;
  - source/destination size and checksum;
  - cleanup verification.
- A required scenario that did not run is failure, not pass.
- **Acceptance:** A generated coverage manifest has no unmapped production modification,
  no missing required scenario, and no scenario lacking final data-integrity verification.

### R15. Mandatory code review gate

- **Current:** No protocol-specific review gate is required by CI or Issue closure.
- **Target:** Every Issue and PR receives a recorded review covering:
  1. RFC 5661 request/reply and state-transition correctness;
  2. replay, exactly-once, and uncertain-outcome semantics;
  3. stateid and session-generation lifetime;
  4. cancellation, concurrency, lock ordering, and recovery races;
  5. pNFS MDS/DS ordering and data-integrity invariants;
  6. RPC record marking, XID dispatch, authentication framing, and reconnect behavior;
  7. XDR truncation, length bounds, allocation limits, and integer overflow;
  8. public API, NFSv3, and server compatibility;
  9. CI and nightly traceability, negative cases, and test determinism;
  10. observability, redaction, rollout, and rollback;
  11. zero-copy framing, buffer ownership, retry behavior, and payload lifetime;
  12. throughput, latency, allocations, memory, CPU, and concurrency regression;
  13. panic safety, including the production-code ban on `.unwrap()` and `.expect()`.
- Findings MUST be classified as Critical, High, Medium, or Low.
- Critical and High findings MUST be fixed and receive a regression test before merge.
- A Critical/High finding discovered outside the implementing Issue MUST create or reopen a
  GitHub Issue; resolving it only in a PR comment is insufficient.
- **Acceptance:** PR checks fail while any Critical/High finding is open or while the
  review record/checklist is absent.

### R15.1 Zero-copy correctness review

- Changes to RPC framing, WRITE, DS WRITE, retry, reconnect, checksum, diagnostics, or
  buffer ownership MUST receive a zero-copy review.
- The review MUST prove:
  - RPC record length includes header, payload, and XDR padding exactly once;
  - normal MDS/DS WRITE does not copy payload into an intermediate contiguous request;
  - header and payload ownership remains valid until write completion or cancellation;
  - retry reuses immutable payload bytes without cumulative framing or mutation;
  - concurrent writes cannot alias mutable buffers or interleave RPC frames;
  - partial socket writes remain serialized;
  - tracing, hashing, diagnostics, and fault injection do not add an unconditional
    payload-sized copy;
  - fallback paths have an explicit copy budget and do not become the normal path.
- CI MUST test XDR alignment boundaries, partial writes, cancellation, retransmission, and
  payload integrity, with allocation/copy instrumentation for large WRITE.
- Nightly MUST exercise multi-chunk MDS and DS writes and retain throughput, CPU, and
  peak-memory evidence.
- **Acceptance:** Each affected review records the before/after data path; CI proves
  byte-exact framing and the copy invariant; nightly satisfies R18.

### R15.2 Panic-free production paths

- Production Rust code MUST NOT introduce `.unwrap()` or `.expect()`.
- Review MUST also cover equivalent panic paths from direct indexing, conversions,
  arithmetic, poisoned locks, task joins, and assumed invariants.
- Tests MAY use unwrap/expect only under the repository's existing test exception.
- CI MUST scan production Rust sources and fail on forbidden calls outside test code.
- Malformed/truncated input, cancellation, closed channels, and task failure MUST return
  structured errors rather than panic.
- **Acceptance:** The production panic audit is green and no network/server/caller input can
  reach an unhandled panic path.

### R16. Structured diagnostics

- **Current:** Recovery behavior is primarily observable through unstructured log messages.
- **Target:** Structured events identify connection generation, session generation,
  operation class, slot/sequence, recovery phase, callback health, layout/DS phase, fallback
  decision, capability result, and uncertain outcome.
- Diagnostics MUST be bounded and MUST NOT contain credentials, authentication bodies, raw
  file handles, or file payloads.
- Repeated identical events MUST be rate-limited or aggregated.
- **Acceptance:** CI snapshot tests verify required fields and redaction. Nightly artifacts
  allow each injected fault to be associated with the expected state transition and final
  disposition.

### R17. Compatibility and rollout

- **Current:** `Mount` is a public version-agnostic trait used by migration consumers.
- **Target:** Existing NFSv3 behavior and successful NFSv4.1 happy-path behavior remain
  compatible. New structured errors MAY extend `NfsError` but MUST preserve the source
  error and allow consumers to distinguish retryable, reopen-required, remount-required,
  and uncertain-result outcomes without parsing strings.
- Safer behavior MAY turn a previously blind retry into an explicit error; this is an
  intentional compatibility change and MUST be documented in the Issue and changelog.
- pNFS WRITE MUST have an emergency disable/rollback mechanism available to the migration
  application without disabling NFSv4.1 MDS I/O.
- **Acceptance:** CI runs all existing tests and compile-time API fixtures. Nightly runs
  NFSv3, NFSv4.1 MDS-only, and pNFS WRITE suites, and verifies fallback/disable behavior.

### R18. Performance and resource regression budgets

- **Current:** Reliability changes have no uniform data-path performance gate.
- **Target:** Every Issue touching I/O, RPC, synchronization, allocation, retry, or
  diagnostics provides comparable before/after evidence.
- Benchmarks MUST separately measure MDS and pNFS sequential READ/WRITE, small-file
  create/write/close, and concurrent streams at 1, 8, and the negotiated slot limit.
- Unless an Issue approves a different evidence-backed budget, a change MUST NOT cause:
  - more than 5% median throughput regression;
  - more than 10% p95 operation-latency regression;
  - more than 5% peak resident-memory regression;
  - a new payload-size-proportional allocation/copy on normal zero-copy WRITE;
  - unbounded pending RPC, callback slot, dirty range, log, or recovery-task growth.
- CI MAY use structural allocation/copy assertions instead of noisy timing thresholds.
  Nightly is authoritative for throughput, latency, CPU, and RSS.
- Comparisons MUST use the same host, server, payload, concurrency, warm-up, sample count,
  and negotiated layout/session configuration.
- **Acceptance:** Each affected PR links CI structural evidence and a nightly before/after
  artifact. An unapproved budget breach blocks merge.

## 5. GitHub Issue implementation plan

The following is the minimum Issue set. Issue numbers are assigned when created. Issues MAY
be split further; they MUST NOT be merged if doing so removes independent testability.

Before opening the child Issues, create the parent tracking Issue from R1.1 and assign all
children to the same GitHub milestone. Recommended labels are:

- priority: `priority:p0`, `priority:p1`;
- area: `area:nfs41-session`, `area:rpc`, `area:backchannel`, `area:pnfs`,
  `area:lab`, `area:ci`;
- type: `type:protocol`, `type:test`, `type:infrastructure`, `type:review`;
- state: `state:ready`, `state:in-progress`, `state:blocked`, `state:in-review`;
- risk: `risk:data-integrity`, `risk:compatibility`, `risk:lab-safety`.

If the repository uses a GitHub Project, the controlled lifecycle from R1.1 SHOULD be
represented by a single-select `Status` field. Otherwise, the parent checklist and state
labels are normative.

| Order | Suggested title | Priority | Requirements | Depends on |
|---:|---|---|---|---|
| 1 | `[P0] Add scripted NFSv4.1 fault server and coverage manifest` | P0 | R13, R14 | — |
| 2 | `[P0] Classify COMPOUND operations and select SEQUENCE cache policy` | P0 | R2 | 1 |
| 3 | `[P0] Represent uncertain NFS operation outcomes` | P0 | R3, R17 | 1 |
| 4 | `[P0] Correct fore-channel retransmission and slot fencing` | P0 | R2, R3 | 2, 3 |
| 5 | `[P0] Validate lease-renewal SEQUENCE before slot advance` | P0 | R4 | 1 |
| 6 | `[P0] Fence stateids and layouts by session generation` | P0 | R5 | 3 |
| 7 | `[P1] Rebind NFSv4.1 connections after TCP/session recovery` | P1 | R6 | 5, 6 |
| 8 | `[P1] Implement callback slot replay and CB_COMPOUND correctness` | P1 | R7, R8 | 1, 7 |
| 9 | `[P1] Enforce negotiated NFSv4.1 channel and slot limits` | P1 | R9 | 4 |
| 10 | `[P1] Make pNFS WRITE recovery data-integrity safe` | P1 | R10 | 4, 6, 8, 9 |
| 11 | `[P1] Add lab advanced-capability discovery and artifacts` | P1 | R11 | 1 |
| 12 | `[P1] Add run-scoped nightly MDS/DS/callback fault injection` | P1 | R12 | 11 |
| 13 | `[P1] Add structured recovery and pNFS diagnostics` | P1 | R16 | 4–12 |
| 14 | `[Quality] Enforce Issue/test/review traceability gates` | P0 | R1, R13–R15 | 1 |
| 15 | `[Quality] Enforce zero-copy, panic-free, and performance gates` | P0 | R15, R18 | 1 |

Each Issue MUST use the repository's spec-driven Issue template or an updated reliability
template containing all R1 fields.

### 5.1 Child Issue progress checklist

Every child Issue MUST contain this checklist:

```markdown
## Progress

- [ ] Requirements and RFC references reviewed
- [ ] Dependencies resolved
- [ ] Design/recovery state transitions approved
- [ ] CI regression test demonstrated red before fix
- [ ] Implementation PR linked
- [ ] CI green after fix
- [ ] Nightly capability prerequisites green
- [ ] Nightly E2E scenario green
- [ ] Failure and cleanup evidence attached
- [ ] Code review complete
- [ ] Zero-copy review complete or N/A with reason
- [ ] Performance/resource budget evidence attached or N/A with reason
- [ ] Production panic audit green
- [ ] Critical/High findings: 0
- [ ] Parent tracking Issue updated
```

Progress MUST be based on linked evidence, not manually asserted completion alone.

## 6. Required CI and nightly test matrix

Test IDs are stable identifiers referenced by Issues, PRs, code review, and evidence
artifacts.

| Test ID | Behavior | CI requirement | Nightly-lab requirement |
|---|---|---|---|
| T01 | Modifying reply replay | Scripted server drops reply and proves one execution | Drop real WRITE/CREATE reply; verify result and checksum |
| T02 | RETRY_UNCACHED_REP | Inject error for each operation class | Real/proxy-induced uncached replay; verify uncertain disposition |
| T03 | SEQ_FALSE_RETRY | Same seq, different request digest | Fault proxy mutates replay timing; session remains controlled |
| T04 | Renewal validation | Every SEQUENCE error leaves slot unchanged | Three renewals plus injected session fault |
| T05 | Recovery convergence | 64 callers encounter one generation fault | Parallel migration during session invalidation |
| T06 | Old-state fencing | Attempt to encode every old state type | Reopen/resume after invalidation; checksum |
| T07 | TCP rebind | Concurrent reconnect produces one binding transition | Reset MDS TCP mid-I/O; observe rebind |
| T08 | Callback replay | Duplicate callback executes recall once | Drop callback reply; observe server retransmit |
| T09 | Callback response XDR | Golden tags/status/error framing | Real CB_LAYOUTRECALL accepted by server |
| T10 | Channel bounds | Boundary/property tests for every negotiated limit | Operate at effective request/op/concurrency limits |
| T11 | pNFS DS fail before write | No DS mutation, safe MDS fallback | Isolate DS before first write; checksum |
| T12 | pNFS DS fail after partial write | Uncertain result; no blind fallback | Isolate DS after stripe; restart/resume; checksum |
| T13 | LAYOUTCOMMIT failure | Dirty state retained or explicit uncertainty | Fault LAYOUTCOMMIT; restore and verify |
| T14 | Recall/write/close race | Deterministic barriers prove required ordering | Recall during active striped WRITE |
| T15 | Multi-DS partial failure | Stripe-specific deterministic failure | Isolate one DS while another remains reachable |
| T16 | pNFS disable | MDS-only path emits no layout operations | Nightly MDS-only suite succeeds |
| T17 | Capability discovery | Fixture outputs for supported/missing/malformed | Probe real lab and retain artifact |
| T18 | Fault cleanup | Fake SSH proves allow-list and idempotency | Every fault restored; clean smoke migration passes |
| T19 | Diagnostic redaction | Snapshots reject secrets/raw FH/payload | Scan retained artifacts for prohibited data |
| T20 | Compatibility | Existing tests plus API compile fixture | NFSv3 and v4.1 happy-path suites |
| T21 | Long-run stability | Deterministic simulated repeated renewal/reconnect | Migration lasting >=3 lease intervals |
| T22 | Cleanup under failure | Cancellation/drop state-machine tests | Workflow cancellation/failure cleanup verification |
| T23 | Zero-copy framing | Alignment, retry, partial-write, cancellation, and copy-count tests | Large MDS/DS writes with checksum, CPU, RSS |
| T24 | Production panic audit | Reject unwrap/expect; malformed and cancelled path tests | Fault suites prove process remains alive |
| T25 | Performance budgets | Structural allocation checks and stable microbenchmarks | Before/after throughput, p95, CPU, and RSS |

### 6.1 CI workflow gate

The normal GitHub Actions CI MUST:

1. run formatting, check, clippy, and all existing tests;
2. run the scripted MDS/DS/callback fault suite;
3. run deterministic concurrency/state-machine/property tests;
4. run lab-script unit tests without private credentials;
5. validate the Issue/requirement/test/nightly coverage manifest;
6. validate code-review checklist presence for protocol PRs;
7. upload failure diagnostics from the scripted server.

The CI suite MUST complete without a private lab. CI MUST fail if:

- a production file changed with no mapped Issue and test IDs;
- a mapped CI test is absent or filtered out;
- a test relies solely on wall-clock sleeps for ordering;
- a required negative test cannot demonstrate red-before behavior;
- the production unwrap/expect audit fails;
- a required zero-copy structural assertion or performance test is absent;
- existing NFSv3 or API compatibility tests regress.

### 6.2 Nightly workflow gates

Nightly MUST be split into observable stages:

1. **Base health:** existing host, NFSv3, NFSv4.1, and object-store checks.
2. **Advanced capability discovery:** R11 artifact generation.
3. **Baseline migration:** NFSv3, NFSv4.1 MDS-only, and pNFS WRITE.
4. **Fore-channel faults:** T01–T07.
5. **Backchannel faults:** T08–T09 and callback portions of T14.
6. **pNFS faults:** T11–T15.
7. **Long-running recovery:** T21.
8. **Post-fault clean smoke:** proves the lab was restored.
9. **Cleanup verification:** runs under `if: always()`.
10. **Artifact upload:** also runs on failure.

Nightly MUST use the existing global lab lock. A stage that requires an unavailable
capability MUST fail with `BLOCKED_CAPABILITY(<name>)`. It MAY be reported separately from
a product regression, but it MUST keep the required nightly check non-green.

## 7. Code-review procedure

### 7.1 Review timing

Review occurs at four points:

1. Issue/spec review before implementation;
2. implementation PR review;
3. post-CI protocol/concurrency review;
4. post-nightly evidence review before Issue closure.

### 7.2 Required review record

Each PR MUST contain a table:

| Area | Reviewer disposition | Evidence |
|---|---|---|
| RFC/state machine | Pass / findings | RFC links, state diagram, tests |
| Replay/exactly-once | Pass / findings | T01–T04 evidence |
| Concurrency/cancellation | Pass / findings | deterministic race tests |
| pNFS integrity | Pass / N/A with reason | T11–T15 evidence |
| RPC/XDR safety | Pass / findings | golden/property tests |
| Compatibility | Pass / findings | T20 evidence |
| CI/nightly traceability | Pass / findings | manifest and run URLs |
| Diagnostics/security | Pass / findings | T19 evidence |
| Zero-copy/buffer ownership | Pass / N/A with reason | T23 and data-path analysis |
| Performance/resources | Pass / N/A with reason | T25 before/after evidence |
| Panic safety | Pass / findings | T24 audit and fault evidence |

Reviewers MUST explicitly consider counterexamples:

- request executed but reply lost;
- callback executed but reply lost;
- old response arrives after slot reuse;
- recovery and a new request race;
- TCP reconnect succeeds but BIND fails;
- session changes while callbacks are in flight;
- DS write succeeds but LAYOUTCOMMIT is unknown;
- recall races with dirty-range update and CLOSE;
- target-highest-slot shrinks below an in-flight slot;
- cleanup fails after nightly fault injection.
- retry prepends a second record marker or copies the complete payload;
- cancellation releases or mutates a payload while socket write still references it;
- tracing/checksum logic turns zero-copy WRITE into a payload-sized allocation;
- malformed server data reaches unwrap, expect, indexing, conversion, or arithmetic panic;
- a correct recovery fix serializes all slots or causes a throughput/RSS regression.

### 7.3 Severity and closure

- **Critical:** plausible silent corruption, incorrect success, credential disclosure, or
  broad destructive fault action.
- **High:** duplicate mutation, stale-state wire use, session/slot desynchronization,
  unrecoverable lab contamination, or required test silently skipped.
- **Medium:** controlled failure, compatibility gap, observability gap, or incomplete edge
  handling without silent corruption.
- **Low:** maintainability or non-blocking diagnostic improvement.

No PR may merge and no Issue may close with an open Critical or High finding.

## 8. Boundaries

### In scope

- NFSv4.1 fore-channel Session/SEQUENCE correctness required by migration.
- Structured uncertain outcomes and session-generation fencing.
- TCP reconnect and BIND recovery.
- Backchannel CB_SEQUENCE replay and CB_COMPOUND correctness.
- Negotiated channel/slot enforcement.
- pNFS file-layout WRITE, DS sessions, LAYOUTCOMMIT, recall, return, fallback, and checksum.
- CI scripted MDS/DS/callback fault harness.
- Nightly lab capability discovery and run-scoped fault injection.
- GitHub Issue traceability and mandatory code-review gates.
- NFSv3 and public API compatibility regression coverage.

### Out of scope

- NFSv4.0 support.
- NFSv4.2 operations.
- Full transparent reclaim of all OPEN and LOCK state after server restart.
- Delegation-based client-side data caching.
- pNFS object and block layouts.
- Kerberos/RPCSEC_GSS implementation.
- A migration application's checkpoint file format or scheduler.
- Performance optimization unrelated to correctness, except preventing unacceptable
  regression caused by these changes.

## 9. Constraints

- No production `.unwrap()` or `.expect()`.
- Protocol behavior MUST cite RFC 5661; RPC/XDR changes MUST cite RFC 5531/RFC 4506.
- Existing code style and generated-XDR ownership rules apply.
- Tests MUST be deterministic and safe to run repeatedly.
- Nightly faults MUST be isolated to validated lab hosts and the unique run ID.
- Cleanup MUST be idempotent and verifiably restore service.
- Logs and artifacts MUST not expose secrets or file payloads.
- Production Rust code MUST contain no `.unwrap()` or `.expect()` calls.
- Normal MDS and DS WRITE framing MUST preserve the zero-copy payload path.
- Performance-sensitive changes MUST satisfy R18 or carry an explicitly approved Issue
  exception.
- The lab's real pNFS and fault capabilities are unverified as of this specification.
  Implementing Issues R11/R12 are prerequisites for claiming P1 completion.

## 10. Acceptance criteria

### P0 completion

- [ ] Parent tracking Issue exists and contains every P0/P1 child Issue.
- [ ] Issues 1–6 and 14–15 exist with all mandatory R1 content.
- [ ] Replay-sensitive operations use an operation-aware cache/replay policy.
- [ ] Lost-reply tests prove modifying operations execute at most once when a cached reply
      is available.
- [ ] RETRY_UNCACHED_REP and SEQ_FALSE_RETRY produce defined, tested dispositions.
- [ ] Uncertain modifying results cannot be reported as success.
- [ ] Lease renewal advances a slot only after a validated successful SEQUENCE.
- [ ] Session replacement fences every old-generation state type.
- [ ] Concurrent recovery converges on one active generation.
- [ ] Every P0 production modification maps to a red-before CI test and nightly scenario.
- [ ] P0 code review has zero open Critical/High findings.
- [ ] Existing NFSv3 and NFSv4.1 happy paths remain green.
- [ ] Production panic audit T24, zero-copy T23, and performance T25 gates are green.

### P1 completion

- [ ] Parent tracking Issue shows every P0 dependency complete or explicitly waived through
      a separately approved Issue decision.
- [ ] All P1 Issues exist with all mandatory R1 content.
- [ ] TCP reconnect and session replacement restore required channel binding.
- [ ] Callback context follows the active session and callback replay executes side effects
      once.
- [ ] CB_COMPOUND tag, status, operation stopping, and errors match golden tests.
- [ ] Negotiated channel limits and target-highest-slot changes are enforced.
- [ ] Nightly capability discovery proves a real pNFS file-layout WRITE path and retains
      evidence.
- [ ] Required advanced capability absence blocks the nightly check rather than passing via
      skip.
- [ ] Run-scoped MDS, DS, and callback fault injection is available and cleanup-verified.
- [ ] pNFS WRITE passes T11–T15 with full-file checksum verification.
- [ ] A DS mutation with unknown visibility produces uncertainty, not blind MDS fallback.
- [ ] Every P1 production modification maps to a red-before CI test and nightly scenario.
- [ ] P1 code review has zero open Critical/High findings.
- [ ] Post-fault clean smoke migration passes and proves lab restoration.
- [ ] MDS and pNFS WRITE meet the R18 performance/resource budgets.

## 11. Edge coverage

The specification explicitly covers the stateful/I/O edge categories identified by the
spec completeness probe:

| Category | Coverage |
|---|---|
| Idempotency/repetition | Fore-channel replay, callback replay, rebind, recovery, cleanup |
| Concurrency/effect ordering | Slot reuse, generation fencing, recall/write/close, recovery convergence |
| Boundary values | Negotiated limits at 0/1/max/max±1 and slot shrink/growth |
| Empty/degenerate input | Zero-op callback, empty tag, missing capability, no available DS |
| Ordering | SEQUENCE first, LAYOUTCOMMIT before return/close, first-error stop |
| Encoding/representation | Opaque tag bytes, XDR length/truncation, bounded diagnostics |
| Partial I/O | Lost reply, partial stripe write, unknown layout commit |

All applicable edges are resolved by explicit acceptance criteria or by required
deterministic CI plus nightly fault tests. No applicable edge is intentionally deferred.

## 12. Prohibitions (must-NOT)

- The implementation MUST NOT report migration success for an uncertain modifying result.
- It MUST NOT reuse a slot for a different request while the prior request outcome is
  ambiguous.
- It MUST NOT transmit an old-generation stateid after session replacement.
- It MUST NOT execute callback recall side effects twice for a legal replay.
- It MUST NOT silently fall back to MDS after an ambiguous DS mutation.
- It MUST NOT treat a skipped required nightly scenario as a passing check.
- It MUST NOT execute broad or non-run-scoped destructive lab fault commands.
- It MUST NOT retain credentials, raw authentication data, raw file handles, or payload
  bytes in logs/artifacts.
- It MUST NOT merge with unresolved Critical or High review findings.
- It MUST NOT introduce `.unwrap()` or `.expect()` into production Rust code.
- It MUST NOT silently replace normal zero-copy MDS/DS WRITE with a payload-sized copy.
- It MUST NOT accept an unapproved performance or memory regression beyond R18.

All prohibitions are mechanically verified by negative CI tests, nightly assertions, or
repository branch protection. Generic credential/security hardening remains additionally
subject to the project's normal security review.

## 13. Ambiguity report

| Dimension | Score | Minimum | Status | Notes |
|---|---:|---:|---|---|
| Goal clarity | 0.96 | 0.75 | Met | Migration correctness and P0/P1 outcomes are explicit |
| Boundary clarity | 0.94 | 0.70 | Met | In/out scope and recovery limits are explicit |
| Constraint clarity | 0.91 | 0.65 | Met | CI, nightly, pNFS, lab safety, and compatibility locked |
| Acceptance criteria | 0.95 | 0.70 | Met | Requirement, issue, test, and review gates defined |
| **Ambiguity** | **0.06** | **<=0.20** | **Met** | Lab capability is unknown but handled as a prerequisite gate |

## 14. Locked decisions

- The library is optimized for migration correctness, not a fully transparent OS NFS
  client.
- P0 and P1 both require CI and nightly coverage for every production modification.
- P1 includes pNFS WRITE and real DS/MDS fault cases.
- The actual nightly lab capability must be discovered and proven before advanced tests can
  be considered passing.
- Every independently implementable problem is tracked by a GitHub Issue before code work.
- Code review is a mandatory, evidence-backed completion gate.
- Full transparent state reclaim is out of scope; verified reopen/restart is acceptable.
