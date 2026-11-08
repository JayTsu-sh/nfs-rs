# Bounded server-busy retries

All supported versions use one fixed policy for protocol requests that can be
safely reissued: NFSv3 JUKEBOX, NFSv4.0 DELAY/GRACE and NFSv4.1/v4.2
DELAY/GRACE. There is no disabled mode or retry-policy connection option.

The policy permits at most 16 busy reissues after the first request. Equal-jitter
exponential backoff starts at 200 ms and caps at 5 s: each delay is selected from
the upper half of that attempt's exponential window. A shared 60 s budget bounds
busy waiting and RPC transport retries. Existing shorter RPC limits and explicit
Python operation timeouts still apply. An NFS operation is a protocol call or
one pNFS read, not an entire file transfer; large files do not share one global
60 s transfer limit. Nested layout operations, DS reads, refresh and MDS fallback
inherit the read's budget. Moving admitted work to an owned task preserves its
active budget and explicit caller deadline.

The policy does not turn transport timeouts, truncated replies or lost modifying
results into server-busy errors. Existing transport replay and uncertain-outcome
rules remain in force. Waiting for file/session locks and terminal cleanup is
not forcibly cancelled by this busy budget. Temporary Python v4.2 OPENs receive
a separate bounded CLOSE cleanup after an expired operation deadline; this does
not reissue the original operation. Admitted mutations retain settlement
responsibility after their caller stops waiting.

## Protocol boundaries

- NFSv3's generated procedure wrappers, READ, WRITE and native NFSACL
  GETACL/SETACL recognize an authoritative
  JUKEBOX reply before decoding the success body. Reissues use fresh RPC XIDs,
  as required by [RFC 1813 section 2.6](https://www.rfc-editor.org/rfc/rfc1813.html#section-2.6).
  A permanent status such as NOENT is returned without a busy retry.
- NFSv4.0 READ, including reads of already OPEN files, and ordinary metadata
  requests use the same policy. Safe namespace/attribute/WRITE compounds can
  retry a rejected operation only while their preceding operations are known
  side-effect-free. Successful mutations are never repeated. Owner-seqid
  OPEN/CLOSE/LOCK/LOCKU/OPEN_CONFIRM failures stay with their existing owner
  state machine; replaying an old owner seqid could replay a cached error.
  They are not blindly reissued by the generic busy loop. See
  [RFC 7530 section 13.1.1.3](https://www.rfc-editor.org/rfc/rfc7530.html#section-13.1.1.3).
- NFSv4.1 and v4.2 MDS and independent pNFS DS calls use the same shared policy.
  DELAY on SEQUENCE retains that slot/sequence identity; DELAY on a subsequent
  operation uses a new sequence after the successful SEQUENCE is acknowledged.
  Busy replies are checked against the requested operation order and status.
  A COMPOUND with an already successful modifying operation cannot be replayed
  in full; existing partial-response cleanup is retained, otherwise the caller
  receives Uncertain/VerifyThenResume. See
  [RFC 8881 section 15.1.1.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-15.1.1.3).
  RECLAIM_COMPLETE also uses the shared bounded policy.
- A pNFS busy error that exhausts the budget is returned directly, including
  cold LAYOUTGET and GETDEVICEINFO requests. It does not
  trigger layout invalidation or acquire another retry budget through MDS
  fallback. A cancelled read waiting in DS or metadata busy backoff also
  exits without refresh or fallback. Layout/session recovery for other error classes remains distinct.

These are shared scheduling and error semantics; protocol request identities
and stateful replay rules remain version-specific. GRACE retains its original
status and denotes server recovery rather than ordinary load.

## Uncached read replies

RFC 8881 [section 2.10.6.1.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-2.10.6.1.3)
permits a server to cache only SEQUENCE for an idempotent request. After losing
that response, replay can return `NFS4ERR_RETRY_UNCACHED_REP`. NFSv4.1/v4.2 MDS
and DS execution validate the reply and requery a wholly read-only COMPOUND once
with a fresh sequence. This uses the current operation deadline without opening
another budget or treating the response as server busy. Repeated uncached replies
remain typed errors; mutations and malformed replies are never reissued by this
path. A recovered reply loss is logged.

Deterministic TCP tests cover both minor versions and MDS/DS identities, repeated
failure, malformed replies, mutation uncertainty, cancellation during subsequent
backoff and slot reuse. Real Linux NFSv4.1 `lookup_path` response-loss tests through
data-mover also verified directory and broken-symlink observations without file
COMMITs; flash, disconnect, silent loss and cancellation were checked separately.
This is not physical pNFS DS, NFSv4.2, Python wheel or performance certification.

## Namespace session loss

For NFSv4.1/v4.2 CREATE, REMOVE (including directory removal), RENAME and LINK,
BADSESSION, DEADSESSION or SEQ_MISORDERED do not start a new session and repeat
the mutation. They return `Uncertain/VerifyThenResume` with the original request
context. Losing the reply cache cannot establish whether the mutation ran
([RFC 8881 section 2.10.13.1.4](https://www.rfc-editor.org/rfc/rfc8881.html#section-2.10.13.1.4)).
The caller must observe the affected names and identities before starting a new
operation. Live-session transport replay retains the existing exactly-once policy.
This extends the existing v4.2 mutation guard without adding a public API.

The 2026-11-08 Rust 1.95 regression covers both minor versions, four namespace
mutations and BADSESSION/DEADSESSION/SEQ_MISORDERED. In a private relay connected
to the real Linux NFSv4.1 server, an armed REMOVE received a synthetic BADSESSION:
the adapter retained `Connectivity/Unknown`, and the relay observed zero fresh
EXCHANGE_ID requests. Independent reads confirmed the original backup and a
subsequent same-name replacement were intact. Transport flash and lost REMOVE
response recovered; disconnect (323 ms), silent loss (15.0 s) and cancellation
(253 ms) preserved uncertainty. These are storage-operation measurements, not a
three-second application fault budget, physical server restart, v4.2 service,
Python wheel or throughput/RSS certification.

## Explicit ACL acquisition

Ordinary NFSv4 GETATTR, LOOKUP and directory observations request common attributes
without ACL. Callers select the existing `Mount::get_acl` when their metadata plan
requires it. `Attr::acl == None` in an ordinary observation is not evidence that
the server lacks ACL support. This avoids both repeated ACL acquisition and an
unrelated ACL refusal hiding the whole attribute result
([RFC 8881 section 18.7.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-18.7.3)).
No public method, request type or retry policy was added.

The Rust 1.95 regression first reproduced ACCESS from an ordinary GETATTR when ACL
was denied, then confirmed basic fields remain available while an explicit ACL
query retains ACCESS. Against real Linux NFSv4.1, a private relay audited 118
GETATTR/LOOKUP/READDIR attribute requests: zero implicit ACL requests, three explicit
ACL requests and two injected ACCESS replies. The omitted phase made no ACL query.
The application retained the complete copied payload and typed ACL acquisition
failure; source removal was refused. The successful selected raw ACL survived
snapshot roundtrip and copying. Independent reads confirmed all three source and
target payloads and no transfer artifacts. The object-destination case projects
S3 target capabilities; its actual data destination was NFS, not an S3 service.

The new request bitmap also passed NFSv3/v4.1 flash, persistent disconnect, silent
loss, cancellation and lost REMOVE response regressions, plus the v4.1 synthetic
BADSESSION guard. V3 silent loss remained 5.0 seconds, v4.1 15.0 seconds and
cancellation about 252 ms; no new application-wide recovery budget is claimed.
These checks do not certify server restart, power-cut durability, physical v4.0/
v4.2/pNFS, an installed Python wheel, full CLI/database/callback or throughput/RSS.

## Errors and application recovery

A definite temporary rejection that exhausts the policy returns
`NfsError::ServerBusy(Box<ServerBusyError>)` in Rust. The error includes
`context`, `retries`, `elapsed`, `reason` (`RetryLimit` or `Deadline`) and the
original protocol error in `source`. `is_server_busy()` identifies backpressure
without parsing display strings; an uncertain mutation takes precedence over
its nested status. Conversion to `std::io::Error` retains the structured source
and maps to `WouldBlock`.

Python raises `NfsServerBusyError`, a subclass of `NfsRetryableError`, preserving
`code`, `code_name`, `operation`, `protocol`, `filename` and the original error as
its cause. It adds `retry_count`, `retry_elapsed_ms` and `retry_reason`
(`retry_limit` or `deadline`), with `recovery_action=RETRY`. These optional fields
are immutable and survive pickle/context enrichment. Other errors leave the
retry fields as None. A transport timeout during an in-flight retry remains a
transport/uncertain error instead of claiming an authoritative busy rejection.

Each retry emits a debug event with operation, protocol, original status,
retry count, elapsed time and delay. Exhaustion emits one warning with its
reason. Applications should queue an exhausted file for later work and consider
reducing source concurrency. Immediately nesting another full-budget retry loop
multiplies load and latency. Never restart a modifying transfer merely because
a nested protocol error contains DELAY; honor Uncertain/VerifyThenResume first.
