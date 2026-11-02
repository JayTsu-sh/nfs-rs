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
