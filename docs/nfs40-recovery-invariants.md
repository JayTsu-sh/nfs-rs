# NFSv4.0 recovery gating invariants

Reference for the recovery/state-reuse work in PR #163. Every row is meant to be
checkable against the code. When a change invalidates a row, update the row in
the same commit instead of adding a new special case.

## Terms

- **open lane** — per `(owner, filehandle)` open state: stateid, access mask,
  `closing` flag, `io_fence`. Registered in `OpenState`.
- **lock lane** — per granted byte-range lock: stateid, type, range, owner.
  Registered in `LockState`.
- **lost-lock gate** — the per-filehandle set of locks that recovery could not
  restore. While it is non-empty, data I/O on that handle fails locally instead
  of running unprotected.
- **io_fence** — `tokio::sync::RwLock<()>` on each open lane. Data requests take
  it for read; recovery and CLOSE take it for write when they publish a loss or
  release the state.

## Admission rule

Data I/O on a filehandle is admitted only when both hold:

1. no lock loss is published for the handle, and
2. the mount lifecycle permits the request: `Ready` for stateful dispatch (via
   `begin_stateful`), `Ready` for anonymous dispatch (via `gate_stateful`).

There is one implementation, `admit_data_io` in `src/nfs40/mount.rs`; new data
operations must route through it rather than open-coding either check.

The authoritative evaluation point is:

- **stateful** — while holding the lane that will carry the request. Recovery
  publishes the loss while it holds that same lane, so a check without the lane
  can miss a loss published during a wait.
- **anonymous** — immediately after the blocking lookup returned no lane.

A call at an operation's entry is only a fast path; it never replaces the
authoritative check.

## Operation matrix

| Operation | Writes data | Stateid | Lost-lock gate | Lifecycle | Fence | Fence owner |
| --- | --- | --- | --- | --- | --- | --- |
| READ with lane | yes | lane | entry (fast) + under lane | `begin_stateful` | read | detached settlement |
| READ without lane | yes | anonymous | entry + after lookup | `gate_stateful` | none (no lane) | — |
| WRITE | yes | lane | entry (fast) + under lane | `begin_stateful` | read | detached settlement |
| SETATTR with `size` and lane | yes (writes the truncated range) | lane | under lane | `begin_stateful` | read | settlement task (`settled_call_holding`) |
| SETATTR with `size`, no lane | yes | anonymous | after lookup | `gate_stateful` | none (no lane) | — |
| SETATTR without `size`, SETACL | no | lane if present, else anonymous | not gated | `begin_stateful` if a lane exists | none | — |
| LOCK | no (installs protection) | lane | not gated — a LOCK is what clears the gate | `begin_stateful` | read | LOCK task |
| LOCKU | no | lock lane | not gated — releasing cannot bypass anything | `begin_teardown` | none | — |
| COMMIT | flush only | none in v4.0 | not gated | `begin_stateful` if a lane exists | none | — |
| CLOSE / `close_stateful` | no | open lane | not gated — clears the gate when the last open leaves | `begin_stateful` | write | close task |
| LOCKT | no | ephemeral owner | not gated | none | none | — |
| GETATTR, READDIR, LOOKUP, ACCESS, SECINFO, pathconf | no | none | not gated | none | none | — |

Rationale for the ungated rows: COMMIT carries no stateid in NFSv4.0 and only
flushes already-written data; LOCKT installs no state; metadata operations are
legal with the anonymous stateid (RFC 7530 §9.1.4.3). None of them can expose
data of a range whose protection was lost.

## Publication and clearing

A loss is published only from recovery, always before the affected lane is
dropped:

| Trigger | Code | Held while publishing |
| --- | --- | --- |
| Reclaim LOCK fails authoritatively, open survives | `recovery::reclaim_lock_lane` | open lane + lock lane + write fence |
| Reclaim LOCK's open owner is already gone | `recovery::reclaim_lock_lane` (no-open branch) | lock lane (no fence: no open lane exists to borrow) |
| Session-wide failure (`STALE_CLIENTID`, `EXPIRED`) | lease/state reset | publication lock; clears every lane and the whole gate |

A gate is cleared only when it can no longer protect anything:

| Trigger | Code | Condition |
| --- | --- | --- |
| LOCK restores coverage | `LockState::resolve_lost` | new lock type is at least the lost type and its range covers the lost range |
| CLOSE on the handle | `Mount40::close` | no open lane remains for the handle |
| CLOSE on the open state | `close_stateful` | no open lane remains for the handle |
| Session-wide failure | `LockState::clear` | all lanes are gone |

Lane teardown (`release_locks`) does not clear the gate: the point of the gate is
to outlive the state that was lost.

## Lock order

`io_fence` → open lane → lock lane → lost map.

- The fence is always taken before any lane.
- No path holds a lane guard while waiting for a fence write; readers clone the
  fence inside a lane guard, release the lane, then wait on the fence.
- The fence guard must live in the future that sends the request (READ/WRITE
  detached settlement, truncate settlement task, LOCK task, close task), never in
  the caller's future: cancelling a caller drops its future but not the spawned
  settlement, and a released fence would let CLOSE or recovery overtake an RPC
  that is still on the wire.

## Cancellation coverage

Cancelling a caller must never release the fence while its request is still on
the wire. Each data operation has a test that aborts the operation with the RPC
pending and asserts that CLOSE does not reach the server before the reply:

| Operation | Test |
| --- | --- |
| READ | `cancelled_read_keeps_its_close_fence_until_rpc_settlement` |
| WRITE | `cancelled_write_keeps_its_close_fence_until_rpc_settlement` |
| SETATTR with `size` | `cancelled_truncate_keeps_its_close_fence_until_rpc_settlement` |
| LOCK | CLOSE cannot snapshot the lock before the grant is registered (`wait_for_acquisitions`), and the fence adds the same ordering for the RPC itself |

RECLAIM uses the same write fence as CLOSE, so a cancelled request blocks both
identically; `a_truncate_waits_for_the_publish_fence_before_sending` covers the
opposite direction (a request waiting for a publish).

## Coverage map

Every behavioural statement above is backed by a test that fails when the
corresponding code is removed (each was verified by mutation). Names are in
`src/nfs40/mount/tests.rs` unless noted.

| Statement | Test |
| --- | --- |
| READ with a lane honours the gate, including when it queued behind recovery | `io_queued_behind_a_denied_reclaim_is_gated_at_dispatch` |
| READ with a lane rechecks after waiting on a lane mutex | `a_read_that_queued_on_a_lane_sees_a_loss_published_during_the_wait` |
| READ without a lane honours the gate | `a_lock_whose_open_recovery_dropped_still_gates_io_until_close` |
| READ without a lane fails after session-wide loss | `a_stateless_read_after_total_state_loss_fails_locally` |
| WRITE honours the gate | same two gate tests, plus `cancelled_write_keeps_its_close_fence_until_rpc_settlement` for the fence |
| SETATTR with `size` honours the gate with a surviving lane | `a_truncate_with_a_surviving_open_still_honours_the_lost_gate` |
| SETATTR with `size` honours the gate without a lane | `a_truncate_without_any_open_lane_still_honours_the_lost_gate` |
| Metadata SETATTR and GETATTR are not gated | `setattr_size_after_state_loss_fails_locally_but_metadata_still_works`, `metadata_is_not_gated_by_a_lost_lock` |
| A covering LOCK lifts the gate; LOCK is never gated | `a_covering_lock_lifts_the_gate_and_io_resumes` |
| LOCKU does not lift the gate | `locku_does_not_clear_the_gate` |
| COMMIT is not gated | `commit_is_not_gated_by_a_lost_lock` |
| LOCKT is not gated | `lockt_is_not_gated_by_a_lost_lock` |
| CLOSE lifts the gate with and without a surviving lane | `close_with_a_surviving_lane_lifts_the_gate`, `a_lock_whose_open_recovery_dropped_still_gates_io_until_close` |
| `close_stateful` lifts the gate | `close_stateful_lifts_the_gate_when_recovery_dropped_the_last_open` |
| Lane teardown does not lift the gate | `release_locks_does_not_lift_the_gate` |
| Zero-length READ never reaches the wire | `a_zero_length_read_short_circuits_before_the_gate` |
| A loss published under a lane is seen by queued I/O | `io_queued_behind_a_denied_reclaim_is_gated_at_dispatch` |
| A loss published when the open is gone | `a_lock_whose_open_recovery_dropped_still_gates_io_until_close` |
| A publish waits behind an in-flight modification | `a_truncate_waits_for_the_publish_fence_before_sending` |
| The EOF sentinel restores coverage | `nfs40::state` unit tests (`restores`, `end`) and `a_covering_lock_lifts_the_gate_and_io_resumes` |
| Session-wide loss clears state and the gate | `a_stateless_read_after_total_state_loss_fails_locally` |

## Known residuals

1. The gate is keyed by filehandle, not by lock owner. With two opens on one
   handle, a loss on one gates I/O through the other until a covering lock is
   taken or the handle is closed. Conservative, never permissive.
2. With no open lane on the handle, the gate can only be lifted by a covering
   LOCK, a CLOSE on that handle, or a session-wide loss. Path-based callers such
   as `truncate(path, n)` never open a lane and have no handle to close, so their
   requests keep failing with `StateRecoveryExhausted` until the owner of the
   lost lock re-locks, closes, or the mount loses its state. This is the
   conservative direction, but it is caller-visible.
3. Anonymous dispatch has no fence to borrow. It rechecks after the blocking
   lookup but cannot be made atomic with a publish; the window is inherent to
   serving a request with no lane state.
4. COMMIT, LOCKT and metadata operations are not gated (see the matrix).
5. `close_stateful` reports `InvalidInput` for an owner whose lane recovery
   dropped while sibling opens remain; CLOSE on the handle still clears the gate.
6. The gate protects the client's own callers only. Servers enforce share
   reservations and mandatory locks, not advisory locks (RFC 7530 §9.1.6).

## Checklist for a new data operation

1. Route admission through `admit_data_io` and pick `Stateful` or `Anonymous`.
2. If it modifies data, hold the lane's read fence and move the guard into the
   future that sends the request.
3. If it can run without a lane, state the residual explicitly.
4. If it can publish or clear a loss, add it to the tables above and to the
   coverage map, with a test that fails when the behaviour is removed.
5. Add a cancellation test: abort the operation with the RPC pending, then CLOSE.
