# NFSv4.0 protocol and recovery contract for nfs-rs

## Question and scope

This report answers which NFSv4.0 requirements and state machines `nfs-rs` must implement to provide the existing `Mount` capability surface, preserve uncertain-outcome safety, optionally retain delegations, and recover correctly. It also identifies NFSv4.1 assumptions that cannot be reused for minor version 0.

The normative baseline is [RFC 7530](https://www.rfc-editor.org/rfc/rfc7530.html), with wire definitions taken from its normative companion [RFC 7531](https://www.rfc-editor.org/rfc/rfc7531.html). RFC 7530 obsoletes RFC 3530, and explicitly makes RFC 7531 authoritative when prose and XDR differ ([RFC 7530 §§1.3, 21.1](https://www.rfc-editor.org/rfc/rfc7530.html#section-1.3)). This contract intentionally excludes referrals, full cross-server migration, RPCSEC_GSS, pNFS, and controller takeover/giveback from the first release; those exclusions do not relax ordinary state recovery on one server.

There is one explicit standards-positioning caveat: RFC 7530 says an NFSv4 implementation **must implement** RPCSEC_GSS even though deploying it is optional ([RFC 7530 §3](https://www.rfc-editor.org/rfc/rfc7530.html#section-3)). An AUTH_SYS-only first release can truthfully claim interoperable NFSv4.0 functionality for the configured profile, but not unconditional RFC 7530 conformance. Documentation and release criteria must state that profile limitation until RPCSEC_GSS is implemented.

## Decision

NFSv4.0 must be implemented as a separate protocol-state engine that may share stateless operation codecs and higher-level `Mount` helpers with `nfs41`, but must not reuse the v4.1 session executor as its control plane. A conforming first release needs:

1. a confirmed `clientid` established by `SETCLIENTID` and `SETCLIENTID_CONFIRM`;
2. independent, serialized open-owner and lock-owner sequence machines with replay-aware error handling;
3. explicit open, lock, and delegation stateid ownership and lifecycle tracking;
4. lease accounting and `RENEW` when ordinary stateful traffic is insufficient;
5. a recovery gate that stops state-dependent I/O while client/server incarnation and reclaim state are repaired;
6. an optional v4.0 callback RPC service for `CB_NULL`, `CB_GETATTR`, and `CB_RECALL` when delegation retention is enabled; and
7. an operation policy that distinguishes protocol replay from application-level retry and surfaces an uncertain outcome whenever execution cannot be proved.

The implementation should expose the existing `Mount` operations that RFC 7530 can express. NFSv4.1-only diagnostics and pNFS/session facilities remain explicitly unsupported on a v4.0 mount.

## Wire and connection contract

- Send NFS RPC program `100003`, version `4`, procedure `COMPOUND`, normally over persistent TCP to port 2049. TCP support is mandatory and persistent connections are recommended ([RFC 7530 §3.1](https://www.rfc-editor.org/rfc/rfc7530.html#section-3.1)). The integrated NFSv4 namespace starts with `PUTROOTFH`; there is no separate MOUNT protocol.
- Every v4.0 `COMPOUND` carries `minorversion = 0`. The server evaluates operations in order and stops on the first non-`NFS4_OK` result ([RFC 7530 §§14.1–14.2](https://www.rfc-editor.org/rfc/rfc7530.html#section-14.1)). A COMPOUND is not a transaction and may be only partly executed, so builders should remain short enough that partial completion can be classified.
- Wire operation numbers, unions, discriminants, attribute numbers, callback program structures, and status values must be generated from RFC 7531's NFSv4.0 XDR, not inferred from the v4.1 codec ([RFC 7531](https://www.rfc-editor.org/rfc/rfc7531.html)). Unknown or minor-version-forbidden operations are not silently accepted; the protocol defines `NFS4ERR_OP_ILLEGAL`/`NFS4ERR_NOTSUPP` behavior ([RFC 7530 §§13.1.1.5, 16.38](https://www.rfc-editor.org/rfc/rfc7530.html#section-13.1.1.5)).
- Reconnecting TCP does not itself create a new NFS client incarnation. The confirmed client identity and state can continue across transport connections while the lease is valid. RPC retransmission uses the RPC transaction identity; state-owner replay additionally depends on the owner `seqid` described below. A new connection must therefore not automatically allocate a new verifier/client identity.

## Client identity state machine

The v4.0 client identity is the root of all leased state:

```text
Uninitialized
  -> SETCLIENTID(stable opaque id, incarnation verifier, callback endpoint)
Unconfirmed(clientid, confirm verifier)
  -> SETCLIENTID_CONFIRM
Confirmed(clientid, lease epoch)
  -> ordinary OPEN/LOCK/delegation state
  -> stale/expired/restart evidence -> Recovering -> SETCLIENTID sequence
```

The client-supplied opaque ID must distinguish independent clients and remain stable for later incarnations; the verifier changes on client reinitialization so the server can discard the old incarnation's leased state ([RFC 7530 §§9.1.1, 9.6.1](https://www.rfc-editor.org/rfc/rfc7530.html#section-9.1.1)). `SETCLIENTID` returns both the server-assigned `clientid` and the confirmation verifier; `SETCLIENTID_CONFIRM` must complete before the client uses the ID for stateful work ([RFC 7530 §§16.33–16.34](https://www.rfc-editor.org/rfc/rfc7530.html#section-16.33)). Both operations are non-idempotent and require exact RPC-replay treatment, not reconstruction as a fresh logical request.

For this project, one mounted client object should own one stable identity and may share its persistent transport across concurrent tasks. Different mount objects must not accidentally present an identity/verifier combination that destroys each other's state. Callback program/address information is supplied during `SETCLIENTID`, so changing callback reachability requires re-establishing that information rather than a v4.1 backchannel bind.

## Open-owner and lock-owner sequencing

NFSv4.0 has no session slots. At-most-once behavior for state-changing open/lock requests is instead scoped to a state owner. Each open-owner and each lock-owner has its own monotonically increasing 32-bit `seqid`; no more than one request for the same owner may be outstanding. The server remembers the last sequence and response, returns the cached response for a duplicate last request, and rejects other misordering with `NFS4ERR_BAD_SEQID` ([RFC 7530 §§9.1.3, 9.1.7–9.1.9](https://www.rfc-editor.org/rfc/rfc7530.html#section-9.1.7)).

The executor therefore needs a per-owner async serialization boundary that spans send, reply classification, and sequence advancement. It must not impose a mount-wide mutex: different owners can progress concurrently. It must retain the exact encoded request (or enough immutable inputs to reproduce it exactly) until the reply is resolved, because retrying a state-owner request uses the same owner sequence and logical arguments. Sequence advancement follows the RFC's operation/error rules; it is not simply “increment on every response.” `NFS4ERR_BAD_SEQID` is a state-machine fault/recovery trigger, not a generic transient retry.

`OPEN` establishes share state and returns an open stateid. The server can require `OPEN_CONFIRM` on the first use of an open-owner; the client must complete it before treating that open as established. Later access/deny changes use additional `OPEN` operations and `OPEN_DOWNGRADE`; final release uses `CLOSE` ([RFC 7530 §§9.1.11, 9.10–9.11](https://www.rfc-editor.org/rfc/rfc7530.html#section-9.1.11)). This differs materially from the current v4.1 path, where `OPEN_CONFIRM` is not part of the normal minor-version-1 lifecycle.

The first `LOCK` for a lock-owner ties new lock-owner sequencing to a valid open-owner/open-stateid; later `LOCK` and `LOCKU` requests use the lock stateid and that lock-owner's sequence. `LOCKT` tests conflicts without acquiring state. `RELEASE_LOCKOWNER` is legal only after its locks are released ([RFC 7530 §§9.1.5–9.1.10, 16.10–16.12, 16.37](https://www.rfc-editor.org/rfc/rfc7530.html#section-9.1.5)). The public lock token must therefore resolve to a tracked lock-owner and full 128-bit stateid; an unscoped byte string alone is insufficient for recovery.

## Stateid and I/O contract

Open, byte-range-lock, and delegation stateids are distinct server objects. Their `other` field identifies state and their sequence component changes with state transitions; the client treats ordinary stateids as server-issued, read-only values ([RFC 7530 §§2.2.16, 9.1.4](https://www.rfc-editor.org/rfc/rfc7530.html#section-9.1.4)). Every tracked stateid must be associated with its client epoch, filehandle, state type, and owner. This prevents a stale stateid from crossing a recovery generation or being used with the wrong current filehandle.

State-dependent `READ`, `WRITE`, and size-changing `SETATTR` must select a valid open/lock/delegation stateid with sufficient access. Anonymous/special stateids have deliberately weaker semantics and cannot substitute for correct OPEN/LOCK tracking ([RFC 7530 §§9.1.4.3, 9.1.6](https://www.rfc-editor.org/rfc/rfc7530.html#section-9.1.4.3)). `NFS4ERR_OLD_STATEID` can permit using the current known stateid after synchronization; `NFS4ERR_BAD_STATEID`, `NFS4ERR_STALE_STATEID`, `NFS4ERR_EXPIRED`, and `NFS4ERR_ADMIN_REVOKED` require different recovery/application outcomes and must not share one retry bucket ([RFC 7530 §§13.1.5, 9.6.3.3, 9.8](https://www.rfc-editor.org/rfc/rfc7530.html#section-13.1.5)).

Successful modifying operations are synchronous except an `UNSTABLE4` WRITE; unstable data needs `COMMIT`, whose verifier must be checked so a server reboot cannot be mistaken for durable completion ([RFC 7530 §§14.3, 16.3, 16.36](https://www.rfc-editor.org/rfc/rfc7530.html#section-14.3)).

## Lease and recovery contract

All state under one client ID shares one lease. Valid stateful operations implicitly renew it; `SETCLIENTID`/`SETCLIENTID_CONFIRM` do not. When traffic cannot guarantee timely renewal, send `RENEW`; the worst case is one renewal RPC per lease period ([RFC 7530 §9.5](https://www.rfc-editor.org/rfc/rfc7530.html#section-9.5)). The implementation should read `lease_time`, maintain conservative send/reply bounds, renew well before expiration, and treat renewal task shutdown as part of mount teardown.

Recovery is a gate, not a background best effort. Once server restart, stale client/state IDs, or possible lease expiry is detected, new state-dependent I/O must wait. RFC 7530 explicitly requires queued READ/WRITE operations to wait until their protecting locks have been recovered ([RFC 7530 §9.6](https://www.rfc-editor.org/rfc/rfc7530.html#section-9.6)). One recovery coordinator per mount should:

1. freeze creation/use of affected state and establish/confirm the correct client identity;
2. distinguish server restart/grace reclaim from lease cancellation and client restart;
3. during server grace, reclaim eligible opens using `CLAIM_PREVIOUS`, then byte-range locks and eligible delegations, preserving the original owner/range/access model;
4. retry `NFS4ERR_GRACE` with bounded backoff while grace remains, and classify `NFS4ERR_NO_GRACE`/`NFS4ERR_RECLAIM_BAD` as lost state rather than ordinary transient errors;
5. prevent new non-reclaim locking until reclaim is resolved; and
6. publish a new epoch and resume only state that is known restored, reporting lost/revoked state to its caller.

After a partition longer than the lease, `NFS4ERR_EXPIRED` means the lease may have been canceled and the client must establish a new client ID; it must not pretend old locks still protect I/O ([RFC 7530 §§9.6.3, 9.8](https://www.rfc-editor.org/rfc/rfc7530.html#section-9.6.3)). If lease expiry is merely possible, mark locks unvalidated and validate them as specified before reliance. Client restart uses a new incarnation verifier and does not reclaim state that was not held at the end of its last successfully established lease.

## Delegation and callback contract

Delegation retention remains optional and off by default for migration workloads. Correct base I/O cannot depend on callbacks: servers probe callback continuity with `CB_NULL`, clients must tolerate OPEN succeeding without a delegation, and the server grants delegations only when it judges callback support available ([RFC 7530 §10.2](https://www.rfc-editor.org/rfc/rfc7530.html#section-10.2)). Minor version 0 has no OPEN argument that means “do not grant a delegation.” The disabled mode must therefore advertise no usable callback service during `SETCLIENTID` and defensively return any delegation a server nevertheless supplies before exposing the OPEN as complete.

When retention is enabled, v4.0 requires a separately reachable callback RPC program advertised in `SETCLIENTID`; it does not use the v4.1 session backchannel, `CB_SEQUENCE`, or callback slots. Implement procedure 0 `CB_NULL` and callback `CB_COMPOUND` minor version 0 with `CB_GETATTR` and `CB_RECALL` ([RFC 7530 §§17–18](https://www.rfc-editor.org/rfc/rfc7530.html#section-17)). Authenticate callbacks according to the callback credential rules in RFC 7530 §3.3.3.

On `CB_RECALL`, stop granting new local use under the delegation, flush dirty data and locally represented state, and send `DELEGRETURN`. The recall may race with the OPEN reply that grants the delegation, so the callback dispatcher and OPEN completion need a shared handoff that cannot lose an early recall ([RFC 7530 §§10.4.4–10.4.5](https://www.rfc-editor.org/rfc/rfc7530.html#section-10.4.4)). `CB_GETATTR` must report delegation-consistent size/change attributes. Callback failure or recall timeout can cause server revocation; handle the resulting stateid errors without corrupting ordinary open state.

## Retry and uncertain-outcome policy

The library must distinguish three layers:

1. **RPC duplicate replay:** retransmit the same encoded RPC with the same transaction identity when the transport outcome permits RPC replay. For sequenced state-owner operations, preserve the same owner `seqid`; the server's last-response cache supplies at-most-one behavior.
2. **Protocol-directed retry:** retry results such as `NFS4ERR_DELAY` as a new RPC transaction after waiting, but only after accounting for which preceding COMPOUND operations completed. RFC 7530 says COMPOUND is non-atomic and the client owns partial-completion recovery ([RFC 7530 §§13.1.1.3, 14.2](https://www.rfc-editor.org/rfc/rfc7530.html#section-14.2)).
3. **Application operation retry:** automatically issue a new logical request only when it is read-only or the library can prove the desired state/result. If a CREATE, REMOVE, RENAME, WRITE, SETATTR, LINK, or other mutation may have executed but neither a reply nor protocol replay proof survives, return the project's explicit uncertain outcome. Do not turn a reconnect, `NFS4ERR_STALE_*`, or server restart into blind mutation replay.

This product rule is intentionally conservative. RFC 7530 provides state-owner replay for locking and special mechanisms such as exclusive CREATE verifiers, but does not make an arbitrary COMPOUND transactional. Where reconciliation is possible, it must compare authoritative post-state (file identity/change attributes, namespace entries, size/content or migration checksum) rather than assume that an error means “not executed.”

## Existing `Mount` capability mapping

| Capability family | Required v4.0 operations/state | Contract |
|---|---|---|
| Namespace and metadata | `PUTROOTFH`, `PUTFH`, `GETFH`, `LOOKUP`, `LOOKUPP`, `GETATTR`, `SETATTR`, `ACCESS`, `READDIR`, `READLINK`, `CREATE`, `REMOVE`, `RENAME`, `LINK` | Support existing handle/path helpers; stop-on-error and partial-COMPOUND rules apply. |
| Regular file lifecycle | `OPEN`, conditional `OPEN_CONFIRM`, `OPEN_DOWNGRADE`, `CLOSE` | Track per-owner seqid and full open stateid; never reduce v4.0 OPEN to stateless LOOKUP. |
| Data | `READ`, `WRITE`, `COMMIT` | Use valid access stateid; validate count/eof/stability/write verifier; preserve uncertain outcomes. |
| Locks | `LOCK`, `LOCKT`, `LOCKU`, `RELEASE_LOCKOWNER` | Independent lock-owner sequence/stateid; serialize per owner, not globally. |
| ACL/identity | `GETATTR`/`SETATTR` for `acl`, `aclsupport`, `owner`, `owner_group`, `mode` | Respect server-supported attribute bitmap and RFC 7530 ACL/mode interaction (§§5–6); numeric AUTH_SYS migration policy needs separate validation. |
| Named attributes | `OPENATTR` plus namespace/data operations | Optional server capability: return an explicit unsupported result when ONTAP/RFC attribute support does not permit it. |
| Delegations | OPEN delegation result, `DELEGRETURN`, `DELEGPURGE`; callback procedures | Supported only when retention enabled; base correctness must not depend on a grant. The current `delegreturn(u64)` surface cannot carry RFC 7530's 128-bit `stateid4`; either return a typed opaque delegation token from the API or restrict this method to internally tracked tokens with explicit validation. |
| v4.1-only surface | channel limits/stats, sessions, slots, pNFS layout/device operations | Return `None`/`Unsupported` consistently; do not emulate them. |

For each family, implementation tests should include successful semantics and the significant failure/recovery branches rather than only proving that an opcode received `NFS4_OK`.

## NFSv4.1 assumptions that are invalid for v4.0

The current `src/nfs41` implementation makes these minor-version-1 assumptions; each needs a v4.0 seam rather than a conditional scattered through callers:

- `EXCHANGE_ID -> CREATE_SESSION -> RECLAIM_COMPLETE` establishes identity and readiness. V4.0 instead uses `SETCLIENTID -> SETCLIENTID_CONFIRM` and has no `RECLAIM_COMPLETE`.
- Every COMPOUND starts with `SEQUENCE`, and session slots provide concurrency, duplicate caching, limits, and status flags. V4.0 has no `SEQUENCE`, session ID, slot ID, fore-channel negotiation, or `SEQ4_STATUS_*`; concurrency and replay are owner/RPC scoped.
- Empty `COMPOUND(SEQUENCE)` renews leases. V4.0 uses implicit renewal from valid clientid/stateid operations or explicit `RENEW`.
- Callback transport is negotiated by `CREATE_SESSION`, callbacks start with `CB_SEQUENCE`, and `BIND_CONN_TO_SESSION` restores a backchannel. V4.0 advertises a separate callback program/address in `SETCLIENTID` and has neither callback sessions nor `CB_SEQUENCE`.
- `DESTROY_SESSION`/`DESTROY_CLIENTID` are teardown primitives. They are not v4.0 operations; teardown consists of releasing known opens, locks, and delegations, stopping renewal/callback work, and letting any remaining lease expire.
- V4.1 `OPEN` does not use `OPEN_CONFIRM`; v4.0 servers may require it for a new open-owner.
- pNFS MDS/DS identity, layouts, device information, and DS sessions are minor-version-1 facilities and cannot appear on a v4.0 mount.
- V4.1 error/status handling (`BADSESSION`, `BADSLOT`, `SEQ_MISORDERED`, session rebind) cannot drive v4.0 recovery. V4.0 recovery centers on owner seqids, client/state ID errors, lease/grace, and reclaim.

## Architecture constraints for the later implementation plan

The protocol boundary should consist of a shared RPC/XDR transport and reusable stateless operation representations, above which `Nfs40Mount` owns four deep modules:

- **Client epoch:** identity confirmation, lease clock, renewal and recovery coordinator.
- **Owner state:** per-open-owner/per-lock-owner serialization, seqid/replay record, stateids and reclaim description.
- **Compound executor:** minor version 0 encoding, operation-level result decoding, RPC replay classification, partial-completion/uncertain-outcome reporting.
- **Callback service:** optional v4.0 callback listener and race-safe delegation lifecycle.

This separation makes protocol-invalid states difficult to express: no request can accidentally prepend `SEQUENCE`, no v4.1 session status can enter v4.0 recovery, and state-dependent I/O cannot bypass the epoch gate. Stateless `Mount` helpers may be shared only above these invariants.

## Required verification evidence implied by the contract

The later FAS2750 validation contract must demonstrate at least:

- exact `minorversion=0`, client confirmation, ordinary lease renewal, idle `RENEW`, reconnect without identity churn, and clean teardown;
- concurrent different-owner progress and same-owner sequencing for OPEN/CLOSE and LOCK/LOCKU, including denied locks and replay/timeout cases;
- conditional `OPEN_CONFIRM`, share deny/access behavior, stateid access checks, unstable WRITE/COMMIT verifier handling, and uncertain mutation outcomes;
- server restart/grace reclaim, partition beyond lease, stale/expired state classification, lost-lock notification, and blocking of protected I/O during recovery;
- callback-disabled operation, callback reachability probe, optional delegation grant, early/ordinary recall, dirty-data flush, `DELEGRETURN`, callback loss, and revocation recovery; and
- semantic families across namespace, metadata, data, ACL/identity, named attributes (or explicit server unsupported), two LIFs, and AUTH_SYS root/non-root identities.

These tests are evidence for the state machines above; a successful Linux mount or a single successful opcode is not sufficient evidence of NFSv4.0 client support.

## Primary sources

- T. Haynes and D. Noveck, [RFC 7530: Network File System (NFS) Version 4 Protocol](https://www.rfc-editor.org/rfc/rfc7530.html), March 2015.
- T. Haynes and D. Noveck, [RFC 7531: Network File System (NFS) Version 4 External Data Representation Standard (XDR) Description](https://www.rfc-editor.org/rfc/rfc7531.html), March 2015.
