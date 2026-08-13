# FAS2750 NFSv4.0 capabilities and controlled fault surfaces

Research date: 2026-08-13 (Asia/Shanghai)  
Reference system: FAS2750 cluster at `10.128.61.20`, ONTAP 9.19.1  
Reference SVM/share: `lizy:/nfsrs_v40_test`

## Decision

The FAS2750 is suitable as the reference server for NFSv4.0 functional,
identity, callback/delegation, locking, reconnect, and single-LIF migration
validation. It is **not currently safe to use for automated server-restart
grace/reclaim testing**: the test volume is isolated, but its NFS service is
SVM-wide and `lizy` has unrelated active NFS clients. A dedicated test SVM (or
an approved maintenance window covering all `lizy` users) is required before
`vserver nfs stop/start` or controller faults become release-validation steps.

Use three distinct fault tiers:

1. Nightly-safe: client-side, destination-scoped packet loss/reset against one
   test LIF, with a trap that always restores firewall state.
2. Release-only and operator-approved: migrate exactly one test LIF and revert
   it, after checking connected clients and the target port.
3. Prohibited on the present shared SVM: NFS service stop/start, node
   takeover/giveback, port disablement, reboot, or power faults.

This distinction matters: a client-side partition validates reconnect, lease
expiry, callback loss, and uncertain-operation handling, but cannot create the
server recovery grace period needed to prove lock/delegation reclaim.

## Primary-source capability baseline

ONTAP documents support for all mandatory NFSv4.0 functionality except SPKM3
and LIPKEY, including COMPOUND, pseudo-filesystem traversal, file delegation,
and lease-based locking without separate NLM/NSM protocols
([NFSv4.0 functionality](https://docs.netapp.com/us-en/ontap/nfs-admin/nfsv40-functionality-supported-concept.html)).
ONTAP also documents important limits: no named attributes, no protocol-level
migration/replication support, persistent rather than volatile file handles,
and a subset of recommended attributes omitted
([NFSv4 limitations](https://docs.netapp.com/us-en/ontap/nfs-admin/limitations-support-nfsv4-concept.html)).
Those limits should be reflected as explicit expected behavior rather than
treated as gaps in the FAS2750 fixture.

Read-only CLI inspection established this actual baseline:

| Surface | Observed value |
| --- | --- |
| ONTAP | 9.19.1 |
| NFSv4.0 | enabled |
| NFSv4.0 ACL | enabled; ACL preservation enabled |
| NFSv4.0 read/write delegation | enabled / enabled |
| ID mapping domain | `localdomain` |
| Numeric owner IDs | enabled |
| Lease / grace | 30 s / 45 s |
| Test volume | `nfsrs_v40_test`, FlexVol, UNIX security style, 10 GiB |
| Junction/export | `/nfsrs_v40_test`; `nfs4`, AUTH_SYS RO/RW/superuser |
| Allowed clients | `10.131.0.0/20` |
| Data LIFs | `.200` home on FAS2750-01; `.201` home on FAS2750-02 |

The server's connected-client table showed real `nfs4` connections from
`10.131.9.11` to the test volume through both LIFs. This corroborates that the
fixture is usable, but wire evidence must still assert COMPOUND
`minorversion = 0`; ONTAP's `nfs4` display alone should not be the sole protocol
oracle.

## Identity mapping contract

ONTAP requires the NFSv4 ID domain to match between server and clients, and
documents numeric-string owner IDs as a separately enabled capability
([create an NFS server](https://docs.netapp.com/us-en/ontap/nfs-config/create-server-task.html),
[configure the ID domain](https://docs.netapp.com/us-en/ontap/nfs-admin/specify-user-id-domain-nfsv4-task.html)).
The observed SVM has `v4-id-domain=localdomain`, numeric IDs enabled, and name
service switch order `files` only for `passwd`, `group`, and `namemap`. Its local
UNIX database is small: users include `root` (UID 0), `lizy` (1004), `pcuser`
(65534), and `nobody` (65535); local groups include IDs 0, 1, 65534, and 65535.
ONTAP documents that NFS authentication consults the configured name services
for UNIX credentials
([ONTAP name services](https://docs.netapp.com/us-en/ontap/nfs-admin/ontap-name-services-concept.html))
and recommends retaining local entries as fallback
([name-service switch](https://docs.netapp.com/us-en/ontap/nfs-admin/ontap-name-service-switch-config-concept.html)).

Therefore the release matrix must not infer metadata fidelity from root-only
tests. It must use AUTH_SYS and verify both protocol attributes and a second
reader's `GETATTR` for:

- known UID/GID pairs represented in ONTAP local files;
- an ordinary test UID/GID not represented there;
- root, under the observed export rule (`superuser=sys`, anonymous UID 65534);
- `owner`/`owner_group` as `name@localdomain` and as numeric strings;
- mode, chown/chgrp permission failures, NFSv4 ACL set/get/inheritance, and
  unknown-owner behavior.

The numeric-ID option being enabled is capability, not proof that every
unknown identity round-trips. A mismatch, `nobody`, or 65534 result must be
treated as a failed metadata-preservation case, not normalized by the client.

## Callback and delegation contract

The SVM enables NFSv4.0 read and write delegations. ONTAP describes delegation
as optional client caching and warns that delegations must be recovered after a
server/client restart or network partition
([read delegation](https://docs.netapp.com/us-en/ontap/nfs-admin/enable-disable-nfsv4-read-file-delegations-task.html)).
Server enablement does not guarantee that an individual OPEN receives one.

Validation must therefore capture the OPEN result and skip-with-evidence when
ONTAP elects not to grant a delegation; it must never report server enablement
as a successful delegation test. When granted, verify:

- the callback endpoint supplied during NFSv4.0 client establishment is
  reachable from the data network;
- a conflicting OPEN from a second client produces `CB_RECALL`;
- the first client quiesces conflicting use, returns the delegation, and the
  second OPEN completes;
- callback packet loss causes safe fallback/recovery rather than data loss;
- explicit client shutdown returns or abandons state without hanging.

During the test, correlate wire events with the read-only
`vserver locks show -vserver lizy -volume nfsrs_v40_test`; ONTAP documents that
this command exposes delegation, share, and byte-range lock state, although it
cannot display client IP for NFSv4 locks
([display locks](https://docs.netapp.com/us-en/ontap/nfs-admin/display-locks-task.html)).
The migration-oriented client default should remain “do not retain
delegations”; explicit delegation tests turn retention on.

## Locking, lease, grace, and reclaim

ONTAP maintains NFSv4 locks under a lease model
([NFSv4 locking](https://docs.netapp.com/us-en/ontap/nfs-admin/nfsv4-file-record-locking-concept.html)).
The command reference defines the observed 30-second lease and 45-second grace:
the lease is the period for which a lock is irrevocably granted, while grace is
the recovery interval in which clients reclaim lock state
([`vserver nfs modify`](https://docs.netapp.com/us-en/ontap-cli-991/vserver-nfs-modify.html)).
ONTAP's recovery guidance explicitly says grace permits clients to reclaim
locking state after server recovery
([grace period](https://docs.netapp.com/us-en/ontap/nfs-admin/specify-nfsv4-locking-grace-period-task.html)).

Functional validation must cover conflicting byte-range locks, shared/exclusive
OPEN state, LOCKT, LOCKU, owner sequencing, CLOSE with outstanding locks, and
independent owners. Nightly partition tests should bracket durations below and
above 30 seconds and verify lease renewal, expiration, reconnect, and the
absence of stale server state.

True reclaim validation additionally requires a server state-loss event and
wire assertions for grace responses and reclaim OPEN/LOCK. A network partition
alone does not establish this. `vserver nfs stop/start` is SVM-scoped; the
read-only connected-client inventory showed unrelated NFSv3 and NFSv4.1 users
and volumes on `lizy`. It is consequently forbidden in unattended testing on
the current fixture. Provision a dedicated SVM before making server
grace/reclaim a release gate, or run it only in an explicitly approved SVM-wide
maintenance window.

## LIF migration

Both test LIFs use the `system-defined` failover policy and have targets on both
FAS2750 nodes. This makes a one-LIF release experiment feasible, but it is not
volume-isolated: any client using that LIF can be affected. ONTAP documents a
delay of up to 45 seconds when an NFSv4 LIF migrates between nodes
([migrate a LIF](https://docs.netapp.com/us-en/ontap/networking/migrate_a_lif.html)).
It also warns that administratively taking a LIF down holds outstanding NFSv4
locks until it returns, which can cause conflicts through other LIFs
([modify a LIF](https://docs.netapp.com/us-en/ontap/networking/modify_a_lif.html)).

Release validation may migrate **one named LIF only**, never `migrate-all`:

1. Record home/current node and port, failover targets, NFS connected clients,
   active test locks, and NFS service status.
2. Refuse to run if non-test clients are active on the chosen LIF.
3. Migrate to the explicitly prevalidated peer `e0c-61` port; do not modify the
   home location or administrative status.
4. Bound expected interruption by the documented 45 seconds and validate
   reconnect, owner/lock continuity, and uncertain modifying-operation results.
5. Revert the LIF and verify its exact original current node/port and `up/up`
   state in a cleanup trap.

This tests transport relocation. It does not validate the unsupported NFSv4
filesystem migration/referral feature and does not substitute for server-state
reclaim.

## Protocol evidence and tracing

The preferred evidence source is client-side `tcpdump`/pcap on the isolated
runner, filtered to one server LIF and TCP/2049. It requires no ONTAP mutation
and can prove minor version, COMPOUND operation order/status, SETCLIENTID
exchange, callbacks, lock/reclaim flags, and request/reply ambiguity.

NetApp documentation lists ONTAP packet tracing commands
([network diagnostics](https://docs.netapp.com/us-en/ontap/networking/commands_for_diagnosing_network_problems.html)),
but the documented `network trace` and `network tcpdump` command families were
not recognized in the inspected 9.19.1 admin/advanced CLI. Do not place them in
automation until their availability and cleanup are proven for this exact
build. Older command documentation also warns that a server-side trace writes
bounded rolling files and can affect the root volume
([`network trace start`](https://docs.netapp.com/us-en/ontap-cli-991/network-trace-start.html));
that is another reason to prefer client capture.

Every real test record should include ONTAP version, SVM/volume, selected LIF,
runner address, exact `version=4.0`, run ID, monotonic timestamps, pcap/artifact
hashes, pre/post server state, result classification, and cleanup verification.
Captures may contain filenames and numeric identities and must follow the lab's
artifact access and retention policy.

## Safe fault-injection matrix

| Fault | Scope / evidence | Allowed tier |
| --- | --- | --- |
| Drop TCP/2049 packets on runner for one destination LIF | Reconnect, lease renewal/expiry, callback loss, uncertain outcome | Nightly; cleanup trap mandatory |
| Kill only the test client process | Client restart and state cleanup | Nightly |
| Migrate one test LIF to peer and revert | Up to 45 s NFSv4 interruption; transport relocation | Release only, operator approval and zero unrelated clients on LIF |
| Set a LIF admin-down | Holds NFSv4 locks and affects every user of the LIF | Prohibited |
| Stop/start NFS on `lizy` | Interrupts every NFS share/client on the SVM; creates server recovery surface | Prohibited until dedicated SVM or maintenance approval |
| Controller takeover/giveback, reboot, power fault | Cluster/node-wide impact | Out of current support contract |

No fault case may run concurrently with another destructive lab job. Each must
have explicit preconditions, bounded timeout, idempotent recovery, postcondition
checks, and an operator-visible abort/recovery procedure.

## Read-only inspection commands

The following command families were used without configuration changes:

```text
version
vserver nfs show/status/connected-clients show
vserver export-policy rule show
vserver services name-service ns-switch show
vserver services name-service unix-user/unix-group show
vserver name-mapping show
vserver locks show
volume show
network interface show
network interface failover-groups show
```

No ONTAP setting, LIF, service, lock, or file was changed during this research.
