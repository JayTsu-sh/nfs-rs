# Issue #109: Python `open("wb")` protocol research

Date: 2026-11-03. Source snapshot: `34d3b691e5e8372eab6e2bbc2374b7033999400e`.
Scope: source, protocol, and supplemental FAS2750 measurements for
[issue #109](https://github.com/JayTsu-sh/nfs-rs/issues/109). The research sections
describe this baseline snapshot; the implementation and its validation are
recorded in the follow-up at the end.

## Findings

The extra work reported in the issue is present in the current source. However,
deleting the truncate after a successful create branch is unsafe: all three
engines currently use unchecked creation, which can return an existing file
after a competing client creates the name. A previous `NOENT` does not establish
ownership of the later create result.

The Python/Rust comparison also covers different work. Python `open("wb")`
means create-or-open, truncate, and retain an open resource until `close()`.
Rust `create_path` is a create operation and, for NFSv4, leaves state available
for later I/O and cleanup. A comparison with an explicit Rust close and the
same permissions is needed before attributing the entire latency gap to the
Python binding. The measurements below include these controls and reproduce
the additional RPCs on both supplied LIFs.

## Current call paths

Source links are relative to this document; line numbers below identify this
snapshot and may move with later changes.

| Surface | Current behavior and source |
| --- | --- |
| Mode parsing | `FileMode::parse` sets `create=true`, `truncate=true`, and write-only OPEN access for `wb`; creation requests mode `0666`. [`src/python_adapter.rs`](../src/python_adapter.rs), lines 288–325. |
| Python open | `open_file` first calls `open_path_stateful`, even for a new file. Only a not-found result selects `create_path_stateful_with_access`; an existing-name error from create triggers another open. It then unconditionally sends size-zero SETATTR for `wb`. If that fails it attempts `close_stateful` before returning the error. Same source, lines 3346–3408. |
| NFSv3 | Stateful open defaults to stateless lookup and close defaults to no-op in [`src/mount.rs`](../src/mount.rs), lines 574–612. [`src/nfs3/create.rs`](../src/nfs3/create.rs), lines 32–47, sends UNCHECKED CREATE with requested mode and no size attribute. Python subsequently adds SETATTR(size=0). |
| NFSv4.0 | [`src/nfs40/mount.rs`](../src/nfs40/mount.rs), `open_file` at line 705, sends PUTFH/OPEN/GETFH; OPEN_CONFIRM is another RPC when required. Stateful create at line 1680 adds a separate mode SETATTR. Python adds another size SETATTR. [`src/nfs40/compound.rs`](../src/nfs40/compound.rs), lines 166–184, encodes UNCHECKED4 and an empty creation attribute list. |
| NFSv4.1 | [`src/nfs41/write.rs`](../src/nfs41/write.rs), `create_with_access` at line 241, sends PUTFH/OPEN/GETFH/GETATTR through the session compound helper; creation attributes are empty and mode is set in a subsequent compound. Python then sends size SETATTR. |

Thus the new-file path includes an unsuccessful initial lookup/OPEN, creation,
and truncation. NFSv4 additionally has the mode-setting compound and final CLOSE;
NFSv4.0 can also require OPEN_CONFIRM. Parent-directory lookups, delegations,
layout state, and path depth can add further RPCs. These are source-level
observations, not a packet-count measurement.

Two existing details matter to any optimization:

- NFSv4.1 `create_with_access` deliberately omits creation mode attributes to
  avoid `NFS4ERR_ATTRNOTSUPP`, according to its source comment. Its later mode
  SETATTR logs transport errors and does not validate the individual SETATTR
  result before returning success. A new fast path needs explicit error
  semantics; this existing behavior is not evidence that creation attributes
  are universally unsupported.
- [`src/nfs41/setattr.rs`](../src/nfs41/setattr.rs), lines 21–29, takes the file
  I/O lock and flushes pending layout commits before changing size. This prevents
  retained DS writes from restoring old data after truncation. Moving truncation
  into OPEN must preserve this ordering when an existing file has active state.

## Protocol evidence

- NFSv3 distinguishes unchecked, guarded, and exclusive creation. Guarded
  creation rejects an existing name, but a lost reply makes a later EXIST
  ambiguous; guarded creation alone does not provide exactly-once execution.
  See [RFC 1813 §3.3.8](https://www.rfc-editor.org/rfc/rfc1813.html#section-3.3.8).
- NFSv4 unchecked OPEN can return an existing file. Existing-file creation
  attributes are ignored except size zero, which truncates it; guarded OPEN
  rejects an existing name. See [RFC 7530 §16.16.5](https://www.rfc-editor.org/rfc/rfc7530.html#section-16.16.5)
  and [RFC 8881 §18.16.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-18.16.3).
- NFSv4.0 OPEN must be followed by GETFH in the same compound so that the client
  can later close the state. Compounds are not transactions and partial success
  requires client recovery. See [RFC 7530 §16.16.4](https://www.rfc-editor.org/rfc/rfc7530.html#section-16.16.4)
  and [§15.2.3](https://www.rfc-editor.org/rfc/rfc7530.html#section-15.2.3).
- NFSv4.1 adds a current-stateid facility allowing a later operation in the
  compound to use the state returned by OPEN; it is not a v4.0 facility.
  SETATTR size changes need locking context. See [RFC 8881 §16.2.3.1.2](https://www.rfc-editor.org/rfc/rfc8881.html#section-16.2.3.1.2),
  [§8.2.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-8.2.3), and
  [§18.30.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-18.30.3).

## Optimization choices

These are implementation proposals inferred from the source and RFCs, not
validated behavior on every server.

| Choice | Benefit | Correctness boundary |
| --- | --- | --- |
| Skip truncate whenever create returned success | Saves one request | Reject: unchecked success does not prove a new file. A concurrent creation can leave old contents intact. |
| Skip truncate if returned attributes report size zero | Saves the standalone truncate in some cases | A size observation does not establish a truncate ordering point relative to concurrent writes. Do not use it as a general proof of `wb` semantics. |
| Explicit guarded creation followed by existing-file open/truncate | Removes the failed initial open on the new-file path; provides a definite fresh-create case | Requires a dedicated operation/creation result and unchanged uncertain-outcome handling. It cannot be implemented by treating the current unchecked result as exclusive. |
| NFSv4 unchecked OPEN with mode and size-zero creation attributes | Can combine open/create/truncate while preserving existing-file mode by protocol definition | Implement behind a common mount operation; validate server behavior, creation attribute masks, access, pNFS ordering, and any compatibility fallback. Avoid silently changing the general Rust `create_path` contract to truncate existing files. |
| NFSv4.1 OPEN followed by SETATTR using current stateid in one compound | Reduces wire round trips while retaining OPEN state context | Preserve GETFH before operations that can fail; parse successful OPEN even when later operations fail and close/track that state. Keep CLOSE separate until the Python file is actually closed. |

The preferred interface seam is a common stateful open operation expressing
create and truncate intent, with protocol-specific implementations and a
conservative default. That places race handling and state ownership in the
engines instead of adding Python checks for protocol versions. A minimal first
step could address known-new creation with guarded semantics; the full v4
create-or-truncate path offers larger savings but crosses more state boundaries.

## Validation needed before a fix is accepted

1. Scripted RPC tests must check the encoded creation method, size and mode
   attributes, requested share access (`wb` versus `w+b`), and RPC sequence for
   new and existing files. Include a name created between the original miss
   and create. New-file success must not merely mock away the race.
2. Preserve mode/content contracts: new `wb` returns empty; existing `wb` and
   `w+b` truncate while preserving permissions; append modes retain contents;
   read modes do not create. Check simultaneous opens and another writer.
3. Exercise OPEN success followed by GETFH/GETATTR/SETATTR failure, malformed
   replies, cancellation, and lost responses. Verify closeable state is retained
   or cleaned up, and uncertain operations do not become blind retries.
4. Cover v4.0 OPEN_CONFIRM and delegation handling, v4.1 sessions and replay,
   and truncate racing with retained pNFS writes/layout commits. Test rejected
   creation attributes and any fallback explicitly.
5. Build the Python wheel from the tested checkout and exercise the installed
   Rust adapter. The in-memory test backend clears a vector for truncation; it
   does not prove wire behavior or protocol state cleanup.
6. On each allowed physical endpoint/protocol, compare the same unique-file
   workload before and after with the same build configuration. Report open
   and close separately, total latency, RPC counts, new/existing target cases,
   permissions, and sample count. Compare Rust create plus explicit close and
   equivalent kernel creation semantics. Keep packet traces and machine-local
   environment details outside the repository; use the existing performance
   acceptance gates for a release decision.

## Supplemental FAS2750 investigation

The historical report cited by the issue is available in
[repository history](https://github.com/JayTsu-sh/nfs-rs/blob/4d9ab966e22b635f9fae5af4d494a233069f8899/docs/benchmarks/fas2750-nfsrs-vs-kernel-2026-09-04.md).
It used nfs-rs 0.6.1 and a different client/kernel. The following measurements
use the current snapshot above, so their absolute times are not a historical
before/after comparison.

Read-only ONTAP discovery confirmed FAS2750 hardware, ONTAP 9.19.1, and that
the supplied source and target LIFs belong to the same SVM. They are called
LIF A and LIF B here. A preexisting export allowing all three protocols was
used because the dedicated performance export did not allow this executor.
No server configuration was changed.

### Method

- Rust 1.95.0, release builds, nfs-rs 0.11.1 at the recorded commit. The Rust
  probe reused the repository lockfile dependency versions. Python used a
  freshly built ordinary wheel in a dedicated CPython 3.12 virtual environment;
  `python-test-support` was disabled. The kernel control used Linux 6.8.
- Each case used 205 operations, discarded the first five, and retained 200.
  New-file cases used unique names inside a unique run-owned directory. Mount,
  client setup, and post-operation stat checks were outside the timing window.
  Open/create and close were timed separately; the table reports their total.
- Python measured public `Client.open(path, "wb")` followed by `File.close()`.
  Rust measured `create_path(path, Some(0o666))` and explicit `close(fh)`.
  The latter retains create semantics rather than general create-or-truncate
  semantics. Kernel measured `open(O_WRONLY | O_CREAT | O_TRUNC, 0666)` and
  `close`, with umask zero and default metadata caching.
- The shared test/performance locks were held in the repository's acquisition
  order. `prepare-run.sh` and unconditional `cleanup-run.sh` used the generic
  mounted-directory helper, explicitly retargeted to the supplied FAS export.
  That helper's `afx` profile name does not identify the tested hardware;
  its temporary NFSv4.2 mount only prepared/removed the directory.
- RPC requests were counted by unique XID within each operation's timestamp
  window, excluding setup and validation. Raw scripts, JSON, and packet
  captures remain outside the checkout. These are diagnostic measurements,
  not the cross-environment release performance acceptance gate.

### Main pass: new-file create and close

Times are milliseconds, p50 / p95. Each cell contains 200 successful samples.

| LIF | Protocol | Python `wb` + close | Rust create `0666` + close | Kernel create + close |
| --- | --- | --- | --- | --- |
| A | 3 | 2.229 / 5.027 | 1.007 / 1.109 | 0.963 / 1.355 |
| A | 4.0 | 4.544 / 7.826 | 2.642 / 5.111 | 1.148 / 1.474 |
| A | 4.1 | 3.967 / 7.764 | 2.301 / 4.052 | 1.192 / 1.541 |
| B | 3 | 1.721 / 2.783 | 0.776 / 1.166 | 0.790 / 1.151 |
| B | 4.0 | 3.023 / 6.207 | 2.000 / 4.241 | 0.953 / 1.296 |
| B | 4.1 | 3.374 / 5.170 | 1.777 / 5.495 | 1.056 / 1.471 |

Python new-file `wb` issued exactly 5, 7, and 7 NFS RPC requests per sample
for v3, v4.0, and v4.1 respectively, on both LIFs:

| Protocol | Observed Python new-file request sequence |
| --- | --- |
| 3 | LOOKUP(parent), LOOKUP(target) → NOENT, LOOKUP(parent), CREATE, SETATTR(size=0). CLOSE is local. |
| 4.0 | Parent lookup compound, OPEN(NOCREATE) → NOENT, parent lookup compound, OPEN(CREATE), SETATTR(mode), SETATTR(size=0), CLOSE. |
| 4.1 | Same seven compounds as v4.0, with SEQUENCE in each compound. GETATTR is included in OPEN compounds. |

No OPEN_CONFIRM occurred in these new-file samples. It remains a protocol
case that an implementation must support. The two SETATTR requests were
identified against the source and decoded request order.

Rust create with mode `0666` and close used 2 / 5 / 4 RPCs. The v4.0 raw
`create_path` control includes a separate GETATTR at
[`src/nfs40/mount.rs`](../src/nfs40/mount.rs), line 1665; its stateful creation
path used by Python omits that GETATTR. Rust create with `mode=None` used
2 / 4 / 3 RPCs. Kernel creation typically used two requests; one final kernel
sample's requests were absent from the capture tail, so it is excluded from
packet-count attribution but retained in syscall latency statistics.

Existing-file Python `wb` used 3 / 4 / 4 requests and had p50 total latencies
of 1.265 / 2.443 / 2.193 ms on A and 0.964 / 1.586 / 1.826 ms on B. These
cases repeatedly truncated an already empty target; separate untimed semantic
checks exercised truncation of nonempty contents. Thus the new-file penalty
also includes the initial miss and repeated parent lookup, not only truncate.

### Control: execute the Python call sequence directly in Rust

A supplemental probe mirrored `open_file`: stateful OPEN with write access,
create with `0666` on NOENT, size-zero SETATTR, and stateful CLOSE. A second
control created directly with the same access/mode and then closed. This
separates the operation sequence from Python binding and resource bookkeeping.

The first five rows are from the supplemental pass. The final row is a
separate reduced-workload pass after the failure described below. Every
reported cell has 200 samples; the different passes must not be pooled.

| LIF | Protocol | Python `wb` total p50 ms | Rust mirrored sequence p50 ms | Rust direct stateful create/close p50 ms |
| --- | --- | --- | --- | --- |
| A | 3 | 2.696 | 2.401 | 1.093 |
| A | 4.0 | 4.394 | 4.074 | 2.413 |
| A | 4.1 | 4.782 | 3.925 | 2.409 |
| B | 3 | 2.143 | 1.739 | 0.826 |
| B | 4.0 | 2.931 | 2.908 | 1.752 |
| B | 4.1 | 2.795 | 2.848 | 1.749 |

The Rust mirror reproduced the Python 5 / 7 / 7 request counts. Direct
stateful create/close used 2 / 4 / 4. This supports the operation sequence
as the principal optimization target. Sequential phases and observed latency
variation prevent assigning an exact cost to the Python binding by subtraction.

### Functional checks, failures, and limits

Public installed-wheel checks passed on all six endpoint/protocol combinations
for nonempty-file `wb` truncation, binary payload readback, and append preserving
contents. The supplemental checks also verified that existing-file mode `0640`
survives `wb`, and exercised `w+b` read/write on the first five combinations.

Adding the `w+b` read/write check before the timed workload on B/v4.1 twice
led to `NFS4ERR_RETRY_UNCACHED_REP` during a subsequent parent lookup. Packet
decoding located status 10068 at PUTFH after successful SEQUENCE; cleanup
LAYOUTRETURN requests also received that status, and client close reported an
error. Both runs failed and were cleaned up. Their B/v4.1 timed measurements
are not reported as successes. Removing that additional `w+b` exercise allowed
the ordinary create workload and Rust controls to complete. This is a separate
observed state/replay failure with unresolved cause; it does not establish
that `w+b` itself caused it, or that #109 explains it.

An initial kernel v3 mount with `noresvport` was rejected. The main pass used
`resvport`, which succeeded; initial partial measurements were excluded. Every
run-owned directory, including failed runs, was independently checked absent
after cleanup, and no investigation mount remained.

At the research snapshot, no optimization was implemented or benchmarked.
Creation attributes on this server, concurrent-create races, OPEN_CONFIRM, cancellation/lost-reply cleanup,
and truncation with outstanding pNFS writes remain validation requirements for
a fix. The standard Rust gate and full Python unit suite were not run for this
documentation-only change. Source/RFC review, native release builds, physical
probes, packet attribution, and `git diff --check` were performed.

## Implementation follow-up: 2026-11-03

The working-tree implementation based on the snapshot above adds
`OpenOptions` and `Mount::open_path_with_options`. Python supplies access,
creation permissions, and truncate intent through this shared API. Built-in
engines resolve the parent once, open the target, and use guarded creation on
NOENT. Only confirmed fresh creation skips size-zero SETATTR. EXIST from a
competing creator returns to open/truncate without changing that file's mode.
The existing-file truncate still uses the engine's ordinary SETATTR path,
including its pNFS write/layout ordering. General Rust `create_path` semantics
are unchanged, and external `Mount` implementations receive a conservative
default that retains the truncate after unchecked creation.

NFSv4 guarded OPEN includes creation permissions. A definite OPEN
ATTRNOTSUPP rejection retries guarded creation without attributes, then applies
mode through SETATTR. Partial OPEN success cannot select that fallback or
reissue creation because a later GETFH/GETATTR failed. When the handle and
state are available, failed follow-up work attempts CLOSE; a missing handle
remains an explicit uncertain outcome rather than a blind create retry.

### Deterministic validation

- `cargo fmt --all -- --check` and `cargo clippy --all-targets -- -D warnings`
  passed.
- `cargo test --all-targets --no-fail-fast`: 730 passed, 30 physical-lab tests
  ignored by default. All nine open-options regression tests passed. These
  exercise encoded GUARDED creation, one parent resolution, fresh-create RPC
  sequences, competing creation, permission preservation, append without
  truncate, rejected creation attributes, invalid access, failure cleanup,
  v4.0 OPEN_CONFIRM and lost replies, partial OPEN results, and unsolicited
  READ/WRITE delegations with successful or failed GETATTR.
- `cargo test --doc`: 48 passed, including the new stateful-open example.
- `cargo test --locked --features python-bindings --lib`: 699 passed.
- The concrete reliability mapping checker passed; its existing partial and
  unmapped coverage remains explicit rather than being counted as new evidence.
- A newly built `python-test-support` wheel was installed in a dedicated
  virtual environment. With `NFS_RS_TEST_INSTALLED=1`, the Python suite reported
  398 passed and one skipped module requiring `NFS_RS_PYTHON_REAL_URL`.
  `mypy.stubtest` passed for all four modules.

The v4.0 fixture initially waited forever after returning a GETFH failure;
it now finishes after that scripted response and bounds the server join. The
new v4.1 test module also carries a file-level `cfg(test)` marker so the
production panic scan recognizes its scope. Both failures were reproduced
before their fixes and the full Rust gate passed afterward.

Standards and spec review independently found that the guarded-create path
decoded delegation from the start of OPEN data instead of the cursor after
its stateid. The new delegation regression failed before the fix. The parser
now uses one advancing buffer, and all four READ/WRITE delegation and
GETATTR-success/failure combinations enqueue the exact delegation stateid and
file handle. Both review axes passed after the correction.

### Supplemental physical comparison

A separate ordinary release wheel, without `python-test-support`, was built
from the reviewed working tree after the delegation fix and installed in
another dedicated environment. The
same two FAS2750 LIFs, export, URL settings, and Python timing script from the
main research pass were used. Each case again ran 205 operations and retained
200 after discarding five warmups. The shared test/performance locks were held
in acquisition order; `prepare-run.sh` and unconditional `cleanup-run.sh`
managed a unique allowed directory. Cleanup completed successfully. Packet
capture reported zero kernel drops. Raw scripts, packets, and machine-local
evidence remain outside the checkout.

Times below are milliseconds. Before/after total columns show p50 / p95;
open and close columns show p50. Totals measure Python `open("wb")` and close
on new files. The before values are the main-pass measurements above.

| LIF | Protocol | Before total | After open | After close | After total |
| --- | --- | --- | --- | --- | --- |
| A | 3 | 2.229 / 5.027 | 1.500 | 0.063 | 1.564 / 2.062 |
| A | 4.0 | 4.544 / 7.826 | 1.930 | 0.508 | 2.439 / 4.798 |
| A | 4.1 | 3.967 / 7.764 | 1.966 | 0.508 | 2.476 / 6.994 |
| B | 3 | 1.721 / 2.783 | 1.158 | 0.054 | 1.218 / 2.911 |
| B | 4.0 | 3.023 / 6.207 | 1.390 | 0.361 | 1.754 / 3.900 |
| B | 4.1 | 3.374 / 5.170 | 1.404 | 0.367 | 1.781 / 3.826 |

The p50 totals were 29.2–47.2% lower. These sequential passes include ordinary
lab variation; B/v3 p95 did not improve. They are diagnostic comparisons,
not the cross-environment release acceptance gate, and no accepted baseline
or threshold was changed.

Packet attribution confirmed the following counts on both LIFs, for all 200
retained samples of each case:

| Python operation including close | v3 before → after | v4.0 before → after | v4.1 before → after |
| --- | --- | --- | --- |
| New `wb` | 5 → 3 | 7 → 4 | 7 → 4 |
| New `ab` | 5 → 4 | 7 (one B sample: 8) → 5 | 7 → 5 |
| Existing `wb` | 3 → 3 | 4 → 4 | 4 → 4 |

One unrelated kernel-client SEQUENCE renewal fell inside a B/v3 append timing
window. After-count attribution excludes that independent TCP stream; the
four requests on the operation's own connection are all included. Timing
samples were not removed or changed.

New `wb` now sends parent lookup, target lookup/OPEN returning NOENT, guarded
CREATE/OPEN, and NFSv4 CLOSE. No separate mode or size SETATTR was observed
on that fresh-create path. Append retains its explicit EOF GETATTR. Existing
`wb` retains size SETATTR and its original RPC count.

All six LIF/protocol combinations passed physical sync and async file-mode
checks: new `w+b` read/write, existing-file truncation, append preserving data,
mode `0640` preserved on existing files, and full payload readback. Sync `rb`
and `r+b` also rejected missing files without creating them. These checks
used separate clients after each timed workload. B/v4.1 passed this run; that
does not resolve the earlier RETRY_UNCACHED_REP observation or its cause.

Physical concurrent-create races, OPEN_CONFIRM, lost replies/cancellation,
and truncation with outstanding pNFS writes were not fault-injected in this
follow-up. NFSv4.2 and other physical environments were not exercised. The
manifest-based cross-environment release performance gate remains required
before release acceptance; these FAS measurements do not substitute for it.
