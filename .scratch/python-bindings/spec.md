# Python bindings for nfs-rs

Status: ready-for-agent

## Problem Statement

Python users cannot currently use `nfs-rs` through an idiomatic, supported
Python API. They would have to understand the asynchronous Rust `Mount`
interface, protocol procedures, Tokio, opaque file handles, and differences
between NFSv3, exact NFSv4.0, and NFSv4.1.

Users need a userspace NFS client that behaves like a Python filesystem API:
connected clients, open files, familiar exceptions, synchronous and asyncio
entry points, deterministic cleanup, safe cancellation, and installable Linux
artifacts. The API must retain NFS operation-outcome truth—especially uncertain
mutations—without exposing protocol state or encouraging unsafe retries.

## Solution

Ship a version-coupled Python distribution backed by the protocol-neutral
`nfs-rs` core. Its stable pure-Python facade exposes synchronous `Client` and
`File` objects alongside equivalent `AsyncClient` and `AsyncFile` objects. A
private PyO3 Adapter translates between Python values and a shared Rust
`ClientCore` that owns the connected client lifecycle, open-resource registry,
in-flight work, recovery events, and protocol engine.

The facade presents POSIX export-relative paths, Python binary-file semantics,
immutable metadata and diagnostics, and Python-native filesystem exceptions
augmented with structured protocol, outcome, and recovery information. Explicit
close and context managers provide deterministic cleanup. Cancellation never
pretends that a sent modifying operation was cancelled remotely.

The first release targets GIL-enabled CPython 3.10+ with an `abi3` Linux x86_64
wheel plus a tested source distribution. It validates public
behavior primarily at the installed facade seam, uses one private deterministic
injection seam for otherwise-unreproducible protocol faults, and runs packaged
artifacts against the existing real-server and performance labs.

## User Stories

1. As a Python developer, I want to install a binary wheel without installing
   libnfs or mounting a kernel filesystem, so that I can access NFS directly
   from my application.
2. As a synchronous application developer, I want to connect with
   `Client.connect`, so that I can use NFS without managing an event loop.
3. As an asyncio application developer, I want to connect with
   `AsyncClient.connect`, so that network I/O does not block my event loop.
4. As a Python developer, I want the sync and async clients to expose matching
   operations and errors, so that I do not learn two filesystem models.
5. As a user connecting to mixed infrastructure, I want to provide an ordered
   NFS version preference, so that negotiation follows my deployment policy.
6. As an NFSv4.0 user, I want exact `4.0` selection, so that the client does not
   silently substitute another NFSv4 protocol.
7. As an AUTH_SYS user, I want to configure numeric UID and GID, so that server
   access checks use the intended identity.
8. As an infrastructure operator, I want to configure service ports, transfer
   sizes, directory page sizes, and reserved-port behavior, so that the client
   works with my network and server policy.
9. As a caller using URL configuration, I want explicit keywords to override
   matching query values, so that configuration precedence is predictable.
10. As a caller, I want invalid or unknown connection options rejected before
    network work, so that configuration errors are immediate.
11. As a Python developer, I want paths to use POSIX export-relative semantics
    on every host, so that behavior does not change with the local OS.
12. As a security-conscious caller, I want NUL and export-root traversal
    rejected locally, so that paths cannot escape the connected export.
13. As a caller, I want familiar operations such as stat, scandir, mkdir,
    remove, rename, link, symlink, xattrs, chmod, chown, utime, truncate, and
    access, so that I can build filesystem workflows without raw NFS calls.
14. As a caller listing large directories, I want streaming iteration, so that
    directory size does not force full materialization.
15. As a caller, I want directory entries to contain their available metadata
    without hidden stat calls, so that I can reason about network traffic.
16. As a caller, I want `exists` to suppress only not-found errors, so that
    permission and transport failures remain visible.
17. As a caller creating parent directories, I want non-transactional behavior
    documented, so that I understand what may remain after partial failure.
18. As a caller, I want convenient list, touch, read-all, and write-all helpers
    built on stable primitives, so that common tasks stay concise.
19. As a Python developer, I want binary modes matching normal Python file
    meanings, so that create, truncate, append, read, and update behavior is
    unsurprising.
20. As a synchronous caller, I want files to implement `io.RawIOBase`, so that
    I can compose standard buffering and text encoding.
21. As an asyncio caller, I want an async file context manager, so that file
    cleanup fits ordinary async control flow.
22. As a file user, I want read, readinto, write, seek, tell, truncate, flush,
    and close, so that the object supports normal streaming workflows.
23. As a concurrent I/O user, I want positional read and write methods that do
    not change logical position, so that independent ranges can overlap safely.
24. As a caller sharing one open file, I want relative operations serialized,
    so that position changes have a deterministic order.
25. As an append-mode caller, I want every ordinary write to target the
    then-current EOF for that file object, so that local append behavior is
    consistent.
26. As a caller, I want successful high-level writes to consume the complete
    input despite server partial writes, so that return values follow Python
    expectations.
27. As a caller, I want uncertain completed bytes distinguished from confirmed
    bytes, so that I do not continue from an invented position.
28. As a durability-sensitive caller, I want flush to commit dirty data and
    preserve dirty state after uncertain commit results, so that durability is
    never falsely reported.
29. As a caller, I want close to finish all possible cleanup even when flush
    fails, so that one failure does not leak the remaining resource state.
30. As a caller, I want repeated close to reuse the same terminal result without
    new network work, so that close is deterministic and safe to call again.
31. As a context-manager user, I want my original exception preserved when
    cleanup also fails, so that the primary failure is not hidden.
32. As a client owner, I want client close to reject new work, drain files,
    finish in-flight outcomes, and attempt unmount, so that shutdown is ordered.
33. As an asyncio caller, I want cancelled close waits to leave core cleanup
    running, so that cancellation does not orphan resources.
34. As a caller, I want an open file preserved only when the core can prove its
    identity and state survived recovery, so that path reuse cannot target a
    different object.
35. As a caller with lost open state, I want explicit reopen or remount
    guidance, so that recovery is safe instead of transparent guesswork.
36. As a caller, I want familiar built-in filesystem exceptions, so that normal
    Python exception handling continues to work.
37. As an NFS-aware caller, I want every exception to preserve structured
    protocol code, operation, outcome, and recovery action, so that I never
    parse error strings.
38. As a caller handling a sent mutation with no authoritative reply, I want an
    uncertain-outcome exception, so that I verify instead of blindly retrying.
39. As an asyncio caller, I want cancellation to remain
    `asyncio.CancelledError`, so that standard cancellation control flow works.
40. As an operator, I want bounded recovery events for abandoned operations
    whose outcomes arrive later, so that important uncertainty is observable.
41. As an operator, I want queue overflow exposed through a dropped-event count,
    so that diagnostics cannot disappear silently.
42. As a buffer user, I want write bytes snapshotted before detached or async
    execution, so that later mutation cannot alter transmitted data.
43. As a readinto user, I want the target revalidated before copy-back, so that
    concurrent resize becomes a deterministic error rather than memory
    unsafety.
44. As a large-transfer user, I want hidden memory bounded by the payload and
    negotiated transfer chunk, so that copying remains predictable.
45. As a multithreaded caller, I want to share one synchronous client while
    blocking work releases the GIL, so that other Python threads make progress.
46. As an asyncio caller, I want a client bound to its creating loop and
    cross-loop use rejected, so that runtime mistakes fail deterministically.
47. As an operator, I want immutable capability, health, I/O-limit, callback,
    filesystem, and metadata values, so that diagnostics are safe snapshots.
48. As a privacy-conscious operator, I want representations and errors redacted,
    so that credentials and protocol identifiers do not leak into logs.
49. As a typing user, I want complete public stubs and `py.typed`, so that static
    checking reflects the runtime API.
50. As a release consumer, I want Rust and Python artifacts to share one exact
    version, so that compatibility is unambiguous.
51. As a minimum-version user, I want the installed artifact tested on CPython
    3.10, so that the declared floor is evidence-based.
52. As an x86_64 user, I want a tested manylinux wheel, so that installation
    does not depend on a local Rust toolchain.
53. As a source-build user, I want a complete and tested source distribution,
    so that supported Linux/glibc builds are reproducible from the tarball.
54. As a release consumer, I want the exact tested bytes published with checksums
    and provenance, so that publication cannot invalidate verification.
55. As a maintainer, I want deterministic fault injection and real-server
    coverage, so that cancellation and uncertain outcomes are release gates.
56. As a maintainer, I want performance, memory, GIL, and event-loop baselines,
    so that later changes cannot silently degrade the Python binding.

## Implementation Decisions

- The product has four layers: protocol-neutral Rust core, private PyO3
  Adapter, stable pure-Python facade, and a future optional `fsspec` adapter.
- The private extension is named `_internal`; raw Adapter objects, converters,
  and private stubs are not compatibility surfaces. The facade owns public
  names, multiple-inheritance exceptions, standard-library composition, and
  typing metadata.
- Synchronous and asynchronous Interfaces are explicit twins. `Client`/`File`
  block; `AsyncClient`/`AsyncFile` preserve the same arguments, values, errors,
  capabilities, lifecycle, and outcome semantics while awaiting operations
  that may block.
- Connected clients are created only through class-level `connect` factories.
  The first release has no half-connected constructor and no separate options
  object.
- Connection options cover ordered versions, AUTH_SYS identity, service ports,
  directory and transfer sizing, reserved-port behavior, delegation retention,
  connection and operation deadlines, and recovery-event capacity. Explicit
  keywords override URL query values. Omitted options retain URL or Rust-core
  defaults and do not add Adapter retries.
- Paths accept strings and path-like strings, never bytes. They use normalized
  POSIX export-relative semantics on every host. Root escape and NUL are
  rejected; symlink target text retains caller meaning.
- The public client Interface contains Python filesystem primitives and
  protocol-neutral diagnostics rather than mirroring `Mount` procedures.
  Export discovery remains a module operation outside connected clients.
- Synchronous files implement `io.RawIOBase`; buffering and text encoding use
  the Python standard library. Async files remain separate async objects.
- Supported modes are binary read, write, append, and their update variants.
  Open may consist of multiple protocol operations and is explicitly
  non-transactional.
- Each open file owns one logical position. Relative operations serialize;
  positional operations do not change position and may overlap. Append is
  serialized within one file object but is not atomic across clients.
- High-level writes hide server partial writes and return only after the entire
  buffer succeeds. Confirmed preceding chunks and an uncertain current chunk
  remain distinguishable.
- An uncertain mutation that leaves the next relative offset unknowable puts
  the file in uncertain-file-position state. Absolute repositioning can reset
  local position but does not claim remote verification.
- Dirty ranges clear only after authoritative commit. File close always reaches
  a closed terminal state, continues cleanup after individual failures, and
  stores an immutable terminal report reused by later closes.
- Client close is one shared transition that rejects new work, drains registered
  files, settles in-flight outcomes, attempts unmount, and produces one terminal
  result. Cancelling an async waiter does not cancel core-owned cleanup.
- A synchronous client owns one bounded multi-thread Tokio runtime and releases
  the GIL around blocking calls. Async clients use the process-global PyO3
  Tokio bridge. Both hold a shared Rust core abstraction containing lifecycle,
  protocol engine, resource registry, continuing work, and recovery events.
- An async client belongs to its creating asyncio loop. A sync client may be
  shared across Python threads. Python finalizers never block; explicit close
  and context managers are the correctness mechanisms.
- The Adapter never transparently reopens by path. An existing file survives
  only recovery that proves the same identity and open state; otherwise it
  reports lost open state and remains closable.
- One centralized conversion seam maps every Rust error to a Python-semantic
  class while retaining immutable operation, protocol, status, outcome,
  completed-byte, and recovery fields. Common classes also inherit matching
  Python built-ins.
- The Rust core is authoritative for whether work was sent and whether replay
  is safe. The Adapter never derives this from strings or method names and
  never automatically retries an error that has crossed into Python.
- Recovery actions are stable values for retry, reopen, remount, verify then
  resume, and do not retry. A sent modifying operation that cannot be fully
  classified is conservatively uncertain.
- Python cancellation remains native. Core-owned sent mutations continue until
  their result is authoritative or uncertain. Later uncertainty or state loss
  enters a bounded, redacted recovery-event queue with visible overflow.
- No borrowed Python buffer or pointer crosses GIL detachment or async
  suspension. Writes use one Rust-owned snapshot; reads use Rust-owned network
  data; readinto copies back while attached after revalidating the target.
- Immutable, slotted, protocol-neutral values represent file and filesystem
  information, directory entries, capabilities, health, I/O limits, callbacks,
  exports, and recovery events. They never expose raw handles or session state.
- The first release is a Maturin mixed package for GIL-enabled CPython 3.10+
  using `abi3-py310`. It publishes an audited manylinux2014 x86_64 wheel
  and a tested Linux/glibc source distribution with no mandatory Python runtime
  dependencies.
- The preferred distribution name is `nfs-rs` and the stable import is
  `nfs_rs`. If the distribution name cannot be reserved, the predetermined
  fallback is `nfs-rs-client`; the import name does not change.
- Rust crate, Python distribution, manifests, changelog, and git tag share one
  exact version. Release automation builds artifacts once, verifies those exact
  immutable bytes, and publishes through protected trusted publishing.
- Free-threaded CPython, broader platforms, and zero-copy buffers require later
  explicit compatibility or safety decisions. A zero-copy fast path additionally
  requires material end-to-end evidence without semantic regression.

## Testing Decisions

- Tests assert observable public behavior rather than PyO3 layout, Tokio task
  identity, raw handles, RPC encodings, or facade implementation details.
- The primary and highest test seam is the installed public `nfs_rs` facade.
  One parameterized contract suite drives sync and async clients through the
  same operations and asserts values, exceptions, lifecycle, cancellation,
  capabilities, diagnostics, buffers, and resource cleanup.
- A single private injection seam behind the Adapter accepts a deterministic
  fake core/protocol implementation. It exists only for conditions that cannot
  be induced reliably through an ordinary server, such as precise before-send,
  after-send, cancellation, partial-write, commit-verifier, and state-loss
  barriers.
- Existing scripted RPC harness patterns are reused below the injection seam
  for wire and protocol-engine behavior. Existing real-server lab patterns are
  reused above it for NFSv3, exact NFSv4.0, NFSv4.1, callbacks, recovery, and
  pNFS interoperability.
- Every public method has success and failure coverage. Every public exception,
  built-in inheritance relationship, stable field, enum, operation outcome,
  and recovery action is exercised on supported boundary Python versions.
- Fault tests explicitly distinguish failure before send from failure after
  send but before authoritative response for write, create, truncate, rename,
  remove, mkdir, link, symlink, commit, and open. Sleeps are not accepted as
  synchronization; deterministic barriers are required.
- Cancellation tests cover open, read, write, flush, file close, and client
  close. Tests prove cleanup ownership, uncertain outcomes, poisoned file
  position, recovery-event behavior, and the absence of blind retries.
- Lifecycle tests cover concurrent and repeated close, failed flush and cleanup,
  context-manager exception precedence, retained files after client close,
  finalizer warnings, stale identity, and lost open state.
- Buffer tests mutate, resize, release, and cancel around sync/async read,
  readinto, write, and positional calls. They prove snapshot timing, safe
  copy-back, and hidden-memory bounds.
- Threading tests prove that blocking calls release the GIL, allowed operations
  overlap, and same-file relative operations serialize. Async tests prove
  event-loop responsiveness, same-loop concurrency, and deterministic
  cross-loop rejection.
- Stress coverage includes 32 synchronous threads, 128 async tasks, multiple
  clients/files, same-file positional I/O, and close races. Hangs, deadlocks,
  timeouts, leaks, or leftover tasks/sockets/registry entries are hard failures.
- The real-server release matrix runs common public scenarios against existing
  NFSv3 Linux/source/destination exports, the exact NFSv4.0 reference baseline,
  NFSv4.1 exports, and NetApp pNFS through the protocol-neutral Interface.
  Required unavailable servers fail closed rather than skip.
- Packaged integration tests install and exercise the final wheel and source
  distribution, not a development-tree extension. x86_64 runs the full matrix.
- Existing performance-baseline conventions are reused by the authoritative
  storage performance gate. Packaged Python tests still run five comparable
  runs with at least four valid samples; Python throughput and latency drift is
  retained as diagnostic evidence without creating a second release gate.
- Memory tests enforce one input snapshot plus a bounded transfer chunk for
  writes and at most one negotiated read chunk for readinto. Repeated lifecycle
  tests require RSS to plateau rather than grow linearly.
- Correctness tests are not automatically retried, timeouts are not enlarged to
  mask failures, and quarantining a flaky required test does not make a release
  pass. Failure artifacts retain sanitized reproduction context.
- Minimum and newest supported CPython versions run the full suite; intermediate
  versions install/import and run protocol smoke tests. Dynamic dependency,
  typing, lint, Rust quality, artifact integrity, checksum, and provenance
  checks are release gates.

## Out of Scope

- Kerberos and RPCSEC_GSS authentication.
- Public raw file handles, stateids, sessions, channels, XDR/RPC values, raw
  attributes, and low-level NFS procedures.
- ACL and byte-range lock Interfaces.
- Non-UTF-8 path names.
- Recursive deletion and `pathconf`.
- Implementing the optional `fsspec` adapter.
- Cross-client atomic append or transactional rollback of multi-step operations.
- Transparent reopen by path after identity or state loss.
- Python 3.9 and older, PyPy, GraalPy, free-threaded CPython, and `abi3t`.
- First-release musllinux, macOS, and Windows wheel commitments.
- Mandatory third-party Python runtime dependencies.
- Offline source-build guarantees or vendoring the Cargo registry.
- Publishing the private binding crate independently to crates.io.
- A first-release zero-copy Python-buffer fast path.

## Further Notes

- This spec synthesizes the accepted Wayfinder decisions for runtime ownership,
  open-file lifecycle, errors and recovery, public Interface, distribution,
  buffer ownership, and release verification. Those decisions are closed; this
  document is ready for implementation-ticket decomposition rather than another
  product-design interview.
- The public vocabulary is connected client, open file, uncertain outcome,
  uncertain file position, lost open state, and recovery event. Implementations
  and documentation should use these terms consistently.
- Exact NFSv4.0 means the RFC-defined `4.0` engine. NFSv4.0 remains explicitly
  experimental in first-release Python metadata and documentation.
- PyPI availability for the preferred distribution name is a release-preparation
  check with a predetermined fallback, not an open design question.
- Documentation must lead with sync and async examples, explain that this is a
  userspace protocol client, describe reserved-port requirements, recommend
  chunked I/O for large transfers, and state the absence of Kerberos/RPCSEC_GSS.
