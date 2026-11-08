# nfs-rs

[![CI](https://github.com/JayTsu-sh/nfs-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/JayTsu-sh/nfs-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/nfs-rs.svg)](https://crates.io/crates/nfs-rs)
[![docs.rs](https://docs.rs/nfs-rs/badge.svg)](https://docs.rs/nfs-rs)
[![license](https://img.shields.io/crates/l/nfs-rs.svg)](LICENSE)

An asynchronous, pure Rust client library for NFSv3, NFSv4.0, NFSv4.1, and NFSv4.2.

`nfs-rs` implements the NFS client protocol without linking to a C NFS
implementation. It is intended for applications that need to access NFS
exports directly from Rust, including services that cannot rely on a
kernel-mounted filesystem.

## Status

- NFSv3 client operations are supported.
- NFSv4.0 (experimental) is supported through the common `Mount` API using an
  AUTH_SYS interoperability profile. RPCSEC_GSS/Kerberos is not implemented,
  so this release does not claim unconditional RFC 7530 conformance.
- NFSv4.1 client operations are supported.
- NFSv4.2 runs the existing file APIs over minor version 2, including sessions,
  locks, callbacks, and negotiated pNFS file layouts. Optional synchronous
  intra-server COPY, CLONE, SEEK, ALLOCATE/DEALLOCATE, READ_PLUS and RFC 8276
  xattrs are exposed; server support is required and rejection is explicit.
- The library uses Tokio and communicates with the server over TCP.
- Linux is exercised by CI and by the physical NFS integration lab.

NFSv4.2 support is available in 0.10.0. Select it explicitly with
`version=4.2`; this does not silently downgrade to another minor version.
AFX validation with the related server features enabled confirmed ordinary
file APIs, ACLs, RFC 8276 xattrs, ALLOCATE, SEEK and whole-file DEALLOCATE.
The tested AFX export rejects COPY, CLONE and READ_PLUS as unsupported;
partial DEALLOCATE returns a server IO error. Optional operations therefore
still require support from the selected server. See the
[AFX validation report](https://github.com/JayTsu-sh/nfs-rs/blob/v0.10.0/docs/validation/nfsv42-afx-enabled-2026-11-02.md).

The public API is still evolving while the crate is below version 1.0.

## Installation

```toml
[dependencies]
nfs-rs = "0.10.1"
bytes = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The minimum supported Rust version is 1.95.

Export URL path segments are percent-decoded once before mounting. For example,
`/测试`, `/%E6%B5%8B%E8%AF%95`, `/a b`, and `/a%20b` use the corresponding
server-side names. Encode a literal `%` as `%25`; encoded separators and invalid
UTF-8 are rejected. To name a literal `%20` in an export, write `%2520` in
the URL.

For Python on Linux x86_64:

```console
python -m pip install nfs-rs
```

See the [Python API guide](docs/python-api.md) for synchronous and asyncio
examples, typing, large-transfer guidance, cancellation and uncertain outcomes,
and the precise first-release support boundaries.

## Example

```rust,no_run
use bytes::Bytes;
use nfs_rs::{OPEN_READ, Result, parse_url_and_mount};

#[tokio::main]
async fn main() -> Result<()> {
    let mount = parse_url_and_mount(
        "nfs://127.0.0.1/some/export?version=4.1&noresvport=true",
    )
    .await?;

    let created = mount.create_path("hello.txt", Some(0o644)).await?;
    nfs_rs::write_all(&*mount, created.fh.clone(), 0, Bytes::from_static(b"hello NFS"))
        .await?;
    mount.close(created.fh).await?;

    let opened = mount.open_path("hello.txt", OPEN_READ).await?;
    let contents = mount.read(opened.fh.clone(), 0, 9).await?;
    mount.close(opened.fh).await?;
    assert_eq!(&contents[..], b"hello NFS");

    mount.umount().await?;
    Ok(())
}
```

## URL format

```text
nfs://<server|ipv4|ipv6>[:<port>]/path[?arg=value[&arg=value]*]
```

Supported arguments:

- `uid=<integer>` — UID sent to the server. It defaults to the process UID on
  Unix and 65534 on Windows.
- `gid=<integer>` — GID sent to the server. It defaults to the process GID on
  Unix and 65534 on Windows.
- `version=<3|4.0|4.1|4.2>` — preferred protocol version or a comma-separated
  preference list such as `4.1,4.0,3`. The default is `3`. The exact `4.0`
  selector selects the experimental NFSv4.0 engine; ambiguous `4` is rejected.
  Exact `version=4.2` requires v4.2; `version=4.2,4.1` explicitly allows fallback.
  pNFS data servers negotiate their version independently (v4.2 or v4.1).
- `retain-delegations=true` — opt into automatic delegation retention. It is
  disabled by default; NFSv4.0 publishes a separately reachable callback
  listener when enabled.
- `nfsport=<port>` — NFS service port. This bypasses portmapper discovery.
- `mountport=<port>` — MOUNT protocol port for NFSv3.
- `readdir-buffer=<count>` or `<dircount>,<maxcount>` — response buffer limits
  for directory reads. Without it, both come from the server's published read
  limit at mount time: NFSv3 FSINFO `rtmax`, NFSv4.0 `maxread`, NFSv4.1
  `maxread` bounded by the session's maximum response size, all bounded by the
  client payload ceiling (4 MiB); an NFSv4.0 server without `maxread` keeps 8192. A
  server may still answer with smaller replies. All three implemented versions
  honor an explicit value; v4.1 further bounds the reply by session capacity.
- `noresvport=<true|false>` — use an ephemeral source port when true. It
  defaults to false.

Read and write RPC sizes are determined automatically during mount from server limits, bounded by the client payload ceiling (4 MiB) and negotiated NFSv4.1 session capacities. URL and Python `rsize`/`wsize` options are no longer accepted. Python `io_limits` reports the effective mount limits; pNFS data servers may require smaller chunks.

`BufferedFile` provides bounded concurrent reads of the requested range and per-call durable writes. `write_all`
and `BufferedFile::write_at` send at most 8 concurrent UNSTABLE chunks sized by
negotiated `max_write`, complete short writes, then finish a batch commit
before returning. There is no writeback mode or automatic byte threshold.
pNFS READ also reuses a single owned data-server response. Short replies and
multiple stripes are assembled only when contiguous output needs joining;
returned slices can keep their complete RPC backing alive until dropped.

`BufferedFile::read_at` returns a single owned response without copying its
payload; it allocates a contiguous output buffer only when joining multiple
response pieces. Returned bytes remain valid after the file is closed.
`Mount::write` is the low-level UNSTABLE primitive. Its `WriteOutcome` exposes
`count`, `committed: WriteCommitted` (`Unstable`, `DataSync`, or `FileSync`),
and `verifier`. `committed` is the server's response, not the requested level;
pNFS reports the weakest level across the contributing DS replies. Retain its acknowledgement
for `Mount::commit_write_batch`, which also routes pNFS commits and completes
required LAYOUTCOMMITs.

NFSv4.1 pNFS reuses valid layouts without a periodic large-write barrier.
Uncovered ranges fetch new layouts; device-change callbacks invalidate address
mappings. When every DS write is explicitly rejected with `NFS4ERR_STALE`, the
client refreshes the layout/device mapping and retries once before safe MDS
fallback. Partial writes and transport-uncertain outcomes are never replayed
through this recovery path.

Single-file pNFS striping follows the server's stripe table, independently of
the number of filehandles or physical DSs. Sparse layouts support OPEN, shared,
and per-DS filehandles; dense layouts use per-stripe filehandles. Requests that
cross stripe boundaries can issue data RPCs to multiple DSs concurrently;
requests within one stripe remain on its assigned DS.

Each Rust `BufferedFile` owns one OPEN reference; separately constructed wrappers
need separate opens even when their file handles are equal. Successful `close`
is idempotent and subsequent reads, writes and flushes return `ClosedResource`.
A failed or cancelled close keeps the wrapper unusable; release remaining protocol
state with `Mount::umount` before discarding the mount.

The opt-in [memory regression benchmark](docs/benchmarks/memory-regression.md)
separates live Rust allocations from RSS and uses the same shared payload pattern
for every protocol. Its physical diagnostics supplement the existing release
performance gate.

RPC deadlines include waiting for readiness, the writer lock, socket transmission
and the reply. Dropping a Rust future during a partial send shuts down that TCP
connection; later retryable calls can reconnect. Cancellation still does not undo
remote mutations. NFSv4.1 owns an admitted `Mount::write` until the result is
claimed or settled: dropping its caller does not cancel the sibling DS writes.
An unclaimed successful write is committed; an unclaimed error remains attached
to that file independently of layout-cache or session changes. Later WRITE and
COMMIT calls report `Uncertain/VerifyThenResume`. CLOSE waits for these writes,
attempts to release its OPEN reference, and preserves the uncertainty. UMOUNT
stops admission, drains admitted operations, and attempts all protocol cleanup
even when an earlier phase fails; its result still reports those errors. DS
session/client-id destruction remains best-effort. Directory page requests also
drain independently of a cancelled iterator wait, without blocking shutdown on
an idle retained iterator. Verify
the remote file and use a fresh mount before resuming an uncertain write.

Other raw Rust modifying futures still require caller settlement or remote
verification after cancellation. Python owns and settles admitted operations as
described in its API guide. Directory scans report non-EOF pages that make no progress as
errors instead of silently reporting a complete listing.

Python `readinto(buffer)` and `readinto_at(buffer, offset)` fill a caller-owned
writable contiguous buffer using up to 8 concurrent, negotiated-size reads.
Short responses are continued until the buffer is full or EOF is reached.
They return the number of bytes filled and leave the tail beyond EOF untouched.
There is no read-ahead cache or `readahead` option. Each RPC payload is copied
once into the target, without an intermediate Python `bytes` object.

When `noresvport=false`, the client binds below port 1024 for servers enforcing
the RFC 1813 secure-port convention. This may require elevated privileges.
Setting `noresvport=true` avoids privileged-port exhaustion, but the NFS server
must accept non-privileged source ports (the `insecure` export option on Linux).

### NFSv4.0 migration notes

Use exact `version=4.0`; the ambiguous selector `version=4` is invalid. Existing
NFSv3 remains the default. NFSv4.0 uses the same public `Mount` methods as v3
and v4.1, but reports session and pNFS capabilities as unavailable. Delegation
retention is optional and automatic; applications never handle raw stateids.
The experimental profile has real FAS2750 reconnect and lease validation, but
dedicated-server restart grace/reclaim evidence remains an explicit exception
until a safe maintenance fixture is available.

### Native POSIX ACLs over NFSv3

The Rust `Mount::get_posix_acl` and `Mount::set_posix_acl` methods support the
optional Sun NFSACL v3 extension (RPC program 100227). They transfer complete
`PosixAcl` access/default entry lists, including raw permissions hidden by a mask.
The `get_posix_acl_path` and `set_posix_acl_path` forms resolve a path first.
Setting an empty default list explicitly clears the destination default ACL.
`MountCapabilities::posix_acl` reports the mount-time extension probe; the existing
`acl` capability continues to describe NFSv4 ACLs.

NFSv3 does not guarantee this extension, and an individual filesystem or object
can still return `Unsupported`. SETACL is sent once; after a failed or uncertain
write, read back the ACL before deciding whether to resume. Native ACL transport
does not perform identity mapping or POSIX/NFSv4 semantic conversion. See the
[native POSIX ACL guide](docs/posix-acl.md) for tags, qualifiers and failure behavior.

## Documentation

The complete API documentation is published on
[docs.rs](https://docs.rs/nfs-rs). See the [`Mount`](https://docs.rs/nfs-rs/latest/nfs_rs/trait.Mount.html)
trait for supported filesystem operations.

## Testing

Run `cargo test --all-targets --no-fail-fast` and `cargo test --doc`.
Normal unit and integration tests run without access to an NFS server. The
ignored physical-lab test exercises NFSv3 and NFSv4.1 against dedicated exports;
its setup is documented in the source repository and is not part of the
published crate.

## License

Licensed under the [Apache License 2.0](LICENSE).

## Contributing

See [CONTRIBUTING.md](https://github.com/JayTsu-sh/nfs-rs/blob/main/CONTRIBUTING.md).

Definite connection loss uses a short default recovery budget: at most four
reconnections within 500 ms of the first disconnect, bounded further by the
request's existing replay policy and deadline. Backoff, TCP setup, session
rebinding and replay share that budget. Brief flaps can recover transparently;
exhaustion returns the first typed disconnect with its transmission and uncertain
outcome evidence. No URL switch is required. A failed mount may need to be
replaced by its owner; application-level scan recovery remains the caller's policy.
Silent packet loss uses the separate operation deadline and remains a timeout.

For NFSv4.1/v4.2, an uncached read-only reply (`NFS4ERR_RETRY_UNCACHED_REP`)
is queried once with a fresh sequence, within the same operation deadline.
This applies to MDS and DS requests. Repeated uncached replies remain typed
errors; modifying requests are never reissued through this recovery path.

Server-busy responses use a fixed bounded retry policy across v3, v4.0, v4.1
and v4.2, including independent pNFS DS reads. Safely reissuable requests have
at most 16 busy reissues within 60 s, using equal-jitter exponential backoff
(200 ms base, 5 s cap). Exhaustion preserves the original protocol status in a
structured ServerBusy error. See [retry behavior](docs/bounded-retries.md) for
stateful-operation safety, deadlines and application recovery.
