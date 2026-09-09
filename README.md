# nfs-rs

[![CI](https://github.com/JayTsu-sh/nfs-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/JayTsu-sh/nfs-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/nfs-rs.svg)](https://crates.io/crates/nfs-rs)
[![docs.rs](https://docs.rs/nfs-rs/badge.svg)](https://docs.rs/nfs-rs)
[![license](https://img.shields.io/crates/l/nfs-rs.svg)](LICENSE)

An asynchronous, pure Rust client library for NFSv3, NFSv4.0, and NFSv4.1.

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
- NFSv4.2 may be accepted in a URL preference list but is not implemented.
- The library uses Tokio and communicates with the server over TCP.
- Linux is exercised by CI and by the physical NFS integration lab.

The public API is still evolving while the crate is below version 1.0.

## Installation

```toml
[dependencies]
nfs-rs = "0.8.2"
bytes = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The minimum supported Rust version is 1.95.

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
  Version 4.2 is not currently implemented.
- `retain-delegations=true` — opt into automatic delegation retention. It is
  disabled by default; NFSv4.0 publishes a separately reachable callback
  listener when enabled.
- `nfsport=<port>` — NFS service port. This bypasses portmapper discovery.
- `mountport=<port>` — MOUNT protocol port for NFSv3.
- `readdir-buffer=<count>` or `<dircount>,<maxcount>` — response buffer limits
  for directory reads. Both values default to 8192. All three implemented
  versions honor this option; v4.1 further bounds the reply by session capacity.
- `noresvport=<true|false>` — use an ephemeral source port when true. It
  defaults to false.

Read and write RPC sizes are determined automatically during mount from server limits, bounded by the client payload ceiling (4 MiB) and negotiated NFSv4.1 session capacities. URL and Python `rsize`/`wsize` options are no longer accepted. Python `io_limits` reports the effective mount limits; pNFS data servers may require smaller chunks.

`BufferedFile` provides bounded concurrent reads of the requested range and per-call durable writes. `write_all`
and `BufferedFile::write_at` send at most 8 concurrent UNSTABLE chunks sized by
negotiated `max_write`, complete short writes, then finish a batch commit
before returning. There is no writeback mode or automatic byte threshold.
`Mount::write` is the low-level UNSTABLE primitive. Its `WriteOutcome` exposes
`count`, `committed: WriteCommitted` (`Unstable`, `DataSync`, or `FileSync`),
and `verifier`. `committed` is the server's response, not the requested level;
pNFS reports the weakest level across the contributing DS replies. Retain its acknowledgement
for `Mount::commit_write_batch`, which also routes pNFS commits and completes
required LAYOUTCOMMITs.

Each Rust `BufferedFile` owns one OPEN reference; separately constructed wrappers
need separate opens even when their file handles are equal. Successful `close`
is idempotent and subsequent reads, writes and flushes return `ClosedResource`.
A failed or cancelled close keeps the wrapper unusable; release remaining protocol
state with `Mount::umount` before discarding the mount.

RPC deadlines include waiting for readiness, the writer lock, socket transmission
and the reply. Dropping a Rust future during a partial send shuts down that TCP
connection; later retryable calls can reconnect. Cancellation still does not undo
remote mutations. Settle a modifying future to obtain its outcome, or verify the
remote file before resuming. Python owns and settles admitted operations as described
in its API guide. Directory scans report non-EOF pages that make no progress as
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
