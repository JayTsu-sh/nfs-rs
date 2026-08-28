# Python API

`nfs-rs` provides typed synchronous and `asyncio` clients for direct userspace
NFS access. The first Python release supports CPython 3.10 or newer on
Linux/glibc x86_64 and aarch64. It supports NFSv3 and NFSv4.1; exact
`version=4.0` is experimental. NFSv4.2, Kerberos, and RPCSEC_GSS are not
implemented. Authentication uses AUTH_SYS.

## Install and type check

```console
python -m pip install nfs-rs
```

The distribution includes `py.typed` and complete public stubs, so mypy and
other PEP 561 consumers work without a separate stubs package. Wheels have no
mandatory Python runtime dependencies. Building the source distribution needs
Rust 1.95 and a compatible native build toolchain.

## Synchronous workflow

```python
from nfs_rs import Client

with Client.connect("nfs://server/export?version=4.1&noresvport=true") as client:
    client.write_bytes("incoming/message.bin", b"hello")
    with client.open("incoming/message.bin", "rb") as source:
        header = source.read(5)
    assert header == b"hello"
```

Files use binary modes only. Compose them with `io.BufferedReader` or
`io.BufferedWriter` when application-level buffering is useful; do not wrap
them in `TextIOWrapper` unless the application explicitly owns the text
encoding and newline policy. Positional `read_at` and `write_at` do not change
the file position and are preferable for concurrent range I/O.

## Async workflow and cancellation

```python
import asyncio
from nfs_rs import AsyncClient

async def copy() -> None:
    async with await AsyncClient.connect(
        "nfs://server/export?version=3&noresvport=true"
    ) as client:
        data = await client.read_bytes("source.bin")
        await client.write_bytes("destination.bin", data)

asyncio.run(copy())
```

An `AsyncClient` belongs to the event loop that created it. Cancelling an await
cancels the Python waiter, not protocol work already sent to the server. The
client keeps owned cleanup work alive and records a later uncertain result as a
recovery event. Always close clients and files, preferably with `async with`.

## Large transfers and memory

`read_bytes` and `write_bytes` are conveniences for data that comfortably fits
in memory. For large objects, use `open`, then repeatedly call `read`/`write`
with bounded chunks (for example 1–8 MiB). The native client further splits
requests to negotiated server limits. A successful `write` reports bytes
accepted; call `flush` or close the file to request stable storage. Avoid one
unbounded allocation for an entire large object.

## Ports and connection URLs

The default `noresvport=false` binds a privileged source port below 1024, as
required by NFS exports using the secure-port convention. That usually requires
the process or container to have permission to bind privileged ports. Use
`noresvport=true` only when the server permits non-privileged clients. Select
NFSv4.0 exactly with `version=4.0`; ambiguous `version=4` is rejected. A fallback
list such as `version=4.1,3` is explicit and separate from exact-version tests.

## Errors, uncertain outcomes, and recovery

Catch specific `NfsError` subclasses. A modifying operation that fails before
send has a definite outcome and may be retried according to its recovery
guidance. `NfsUncertainOutcomeError` means the request may have reached the
server. Never blindly retry it: first inspect authoritative state (existence,
size, checksum, destination name, or application transaction identity), then
resume, compensate, or ask an operator. `completed_bytes` is authoritative only
for the confirmed prefix reported by the exception.

`client.recovery_events()` returns a non-destructive bounded snapshot;
`client.drain_recovery_events()` atomically consumes current events. Monitor
these events after cancellation and during long-lived mounts. State-loss errors
may require reopening a file or remounting; follow the exception's
`recovery_action` rather than retrying every operation uniformly.

## First-release boundaries

- Linux/glibc x86_64 and aarch64 wheels; CPython 3.10+ through the stable ABI.
- NFSv3 and NFSv4.1 supported; NetApp file-layout pNFS is exercised in the
  release lab when negotiated.
- Exact NFSv4.0 uses an experimental AUTH_SYS interoperability profile.
- No NFSv4.2, Kerberos, RPCSEC_GSS, Windows, macOS, musl, PyPy, or free-threaded
  CPython support is claimed by the first release.
