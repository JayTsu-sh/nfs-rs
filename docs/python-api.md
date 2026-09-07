# Python user guide

`nfs-rs` is a userspace NFS client for Python. It talks to NFS servers directly,
so applications do not need a kernel-mounted filesystem. The public package
contains matching synchronous and `asyncio` APIs with PEP 561 type information.

## Support matrix

| Area | Support |
|---|---|
| Python | CPython 3.11+ via the stable ABI |
| Platform | Linux/glibc x86_64 |
| Protocols | NFSv3, NFSv4.1, experimental NFSv4.0 |
| Authentication | AUTH_SYS |
| Not supported | NFSv4.2, Kerberos/RPCSEC_GSS, Windows, macOS, musl, PyPy, free-threaded CPython |

NFSv4.0 uses an experimental interoperability profile. Server capabilities such
as named attributes, ACLs, callbacks, locks, delegation retention, and pNFS are
negotiated and exposed through `client.capabilities`.

These protocol claims are exercised by the release pipeline using the final
x86_64 wheel and a wheel rebuilt from the final source distribution against
real NFSv3, NFSv4.0, NFSv4.1, and NFSv4.1 pNFS environments.

## Installation

```console
python -m pip install nfs-rs
```

The wheel has no mandatory Python runtime dependencies. The source distribution
requires Rust 1.95 and a compatible native build toolchain. The installed
`py.typed` marker and public stubs work with mypy, pyright, and other PEP 561
consumers.

```python
import nfs_rs

print(nfs_rs.__version__)
```

## Connection URLs

```text
nfs://<server>[:port]/<export>[?option=value&option=value]
```

Common options:

| Option | Meaning |
|---|---|
| `version=3` | Use NFSv3 |
| `version=4.0` | Use experimental NFSv4.0 |
| `version=4.1` | Use NFSv4.1 |
| `version=4.1,4.0,3` | Try an explicit fallback order |
| `uid=`, `gid=` | AUTH_SYS numeric identity |
| `nfsport=`, `mountport=` | Override service ports |
| `rsize=`, `wsize=` | Requested I/O sizes |
| `readdir-buffer=` | Directory response limit, or `dircount,maxcount` |
| `noresvport=true` | Use an unprivileged source port |
| `retain-delegations=true` | Retain delegations when supported |
| `readahead=` | READ chunks prefetched ahead of a lone sequential reader (default 8, `0` disables) |
| `writeback=` | UNSTABLE WRITE chunks kept in flight behind a writer, COMMIT on `flush()`/`close()` (default 0) |

With `writeback` enabled, `write()`/`write_at()` return once the data is queued;
durability and deferred write errors surface on `flush()` and `close()`, like a
buffered POSIX file. Leave it at 0 when every write must be acknowledged as
stable before the call returns.

The default `noresvport=false` binds below port 1024 for exports enforcing the
secure-port convention and may require elevated privileges. Use
`noresvport=true` only when the server accepts non-privileged source ports.

If no `version` is present and no `versions` argument is passed, the client
uses NFSv3. This is a default, not automatic negotiation across all versions.

Options can also be passed as keyword arguments. Explicit arguments override
defaults while the URL selects the export and may carry the same connection
policy:

```python
from nfs_rs import Client

client = Client.connect(
    "nfs://server/export?version=4.1",
    uid=1000,
    gid=1000,
    rsize=1024 * 1024,
    wsize=1024 * 1024,
    readahead=8,
    writeback=8,
    connect_timeout=10,
    operation_timeout=30,
    recovery_event_capacity=256,
)
client.close()
```

`versions=["4.1", "4.0", "3"]` provides a programmatic fallback list and
overrides a version already present in the URL. The client tries entries in
order and `client.version` reports the connected protocol. The Python facade
accepts only `"3"`, `"4.0"`, and `"4.1"`; ambiguous `"4"`, NFSv4.2, empty
lists, and unknown values are rejected before connecting.

## Discover exports

`list_exports()` queries the NFSv3 MOUNT service independently of a mounted
client (similar to `showmount -e`):

```python
from nfs_rs import list_exports

for export in list_exports("nfs://server/"):
    print(export.path, export.groups)
```

Use `await list_exports_async(...)` in async code. It is not NFSv4 namespace
discovery; availability depends on the server exposing the MOUNT service.

## Synchronous workflow

Clients and files are context managers. Closing the client closes resources it
owns; prefer a `with` block so cleanup also occurs on exceptions.

```python
from nfs_rs import Client

with Client.connect("nfs://server/export?version=4.1") as client:
    print(client.version)
    print(client.health.lifecycle, client.health.lease_healthy)
    print(client.io_limits.max_read, client.io_limits.max_write)
    print(client.capabilities)
```

### Paths and directories

All paths are relative to the mounted export. `str` and `os.PathLike[str]` are
accepted. Absolute paths, attempts to escape with `..`, embedded NUL bytes, and
byte-string paths are rejected before reaching the server.

```python
from pathlib import Path

from nfs_rs import Client, FileType

with Client.connect("nfs://server/export?version=4.1") as client:
    client.mkdir("jobs/2026/input", parents=True, exist_ok=True, mode=0o750)
    client.touch(Path("jobs/2026/input/ready"), exist_ok=True)

    assert client.exists("jobs/2026/input/ready")
    info = client.stat("jobs/2026/input/ready")
    assert info.type is FileType.FILE
    print(info.size, info.mode, info.uid, info.gid, info.mtime_ns)

    print(client.listdir("jobs/2026/input"))
    for entry in client.scandir("jobs/2026/input"):
        print(entry.name, entry.path, entry.info.type)

    client.rename("jobs/2026/input/ready", "jobs/2026/input/started")
    client.unlink("jobs/2026/input/started", missing_ok=True)
    client.rmdir("jobs/2026/input")
```

`scandir()` streams entries and avoids constructing a complete list. Consume or
close the client before discarding a partially consumed iterator.

Namespace operations include `mkdir`, `touch`, `remove`/`unlink`, `rmdir`,
`rename`, hard `link`, `symlink`, and `readlink`. `remove` and `unlink` are
equivalent file-removal operations.

### Small-file convenience methods

```python
from nfs_rs import Client

with Client.connect("nfs://server/export?version=4.1") as client:
    count = client.write_bytes("message.bin", b"hello")
    assert count == 5
    assert client.read_bytes("message.bin") == b"hello"
```

These helpers hold the complete value in memory. Use the file API for large
objects.

### File objects and large transfers

Supported binary modes are `rb`, `wb`, `ab`, `r+b`, `w+b`, and `a+b`.

```python
from nfs_rs import Client

chunk_size = 1024 * 1024

with Client.connect("nfs://server/export?version=4.1") as client:
    with client.open("large.bin", "w+b") as file:
        file.write(b"header")
        file.seek(1024)
        file.write(b"payload")
        file.write_at(b"NFS", 0)  # positional I/O does not move the cursor
        file.flush()

        file.seek(0)
        first = file.read(chunk_size)
        print(file.tell(), len(first))
```

`File` implements `io.RawIOBase` behavior including `read`, `readinto`, `seek`,
`tell`, `write`, `truncate`, and `flush`. It has no operating-system file
descriptor, so `fileno()` is unsupported. `read_at`/`readinto_at` and `write_at`
perform positional I/O without changing the file position.

For large transfers, loop over bounded buffers (commonly 1–8 MiB). The native
client further splits them to negotiated `client.io_limits`. A successful
`write` reports the accepted byte count; `flush` or clean close requests stable
storage.

### Metadata, filesystem information, and access checks

```python
import os
import time

from nfs_rs import Client

with Client.connect("nfs://server/export?version=4.1") as client:
    client.chmod("data.bin", 0o640)
    client.chown("data.bin", 1000, 1000)
    now = time.time_ns()
    client.utime("data.bin", ns=(now, now))
    client.truncate("data.bin", 4096)

    if client.access("data.bin", os.R_OK | os.W_OK):
        print("readable and writable")

    fs = client.fs_stat()
    limits = client.fs_info()
    print(fs.available_bytes, limits.max_file_size)
```

Pass `-1` to one side of `chown` when that identity should remain unchanged.

### Extended attributes and ACL capability

Extended attributes are optional and server dependent. Check the negotiated
capability before using them:

```python
from nfs_rs import Client

with Client.connect("nfs://server/export?version=4.1") as client:
    if client.capabilities.named_attributes:
        client.setxattr("data.bin", "user.checksum", b"sha256:...")
        value = client.getxattr("data.bin", "user.checksum")
        names = client.listxattr("data.bin")
        client.removexattr("data.bin", "user.checksum")
        print(value, names)

    print("server advertises ACL support:", client.capabilities.acl)
```

The public Python API exposes the negotiated `capabilities.acl` flag but does
not currently expose structured ACL read/write methods. Do not treat generic
xattr calls as a portable ACL encoding.

## Async workflow and cancellation

Every protocol operation has an async twin. `AsyncClient.open()` is awaited and
returns an `AsyncFile`; `scandir()` itself returns an async iterator.

```python
import asyncio

from nfs_rs import AsyncClient


async def process() -> None:
    async with await AsyncClient.connect(
        "nfs://server/export?version=4.1",
        operation_timeout=30,
    ) as client:
        await client.mkdir("results", exist_ok=True)
        await client.write_bytes("results/one.bin", b"one")

        async with await client.open("results/one.bin", "r+b") as file:
            await file.write_at(b"ONE", 0)
            await file.flush()
            assert await file.read_at(0, 3) == b"ONE"

        async for entry in client.scandir("results"):
            print(entry.name, entry.info.size)


asyncio.run(process())
```

An `AsyncClient` belongs to the event loop that created it. Do not share it
across loops. Multiple clients may be used independently, and positional file
I/O is preferable when concurrent tasks operate on distinct ranges.

## Errors and retry decisions

Errors map to familiar Python families where possible:

| Error | Also behaves as |
|---|---|
| `NfsNotFoundError` | `FileNotFoundError` |
| `NfsAlreadyExistsError` | `FileExistsError` |
| `NfsPermissionError` | `PermissionError` |
| `NfsIsADirectoryError` | `IsADirectoryError` |
| `NfsNotADirectoryError` | `NotADirectoryError` |
| `NfsTimeoutError` | `TimeoutError` |
| `NfsConnectionError` | `ConnectionError` |
| `NfsUnsupportedError` | `NotImplementedError` |

```python
from nfs_rs import Client, NfsError, NfsNotFoundError

with Client.connect("nfs://server/export?version=4.1") as client:
    try:
        client.stat("missing")
    except NfsNotFoundError as error:
        print(error.filename, error.operation, error.protocol)
    except NfsError as error:
        print(error.code_name, error.recovery_action, error.outcome)
```

## NFSv4.1 DACL and SACL

NFSv4.1 clients expose `getdacl`, `setdacl`, `getsacl`, and `setsacl` on both
`Client` and `AsyncClient`. Values are immutable `NfsAcl41` objects containing
the ACL flags and an ordered tuple of `NfsAce` entries. A set operation replaces
the complete DACL or SACL attribute; it is not an ACE-level patch.

```python
from nfs_rs import Acl41Flags, Client, NfsAcl41

with Client.connect("nfs://server/export?version=4.1") as client:
    current = client.getdacl("directory")
    client.setdacl(
        "directory",
        NfsAcl41(current.flags | Acl41Flags.PROTECTED, current.aces),
    )
```

These attributes are optional server capabilities. A server that omits DACL or
SACL from a GETATTR response, or returns `NFS4ERR_ATTRNOTSUPP` while setting it,
is reported as `NfsUnsupportedError`. The ordinary NFSv4 `acl` attribute remains
available through the existing Rust API and is distinct from NFSv4.1 DACL/SACL.

For replay-sensitive mutations, `NfsUncertainOutcomeError` means the server may
have completed the request. Never retry it blindly. Inspect authoritative state
(existence, size, checksum, destination name, or application transaction ID),
then follow `error.recovery_action`. `completed_bytes` is authoritative only for
the confirmed prefix reported by the exception.

State-loss errors may require reopening a file or remounting. A
`NfsPositionUncertainError` means the sequential file cursor cannot be trusted;
prefer verification and positional I/O during recovery.

## Cancellation and recovery events

Cancelling an async waiter does not retract protocol work already sent to the
server. The client retains owned cleanup work and records delayed uncertain
results in a bounded recovery-event queue:

```python
from nfs_rs import AsyncClient, OperationOutcome


async def inspect_recovery(client: AsyncClient) -> None:
    for event in client.drain_recovery_events():
        print(event.operation, event.path, event.recovery_action, event.message)
        if event.outcome is OperationOutcome.UNCERTAIN:
            # Verify authoritative server state before deciding what to do.
            pass
```

`recovery_events()` returns a non-destructive snapshot;
`drain_recovery_events()` atomically consumes current events.
`dropped_recovery_event_count` reports overflow of the configured bounded
queue. Monitor it in long-lived services.

## Cleanup and close failures

Use context managers whenever possible. `File.close()` may flush pending data
and therefore may raise `NfsFileCloseError`; `Client.close()` may raise
`NfsClientCloseError` containing component errors. If the body of a context
manager already raised, cleanup failures are preserved without replacing the
original error.

Do not continue using clients or files after close. Closed-resource operations
raise the relevant `NfsClientClosedError` or `NfsClosedResourceError` family.

## Public API summary

The stable facade exports:

- clients: `Client`, `AsyncClient`
- files: `File`, `AsyncFile`
- export discovery: `list_exports`, `list_exports_async`
- information values: `FileInfo`, `DirEntry`, `ExportEntry`, `FsInfo`, `FsStat`,
  `Capabilities`, `IoLimits`, `Health`, `RecoveryEvent`
- enums: `Version`, `FileType`, `Lifecycle`, `OperationOutcome`,
  `OperationClass`, `RecoveryAction`
- typed `NfsError` subclasses

The native `nfs_rs._internal` module is private. Applications should import
only from `nfs_rs`.
