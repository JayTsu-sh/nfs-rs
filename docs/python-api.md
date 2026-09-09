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

Read and write RPC sizes are determined automatically during mount from server limits, bounded by the client payload ceiling (4 MiB) and negotiated NFSv4.1 session capacities. URL and Python `rsize`/`wsize` options are no longer accepted. Python `io_limits` reports the effective mount limits; pNFS data servers may require smaller chunks.

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
| `readdir-buffer=` | Directory response limit, or `dircount,maxcount` |
| `noresvport=true` | Use an unprivileged source port |
| `retain-delegations=true` | Retain delegations when supported |

`write()` and `write_at()` return successfully only after all bytes in that
call are durable. Each call splits its buffer by negotiated `max_write`, runs at most 8
UNSTABLE WRITE chunks concurrently, completes short writes within each chunk,
and then completes batch durability synchronization, including required pNFS metadata synchronization.
If every WRITE reply reports FILE_SYNC, no extra COMMIT RPC is needed. There is no
`writeback` option or 16 MiB commit threshold. `flush()` waits for active writes;
`close()` releases file state. Neither defers the durability of successful writes.
A failed write can have accepted some bytes without making the batch durable;
`completed_bytes` is the total acknowledged byte count, which may cover
non-contiguous ranges; it is neither a resume offset nor a persistence guarantee.
On failure, new chunks stop being scheduled and all active chunks settle before
the error is returned. The normal batch commit runs only after every chunk succeeds.

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
    print(info.size, info.mode, info.uid, info.gid, info.mtime)

    print(client.listdir("jobs/2026/input"))
    for entry in client.scandir("jobs/2026/input"):
        print(entry.name, entry.path, entry.info.type)

    client.rename("jobs/2026/input/ready", "jobs/2026/input/started")
    client.unlink("jobs/2026/input/started", missing_ok=True)
    client.rmdir("jobs/2026/input")
```

`FileInfo.atime`, `mtime`, and `ctime` are Python integers measured in **ns
since Unix epoch (UTC)**. The field names omit the unit suffix; their values
remain nanoseconds. `atime` is the last access time, `mtime` is the last data
modification time, and `ctime` is the last metadata/status change time, not the
creation time. The same fields appear on `DirEntry.info` from `scandir`.
`utime(ns=(atime, mtime))` continues to take nanosecond values.

`scandir` accepts `DirectoryRef(path, fh=None)` or a previously returned
`DirEntry`, in addition to a string/path-like value. With `fh`, the adapter calls
`Mount::readdirplus(fh)` directly; without it, it first calls `lookup_path`.
`DirEntry.fh` contains the server's opaque handle when supplied, otherwise `None`.
Use handles from the same mount. The path labels results and errors; it is not
looked up or checked against a supplied handle. Invalid/stale handles produce an
error instead of silently falling back to path lookup. Use `None` for an absent
handle; an empty or non-bytes handle is rejected.

```python
from nfs_rs import DirectoryRef, FileType

for entry in client.scandir(DirectoryRef("jobs", fh=None)):
    if entry.info.type == FileType.DIRECTORY:
        for child in client.scandir(entry):
            print(child.path)  # reuses entry.fh when present
```

The same target structures work with `AsyncClient.scandir`.

Both clients transfer directory results from Rust in batches of at most 128
entries, while the public iterator yields one `DirEntry` at a time. Ready
partial batches are delivered before waiting for another directory page. The
producer queues at most one batch; it does not collect the entire directory or
acquire the Python GIL to produce entries. Valid entries preceding a protocol
error are yielded before that error is raised.

`scandir()` streams entries and avoids constructing a complete list. Consume or
close the client before discarding a partially consumed iterator.

Namespace operations include `mkdir`, `touch`, `remove`/`unlink`, `rmdir`,
`rename`, hard `link`, `symlink`, and `readlink`. `remove` and `unlink` are
equivalent file-removal operations.

### Reading and writing whole files

```python
from nfs_rs import Client

with Client.connect("nfs://server/export?version=4.1") as client:
    with client.open("message.bin", "wb") as file:
        count = file.write(b"hello")
    assert count == 5
    with client.open("message.bin", "rb") as file:
        assert file.read() == b"hello"
```

`read()` holds the complete file in memory. For large files, reuse a buffer with
`readinto()` instead.

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

        file.seek(0)
        first = file.read(chunk_size)
        print(file.tell(), len(first))
```

`File` implements `io.RawIOBase` behavior including `read`, `readinto`, `seek`,
`tell`, `write`, `truncate`, and `flush`. It has no operating-system file
descriptor, so `fileno()` is unsupported. `read_at`/`readinto_at` and `write_at`
perform positional I/O without changing the file position.

`File.read(size=-1)` and `File.read_at(offset, size=-1)` return `bytes` and
use the same negotiated chunk size and maximum of 8 concurrent reads as
`readinto` and `readinto_at`. Non-empty short responses are completed until
the requested range is filled or EOF is reached. Omitting `size` reads to EOF
in bounded batches. `read` advances the position; `read_at` leaves it unchanged.
Response buffers are retained without copying their payload, then copied once
into the final Python `bytes`. Use `readinto` with a reusable buffer to avoid
allocating a new result for every call.

`Client.read_bytes` and `Client.write_bytes` (including their async variants)
have been removed. Open a file with `client.open()` and use its read/write methods.

`readinto(buffer)` and `readinto_at(buffer, offset)` accept writable,
C-contiguous buffer objects, including `bytearray`, writable `memoryview`, and
contiguous arrays. They split the entire requested range by negotiated
`max_read`, with at most 8 concurrent requests per call. Short non-empty
responses are continued until the range is filled or EOF is reached; an empty
response without EOF is an error. The returned count covers a contiguous prefix;
the tail past EOF is untouched. `readinto` advances the cursor by that count;
`readinto_at` leaves it unchanged. An empty buffer returns zero without a READ.
There is no speculative read-ahead or retained read cache; `readahead` is removed.

```python
buffer = bytearray(8 * 1024 * 1024)
count = file.readinto(buffer)  # await file.readinto(buffer) for AsyncFile
with memoryview(buffer)[:count] as data:
    consume(data)
```

Each response payload is copied directly into the target once. The RPC transport
still owns its response storage; this removes intermediate concatenation and
Python `bytes` copies rather than providing socket-to-buffer zero-copy I/O.
The target is pinned against resizing during the call. Do not access it from
other tasks or threads until the call completes. On cancellation, timeout, or
failure a prefix may already have been filled, but no later background work can
modify the target after the public call returns. The buffer can then be reused.

For large transfers, loop over bounded buffers (commonly 1–8 MiB). The native
client further splits them to negotiated `client.io_limits`. A successful
`write` returns the byte count after committing all bytes to stable storage.

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

The public Python API exposes the negotiated `capabilities.acl` flag. The
ordinary NFSv4 `acl` attribute is available through Rust; Python exposes the
separate NFSv4.1 DACL/SACL methods described below. Do not treat generic xattr
calls as a portable ACL encoding.

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
        async with await client.open("results/one.bin", "wb") as file:
            await file.write(b"one")

        async with await client.open("results/one.bin", "r+b") as file:
            await file.write_at(b"ONE", 0)
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
then follow `error.recovery_action`. For concurrent writes, `completed_bytes`
counts acknowledged bytes that may span non-contiguous ranges; it is not a
confirmed prefix, resume offset, or durability guarantee.

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

Use context managers whenever possible. `File.close()` waits for active writes and releases protocol state
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
- information values: `FileInfo`, `DirectoryRef`, `DirEntry`, `ExportEntry`, `FsInfo`, `FsStat`,
  `Capabilities`, `IoLimits`, `Health`, `RecoveryEvent`, `NfsAce`, `NfsAcl41`
- enums: `Version`, `FileType`, `Lifecycle`, `OperationOutcome`,
  `OperationClass`, `RecoveryAction`, `AceFlags`, `AceMask`, `AceType`, `Acl41Flags`
- typed `NfsError` subclasses

The native `nfs_rs._internal` module is private. Applications should import
only from `nfs_rs`.
