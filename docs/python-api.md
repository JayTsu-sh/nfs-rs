# Python user guide

See the [complete API reference](https://github.com/JayTsu-sh/nfs-rs/blob/v0.11.3/python/nfs_rs/API.md)
for every public method, property, type, field and exception. Both documents ship
inside the installed package as `nfs_rs/API.md` and `nfs_rs/GUIDE.md`.

`nfs-rs` is a userspace NFS client for Python. It talks to NFS servers directly,
so applications do not need a kernel-mounted filesystem. The public package
contains matching synchronous and `asyncio` APIs with PEP 561 type information.

## Support matrix

| Area | Support |
|---|---|
| Python | CPython 3.11+ via the stable ABI |
| Platform | Linux/glibc x86_64 |
| Protocols | NFSv3, NFSv4.1, NFSv4.2, experimental NFSv4.0 |
| Authentication | AUTH_SYS |
| Not supported | NFSv4.2 cross-server/async offload, Kerberos/RPCSEC_GSS, Windows, macOS, musl, PyPy, free-threaded CPython |

NFSv4.0 uses an experimental interoperability profile. Server capabilities such
as named attributes, ACLs, callbacks, locks, delegation retention, and pNFS are
negotiated and exposed through `client.capabilities`.

The release pipeline exercises NFSv3, NFSv4.0 and NFSv4.1 using the final
x86_64 wheel and a wheel rebuilt from the final source distribution against
real NFSv3, NFSv4.0, NFSv4.1, and NFSv4.1 pNFS environments. NFSv4.2
was separately validated on all four AFX data LIFs with the relevant server
features enabled; see the [AFX report](https://github.com/JayTsu-sh/nfs-rs/blob/v0.11.3/docs/validation/nfsv42-review-2026-11-02.md).
Optional operations remain dependent on the server, as described below.

Read and write RPC sizes are determined automatically during mount from server limits, bounded by the client payload ceiling (4 MiB) and negotiated NFSv4.1 session capacities. URL and Python `rsize`/`wsize` options are no longer accepted. Python `io_limits` reports the effective mount limits; pNFS data servers may require smaller chunks.

NFSv4.1 pNFS reuses valid layouts without periodic large-write barriers. It
fetches missing file ranges and reacts to device-change callbacks. A write
wholly rejected with `NFS4ERR_STALE` refreshes the layout and device mapping and
retries once on the DS before safe MDS fallback. Partial writes and uncertain
transport outcomes still require verification and are not replayed by this path.

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
| `version=4.2` | Use NFSv4.2 for existing file APIs; exact selectors do not silently downgrade |
| `version=4.1,4.0,3` | Try an explicit fallback order |
| `uid=`, `gid=` | AUTH_SYS numeric identity |
| `nfsport=`, `mountport=` | Override service ports |
| `readdir-buffer=` | Directory response limit, or `dircount,maxcount`; honored by v3/v4.0/v4.1, additionally bounded by v4.1 session capacity. Without it, both come from the server's read limit at mount (v3 `rtmax`, v4.x `maxread`, v4.1 also the session); 8192 on a v4.0 server without `maxread` |
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

Export URL path segments are percent-decoded once before mounting. Raw Unicode
names and percent-encoded UTF-8 spellings address the same export; `%25` names a
literal `%`. A literal `%20` in a server name is written `%2520` in the URL.
Encoded separators, NUL, and invalid UTF-8 are rejected.

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
accepts only `"3"`, `"4.0"`, `"4.1"`, and `"4.2"`; ambiguous `"4"`, empty
lists, and unknown values are rejected before connecting.
`versions=["4.2", "4.1"]` allows fallback and `Version.NFS_V4_2` reports v4.2.
Session recovery, callbacks, locks, and negotiated pNFS file layouts reuse the
v4.1 engine; data servers can independently negotiate v4.1.

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

`listdir()` collects names using READDIR without fetching full per-entry metadata.
Use `scandir()` for metadata and file handles. `scandir()` streams entries and
avoids constructing a complete list. Consume or
close the client before discarding a partially consumed iterator.

### Resumable directory pages

`scandir_page()` reads one directory page at a time and returns a
`DirectoryPage` with `entries`, `next` and `eof`. The caller holds the position
(`DirectoryCookie`: the server cookie and its 8-byte verifier), so a listing
interrupted by a timeout or reconnect continues from the last page received,
even on a new client of the same server, instead of starting over. Persist
`page.next` only after the page's entries have been processed.

```python
from nfs_rs import Client, DirectoryCookie, NfsBadCookieError

def list_resumably(client: Client, path: str, position: DirectoryCookie) -> None:
    while True:
        try:
            page = client.scandir_page(path, position)
        except NfsBadCookieError:
            # The server no longer accepts this position; only a full restart is safe.
            position = DirectoryCookie()
            continue
        for entry in page.entries:
            print(entry.name, entry.info.size)
        if page.eof:
            return
        position = page.next  # save this to resume after a failure
```

`AsyncClient.scandir_page()` is awaited and returns the same page. A server may
reject a saved position after the directory changed or the server restarted;
that raises `NfsBadCookieError` (NFS3ERR_BAD_COOKIE, NFS4ERR_BAD_COOKIE or
NFS4ERR_NOT_SAME). Some servers accept any cookie and simply return fewer
entries, so a restarted listing can repeat entries already processed. A page
that makes no progress raises `NfsEncodingError`. Repeated cookies across pages
are not detected for you: check `page.eof` first (an EOF page without entries
repeats the requested cookie), then keep the cookies already received and stop
when `page.next.cookie` repeats one; otherwise a server whose cookies cycle can
page forever.

Namespace operations include `mkdir`, `touch`, `remove`/`unlink`, `rmdir`,
`rename`, hard `link`, `symlink`, and `readlink`. `remove` and `unlink` are
equivalent file-removal operations.

### Checking symbolic-link targets

`readlink(path)` returns the stored target text. `stat(path)` and `exists(path)`
inspect the link itself; a dangling link can therefore pass `exists`.
The String-based interface raises `NfsEncodingError` for a non-UTF-8 target;
it never replaces undecodable bytes with a different filename.
Use `resolve(path)` to follow intermediate and final links and inspect the target:

```python
from nfs_rs import PathResolutionStatus

result = client.resolve("jobs/latest")  # await client.resolve(...) for AsyncClient
if result.exists is True:
    print(result.path, result.info.type, result.info.size)
elif result.exists is False:
    print("Target is missing or a path component is not a directory", result.status)
elif result.status is PathResolutionStatus.OUTSIDE_MOUNT:
    print("Target requires a namespace outside this mount")
else:
    print("Resolution exceeded the 40-link limit (possibly a cycle)")
```

`PathResolution.status` is `RESOLVED`, `MISSING`, `NOT_DIRECTORY`, `OUTSIDE_MOUNT`
or `TOO_MANY_SYMLINKS`. `path` and `info` are present only for `RESOLVED`;
`path` is export-relative (`.` for root). `exists` is `True` for `RESOLVED`,
`False` for `MISSING`/`NOT_DIRECTORY`, and `None` for the other two states.
Check with `is True` / `is False` to preserve that distinction.

Relative targets start at the link's directory. `..` is evaluated after following
preceding links, and trailing slashes require a directory. Unlike the ordinary
path helpers, `resolve` preserves those components for traversal and reports
`OUTSIDE_MOUNT` if they leave the mounted root. Input paths must be relative;
absolute link targets also return `OUTSIDE_MOUNT`, because the library cannot map
the client's root namespace to this export. This does not mean the target is broken.

The resolver follows at most 40 links and accepts at most 4096 cumulative path
components / 64 KiB of cumulative UTF-8 path text, including link expansions.
Oversized or NUL-containing text raises `ValueError`. Permission, connection,
timeout, stale-handle and other errors propagate instead of becoming a missing
target. `operation_timeout` covers the complete Python resolution. This is an
observation of the namespace; it neither checks read/write permissions nor
guarantees that a later open will see the same object.

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

`wb` and `w+b` create a missing file or truncate an existing file before
returning. `ab` and `a+b` create if needed and preserve existing contents.
Creation requests permissions `0666`; existing-file permissions are preserved,
including when another client creates the name concurrently. Creating opens
reuse the resolved parent directory. A confirmed fresh create needs no extra
truncate; NFSv4 also sends permissions with OPEN, with a separate SETATTR
fallback if the server rejects those creation attributes. Creating/truncating
opens are non-transactional: a later error can leave the file created or
truncated. These rules also apply to `AsyncClient.open()`.

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

On NFSv4.0/v4.1, the xattr methods use the optional OPENATTR named-attribute
file interface. On NFSv4.2 they use RFC 8276 GETXATTR/SETXATTR/LISTXATTRS/
REMOVEXATTR after checking the target filesystem's xattr_support attribute.
These are distinct namespaces; no fallback between them is implicit. Keys are
sent unchanged (including a supplied `user.` prefix). SETXATTR is atomic and
accepts `mode=XattrSetMode.CREATE` or `REPLACE`; EITHER is the default. Empty
and binary values are supported within server and negotiated channel limits.
The existing `capabilities.named_attributes` flag describes OPENATTR, not the
RFC 8276 extension; inspect `client.nfs42_attributes(path).xattr_support`.
There is no fixed client limit on the complete value size; server and file system
limits still apply. Negotiated read/write sizes limit individual requests.
`getxattr` continues short reads to EOF and buffers the complete value in memory.
`setxattr` replaces the entire value, including truncating old contents when
setting a shorter or empty value, and completes short writes before closing.
Replacement is not atomic across clients: an error after truncation can leave a
partial value. Inspect the uncertain outcome and verify the value before retrying;
write and cleanup failures remain available in the error source chain.

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

Definite connection loss gets a short default recovery budget: at most four
safe reconnects within 500 ms of the first disconnect. Backoff, TCP setup,
session binding and replay share this limit, subject to the request's own
replay permission and deadline. Brief flaps can recover transparently; exhaustion
preserves the first connection error and sent/uncertain outcome evidence.
No URL setting is needed. Ordinary response timeouts remain separate.

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

## NFSv4.1/v4.2 DACL and SACL

NFSv4.1 and NFSv4.2 clients expose `getdacl`, `setdacl`, `getsacl`, and `setsacl` on both
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

RPC timeouts cover admission, sending and response waiting. A Python
`operation_timeout` limits how long the caller waits; an admitted modifying
operation may continue settling after that timeout. It is not a rollback or
proof that the server did nothing. Close the client to drain owned operations,
and inspect recovery events before retrying uncertain mutations.

Awaited native operations also drain their Python completion notification before
resuming the caller. This prevents a result-notification thread from racing
interpreter shutdown after `await client.close()`. Synchronous and asynchronous
clients may be used in the same process; close each client before exiting.

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
  `Capabilities`, `IoLimits`, `Health`, `RecoveryEvent`, `NfsAce`, `NfsAcl41`, `PathResolution`
- enums: `Version`, `FileType`, `Lifecycle`, `OperationOutcome`,
  `OperationClass`, `RecoveryAction`, `AceFlags`, `AceMask`, `AceType`, `Acl41Flags`, `PathResolutionStatus`
- typed `NfsError` subclasses

The native `nfs_rs._internal` module is private. Applications should import
only from `nfs_rs`.


## NFSv4.2 file operations

`allocate(path, offset, length)` reserves space; `deallocate` punches a hole
without changing EOF. `seek_content(path, offset, SeekContent.DATA/HOLE)`
queries sparse content boundaries; ordinary `File.seek` still moves only the
local cursor. `read_plus(path, offset, count)` returns `ReadPlusResult` whose
extents contain bytes or `data=None` for holes, clipped to the requested range.
Counts are bounded by the negotiated READ/session limit and can be short.
The client accepts at most 4096 extents per reply and 65536 names per xattr
listing. Xattr values are bounded by the 4 MiB client ceiling as well as
negotiated channel and server limits; oversized results are errors, not
truncated successes.

`copy_range(source, destination, count=0)` performs synchronous intra-server
COPY and returns the durable `CopyResult.count`. Both paths must already exist;
a zero count means source EOF. `clone_range` performs atomic CLONE with the
server's alignment rules. COPY/CLONE have no software fallback and can be
unsupported even when v4.2 mounts succeed. Cross-server and asynchronous
copy offload are not exposed.

All these methods are available on Client and AsyncClient. Temporary OPENs
and file barriers remain held until admitted work and CLOSEs settle, including
a caller timeout or cancellation. An uncertain mutation requires verifying
the target before resuming. Inspect `nfs42_attributes(path)` for per-filesystem
xattr support and optional CLONE block alignment; operation availability must
be tested separately. AFX's configured outcomes are documented in the
repository's validation notes and do not define other servers' capabilities.
On the tested AFX export, enabled sparse operations support whole-file
deallocation; partial punching returns NFS4ERR_IO, also reproduced with the
Linux kernel client. See the
[enabled-feature validation](https://github.com/JayTsu-sh/nfs-rs/blob/v0.11.3/docs/validation/nfsv42-afx-enabled-2026-11-02.md).


## Temporary server-busy responses

NFSv3, NFSv4.0, NFSv4.1 and NFSv4.2 use fixed bounded retries for safely
reissuable JUKEBOX/DELAY/GRACE responses: at most 16 busy reissues, equal-jitter
exponential delays with 200 ms base and 5 s cap, within a shared 60 s budget.
There is no off mode or retry connection option. Shorter RPC limits and explicit
operation_timeout remain effective. Independent pNFS DS calls share the policy;
exhausted backpressure does not restart through layout refresh/MDS fallback.
A large file transfer does not share one global 60 s limit across all chunks.

Exhaustion raises NfsServerBusyError (also an NfsRetryableError) with the original
code/code_name and cause, retry_count, retry_elapsed_ms and retry_reason
("retry_limit" or "deadline"). Queue that work for later instead of immediately
starting another full retry budget. Permanent errors are returned immediately.
Owner-seqid state transitions and already successful modifications are not
blindly replayed; uncertain mutations still require verification. Cancellation
stops read backoff, while admitted mutations retain settlement responsibility.
Details and protocol boundaries are in the repository's bounded-retry notes.
