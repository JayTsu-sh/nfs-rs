# nfs-rs for Python

`nfs-rs` provides typed synchronous and `asyncio` clients for accessing NFS
exports directly from Python without a kernel mount or a C NFS library.

- NFSv3 (the default when no version is selected)
- experimental NFSv4.0, selected explicitly as `4.0`
- NFSv4.1, including negotiated file-layout pNFS
- synchronous and native async APIs
- file, directory, metadata, link, and extended-attribute operations
- directory scans with reusable file handles and file attributes
- durable writes and caller-buffer reads with up to 8 concurrent chunks
- PEP 561 type information included

Authentication uses AUTH_SYS. Kerberos and RPCSEC_GSS are not implemented.

## Complete API documentation

- [API reference: all public Python interfaces](https://github.com/JayTsu-sh/nfs-rs/blob/v0.8.1/python/nfs_rs/API.md)
- [User guide](https://github.com/JayTsu-sh/nfs-rs/blob/v0.8.1/python/nfs_rs/GUIDE.md)

Both complete documents are included in the PyPI wheel and source distribution,
including signatures, defaults, return values, timestamp units, error types,
synchronous/asynchronous usage and protocol limitations. Read them offline:

```python
from importlib.resources import files

print(files("nfs_rs").joinpath("API.md").read_text(encoding="utf-8"))
print(files("nfs_rs").joinpath("GUIDE.md").read_text(encoding="utf-8"))
```

Named attributes use the optional NFSv4.1 OPENATTR file interface, not the
NFSv4.2 GETXATTR/SETXATTR extension or a portable mapping of Linux POSIX xattrs.
There is no fixed client limit on the complete value size; server and file system
limits still apply. Negotiated read/write sizes limit individual requests.
`getxattr` continues short reads to EOF and buffers the complete value in memory.
`setxattr` replaces the entire value, including truncating old contents when
setting a shorter or empty value, and completes short writes before closing.
Replacement is not atomic across clients: an error after truncation can leave a
partial value. Inspect the uncertain outcome and verify the value before retrying;
write and cleanup failures remain available in the error source chain.

## Select a protocol version

The Python API accepts exactly `"3"`, `"4.0"`, and `"4.1"`. Select one in the
URL or pass an ordered fallback list to `versions`:

```python
from nfs_rs import Client, Version

# NFSv3 is the default when the URL has no version query parameter.
with Client.connect("nfs://server.example.com/export") as client:
    assert client.version is Version.NFS_V3

# Select one exact NFSv4 minor version.
with Client.connect("nfs://server.example.com/export?version=4.0") as client:
    assert client.version is Version.NFS_V4_0

with Client.connect("nfs://server.example.com/export?version=4.1") as client:
    assert client.version is Version.NFS_V4_1

# Try NFSv4.1 first, then NFSv4.0, then NFSv3.
with Client.connect(
    "nfs://server.example.com/export",
    versions=["4.1", "4.0", "3"],
) as client:
    print("negotiated", client.version)
```

The ambiguous selector `"4"` and unimplemented NFSv4.2 are rejected. NFSv4.0
is experimental and requires the exact `"4.0"` selector.

## Install

```console
python -m pip install nfs-rs
```

The wheel supports CPython 3.11 or newer on Linux/glibc x86_64.

## Connect and work with files

```python
from nfs_rs import Client

url = "nfs://server.example.com/export?version=4.1&noresvport=true"

with Client.connect(url, connect_timeout=10, operation_timeout=30) as client:
    client.mkdir("incoming", parents=True, exist_ok=True)
    with client.open("incoming/hello.txt", "wb") as file:
        written = file.write(b"hello NFS")
    assert written == 9

    info = client.stat("incoming/hello.txt")
    print(info.size, info.mode, info.uid, info.gid)
    print(info.atime, info.mtime, info.ctime)  # Integer nanoseconds since Unix epoch.

    with client.open("incoming/hello.txt", "rb") as source:
        assert source.read(5) == b"hello"
        assert source.read_at(6, 3) == b"NFS"

    for entry in client.scandir("incoming"):
        print(entry.name, entry.info.size)
```

Paths are relative to the export root. Absolute paths, `..` escapes, NUL bytes,
and byte-string paths are rejected. File modes are binary: `rb`, `wb`, `ab`,
`r+b`, `w+b`, and `a+b`.

`FileInfo.atime`, `mtime`, and `ctime` are integer nanoseconds since the Unix
epoch. The attribute names have no `_ns` suffix; `ctime` is the metadata change
time, not the file creation time.

## Scan directories using file handles

`scandir()` yields entries from one directory. Each `DirEntry` contains `name`,
`path`, `info`, and an optional `fh`. Use `DirectoryRef(path, fh)` to reuse a
returned handle when scanning a child directory:

```python
from collections import deque

from nfs_rs import Client, DirectoryRef, FileType

url = "nfs://server.example.com/export?version=4.1&noresvport=true"

with Client.connect(url) as client:
    pending = deque([DirectoryRef("incoming")])
    while pending:
        directory = pending.popleft()
        for entry in client.scandir(directory):
            if entry.name in (".", ".."):
                continue
            print(entry.path, entry.info.size, entry.info.mtime)
            if entry.info.type is FileType.DIRECTORY:
                pending.append(DirectoryRef(entry.path, entry.fh))
```

When `fh` is supplied, the adapter calls `Mount::readdirplus(fh)` directly.
Otherwise, it resolves `path` first. The loop above implements recursion;
`scandir()` itself does not recurse or follow directory symlinks. Reuse handles
with the client that returned them; an invalid or stale handle is reported as
an error rather than silently replaced through path lookup.

## Read and write with a large buffer

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

`readinto()` fills a writable buffer and returns the number of bytes read. It
continues short server responses until the buffer is full or EOF is reached;
zero means EOF for a nonempty buffer. Only the first returned number of bytes
is valid data for that call.

This example copies a file with one reusable 40 MiB read buffer. The source
file must already exist; opening the destination with `"wb"` truncates it.

```python
from nfs_rs import Client

url = "nfs://server.example.com/export?version=4.1&noresvport=true"
buffer = bytearray(40 * 1024 * 1024)  # Caller-selected size, not a library default.

with Client.connect(url, operation_timeout=120) as client:
    print(client.io_limits.max_read, client.io_limits.max_write)
    with client.open("incoming/source.bin", "rb") as source:
        with client.open("incoming/copy.bin", "wb") as destination:
            with memoryview(buffer) as view:
                while (count := source.readinto(buffer)) != 0:
                    with view[:count] as chunk:
                        written = destination.write(chunk)
                    assert written == count  # These bytes are already durable.
```

Read and write limits are negotiated at mount time. Each call splits the
buffer by its corresponding negotiated limit and schedules at most 8 chunks
concurrently. For a 1 MiB limit, 40 KiB needs one chunk, 4 MiB needs four, and
40 MiB needs forty with at most eight active at once. Smaller calls do not
force eight-way concurrency. There are no `rsize`, `wsize`, `readahead`, or
`writeback` options.

The read buffer is reused, and the `memoryview` slice avoids a Python slice
copy. `write()` snapshots its input internally, so this is not an end-to-end
zero-copy file transfer.

`write()` and `write_at()` return successfully only after all bytes in that
call are durable. They finish all UNSTABLE WRITE chunks and any required batch
commit, including pNFS synchronization. When every WRITE reply reports
FILE_SYNC, no extra COMMIT RPC is needed. There is no delayed commit threshold;
`flush()` waits for active writes, and `close()` releases file state. A write
failure may leave some ranges modified: `completed_bytes` is an acknowledged
byte count, not a safe resume offset or a durability guarantee.

## Asyncio

```python
import asyncio

from nfs_rs import AsyncClient, DirectoryRef, FileType


async def main() -> None:
    url = "nfs://server.example.com/export?version=4.1&noresvport=true"
    async with await AsyncClient.connect(url) as client:
        await client.mkdir("outgoing", exist_ok=True)
        payload = b"x" * (4 * 1024 * 1024)
        async with await client.open("outgoing/result.bin", "wb") as destination:
            written = await destination.write(payload)
            assert written == len(payload)  # Durable before the await completes.

        buffer = bytearray(len(payload))
        async with await client.open("outgoing/result.bin", "rb") as source:
            count = await source.readinto(buffer)
            assert count == len(payload)
            assert buffer == payload

        async for entry in client.scandir(DirectoryRef("outgoing")):
            if entry.name in (".", ".."):
                continue
            print(entry.path)
            if entry.info.type is FileType.DIRECTORY:
                async for child in client.scandir(DirectoryRef(entry.path, entry.fh)):
                    if child.name not in (".", ".."):
                        print(child.path)


asyncio.run(main())
```

`AsyncClient.scandir()` is consumed with `async for`; do not await the iterator
itself. Keep a buffer passed to an in-progress async read unchanged until the
await completes. Async reads and writes use the same chunking and durability
rules as the synchronous API.

## Metadata and extended attributes

```python
import os

from nfs_rs import Client

with Client.connect("nfs://server/export?version=4.1") as client:
    client.chmod("data.bin", 0o640)
    assert client.access("data.bin", os.R_OK)

    if client.capabilities.named_attributes:
        client.setxattr("data.bin", "user.content-type", b"application/octet-stream")
        assert client.getxattr("data.bin", "user.content-type") == b"application/octet-stream"
        print(client.listxattr("data.bin"))
        client.removexattr("data.bin", "user.content-type")
```

Capability values are negotiated with the server. Check them before depending
on optional behavior such as named attributes, ACL support, callbacks, or pNFS.

## Errors and uncertain outcomes

```python
from nfs_rs import Client, NfsNotFoundError, NfsUncertainOutcomeError

with Client.connect("nfs://server/export?version=4.1") as client:
    try:
        with client.open("missing.bin", "rb") as file:
            data = file.read()
    except NfsNotFoundError:
        data = b""

    try:
        client.rename("staging.bin", "committed.bin")
    except NfsUncertainOutcomeError as error:
        # Do not retry blindly: the server may have completed the operation.
        print(error.recovery_action, error.outcome)
        print(client.exists("committed.bin"))
```

Built-in families such as `FileNotFoundError`, `PermissionError`,
`IsADirectoryError`, `TimeoutError`, and `ConnectionError` also work. For
modifying operations, inspect `recovery_action`, `outcome`, and
`client.recovery_events()` before retrying.

## Documentation

See the complete [Python user guide](https://github.com/JayTsu-sh/nfs-rs/blob/main/docs/python-api.md)
for URL options, export discovery, all filesystem operations, streaming large
files, concurrency, cancellation, recovery, and the support matrix.

## License

Apache-2.0
