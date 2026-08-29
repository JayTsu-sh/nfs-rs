# nfs-rs for Python

`nfs-rs` provides typed synchronous and `asyncio` clients for accessing NFS
exports directly from Python without a kernel mount or a C NFS library.

- NFSv3 (the default when no version is selected)
- experimental NFSv4.0, selected explicitly as `4.0`
- NFSv4.1, including negotiated file-layout pNFS
- synchronous and native async APIs
- file, directory, metadata, link, and extended-attribute operations
- PEP 561 type information included

Authentication uses AUTH_SYS. Kerberos and RPCSEC_GSS are not implemented.

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
    written = client.write_bytes("incoming/hello.txt", b"hello NFS")
    assert written == 9

    info = client.stat("incoming/hello.txt")
    print(info.size, info.mode, info.uid, info.gid)

    with client.open("incoming/hello.txt", "rb") as source:
        assert source.read(5) == b"hello"
        assert source.read_at(6, 3) == b"NFS"

    for entry in client.scandir("incoming"):
        print(entry.name, entry.info.size)
```

Paths are relative to the export root. Absolute paths, `..` escapes, NUL bytes,
and byte-string paths are rejected. File modes are binary: `rb`, `wb`, `ab`,
`r+b`, `w+b`, and `a+b`.

## Asyncio

```python
import asyncio

from nfs_rs import AsyncClient


async def main() -> None:
    url = "nfs://server.example.com/export?version=4.1&noresvport=true"
    async with await AsyncClient.connect(url) as client:
        await client.mkdir("outgoing", exist_ok=True)
        await client.write_bytes("outgoing/result.bin", b"result")

        async with await client.open("outgoing/result.bin", "rb") as source:
            assert await source.read() == b"result"

        async for entry in client.scandir("outgoing"):
            print(entry.path)


asyncio.run(main())
```

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
        data = client.read_bytes("missing.bin")
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
