import asyncio
import os
import time

import pytest

REAL_URL = os.environ.get("NFS_RS_PYTHON_REAL_URL")
if not REAL_URL:
    pytest.skip("requires NFS_RS_PYTHON_REAL_URL", allow_module_level=True)

from nfs_rs import AsyncClient, Client, FileType


def test_real_server_sync_root_metadata_and_streaming_directory():
    with Client.connect(REAL_URL) as client:
        root = client.stat(".")
        assert root.type is FileType.DIRECTORY
        entries = client.scandir(".")
        first = next(entries, None)
        if first is not None:
            assert first.info.fileid > 0
            assert first.name in client.listdir(".")


def test_real_server_async_root_metadata_and_streaming_directory():
    async def scenario():
        async with await AsyncClient.connect(REAL_URL) as client:
            root = await client.stat(".")
            assert root.type is FileType.DIRECTORY
            entries = [entry async for entry in client.scandir(".")]
            assert all(entry.info.fileid > 0 for entry in entries)

    asyncio.run(scenario())


def test_real_server_metadata_xattrs_and_filesystem_information():
    path = f"python-ticket08-{os.getpid()}-{time.time_ns()}"
    with Client.connect(REAL_URL) as client:
        try:
            assert client.write_bytes(path, b"metadata") == 8
            client.chmod(path, 0o600)
            before = client.stat(path)
            client.chown(path, -1, -1)
            after = client.stat(path)
            assert (after.uid, after.gid) == (before.uid, before.gid)
            now = time.time_ns()
            client.utime(path, ns=(now, now))
            client.truncate(path, 4)
            assert client.stat(path).size == 4
            assert client.access(path, os.F_OK | os.R_OK)
            assert client.fs_info().max_file_size >= 4
            assert client.fs_stat().total_bytes >= client.fs_stat().available_bytes
            assert client.io_limits.max_read > 0
            if client.capabilities.named_attributes:
                client.setxattr(path, "user.nfs_rs_ticket08", b"roundtrip")
                assert client.getxattr(path, "user.nfs_rs_ticket08") == b"roundtrip"
                assert "user.nfs_rs_ticket08" in client.listxattr(path)
                client.removexattr(path, "user.nfs_rs_ticket08")
        finally:
            client.remove(path, missing_ok=True)
        with pytest.raises(FileNotFoundError):
            client.chmod(path, 0o600)


def test_real_server_async_metadata_information_parity():
    async def scenario():
        async with await AsyncClient.connect(REAL_URL) as client:
            assert await client.access(".", os.F_OK | os.R_OK | os.X_OK)
            assert (await client.fs_info()).max_file_size > 0
            assert (await client.fs_stat()).total_bytes > 0
            assert client.io_limits.max_write > 0

    asyncio.run(scenario())


def test_real_server_configured_permission_failure():
    denied_path = os.environ.get("NFS_RS_PYTHON_DENIED_PATH")
    if not denied_path:
        pytest.skip("requires NFS_RS_PYTHON_DENIED_PATH")
    with Client.connect(REAL_URL) as client:
        assert not client.access(denied_path, os.R_OK)
        with pytest.raises(PermissionError):
            client.chmod(denied_path, 0o600)
