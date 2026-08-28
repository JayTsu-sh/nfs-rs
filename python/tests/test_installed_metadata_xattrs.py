from __future__ import annotations

import os

import pytest


pytestmark = pytest.mark.skipif(os.environ.get("NFS_RS_TEST_INSTALLED") != "1", reason="requires test-support wheel")


def test_sync_metadata_xattrs_and_filesystem_snapshots() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    client.chmod("file", 0o640)
    client.chown("file", 1000, 1001)
    client.utime("file", ns=(1_000_000_002, 3_000_000_004))
    client.truncate("file", 9)
    assert client.access("file", os.R_OK | os.W_OK)
    assert not client.access("denied", os.R_OK)
    client.setxattr("file", "user.key", b"value")
    assert client.getxattr("file", "user.key") == b"value"
    assert client.listxattr("file") == ["user.key"]
    client.removexattr("file", "user.key")
    with pytest.raises(FileNotFoundError): client.getxattr("file", "user.key")
    with pytest.raises(NotImplementedError): client.listxattr("unsupported")
    assert client.fs_info().max_file_size == 2**40
    assert client.fs_stat().available_bytes == 500
    assert not client.capabilities.named_attributes
    assert client.io_limits.max_read == 4
    client.close()


def test_async_metadata_and_xattr_twins() -> None:
    from nfs_rs import AsyncClient

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        await client.chmod("file", 0o600)
        await client.chown("file", 1, 2)
        await client.utime("file", ns=(10, 20))
        await client.truncate("file", 4)
        assert await client.access("file", os.R_OK)
        await client.setxattr("file", "user.async", b"data")
        assert await client.getxattr("file", "user.async") == b"data"
        assert await client.listxattr("file") == ["user.async"]
        await client.removexattr("file", "user.async")
        assert (await client.fs_stat()).total_bytes == 1000
        assert client.io_limits.max_write == 4
        await client.close()

    import asyncio

    asyncio.run(scenario())
