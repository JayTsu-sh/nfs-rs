from __future__ import annotations

import os

import pytest


pytestmark = pytest.mark.skipif(os.environ.get("NFS_RS_TEST_INSTALLED") != "1", reason="requires test-support wheel")


def test_sync_metadata_xattrs_and_filesystem_snapshots() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    client.chmod(b'file', 0o640)
    client.chown(b'file', 1000, 1001)
    client.chown(b'file', -1, -1)
    client.utime(b'file', ns=(1_000_000_002, 3_000_000_004))
    client.truncate(b'file', 9)
    assert client.access(b'file', os.R_OK | os.W_OK)
    assert not client.access(b'denied', os.R_OK)
    assert not client.access(b'missing', os.F_OK)
    with pytest.raises(ValueError): client.access(b'file', 8)
    with pytest.raises(PermissionError): client.chmod(b'denied', 0o600)
    with pytest.raises(RuntimeError, match="(?i)i/o error"):
        client.truncate(b'protocol-error', 1)
    client.setxattr(b'file', "user.key", b"value")
    assert client.getxattr(b'file', "user.key") == b"value"
    assert client.listxattr(b'file') == ["user.key"]
    client.removexattr(b'file', "user.key")
    with pytest.raises(FileNotFoundError): client.getxattr(b'file', "user.key")
    with pytest.raises(NotImplementedError): client.listxattr(b'unsupported')
    assert client.fs_info().max_file_size == 2**40
    assert client.fs_stat().available_bytes == 500
    assert not client.capabilities.named_attributes
    assert client.io_limits.max_read == 4
    client.close()


def test_async_metadata_and_xattr_twins() -> None:
    from nfs_rs import AsyncClient

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        await client.chmod(b'file', 0o600)
        await client.chown(b'file', 1, 2)
        await client.utime(b'file', ns=(10, 20))
        await client.truncate(b'file', 4)
        assert await client.access(b'file', os.R_OK)
        await client.setxattr(b'file', "user.async", b"data")
        assert await client.getxattr(b'file', "user.async") == b"data"
        assert await client.listxattr(b'file') == ["user.async"]
        await client.removexattr(b'file', "user.async")
        assert (await client.fs_stat()).total_bytes == 1000
        assert client.io_limits.max_write == 4
        await client.close()

    import asyncio

    asyncio.run(scenario())
