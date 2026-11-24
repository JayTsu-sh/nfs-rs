from __future__ import annotations

import os
import errno

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NFS_RS_TEST_INSTALLED") != "1",
    reason="requires the non-editable test-support wheel",
)


def test_sync_namespace_and_whole_file_conveniences() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    client.mkdir(b'parent/child', parents=True)
    client.rename(b'old', b'new')
    client.link(b'new', b'hard')
    client.symlink(b'../raw/target', b'sym')
    assert client.readlink(b'sym') == b'../target'
    with client.open(b'whole.bin', "wb") as file:
        assert file.write(b"contents") == 8
    with client.open(b'whole.bin', "rb") as file:
        assert file.read() == b"contents"
    client.touch(b'empty.bin')
    client.remove(b'whole.bin')
    client.rmdir(b'parent/child')
    client.close()


def test_namespace_faults_preserve_before_and_after_send_outcomes() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    with pytest.raises(RuntimeError, match="DefiniteFailure"):
        client.remove(b'__before_send__')
    with pytest.raises(RuntimeError, match="Uncertain"):
        client.rename(b'__after_send__', b'destination')
    with pytest.raises(NotADirectoryError):
        client.remove(b'__notdir__')
    with pytest.raises(IsADirectoryError):
        client.remove(b'__isdir__')
    with pytest.raises(OSError) as not_empty:
        client.rmdir(b'__notempty__')
    assert not_empty.value.errno == errno.ENOTEMPTY
    client.close()


def test_async_namespace_twins() -> None:
    from nfs_rs import AsyncClient

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        await client.mkdir(b'async/child', parents=True)
        await client.rename(b'from', b'to')
        await client.link(b'to', b'hard')
        await client.symlink(b'../../target', b'sym')
        assert await client.readlink(b'sym') == b'../target'
        async with await client.open(b'async.bin', "wb") as file:
            assert await file.write(b"value") == 5
        async with await client.open(b'async.bin', "rb") as file:
            assert await file.read() == b"value"
        await client.remove(b'async.bin')
        await client.rmdir(b'async/child')
        await client.close()

    import asyncio

    asyncio.run(scenario())
