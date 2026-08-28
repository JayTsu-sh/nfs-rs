from __future__ import annotations

import os

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NFS_RS_TEST_INSTALLED") != "1",
    reason="requires the non-editable test-support wheel",
)


def test_sync_namespace_and_whole_file_conveniences() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    client.mkdir("parent/child", parents=True)
    client.rename("old", "new")
    client.link("new", "hard")
    client.symlink("../raw/target", "sym")
    assert client.readlink("sym") == "../target"
    assert client.write_bytes("whole.bin", b"contents") == 8
    assert client.read_bytes("whole.bin") == b"contents"
    client.touch("empty.bin")
    client.remove("whole.bin")
    client.rmdir("parent/child")
    client.close()


def test_namespace_faults_preserve_before_and_after_send_outcomes() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    with pytest.raises(RuntimeError, match="DefiniteFailure"):
        client.remove("__before_send__")
    with pytest.raises(RuntimeError, match="Uncertain"):
        client.rename("__after_send__", "destination")
    with pytest.raises(NotADirectoryError):
        client.remove("__notdir__")
    with pytest.raises(IsADirectoryError):
        client.remove("__isdir__")
    with pytest.raises(OSError) as not_empty:
        client.rmdir("__notempty__")
    assert not_empty.value.errno == 39
    client.close()


def test_async_namespace_twins() -> None:
    from nfs_rs import AsyncClient

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        await client.mkdir("async/child", parents=True)
        await client.rename("from", "to")
        await client.link("to", "hard")
        await client.symlink("../../target", "sym")
        assert await client.readlink("sym") == "../target"
        assert await client.write_bytes("async.bin", b"value") == 5
        assert await client.read_bytes("async.bin") == b"value"
        await client.remove("async.bin")
        await client.rmdir("async/child")
        await client.close()

    import asyncio

    asyncio.run(scenario())
