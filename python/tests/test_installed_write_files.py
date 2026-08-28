from __future__ import annotations

import asyncio
import io
import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("NFS_RS_TEST_INSTALLED") != "1",
    reason="requires the installed native extension",
)


def test_sync_complete_write_append_positional_and_buffered_io() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    file = client.open("fixture.bin", "w+b")
    assert file.write(b"abcdefghij") == 10
    assert file.tell() == 10
    assert file.write_at(b"XY", 2) == 2
    assert file.tell() == 10
    file.flush()
    file.seek(0)
    assert file.read() == b"abXYefghij"
    assert file.truncate(5) == 5
    file.close()

    appended = client.open("fixture.bin", "a+b")
    start = appended.tell()
    assert appended.write(b"!") == 1
    assert appended.tell() == start + 1
    appended.close()
    verify = client.open("fixture.bin", "rb")
    assert verify.read() == b"abXYe!"
    verify.close()

    buffered_raw = client.open("buffered.bin", "w+b")
    buffered = io.BufferedRandom(buffered_raw, buffer_size=8)
    assert buffered.write(b"buffered payload") == 16
    buffered.seek(0)
    assert buffered.read() == b"buffered payload"
    buffered.close()
    client.close()


def test_native_permissions_are_enforced_even_without_facade() -> None:
    from nfs_rs import _internal

    client = _internal.SyncClient.connect("nfs-test://fixture/export")
    read_only = client.open("fixture.bin", "rb")
    with pytest.raises(RuntimeError, match="not writable"):
        read_only.write(b"x")
    write_only = client.open("fixture.bin", "wb")
    with pytest.raises(RuntimeError, match="not readable"):
        write_only.read()
    client.close()


def test_failed_close_reuses_terminal_flush_error() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    file = client.open("__commit_error__", "w+b")
    assert file.write(b"dirty") == 5
    with pytest.raises(RuntimeError, match="scripted commit failure"):
        file.close()
    with pytest.raises(RuntimeError, match="scripted commit failure"):
        file.close()
    client.close()


def test_async_write_positional_flush_and_truncate() -> None:
    from nfs_rs import AsyncClient

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        file = await client.open("fixture.bin", "w+b")
        assert await file.write(b"abcdefghij") == 10
        await file.seek(0)
        assert await file.read() == b"abcdefghij"

        assert await file.write_at(b"YZ", 3) == 2
        assert file.tell() == 10
        await file.flush()
        assert await file.truncate(6) == 6
        await file.close()
        await client.close()

    asyncio.run(scenario())
