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
    assert io.IOBase.closed.__get__(file, type(file))

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


def test_partial_and_zero_write_failures_preserve_position_rules() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    partial = client.open("__partial_write_error__", "w+b")
    with pytest.raises(RuntimeError):
        partial.write(b"abcde")
    assert partial.tell() == 2
    partial.close()

    zero = client.open("__zero_write__", "w+b")
    with pytest.raises(RuntimeError):
        zero.write(b"x")
    with pytest.raises(RuntimeError, match="uncertain"):
        zero.seek(0, io.SEEK_CUR)
    assert zero.seek(0, io.SEEK_SET) == 0
    zero.close()
    client.close()


def test_commit_verifier_change_keeps_flush_failed_and_close_terminal() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    file = client.open("__verifier_change__", "w+b")
    assert file.write(b"dirty") == 5
    with pytest.raises(RuntimeError, match="Uncertain") as flush_error:
        file.flush()
    with pytest.raises(RuntimeError, match="Uncertain") as first_close_error:
        file.close()
    with pytest.raises(RuntimeError, match="Uncertain") as second_close_error:
        file.close()
    assert str(first_close_error.value) == str(second_close_error.value)
    assert str(flush_error.value) == str(first_close_error.value)
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
