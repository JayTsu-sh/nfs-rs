from __future__ import annotations

import asyncio
import io
import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("NFS_RS_TEST_INSTALLED") != "1",
    reason="requires the installed native extension",
)


def test_sync_file_supports_raw_io_and_positional_reads() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    file = client.open("fixture.bin")

    assert isinstance(file, io.RawIOBase)
    assert file.mode == "rb"
    assert file.read(3) == b"abc"
    assert file.tell() == 3
    assert file.read_at(10, 5) == b"klmno"
    assert file.tell() == 3

    target = bytearray(4)
    assert file.readinto_at(target, 20) == 4
    assert target == b"uvwx"

    file.seek(0)
    buffered = io.BufferedReader(file, buffer_size=8)
    assert buffered.read() == b"abcdefghijklmnopqrstuvwxyz"
    buffered.close()
    client.close()


def test_client_close_closes_registered_sync_file() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    file = client.open("fixture.bin")
    client.close()
    assert file.closed
    for operation in (
        lambda: file.read(1),
        lambda: file.read_at(0, 1),
        lambda: file.readinto(bytearray(1)),
        lambda: file.readinto_at(bytearray(1), 0),
        lambda: file.seek(0),
        file.tell,
    ):
        with pytest.raises(ValueError, match="closed file"):
            operation()


def test_open_rejects_non_binary_modes() -> None:
    from nfs_rs import Client

    client = Client.connect("nfs-test://fixture/export")
    with pytest.raises(ValueError, match="mode must be"):
        client.open("fixture.bin", "r")
    client.close()


def test_async_file_has_relative_and_positional_parity() -> None:
    from nfs_rs import AsyncClient

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        file = await client.open("fixture.bin")

        assert await file.read(3) == b"abc"
        assert file.tell() == 3
        left, right = await asyncio.gather(file.read_at(0, 4), file.read_at(4, 4))
        assert (left, right) == (b"abcd", b"efgh")
        assert file.tell() == 3

        target = bytearray(5)
        assert await file.readinto_at(target, 10) == 4
        assert target == b"klmn\0"
        await client.close()
        assert file.closed
        for operation in (
            lambda: file.read(1),
            lambda: file.read_at(0, 1),
            lambda: file.readinto(bytearray(1)),
            lambda: file.readinto_at(bytearray(1), 0),
            lambda: file.seek(0),
        ):
            with pytest.raises(ValueError, match="closed file"):
                await operation()
        with pytest.raises(ValueError, match="closed file"):
            file.tell()

    asyncio.run(scenario())


def test_cancelled_open_finishes_registration_under_client_ownership() -> None:
    from nfs_rs import AsyncClient
    from nfs_rs import _internal

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        _internal._arm_open_test_barrier()
        opening = asyncio.create_task(client.open("__blocked_open__"))
        await _internal._wait_open_test_entered()
        opening.cancel()
        with pytest.raises(asyncio.CancelledError):
            await opening

        _internal._release_open_test_barrier()
        await _internal._wait_open_test_registered()
        await client.close()
        assert client.closed

    asyncio.run(scenario())
