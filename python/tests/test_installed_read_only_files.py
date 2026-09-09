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
        assert await file.readinto_at(target, 10) == 5
        assert target == b"klmno"
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


@pytest.mark.parametrize("offset, size, expected", [(0, 26, 26), (3, 40, 23), (26, 12, 0), (0, 0, 0)])
def test_native_readinto_fills_all_chunks_and_preserves_eof_tail(offset, size, expected):
    from nfs_rs import Client
    with Client.connect("nfs-test://fixture/export") as client:
        with client.open("fixture.bin") as file:
            target = bytearray(b"!" * size)
            assert file.readinto_at(target, offset) == expected
            assert target[:expected] == b"abcdefghijklmnopqrstuvwxyz"[offset:offset + expected]
            assert target[expected:] == b"!" * (size - expected)
            assert file.tell() == 0
            file.seek(offset)
            assert file.readinto(target) == expected
            assert file.tell() == offset + expected
            target.extend(b"released")


def test_native_readinto_accepts_typed_and_sliced_writable_buffers():
    from array import array
    from nfs_rs import Client
    with Client.connect("nfs-test://fixture/export") as client:
        with client.open("fixture.bin") as file:
            target = array("I", [0] * 4)
            assert file.readinto(target) == 16
            assert target.tobytes() == b"abcdefghijklmnop"
            backing = bytearray(b"!" * 20)
            with memoryview(backing)[2:18] as view:
                assert file.readinto_at(view, 0) == 16
            assert backing == b"!!abcdefghijklmnop!!"
            for invalid in (b"immutable", memoryview(backing)[::2]):
                with pytest.raises(TypeError):
                    file.readinto(invalid)


def test_cancelled_readinto_releases_buffer_and_prevents_late_writes():
    from nfs_rs import AsyncClient, _internal
    async def scenario():
        async with await AsyncClient.connect("nfs-test://fixture/export") as client:
            async with await client.open("fixture.bin") as file:
                target = bytearray(b"!" * 20)
                _internal._arm_operation_test_barrier("readinto")
                task = asyncio.create_task(file.readinto(target))
                try:
                    await _internal._wait_operation_test_entered()
                    with pytest.raises(BufferError):
                        target.extend(b"blocked")
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                    target.extend(b"released")
                    settled = _internal._wait_operation_test_settled()
                finally:
                    _internal._release_operation_test_barrier()
                await settled
                assert target == b"!" * 20 + b"released"
                assert file.tell() == 0
    asyncio.run(scenario())


def test_timed_out_readinto_prevents_late_writes():
    from nfs_rs import AsyncClient, _internal
    async def scenario():
        async with await AsyncClient.connect("nfs-test://fixture/export", operation_timeout=0.05) as client:
            async with await client.open("fixture.bin") as file:
                target = bytearray(b"!" * 20)
                _internal._arm_operation_test_barrier("readinto")
                task = asyncio.create_task(file.readinto_at(target, 0))
                try:
                    await _internal._wait_operation_test_entered()
                    with pytest.raises(TimeoutError):
                        await task
                    target.extend(b"released")
                finally:
                    _internal._release_operation_test_barrier()
                # File close waits for the abandoned read to settle.
                await file.close()
                assert target == b"!" * 20 + b"released"
    asyncio.run(scenario())


@pytest.mark.parametrize("size", [0, 3, 4, 5, 32, 40, 4096, -1])
def test_bytes_reads_across_chunks_and_eof(size: int) -> None:
    from nfs_rs import Client

    payload = bytes(range(251)) * 3  # Test server negotiates four-byte chunks.
    with Client.connect("nfs-test://fixture/export") as client:
        assert not hasattr(client, "read_bytes")
        assert not hasattr(client, "write_bytes")
        with client.open("read-chunks.bin", "w+b") as file:
            file.write(payload)
            file.seek(2)
            expected = payload[2:] if size == -1 else payload[2:2 + size]
            assert file.read_at(2, size) == expected
            assert file.tell() == 2
            assert file.read(size) == expected
            assert file.tell() == 2 + len(expected)
            assert file.read_at(len(payload), size) == b""


@pytest.mark.parametrize("size", [0, 3, 4, 5, 32, 40, 4096, -1])
def test_async_bytes_reads_across_chunks_and_eof(size: int) -> None:
    from nfs_rs import AsyncClient

    async def scenario() -> None:
        payload = bytes(range(251)) * 3
        async with await AsyncClient.connect("nfs-test://fixture/export") as client:
            assert not hasattr(client, "read_bytes")
            assert not hasattr(client, "write_bytes")
            async with await client.open("async-read-chunks.bin", "w+b") as file:
                await file.write(payload)
                await file.seek(2)
                expected = payload[2:] if size == -1 else payload[2:2 + size]
                assert await file.read_at(2, size) == expected
                assert file.tell() == 2
                assert await file.read(size) == expected
                assert file.tell() == 2 + len(expected)
                assert await file.read_at(len(payload), size) == b""

    asyncio.run(scenario())
