from __future__ import annotations

import asyncio
import io
import sys
from types import ModuleType

import pytest


class WriteFileInner:
    max_read_size = 4

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.closed = False
        self.position = 0
        self.writes: list[tuple[int | None, bytes]] = []
        self.flushes = 0

    def write(self, data: bytes) -> int:
        self.writes.append((None, data))
        self.position += len(data)
        return len(data)

    def write_at(self, data: bytes, offset: int) -> int:
        self.writes.append((offset, data))
        return len(data)

    def truncate(self, size: int | None) -> int:
        return self.position if size is None else size

    def flush(self) -> None:
        self.flushes += 1

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, _whence: int = 0) -> int:
        self.position = offset
        return offset

    def close(self) -> None:
        self.closed = True


class AsyncWriteFileInner(WriteFileInner):
    def __init__(self, mode: str) -> None:
        super().__init__(mode)
        self.write_entered = asyncio.Event()
        self.write_release = asyncio.Event()

    async def write(self, data: bytes) -> int:
        self.write_entered.set()
        await self.write_release.wait()
        return super().write(data)

    async def write_at(self, data: bytes, offset: int) -> int:
        return super().write_at(data, offset)

    async def truncate(self, size: int | None) -> int:
        return super().truncate(size)

    async def flush(self) -> None:
        super().flush()

    async def close(self) -> None:
        self.closed = True


class SyncClientInner:
    version = (4, 1)
    health = {"lifecycle": "ready", "generation": 0, "lease_healthy": True}
    closed = False

    @classmethod
    def connect(cls, *_args, **_kwargs):
        return cls()

    def open(self, _path: str, mode: str) -> WriteFileInner:
        return WriteFileInner(mode)


class AsyncClientInner(SyncClientInner):
    @classmethod
    async def connect(cls, *_args, **_kwargs):
        return cls()

    async def open(self, _path: str, mode: str) -> AsyncWriteFileInner:
        return AsyncWriteFileInner(mode)


fake = ModuleType("nfs_rs._internal")
fake.SyncClient = SyncClientInner
fake.AsyncClient = AsyncClientInner

from nfs_rs import AsyncClient, Client


@pytest.fixture(autouse=True)
def fake_adapter(monkeypatch):
    monkeypatch.setitem(sys.modules, "nfs_rs._internal", fake)


@pytest.mark.parametrize("mode", ["rb", "wb", "ab", "r+b", "w+b", "a+b"])
def test_selected_binary_modes_are_accepted(mode: str) -> None:
    file = Client.connect("nfs://server/export").open("file", mode)
    assert file.mode == mode


@pytest.mark.parametrize(
    "mode", ["r", "w", "a", "rt", "x", "xb", "br", "", "r++b", "rb+", "wb+", "ab+"]
)
def test_other_modes_are_rejected_before_native(mode: str) -> None:
    with pytest.raises(ValueError, match="mode must be"):
        Client.connect("nfs://server/export").open("file", mode)


def test_permissions_are_enforced_locally() -> None:
    read_only = Client.connect("nfs://server/export").open("file", "rb")
    with pytest.raises(io.UnsupportedOperation, match="not writable"):
        read_only.write(b"x")

    write_only = Client.connect("nfs://server/export").open("file", "wb")
    with pytest.raises(io.UnsupportedOperation, match="not readable"):
        write_only.read()
    with pytest.raises(TypeError):
        write_only.write(3)


def test_sync_write_snapshots_input_and_positional_write_preserves_position() -> None:
    file = Client.connect("nfs://server/export").open("file", "w+b")
    source = bytearray(b"hello")
    assert file.write(source) == 5
    source[:] = b"xxxxx"
    assert file._inner.writes == [(None, b"hello")]
    assert file.tell() == 5

    assert file.write_at(memoryview(b"XY"), 10) == 2
    assert file.tell() == 5
    assert file.truncate() == 5
    file.flush()
    assert file._inner.flushes == 1
    file.close()
    assert io.IOBase.closed.__get__(file, type(file))

    append = Client.connect("nfs://server/export").open("file", "a+b")
    with pytest.raises(io.UnsupportedOperation, match="append mode"):
        append.write_at(b"x", 0)


def test_async_write_snapshots_before_network_suspension() -> None:
    async def scenario() -> None:
        client = await AsyncClient.connect("nfs://server/export")
        file = await client.open("file", "w+b")
        source = bytearray(b"hello")
        writing = asyncio.create_task(file.write(source))
        await file._inner.write_entered.wait()
        source[:] = b"xxxxx"
        file._inner.write_release.set()
        assert await writing == 5
        assert file._inner.writes == [(None, b"hello")]

        assert await file.write_at(b"XY", 12) == 2
        assert file.tell() == 5
        assert await file.truncate(3) == 3
        await file.flush()
        assert file._inner.flushes == 1

    asyncio.run(scenario())
