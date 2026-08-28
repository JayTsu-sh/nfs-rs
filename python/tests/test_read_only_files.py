import asyncio
import io
import sys
import threading
from types import ModuleType

import pytest


DATA = b"abcdefghijklmnopqrstuvwxyz"


class SyncFileInner:
    def __init__(self):
        self.position = 0
        self.closed = False
        self.lock = threading.Lock()

    def read(self, size=-1):
        with self.lock:
            end = len(DATA) if size < 0 else min(len(DATA), self.position + size)
            result = DATA[self.position:end]
            self.position = end
            return result

    def read_at(self, offset, size=-1):
        end = len(DATA) if size < 0 else min(len(DATA), offset + size)
        return DATA[offset:end]

    def seek(self, offset, whence=io.SEEK_SET):
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self.position, io.SEEK_END: len(DATA)}[whence]
        position = base + offset
        if position < 0:
            raise ValueError("negative seek position")
        self.position = position
        return position

    def tell(self):
        return self.position

    def close(self):
        self.closed = True


class AsyncFileInner(SyncFileInner):
    async def read(self, size=-1):
        return super().read(size)

    async def read_at(self, offset, size=-1):
        await asyncio.sleep(0)
        return super().read_at(offset, size)

    async def seek(self, offset, whence=io.SEEK_SET):
        return super().seek(offset, whence)

    async def close(self):
        self.closed = True


class SyncClientInner:
    version = (4, 1)
    health = {"lifecycle": "ready", "generation": 0, "lease_healthy": True}
    closed = False

    @classmethod
    def connect(cls, *_args, **_kwargs):
        return cls()

    def open(self, _path, _mode):
        return SyncFileInner()

    def close(self):
        self.closed = True


class AsyncClientInner(SyncClientInner):
    @classmethod
    async def connect(cls, *_args, **_kwargs):
        return cls()

    async def open(self, _path, _mode):
        return AsyncFileInner()

    async def close(self):
        self.closed = True


fake = ModuleType("nfs_rs._internal")
fake.SyncClient = SyncClientInner
fake.AsyncClient = AsyncClientInner

from nfs_rs import AsyncClient, AsyncFile, Client, File


@pytest.fixture(autouse=True)
def fake_adapter(monkeypatch):
    monkeypatch.setitem(sys.modules, "nfs_rs._internal", fake)


@pytest.mark.parametrize("mode", ["r", "rt", "wb", "r+b", ""])
def test_open_rejects_every_mode_except_rb_before_native(mode):
    client = Client.connect("nfs://server/export")
    with pytest.raises(ValueError, match="rb"):
        client.open("file", mode)


def test_sync_file_is_raw_io_and_composes_with_buffered_reader():
    client = Client.connect("nfs://server/export")
    raw = client.open("folder/../file", "rb")
    assert isinstance(raw, io.RawIOBase)
    assert isinstance(raw, File)
    assert raw.name == "file"
    assert raw.readable() and raw.seekable() and not raw.writable()
    buffered = io.BufferedReader(raw, buffer_size=4)
    assert buffered.read(5) == b"abcde"
    assert buffered.read() == DATA[5:]
    buffered.close()
    assert raw.closed


def test_sync_relative_and_positional_reads_keep_separate_positions():
    with Client.connect("nfs://server/export").open("file") as file:
        assert file.read(3) == b"abc"
        assert file.tell() == 3
        assert file.read_at(10, 3) == b"klm"
        assert file.tell() == 3
        assert file.seek(-1, io.SEEK_CUR) == 2
        assert file.read(2) == b"cd"
        target = bytearray(3)
        assert file.readinto_at(target, 20) == 3
        assert target == b"uvw"
        assert file.tell() == 4


def test_sync_readinto_revalidates_target_after_network_work():
    file = Client.connect("nfs://server/export").open("file")
    target = bytearray(4)
    original_read = file._inner.read

    def resizing_read(size):
        target.extend(b"x")
        return original_read(size)

    file._inner.read = resizing_read
    with pytest.raises(BufferError, match="changed size"):
        file.readinto(target)


def test_async_file_matches_read_seek_and_positional_contract():
    async def scenario():
        client = await AsyncClient.connect("nfs://server/export")
        async with await client.open("file") as file:
            assert isinstance(file, AsyncFile)
            assert await file.read(3) == b"abc"
            assert file.tell() == 3
            first, second = await asyncio.gather(file.read_at(10, 3), file.read_at(20, 3))
            assert (first, second) == (b"klm", b"uvw")
            assert file.tell() == 3
            assert await file.seek(-1, io.SEEK_END) == len(DATA) - 1
            target = bytearray(2)
            assert await file.readinto(target) == 1
            assert target[:1] == b"z"
        assert file.closed

    asyncio.run(scenario())


def test_async_readinto_detects_resize_during_suspension():
    async def scenario():
        client = await AsyncClient.connect("nfs://server/export")
        file = await client.open("file")
        target = bytearray(4)
        original_read = file._inner.read

        async def resizing_read(size):
            await asyncio.sleep(0)
            target.extend(b"x")
            return await original_read(size)

        file._inner.read = resizing_read
        with pytest.raises(BufferError, match="changed size"):
            await file.readinto(target)

    asyncio.run(scenario())
