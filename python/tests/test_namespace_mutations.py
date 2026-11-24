import asyncio
import sys
from types import ModuleType

import pytest


class FakeFile:
    max_read_size = 1024

    def __init__(self, owner, path, mode):
        self.owner, self.path, self.mode = owner, path, mode
        self.closed = False

    def write(self, data):
        value = bytes(data)
        self.owner.data[self.path] = value
        return len(value)

    def read(self, _size=-1):
        return self.owner.data.get(self.path, b"")

    def tell(self): return 0
    def flush(self): pass
    def close(self): self.closed = True


class SyncInner:
    version = (3, None)
    health = {"lifecycle": "ready", "generation": 0, "lease_healthy": None}
    closed = False

    def __init__(self):
        self.calls, self.data = [], {}

    @classmethod
    def connect(cls, *_args, **_kwargs): return cls()
    def mkdir(self, path, mode):
        self.calls.append(("mkdir", path, mode))
        if path.startswith(b"existing-"): raise FileExistsError(path)
    def remove(self, path): self.calls.append(("remove", path))
    def rmdir(self, path): self.calls.append(("rmdir", path))
    def rename(self, source, destination): self.calls.append(("rename", source, destination))
    def link(self, source, destination): self.calls.append(("link", source, destination))
    def symlink(self, target, link_path): self.calls.append(("symlink", target, link_path))
    def readlink(self, path): self.calls.append(("readlink", path)); return b"../raw-target"
    def utime(self, path, atime_ns, mtime_ns): self.calls.append(("utime", path, atime_ns, mtime_ns))
    def stat(self, path):
        if path == b'existing-dir':
            return {"type": "directory", "mode": 0o755, "nlink": 1, "uid": 0, "gid": 0, "size": 0, "used": 0, "fsid": 1, "fileid": 1, "atime_ns": 0, "mtime_ns": 0, "ctime_ns": 0}
        if path == b'existing-file':
            return {"type": "file", "mode": 0o644, "nlink": 1, "uid": 0, "gid": 0, "size": 0, "used": 0, "fsid": 1, "fileid": 2, "atime_ns": 0, "mtime_ns": 0, "ctime_ns": 0}
        raise FileNotFoundError(path)
    def open(self, path, mode): self.calls.append(("open", path, mode)); return FakeFile(self, path, mode)


class AsyncFile(FakeFile):
    async def write(self, data): return super().write(data)
    async def read(self, _size=-1): return super().read(_size)
    async def flush(self): pass
    async def close(self): self.closed = True


class AsyncInner(SyncInner):
    @classmethod
    async def connect(cls, *_args, **_kwargs): return cls()
    async def mkdir(self, *args): return super().mkdir(*args)
    async def remove(self, *args): return super().remove(*args)
    async def rmdir(self, *args): return super().rmdir(*args)
    async def rename(self, *args): return super().rename(*args)
    async def link(self, *args): return super().link(*args)
    async def symlink(self, *args): return super().symlink(*args)
    async def readlink(self, *args): return super().readlink(*args)
    async def utime(self, *args): return super().utime(*args)
    async def stat(self, path): return super().stat(path)
    async def open(self, path, mode): self.calls.append(("open", path, mode)); return AsyncFile(self, path, mode)


fake = ModuleType("nfs_rs._internal")
fake.SyncClient, fake.AsyncClient = SyncInner, AsyncInner

from nfs_rs import AsyncClient, Client


@pytest.fixture(autouse=True)
def fake_adapter(monkeypatch): monkeypatch.setitem(sys.modules, "nfs_rs._internal", fake)


def test_sync_namespace_paths_targets_and_conveniences():
    client = Client.connect("nfs://server/export")
    client.mkdir(b'a/./b/c', parents=True)
    assert client._inner.calls[:3] == [("mkdir", b"a", 0o777), ("mkdir", b'a/b', 0o777), ("mkdir", b"a/b/c", 0o777)]
    client.rename(b'a/../old', b'/new')
    client.link(b'new', b'links/item')
    client.symlink(b'../raw/./target', b'links/sym')
    assert ("symlink", b'../raw/./target', b'links/sym') in client._inner.calls
    assert client.readlink(b'links/./sym') == b'../raw-target'
    with client.open(b'data', "wb") as file:
        assert file.write(bytearray(b"value")) == 5
    with client.open(b'data', "rb") as file:
        assert file.read() == b"value"
    client.touch(b'created')
    assert ("open", b'created', "ab") in client._inner.calls


def test_symlink_target_validates_text_without_normalizing():
    client = Client.connect("nfs://server/export")
    with pytest.raises(TypeError): client.symlink("target", b'link')
    with pytest.raises(ValueError): client.symlink(b'bad\x00target', b'link')
    with pytest.raises(ValueError): client.symlink(b'target', b'../escape')


def test_mkdir_suppresses_exists_only_for_a_confirmed_directory():
    client = Client.connect("nfs://server/export")
    client.mkdir(b'existing-dir', exist_ok=True)
    with pytest.raises(FileExistsError):
        client.mkdir(b'existing-dir', parents=True)
    with pytest.raises(FileExistsError):
        client.mkdir(b'existing-file', exist_ok=True)
    with pytest.raises(NotImplementedError, match="exclusive create"):
        client.touch(b'new', exist_ok=False)


def test_async_surface_matches_sync():
    async def scenario():
        client = await AsyncClient.connect("nfs://server/export")
        await client.mkdir(b'a/b', parents=True)
        await client.rename(b'a', b'b')
        await client.link(b'b', b'hard')
        await client.symlink(b'../target', b'sym')
        assert await client.readlink(b'sym') == b'../raw-target'
        async with await client.open(b'data', "wb") as file:
            assert await file.write(memoryview(b"async")) == 5
        async with await client.open(b'data', "rb") as file:
            assert await file.read() == b"async"
        await client.remove(b'data')
        await client.rmdir(b'a/b')
    asyncio.run(scenario())
