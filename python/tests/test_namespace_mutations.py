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
        if path.startswith("existing-"): raise FileExistsError(path)
    def remove(self, path): self.calls.append(("remove", path))
    def rmdir(self, path): self.calls.append(("rmdir", path))
    def rename(self, source, destination): self.calls.append(("rename", source, destination))
    def link(self, source, destination): self.calls.append(("link", source, destination))
    def symlink(self, target, link_path): self.calls.append(("symlink", target, link_path))
    def readlink(self, path): self.calls.append(("readlink", path)); return "../raw-target"
    def stat(self, path):
        if path == "existing-dir":
            return {"type": "directory", "mode": 0o755, "nlink": 1, "uid": 0, "gid": 0, "size": 0, "used": 0, "fsid": 1, "fileid": 1, "atime_ns": 0, "mtime_ns": 0, "ctime_ns": 0}
        if path == "existing-file":
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
    async def stat(self, path): return super().stat(path)
    async def open(self, path, mode): self.calls.append(("open", path, mode)); return AsyncFile(self, path, mode)


fake = ModuleType("nfs_rs._internal")
fake.SyncClient, fake.AsyncClient = SyncInner, AsyncInner

from nfs_rs import AsyncClient, Client


@pytest.fixture(autouse=True)
def fake_adapter(monkeypatch): monkeypatch.setitem(sys.modules, "nfs_rs._internal", fake)


def test_sync_namespace_paths_targets_and_conveniences():
    client = Client.connect("nfs://server/export")
    client.mkdir("a/./b/c", parents=True)
    assert client._inner.calls[:3] == [("mkdir", "a", 0o777), ("mkdir", "a/b", 0o777), ("mkdir", "a/b/c", 0o777)]
    client.rename("a/../old", "/new")
    client.link("new", "links/item")
    client.symlink("../raw/./target", "links/sym")
    assert ("symlink", "../raw/./target", "links/sym") in client._inner.calls
    assert client.readlink("links/./sym") == "../raw-target"
    assert client.write_bytes("data", bytearray(b"value")) == 5
    assert client.read_bytes("data") == b"value"
    client.touch("created")
    assert ("open", "created", "ab") in client._inner.calls


def test_symlink_target_validates_text_without_normalizing():
    client = Client.connect("nfs://server/export")
    with pytest.raises(TypeError): client.symlink(b"target", "link")
    with pytest.raises(ValueError): client.symlink("bad\0target", "link")
    with pytest.raises(ValueError): client.symlink("target", "../escape")


def test_mkdir_suppresses_exists_only_for_a_confirmed_directory():
    client = Client.connect("nfs://server/export")
    client.mkdir("existing-dir", exist_ok=True)
    with pytest.raises(FileExistsError):
        client.mkdir("existing-dir", parents=True)
    with pytest.raises(FileExistsError):
        client.mkdir("existing-file", exist_ok=True)
    with pytest.raises(NotImplementedError, match="exclusive create"):
        client.touch("new", exist_ok=False)


def test_async_surface_matches_sync():
    async def scenario():
        client = await AsyncClient.connect("nfs://server/export")
        await client.mkdir("a/b", parents=True)
        await client.rename("a", "b")
        await client.link("b", "hard")
        await client.symlink("../target", "sym")
        assert await client.readlink("sym") == "../raw-target"
        assert await client.write_bytes("data", memoryview(b"async")) == 5
        assert await client.read_bytes("data") == b"async"
        await client.remove("data")
        await client.rmdir("a/b")
    asyncio.run(scenario())
