import asyncio
import sys
from dataclasses import FrozenInstanceError
from types import ModuleType

import pytest


class SyncInner:
    version = (4, 1)
    health = {"lifecycle": "ready", "generation": 0, "lease_healthy": True}
    capabilities = {"acl": True, "named_attributes": True, "locks": True, "callbacks": True, "delegation_retention": False, "pnfs": False, "session_diagnostics": True}
    io_limits = {"max_read": 8, "max_write": 4, "preferred_read": 8, "preferred_write": 4, "read_multiple": 1, "write_multiple": 1, "preferred_directory": 8}
    closed = False

    def __init__(self): self.calls, self.xattrs = [], {}
    @classmethod
    def connect(cls, *_args, **_kwargs): return cls()
    def chmod(self, *args): self.calls.append(("chmod", *args))
    def chown(self, *args): self.calls.append(("chown", *args))
    def utime(self, *args): self.calls.append(("utime", *args))
    def truncate_path(self, *args): self.calls.append(("truncate", *args))
    def access(self, path, mode): self.calls.append(("access", path, mode)); return path != "denied"
    def getxattr(self, path, name): return self.xattrs[(path, name)]
    def setxattr(self, path, name, value): self.xattrs[(path, name)] = value; self.calls.append(("setxattr", path, name, value))
    def listxattr(self, path): return sorted(name for candidate, name in self.xattrs if candidate == path)
    def removexattr(self, path, name): del self.xattrs[(path, name)]
    def fs_info(self): return {"max_file_size": 2**40, "time_delta_ns": 1, "supports_links": True, "supports_symlinks": True, "homogeneous": False, "can_set_time": False}
    def fs_stat(self): return {"total_bytes": 1000, "free_bytes": 600, "available_bytes": 500, "total_files": 100, "free_files": 60, "available_files": 50, "invariant_seconds": 1}


class AsyncInner(SyncInner):
    @classmethod
    async def connect(cls, *_args, **_kwargs): return cls()
    async def chmod(self, *args): return super().chmod(*args)
    async def chown(self, *args): return super().chown(*args)
    async def utime(self, *args): return super().utime(*args)
    async def truncate_path(self, *args): return super().truncate_path(*args)
    async def access(self, *args): return super().access(*args)
    async def getxattr(self, *args): return super().getxattr(*args)
    async def setxattr(self, *args): return super().setxattr(*args)
    async def listxattr(self, *args): return super().listxattr(*args)
    async def removexattr(self, *args): return super().removexattr(*args)
    async def fs_info(self): return super().fs_info()
    async def fs_stat(self): return super().fs_stat()


fake = ModuleType("nfs_rs._internal")
fake.SyncClient, fake.AsyncClient = SyncInner, AsyncInner

from nfs_rs import AsyncClient, Client


@pytest.fixture(autouse=True)
def fake_adapter(monkeypatch): monkeypatch.setitem(sys.modules, "nfs_rs._internal", fake)


def test_sync_metadata_xattrs_and_immutable_information():
    client = Client.connect("nfs://server/export")
    client.chmod("a/./file", 0o640)
    client.chown("file", 10, 20)
    client.utime("file", ns=(1_000_000_002, 3_000_000_004))
    client.truncate("file", 12)
    assert client.access("file", 4)
    assert not client.access("denied", 4)
    source = bytearray(b"value")
    client.setxattr("file", "user.key", source)
    source[:] = b"other"
    assert client.getxattr("file", "user.key") == b"value"
    assert client.listxattr("file") == ["user.key"]
    client.removexattr("file", "user.key")
    assert client.fs_info().time_delta_ns == 1
    assert client.fs_stat().available_bytes == 500
    assert client.capabilities.named_attributes
    assert client.io_limits.max_write == 4
    with pytest.raises(FrozenInstanceError): client.capabilities.acl = False


def test_utime_requires_exact_ns_tuple():
    client = Client.connect("nfs://server/export")
    with pytest.raises(TypeError): client.utime("file", ns=[1, 2])
    with pytest.raises(TypeError): client.utime("file", ns=(1,))


def test_async_surface_matches_sync():
    async def scenario():
        client = await AsyncClient.connect("nfs://server/export")
        await client.chmod("file", 0o600)
        await client.chown("file", 1, 2)
        await client.utime("file", ns=(10, 20))
        await client.truncate("file", 3)
        assert await client.access("file", 6)
        await client.setxattr("file", "user.key", memoryview(b"async"))
        assert await client.getxattr("file", "user.key") == b"async"
        assert await client.listxattr("file") == ["user.key"]
        await client.removexattr("file", "user.key")
        assert (await client.fs_info()).max_file_size == 2**40
        assert (await client.fs_stat()).free_files == 60
        assert client.capabilities == Client.connect("nfs://server/export").capabilities
    asyncio.run(scenario())
