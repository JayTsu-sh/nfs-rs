import asyncio
import sys
from pathlib import Path
from types import ModuleType

import pytest


INFO = {
    "type": "file",
    "mode": 0o644,
    "nlink": 1,
    "uid": 1000,
    "gid": 1000,
    "size": 12,
    "used": 512,
    "fsid": 7,
    "fileid": 9,
    "atime_ns": 1_000_000_002,
    "mtime_ns": 3_000_000_004,
    "ctime_ns": 5_000_000_006,
}


class SyncInner:
    version = (3, None)
    health = {"lifecycle": "ready", "generation": 0, "lease_healthy": None}
    closed = False

    @classmethod
    def connect(cls, _url, **_options):
        return cls()

    def close(self):
        self.closed = True

    def stat(self, path):
        if path == "missing":
            raise FileNotFoundError(path)
        if path == "denied":
            raise PermissionError(path)
        return dict(INFO)

    def scandir(self, _path, fh=None):
        self.last_scan = (_path, fh)
        yield [{"name": "first", "info": dict(INFO), "fh": b"child-fh\0"}]
        if _path == "denied-directory":
            raise PermissionError(_path)
        yield [{"name": "second", "info": {**INFO, "fileid": 10}}]


class AsyncInner(SyncInner):
    @classmethod
    async def connect(cls, _url, **_options):
        return cls()

    async def close(self):
        self.closed = True

    async def stat(self, path):
        return super().stat(path)

    async def scandir(self, _path, fh=None):
        self.last_scan = (_path, fh)
        yield [{"name": "first", "info": dict(INFO), "fh": b"child-fh\0"}]
        if _path == "denied-directory":
            raise PermissionError(_path)
        yield [{"name": "second", "info": {**INFO, "fileid": 10}}]


fake = ModuleType("nfs_rs._internal")
fake.SyncClient = SyncInner
fake.AsyncClient = AsyncInner
fake.list_exports = lambda *_args, **_kwargs: [{"path": "/data", "groups": ["team"]}]


async def async_exports(*_args, **_kwargs):
    return [{"path": "/data", "groups": ["team"]}]


fake.async_list_exports = async_exports

from nfs_rs import AsyncClient, Client, FileType, list_exports, list_exports_async


@pytest.fixture(autouse=True)
def fake_adapter(monkeypatch):
    monkeypatch.setitem(sys.modules, "nfs_rs._internal", fake)


@pytest.mark.parametrize("value", ["a/./b", "/a/b", Path("a/b"), "a/c/../b"])
def test_paths_normalize_with_export_relative_posix_semantics(value):
    client = Client.connect("nfs://server/export")
    info = client.stat(value)
    assert info.type is FileType.FILE
    assert info.path == "a/b"
    assert (info.atime, info.mtime, info.ctime) == (1_000_000_002, 3_000_000_004, 5_000_000_006)
    assert info.owner is None
    assert info.group is None


@pytest.mark.parametrize("value", [b"bytes", "bad\0name", "../escape", "a/../../escape"])
def test_invalid_paths_fail_before_adapter(value):
    client = Client.connect("nfs://server/export")
    with pytest.raises((TypeError, ValueError)):
        client.stat(value)


def test_exists_suppresses_only_not_found():
    client = Client.connect("nfs://server/export")
    assert not client.exists("missing")
    with pytest.raises(PermissionError):
        client.exists("denied")


def test_sync_scandir_is_lazy_and_entries_carry_metadata():
    client = Client.connect("nfs://server/export")
    entries = client.scandir("folder")
    first = next(entries)
    assert first.name == "first"
    assert first.path == "folder/first"
    assert first.info.path == first.path
    assert first.info.fileid == 9
    assert [entry.name for entry in entries] == ["second"]
    assert client.listdir("folder") == ["first", "second"]


def test_async_operations_match_sync_contract():
    async def scenario():
        client = await AsyncClient.connect("nfs://server/export")
        assert (await client.stat("a/../file")).fileid == 9
        assert not await client.exists("missing")
        assert [entry.name async for entry in client.scandir("folder")] == ["first", "second"]
        assert await client.listdir("folder") == ["first", "second"]

    asyncio.run(scenario())


def test_export_discovery_has_matching_sync_and_async_values():
    assert list_exports("nfs://server/")[0].groups == ("team",)
    assert asyncio.run(list_exports_async("nfs://server/")) == list_exports("nfs://server/")


def test_sync_and_async_permission_and_mid_stream_errors_match():
    sync_client = Client.connect("nfs://server/export")
    with pytest.raises(PermissionError):
        sync_client.stat("denied")
    sync_entries = sync_client.scandir("denied-directory")
    assert next(sync_entries).name == "first"
    with pytest.raises(PermissionError):
        next(sync_entries)

    async def scenario():
        client = await AsyncClient.connect("nfs://server/export")
        with pytest.raises(PermissionError):
            await client.stat("denied")
        entries = client.scandir("denied-directory")
        assert (await anext(entries)).name == "first"
        with pytest.raises(PermissionError):
            await anext(entries)

    asyncio.run(scenario())


@pytest.mark.parametrize("fh", [None, b"opaque\0directory-fh"])
def test_scandir_accepts_directory_reference_and_preserves_child_handles(fh):
    from nfs_rs import DirectoryRef
    client = Client.connect("nfs://server/export")
    entries = list(client.scandir(DirectoryRef(Path("a/./b"), fh)))
    assert client._inner.last_scan == ("a/b", fh)
    assert entries[0].fh == b"child-fh\0"
    assert entries[0].path == "a/b/first"
    list(client.scandir(entries[0]))
    assert client._inner.last_scan == ("a/b/first", b"child-fh\0")
    assert entries[1].fh is None
    list(client.scandir(entries[1]))
    assert client._inner.last_scan == ("a/b/second", None)


def test_async_scandir_accepts_references_and_returned_entries():
    from nfs_rs import DirectoryRef
    async def scenario():
        client = await AsyncClient.connect("nfs://server/export")
        for fh in (None, b"opaque-fh"):
            entries = [entry async for entry in client.scandir(DirectoryRef("a/./b", fh))]
            assert client._inner.last_scan == ("a/b", fh)
            assert entries[0].fh == b"child-fh\0"
            children = [entry async for entry in client.scandir(entries[0])]
            assert client._inner.last_scan == ("a/b/first", b"child-fh\0")
            assert children[0].path == "a/b/first/first"
    asyncio.run(scenario())


@pytest.mark.parametrize("fh, error", [(b"", ValueError), ("text", TypeError), (123, TypeError)])
def test_scandir_rejects_invalid_handles(fh, error):
    from nfs_rs import DirectoryRef
    client = Client.connect("nfs://server/export")
    with pytest.raises(error, match="fh"):
        client.scandir(DirectoryRef("folder", fh))
    async def scenario():
        client = await AsyncClient.connect("nfs://server/export")
        with pytest.raises(error, match="fh"):
            _ = [entry async for entry in client.scandir(DirectoryRef("folder", fh))]
    asyncio.run(scenario())
