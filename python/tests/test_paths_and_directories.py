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

    def scandir(self, _path):
        yield {"name": "first", "info": dict(INFO)}
        yield {"name": "second", "info": {**INFO, "fileid": 10}}


class AsyncInner(SyncInner):
    @classmethod
    async def connect(cls, _url, **_options):
        return cls()

    async def close(self):
        self.closed = True

    async def stat(self, path):
        return super().stat(path)

    async def scandir(self, _path):
        yield {"name": "first", "info": dict(INFO)}
        yield {"name": "second", "info": {**INFO, "fileid": 10}}


fake = ModuleType("nfs_rs._internal")
fake.SyncClient = SyncInner
fake.AsyncClient = AsyncInner
fake.list_exports = lambda *_args, **_kwargs: [{"path": "/data", "groups": ["team"]}]


async def async_exports(*_args, **_kwargs):
    return [{"path": "/data", "groups": ["team"]}]


fake.async_list_exports = async_exports

from nfs_rs import AsyncClient, Client, FileType, async_list_exports, list_exports


@pytest.fixture(autouse=True)
def fake_adapter(monkeypatch):
    monkeypatch.setitem(sys.modules, "nfs_rs._internal", fake)


@pytest.mark.parametrize("value", ["a/./b", "/a/b", Path("a/b"), "a/c/../b"])
def test_paths_normalize_with_export_relative_posix_semantics(value):
    client = Client.connect("nfs://server/export")
    assert client.stat(value).type is FileType.FILE


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
    assert asyncio.run(async_list_exports("nfs://server/")) == list_exports("nfs://server/")
