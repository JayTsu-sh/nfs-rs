import asyncio
import sys
from types import ModuleType

import pytest


class FakeSyncClient:
    version = (4, 1)
    health = {"lifecycle": "ready", "generation": 7, "lease_healthy": True}
    fail_close = False

    def __init__(self):
        self.closed = False

    @classmethod
    def connect(cls, _url, **_options):
        cls.last_url = _url
        return cls()

    def close(self):
        if self.fail_close:
            raise RuntimeError("cleanup failed")
        self.closed = True


class FakeAsyncClient(FakeSyncClient):
    @classmethod
    async def connect(cls, _url, **_options):
        return cls()

    async def close(self):
        if self.fail_close:
            raise RuntimeError("cleanup failed")
        self.closed = True


fake = ModuleType("nfs_rs._internal")
fake.SyncClient = FakeSyncClient
fake.AsyncClient = FakeAsyncClient

from nfs_rs import AsyncClient, Client, Health, Lifecycle, Version


@pytest.fixture(autouse=True)
def fake_adapter(monkeypatch):
    FakeSyncClient.fail_close = False
    FakeAsyncClient.fail_close = False
    monkeypatch.setitem(sys.modules, "nfs_rs._internal", fake)


def test_sync_connect_inspect_and_context_close():
    with Client.connect("nfs://server/export", versions=["4.1"]) as client:
        assert client.version == Version.NFS_V4_1
        assert client.health == Health(Lifecycle.READY, 7, True)
        assert "server" not in repr(client)
        assert not client.closed
    assert client.closed
    client.close()
    assert "version=4.1" in FakeSyncClient.last_url


def test_async_connect_inspect_and_context_close():
    async def scenario():
        async with await AsyncClient.connect("nfs://server/export", versions=["4.1"]) as client:
            assert client.version == Version.NFS_V4_1
            assert client.health.lifecycle is Lifecycle.READY
            assert not client.closed
        assert client.closed
        await client.close()

    asyncio.run(scenario())


def test_sync_factory_rejects_invalid_options_before_adapter():
    with pytest.raises(ValueError, match="versions"):
        Client.connect("nfs://server/export", versions=["4.2"])


def test_async_factory_rejects_invalid_options_before_adapter():
    async def scenario():
        with pytest.raises(ValueError, match="versions"):
            await AsyncClient.connect("nfs://server/export", versions=["4.2"])

    asyncio.run(scenario())


def test_sync_context_preserves_body_exception_when_close_also_fails():
    FakeSyncClient.fail_close = True
    with pytest.raises(ValueError, match="body failed") as raised:
        with Client.connect("nfs://server/export"):
            raise ValueError("body failed")
    assert any("cleanup also failed" in note for note in raised.value.__notes__)


def test_async_context_preserves_body_exception_when_close_also_fails():
    async def scenario():
        FakeAsyncClient.fail_close = True
        with pytest.raises(ValueError, match="body failed") as raised:
            async with await AsyncClient.connect("nfs://server/export"):
                raise ValueError("body failed")
        assert any("cleanup also failed" in note for note in raised.value.__notes__)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "options, message",
    [
        ({"readdir_buffer": (1, 0)}, "readdir_buffer"),
        ({"readdir_buffer": (1, 2, 3)}, "readdir_buffer"),
        ({"operation_timeout": 0}, "operation_timeout"),
    ],
)
def test_connection_option_validation_is_shared(options, message):
    with pytest.raises(ValueError, match=message):
        Client.connect("nfs://server/export", **options)
