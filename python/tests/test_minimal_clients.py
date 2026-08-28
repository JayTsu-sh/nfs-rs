import asyncio
import sys
from types import ModuleType

import pytest


class FakeSyncClient:
    version = (4, 1)
    health = {"lifecycle": "ready", "generation": 7, "lease_healthy": True}

    def __init__(self):
        self.closed = False

    @classmethod
    def connect(cls, _url, **_options):
        cls.last_url = _url
        return cls()

    def close(self):
        self.closed = True


class FakeAsyncClient(FakeSyncClient):
    @classmethod
    async def connect(cls, _url, **_options):
        return cls()

    async def close(self):
        self.closed = True


fake = ModuleType("nfs_rs._internal")
fake.SyncClient = FakeSyncClient
fake.AsyncClient = FakeAsyncClient

from nfs_rs import AsyncClient, Client, Health, Version


@pytest.fixture(autouse=True)
def fake_adapter(monkeypatch):
    monkeypatch.setitem(sys.modules, "nfs_rs._internal", fake)


def test_sync_connect_inspect_and_context_close():
    with Client.connect("nfs://server/export", versions=["4.1"]) as client:
        assert client.version == Version(4, 1)
        assert client.health == Health("ready", 7, True)
        assert "server" not in repr(client)
        assert not client.closed
    assert client.closed
    client.close()
    assert "version=4.1" in FakeSyncClient.last_url


def test_async_connect_inspect_and_context_close():
    async def scenario():
        async with await AsyncClient.connect("nfs://server/export", versions=["4.1"]) as client:
            assert client.version == Version(4, 1)
            assert client.health.lifecycle == "ready"
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
