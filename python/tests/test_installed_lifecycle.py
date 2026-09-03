import asyncio
import os
import threading

import pytest

if os.environ.get("NFS_RS_TEST_INSTALLED") != "1":
    pytest.skip("requires an installed test-support wheel", allow_module_level=True)

import nfs_rs._internal

from nfs_rs import AsyncClient, Client, Health, Lifecycle, Version


def test_installed_sync_artifact_connects_inspects_and_closes():
    client = Client.connect("nfs-test://fixture/export")
    assert client.version == Version.NFS_V4_1
    assert client.health == Health(Lifecycle.READY, 0, None)
    assert not client.closed
    with client:
        pass
    assert client.closed
    assert client.health.lifecycle is Lifecycle.CLOSED
    client.close()


def test_installed_async_artifact_connects_inspects_and_closes():
    async def scenario():
        client = await AsyncClient.connect("nfs-test://fixture/export")
        assert client.version == Version.NFS_V4_1
        assert client.health.lifecycle is Lifecycle.READY
        async with client:
            pass
        assert client.closed
        assert client.health.lifecycle is Lifecycle.CLOSED
        await client.close()

    asyncio.run(scenario())


def test_sync_connect_releases_the_gil_while_runtime_blocks():
    started = threading.Event()
    progressed = threading.Event()

    def worker():
        started.wait()
        progressed.set()

    thread = threading.Thread(target=worker)
    thread.start()
    started.set()
    client = Client.connect("nfs-test://fixture/delay")
    thread.join(timeout=1)
    assert progressed.is_set()
    client.close()


@pytest.mark.parametrize("client_kind", ["sync", "async"])
def test_connect_timeout_raises_public_timeout_error(client_kind):
    from nfs_rs import NfsTimeoutError

    if client_kind == "sync":
        with pytest.raises(NfsTimeoutError, match="connection deadline exceeded"):
            Client.connect("nfs-test://fixture/delay", connect_timeout=0.001)
        return

    async def scenario():
        with pytest.raises(NfsTimeoutError, match="connection deadline exceeded"):
            await AsyncClient.connect("nfs-test://fixture/delay", connect_timeout=0.001)

    asyncio.run(scenario())


def test_async_connect_keeps_loop_responsive_and_cross_loop_use_is_rejected():
    first_loop = asyncio.new_event_loop()

    async def connect_with_marker():
        marker = asyncio.Event()

        async def mark_progress():
            await asyncio.sleep(0)
            marker.set()

        task = asyncio.create_task(mark_progress())
        client = await AsyncClient.connect("nfs-test://fixture/delay")
        await task
        assert marker.is_set()
        return client

    client = first_loop.run_until_complete(connect_with_marker())

    async def wrong_loop():
        with pytest.raises(RuntimeError, match="creating event loop"):
            await client.close()

    asyncio.run(wrong_loop())
    first_loop.run_until_complete(client.close())
    first_loop.close()
