import asyncio
import threading

import pytest

pytest.importorskip("nfs_rs._internal")

from nfs_rs import AsyncClient, Client, Health, Version


def test_installed_sync_artifact_connects_inspects_and_closes():
    client = Client.connect("nfs-test://fixture/export")
    assert client.version == Version(4, 1)
    assert client.health == Health("ready", 0, None)
    assert not client.closed
    with client:
        pass
    assert client.closed
    client.close()


def test_installed_async_artifact_connects_inspects_and_closes():
    async def scenario():
        client = await AsyncClient.connect("nfs-test://fixture/export")
        assert client.version == Version(4, 1)
        assert client.health.lifecycle == "ready"
        async with client:
            pass
        assert client.closed
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
