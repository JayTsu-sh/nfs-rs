import asyncio
import os

import pytest

if os.environ.get("NFS_RS_TEST_INSTALLED") != "1":
    pytest.skip("requires an installed wheel", allow_module_level=True)

import nfs_rs._internal as native


def test_installed_native_sync_factory_rejects_invalid_url_without_network_work():
    with pytest.raises(RuntimeError, match="scheme nfs"):
        native.SyncClient.connect("http://invalid/export")


def test_installed_native_async_factory_rejects_invalid_url_without_blocking_loop():
    async def scenario():
        marker = asyncio.Event()

        async def responsive_task():
            await asyncio.sleep(0)
            marker.set()

        task = asyncio.create_task(responsive_task())
        with pytest.raises(RuntimeError, match="scheme nfs"):
            await native.AsyncClient.connect("http://invalid/export")
        await task
        assert marker.is_set()

    asyncio.run(scenario())
