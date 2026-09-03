import asyncio
import os
from pathlib import Path

import pytest

if os.environ.get("NFS_RS_TEST_INSTALLED") != "1":
    pytest.skip("requires an installed test-support wheel", allow_module_level=True)

import nfs_rs._internal

from nfs_rs import (
    AsyncClient,
    Client,
    FileType,
    NfsTimeoutError,
    list_exports,
    list_exports_async,
)


def test_installed_sync_paths_metadata_and_streaming_directory():
    client = Client.connect("nfs-test://fixture/export")
    info = client.stat(Path("folder/../file"))
    assert info.type is FileType.FILE
    assert info.path == "file"
    assert info.owner is None
    assert info.group is None
    assert info.fileid == 9
    assert info.mtime_ns == 3_000_000_004
    assert not client.exists("missing")
    with pytest.raises(PermissionError):
        client.exists("denied")
    entries = client.scandir("folder")
    assert next(entries).name == "first"
    assert [entry.name for entry in entries] == ["second"]
    assert client.listdir("folder") == ["first", "second"]
    client.close()


def test_client_close_cancels_unconsumed_directory_producer_without_hanging():
    client = Client.connect("nfs-test://fixture/export")
    entries = client.scandir("large")
    assert next(entries).name == "first"
    client.close()
    assert client.closed


def test_client_close_interrupts_directory_producer_blocked_before_first_protocol_result():
    client = Client.connect("nfs-test://fixture/export")
    client.scandir("blocked")
    client.close()
    assert client.closed


def test_operation_timeout_bounds_sync_and_async_directory_iteration():
    client = Client.connect("nfs-test://fixture/export", operation_timeout=0.01)
    entries = client.scandir("blocked")
    with pytest.raises(NfsTimeoutError, match="scandir deadline exceeded"):
        next(entries)
    client.close()

    async def scenario():
        async_client = await AsyncClient.connect(
            "nfs-test://fixture/export", operation_timeout=0.01
        )
        async_entries = async_client.scandir("blocked")
        with pytest.raises(NfsTimeoutError, match="scandir deadline exceeded"):
            await anext(async_entries)
        await async_client.close()

    asyncio.run(scenario())


def test_installed_async_paths_match_sync_values():
    async def scenario():
        client = await AsyncClient.connect("nfs-test://fixture/export")
        assert (await client.stat("file")).fileid == 9
        assert not await client.exists("missing")
        assert [entry.name async for entry in client.scandir("folder")] == ["first", "second"]
        assert await client.listdir("folder") == ["first", "second"]
        await client.close()

    asyncio.run(scenario())


def test_installed_sync_and_async_permission_families_and_stream_errors_match():
    sync_client = Client.connect("nfs-test://fixture/export")
    for path in ("denied", "forbidden"):
        with pytest.raises(PermissionError):
            sync_client.stat(path)
    entries = sync_client.scandir("denied-directory")
    assert next(entries).name == "first"
    with pytest.raises(PermissionError):
        next(entries)

    async def scenario():
        client = await AsyncClient.connect("nfs-test://fixture/export")
        for path in ("denied", "forbidden"):
            with pytest.raises(PermissionError):
                await client.stat(path)
        entries = client.scandir("denied-directory")
        assert (await anext(entries)).name == "first"
        with pytest.raises(PermissionError):
            await anext(entries)
        await client.close()

    asyncio.run(scenario())


def test_installed_export_discovery_matches_sync_and_async():
    sync_values = list_exports("nfs-test://fixture/")
    async_values = asyncio.run(list_exports_async("nfs-test://fixture/"))
    assert sync_values == async_values
    assert sync_values[0].path == "/data"
    assert sync_values[0].groups == ("team",)


@pytest.mark.parametrize("path", [b"bytes", "bad\0name", "../escape"])
def test_installed_invalid_paths_never_reach_native_adapter(path):
    client = Client.connect("nfs-test://fixture/export")
    with pytest.raises((TypeError, ValueError)):
        client.stat(path)
    client.close()
