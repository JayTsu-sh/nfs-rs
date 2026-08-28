import asyncio
import os

import pytest

REAL_URL = os.environ.get("NFS_RS_PYTHON_REAL_URL")
if not REAL_URL:
    pytest.skip("requires NFS_RS_PYTHON_REAL_URL", allow_module_level=True)

from nfs_rs import AsyncClient, Client, FileType


def test_real_server_sync_root_metadata_and_streaming_directory():
    with Client.connect(REAL_URL) as client:
        root = client.stat(".")
        assert root.type is FileType.DIRECTORY
        entries = client.scandir(".")
        first = next(entries, None)
        if first is not None:
            assert first.info.fileid > 0
            assert first.name in client.listdir(".")


def test_real_server_async_root_metadata_and_streaming_directory():
    async def scenario():
        async with await AsyncClient.connect(REAL_URL) as client:
            root = await client.stat(".")
            assert root.type is FileType.DIRECTORY
            entries = [entry async for entry in client.scandir(".")]
            assert all(entry.info.fileid > 0 for entry in entries)

    asyncio.run(scenario())
