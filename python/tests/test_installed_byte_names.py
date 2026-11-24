"""Verify byte identities through the installed PyO3 extension, not a facade mock."""
import asyncio
import os
import pytest

if os.environ.get("NFS_RS_TEST_INSTALLED") != "1":
    pytest.skip("requires installed test-support wheel", allow_module_level=True)

from nfs_rs import Client, AsyncClient

NAMES = [b"\xff", b"\xfe", "�".encode("utf-8"), b"~nfs-rs-raw:FF"]

def test_sync_names_and_file_paths_are_lossless():
    with Client.connect("nfs-test://fixture/export") as client:
        entries = list(client.scandir(b"byte-names"))
        assert [entry.name for entry in entries] == NAMES
        assert client.listdir(b"byte-names") == NAMES
        assert len({entry.path for entry in entries}) == len(NAMES)
        for index, entry in enumerate(entries):
            assert entry.path == b"byte-names/" + entry.name
            assert entry.info.path == entry.path
            with client.open(entry.path, "wb") as output:
                output.write(bytes([index]))
        for index, entry in enumerate(entries):
            with client.open(entry.path, "rb") as source:
                assert source.name == entry.path
                assert source.read() == bytes([index])
        with pytest.raises(TypeError):
            client.listdir("byte-names")

def test_async_names_and_file_paths_are_lossless():
    async def run():
        async with await AsyncClient.connect("nfs-test://fixture/export") as client:
            entries = [entry async for entry in client.scandir(b"byte-names")]
            assert [entry.name for entry in entries] == NAMES
            assert await client.listdir(b"byte-names") == NAMES
            for index, entry in enumerate(entries):
                async with await client.open(entry.path, "wb") as output:
                    await output.write(bytes([index]))
            for index, entry in enumerate(entries):
                async with await client.open(entry.path, "rb") as source:
                    assert await source.read() == bytes([index])
    asyncio.run(run())
