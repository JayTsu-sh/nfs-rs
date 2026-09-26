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
    assert info.atime == 1_000_000_002
    assert info.mtime == 3_000_000_004
    assert info.ctime == 5_000_000_006
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
    assert next(entries).name == "entry-0"
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


def test_scandir_reference_crosses_native_boundary_in_sync_and_async():
    from nfs_rs import AsyncClient, Client, DirectoryRef

    with Client.connect("nfs-test://fixture/export") as client:
        for fh in (None, b"opaque\x00directory-handle"):
            entries = list(client.scandir(DirectoryRef("folder", fh)))
            assert [entry.path for entry in entries] == ["folder/first", "folder/second"]
            assert all(entry.fh is None for entry in entries)  # fixture omits handles

    async def scenario():
        async with await AsyncClient.connect("nfs-test://fixture/export") as client:
            for fh in (None, b"opaque\x00directory-handle"):
                entries = [entry async for entry in client.scandir(DirectoryRef("folder", fh))]
                assert [entry.path for entry in entries] == ["folder/first", "folder/second"]
                assert all(entry.fh is None for entry in entries)

    asyncio.run(scenario())


def test_batched_scandir_preserves_all_entries_in_sync_and_async():
    expected = [f"entry-{i}" for i in range(519)]
    with Client.connect("nfs-test://fixture/export") as client:
        entries = list(client.scandir("batched"))
        assert [entry.name for entry in entries] == expected
        assert [entry.info.fileid for entry in entries] == list(range(519))

    async def scenario():
        async with await AsyncClient.connect("nfs-test://fixture/export") as client:
            entries = [entry async for entry in client.scandir("batched")]
            assert [entry.name for entry in entries] == expected
            assert [entry.info.fileid for entry in entries] == list(range(519))
    asyncio.run(scenario())


def test_scandir_page_resumes_from_saved_position_in_a_new_client():
    from nfs_rs import DirectoryCookie, NfsBadCookieError

    expected = [f"page-entry-{index}" for index in range(7)]
    with Client.connect("nfs-test://fixture/export") as client:
        pages = [client.scandir_page("paged")]
        while not pages[-1].eof:
            pages.append(client.scandir_page("paged", pages[-1].next))
    assert [[entry.name for entry in page.entries] for page in pages] == [
        expected[0:3], expected[3:6], expected[6:7]
    ]
    assert [page.next.cookie for page in pages] == [3, 6, 7]
    with Client.connect("nfs-test://fixture/export") as client:
        # A caller that keeps going after EOF gets an empty EOF page at the same position.
        after_end = client.scandir_page("paged", pages[-1].next)
    assert not after_end.entries and after_end.eof
    assert after_end.next == pages[-1].next
    assert all(page.next.verifier == b"fixture1" for page in pages)
    assert pages[0].entries[0].path == "paged/page-entry-0"
    assert pages[0].entries[0].info.fileid == 1

    saved = DirectoryCookie(pages[0].next.cookie, pages[0].next.verifier)
    with Client.connect("nfs-test://fixture/export") as client:
        resumed = []
        position = saved
        while True:
            page = client.scandir_page("paged", position)
            resumed += [entry.name for entry in page.entries]
            if page.eof:
                break
            position = page.next
        assert resumed == expected[3:]

        for stale in (DirectoryCookie(4, b"fixture1"), DirectoryCookie(3, b"otherver")):
            with pytest.raises(NfsBadCookieError) as caught:
                client.scandir_page("paged", stale)
            error = caught.value
            assert error.code == 10003
            assert error.code_name == "NFS3ERR_BAD_COOKIE"
            assert error.operation == "scandir_page"
            assert error.filename == "paged"
            assert error.protocol == "3"


def test_async_scandir_page_resumes_and_types_stale_cookies():
    from nfs_rs import DirectoryCookie, NfsBadCookieError, NfsProtocolError

    async def scenario():
        async with await AsyncClient.connect("nfs-test://fixture/export") as client:
            first = await client.scandir_page("paged")
            assert [entry.name for entry in first.entries] == [
                "page-entry-0", "page-entry-1", "page-entry-2"
            ]
        async with await AsyncClient.connect("nfs-test://fixture/export") as client:
            second = await client.scandir_page("paged", first.next)
            assert second.entries[0].name == "page-entry-3"
            with pytest.raises(NfsBadCookieError) as caught:
                await client.scandir_page("paged", DirectoryCookie(5, first.next.verifier))
            assert isinstance(caught.value, NfsProtocolError)
            assert caught.value.operation == "scandir_page"

    asyncio.run(scenario())


def test_scandir_page_without_progress_raises_encoding_error():
    from nfs_rs import NfsEncodingError, NfsProtocolError

    with Client.connect("nfs-test://fixture/export") as client:
        with pytest.raises(NfsEncodingError) as caught:
            client.scandir_page("stalled")
    assert not isinstance(caught.value, NfsProtocolError)
    assert caught.value.operation == "scandir_page"
    assert "no progress" in str(caught.value)
