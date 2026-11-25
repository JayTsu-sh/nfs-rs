from byte_paths import BytesPath
"""Exercise the actual Rust resolver through an installed test-support wheel."""
import asyncio
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("NFS_RS_TEST_INSTALLED") != "1",
    reason="requires the installed test-support wheel",
)


@pytest.mark.parametrize("path,status,exists,target", [
    ("file", "resolved", True, "file"),
    ("nested", "resolved", True, "nested"),
    (b'link', "resolved", True, "file"),
    (b'nested/up', "resolved", True, "file"),
    ("alias/file", "resolved", True, "nested/file"),
    ("alias/../link", "resolved", True, "file"),
    ("deep-alias/../peer", "resolved", True, "nested/peer"),
    ("nested/back/nested/file", "resolved", True, "nested/file"),
    ("alias/../alias/file", "resolved", True, "nested/file"),
    ("alias/", "resolved", True, "nested"),
    (".", "resolved", True, "."),
    ("missing", "missing", False, None),
    ("dangling", "missing", False, None),
    ("file/child", "not_directory", False, None),
    ("file/..", "not_directory", False, None),
    ("missing/../file", "missing", False, None),
    ("file/../file", "not_directory", False, None),
    ("link/", "not_directory", False, None),
    ("server-notdir", "not_directory", False, None),
    ("absolute", "outside_mount", None, None),
    ("outside", "outside_mount", None, None),
    ("cycle", "too_many_symlinks", None, None),
    ("cycle-a", "too_many_symlinks", None, None),
    ("chain-39", "resolved", True, "file"),
    ("chain-40", "too_many_symlinks", None, None),
])
def test_native_sync_and_async_resolution(path, status, exists, target):
    from nfs_rs import AsyncClient, Client, PathResolution, PathResolutionStatus

    path = path.encode() if isinstance(path, str) else path
    target = target.encode() if isinstance(target, str) else target

    def verify(result):
        assert isinstance(result, PathResolution)
        assert result.status is PathResolutionStatus(status)
        assert result.exists is exists
        assert result.path == target
        if target is None:
            assert result.info is None
        else:
            assert result.info.path == target
            assert result.info.fileid == 9

    with Client.connect("nfs-test://fixture/export") as client:
        verify(client.resolve(path))

    async def scenario():
        async with await AsyncClient.connect("nfs-test://fixture/export") as client:
            verify(await client.resolve(path))

    asyncio.run(scenario())


def test_native_resolution_preserves_errors_and_context():
    from nfs_rs import AsyncClient, Client, NfsConnectionError, NfsEncodingError, NfsPermissionError, NfsProtocolError, NfsStateLostError

    cases = [(b"denied", NfsPermissionError), (b"disconnected", NfsConnectionError),
             (b"stale", NfsStateLostError), (b"readlink-error", NfsProtocolError)]
    with Client.connect("nfs-test://fixture/export") as client:
        for path, error in cases:
            with pytest.raises(error) as caught:
                client.resolve(path)
            assert caught.value.operation == "resolve"
            assert caught.value.filename == path
            assert caught.value.protocol == str(client.version)

    async def scenario():
        async with await AsyncClient.connect("nfs-test://fixture/export") as client:
            for path, error in cases:
                with pytest.raises(error) as caught:
                    await client.resolve(path)
                assert caught.value.operation == "resolve"
                assert caught.value.filename == path

    asyncio.run(scenario())


def test_native_resolution_validates_paths_and_bounds_expansion():
    from nfs_rs import Client

    with Client.connect("nfs-test://fixture/export") as client:
        assert client.resolve(BytesPath(b'nested/up')).path == b'file'
        for path in ["text", b"bad\0name", b"/file", b"x" * 65537, b"./" * 4096, b"nul", b"long", b"many"]:
            with pytest.raises((TypeError, ValueError)):
                client.resolve(path)


def test_ordinary_normalization_and_explicit_resolution_have_distinct_contracts():
    from nfs_rs import AsyncClient, Client

    cases = [(b"deep-alias/../peer", "resolved", b"nested/peer"),
             (b"file/../peer", "not_directory", None),
             (b"missing/../peer", "missing", None)]
    with Client.connect("nfs-test://fixture/export") as client:
        for path, status, target in cases:
            # Ordinary operations normalize before native filesystem lookup.
            assert client.stat(path).path == b"peer"
            result = client.resolve(path)
            assert result.status.value == status
            assert result.path == target

    async def scenario():
        async with await AsyncClient.connect("nfs-test://fixture/export") as client:
            for path, status, target in cases:
                assert (await client.stat(path)).path == b"peer"
                result = await client.resolve(path)
                assert result.status.value == status
                assert result.path == target
    asyncio.run(scenario())


def test_native_resolution_deadline_cancellation_and_closed_client():
    from nfs_rs import AsyncClient, Client, NfsClientClosedError, NfsTimeoutError

    client = Client.connect("nfs-test://fixture/export", operation_timeout=0.01)
    with pytest.raises(NfsTimeoutError):
        client.resolve(b'slow')
    assert client.resolve(b'link').exists is True
    client.close()
    with pytest.raises(NfsClientClosedError):
        client.resolve(b'link')

    async def scenario():
        client = await AsyncClient.connect("nfs-test://fixture/export", operation_timeout=0.01)
        with pytest.raises(NfsTimeoutError):
            await client.resolve(b'slow')
        task = asyncio.create_task(client.resolve(b'slow'))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (await client.resolve(b'link')).exists is True
        await client.close()
        with pytest.raises(NfsClientClosedError):
            await client.resolve(b'link')

    asyncio.run(scenario())
