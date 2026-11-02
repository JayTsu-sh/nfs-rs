import asyncio
from types import SimpleNamespace

import pytest
import nfs_rs
import nfs_rs._client as facade


class Adapter:
    version = (4, 2)
    calls = []
    fail = False

    @classmethod
    def connect(cls, *args, **kwargs):
        return cls()

    def v42(self, operation, path, *args, **kwargs):
        self.calls.append((operation, path, args, kwargs))
        if self.fail:
            raise nfs_rs.NfsUnsupportedError(message="server returned NFS4ERR_NOTSUPP", code=10004)
        return {
            "allocate": {}, "deallocate": {}, "clone_range": {}, "setxattr": {},
            "seek_content": {"eof": False, "offset": 4096},
            "copy_range": {"count": 1024},
            "read_plus": {"eof": True, "contents": [{"offset": 0, "length": 4096, "data": None}, {"offset": 4096, "length": 3, "data": b"abc"}]},
            "nfs42_attributes": {"supported_attrs": [1, 0, 1 << 18], "clone_blksize": 4096, "xattr_support": True},
        }[operation]

    def setxattr(self, path, name, value):
        self.calls.append(("legacy-set", path, name, value))

    def close(self):
        pass


class AsyncAdapter(Adapter):
    @classmethod
    async def connect(cls, *args, **kwargs):
        return cls()

    async def v42(self, *args, **kwargs):
        return super().v42(*args, **kwargs)

    async def setxattr(self, *args, **kwargs):
        return super().setxattr(*args, **kwargs)

    async def close(self):
        pass


@pytest.fixture(autouse=True)
def adapter(monkeypatch):
    Adapter.calls.clear()
    Adapter.fail = False
    monkeypatch.setattr(facade, "_adapter", lambda: SimpleNamespace(SyncClient=Adapter, AsyncClient=AsyncAdapter))


def test_sync_v42_public_types_and_ranges():
    with nfs_rs.Client.connect("nfs://server/export?version=4.2") as client:
        client.allocate("folder/../file", 0, 4096)
        client.deallocate("file", 0, 4096)
        assert client.seek_content("file", 0, nfs_rs.SeekContent.HOLE) == nfs_rs.SeekResult(False, 4096)
        assert client.copy_range("source", "destination", count=1024) == nfs_rs.CopyResult(1024)
        client.clone_range("source", "destination")
        result = client.read_plus("file", 0, 8192)
        assert result == nfs_rs.ReadPlusResult(True, (nfs_rs.ReadPlusExtent(0, 4096, None), nfs_rs.ReadPlusExtent(4096, 3, b"abc")))
        attrs = client.nfs42_attributes()
        assert attrs.xattr_support and attrs.clone_blksize == 4096
        client.setxattr("file", "user.test", bytearray(b"binary\0"), mode=nfs_rs.XattrSetMode.CREATE)
        assert Adapter.calls[-1][2] == ("user.test", 1)
        assert Adapter.calls[-1][3]["value"] == b"binary\0"
        client.setxattr("file", "user.test", b"replace")
        assert Adapter.calls[-1][0] == "legacy-set"
    assert Adapter.calls[0][1] == "file"


def test_async_v42_uses_the_same_types_and_error_context():
    async def scenario():
        async with await nfs_rs.AsyncClient.connect("nfs://server/export?version=4.2") as client:
            await client.allocate("file", 0, 4096)
            await client.deallocate("file", 0, 4096)
            assert (await client.seek_content("file", 0, nfs_rs.SeekContent.DATA)).offset == 4096
            assert (await client.read_plus("file", 0, 8192)).contents[0].data is None
            assert (await client.copy_range("src", "dst", count=1024)).count == 1024
            await client.clone_range("src", "dst")
            assert (await client.nfs42_attributes()).xattr_support
            await client.setxattr("file", "key", b"x", mode=nfs_rs.XattrSetMode.REPLACE)
            Adapter.fail = True
            with pytest.raises(nfs_rs.NfsUnsupportedError) as caught:
                await client.seek_content("file", 0, nfs_rs.SeekContent.HOLE)
            assert caught.value.protocol == "4.2"
            assert caught.value.operation == "seek_content"
            assert caught.value.filename == "file"
    asyncio.run(scenario())


@pytest.mark.parametrize("offset", [-1, 2**64, True])
def test_invalid_ranges_are_rejected_before_native_io(offset):
    with nfs_rs.Client.connect("nfs://server/export") as client:
        with pytest.raises(ValueError):
            client.allocate("file", offset, 4096)
        with pytest.raises(ValueError):
            client.copy_range("src", "dst", source_offset=offset)
    assert not Adapter.calls


def test_invalid_content_kind_and_set_mode_are_rejected():
    with nfs_rs.Client.connect("nfs://server/export") as client:
        with pytest.raises(TypeError):
            client.seek_content("file", 0, 1)
        with pytest.raises(TypeError):
            client.setxattr("file", "key", b"value", mode=1)
        with pytest.raises(ValueError):
            client.read_plus("file", 0, 2**32)
