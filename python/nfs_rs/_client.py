from __future__ import annotations

import asyncio
import importlib
import inspect
import io
import os
import warnings
from enum import Enum
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from dataclasses import dataclass
from types import ModuleType
from collections.abc import AsyncIterator, Iterator
from typing import Any, ClassVar

_CONSTRUCTION_TOKEN = object()


class Version(str, Enum):
    NFS_V3 = "3"
    NFS_V4_0 = "4.0"
    NFS_V4_1 = "4.1"

    def __str__(self) -> str:
        return self.value


class Lifecycle(str, Enum):
    READY = "ready"
    CLOSING = "closing"
    CLOSED = "closed"


class FileType(str, Enum):
    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"
    BLOCK_DEVICE = "block_device"
    CHARACTER_DEVICE = "character_device"
    FIFO = "fifo"
    SOCKET = "socket"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class Health:
    lifecycle: Lifecycle
    generation: int
    lease_healthy: bool | None


@dataclass(frozen=True, slots=True)
class FileInfo:
    path: str
    type: FileType
    mode: int
    nlink: int
    uid: int
    gid: int
    size: int
    used: int
    fsid: int
    fileid: int
    atime_ns: int
    mtime_ns: int
    ctime_ns: int
    owner: str | None
    group: str | None


@dataclass(frozen=True, slots=True)
class DirEntry:
    name: str
    path: str
    info: FileInfo


@dataclass(frozen=True, slots=True)
class ExportEntry:
    path: str
    groups: tuple[str, ...]


def _normalize_path(path: os.PathLike[str] | str) -> str:
    raw = os.fspath(path)
    if isinstance(raw, bytes):
        raise TypeError("NFS paths must be strings, not bytes")
    if "\0" in raw:
        raise ValueError("NFS paths may not contain NUL")
    parts: list[str] = []
    for part in raw.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if not parts:
                raise ValueError("NFS path escapes the export root")
            parts.pop()
        else:
            parts.append(part)
    return "/".join(parts) or "."


def _file_info(path: str, values: dict[str, Any]) -> FileInfo:
    return FileInfo(
        path=path,
        type=FileType(values["type"]),
        mode=values["mode"],
        nlink=values["nlink"],
        uid=values["uid"],
        gid=values["gid"],
        size=values["size"],
        used=values["used"],
        fsid=values["fsid"],
        fileid=values["fileid"],
        atime_ns=values["atime_ns"],
        mtime_ns=values["mtime_ns"],
        ctime_ns=values["ctime_ns"],
        owner=values.get("owner") or None,
        group=values.get("group") or None,
    )


def _directory_entry(parent: str, values: dict[str, Any]) -> DirEntry:
    path = values["name"] if parent == "." else f"{parent}/{values['name']}"
    return DirEntry(values["name"], path, _file_info(path, values["info"]))


def _adapter() -> ModuleType:
    return importlib.import_module("nfs_rs._internal")


def _record_cleanup_failure(exc: BaseException, cleanup_error: BaseException, owner: str) -> None:
    add_note = getattr(exc, "add_note", None)
    if callable(add_note):
        add_note(f"{owner} cleanup also failed: {cleanup_error}")
    else:
        exc.__context__ = cleanup_error


def _configured_url(url: str, options: dict[str, Any]) -> str:
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    names = {
        "uid": "uid",
        "gid": "gid",
        "nfs_port": "nfsport",
        "mount_port": "mountport",
        "rsize": "rsize",
        "wsize": "wsize",
        "readdir_buffer": "readdir-buffer",
        "noresvport": "noresvport",
        "retain_delegations": "retain-delegations",
    }
    if "versions" in options:
        query["version"] = ",".join(options["versions"])
    for option, query_name in names.items():
        if option not in options:
            continue
        value = options[option]
        if isinstance(value, tuple):
            value = ",".join(str(part) for part in value)
        elif isinstance(value, bool):
            value = str(value).lower()
        query[query_name] = str(value)
    return urlunsplit(parsed._replace(query=urlencode(query)))


def _export_url(host: str, options: dict[str, Any]) -> str:
    url = host if "://" in host else f"nfs://{host}/."
    return _configured_url(url, options)


def _options(
    *,
    versions: tuple[str, ...] | list[str] | None,
    uid: int | None,
    gid: int | None,
    nfs_port: int | None,
    mount_port: int | None,
    rsize: int | None,
    wsize: int | None,
    readdir_buffer: int | tuple[int, int] | None,
    noresvport: bool | None,
    retain_delegations: bool | None,
    connect_timeout: float | None,
    operation_timeout: float | None,
    recovery_event_capacity: int,
) -> dict[str, Any]:
    if versions is not None:
        versions = tuple(versions)
        if not versions or any(version not in {"3", "4.0", "4.1"} for version in versions):
            raise ValueError("versions must be a non-empty sequence containing 3, 4.0, or 4.1")
    for name, value in (("uid", uid), ("gid", gid)):
        if value is not None and (isinstance(value, bool) or not 0 <= value <= 2**32 - 1):
            raise ValueError(f"{name} must be an unsigned 32-bit integer")
    for name, value in (("nfs_port", nfs_port), ("mount_port", mount_port)):
        if value is not None and (isinstance(value, bool) or not 1 <= value <= 65535):
            raise ValueError(f"{name} must be between 1 and 65535")
    for name, value in (("rsize", rsize), ("wsize", wsize)):
        if value is not None and (isinstance(value, bool) or value <= 0):
            raise ValueError(f"{name} must be positive")
    for name, value in (("connect_timeout", connect_timeout), ("operation_timeout", operation_timeout)):
        if value is not None and (isinstance(value, bool) or value <= 0):
            raise ValueError(f"{name} must be positive")
    if isinstance(recovery_event_capacity, bool) or recovery_event_capacity <= 0:
        raise ValueError("recovery_event_capacity must be positive")
    if not isinstance(noresvport, (bool, type(None))):
        raise TypeError("noresvport must be bool or None")
    if not isinstance(retain_delegations, (bool, type(None))):
        raise TypeError("retain_delegations must be bool or None")
    if readdir_buffer is not None:
        values = (readdir_buffer,) if isinstance(readdir_buffer, int) else readdir_buffer
        if (
            not isinstance(values, tuple)
            or len(values) not in {1, 2}
            or any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in values)
        ):
            raise ValueError("readdir_buffer must be a positive integer or a pair of positive integers")
    return {
        name: value
        for name, value in {
            "versions": versions,
            "uid": uid,
            "gid": gid,
            "nfs_port": nfs_port,
            "mount_port": mount_port,
            "rsize": rsize,
            "wsize": wsize,
            "readdir_buffer": readdir_buffer,
            "noresvport": noresvport,
            "retain_delegations": retain_delegations,
            "connect_timeout": connect_timeout,
            "operation_timeout": operation_timeout,
            "recovery_event_capacity": recovery_event_capacity,
        }.items()
        if value is not None
    }


class _ClientOptions:
    _factory_name: ClassVar[str]

    @classmethod
    def _connection_options(
        cls,
        *,
        versions: tuple[str, ...] | list[str] | None = None,
        uid: int | None = None,
        gid: int | None = None,
        nfs_port: int | None = None,
        mount_port: int | None = None,
        rsize: int | None = None,
        wsize: int | None = None,
        readdir_buffer: int | tuple[int, int] | None = None,
        noresvport: bool | None = None,
        retain_delegations: bool | None = None,
        connect_timeout: float | None = None,
        operation_timeout: float | None = None,
        recovery_event_capacity: int = 256,
    ) -> dict[str, Any]:
        values = locals()
        values.pop("cls")
        return _options(**values)


class Client(_ClientOptions):
    __slots__ = ("_inner",)

    def __init__(self, inner: Any, token: object = None) -> None:
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("Client objects are created only by Client.connect()")
        self._inner = inner

    @classmethod
    def connect(cls, url: str, **options: Any) -> Client:
        validated = cls._connection_options(**options)
        return cls(
            _adapter().SyncClient.connect(_configured_url(url, validated), **validated),
            _CONSTRUCTION_TOKEN,
        )

    @property
    def version(self) -> Version:
        major, minor = self._inner.version
        return Version(str(major) if minor is None else f"{major}.{minor}")

    @property
    def health(self) -> Health:
        values = self._inner.health
        return Health(Lifecycle(values["lifecycle"]), values["generation"], values["lease_healthy"])

    @property
    def closed(self) -> bool:
        return self._inner.closed

    def close(self) -> None:
        self._inner.close()

    def stat(self, path: os.PathLike[str] | str) -> FileInfo:
        normalized = _normalize_path(path)
        return _file_info(normalized, self._inner.stat(normalized))

    def exists(self, path: os.PathLike[str] | str) -> bool:
        try:
            self.stat(path)
        except FileNotFoundError:
            return False
        return True

    def scandir(self, path: os.PathLike[str] | str = ".") -> Iterator[DirEntry]:
        normalized = _normalize_path(path)
        return (_directory_entry(normalized, values) for values in self._inner.scandir(normalized))

    def listdir(self, path: os.PathLike[str] | str = ".") -> list[str]:
        return [entry.name for entry in self.scandir(path)]

    def open(self, path: os.PathLike[str] | str, mode: str = "rb") -> File:
        """Open a binary file.

        Creating and truncating modes may require multiple NFS operations. Their
        effects are non-transactional: a created or truncated file can remain if
        a later open step fails.
        """
        normalized = _normalize_path(path)
        mode = _validate_binary_mode(mode)
        return File(self._inner.open(normalized, mode), normalized, mode, _CONSTRUCTION_TOKEN)

    def __enter__(self) -> Client:
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> None:
        try:
            self.close()
        except BaseException as cleanup_error:
            if exc is None:
                raise
            _record_cleanup_failure(exc, cleanup_error, "Client")

    def __repr__(self) -> str:
        return f"Client(version={self.version!s}, closed={self.closed})"


class AsyncClient(_ClientOptions):
    __slots__ = ("_inner", "_loop")

    def __init__(
        self,
        inner: Any,
        loop: asyncio.AbstractEventLoop,
        token: object = None,
    ) -> None:
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("AsyncClient objects are created only by AsyncClient.connect()")
        self._inner = inner
        self._loop = loop

    @classmethod
    async def connect(cls, url: str, **options: Any) -> AsyncClient:
        loop = asyncio.get_running_loop()
        validated = cls._connection_options(**options)
        return cls(
            await _adapter().AsyncClient.connect(_configured_url(url, validated), **validated),
            loop,
            _CONSTRUCTION_TOKEN,
        )

    def _check_loop(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("AsyncClient may only be used from its creating event loop")

    @property
    def version(self) -> Version:
        major, minor = self._inner.version
        return Version(str(major) if minor is None else f"{major}.{minor}")

    @property
    def health(self) -> Health:
        values = self._inner.health
        return Health(Lifecycle(values["lifecycle"]), values["generation"], values["lease_healthy"])

    @property
    def closed(self) -> bool:
        return self._inner.closed

    async def close(self) -> None:
        self._check_loop()
        await self._inner.close()

    async def stat(self, path: os.PathLike[str] | str) -> FileInfo:
        self._check_loop()
        normalized = _normalize_path(path)
        return _file_info(normalized, await self._inner.stat(normalized))

    async def exists(self, path: os.PathLike[str] | str) -> bool:
        try:
            await self.stat(path)
        except FileNotFoundError:
            return False
        return True

    async def scandir(self, path: os.PathLike[str] | str = ".") -> AsyncIterator[DirEntry]:
        self._check_loop()
        normalized = _normalize_path(path)
        cursor = self._inner.scandir(normalized)
        if inspect.isawaitable(cursor):
            cursor = await cursor
        async for values in cursor:
            yield _directory_entry(normalized, values)

    async def listdir(self, path: os.PathLike[str] | str = ".") -> list[str]:
        return [entry.name async for entry in self.scandir(path)]

    async def open(self, path: os.PathLike[str] | str, mode: str = "rb") -> AsyncFile:
        """Open a binary file with the same non-transactional effects as Client.open()."""
        self._check_loop()
        normalized = _normalize_path(path)
        mode = _validate_binary_mode(mode)
        inner = await self._inner.open(normalized, mode)
        return AsyncFile(inner, normalized, mode, self._loop, _CONSTRUCTION_TOKEN)

    async def __aenter__(self) -> AsyncClient:
        self._check_loop()
        return self

    async def __aexit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> None:
        try:
            await self.close()
        except BaseException as cleanup_error:
            if exc is None:
                raise
            _record_cleanup_failure(exc, cleanup_error, "AsyncClient")

    def __repr__(self) -> str:
        return f"AsyncClient(version={self.version!s}, closed={self.closed})"


def _buffer_length(target: Any) -> int:
    view = memoryview(target)
    try:
        if view.readonly:
            raise TypeError("readinto target must be writable")
        if not view.c_contiguous:
            raise TypeError("readinto target must be C-contiguous")
        return view.nbytes
    finally:
        view.release()


def _copy_to_buffer(target: Any, expected_length: int, data: bytes) -> int:
    view = memoryview(target)
    try:
        if view.readonly or not view.c_contiguous:
            raise TypeError("readinto target must remain writable and C-contiguous")
        if view.nbytes != expected_length:
            raise BufferError("readinto target changed size during I/O")
        bytes_view = view.cast("B")
        bytes_view[: len(data)] = data
        return len(data)
    finally:
        view.release()


_BINARY_MODES = frozenset({"rb", "wb", "ab", "r+b", "w+b", "a+b", "rb+", "wb+", "ab+"})


def _validate_binary_mode(mode: str) -> str:
    if mode not in _BINARY_MODES:
        raise ValueError(
            "mode must be one of 'rb', 'wb', 'ab', 'r+b', 'w+b', 'a+b', "
            "'rb+', 'wb+', or 'ab+'"
        )
    return mode


class File(io.RawIOBase):
    __slots__ = ("_inner", "_name", "_mode")

    def __init__(self, inner: Any, name: str, mode: str, token: object = None) -> None:
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("File objects are created only by Client.open()")
        super().__init__()
        self._inner = inner
        self._name = name
        self._mode = mode

    @property
    def name(self) -> str:
        return self._name

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def closed(self) -> bool:
        return self._inner.closed

    def readable(self) -> bool:
        return self._mode.startswith("r") or "+" in self._mode

    def _check_closed(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file")

    def writable(self) -> bool:
        return self._mode.startswith(("w", "a")) or "+" in self._mode

    def seekable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        self._check_closed()
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        return self._inner.read(size)

    def readinto(self, target: Any) -> int:
        self._check_closed()
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        length = _buffer_length(target)
        request = min(length, self._inner.max_read_size)
        return _copy_to_buffer(target, length, self._inner.read(request))

    def read_at(self, offset: int, size: int = -1) -> bytes:
        self._check_closed()
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        return self._inner.read_at(offset, size)

    def readinto_at(self, target: Any, offset: int) -> int:
        self._check_closed()
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        length = _buffer_length(target)
        request = min(length, self._inner.max_read_size)
        return _copy_to_buffer(target, length, self._inner.read_at(offset, request))

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._check_closed()
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        self._check_closed()
        return self._inner.tell()

    def write(self, data: Any) -> int:
        self._check_closed()
        if not self.writable():
            raise io.UnsupportedOperation("not writable")
        return self._inner.write(bytes(data))

    def write_at(self, data: Any, offset: int) -> int:
        self._check_closed()
        if not self.writable():
            raise io.UnsupportedOperation("not writable")
        return self._inner.write_at(bytes(data), offset)

    def truncate(self, size: int | None = None) -> int:
        self._check_closed()
        if not self.writable():
            raise io.UnsupportedOperation("not writable")
        return self._inner.truncate(size)

    def flush(self) -> None:
        self._check_closed()
        if self.writable():
            self._inner.flush()

    def fileno(self) -> int:
        raise io.UnsupportedOperation("nfs-rs files do not expose OS file descriptors")

    def close(self) -> None:
        self._inner.close()

    def __enter__(self) -> File:
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> None:
        try:
            self.close()
        except BaseException as cleanup_error:
            if exc is None:
                raise
            _record_cleanup_failure(exc, cleanup_error, "File")

    def __del__(self) -> None:
        inner = getattr(self, "_inner", None)
        if inner is not None and not inner.closed:
            warnings.warn(
                f"unclosed NFS file {self._name!r}",
                ResourceWarning,
                stacklevel=2,
            )


class AsyncFile:
    __slots__ = ("_inner", "_name", "_mode", "_loop")

    def __init__(
        self,
        inner: Any,
        name: str,
        mode: str,
        loop: asyncio.AbstractEventLoop,
        token: object = None,
    ) -> None:
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("AsyncFile objects are created only by AsyncClient.open()")
        self._inner = inner
        self._name = name
        self._mode = mode
        self._loop = loop

    def _check_loop(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("AsyncFile may only be used from its creating event loop")

    @property
    def name(self) -> str:
        return self._name

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def closed(self) -> bool:
        return self._inner.closed

    def tell(self) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file")
        return self._inner.tell()

    def _check_closed(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file")

    async def read(self, size: int = -1) -> bytes:
        self._check_loop()
        self._check_closed()
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        return await self._inner.read(size)

    async def readinto(self, target: Any) -> int:
        self._check_loop()
        self._check_closed()
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        length = _buffer_length(target)
        request = min(length, self._inner.max_read_size)
        data = await self._inner.read(request)
        return _copy_to_buffer(target, length, data)

    async def read_at(self, offset: int, size: int = -1) -> bytes:
        self._check_loop()
        self._check_closed()
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        return await self._inner.read_at(offset, size)

    async def readinto_at(self, target: Any, offset: int) -> int:
        self._check_loop()
        self._check_closed()
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        length = _buffer_length(target)
        request = min(length, self._inner.max_read_size)
        data = await self._inner.read_at(offset, request)
        return _copy_to_buffer(target, length, data)

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._check_loop()
        self._check_closed()
        return await self._inner.seek(offset, whence)

    def readable(self) -> bool:
        return self._mode.startswith("r") or "+" in self._mode

    def writable(self) -> bool:
        return self._mode.startswith(("w", "a")) or "+" in self._mode

    async def write(self, data: Any) -> int:
        self._check_loop()
        self._check_closed()
        if not self.writable():
            raise io.UnsupportedOperation("not writable")
        return await self._inner.write(bytes(data))

    async def write_at(self, data: Any, offset: int) -> int:
        self._check_loop()
        self._check_closed()
        if not self.writable():
            raise io.UnsupportedOperation("not writable")
        return await self._inner.write_at(bytes(data), offset)

    async def truncate(self, size: int | None = None) -> int:
        self._check_loop()
        self._check_closed()
        if not self.writable():
            raise io.UnsupportedOperation("not writable")
        return await self._inner.truncate(size)

    async def flush(self) -> None:
        self._check_loop()
        self._check_closed()
        if self.writable():
            await self._inner.flush()

    async def close(self) -> None:
        self._check_loop()
        await self._inner.close()

    async def __aenter__(self) -> AsyncFile:
        self._check_loop()
        return self

    async def __aexit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> None:
        try:
            await self.close()
        except BaseException as cleanup_error:
            if exc is None:
                raise
            _record_cleanup_failure(exc, cleanup_error, "AsyncFile")

    def __del__(self) -> None:
        inner = getattr(self, "_inner", None)
        if inner is not None and not inner.closed:
            warnings.warn(
                f"unclosed async NFS file {self._name!r}",
                ResourceWarning,
                stacklevel=2,
            )


def list_exports(host: str, **options: Any) -> tuple[ExportEntry, ...]:
    validated = Client._connection_options(**options)
    values = _adapter().list_exports(_export_url(host, validated), **validated)
    return tuple(
        ExportEntry(value["path"], tuple(value["groups"]))
        if isinstance(value, dict)
        else ExportEntry(value[0], tuple(value[1]))
        for value in values
    )


async def list_exports_async(host: str, **options: Any) -> tuple[ExportEntry, ...]:
    validated = AsyncClient._connection_options(**options)
    values = await _adapter().async_list_exports(_export_url(host, validated), **validated)
    return tuple(
        ExportEntry(value["path"], tuple(value["groups"]))
        if isinstance(value, dict)
        else ExportEntry(value[0], tuple(value[1]))
        for value in values
    )
