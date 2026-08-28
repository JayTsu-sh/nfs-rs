from __future__ import annotations

import asyncio
import importlib
import inspect
import io
import os
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
        normalized = _normalize_path(path)
        if mode != "rb":
            raise ValueError("Ticket 05 supports only binary read mode 'rb'")
        return File(self._inner.open(normalized, mode), normalized, _CONSTRUCTION_TOKEN)

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
        self._check_loop()
        normalized = _normalize_path(path)
        if mode != "rb":
            raise ValueError("Ticket 05 supports only binary read mode 'rb'")
        inner = await self._inner.open(normalized, mode)
        return AsyncFile(inner, normalized, self._loop, _CONSTRUCTION_TOKEN)

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


class File(io.RawIOBase):
    __slots__ = ("_inner", "_name")

    def __init__(self, inner: Any, name: str, token: object = None) -> None:
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("File objects are created only by Client.open()")
        super().__init__()
        self._inner = inner
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def mode(self) -> str:
        return "rb"

    @property
    def closed(self) -> bool:
        return self._inner.closed

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        return self._inner.read(size)

    def readinto(self, target: Any) -> int:
        length = _buffer_length(target)
        return _copy_to_buffer(target, length, self._inner.read(length))

    def read_at(self, offset: int, size: int = -1) -> bytes:
        return self._inner.read_at(offset, size)

    def readinto_at(self, target: Any, offset: int) -> int:
        length = _buffer_length(target)
        return _copy_to_buffer(target, length, self._inner.read_at(offset, length))

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        return self._inner.tell()

    def fileno(self) -> int:
        raise io.UnsupportedOperation("nfs-rs files do not expose OS file descriptors")

    def close(self) -> None:
        if self.closed:
            return
        self._inner.close()
        super().close()

    def __enter__(self) -> File:
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> None:
        try:
            self.close()
        except BaseException as cleanup_error:
            if exc is None:
                raise
            _record_cleanup_failure(exc, cleanup_error, "File")


class AsyncFile:
    __slots__ = ("_inner", "_name", "_loop")

    def __init__(
        self,
        inner: Any,
        name: str,
        loop: asyncio.AbstractEventLoop,
        token: object = None,
    ) -> None:
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("AsyncFile objects are created only by AsyncClient.open()")
        self._inner = inner
        self._name = name
        self._loop = loop

    def _check_loop(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("AsyncFile may only be used from its creating event loop")

    @property
    def name(self) -> str:
        return self._name

    @property
    def mode(self) -> str:
        return "rb"

    @property
    def closed(self) -> bool:
        return self._inner.closed

    def tell(self) -> int:
        return self._inner.tell()

    async def read(self, size: int = -1) -> bytes:
        self._check_loop()
        return await self._inner.read(size)

    async def readinto(self, target: Any) -> int:
        self._check_loop()
        length = _buffer_length(target)
        data = await self._inner.read(length)
        return _copy_to_buffer(target, length, data)

    async def read_at(self, offset: int, size: int = -1) -> bytes:
        self._check_loop()
        return await self._inner.read_at(offset, size)

    async def readinto_at(self, target: Any, offset: int) -> int:
        self._check_loop()
        length = _buffer_length(target)
        data = await self._inner.read_at(offset, length)
        return _copy_to_buffer(target, length, data)

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._check_loop()
        return await self._inner.seek(offset, whence)

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
