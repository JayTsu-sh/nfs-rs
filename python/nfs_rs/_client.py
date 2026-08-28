from __future__ import annotations

import asyncio
import importlib
import inspect
import io
import os
import platform
import sys
import sysconfig
import time
import warnings
from enum import Enum
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from dataclasses import dataclass
from types import ModuleType
from collections.abc import AsyncIterator, Iterator
from typing import Any, ClassVar

from ._errors import (
    NfsClosedResourceError, NfsError, NfsFileCloseError, NfsModeError,
    OperationOutcome, RecoveryAction,
)

_CONSTRUCTION_TOKEN = object()


async def _await_adapter_result(awaitable: Any, operation: str, protocol: str | None, filename: str | None) -> Any:
    try:
        return await awaitable
    except NfsError as error:
        raise error.with_context(operation=operation, protocol=protocol, filename=filename) from error


class _AdapterContext:
    __slots__ = ("_target", "_protocol", "_filename")

    def __init__(self, target: Any, protocol: str | None = None, filename: str | None = None) -> None:
        object.__setattr__(self, "_target", target)
        object.__setattr__(self, "_protocol", protocol)
        object.__setattr__(self, "_filename", filename)

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self.__slots__:
            object.__setattr__(self, name, value)
        else:
            setattr(self._target, name, value)

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._target, name)
        if not callable(attribute):
            return attribute
        operation = "truncate" if name == "truncate_path" else name

        def contextual_call(*args: Any, **kwargs: Any) -> Any:
            filename = self._filename
            if filename is None and args and isinstance(args[0], str):
                filename = args[1] if name == "symlink" and len(args) > 1 else args[0]
            try:
                result = attribute(*args, **kwargs)
            except NfsError as error:
                raise error.with_context(operation=operation, protocol=self._protocol, filename=filename) from error
            if inspect.isawaitable(result):
                return _await_adapter_result(result, operation, self._protocol, filename)
            return result

        return contextual_call


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
class RecoveryEvent:
    operation: str
    path: str | None
    protocol: str
    outcome: OperationOutcome
    recovery_action: RecoveryAction
    completed_bytes: int | None
    message: str


def _recovery_event(values: dict[str, Any]) -> RecoveryEvent:
    return RecoveryEvent(
        operation=values["operation"],
        path=values["path"],
        protocol=values["protocol"],
        outcome=OperationOutcome(values["outcome"]),
        recovery_action=RecoveryAction(values["recovery_action"]),
        completed_bytes=values["completed_bytes"],
        message=values["message"],
    )


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


@dataclass(frozen=True, slots=True)
class FsInfo:
    max_read: int
    preferred_read: int
    read_multiple: int
    max_write: int
    preferred_write: int
    write_multiple: int
    preferred_directory: int
    max_file_size: int
    time_delta_ns: int
    supports_links: bool
    supports_symlinks: bool
    homogeneous: bool
    can_set_time: bool


@dataclass(frozen=True, slots=True)
class FsStat:
    total_bytes: int
    free_bytes: int
    available_bytes: int
    total_files: int
    free_files: int
    available_files: int
    invariant_seconds: int


@dataclass(frozen=True, slots=True)
class Capabilities:
    acl: bool
    named_attributes: bool
    locks: bool
    callbacks: bool
    delegation_retention: bool
    pnfs: bool
    session_diagnostics: bool


@dataclass(frozen=True, slots=True)
class IoLimits:
    max_read: int
    max_write: int


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


def _capabilities(values: dict[str, Any]) -> Capabilities:
    return Capabilities(**values)


def _io_limits(values: dict[str, Any]) -> IoLimits:
    return IoLimits(**values)


def _fs_info(values: dict[str, Any]) -> FsInfo:
    return FsInfo(**values)


def _fs_stat(values: dict[str, Any]) -> FsStat:
    return FsStat(**values)


def _adapter() -> ModuleType:
    try:
        return importlib.import_module("nfs_rs._internal")
    except (ImportError, OSError) as error:
        libc_name, libc_version = platform.libc_ver()
        libc = f"{libc_name or 'unknown'} {libc_version or 'unknown'}"
        runtime = f"{platform.python_implementation()} {platform.python_version()}"
        soabi = sysconfig.get_config_var("SOABI") or "unknown"
        raise ImportError(
            "nfs_rs native extension could not be loaded; install a CPython 3.10+ "
            "Linux/glibc wheel matching this machine's architecture, or rebuild the "
            "source distribution with a supported Rust toolchain; detected "
            f"runtime={runtime}, sys.platform={sys.platform}, "
            f"architecture={platform.machine() or 'unknown'}, libc={libc}, SOABI={soabi}"
        ) from error


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
    for name, integer_value in (("uid", uid), ("gid", gid)):
        if integer_value is not None and (
            isinstance(integer_value, bool) or not 0 <= integer_value <= 2**32 - 1
        ):
            raise ValueError(f"{name} must be an unsigned 32-bit integer")
    for name, port_value in (("nfs_port", nfs_port), ("mount_port", mount_port)):
        if port_value is not None and (
            isinstance(port_value, bool) or not 1 <= port_value <= 65535
        ):
            raise ValueError(f"{name} must be between 1 and 65535")
    for name, size_value in (("rsize", rsize), ("wsize", wsize)):
        if size_value is not None and (isinstance(size_value, bool) or size_value <= 0):
            raise ValueError(f"{name} must be positive")
    for name, timeout_value in (
        ("connect_timeout", connect_timeout),
        ("operation_timeout", operation_timeout),
    ):
        if timeout_value is not None and (
            isinstance(timeout_value, bool) or timeout_value <= 0
        ):
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
        major, minor = inner.version
        protocol = str(major) if minor is None else f"{major}.{minor}"
        self._inner = _AdapterContext(inner, protocol)

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
    def capabilities(self) -> Capabilities:
        return _capabilities(self._inner.capabilities)

    @property
    def io_limits(self) -> IoLimits:
        return _io_limits(self._inner.io_limits)

    @property
    def closed(self) -> bool:
        return self._inner.closed

    @property
    def dropped_recovery_event_count(self) -> int:
        return self._inner.dropped_recovery_event_count

    def recovery_events(self) -> tuple[RecoveryEvent, ...]:
        return tuple(_recovery_event(values) for values in self._inner.recovery_events())

    def drain_recovery_events(self) -> tuple[RecoveryEvent, ...]:
        return tuple(_recovery_event(values) for values in self._inner.drain_recovery_events())

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
        cursor = self._inner.scandir(normalized)
        def entries() -> Iterator[DirEntry]:
            try:
                for values in cursor:
                    yield _directory_entry(normalized, values)
            except NfsError as error:
                raise error.with_context(operation="scandir", protocol=str(self.version), filename=normalized) from error
        return entries()

    def listdir(self, path: os.PathLike[str] | str = ".") -> list[str]:
        return [entry.name for entry in self.scandir(path)]

    def chmod(self, path: os.PathLike[str] | str, mode: int) -> None:
        self._inner.chmod(_normalize_path(path), mode)

    def chown(self, path: os.PathLike[str] | str, uid: int, gid: int) -> None:
        self._inner.chown(_normalize_path(path), uid, gid)

    def utime(self, path: os.PathLike[str] | str, *, ns: tuple[int, int]) -> None:
        if not isinstance(ns, tuple) or len(ns) != 2:
            raise TypeError("ns must be an (atime_ns, mtime_ns) tuple")
        self._inner.utime(_normalize_path(path), ns[0], ns[1])

    def truncate(self, path: os.PathLike[str] | str, size: int) -> None:
        self._inner.truncate_path(_normalize_path(path), size)

    def access(self, path: os.PathLike[str] | str, mode: int) -> bool:
        if isinstance(mode, bool) or not isinstance(mode, int) or mode & ~0o7:
            raise ValueError("mode must contain only F_OK, R_OK, W_OK, and X_OK")
        return bool(self._inner.access(_normalize_path(path), mode))

    def getxattr(self, path: os.PathLike[str] | str, name: str) -> bytes:
        return bytes(self._inner.getxattr(_normalize_path(path), name))

    def setxattr(self, path: os.PathLike[str] | str, name: str, value: Any) -> None:
        self._inner.setxattr(_normalize_path(path), name, _snapshot_bytes(value))

    def listxattr(self, path: os.PathLike[str] | str) -> list[str]:
        return list(self._inner.listxattr(_normalize_path(path)))

    def removexattr(self, path: os.PathLike[str] | str, name: str) -> None:
        self._inner.removexattr(_normalize_path(path), name)

    def fs_info(self) -> FsInfo:
        return _fs_info(self._inner.fs_info())

    def fs_stat(self) -> FsStat:
        return _fs_stat(self._inner.fs_stat())

    def mkdir(
        self, path: os.PathLike[str] | str, mode: int = 0o777, *, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Create directories non-transactionally when ``parents`` is true."""
        normalized = _normalize_path(path)
        paths = [normalized]
        if parents:
            parts = normalized.split("/")
            paths = ["/".join(parts[:index]) for index in range(1, len(parts) + 1)]
        for candidate in paths:
            try:
                self._inner.mkdir(candidate, mode)
            except FileExistsError:
                if not ((parents and candidate != normalized) or exist_ok):
                    raise
                try:
                    existing = self.stat(candidate)
                except BaseException:
                    raise
                if existing.type is not FileType.DIRECTORY:
                    raise

    def remove(self, path: os.PathLike[str] | str, *, missing_ok: bool = False) -> None:
        try:
            self._inner.remove(_normalize_path(path))
        except FileNotFoundError:
            if not missing_ok:
                raise

    unlink = remove

    def rmdir(self, path: os.PathLike[str] | str) -> None:
        self._inner.rmdir(_normalize_path(path))

    def rename(self, source: os.PathLike[str] | str, destination: os.PathLike[str] | str) -> None:
        self._inner.rename(_normalize_path(source), _normalize_path(destination))

    def link(self, source: os.PathLike[str] | str, destination: os.PathLike[str] | str) -> None:
        self._inner.link(_normalize_path(source), _normalize_path(destination))

    def symlink(self, target: os.PathLike[str] | str, link_path: os.PathLike[str] | str) -> None:
        target_text = os.fspath(target)
        if isinstance(target_text, bytes):
            raise TypeError("symlink targets must be strings, not bytes")
        if "\0" in target_text:
            raise ValueError("symlink targets may not contain NUL")
        self._inner.symlink(target_text, _normalize_path(link_path))

    def readlink(self, path: os.PathLike[str] | str) -> str:
        return self._inner.readlink(_normalize_path(path))

    def touch(self, path: os.PathLike[str] | str, *, exist_ok: bool = True) -> None:
        normalized = _normalize_path(path)
        if not exist_ok:
            raise NotImplementedError("touch(exist_ok=False) requires atomic exclusive create support")
        self.open(normalized, "ab").close()
        now = time.time_ns()
        self.utime(normalized, ns=(now, now))

    def read_bytes(self, path: os.PathLike[str] | str) -> bytes:
        with self.open(path, "rb") as file:
            return file.read()

    def write_bytes(self, path: os.PathLike[str] | str, data: Any) -> int:
        with self.open(path, "wb") as file:
            return file.write(data)

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
        major, minor = inner.version
        protocol = str(major) if minor is None else f"{major}.{minor}"
        self._inner = _AdapterContext(inner, protocol)
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
    def capabilities(self) -> Capabilities:
        return _capabilities(self._inner.capabilities)

    @property
    def io_limits(self) -> IoLimits:
        return _io_limits(self._inner.io_limits)

    @property
    def closed(self) -> bool:
        return self._inner.closed

    @property
    def dropped_recovery_event_count(self) -> int:
        return self._inner.dropped_recovery_event_count

    def recovery_events(self) -> tuple[RecoveryEvent, ...]:
        return tuple(_recovery_event(values) for values in self._inner.recovery_events())

    def drain_recovery_events(self) -> tuple[RecoveryEvent, ...]:
        return tuple(_recovery_event(values) for values in self._inner.drain_recovery_events())

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
        try:
            async for values in cursor:
                yield _directory_entry(normalized, values)
        except NfsError as error:
            raise error.with_context(operation="scandir", protocol=str(self.version), filename=normalized) from error

    async def listdir(self, path: os.PathLike[str] | str = ".") -> list[str]:
        return [entry.name async for entry in self.scandir(path)]

    async def chmod(self, path: os.PathLike[str] | str, mode: int) -> None:
        self._check_loop()
        await self._inner.chmod(_normalize_path(path), mode)

    async def chown(self, path: os.PathLike[str] | str, uid: int, gid: int) -> None:
        self._check_loop()
        await self._inner.chown(_normalize_path(path), uid, gid)

    async def utime(self, path: os.PathLike[str] | str, *, ns: tuple[int, int]) -> None:
        self._check_loop()
        if not isinstance(ns, tuple) or len(ns) != 2:
            raise TypeError("ns must be an (atime_ns, mtime_ns) tuple")
        await self._inner.utime(_normalize_path(path), ns[0], ns[1])

    async def truncate(self, path: os.PathLike[str] | str, size: int) -> None:
        self._check_loop()
        await self._inner.truncate_path(_normalize_path(path), size)

    async def access(self, path: os.PathLike[str] | str, mode: int) -> bool:
        self._check_loop()
        if isinstance(mode, bool) or not isinstance(mode, int) or mode & ~0o7:
            raise ValueError("mode must contain only F_OK, R_OK, W_OK, and X_OK")
        return bool(await self._inner.access(_normalize_path(path), mode))

    async def getxattr(self, path: os.PathLike[str] | str, name: str) -> bytes:
        self._check_loop()
        return bytes(await self._inner.getxattr(_normalize_path(path), name))

    async def setxattr(self, path: os.PathLike[str] | str, name: str, value: Any) -> None:
        self._check_loop()
        await self._inner.setxattr(_normalize_path(path), name, _snapshot_bytes(value))

    async def listxattr(self, path: os.PathLike[str] | str) -> list[str]:
        self._check_loop()
        return list(await self._inner.listxattr(_normalize_path(path)))

    async def removexattr(self, path: os.PathLike[str] | str, name: str) -> None:
        self._check_loop()
        await self._inner.removexattr(_normalize_path(path), name)

    async def fs_info(self) -> FsInfo:
        self._check_loop()
        return _fs_info(await self._inner.fs_info())

    async def fs_stat(self) -> FsStat:
        self._check_loop()
        return _fs_stat(await self._inner.fs_stat())

    async def mkdir(
        self, path: os.PathLike[str] | str, mode: int = 0o777, *, parents: bool = False, exist_ok: bool = False
    ) -> None:
        self._check_loop()
        normalized = _normalize_path(path)
        paths = [normalized]
        if parents:
            parts = normalized.split("/")
            paths = ["/".join(parts[:index]) for index in range(1, len(parts) + 1)]
        for candidate in paths:
            try:
                await self._inner.mkdir(candidate, mode)
            except FileExistsError:
                if not ((parents and candidate != normalized) or exist_ok):
                    raise
                try:
                    existing = await self.stat(candidate)
                except BaseException:
                    raise
                if existing.type is not FileType.DIRECTORY:
                    raise

    async def remove(self, path: os.PathLike[str] | str, *, missing_ok: bool = False) -> None:
        self._check_loop()
        try:
            await self._inner.remove(_normalize_path(path))
        except FileNotFoundError:
            if not missing_ok:
                raise

    unlink = remove

    async def rmdir(self, path: os.PathLike[str] | str) -> None:
        self._check_loop()
        await self._inner.rmdir(_normalize_path(path))

    async def rename(self, source: os.PathLike[str] | str, destination: os.PathLike[str] | str) -> None:
        self._check_loop()
        await self._inner.rename(_normalize_path(source), _normalize_path(destination))

    async def link(self, source: os.PathLike[str] | str, destination: os.PathLike[str] | str) -> None:
        self._check_loop()
        await self._inner.link(_normalize_path(source), _normalize_path(destination))

    async def symlink(self, target: os.PathLike[str] | str, link_path: os.PathLike[str] | str) -> None:
        self._check_loop()
        target_text = os.fspath(target)
        if isinstance(target_text, bytes):
            raise TypeError("symlink targets must be strings, not bytes")
        if "\0" in target_text:
            raise ValueError("symlink targets may not contain NUL")
        await self._inner.symlink(target_text, _normalize_path(link_path))

    async def readlink(self, path: os.PathLike[str] | str) -> str:
        self._check_loop()
        return await self._inner.readlink(_normalize_path(path))

    async def touch(self, path: os.PathLike[str] | str, *, exist_ok: bool = True) -> None:
        self._check_loop()
        normalized = _normalize_path(path)
        if not exist_ok:
            raise NotImplementedError("touch(exist_ok=False) requires atomic exclusive create support")
        file = await self.open(normalized, "ab")
        await file.close()
        now = time.time_ns()
        await self.utime(normalized, ns=(now, now))

    async def read_bytes(self, path: os.PathLike[str] | str) -> bytes:
        async with await self.open(path, "rb") as file:
            return await file.read()

    async def write_bytes(self, path: os.PathLike[str] | str, data: Any) -> int:
        async with await self.open(path, "wb") as file:
            return await file.write(data)

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


_BINARY_MODES = frozenset({"rb", "wb", "ab", "r+b", "w+b", "a+b"})


def _validate_binary_mode(mode: str) -> str:
    if mode not in _BINARY_MODES:
        raise NfsModeError(
            message="mode must be one of 'rb', 'wb', 'ab', 'r+b', 'w+b', or 'a+b'",
            operation="open",
        )
    return mode


def _snapshot_bytes(source: Any) -> bytes:
    view = memoryview(source)
    try:
        return view.tobytes()
    finally:
        view.release()


class File(io.RawIOBase):
    __slots__ = ("_inner", "_name", "_mode", "_closing_base")

    def __init__(self, inner: Any, name: str, mode: str, token: object = None) -> None:
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("File objects are created only by Client.open()")
        super().__init__()
        self._inner = _AdapterContext(inner, filename=name)
        self._name = name
        self._mode = mode
        self._closing_base = False

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

    def _check_closed(self, operation: str) -> None:
        if self.closed:
            raise NfsClosedResourceError(message="I/O operation on closed file", operation=operation, filename=self.name)

    def writable(self) -> bool:
        return self._mode.startswith(("w", "a")) or "+" in self._mode

    def seekable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        self._check_closed("read")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="read", filename=self.name)
        return self._inner.read(size)

    def readinto(self, target: Any) -> int:
        self._check_closed("readinto")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="readinto", filename=self.name)
        length = _buffer_length(target)
        request = min(length, self._inner.max_read_size)
        return _copy_to_buffer(target, length, self._inner.read(request))

    def read_at(self, offset: int, size: int = -1) -> bytes:
        self._check_closed("read_at")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="read_at", filename=self.name)
        return self._inner.read_at(offset, size)

    def readinto_at(self, target: Any, offset: int) -> int:
        self._check_closed("readinto_at")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="readinto_at", filename=self.name)
        length = _buffer_length(target)
        request = min(length, self._inner.max_read_size)
        return _copy_to_buffer(target, length, self._inner.read_at(offset, request))

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._check_closed("seek")
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        self._check_closed("tell")
        return self._inner.tell()

    def write(self, data: Any) -> int:
        self._check_closed("write")
        if not self.writable():
            raise NfsModeError(message="not writable", operation="write", filename=self.name)
        return self._inner.write(_snapshot_bytes(data))

    def write_at(self, data: Any, offset: int) -> int:
        self._check_closed("write_at")
        if not self.writable():
            raise NfsModeError(message="not writable", operation="write_at", filename=self.name)
        if self._mode.startswith("a"):
            raise NfsModeError(message="positional writes are unavailable in append mode", operation="write_at", filename=self.name)
        return self._inner.write_at(_snapshot_bytes(data), offset)

    def truncate(self, size: int | None = None) -> int:
        self._check_closed("truncate")
        if not self.writable():
            raise NfsModeError(message="not writable", operation="truncate", filename=self.name)
        return self._inner.truncate(size)

    def flush(self) -> None:
        if self._closing_base:
            return
        self._check_closed("flush")
        if self.writable():
            self._inner.flush()

    def fileno(self) -> int:
        raise NfsModeError(message="nfs-rs files do not expose OS file descriptors", operation="fileno", filename=self.name)

    def close(self) -> None:
        error: BaseException | None = None
        try:
            self._inner.close()
        except NfsFileCloseError as caught:
            error = caught.with_context(operation="close", filename=self.name)
        except NfsError as caught:
            child = caught.with_context(operation="close", filename=self.name)
            error = NfsFileCloseError(
                message=f"file close completed with errors: {caught}",
                operation="close",
                filename=self.name,
                errors=(child,),
            )
        except BaseException as caught:
            error = caught
        finally:
            self._closing_base = True
            try:
                super().close()
            finally:
                self._closing_base = False
        if error is not None:
            raise error

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
    __slots__ = ("_inner", "_name", "_mode", "_loop", "__weakref__")

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
        self._inner = _AdapterContext(inner, filename=name)
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
        self._check_closed("tell")
        return self._inner.tell()

    def _check_closed(self, operation: str) -> None:
        if self.closed:
            raise NfsClosedResourceError(
                message="I/O operation on closed file",
                operation=operation,
                filename=self.name,
            )

    async def read(self, size: int = -1) -> bytes:
        self._check_loop()
        self._check_closed("read")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="read", filename=self.name)
        return await self._inner.read(size)

    async def readinto(self, target: Any) -> int:
        self._check_loop()
        self._check_closed("readinto")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="readinto", filename=self.name)
        length = _buffer_length(target)
        request = min(length, self._inner.max_read_size)
        data = await self._inner.read(request)
        return _copy_to_buffer(target, length, data)

    async def read_at(self, offset: int, size: int = -1) -> bytes:
        self._check_loop()
        self._check_closed("read_at")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="read_at", filename=self.name)
        return await self._inner.read_at(offset, size)

    async def readinto_at(self, target: Any, offset: int) -> int:
        self._check_loop()
        self._check_closed("readinto_at")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="readinto_at", filename=self.name)
        length = _buffer_length(target)
        request = min(length, self._inner.max_read_size)
        data = await self._inner.read_at(offset, request)
        return _copy_to_buffer(target, length, data)

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._check_loop()
        self._check_closed("seek")
        return await self._inner.seek(offset, whence)

    def readable(self) -> bool:
        return self._mode.startswith("r") or "+" in self._mode

    def writable(self) -> bool:
        return self._mode.startswith(("w", "a")) or "+" in self._mode

    async def write(self, data: Any) -> int:
        self._check_loop()
        self._check_closed("write")
        if not self.writable():
            raise NfsModeError(message="not writable", operation="write", filename=self.name)
        return await self._inner.write(_snapshot_bytes(data))

    async def write_at(self, data: Any, offset: int) -> int:
        self._check_loop()
        self._check_closed("write_at")
        if not self.writable():
            raise NfsModeError(message="not writable", operation="write_at", filename=self.name)
        if self._mode.startswith("a"):
            raise NfsModeError(
                message="positional writes are unavailable in append mode",
                operation="write_at",
                filename=self.name,
            )
        return await self._inner.write_at(_snapshot_bytes(data), offset)

    async def truncate(self, size: int | None = None) -> int:
        self._check_loop()
        self._check_closed("truncate")
        if not self.writable():
            raise NfsModeError(message="not writable", operation="truncate", filename=self.name)
        return await self._inner.truncate(size)

    async def flush(self) -> None:
        self._check_loop()
        self._check_closed("flush")
        if self.writable():
            await self._inner.flush()

    async def close(self) -> None:
        self._check_loop()
        try:
            await self._inner.close()
        except NfsFileCloseError as caught:
            raise caught.with_context(operation="close", filename=self.name) from caught
        except NfsError as caught:
            child = caught.with_context(operation="close", filename=self.name)
            raise NfsFileCloseError(
                message=f"file close completed with errors: {caught}",
                operation="close",
                filename=self.name,
                errors=(child,),
            ) from caught

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
