from __future__ import annotations

import asyncio
import inspect
import io
import os
import platform
import re
import sys
import sysconfig
import time
import warnings
from importlib import import_module as _import_module
from enum import Enum, IntEnum, IntFlag
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from dataclasses import dataclass
from types import ModuleType
from collections.abc import AsyncIterator, Iterator
import socket
from typing import Any, ClassVar, Protocol, runtime_checkable

from ._errors import (
    NfsClosedResourceError, NfsError, NfsFileCloseError, NfsModeError,
    OperationOutcome, RecoveryAction,
)

_CONSTRUCTION_TOKEN = object()


async def _await_adapter_result(awaitable: Any, operation: str, protocol: str | None, filename: _PathReference | None) -> Any:
    try:
        return await awaitable
    except NfsError as error:
        raise error.with_context(operation=operation, protocol=protocol, filename=_error_filename(filename)) from error


class _AdapterContext:
    __slots__ = ("_target", "_protocol", "_filename")

    def __init__(self, target: Any, protocol: str | None = None, filename: _PathReference | None = None) -> None:
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
            public_operation = args[0] if name == "v42" and args else operation
            filename = args[1] if name == "v42" and len(args) > 1 else self._filename
            if filename is None and args and isinstance(args[0], (str, bytes, memoryview)):
                filename = args[1] if name == "symlink" and len(args) > 1 else args[0]
            try:
                result = attribute(*args, **kwargs)
            except NfsError as error:
                raise error.with_context(operation=public_operation, protocol=self._protocol, filename=_error_filename(filename)) from error
            if inspect.isawaitable(result):
                return _await_adapter_result(result, public_operation, self._protocol, filename)
            return result

        return contextual_call


class Version(str, Enum):
    NFS_V3 = "3"
    NFS_V4_0 = "4.0"
    NFS_V4_1 = "4.1"
    NFS_V4_2 = "4.2"

    def __str__(self) -> str:
        return self.value


class PathResolutionStatus(str, Enum):
    RESOLVED = "resolved"
    MISSING = "missing"
    NOT_DIRECTORY = "not_directory"
    OUTSIDE_MOUNT = "outside_mount"
    TOO_MANY_SYMLINKS = "too_many_symlinks"


class SeekContent(IntEnum):
    DATA = 0
    HOLE = 1


class XattrSetMode(IntEnum):
    EITHER = 0
    CREATE = 1
    REPLACE = 2


# Public enum defaults must identify the same type as the root PEP 561 stub.
SeekContent.__module__ = "nfs_rs"
XattrSetMode.__module__ = "nfs_rs"


@dataclass(frozen=True, slots=True)
class SeekResult:
    eof: bool
    offset: int


@dataclass(frozen=True, slots=True)
class ReadPlusExtent:
    offset: int
    length: int
    data: bytes | None


@dataclass(frozen=True, slots=True)
class ReadPlusResult:
    eof: bool
    contents: tuple[ReadPlusExtent, ...]


@dataclass(frozen=True, slots=True)
class CopyResult:
    count: int


@dataclass(frozen=True, slots=True)
class Nfs42Attributes:
    supported_attrs: tuple[int, ...]
    clone_blksize: int | None
    xattr_support: bool


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


class AceType(IntEnum):
    ALLOW = 0
    DENY = 1
    AUDIT = 2
    ALARM = 3


class AceFlags(IntFlag):
    FILE_INHERIT = 0x01
    DIRECTORY_INHERIT = 0x02
    NO_PROPAGATE_INHERIT = 0x04
    INHERIT_ONLY = 0x08
    SUCCESSFUL_ACCESS = 0x10
    FAILED_ACCESS = 0x20
    IDENTIFIER_GROUP = 0x40
    INHERITED = 0x80


class AceMask(IntFlag):
    READ_DATA = 0x000001
    WRITE_DATA = 0x000002
    APPEND_DATA = 0x000004
    READ_NAMED_ATTRS = 0x000008
    WRITE_NAMED_ATTRS = 0x000010
    EXECUTE = 0x000020
    DELETE_CHILD = 0x000040
    READ_ATTRIBUTES = 0x000080
    WRITE_ATTRIBUTES = 0x000100
    DELETE = 0x010000
    READ_ACL = 0x020000
    WRITE_ACL = 0x040000
    WRITE_OWNER = 0x080000
    SYNCHRONIZE = 0x100000


class Acl41Flags(IntFlag):
    AUTO_INHERIT = 0x01
    PROTECTED = 0x02
    DEFAULTED = 0x04


@dataclass(frozen=True, slots=True)
class NfsAce:
    type: AceType
    flags: AceFlags
    access_mask: AceMask
    who: str


@dataclass(frozen=True, slots=True)
class NfsAcl41:
    flags: Acl41Flags
    aces: tuple[NfsAce, ...]


@dataclass(frozen=True, slots=True)
class Health:
    lifecycle: Lifecycle
    generation: int
    lease_healthy: bool | None


@dataclass(frozen=True, slots=True)
class RecoveryEvent:
    operation: str
    path: bytes | None
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
    _path: str | bytes | memoryview | _EntryPath
    type: FileType
    mode: int
    nlink: int
    uid: int
    gid: int
    size: int
    used: int
    fsid: int
    fileid: int
    atime: int  # Last access; ns since Unix epoch (UTC).
    mtime: int  # Last data modification; ns since Unix epoch (UTC).
    ctime: int  # Last metadata/status change; ns since Unix epoch (UTC).
    owner: str | None
    group: str | None

    @property
    def path(self) -> str | bytes:
        return _materialize_path(self._path)


@dataclass(frozen=True, slots=True)
class PathResolution:
    status: PathResolutionStatus
    path: memoryview | None
    info: FileInfo | None

    @property
    def exists(self) -> bool | None:
        if self.status is PathResolutionStatus.RESOLVED:
            return True
        if self.status in (PathResolutionStatus.MISSING, PathResolutionStatus.NOT_DIRECTORY):
            return False
        return None


@dataclass(frozen=True, slots=True)
class _EntryPath:
    parent: str | bytes | memoryview | _EntryPath
    name: memoryview

    def to_bytes(self) -> bytes:
        parts: list[bytes | memoryview] = []
        current: str | bytes | memoryview | _EntryPath = self
        while isinstance(current, _EntryPath):
            parts.append(current.name)
            current = current.parent
        root = _normalize_byte_path(current.encode("utf-8") if isinstance(current, str) else current)
        if root != b".":
            parts.append(root)
        return b"/".join(reversed(parts))


_Path = str | bytes | memoryview | os.PathLike[str] | os.PathLike[bytes]
_PathReference = str | bytes | memoryview | _EntryPath


def _materialize_path(path: _PathReference) -> str | bytes:
    if isinstance(path, _EntryPath):
        return path.to_bytes()
    if isinstance(path, (bytes, memoryview)):
        return _normalize_byte_path(path)
    return path


def _normalize_byte_path(path: bytes | memoryview) -> bytes:
    # Only materialize for explicit path access or error diagnostics. A scan
    # carries its parent and the original views without joining them.
    if isinstance(path, memoryview):
        path = path.tobytes()
    components: list[bytes] = []
    for part in path.split(b"/"):
        if part == b"..":
            if not components:
                return path  # retain the original path in error diagnostics
            components.pop()
        elif part not in {b"", b"."}:
            components.append(part)
    return b"/".join(components) or b"."


def _error_filename(path: _PathReference | None) -> bytes | None:
    if isinstance(path, _EntryPath):
        return path.to_bytes()
    if isinstance(path, memoryview):
        return path.tobytes()
    if isinstance(path, str):
        return path.encode("utf-8")
    return path


@dataclass(frozen=True, slots=True)
class DirectoryRef:
    path: _Path
    fh: bytes | None = None


@dataclass(frozen=True, slots=True)
class DirEntry:
    name: memoryview
    info: FileInfo
    fh: bytes | None
    _parent_fh: bytes | None
    _owner: object

    @property
    def path(self) -> bytes:
        path = self.info.path
        return path if isinstance(path, bytes) else path.encode("utf-8")


@dataclass(frozen=True, slots=True)
class DirectoryCookie:
    cookie: int = 0
    verifier: bytes = bytes(8)


@dataclass(frozen=True, slots=True)
class DirectoryPage:
    entries: tuple[DirEntry, ...]
    next: DirectoryCookie
    eof: bool


@dataclass(frozen=True, slots=True)
class ExportEntry:
    path: bytes
    groups: tuple[bytes, ...]


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


def _v42_uint(value: int, name: str, bits: int = 64) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < (1 << bits):
        raise ValueError(f"{name} must be an unsigned {bits}-bit integer")
    return value


def _read_plus_result(values: dict[str, Any]) -> ReadPlusResult:
    return ReadPlusResult(values["eof"], tuple(ReadPlusExtent(item["offset"], item["length"], item["data"]) for item in values["contents"]))


def _nfs42_attributes(values: dict[str, Any]) -> Nfs42Attributes:
    return Nfs42Attributes(tuple(values["supported_attrs"]), values["clone_blksize"], values["xattr_support"])


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


def _file_info(path: _PathReference, values: dict[str, Any]) -> FileInfo:
    return FileInfo(
        _path=path,
        type=FileType(values["type"]),
        mode=values["mode"],
        nlink=values["nlink"],
        uid=values["uid"],
        gid=values["gid"],
        size=values["size"],
        used=values["used"],
        fsid=values["fsid"],
        fileid=values["fileid"],
        atime=values["atime_ns"],
        mtime=values["mtime_ns"],
        ctime=values["ctime_ns"],
        owner=values.get("owner") or None,
        group=values.get("group") or None,
    )


def _stored_path(path: _Path) -> str | bytes | memoryview:
    if isinstance(path, memoryview):
        return path
    raw = os.fspath(path)
    if ("\0" in raw if isinstance(raw, str) else b"\0" in raw):
        raise ValueError("NFS paths may not contain NUL")
    return raw


def _resolution_path(path: _Path) -> str | bytes | memoryview:
    raw = _stored_path(path)
    if (isinstance(raw, str) and raw.startswith("/")) or (
        isinstance(raw, bytes) and raw.startswith(b"/")
    ):
        raise ValueError("resolve requires an export-relative path without NUL")
    return raw


def _path_resolution(values: dict[str, Any]) -> PathResolution:
    path = values["path"]
    return PathResolution(
        PathResolutionStatus(values["status"]), path,
        _file_info(path, values["info"]) if path is not None and values["info"] is not None else None,
    )


def _byte_path(path: _Path) -> str | bytes | memoryview:
    raw = _stored_path(path)
    return _normalize_path(raw) if isinstance(raw, str) else raw


def _namespace_path(path: _Path) -> str | bytes | memoryview:
    raw = _stored_path(path)
    if isinstance(raw, str):
        if raw.rstrip("/").rsplit("/", 1)[-1] in {"", ".", ".."}:
            raise ValueError("NFS mutation requires a leaf name")
        return _normalize_path(raw)
    return raw


def _mkdir_paths(
    path: str | bytes | memoryview, parents: bool,
) -> Iterator[tuple[str | bytes | memoryview, bool]]:
    if not parents:
        yield path, True
        return
    if isinstance(path, str):
        for separator in re.finditer("/", path):
            yield path[:separator.start()], False
        yield path, True
        return

    view = memoryview(path)
    if view.format != "B" or view.ndim != 1 or not view.c_contiguous or not view.readonly:
        raise TypeError("expected a contiguous read-only byte view")
    if len(view) > 65536 or 0 in view:
        raise ValueError("NFS path exceeds 64 KiB or contains NUL")
    endpoints: list[int] = []
    leaf_is_name = False
    for component in re.finditer(rb"[^/]+", view):
        part = view[component.start():component.end()]
        leaf_is_name = part != b"." and part != b".."
        if leaf_is_name:
            endpoints.append(component.end())
        elif part == b"..":
            if not endpoints:
                raise ValueError("NFS path escapes the export root")
            endpoints.pop()
    if not leaf_is_name:
        raise ValueError("NFS mutation requires a leaf name")
    for end in endpoints[:-1]:
        yield view[:end], False
    yield view, True


def _entry_target(
    target: DirectoryRef | DirEntry | _Path, owner: object,
) -> tuple[str | bytes | memoryview, _PathReference, bytes | None, bytes | None]:
    if isinstance(target, DirEntry):
        if target._owner is not owner:
            raise ValueError("directory entry belongs to another mount")
        if target._parent_fh is not None:
            return target.name, target.info._path, target.fh, target._parent_fh
        return target.path, target.info._path, target.fh, None
    path, fh = (target.path, target.fh) if isinstance(target, DirectoryRef) else (target, None)
    normalized = _byte_path(path)
    if fh is not None:
        if not isinstance(fh, bytes):
            raise TypeError("scandir fh must be bytes or None")
        if not fh:
            raise ValueError("scandir fh must not be empty; use None for path lookup")
    return normalized, normalized, fh, None


def _entry_options(fh: bytes | None, parent: bytes | None) -> dict[str, bytes]:
    options = {}
    if fh is not None:
        options["fh"] = fh
    if parent is not None:
        options["parent"] = parent
    return options


def _open_options(path: DirEntry | _Path, mode: str, fh: bytes | None, parent: bytes | None) -> dict[str, bytes]:
    options = _entry_options(None, parent)
    if isinstance(path, DirEntry):
        if fh is not None:
            options["expected_fh"] = fh
        elif mode != "rb":
            raise ValueError("writing through a directory entry requires its file handle")
    return options


def _directory_position(position: DirectoryCookie | None) -> tuple[int, bytes]:
    if position is None:
        return 0, bytes(8)
    if not isinstance(position, DirectoryCookie):
        raise TypeError("scandir_page position must be a DirectoryCookie or None")
    cookie, verifier = position.cookie, position.verifier
    if not isinstance(cookie, int) or isinstance(cookie, bool) or not 0 <= cookie < 2**64:
        raise ValueError("scandir_page cookie must be an unsigned 64-bit integer")
    if not isinstance(verifier, bytes) or len(verifier) != 8:
        raise ValueError("scandir_page verifier must be exactly 8 bytes")
    return cookie, verifier


def _directory_page(parent: _PathReference, values: dict[str, Any], owner: object) -> DirectoryPage:
    return DirectoryPage(
        tuple(_directory_entry(parent, entry, owner) for entry in values["entries"]),
        DirectoryCookie(values["cookie"], bytes(values["verifier"])),
        values["eof"],
    )


def _directory_entry(parent: _PathReference, values: dict[str, Any], owner: object) -> DirEntry:
    name = values["name"]
    # Native adapters already return a view. This branch also supports simple
    # adapters that expose immutable bytes directly.
    if not isinstance(name, memoryview):
        name = memoryview(name.encode("utf-8") if isinstance(name, str) else name)
    path = _EntryPath(parent, name)
    return DirEntry(name, _file_info(path, values["info"]), values.get("fh"), values.get("parent_fh"), owner)


def _capabilities(values: dict[str, Any]) -> Capabilities:
    return Capabilities(**values)


def _io_limits(values: dict[str, Any]) -> IoLimits:
    return IoLimits(**values)


def _acl41(values: dict[str, Any]) -> NfsAcl41:
    return NfsAcl41(
        flags=Acl41Flags(values["flags"]),
        aces=tuple(
            NfsAce(AceType(type_), AceFlags(flags), AceMask(access_mask), who)
            for type_, flags, access_mask, who in values["aces"]
        ),
    )


def _acl41_args(value: NfsAcl41) -> tuple[int, list[tuple[int, int, int, str]]]:
    if not isinstance(value, NfsAcl41):
        raise TypeError("ACL value must be an NfsAcl41")
    return int(value.flags), [
        (int(ace.type), int(ace.flags), int(ace.access_mask), ace.who)
        for ace in value.aces
    ]


def _fs_info(values: dict[str, Any]) -> FsInfo:
    return FsInfo(**values)


def _fs_stat(values: dict[str, Any]) -> FsStat:
    return FsStat(**values)


def _adapter() -> ModuleType:
    try:
        return _import_module("nfs_rs._internal")
    except (ImportError, OSError) as error:
        libc_name, libc_version = platform.libc_ver()
        libc = f"{libc_name or 'unknown'} {libc_version or 'unknown'}"
        runtime = f"{platform.python_implementation()} {platform.python_version()}"
        soabi = sysconfig.get_config_var("SOABI") or "unknown"
        raise ImportError(
            "nfs_rs native extension could not be loaded; install a CPython 3.11+ "
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


@runtime_checkable
class Dialer(Protocol):
    """Opens every connection of a mount: portmapper, MOUNT, NFS, NFSACL,
    pNFS data servers and reconnects.

    ``dial`` receives the server name unresolved and returns a connected
    ``SOCK_STREAM`` socket. The client takes ownership of the socket. It may
    be called again at any time while the client is open, from another thread.
    """

    def dial(self, host: str, port: int) -> socket.socket: ...


def _configured_url(url: str, options: dict[str, Any]) -> str:
    parsed = urlsplit(url)
    query_items = parse_qsl(parsed.query, keep_blank_values=True)
    allowed_query_names = {
        "version",
        "uid",
        "gid",
        "nfsport",
        "mountport",
        "readdir-buffer",
        "noresvport",
        "retain-delegations",
    }
    unknown = sorted({name for name, _ in query_items} - allowed_query_names)
    if unknown:
        raise ValueError(f"unknown NFS URL query option: {', '.join(unknown)}")
    query = dict(query_items)
    names = {
        "uid": "uid",
        "gid": "gid",
        "nfs_port": "nfsport",
        "mount_port": "mountport",
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
    readdir_buffer: int | tuple[int, int] | None,
    noresvport: bool | None,
    retain_delegations: bool | None,
    connect_timeout: float | None,
    operation_timeout: float | None,
    recovery_event_capacity: int,
    dialer: Dialer | None,
) -> dict[str, Any]:
    if versions is not None:
        versions = tuple(versions)
        if not versions or any(version not in {"3", "4.0", "4.1", "4.2"} for version in versions):
            raise ValueError("versions must be a non-empty sequence containing 3, 4.0, 4.1, or 4.2")
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
    if dialer is not None and not callable(getattr(dialer, "dial", None)):
        raise TypeError("dialer must have a callable dial(host, port) method")
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
            "readdir_buffer": readdir_buffer,
            "noresvport": noresvport,
            "retain_delegations": retain_delegations,
            "connect_timeout": connect_timeout,
            "operation_timeout": operation_timeout,
            "recovery_event_capacity": recovery_event_capacity,
            "dialer": dialer,
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
        readdir_buffer: int | tuple[int, int] | None = None,
        noresvport: bool | None = None,
        retain_delegations: bool | None = None,
        connect_timeout: float | None = None,
        operation_timeout: float | None = None,
        recovery_event_capacity: int = 256,
        dialer: Dialer | None = None,
    ) -> dict[str, Any]:
        values = locals()
        values.pop("cls")
        return _options(**values)


class Client(_ClientOptions):
    __slots__ = ("_inner", "_entry_owner")

    def __init__(self, inner: Any, token: object = None) -> None:
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("Client objects are created only by Client.connect()")
        major, minor = inner.version
        protocol = str(major) if minor is None else f"{major}.{minor}"
        self._inner = _AdapterContext(inner, protocol)
        self._entry_owner = object()

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

    def stat(self, path: DirEntry | _Path) -> FileInfo:
        target, label, fh, parent = _entry_target(path, self._entry_owner)
        return _file_info(label, self._inner.stat(target, **_entry_options(fh, parent)))

    def exists(self, path: DirEntry | _Path) -> bool:
        try:
            self.stat(path)
        except FileNotFoundError:
            return False
        return True

    def resolve(self, path: _Path) -> PathResolution:
        return _path_resolution(self._inner.resolve(_resolution_path(path)))

    def scandir(self, path: DirectoryRef | DirEntry | _Path = b".") -> Iterator[DirEntry]:
        target, label, fh, parent = _entry_target(path, self._entry_owner)
        cursor = self._inner.scandir(target, **_entry_options(fh, parent))
        def entries() -> Iterator[DirEntry]:
            try:
                for batch in cursor:
                    for values in batch:
                        yield _directory_entry(label, values, self._entry_owner)
            except NfsError as error:
                raise error.with_context(operation="scandir", protocol=str(self.version), filename=_error_filename(label)) from error
        return entries()

    def scandir_page(
        self,
        path: DirectoryRef | DirEntry | _Path = b".",
        position: DirectoryCookie | None = None,
    ) -> DirectoryPage:
        target, label, fh, parent = _entry_target(path, self._entry_owner)
        cookie, verifier = _directory_position(position)
        kwargs = {} if parent is None else {"parent": parent}
        return _directory_page(label, self._inner.scandir_page(target, fh, cookie, verifier, **kwargs), self._entry_owner)

    def listdir(self, path: _Path = b".") -> list[memoryview]:
        return self._inner.listdir(_byte_path(path))

    def chmod(self, path: _Path, mode: int) -> None:
        self._inner.chmod(_byte_path(path), mode)

    def chown(self, path: _Path, uid: int, gid: int) -> None:
        self._inner.chown(_byte_path(path), uid, gid)

    def utime(self, path: _Path, *, ns: tuple[int, int]) -> None:
        if not isinstance(ns, tuple) or len(ns) != 2:
            raise TypeError("ns must be an (atime_ns, mtime_ns) tuple")
        self._inner.utime(_byte_path(path), ns[0], ns[1])

    def truncate(self, path: _Path, size: int) -> None:
        self._inner.truncate_path(_byte_path(path), size)

    def access(self, path: _Path, mode: int) -> bool:
        if isinstance(mode, bool) or not isinstance(mode, int) or mode & ~0o7:
            raise ValueError("mode must contain only F_OK, R_OK, W_OK, and X_OK")
        return bool(self._inner.access(_byte_path(path), mode))

    def getdacl(self, path: _Path) -> NfsAcl41:
        return _acl41(self._inner.getdacl(_byte_path(path)))

    def setdacl(self, path: _Path, acl: NfsAcl41) -> None:
        flags, aces = _acl41_args(acl)
        self._inner.setdacl(_byte_path(path), flags, aces)

    def getsacl(self, path: _Path) -> NfsAcl41:
        return _acl41(self._inner.getsacl(_byte_path(path)))

    def setsacl(self, path: _Path, acl: NfsAcl41) -> None:
        flags, aces = _acl41_args(acl)
        self._inner.setsacl(_byte_path(path), flags, aces)

    def allocate(self, path: _Path, offset: int, length: int) -> None:
        self._inner.v42("allocate", _byte_path(path), offset=_v42_uint(offset, "offset"), count=_v42_uint(length, "length"))

    def deallocate(self, path: _Path, offset: int, length: int) -> None:
        self._inner.v42("deallocate", _byte_path(path), offset=_v42_uint(offset, "offset"), count=_v42_uint(length, "length"))

    def seek_content(self, path: _Path, offset: int, what: SeekContent) -> SeekResult:
        if not isinstance(what, SeekContent):
            raise TypeError("what must be a SeekContent")
        values = self._inner.v42("seek_content", _byte_path(path), offset=_v42_uint(offset, "offset"), second_offset=int(what))
        return SeekResult(**values)

    def read_plus(self, path: _Path, offset: int, count: int) -> ReadPlusResult:
        values = self._inner.v42("read_plus", _byte_path(path), offset=_v42_uint(offset, "offset"), count=_v42_uint(count, "count", 32))
        return _read_plus_result(values)

    def copy_range(self, source: _Path, destination: _Path, *, source_offset: int = 0, destination_offset: int = 0, count: int = 0) -> CopyResult:
        values = self._inner.v42("copy_range", _byte_path(source), _byte_path(destination), _v42_uint(source_offset, "source_offset"), _v42_uint(destination_offset, "destination_offset"), _v42_uint(count, "count"))
        return CopyResult(**values)

    def clone_range(self, source: _Path, destination: _Path, *, source_offset: int = 0, destination_offset: int = 0, count: int = 0) -> None:
        self._inner.v42("clone_range", _byte_path(source), _byte_path(destination), _v42_uint(source_offset, "source_offset"), _v42_uint(destination_offset, "destination_offset"), _v42_uint(count, "count"))

    def nfs42_attributes(self, path: _Path = ".") -> Nfs42Attributes:
        values = self._inner.v42("nfs42_attributes", _byte_path(path))
        return _nfs42_attributes(values)

    def getxattr(self, path: _Path, name: str) -> bytes:
        return bytes(self._inner.getxattr(_byte_path(path), name))

    def setxattr(self, path: _Path, name: str, value: Any, *, mode: XattrSetMode = XattrSetMode.EITHER) -> None:
        if not isinstance(mode, XattrSetMode):
            raise TypeError("mode must be an XattrSetMode")
        if mode is XattrSetMode.EITHER:
            self._inner.setxattr(_byte_path(path), name, _snapshot_bytes(value))
        else:
            self._inner.v42("setxattr", _byte_path(path), name, int(mode), value=_snapshot_bytes(value))

    def listxattr(self, path: _Path) -> list[str]:
        return list(self._inner.listxattr(_byte_path(path)))

    def removexattr(self, path: _Path, name: str) -> None:
        self._inner.removexattr(_byte_path(path), name)

    def fs_info(self) -> FsInfo:
        return _fs_info(self._inner.fs_info())

    def fs_stat(self) -> FsStat:
        return _fs_stat(self._inner.fs_stat())

    def mkdir(
        self, path: _Path, mode: int = 0o777, *, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Create directories non-transactionally when ``parents`` is true."""
        normalized = _namespace_path(path)
        for candidate, is_leaf in _mkdir_paths(normalized, parents):
            try:
                self._inner.mkdir(candidate, mode)
            except FileExistsError:
                if is_leaf and not exist_ok:
                    raise
                try:
                    existing = self.stat(candidate)
                except BaseException:
                    raise
                if existing.type is not FileType.DIRECTORY:
                    raise

    def remove(self, path: _Path, *, missing_ok: bool = False) -> None:
        try:
            self._inner.remove(_namespace_path(path))
        except FileNotFoundError:
            if not missing_ok:
                raise

    unlink = remove

    def rmdir(self, path: _Path) -> None:
        self._inner.rmdir(_namespace_path(path))

    def rename(self, source: _Path, destination: _Path) -> None:
        self._inner.rename(_namespace_path(source), _namespace_path(destination))

    def link(self, source: _Path, destination: _Path) -> None:
        self._inner.link(_namespace_path(source), _namespace_path(destination))

    def symlink(self, target: _Path, link_path: _Path) -> None:
        self._inner.symlink(_stored_path(target), _namespace_path(link_path))

    def readlink(self, path: _Path) -> memoryview:
        return self._inner.readlink(_byte_path(path))

    def touch(self, path: _Path, *, exist_ok: bool = True) -> None:
        normalized = _namespace_path(path)
        if not exist_ok:
            raise NotImplementedError("touch(exist_ok=False) requires atomic exclusive create support")
        self.open(normalized, "ab").close()
        now = time.time_ns()
        self.utime(normalized, ns=(now, now))

    def open(self, path: DirEntry | _Path, mode: str = "rb") -> File:
        """Open a binary file.

        Creating and truncating modes may require multiple NFS operations. Their
        effects are non-transactional: a created or truncated file can remain if
        a later open step fails.
        """
        target, label, fh, parent = _entry_target(path, self._entry_owner)
        mode = _validate_binary_mode(mode)
        inner = self._inner.open(target, mode, **_open_options(path, mode, fh, parent))
        return File(inner, label, mode, _CONSTRUCTION_TOKEN)

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
    __slots__ = ("_inner", "_loop", "_entry_owner")

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
        self._entry_owner = object()
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

    async def stat(self, path: DirEntry | _Path) -> FileInfo:
        self._check_loop()
        target, label, fh, parent = _entry_target(path, self._entry_owner)
        return _file_info(label, await self._inner.stat(target, **_entry_options(fh, parent)))

    async def exists(self, path: DirEntry | _Path) -> bool:
        try:
            await self.stat(path)
        except FileNotFoundError:
            return False
        return True

    async def resolve(self, path: _Path) -> PathResolution:
        self._check_loop()
        return _path_resolution(await self._inner.resolve(_resolution_path(path)))

    async def scandir(self, path: DirectoryRef | DirEntry | _Path = b".") -> AsyncIterator[DirEntry]:
        self._check_loop()
        target, label, fh, parent = _entry_target(path, self._entry_owner)
        cursor = self._inner.scandir(target, **_entry_options(fh, parent))
        if inspect.isawaitable(cursor):
            cursor = await cursor
        try:
            async for batch in cursor:
                for values in batch:
                    yield _directory_entry(label, values, self._entry_owner)
        except NfsError as error:
            raise error.with_context(operation="scandir", protocol=str(self.version), filename=_error_filename(label)) from error

    async def scandir_page(
        self,
        path: DirectoryRef | DirEntry | _Path = b".",
        position: DirectoryCookie | None = None,
    ) -> DirectoryPage:
        self._check_loop()
        target, label, fh, parent = _entry_target(path, self._entry_owner)
        cookie, verifier = _directory_position(position)
        kwargs = {} if parent is None else {"parent": parent}
        return _directory_page(label, await self._inner.scandir_page(target, fh, cookie, verifier, **kwargs), self._entry_owner)

    async def listdir(self, path: _Path = b".") -> list[memoryview]:
        self._check_loop()
        return await self._inner.listdir(_byte_path(path))

    async def chmod(self, path: _Path, mode: int) -> None:
        self._check_loop()
        await self._inner.chmod(_byte_path(path), mode)

    async def chown(self, path: _Path, uid: int, gid: int) -> None:
        self._check_loop()
        await self._inner.chown(_byte_path(path), uid, gid)

    async def utime(self, path: _Path, *, ns: tuple[int, int]) -> None:
        self._check_loop()
        if not isinstance(ns, tuple) or len(ns) != 2:
            raise TypeError("ns must be an (atime_ns, mtime_ns) tuple")
        await self._inner.utime(_byte_path(path), ns[0], ns[1])

    async def truncate(self, path: _Path, size: int) -> None:
        self._check_loop()
        await self._inner.truncate_path(_byte_path(path), size)

    async def access(self, path: _Path, mode: int) -> bool:
        self._check_loop()
        if isinstance(mode, bool) or not isinstance(mode, int) or mode & ~0o7:
            raise ValueError("mode must contain only F_OK, R_OK, W_OK, and X_OK")
        return bool(await self._inner.access(_byte_path(path), mode))

    async def getdacl(self, path: _Path) -> NfsAcl41:
        self._check_loop()
        return _acl41(await self._inner.getdacl(_byte_path(path)))

    async def setdacl(self, path: _Path, acl: NfsAcl41) -> None:
        self._check_loop()
        flags, aces = _acl41_args(acl)
        await self._inner.setdacl(_byte_path(path), flags, aces)

    async def getsacl(self, path: _Path) -> NfsAcl41:
        self._check_loop()
        return _acl41(await self._inner.getsacl(_byte_path(path)))

    async def setsacl(self, path: _Path, acl: NfsAcl41) -> None:
        self._check_loop()
        flags, aces = _acl41_args(acl)
        await self._inner.setsacl(_byte_path(path), flags, aces)

    async def allocate(self, path: _Path, offset: int, length: int) -> None:
        self._check_loop()
        await self._inner.v42("allocate", _byte_path(path), offset=_v42_uint(offset, "offset"), count=_v42_uint(length, "length"))

    async def deallocate(self, path: _Path, offset: int, length: int) -> None:
        self._check_loop()
        await self._inner.v42("deallocate", _byte_path(path), offset=_v42_uint(offset, "offset"), count=_v42_uint(length, "length"))

    async def seek_content(self, path: _Path, offset: int, what: SeekContent) -> SeekResult:
        self._check_loop()
        if not isinstance(what, SeekContent):
            raise TypeError("what must be a SeekContent")
        values = await self._inner.v42("seek_content", _byte_path(path), offset=_v42_uint(offset, "offset"), second_offset=int(what))
        return SeekResult(**values)

    async def read_plus(self, path: _Path, offset: int, count: int) -> ReadPlusResult:
        self._check_loop()
        values = await self._inner.v42("read_plus", _byte_path(path), offset=_v42_uint(offset, "offset"), count=_v42_uint(count, "count", 32))
        return _read_plus_result(values)

    async def copy_range(self, source: _Path, destination: _Path, *, source_offset: int = 0, destination_offset: int = 0, count: int = 0) -> CopyResult:
        self._check_loop()
        values = await self._inner.v42("copy_range", _byte_path(source), _byte_path(destination), _v42_uint(source_offset, "source_offset"), _v42_uint(destination_offset, "destination_offset"), _v42_uint(count, "count"))
        return CopyResult(**values)

    async def clone_range(self, source: _Path, destination: _Path, *, source_offset: int = 0, destination_offset: int = 0, count: int = 0) -> None:
        self._check_loop()
        await self._inner.v42("clone_range", _byte_path(source), _byte_path(destination), _v42_uint(source_offset, "source_offset"), _v42_uint(destination_offset, "destination_offset"), _v42_uint(count, "count"))

    async def nfs42_attributes(self, path: _Path = ".") -> Nfs42Attributes:
        self._check_loop()
        values = await self._inner.v42("nfs42_attributes", _byte_path(path))
        return _nfs42_attributes(values)

    async def getxattr(self, path: _Path, name: str) -> bytes:
        self._check_loop()
        return bytes(await self._inner.getxattr(_byte_path(path), name))

    async def setxattr(self, path: _Path, name: str, value: Any, *, mode: XattrSetMode = XattrSetMode.EITHER) -> None:
        self._check_loop()
        if not isinstance(mode, XattrSetMode):
            raise TypeError("mode must be an XattrSetMode")
        if mode is XattrSetMode.EITHER:
            await self._inner.setxattr(_byte_path(path), name, _snapshot_bytes(value))
        else:
            await self._inner.v42("setxattr", _byte_path(path), name, int(mode), value=_snapshot_bytes(value))

    async def listxattr(self, path: _Path) -> list[str]:
        self._check_loop()
        return list(await self._inner.listxattr(_byte_path(path)))

    async def removexattr(self, path: _Path, name: str) -> None:
        self._check_loop()
        await self._inner.removexattr(_byte_path(path), name)

    async def fs_info(self) -> FsInfo:
        self._check_loop()
        return _fs_info(await self._inner.fs_info())

    async def fs_stat(self) -> FsStat:
        self._check_loop()
        return _fs_stat(await self._inner.fs_stat())

    async def mkdir(
        self, path: _Path, mode: int = 0o777, *, parents: bool = False, exist_ok: bool = False
    ) -> None:
        self._check_loop()
        normalized = _namespace_path(path)
        for candidate, is_leaf in _mkdir_paths(normalized, parents):
            try:
                await self._inner.mkdir(candidate, mode)
            except FileExistsError:
                if is_leaf and not exist_ok:
                    raise
                try:
                    existing = await self.stat(candidate)
                except BaseException:
                    raise
                if existing.type is not FileType.DIRECTORY:
                    raise

    async def remove(self, path: _Path, *, missing_ok: bool = False) -> None:
        self._check_loop()
        try:
            await self._inner.remove(_namespace_path(path))
        except FileNotFoundError:
            if not missing_ok:
                raise

    unlink = remove

    async def rmdir(self, path: _Path) -> None:
        self._check_loop()
        await self._inner.rmdir(_namespace_path(path))

    async def rename(self, source: _Path, destination: _Path) -> None:
        self._check_loop()
        await self._inner.rename(_namespace_path(source), _namespace_path(destination))

    async def link(self, source: _Path, destination: _Path) -> None:
        self._check_loop()
        await self._inner.link(_namespace_path(source), _namespace_path(destination))

    async def symlink(self, target: _Path, link_path: _Path) -> None:
        self._check_loop()
        await self._inner.symlink(_stored_path(target), _namespace_path(link_path))

    async def readlink(self, path: _Path) -> memoryview:
        self._check_loop()
        return await self._inner.readlink(_byte_path(path))

    async def touch(self, path: _Path, *, exist_ok: bool = True) -> None:
        self._check_loop()
        normalized = _namespace_path(path)
        if not exist_ok:
            raise NotImplementedError("touch(exist_ok=False) requires atomic exclusive create support")
        file = await self.open(normalized, "ab")
        await file.close()
        now = time.time_ns()
        await self.utime(normalized, ns=(now, now))

    async def open(self, path: DirEntry | _Path, mode: str = "rb") -> AsyncFile:
        """Open a binary file with the same non-transactional effects as Client.open()."""
        self._check_loop()
        target, label, fh, parent = _entry_target(path, self._entry_owner)
        mode = _validate_binary_mode(mode)
        inner = await self._inner.open(target, mode, **_open_options(path, mode, fh, parent))
        return AsyncFile(inner, label, mode, self._loop, _CONSTRUCTION_TOKEN)

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


def _writable_byte_view(target: Any) -> memoryview:
    view = memoryview(target)
    try:
        if view.readonly:
            raise TypeError("readinto target must be writable")
        if not view.c_contiguous:
            raise TypeError("readinto target must be C-contiguous")
        return view.cast("B")
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

    def __init__(self, inner: Any, name: _PathReference, mode: str, token: object = None) -> None:
        if token is not _CONSTRUCTION_TOKEN:
            raise TypeError("File objects are created only by Client.open()")
        super().__init__()
        self._inner = _AdapterContext(inner, filename=name)
        self._name = name
        self._mode = mode
        self._closing_base = False

    @property
    def name(self) -> str | bytes:
        return _materialize_path(self._name)

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
            raise NfsClosedResourceError(message="I/O operation on closed file", operation=operation, filename=_error_filename(self._name))

    def writable(self) -> bool:
        return self._mode.startswith(("w", "a")) or "+" in self._mode

    def seekable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        self._check_closed("read")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="read", filename=_error_filename(self._name))
        return self._inner.read(size)

    def readinto(self, target: Any) -> int:
        self._check_closed("readinto")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="readinto", filename=_error_filename(self._name))
        with _writable_byte_view(target) as view:
            return self._inner.readinto(view)

    def read_at(self, offset: int, size: int = -1) -> bytes:
        self._check_closed("read_at")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="read_at", filename=_error_filename(self._name))
        return self._inner.read_at(offset, size)

    def readinto_at(self, target: Any, offset: int) -> int:
        self._check_closed("readinto_at")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="readinto_at", filename=_error_filename(self._name))
        with _writable_byte_view(target) as view:
            return self._inner.readinto_at(view, offset)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._check_closed("seek")
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        self._check_closed("tell")
        return self._inner.tell()

    def write(self, data: Any) -> int:
        self._check_closed("write")
        if not self.writable():
            raise NfsModeError(message="not writable", operation="write", filename=_error_filename(self._name))
        return self._inner.write(_snapshot_bytes(data))

    def write_at(self, data: Any, offset: int) -> int:
        self._check_closed("write_at")
        if not self.writable():
            raise NfsModeError(message="not writable", operation="write_at", filename=_error_filename(self._name))
        if self._mode.startswith("a"):
            raise NfsModeError(message="positional writes are unavailable in append mode", operation="write_at", filename=_error_filename(self._name))
        return self._inner.write_at(_snapshot_bytes(data), offset)

    def truncate(self, size: int | None = None) -> int:
        self._check_closed("truncate")
        if not self.writable():
            raise NfsModeError(message="not writable", operation="truncate", filename=_error_filename(self._name))
        return self._inner.truncate(size)

    def flush(self) -> None:
        if self._closing_base:
            return
        self._check_closed("flush")
        if self.writable():
            self._inner.flush()

    def fileno(self) -> int:
        raise NfsModeError(message="nfs-rs files do not expose OS file descriptors", operation="fileno", filename=_error_filename(self._name))

    def close(self) -> None:
        error: BaseException | None = None
        try:
            self._inner.close()
        except NfsFileCloseError as caught:
            error = caught.with_context(operation="close", filename=_error_filename(self._name))
        except NfsError as caught:
            child = caught.with_context(operation="close", filename=_error_filename(self._name))
            error = NfsFileCloseError(
                message=f"file close completed with errors: {caught}",
                operation="close",
                filename=_error_filename(self._name),
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
        name: _PathReference,
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
    def name(self) -> str | bytes:
        return _materialize_path(self._name)

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
                filename=_error_filename(self._name),
            )

    async def read(self, size: int = -1) -> bytes:
        self._check_loop()
        self._check_closed("read")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="read", filename=_error_filename(self._name))
        return await self._inner.read(size)

    async def readinto(self, target: Any) -> int:
        self._check_loop()
        self._check_closed("readinto")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="readinto", filename=_error_filename(self._name))
        with _writable_byte_view(target) as view:
            return await self._inner.readinto(view)

    async def read_at(self, offset: int, size: int = -1) -> bytes:
        self._check_loop()
        self._check_closed("read_at")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="read_at", filename=_error_filename(self._name))
        return await self._inner.read_at(offset, size)

    async def readinto_at(self, target: Any, offset: int) -> int:
        self._check_loop()
        self._check_closed("readinto_at")
        if not self.readable():
            raise NfsModeError(message="not readable", operation="readinto_at", filename=_error_filename(self._name))
        with _writable_byte_view(target) as view:
            return await self._inner.readinto_at(view, offset)

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
            raise NfsModeError(message="not writable", operation="write", filename=_error_filename(self._name))
        return await self._inner.write(_snapshot_bytes(data))

    async def write_at(self, data: Any, offset: int) -> int:
        self._check_loop()
        self._check_closed("write_at")
        if not self.writable():
            raise NfsModeError(message="not writable", operation="write_at", filename=_error_filename(self._name))
        if self._mode.startswith("a"):
            raise NfsModeError(
                message="positional writes are unavailable in append mode",
                operation="write_at",
                filename=_error_filename(self._name),
            )
        return await self._inner.write_at(_snapshot_bytes(data), offset)

    async def truncate(self, size: int | None = None) -> int:
        self._check_loop()
        self._check_closed("truncate")
        if not self.writable():
            raise NfsModeError(message="not writable", operation="truncate", filename=_error_filename(self._name))
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
            raise caught.with_context(operation="close", filename=_error_filename(self._name)) from caught
        except NfsError as caught:
            child = caught.with_context(operation="close", filename=_error_filename(self._name))
            raise NfsFileCloseError(
                message=f"file close completed with errors: {caught}",
                operation="close",
                filename=_error_filename(self._name),
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
