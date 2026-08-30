"""Stable Python facade for the nfs-rs userspace NFS client."""

from importlib.metadata import PackageNotFoundError, version

from . import _errors as _public_errors
from ._errors import *

from ._client import (
    AceFlags,
    AceMask,
    AceType,
    Acl41Flags,
    AsyncClient,
    Capabilities,
    Client,
    DirEntry,
    ExportEntry,
    File,
    FileInfo,
    FileType,
    FsInfo,
    FsStat,
    Health,
    Lifecycle,
    IoLimits,
    NfsAce,
    NfsAcl41,
    RecoveryEvent,
    Version,
    AsyncFile,
    list_exports,
    list_exports_async,
)

__all__ = [
    "AceFlags",
    "AceMask",
    "AceType",
    "Acl41Flags",
    "AsyncClient",
    "Capabilities",
    "Client",
    "DirEntry",
    "ExportEntry",
    "File",
    "FileInfo",
    "FileType",
    "FsInfo",
    "FsStat",
    "Health",
    "Lifecycle",
    "IoLimits",
    "NfsAce",
    "NfsAcl41",
    "RecoveryEvent",
    "Version",
    "AsyncFile",
    "list_exports",
    "list_exports_async",
    "__version__",
]
__all__ += _public_errors.__all__
try:
    __version__ = version("nfs-rs")
except PackageNotFoundError:
    __version__ = "0+unknown"
