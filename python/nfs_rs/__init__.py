"""Stable Python facade for the nfs-rs userspace NFS client."""

from importlib.metadata import PackageNotFoundError, version

from ._client import (
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
    Version,
    AsyncFile,
    list_exports,
    list_exports_async,
)

__all__ = [
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
    "Version",
    "AsyncFile",
    "list_exports",
    "list_exports_async",
]
try:
    __version__ = version("nfs-rs")
except PackageNotFoundError:
    __version__ = "0+unknown"
