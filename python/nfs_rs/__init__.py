"""Stable Python facade for the nfs-rs userspace NFS client."""

from importlib.metadata import PackageNotFoundError, version

from ._client import (
    AsyncClient,
    Client,
    DirEntry,
    ExportEntry,
    File,
    FileInfo,
    FileType,
    Health,
    Lifecycle,
    Version,
    AsyncFile,
    list_exports,
    list_exports_async,
)

__all__ = [
    "AsyncClient",
    "Client",
    "DirEntry",
    "ExportEntry",
    "File",
    "FileInfo",
    "FileType",
    "Health",
    "Lifecycle",
    "Version",
    "AsyncFile",
    "list_exports",
    "list_exports_async",
]
try:
    __version__ = version("nfs-rs")
except PackageNotFoundError:
    __version__ = "0+unknown"
