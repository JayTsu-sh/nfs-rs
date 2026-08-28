"""Stable Python facade for the nfs-rs userspace NFS client."""

from importlib.metadata import PackageNotFoundError, version

from ._client import AsyncClient, Client, Health, Lifecycle, Version

__all__ = ["AsyncClient", "Client", "Health", "Lifecycle", "Version"]
try:
    __version__ = version("nfs-rs")
except PackageNotFoundError:
    __version__ = "0+unknown"
