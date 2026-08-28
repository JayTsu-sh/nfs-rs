#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import importlib.metadata

from nfs_rs import AsyncClient, Client, __version__


assert __version__ == importlib.metadata.version("nfs-rs")

client = Client.connect("nfs-test://fixture/export")
file = client.open("fixture.bin", "rb")
assert file.read(3) == b"abc"
client.close()


async def smoke_async() -> None:
    client = await AsyncClient.connect("nfs-test://fixture/export")
    file = await client.open("fixture.bin", "rb")
    assert await file.read(3) == b"abc"
    await client.close()


asyncio.run(smoke_async())
