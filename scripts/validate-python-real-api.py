#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import nfs_rs


PUBLIC_API_COVERAGE = {
    "Client": (
        "access", "capabilities", "chmod", "chown", "close", "closed", "connect",
        "drain_recovery_events", "dropped_recovery_event_count", "exists", "fs_info",
        "fs_stat", "getxattr", "health", "io_limits", "link", "listdir", "listxattr",
        "mkdir", "open", "read_bytes", "readlink", "recovery_events", "remove",
        "removexattr", "rename", "rmdir", "scandir", "setxattr", "stat", "symlink",
        "touch", "truncate", "unlink", "utime", "version", "write_bytes",
    ),
    "AsyncClient": (
        "access", "capabilities", "chmod", "chown", "close", "closed", "connect",
        "drain_recovery_events", "dropped_recovery_event_count", "exists", "fs_info",
        "fs_stat", "getxattr", "health", "io_limits", "link", "listdir", "listxattr",
        "mkdir", "open", "read_bytes", "readlink", "recovery_events", "remove",
        "removexattr", "rename", "rmdir", "scandir", "setxattr", "stat", "symlink",
        "touch", "truncate", "unlink", "utime", "version", "write_bytes",
    ),
    "File": (
        "close", "closed", "fileno", "flush", "mode", "name", "read", "read_at",
        "readable", "readinto", "readinto_at", "seek", "seekable", "tell", "truncate",
        "writable", "write", "write_at",
    ),
    "AsyncFile": (
        "close", "closed", "flush", "mode", "name", "read", "read_at", "readable",
        "readinto", "readinto_at", "seek", "tell", "truncate", "writable", "write",
        "write_at",
    ),
    "module": ("list_exports", "list_exports_async"),
}

EXPECTED_UNAVAILABLE = (
    "Client.setxattr", "Client.getxattr", "Client.listxattr", "Client.removexattr",
    "AsyncClient.setxattr", "AsyncClient.getxattr", "AsyncClient.listxattr",
    "AsyncClient.removexattr",
)

SAFE_ID = re.compile(r"^nightly-[A-Za-z0-9._-]{1,80}$")


@dataclass(frozen=True)
class Case:
    name: str
    version: str
    url: str
    require_pnfs: bool


def parse_case(value: str) -> Case:
    parts = value.split("|", 3)
    if len(parts) != 4 or parts[1] not in {"3", "4.0", "4.1"}:
        raise argparse.ArgumentTypeError("case must be NAME|VERSION|URL|REQUIRE_PNFS")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", parts[0]):
        raise argparse.ArgumentTypeError("unsafe case name")
    if parts[3] not in {"true", "false"}:
        raise argparse.ArgumentTypeError("REQUIRE_PNFS must be true or false")
    return Case(parts[0], parts[1], parts[2], parts[3] == "true")


def conformance_url(case: Case, run_id: str) -> tuple[str, str]:
    parsed = urlsplit(case.url)
    if parsed.scheme != "nfs" or not parsed.hostname:
        raise ValueError(f"invalid NFS URL for {case.name}")
    identity = f"{case.name}\0{run_id}\0{os.getpid()}"
    leaf = "api-" + hashlib.sha256(identity.encode()).hexdigest()[:20]
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["version"] = case.version
    path = parsed.path.rstrip("/") + "/" + leaf
    return urlunsplit(parsed._replace(path=path, query=urlencode(query))), leaf


def check(condition: bool, name: str, checks: list[str]) -> None:
    if not condition:
        raise AssertionError(name)
    checks.append(name)
    print(f"PASS {name}", flush=True)


def covered_methods(checks: list[str], class_name: str) -> set[str]:
    prefix = class_name + "."
    covered: set[str] = set()
    for evidence in checks:
        label = evidence.split(":", 1)[0]
        if not label.startswith(prefix):
            continue
        for method in label.removeprefix(prefix).split("/"):
            covered.add(method.split(".", 1)[0].split(" ", 1)[0])
    return covered


def require_complete_coverage(checks: list[str], class_name: str) -> None:
    missing = set(PUBLIC_API_COVERAGE[class_name]) - covered_methods(checks, class_name)
    if missing:
        raise AssertionError(f"{class_name} methods did not execute: {sorted(missing)}")


def unavailable_methods(checks: list[str]) -> set[str]:
    return {
        evidence.split(":", 1)[0]
        for evidence in checks
        if evidence.endswith(":structured-unavailable")
    }


def capability_call(operation, name: str, checks: list[str], *, absent_ok: bool = False) -> None:
    try:
        operation()
    except (nfs_rs.NfsUnsupportedError, nfs_rs.NfsPermissionError):
        checks.append(name + ":structured-unavailable")
    except nfs_rs.NfsNotFoundError:
        if not absent_ok:
            raise
        checks.append(name + ":structured-unavailable")
    else:
        checks.append(name + ":success")


async def capability_call_async(operation, name: str, checks: list[str], *, absent_ok: bool = False) -> None:
    try:
        await operation()
    except (nfs_rs.NfsUnsupportedError, nfs_rs.NfsPermissionError):
        checks.append(name + ":structured-unavailable")
    except nfs_rs.NfsNotFoundError:
        if not absent_ok:
            raise
        checks.append(name + ":structured-unavailable")
    else:
        checks.append(name + ":success")


def remove_tree(client: nfs_rs.Client, path: str) -> None:
    if not client.exists(path):
        return
    info = client.stat(path)
    if info.type == nfs_rs.FileType.DIRECTORY:
        entries = list(client.scandir(path))
        for entry in entries:
            if entry.name not in {".", ".."}:
                remove_tree(client, entry.path)
        client.rmdir(path)
    else:
        client.remove(path, missing_ok=True)


def sync_file_scenario(client: nfs_rs.Client, root: str, checks: list[str]) -> None:
    path = f"{root}/sync-file.bin"
    opened = client.open(path, "w+b")
    checks.append("Client.open")
    check(opened.name == path and opened.mode == "w+b", "File.name/mode", checks)
    check(opened.readable() and opened.writable() and opened.seekable(), "File.readable/writable/seekable", checks)
    check(opened.write(b"abcdef") == 6 and opened.tell() == 6, "File.write/tell", checks)
    check(opened.seek(0) == 0 and opened.read(3) == b"abc", "File.seek/read", checks)
    target = bytearray(3)
    check(opened.readinto(target) == 3 and target == b"def", "File.readinto", checks)
    check(opened.read_at(1, 3) == b"bcd" and opened.tell() == 6, "File.read_at", checks)
    target_at = bytearray(2)
    check(opened.readinto_at(target_at, 2) == 2 and target_at == b"cd", "File.readinto_at", checks)
    check(opened.write_at(b"XY", 2) == 2 and opened.tell() == 6, "File.write_at", checks)
    check(opened.truncate(5) == 5, "File.truncate", checks)
    opened.flush()
    checks.append("File.flush")
    try:
        opened.fileno()
    except io.UnsupportedOperation:
        checks.append("File.fileno")
    else:
        raise AssertionError("File.fileno must be unsupported")
    opened.close()
    check(opened.closed, "File.close/closed", checks)
    opened.close()
    require_complete_coverage(checks, "File")


def sync_client_scenario(url: str, case: Case, root: str) -> list[str]:
    checks: list[str] = []
    client = nfs_rs.Client.connect(url)
    try:
        check(client.version.value == case.version, "Client.connect/version", checks)
        check(client.health.lifecycle == nfs_rs.Lifecycle.READY, "Client.health", checks)
        check(client.io_limits.max_read > 0 and client.io_limits.max_write > 0, "Client.io_limits", checks)
        capabilities = client.capabilities
        if case.require_pnfs:
            check(capabilities.pnfs, "Client.capabilities.pnfs", checks)
        else:
            checks.append("Client.capabilities")
        check(not client.closed, "Client.closed.ready", checks)
        check(client.recovery_events() == (), "Client.recovery_events", checks)
        check(client.drain_recovery_events() == (), "Client.drain_recovery_events", checks)
        check(client.dropped_recovery_event_count == 0, "Client.dropped_recovery_event_count", checks)

        nested = f"{root}/nested"
        data = f"{root}/data.bin"
        renamed = f"{root}/renamed.bin"
        hard_link = f"{root}/hard-link.bin"
        symbolic_link = f"{root}/symbolic-link"
        touched = f"{root}/touched.bin"
        client.mkdir(f"{nested}/child", parents=True)
        client.mkdir(nested, exist_ok=True)
        check(client.exists(f"{nested}/child"), "Client.mkdir/exists", checks)
        check(client.write_bytes(data, b"payload") == 7, "Client.write_bytes", checks)
        check(client.read_bytes(data) == b"payload", "Client.read_bytes", checks)
        info = client.stat(data)
        check(info.size == 7 and info.type == nfs_rs.FileType.FILE, "Client.stat", checks)
        names = client.listdir(root)
        check("data.bin" in names, "Client.listdir", checks)
        entries = {entry.name: entry for entry in client.scandir(root)}
        check(entries["data.bin"].info.size == 7, "Client.scandir", checks)
        check(client.access(data, os.R_OK), "Client.access", checks)
        fs_info = client.fs_info()
        check(fs_info.max_read > 0 and fs_info.max_write > 0, "Client.fs_info", checks)
        fs_stat = client.fs_stat()
        check(fs_stat.total_bytes >= fs_stat.free_bytes, "Client.fs_stat", checks)

        client.chmod(data, 0o640)
        checks.append("Client.chmod")
        current = client.stat(data)
        capability_call(lambda: client.chown(data, current.uid, current.gid), "Client.chown", checks)
        client.utime(data, ns=(1_700_000_000_000_000_000, 1_700_000_001_000_000_000))
        checks.append("Client.utime")
        client.truncate(data, 4)
        check(client.stat(data).size == 4, "Client.truncate", checks)

        xattr = "user.nfs_rs_conformance"
        try:
            client.setxattr(data, xattr, b"value")
        except (nfs_rs.NfsUnsupportedError, nfs_rs.NfsPermissionError):
            checks.append("Client.setxattr:structured-unavailable")
            capability_call(lambda: client.getxattr(data, xattr), "Client.getxattr", checks, absent_ok=True)
            capability_call(lambda: client.listxattr(data), "Client.listxattr", checks)
            capability_call(lambda: client.removexattr(data, xattr), "Client.removexattr", checks, absent_ok=True)
        else:
            check(client.getxattr(data, xattr) == b"value", "Client.getxattr", checks)
            check(xattr in client.listxattr(data), "Client.listxattr", checks)
            client.removexattr(data, xattr)
            checks.extend(("Client.setxattr", "Client.removexattr"))

        client.rename(data, renamed)
        check(client.exists(renamed), "Client.rename", checks)
        if fs_info.supports_links:
            client.link(renamed, hard_link)
            check(client.read_bytes(hard_link) == b"payl", "Client.link", checks)
        else:
            capability_call(lambda: client.link(renamed, hard_link), "Client.link", checks)
        if fs_info.supports_symlinks:
            client.symlink("renamed.bin", symbolic_link)
            check(client.readlink(symbolic_link) == "renamed.bin", "Client.symlink/readlink", checks)
        else:
            capability_call(lambda: client.symlink("renamed.bin", symbolic_link), "Client.symlink", checks)
            capability_call(lambda: client.readlink(symbolic_link), "Client.readlink", checks, absent_ok=True)
        client.touch(touched)
        check(client.exists(touched), "Client.touch", checks)
        sync_file_scenario(client, root, checks)

        client.unlink(touched)
        checks.append("Client.unlink")
        client.remove(f"{root}/missing.bin", missing_ok=True)
        checks.append("Client.remove")
        client.rmdir(f"{nested}/child")
        client.rmdir(nested)
        checks.append("Client.rmdir")
    finally:
        client.close()
    check(client.closed, "Client.close/closed", checks)
    client.close()
    with nfs_rs.Client.connect(url) as context_client:
        check(not context_client.closed, "Client context manager", checks)
    require_complete_coverage(checks, "Client")
    return checks


async def async_file_scenario(client: nfs_rs.AsyncClient, root: str, checks: list[str]) -> None:
    path = f"{root}/async-file.bin"
    opened = await client.open(path, "w+b")
    checks.append("AsyncClient.open")
    check(opened.name == path and opened.mode == "w+b", "AsyncFile.name/mode", checks)
    check(opened.readable() and opened.writable(), "AsyncFile.readable/writable", checks)
    check(await opened.write(b"abcdef") == 6 and opened.tell() == 6, "AsyncFile.write/tell", checks)
    check(await opened.seek(0) == 0 and await opened.read(3) == b"abc", "AsyncFile.seek/read", checks)
    target = bytearray(3)
    check(await opened.readinto(target) == 3 and target == b"def", "AsyncFile.readinto", checks)
    check(await opened.read_at(1, 3) == b"bcd" and opened.tell() == 6, "AsyncFile.read_at", checks)
    target_at = bytearray(2)
    check(await opened.readinto_at(target_at, 2) == 2 and target_at == b"cd", "AsyncFile.readinto_at", checks)
    check(await opened.write_at(b"XY", 2) == 2 and opened.tell() == 6, "AsyncFile.write_at", checks)
    check(await opened.truncate(5) == 5, "AsyncFile.truncate", checks)
    await opened.flush()
    checks.append("AsyncFile.flush")
    await opened.close()
    check(opened.closed, "AsyncFile.close/closed", checks)
    await opened.close()
    require_complete_coverage(checks, "AsyncFile")


async def async_client_scenario(url: str, case: Case, root: str) -> list[str]:
    checks: list[str] = []
    client = await nfs_rs.AsyncClient.connect(url)
    try:
        check(client.version.value == case.version, "AsyncClient.connect/version", checks)
        check(client.health.lifecycle == nfs_rs.Lifecycle.READY, "AsyncClient.health", checks)
        check(client.io_limits.max_read > 0 and client.io_limits.max_write > 0, "AsyncClient.io_limits", checks)
        capabilities = client.capabilities
        if case.require_pnfs:
            check(capabilities.pnfs, "AsyncClient.capabilities.pnfs", checks)
        else:
            checks.append("AsyncClient.capabilities")
        check(not client.closed, "AsyncClient.closed.ready", checks)
        check(client.recovery_events() == (), "AsyncClient.recovery_events", checks)
        check(client.drain_recovery_events() == (), "AsyncClient.drain_recovery_events", checks)
        check(client.dropped_recovery_event_count == 0, "AsyncClient.dropped_recovery_event_count", checks)

        nested = f"{root}/async-nested"
        data = f"{root}/async-data.bin"
        renamed = f"{root}/async-renamed.bin"
        hard_link = f"{root}/async-hard-link.bin"
        symbolic_link = f"{root}/async-symbolic-link"
        touched = f"{root}/async-touched.bin"
        await client.mkdir(f"{nested}/child", parents=True)
        await client.mkdir(nested, exist_ok=True)
        check(await client.exists(f"{nested}/child"), "AsyncClient.mkdir/exists", checks)
        check(await client.write_bytes(data, b"payload") == 7, "AsyncClient.write_bytes", checks)
        check(await client.read_bytes(data) == b"payload", "AsyncClient.read_bytes", checks)
        info = await client.stat(data)
        check(info.size == 7, "AsyncClient.stat", checks)
        check("async-data.bin" in await client.listdir(root), "AsyncClient.listdir", checks)
        entries = {entry.name: entry async for entry in client.scandir(root)}
        check(entries["async-data.bin"].info.size == 7, "AsyncClient.scandir", checks)
        check(await client.access(data, os.R_OK), "AsyncClient.access", checks)
        fs_info = await client.fs_info()
        check(fs_info.max_read > 0 and fs_info.max_write > 0, "AsyncClient.fs_info", checks)
        fs_stat = await client.fs_stat()
        check(fs_stat.total_bytes >= fs_stat.free_bytes, "AsyncClient.fs_stat", checks)

        await client.chmod(data, 0o640)
        checks.append("AsyncClient.chmod")
        current = await client.stat(data)
        await capability_call_async(lambda: client.chown(data, current.uid, current.gid), "AsyncClient.chown", checks)
        await client.utime(data, ns=(1_700_000_000_000_000_000, 1_700_000_001_000_000_000))
        checks.append("AsyncClient.utime")
        await client.truncate(data, 4)
        check((await client.stat(data)).size == 4, "AsyncClient.truncate", checks)

        xattr = "user.nfs_rs_conformance"
        try:
            await client.setxattr(data, xattr, b"value")
        except (nfs_rs.NfsUnsupportedError, nfs_rs.NfsPermissionError):
            checks.append("AsyncClient.setxattr:structured-unavailable")
            await capability_call_async(lambda: client.getxattr(data, xattr), "AsyncClient.getxattr", checks, absent_ok=True)
            await capability_call_async(lambda: client.listxattr(data), "AsyncClient.listxattr", checks)
            await capability_call_async(lambda: client.removexattr(data, xattr), "AsyncClient.removexattr", checks, absent_ok=True)
        else:
            check(await client.getxattr(data, xattr) == b"value", "AsyncClient.getxattr", checks)
            check(xattr in await client.listxattr(data), "AsyncClient.listxattr", checks)
            await client.removexattr(data, xattr)
            checks.extend(("AsyncClient.setxattr", "AsyncClient.removexattr"))

        await client.rename(data, renamed)
        check(await client.exists(renamed), "AsyncClient.rename", checks)
        if fs_info.supports_links:
            await client.link(renamed, hard_link)
            check(await client.read_bytes(hard_link) == b"payl", "AsyncClient.link", checks)
        else:
            await capability_call_async(lambda: client.link(renamed, hard_link), "AsyncClient.link", checks)
        if fs_info.supports_symlinks:
            await client.symlink("async-renamed.bin", symbolic_link)
            check(await client.readlink(symbolic_link) == "async-renamed.bin", "AsyncClient.symlink/readlink", checks)
        else:
            await capability_call_async(lambda: client.symlink("async-renamed.bin", symbolic_link), "AsyncClient.symlink", checks)
            await capability_call_async(lambda: client.readlink(symbolic_link), "AsyncClient.readlink", checks, absent_ok=True)
        await client.touch(touched)
        check(await client.exists(touched), "AsyncClient.touch", checks)
        await async_file_scenario(client, root, checks)

        await client.unlink(touched)
        checks.append("AsyncClient.unlink")
        await client.remove(f"{root}/missing.bin", missing_ok=True)
        checks.append("AsyncClient.remove")
        await client.rmdir(f"{nested}/child")
        await client.rmdir(nested)
        checks.append("AsyncClient.rmdir")
    finally:
        await client.close()
    check(client.closed, "AsyncClient.close/closed", checks)
    await client.close()
    async with await nfs_rs.AsyncClient.connect(url) as context_client:
        check(not context_client.closed, "AsyncClient context manager", checks)
    require_complete_coverage(checks, "AsyncClient")
    return checks


def export_scenario(case: Case) -> list[str]:
    if case.version != "3":
        return []
    parsed = urlsplit(case.url)
    host = urlunsplit(parsed._replace(path="/", query="", fragment=""))
    sync_exports = nfs_rs.list_exports(host, noresvport=True)
    async_exports = asyncio.run(nfs_rs.list_exports_async(host, noresvport=True))
    if not sync_exports or sync_exports != async_exports:
        raise AssertionError("sync/async export discovery mismatch")
    return ["module.list_exports", "module.list_exports_async"]


def validate_case(case: Case, run_id: str) -> dict[str, object]:
    _, leaf = conformance_url(case, run_id)
    parent = nfs_rs.Client.connect(case.url)
    primary_error: Exception | None = None
    sync_checks: list[str] = []
    async_checks: list[str] = []
    try:
        parent.mkdir(leaf)
        try:
            sync_checks = sync_client_scenario(case.url, case, leaf)
        except Exception as error:
            primary_error = RuntimeError(f"sync scenario failed: {type(error).__name__}: {error}")
        if primary_error is None:
            try:
                async_checks = asyncio.run(async_client_scenario(case.url, case, leaf))
            except Exception as error:
                primary_error = RuntimeError(f"async scenario failed: {type(error).__name__}: {error}")
    finally:
        try:
            try:
                remove_tree(parent, leaf)
            except Exception as cleanup_error:
                if primary_error is None:
                    primary_error = RuntimeError(
                        f"cleanup failed: {type(cleanup_error).__name__}: {cleanup_error}"
                    )
                else:
                    primary_error.add_note(
                        f"cleanup also failed: {type(cleanup_error).__name__}: {cleanup_error}"
                    )
        finally:
            parent.close()
    if primary_error is not None:
        raise primary_error
    unavailable = unavailable_methods(sync_checks + async_checks)
    expected_unavailable = set(EXPECTED_UNAVAILABLE)
    if unavailable != expected_unavailable:
        raise AssertionError(
            "real capability contract changed: "
            f"expected unavailable={sorted(expected_unavailable)}, actual={sorted(unavailable)}"
        )
    return {
        "case": asdict(case),
        "sync_checks": sync_checks,
        "async_checks": async_checks,
        "module_checks": export_scenario(case),
        "unavailable_methods": sorted(unavailable),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", type=parse_case, action="append", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if not SAFE_ID.fullmatch(arguments.run_id):
        parser.error("unsafe nightly run id")
    result: dict[str, object] = {
        "schema_version": 1,
        "run_id": arguments.run_id,
        "nfs_rs_version": nfs_rs.__version__,
        "public_api_coverage": PUBLIC_API_COVERAGE,
        "results": [],
    }
    failures: list[str] = []
    for case in arguments.case:
        try:
            result["results"].append(validate_case(case, arguments.run_id))  # type: ignore[union-attr]
        except Exception as error:
            failures.append(f"{case.name}: {type(error).__name__}: {error}")
    result["failures"] = failures
    result["status"] = "failed" if failures else "passed"
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if failures:
        raise RuntimeError("; ".join(failures))


if __name__ == "__main__":
    main()
