from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any, Literal

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NFS_RS_TEST_INSTALLED") != "1",
    reason="requires the non-editable test-support wheel",
)


@dataclass(frozen=True)
class Call:
    method: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MutationCase:
    target: Literal["client", "file"]
    call: Call
    mode: str | None = None
    prepare_dirty: bool = False


CLIENT_CASES = {
    "stat": Call("stat", ("file",)),
    "exists": Call("exists", ("file",)),
    "scandir": Call("scandir", ("folder",)),
    "listdir": Call("listdir", ("folder",)),
    "chmod": Call("chmod", ("file", 0o600)),
    "chown": Call("chown", ("file", 1, 2)),
    "utime": Call("utime", ("file",), {"ns": (1, 2)}),
    "truncate": Call("truncate", ("file", 1)),
    "access": Call("access", ("file", os.R_OK)),
    "getxattr": Call("getxattr", ("file", "user.key")),
    "setxattr": Call("setxattr", ("file", "user.key", b"value")),
    "listxattr": Call("listxattr", ("file",)),
    "removexattr": Call("removexattr", ("file", "user.key")),
    "fs_info": Call("fs_info"),
    "fs_stat": Call("fs_stat"),
    "mkdir": Call("mkdir", ("directory",)),
    "remove": Call("remove", ("file",)),
    "unlink": Call("unlink", ("file",)),
    "rmdir": Call("rmdir", ("directory",)),
    "rename": Call("rename", ("source", "destination")),
    "link": Call("link", ("source", "destination")),
    "symlink": Call("symlink", ("target", "link")),
    "readlink": Call("readlink", ("link",)),
    "touch": Call("touch", ("file",)),
    "read_bytes": Call("read_bytes", ("fixture.bin",)),
    "write_bytes": Call("write_bytes", ("file", b"value")),
    "open": Call("open", ("fixture.bin", "rb")),
}

FILE_CASES = {
    "read": Call("read", (1,)),
    "readinto": Call("readinto", (bytearray(2),)),
    "read_at": Call("read_at", (0, 1)),
    "readinto_at": Call("readinto_at", (bytearray(2), 0)),
    "seek": Call("seek", (0,)),
    "tell": Call("tell"),
    "write": Call("write", (b"x",)),
    "write_at": Call("write_at", (b"x", 0)),
    "truncate": Call("truncate", (1,)),
    "flush": Call("flush"),
}

MUTATION_CASES = {
    "write": MutationCase("file", Call("write", (b"value",)), "w+b"),
    "create": MutationCase("client", Call("open", ("contract-create.bin", "w+b"))),
    "truncate": MutationCase("file", Call("truncate", (7,)), "r+b"),
    "rename": MutationCase("client", Call("rename", ("source", "destination"))),
    "remove": MutationCase("client", Call("remove", ("file",))),
    "mkdir": MutationCase("client", Call("mkdir", ("directory",))),
    "link": MutationCase("client", Call("link", ("source", "link"))),
    "symlink": MutationCase("client", Call("symlink", ("target", "link"))),
    "commit": MutationCase("file", Call("flush"), "w+b", prepare_dirty=True),
    "open": MutationCase("client", Call("open", ("fixture.bin", "rb"))),
}

FAULT_PHASES = ("before-send", "after-send-before-response")
BOUND_SECONDS = 2.0


def _invoke_sync(target: Any, case: Call) -> Any:
    result = getattr(target, case.method)(*case.args, **case.kwargs)
    if case.method == "scandir":
        return next(result)
    return result


async def _invoke_async(target: Any, case: Call) -> Any:
    result = getattr(target, case.method)(*case.args, **case.kwargs)
    if case.method == "scandir":
        return await anext(result)
    if asyncio.iscoroutine(result):
        return await result
    return result


async def _finish_fault_task(task: asyncio.Task[Any], internal: Any) -> Any:
    entry_error: BaseException | None = None
    try:
        await asyncio.wait_for(internal._wait_fault_test_entered(), BOUND_SECONDS)
    except BaseException as error:
        entry_error = error
    finally:
        internal._release_fault_test_barrier()
    try:
        result = await asyncio.wait_for(asyncio.shield(task), BOUND_SECONDS)
    except BaseException as error:
        result = error
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    if entry_error is not None:
        raise entry_error
    return result


async def _run_fault(client_kind: str, operation: str, phase: str) -> BaseException:
    from nfs_rs import AsyncClient, Client, _internal

    mutation = MUTATION_CASES[operation]
    _internal._arm_fault_test_barrier(operation, phase)
    if client_kind == "sync":
        client = Client.connect("nfs-test://fixture/export")
        file = client.open(f"contract-{operation}.bin", mutation.mode) if mutation.mode else None
        if mutation.prepare_dirty:
            assert file.write(b"dirty") == 5
        target = file if mutation.target == "file" else client
        task = asyncio.create_task(asyncio.to_thread(_invoke_sync, target, mutation.call))
        try:
            result = await _finish_fault_task(task, _internal)
        finally:
            if file is not None:
                file.close()
            client.close()
        assert isinstance(result, BaseException)
        return result

    client = await AsyncClient.connect("nfs-test://fixture/export")
    file = (
        await client.open(f"contract-{operation}.bin", mutation.mode)
        if mutation.mode else None
    )
    if mutation.prepare_dirty:
        assert await file.write(b"dirty") == 5
    target = file if mutation.target == "file" else client
    task = asyncio.create_task(_invoke_async(target, mutation.call))
    try:
        result = await _finish_fault_task(task, _internal)
    finally:
        if file is not None:
            await file.close()
        await client.close()
    assert isinstance(result, BaseException)
    return result


@pytest.mark.parametrize("client_kind", ("sync", "async"))
@pytest.mark.parametrize("phase", FAULT_PHASES)
@pytest.mark.parametrize("operation", tuple(MUTATION_CASES))
def test_every_first_release_mutation_has_a_deterministic_fault_gate(
    client_kind: str, phase: str, operation: str,
) -> None:
    from nfs_rs import (
        NfsOperationOutcomeError, NfsUncertainOutcomeError, OperationClass,
        OperationOutcome, RecoveryAction,
    )

    error = asyncio.run(_run_fault(client_kind, operation, phase))
    if phase == "before-send":
        assert isinstance(error, NfsOperationOutcomeError)
        assert error.outcome is OperationOutcome.DEFINITE_FAILURE
        assert error.recovery_action is RecoveryAction.RETRY
        assert error.completed_bytes in {None, 0}
    else:
        assert isinstance(error, NfsUncertainOutcomeError)
        assert error.outcome is OperationOutcome.UNCERTAIN
        assert error.recovery_action is RecoveryAction.VERIFY_THEN_RESUME
        if operation == "write":
            assert error.completed_bytes == 5
    assert error.operation_class is OperationClass.REPLAY_SENSITIVE
    assert error.operation in {operation, "flush"}
    assert error.protocol == "4.1"


@pytest.mark.parametrize("client_kind", ("sync", "async"))
@pytest.mark.parametrize("operation", tuple(CLIENT_CASES))
def test_every_operational_client_method_has_matching_success_and_failure_coverage(
    client_kind: str, operation: str,
) -> None:
    from nfs_rs import AsyncClient, AsyncFile, Client, File, NfsClientClosedError

    case = CLIENT_CASES[operation]
    if client_kind == "sync":
        client = Client.connect("nfs-test://fixture/export")
        if operation in {"getxattr", "listxattr", "removexattr"}:
            client.setxattr("file", "user.key", b"value")
        result = _invoke_sync(client, case)
        if isinstance(result, File):
            result.close()
        client.close()
        with pytest.raises(NfsClientClosedError):
            _invoke_sync(client, case)
        return

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        if operation in {"getxattr", "listxattr", "removexattr"}:
            await client.setxattr("file", "user.key", b"value")
        result = await _invoke_async(client, case)
        if isinstance(result, AsyncFile):
            await result.close()
        await client.close()
        with pytest.raises(NfsClientClosedError):
            await _invoke_async(client, case)

    asyncio.run(scenario())


@pytest.mark.parametrize("client_kind", ("sync", "async"))
@pytest.mark.parametrize("operation", tuple(FILE_CASES))
def test_every_operational_file_method_has_matching_success_and_failure_coverage(
    client_kind: str, operation: str,
) -> None:
    from nfs_rs import AsyncClient, Client, NfsClosedResourceError

    case = FILE_CASES[operation]
    if client_kind == "sync":
        client = Client.connect("nfs-test://fixture/export")
        file = client.open("fixture.bin", "r+b")
        _invoke_sync(file, case)
        file.close()
        with pytest.raises(NfsClosedResourceError):
            _invoke_sync(file, case)
        client.close()
        return

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        file = await client.open("fixture.bin", "r+b")
        await _invoke_async(file, case)
        await file.close()
        with pytest.raises(NfsClosedResourceError):
            await _invoke_async(file, case)
        await client.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("client_kind", ("sync", "async"))
@pytest.mark.parametrize(
    ("path", "error_name", "outcome", "recovery"),
    (
        ("__stale__", "NfsStateLostError", None, "remount"),
        ("__lease_lost__", "NfsStateLostError", None, "reopen"),
        ("__session_lost__", "NfsStateLostError", None, "reopen"),
        ("__callback_failure__", "NfsRetryableError", None, "retry"),
        ("__pnfs_data_path_failure__", "NfsRetryableError", "safe_to_retry", "retry"),
    ),
)
def test_protocol_state_callback_and_pnfs_failures_are_structured(
    client_kind: str, path: str, error_name: str, outcome: str | None, recovery: str,
) -> None:
    import nfs_rs

    async def scenario() -> BaseException:
        if client_kind == "sync":
            client = nfs_rs.Client.connect("nfs-test://fixture/export")
            try:
                if path == "__pnfs_data_path_failure__":
                    with client.open(path, "rb") as file:
                        return await asyncio.to_thread(file.read, 1)
                return await asyncio.to_thread(client.stat, path)
            except BaseException as error:
                return error
            finally:
                client.close()
        client = await nfs_rs.AsyncClient.connect("nfs-test://fixture/export")
        try:
            if path == "__pnfs_data_path_failure__":
                async with await client.open(path, "rb") as file:
                    return await file.read(1)
            return await client.stat(path)
        except BaseException as error:
            return error
        finally:
            await client.close()

    error = asyncio.run(scenario())
    assert type(error).__name__ == error_name
    assert error.recovery_action.value == recovery
    assert error.outcome is None if outcome is None else error.outcome.value == outcome
    assert error.protocol in {"3", "4.1"}
    assert error.filename == path


def test_every_public_exception_has_stable_installed_shape_and_mro() -> None:
    import nfs_rs

    error_names = {
        name for name in nfs_rs.__all__ if name.startswith("Nfs") and name.endswith("Error")
    }
    expected = {
        "NfsError", "NfsNotFoundError", "NfsAlreadyExistsError", "NfsPermissionError",
        "NfsIsADirectoryError", "NfsNotADirectoryError", "NfsTimeoutError",
        "NfsConnectionError", "NfsOSError", "NfsMountError", "NfsRpcError",
        "NfsEncodingError", "NfsDirectoryEntryError", "NfsUnsupportedError",
        "NfsInvalidInputError", "NfsProtocolError", "NfsStateLostError",
        "NfsRetryableError", "NfsOperationOutcomeError", "NfsUncertainOutcomeError",
        "NfsPositionUncertainError", "NfsLostOpenStateError", "NfsClosedResourceError",
        "NfsClientClosedError", "NfsModeError", "NfsFileCloseError", "NfsClientCloseError",
    }
    assert error_names == expected
    for name in sorted(error_names):
        error_type = getattr(nfs_rs, name)
        error = error_type(message="contract failure", operation="contract", protocol="4.1")
        assert isinstance(error, nfs_rs.NfsError)
        assert isinstance(error, RuntimeError)
        assert error.message == "contract failure"
        with pytest.raises(AttributeError):
            error.message = "changed"

    assert issubclass(nfs_rs.NfsNotFoundError, FileNotFoundError)
    assert issubclass(nfs_rs.NfsPermissionError, PermissionError)
    assert issubclass(nfs_rs.NfsTimeoutError, TimeoutError)
    assert issubclass(nfs_rs.NfsModeError, OSError)


def test_non_operational_and_module_level_contracts_are_explicit() -> None:
    from nfs_rs import Client, NfsModeError, Version, list_exports, list_exports_async

    assert list_exports("nfs-test://fixture/") == asyncio.run(
        list_exports_async("nfs-test://fixture/")
    )
    with pytest.raises(ValueError):
        list_exports("nfs://")
    with pytest.raises(ValueError):
        asyncio.run(list_exports_async("nfs://"))
    with pytest.raises(ValueError):
        Client.connect("nfs://")

    with Client.connect("nfs-test://fixture/export") as client:
        assert client.version is Version.NFS_V4_1
        assert client.health.lifecycle.value == "ready"
        assert client.capabilities.pnfs is False
        assert client.io_limits.max_read == 4
        assert client.dropped_recovery_event_count == 0
        with client.open("fixture.bin", "r+b") as file:
            assert file.name == "fixture.bin"
            assert file.mode == "r+b"
            assert file.readable() and file.writable() and file.seekable()
            with pytest.raises(NfsModeError):
                file.fileno()
    client.close()
    assert client.closed
    assert client.version is Version.NFS_V4_1
    assert client.health.lifecycle.value == "closed"
    assert client.capabilities.pnfs is False
    assert client.io_limits.max_read == 4
    assert file.closed and file.name == "fixture.bin" and file.mode == "r+b"
    assert client.recovery_events() == ()
    assert client.drain_recovery_events() == ()

    async def async_contract() -> None:
        from nfs_rs import AsyncClient

        with pytest.raises(ValueError):
            await AsyncClient.connect("nfs://")

        async with await AsyncClient.connect("nfs-test://fixture/export") as async_client:
            assert async_client.version is Version.NFS_V4_1
            assert async_client.health.lifecycle.value == "ready"
            assert async_client.capabilities.pnfs is False
            assert async_client.io_limits.max_write == 4
            async with await async_client.open("fixture.bin", "r+b") as file:
                assert file.name == "fixture.bin"
                assert file.mode == "r+b"
                assert file.readable() and file.writable()
        await async_client.close()
        assert async_client.closed
        assert async_client.version is Version.NFS_V4_1
        assert async_client.health.lifecycle.value == "closed"
        assert async_client.capabilities.pnfs is False
        assert async_client.io_limits.max_write == 4
        assert file.closed and file.name == "fixture.bin" and file.mode == "r+b"
        assert async_client.recovery_events() == ()
        assert async_client.drain_recovery_events() == ()

    asyncio.run(async_contract())
