from __future__ import annotations

import errno
import io
import os

import pytest


pytestmark = pytest.mark.skipif(os.environ.get("NFS_RS_TEST_INSTALLED") != "1", reason="requires test-support wheel")


def test_protocol_statuses_have_structured_python_semantics() -> None:
    from nfs_rs import (
        Client, NfsNotFoundError, NfsOSError, NfsPermissionError,
        NfsRetryableError, NfsStateLostError, NfsUnsupportedError,
        RecoveryAction,
    )

    client = Client.connect("nfs-test://fixture/export")
    cases = [
        ("missing", NfsNotFoundError, 2, "NFS3ERR_NOENT"),
        ("denied", NfsPermissionError, 13, "NFS3ERR_ACCES"),
        ("__xdev__", NfsOSError, 18, "NFS3ERR_XDEV"),
        ("__stale__", NfsStateLostError, 70, "NFS3ERR_STALE"),
        ("__retry__", NfsRetryableError, 10008, "NFS3ERR_JUKEBOX"),
        ("__unsupported__", NfsUnsupportedError, 10004, "NFS3ERR_NOTSUPP"),
    ]
    for path, error_type, code, code_name in cases:
        with pytest.raises(error_type) as caught:
            client.stat(path)
        error = caught.value
        assert error.operation == "stat"
        assert error.protocol == "3"
        assert error.code == code
        assert error.code_name == code_name
        assert error.filename == path
    with pytest.raises(NfsOSError) as not_empty:
        client.stat("__notempty__")
    assert not_empty.value.errno == errno.ENOTEMPTY
    client.close()


def test_outcomes_completed_bytes_and_recovery_are_authoritative() -> None:
    from nfs_rs import (
        Client, NfsOperationOutcomeError, NfsPositionUncertainError,
        NfsUncertainOutcomeError, OperationClass, OperationOutcome,
        RecoveryAction,
    )

    client = Client.connect("nfs-test://fixture/export")
    with pytest.raises(NfsOperationOutcomeError) as before:
        client.remove("__before_send__")
    assert before.value.outcome is OperationOutcome.DEFINITE_FAILURE
    assert before.value.recovery_action is RecoveryAction.RETRY
    assert before.value.operation_class is OperationClass.REPLAY_SENSITIVE
    assert before.value.__cause__ is not None
    assert before.value.__cause__.operation == before.value.operation

    with pytest.raises(NfsUncertainOutcomeError) as after:
        client.rename("__after_send__", "destination")
    assert after.value.outcome is OperationOutcome.UNCERTAIN
    assert after.value.recovery_action is RecoveryAction.VERIFY_THEN_RESUME

    partial = client.open("__partial_write_error__", "w+b")
    with pytest.raises(NfsOperationOutcomeError) as partial_error:
        partial.write(b"abcde")
    assert partial_error.value.completed_bytes == 2

    uncertain = client.open("__zero_write__", "w+b")
    with pytest.raises(NfsUncertainOutcomeError) as zero_error:
        uncertain.write(b"abc")
    assert zero_error.value.completed_bytes == 0
    with pytest.raises(NfsPositionUncertainError):
        uncertain.read(1)
    client.close()


def test_closed_client_uses_dedicated_public_error() -> None:
    from nfs_rs import Client, NfsClientClosedError

    client = Client.connect("nfs-test://fixture/export")
    client.close()
    with pytest.raises(NfsClientClosedError):
        client.stat("fixture.bin")


def test_native_invalid_mode_and_read_size_keep_precise_categories() -> None:
    from nfs_rs import NfsInvalidInputError, NfsModeError, _internal

    client = _internal.SyncClient.connect("nfs-test://fixture/export")
    with pytest.raises(NfsModeError):
        client.open("fixture.bin", "invalid")
    file = client.open("fixture.bin", "rb")
    with pytest.raises(NfsInvalidInputError):
        file.read(-2)
    assert file.tell() == 0
    file.close()
    client.close()


def test_async_native_invalid_mode_and_read_size_match_sync() -> None:
    from nfs_rs import NfsInvalidInputError, NfsModeError, _internal

    async def scenario() -> None:
        client = await _internal.AsyncClient.connect("nfs-test://fixture/export")
        with pytest.raises(NfsModeError):
            await client.open("fixture.bin", "invalid")
        file = await client.open("fixture.bin", "rb")
        with pytest.raises(NfsInvalidInputError):
            await file.read(-2)
        assert file.tell() == 0
        await file.close()
        await client.close()

    import asyncio

    asyncio.run(scenario())


def test_local_file_errors_have_dedicated_public_classes() -> None:
    from nfs_rs import Client, NfsClientCloseError, NfsClosedResourceError, NfsFileCloseError, NfsModeError

    client = Client.connect("nfs-test://fixture/export")
    file = client.open("fixture.bin", "rb")
    with pytest.raises(NfsModeError) as mode:
        file.write(b"x")
    assert isinstance(mode.value, io.UnsupportedOperation)
    file.close()
    with pytest.raises(NfsClosedResourceError):
        file.read(1)
    client.close()

    failed_client = Client.connect("nfs-test://fixture/export")
    failed = failed_client.open("__commit_close_error__", "w+b")
    failed.write(b"dirty")
    with pytest.raises(NfsFileCloseError) as file_close:
        failed.close()
    assert len(file_close.value.errors) == 2
    assert [error.operation for error in file_close.value.errors] == ["commit", "close"]
    second = failed_client.open("__commit_error__", "w+b")
    second.write(b"dirty")
    with pytest.raises(NfsClientCloseError) as client_close:
        failed_client.close()
    assert len(client_close.value.errors) == 1


def test_async_classification_matches_sync() -> None:
    from nfs_rs import AsyncClient, NfsNotFoundError

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        with pytest.raises(NfsNotFoundError) as caught:
            await client.stat("missing")
        assert (caught.value.operation, caught.value.protocol, caught.value.filename) == ("stat", "3", "missing")
        await client.close()

    import asyncio

    asyncio.run(scenario())
