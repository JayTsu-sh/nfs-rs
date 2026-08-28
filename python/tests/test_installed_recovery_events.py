from __future__ import annotations

import os

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NFS_RS_TEST_INSTALLED") != "1",
    reason="requires test-support wheel",
)


@pytest.mark.parametrize("client_kind", ["sync", "async"])
def test_recovery_event_snapshot_drain_and_overflow(client_kind: str) -> None:
    from nfs_rs import AsyncClient, Client, OperationOutcome, RecoveryAction, RecoveryEvent

    if client_kind == "sync":
        client = Client.connect("nfs-test://fixture/export", recovery_event_capacity=2)
        native = client._inner._target
        for index in range(3):
            native._record_recovery_event(f"write-{index}", f"safe-{index}")
        snapshot = client.recovery_events()
        assert snapshot == client.recovery_events()
        drained = client.drain_recovery_events()
        assert client.recovery_events() == ()
        client.close()
    else:
        import asyncio

        async def scenario():
            client = await AsyncClient.connect(
                "nfs-test://fixture/export", recovery_event_capacity=2
            )
            native = client._inner._target
            for index in range(3):
                native._record_recovery_event(f"write-{index}", f"safe-{index}")
            snapshot = client.recovery_events()
            assert snapshot == client.recovery_events()
            drained = client.drain_recovery_events()
            assert client.recovery_events() == ()
            await client.close()
            return client, snapshot, drained

        client, snapshot, drained = asyncio.run(scenario())

    assert snapshot == drained
    assert len(snapshot) == 2
    assert all(isinstance(event, RecoveryEvent) for event in snapshot)
    assert [event.operation for event in snapshot] == ["write-1", "write-2"]
    assert snapshot[0].outcome is OperationOutcome.UNCERTAIN
    assert snapshot[0].recovery_action is RecoveryAction.VERIFY_THEN_RESUME
    assert snapshot[0].completed_bytes == 0
    assert snapshot[0].message == "redacted recovery event"
    assert client.dropped_recovery_event_count == 1
    with pytest.raises((AttributeError, TypeError)):
        snapshot[0].message = "mutated"


def test_cancelled_open_keeps_native_cancellation_and_records_later_uncertainty() -> None:
    import asyncio

    from nfs_rs import AsyncClient, OperationOutcome, RecoveryAction, _internal

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        _internal._arm_open_test_barrier()
        opening = asyncio.create_task(client.open("__blocked_open_uncertain__", "w+b"))
        await _internal._wait_open_test_entered()
        opening.cancel()
        with pytest.raises(asyncio.CancelledError):
            await opening
        assert client.recovery_events() == ()
        settled = _internal._wait_open_test_settled()
        _internal._release_open_test_barrier()
        await settled
        events = client.drain_recovery_events()
        assert len(events) == 1
        event = events[0]
        assert event.operation == "open"
        assert event.path == "__blocked_open_uncertain__"
        assert event.outcome is OperationOutcome.UNCERTAIN
        assert event.recovery_action is RecoveryAction.VERIFY_THEN_RESUME
        assert "scripted" not in event.message
        await client.close()

    asyncio.run(scenario())


def test_lost_open_state_rejects_unsafe_work_but_remains_closable() -> None:
    from nfs_rs import Client, NfsLostOpenStateError, RecoveryAction

    client = Client.connect("nfs-test://fixture/export")
    file = client.open("fixture.bin", "r+b")
    file._inner._target._lose_open_state()
    for operation in (
        lambda: file.read(1),
        lambda: file.write(b"x"),
        lambda: file.seek(0),
        file.flush,
        file.tell,
    ):
        with pytest.raises(NfsLostOpenStateError) as caught:
            operation()
        assert caught.value.recovery_action is RecoveryAction.REOPEN
        assert caught.value.filename == "fixture.bin"
    file.close()
    assert file.closed
    client.close()


def test_cancelled_client_close_waiter_does_not_cancel_cleanup() -> None:
    import asyncio

    from nfs_rs import AsyncClient, NfsClientClosedError, _internal

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        _internal._arm_open_test_barrier()
        opening = asyncio.create_task(client.open("__blocked_open__", "rb"))
        await _internal._wait_open_test_entered()
        closing = asyncio.create_task(client.close())
        await client._inner._target._wait_closing()
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        settled = _internal._wait_open_test_settled()
        _internal._release_open_test_barrier()
        await settled
        with pytest.raises(NfsClientClosedError):
            await opening
        await client.close()
        assert client.closed

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["read", "write", "flush", "close"])
def test_cancelled_file_waiters_settle_owned_work(operation: str) -> None:
    import asyncio

    from nfs_rs import AsyncClient, NfsFileCloseError, NfsUncertainOutcomeError, _internal

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        path = "__verifier_change__" if operation == "flush" else "fixture.bin"
        mode = "rb" if operation == "read" else "w+b"
        file = await client.open(path, mode)
        if operation == "flush":
            assert await file.write(b"dirty") == 5
        _internal._arm_operation_test_barrier(operation)
        if operation == "read":
            awaitable = file.read(1)
        elif operation == "write":
            awaitable = file.write(b"x")
        elif operation == "flush":
            awaitable = file.flush()
        else:
            awaitable = file.close()
        task = asyncio.create_task(awaitable)
        await _internal._wait_operation_test_entered()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        settled = _internal._wait_operation_test_settled()
        _internal._release_operation_test_barrier()
        await settled

        if operation == "read":
            assert file.tell() == 1
            assert client.recovery_events() == ()
            await file.close()
        elif operation == "write":
            assert file.tell() == 1
            assert await file.read_at(0, 1) == b"x"
            assert client.recovery_events() == ()
            await file.close()
        elif operation == "flush":
            events = client.drain_recovery_events()
            assert len(events) == 1
            assert events[0].operation == "commit"
            with pytest.raises(NfsUncertainOutcomeError):
                await file.flush()
            assert client.recovery_events() == ()
            with pytest.raises(NfsFileCloseError) as close_error:
                await file.close()
            assert isinstance(close_error.value.errors[0], NfsUncertainOutcomeError)
        else:
            assert file.closed
            await file.close()
        await client.close()

    asyncio.run(scenario())


def test_cancelled_file_close_reports_later_commit_uncertainty() -> None:
    import asyncio

    from nfs_rs import AsyncClient, OperationOutcome, _internal

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        file = await client.open("__verifier_change__", "w+b")
        await file.write(b"dirty")
        _internal._arm_operation_test_barrier("close")
        closing = asyncio.create_task(file.close())
        await _internal._wait_operation_test_entered()
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        settled = _internal._wait_operation_test_settled()
        _internal._release_operation_test_barrier()
        await settled
        events = client.drain_recovery_events()
        assert len(events) == 1
        assert events[0].operation == "commit"
        assert events[0].outcome is OperationOutcome.UNCERTAIN
        assert file.closed
        await client.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["remove", "rename", "symlink", "setxattr"])
def test_cancelled_path_mutations_report_later_uncertainty(operation: str) -> None:
    import asyncio

    from nfs_rs import AsyncClient, OperationOutcome, _internal

    async def scenario() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        _internal._arm_operation_test_barrier(operation)
        if operation == "remove":
            awaitable = client.remove("__after_send__")
        elif operation == "rename":
            awaitable = client.rename("__after_send__", "destination")
        elif operation == "symlink":
            awaitable = client.symlink("__after_send__", "safe-link")
        else:
            awaitable = client.setxattr("__after_send__", "user.key", b"value")
        task = asyncio.create_task(awaitable)
        await _internal._wait_operation_test_entered()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        settled = _internal._wait_operation_test_settled()
        _internal._release_operation_test_barrier()
        await settled
        events = client.drain_recovery_events()
        assert len(events) == 1
        expected_path = "safe-link" if operation == "symlink" else "__after_send__"
        assert events[0].path == expected_path
        assert events[0].outcome is OperationOutcome.UNCERTAIN
        assert "injected" not in events[0].message
        await client.close()

    asyncio.run(scenario())
