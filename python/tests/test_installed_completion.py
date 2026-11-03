"""Deterministic bridge-teardown tests; each case is bounded by a subprocess."""
import os
import subprocess
import sys

import pytest

if os.environ.get("NFS_RS_TEST_INSTALLED") != "1":
    pytest.skip("requires an installed test-support wheel", allow_module_level=True)


SCRIPT = r'''
import asyncio
import sys
import threading
from nfs_rs import _internal

async def main():
    outcome = sys.argv[1]
    future, barrier = _internal._completion_test_future(outcome)
    resumed = threading.Event()
    errors = []

    def observer():
        try:
            # Both the event-loop fence and sender must reach the native pause.
            # No sleeps or server timing determine this interleaving.
            barrier.wait_until_fenced()
            assert barrier.pending_count() > 0
            assert not resumed.is_set(), "awaiter resumed before notification teardown"
        except BaseException as error:
            errors.append(error)
        finally:
            barrier.release()

    thread = threading.Thread(target=observer)
    thread.start()

    async def consume():
        try:
            if outcome == "success":
                assert await future == 42
            elif outcome == "error":
                try:
                    await future
                except ValueError as error:
                    assert str(error) == "injected bridge failure"
                else:
                    raise AssertionError("missing native exception")
            else:
                try:
                    await future
                except asyncio.CancelledError:
                    pass
                else:
                    raise AssertionError("missing cancellation")
        finally:
            resumed.set()
        assert barrier.pending_count() == 0, "notification work is still active"

    try:
        if outcome == "cancel":
            task = asyncio.create_task(consume())
            # Register the awaiting task before cancellation; this yields a loop
            # turn rather than relying on a wall-clock delay.
            await asyncio.sleep(0)
            future.cancel()
            await task
        else:
            # Do not schedule a separate consumer: result publication may
            # already be queued, and that task could await a done Future before
            # its queued fence callback runs. Match the public immediate await.
            await consume()
    finally:
        barrier.release()
        thread.join(timeout=6)
    assert not thread.is_alive(), "observer did not finish"
    assert not errors, repr(errors)

asyncio.run(main())
'''


@pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
def test_completion_fence_waits_for_notification_teardown(outcome):
    completed = subprocess.run(
        [sys.executable, "-c", SCRIPT, outcome],
        capture_output=True, text=True, timeout=20,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
