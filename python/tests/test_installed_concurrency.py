from __future__ import annotations

import asyncio
import concurrent.futures
import gc
import os
import threading
import time
import weakref

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("NFS_RS_TEST_INSTALLED") != "1",
    reason="requires test-support wheel",
)


def _native_counts(client: object) -> tuple[int, int, int]:
    return client._inner._target._resource_counts()  # type: ignore[attr-defined]


def test_blocked_sync_file_io_releases_gil() -> None:
    from nfs_rs import Client, _internal

    client = Client.connect("nfs-test://fixture/export")
    file = client.open("fixture.bin", "rb")
    entered_python = threading.Event()
    _internal._arm_operation_test_barrier("read")

    def read() -> bytes:
        entered_python.set()
        return file.read(1)

    async def wait_until_native_read_is_blocked() -> None:
        await asyncio.wait_for(_internal._wait_operation_test_entered(), timeout=2)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        reading = executor.submit(read)
        try:
            assert entered_python.wait(timeout=2)
            asyncio.run(wait_until_native_read_is_blocked())
            # Reaching this statement while native read is blocked proves the GIL was released.
        finally:
            _internal._release_operation_test_barrier()
        assert reading.result(timeout=2) == b"a"

    file.close()
    client.close()
    assert _native_counts(client) == (0, 0, 0)


@pytest.mark.parametrize("client_kind", ["sync", "async"])
def test_operation_timeout_bounds_blocked_file_io(client_kind: str) -> None:
    from nfs_rs import AsyncClient, Client, NfsTimeoutError, _internal

    if client_kind == "sync":
        client = Client.connect(
            "nfs-test://fixture/export", operation_timeout=0.01
        )
        file = client.open("fixture.bin", "rb")
        _internal._arm_operation_test_barrier("read")
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            reading = executor.submit(file.read, 1)

            async def wait_until_entered() -> None:
                await _internal._wait_operation_test_entered()

            asyncio.run(wait_until_entered())
            try:
                with pytest.raises(NfsTimeoutError, match="read deadline exceeded"):
                    reading.result(timeout=1)
            finally:
                _internal._release_operation_test_barrier()
        file.close()
        client.close()
        return

    async def scenario() -> None:
        client = await AsyncClient.connect(
            "nfs-test://fixture/export", operation_timeout=0.01
        )
        file = await client.open("fixture.bin", "rb")
        _internal._arm_operation_test_barrier("read")
        reading = asyncio.create_task(file.read(1))
        await _internal._wait_operation_test_entered()
        try:
            with pytest.raises(NfsTimeoutError, match="read deadline exceeded"):
                await reading
        finally:
            settled = _internal._wait_operation_test_settled()
            _internal._release_operation_test_barrier()
            await settled
        await file.close()
        await client.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("client_kind", ["sync", "async"])
def test_operation_timeout_bounds_blocked_namespace_mutation(client_kind: str) -> None:
    from nfs_rs import AsyncClient, Client, NfsTimeoutError, _internal

    if client_kind == "sync":
        client = Client.connect(
            "nfs-test://fixture/export", operation_timeout=0.01
        )
        _internal._arm_operation_test_barrier("remove")
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            removing = executor.submit(client.remove, "safe-path")

            async def wait_until_entered() -> None:
                await _internal._wait_operation_test_entered()

            asyncio.run(wait_until_entered())
            try:
                with pytest.raises(NfsTimeoutError, match="remove deadline exceeded"):
                    removing.result(timeout=1)
            finally:
                _internal._release_operation_test_barrier()
        client.close()
        return

    async def scenario() -> None:
        client = await AsyncClient.connect(
            "nfs-test://fixture/export", operation_timeout=0.01
        )
        _internal._arm_operation_test_barrier("remove")
        removing = asyncio.create_task(client.remove("safe-path"))
        await _internal._wait_operation_test_entered()
        settled = _internal._wait_operation_test_settled()
        try:
            with pytest.raises(NfsTimeoutError, match="remove deadline exceeded"):
                await removing
        finally:
            _internal._release_operation_test_barrier()
            await settled
        await client.close()

    asyncio.run(scenario())


def test_shared_sync_client_survives_32_threads_and_close_races() -> None:
    from nfs_rs import Client, _internal

    client = Client.connect("nfs-test://fixture/export")
    file = client.open("threaded.bin", "w+b")
    start = threading.Barrier(32)

    def positional_worker(index: int) -> None:
        start.wait(timeout=5)
        payload = bytes([index]) * 8
        assert file.write_at(payload, index * 8) == len(payload)
        assert file.read_at(index * 8, 8) == payload

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(positional_worker, index) for index in range(32)]
        for future in futures:
            future.result(timeout=10)

    assert file.tell() == 0
    relative = client.open("fixture.bin", "rb")
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        reads = [executor.submit(relative.read, 1) for _ in range(32)]
        relative_results = [read.result(timeout=10) for read in reads]
    assert sorted(relative_results) == [b""] * 6 + [
        bytes([value]) for value in range(ord("a"), ord("z") + 1)
    ]
    assert relative.tell() == 26
    relative.close()

    race_file = client.open("fixture.bin", "rb")
    _internal._arm_operation_test_barrier("read")

    async def wait_for_racing_read() -> None:
        await asyncio.wait_for(_internal._wait_operation_test_entered(), timeout=2)

    with concurrent.futures.ThreadPoolExecutor(max_workers=33) as executor:
        racing_read = executor.submit(race_file.read, 1)
        closers: list[concurrent.futures.Future[None]] = []
        try:
            asyncio.run(wait_for_racing_read())
            closers = [executor.submit(race_file.close) for _ in range(32)]
            race_file._inner._target._wait_closing(timeout_seconds=2)
        finally:
            _internal._release_operation_test_barrier()
        assert racing_read.result(timeout=10) == b"a"
        for closer in closers:
            closer.result(timeout=10)

    file.close()
    client.close()
    assert file.closed and race_file.closed
    assert _native_counts(client) == (0, 0, 0)


def test_independent_sync_files_reach_native_io_concurrently() -> None:
    from nfs_rs import Client, _internal

    client = Client.connect("nfs-test://fixture/export")
    files = [client.open(f"overlap-{index}.bin", "rb") for index in range(2)]
    _internal._arm_operation_test_barrier("read", parties=2)

    async def wait_for_both_entries() -> None:
        await asyncio.wait_for(_internal._wait_operation_test_entered(), timeout=2)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        reads = [executor.submit(file.read, 1) for file in files]
        try:
            asyncio.run(wait_for_both_entries())
        finally:
            _internal._release_operation_test_barrier()
        for reading in reads:
            assert reading.result(timeout=2) == b"a"
    client.close()
    assert _native_counts(client) == (0, 0, 0)


def test_async_clients_survive_128_tasks_close_races_and_reject_cross_thread_loop() -> None:
    from nfs_rs import AsyncClient, _internal

    async def scenario() -> tuple[list[object], list[object]]:
        clients = list(
            await asyncio.gather(
                *(AsyncClient.connect("nfs-test://fixture/export") for _ in range(4))
            )
        )
        files = list(
            await asyncio.gather(
                *(client.open("fixture.bin", "rb") for client in clients for _ in range(4))
            )
        )
        _internal._arm_operation_test_barrier("read", parties=2)
        overlapping = [asyncio.create_task(files[index].read(1)) for index in range(2)]
        try:
            await asyncio.wait_for(_internal._wait_operation_test_entered(), timeout=2)
            heartbeat = asyncio.Event()
            started = time.monotonic()
            asyncio.get_running_loop().call_soon(heartbeat.set)
            await asyncio.wait_for(heartbeat.wait(), timeout=0.5)
            assert time.monotonic() - started < 0.5
        finally:
            _internal._release_operation_test_barrier()
            await asyncio.gather(*overlapping, return_exceptions=True)
        assert await asyncio.gather(*overlapping) == [b"a", b"a"]
        await asyncio.gather(*(files[index].seek(0) for index in range(2)))

        heartbeat = asyncio.Event()
        asyncio.get_running_loop().call_soon(heartbeat.set)
        results = await asyncio.wait_for(
            asyncio.gather(
                *(files[index % len(files)].read_at(index % 26, 1) for index in range(128))
            ),
            timeout=10,
        )
        await asyncio.wait_for(heartbeat.wait(), timeout=1)
        assert results == [bytes([ord("a") + index % 26]) for index in range(128)]
        assert all(file.tell() == 0 for file in files)
        relative_results = await asyncio.wait_for(
            asyncio.gather(*(files[0].read(1) for _ in range(128))), timeout=10
        )
        assert sorted(relative_results) == [b""] * 102 + [
            bytes([value]) for value in range(ord("a"), ord("z") + 1)
        ]
        assert files[0].tell() == 26

        race_file = files[-1]
        _internal._arm_operation_test_barrier("read")
        racing_read = asyncio.create_task(race_file.read(1))
        closers: list[asyncio.Task[None]] = []
        try:
            await asyncio.wait_for(_internal._wait_operation_test_entered(), timeout=2)
            closers = [asyncio.create_task(race_file.close()) for _ in range(32)]
            await asyncio.wait_for(race_file._inner._target._wait_closing(), timeout=2)
        finally:
            _internal._release_operation_test_barrier()
            await asyncio.gather(racing_read, *closers, return_exceptions=True)
        assert await asyncio.wait_for(racing_read, timeout=2) == b"a"
        await asyncio.wait_for(asyncio.gather(*closers), timeout=5)
        return clients, files

    loop = asyncio.new_event_loop()
    clients, files = loop.run_until_complete(scenario())
    client, file = clients[0], files[0]

    errors: list[BaseException] = []

    def wrong_thread() -> None:
        async def use_foreign_objects() -> None:
            for operation in (lambda: client.stat("fixture.bin"), lambda: file.read_at(0, 1)):
                with pytest.raises(RuntimeError, match="creating event loop"):
                    await operation()

        try:
            asyncio.run(use_foreign_objects())
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=wrong_thread)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert errors == []

    async def close_with_races() -> None:
        await asyncio.wait_for(asyncio.gather(*(client.close() for client in clients)), timeout=5)

    loop.run_until_complete(close_with_races())
    assert all(file.closed for file in files)
    assert all(_native_counts(client) == (0, 0, 0) for client in clients)
    loop.close()


def test_repeated_sync_and_async_lifecycles_converge() -> None:
    from nfs_rs import AsyncClient, Client, _internal

    async def warm_global_async_runtime() -> None:
        client = await AsyncClient.connect("nfs-test://fixture/export")
        await client.close()

    asyncio.run(warm_global_async_runtime())
    baseline_fds = len(os.listdir("/proc/self/fd"))
    baseline_threads = len(os.listdir("/proc/self/task"))
    object_refs: list[weakref.ReferenceType[object]] = []

    for _ in range(20):
        client = Client.connect("nfs-test://fixture/export")
        file = client.open("cycle.bin", "w+b")
        object_refs.extend((weakref.ref(client), weakref.ref(file)))
        assert file.write_at(b"sync", 0) == 4
        client.close()
        assert file.closed
        assert _native_counts(client) == (0, 0, 0)
        del file, client

    async def async_cycles() -> None:
        for _ in range(20):
            client = await AsyncClient.connect("nfs-test://fixture/export")
            file = await client.open("cycle.bin", "w+b")
            object_refs.extend((weakref.ref(client), weakref.ref(file)))
            writing = asyncio.create_task(file.write_at(b"async", 0))
            await asyncio.wait_for(writing, timeout=2)
            await asyncio.wait_for(client.close(), timeout=2)
            assert file.closed
            assert _native_counts(client) == (0, 0, 0)
            del writing, file, client

        for _ in range(10):
            client = await AsyncClient.connect("nfs-test://fixture/export")
            object_refs.append(weakref.ref(client))
            _internal._arm_open_test_barrier()
            opening = asyncio.create_task(client.open("__blocked_open__", "rb"))
            settled = _internal._wait_open_test_settled()
            try:
                await asyncio.wait_for(_internal._wait_open_test_entered(), timeout=2)
                opening.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await opening
            finally:
                opening.cancel()
                _internal._release_open_test_barrier()
                await asyncio.gather(opening, return_exceptions=True)
                await asyncio.wait_for(settled, timeout=2)
            await asyncio.wait_for(client.close(), timeout=2)
            assert _native_counts(client) == (0, 0, 0)
            del opening, client

        for operation in ("read", "close"):
            for _ in range(5):
                client = await AsyncClient.connect("nfs-test://fixture/export")
                file = await client.open("cycle.bin", "rb")
                object_refs.extend((weakref.ref(client), weakref.ref(file)))
                _internal._arm_operation_test_barrier(operation)
                pending = asyncio.create_task(
                    file.read(1) if operation == "read" else file.close()
                )
                settled = _internal._wait_operation_test_settled()
                try:
                    await asyncio.wait_for(_internal._wait_operation_test_entered(), timeout=2)
                    pending.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await pending
                finally:
                    pending.cancel()
                    _internal._release_operation_test_barrier()
                    await asyncio.gather(pending, return_exceptions=True)
                    await asyncio.wait_for(settled, timeout=2)
                if operation == "read":
                    await file.close()
                await asyncio.wait_for(client.close(), timeout=2)
                assert _native_counts(client) == (0, 0, 0)
                del settled, pending, file, client

    asyncio.run(async_cycles())
    gc.collect()
    assert all(reference() is None for reference in object_refs)
    assert len(os.listdir("/proc/self/fd")) <= baseline_fds
    assert len(os.listdir("/proc/self/task")) <= baseline_threads


def test_repeated_lifecycle_rss_reaches_a_bounded_plateau() -> None:
    from nfs_rs import Client

    page_size = os.sysconf("SC_PAGE_SIZE")

    def resident_bytes() -> int:
        with open("/proc/self/statm", encoding="ascii") as statm:
            return int(statm.read().split()[1]) * page_size

    def run_batch(cycles: int) -> None:
        for _ in range(cycles):
            client = Client.connect("nfs-test://fixture/export")
            file = client.open("rss-cycle.bin", "w+b")
            assert file.write(b"x" * 4096) == 4096
            client.close()
            assert _native_counts(client) == (0, 0, 0)
        gc.collect()

    run_batch(20)
    samples: list[int] = []
    for _ in range(5):
        run_batch(20)
        samples.append(resident_bytes())

    batch_indexes = range(len(samples))
    index_mean = sum(batch_indexes) / len(samples)
    rss_mean = sum(samples) / len(samples)
    slope = sum(
        (index - index_mean) * (rss - rss_mean)
        for index, rss in zip(batch_indexes, samples, strict=True)
    ) / sum((index - index_mean) ** 2 for index in batch_indexes)
    assert slope <= 2 * 1024 * 1024, f"RSS grows linearly by {slope:.0f} bytes per batch"
    assert max(samples[-3:]) - min(samples[-3:]) <= 8 * 1024 * 1024, (
        f"RSS did not plateau across final batches: {samples}"
    )
