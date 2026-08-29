#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import platform
import re
import resource
import statistics
import threading
import time
import tracemalloc
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, TypeVar
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import nfs_rs


SAFE_ID = re.compile(r"^(nightly|release|local)-[A-Za-z0-9._-]{1,80}$")
T = TypeVar("T")
RSS_RETESTS = 3


class RssPlateauError(RuntimeError):
    """A retryable failure of the process RSS plateau gate."""


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


def run_url(case: Case, run_id: str, artifact: str) -> tuple[str, str]:
    parsed = urlsplit(case.url)
    if parsed.scheme != "nfs" or not parsed.hostname:
        raise ValueError(f"invalid NFS URL for {case.name}")
    # Some appliances reject otherwise legal long NFSv4 component names. Keep
    # the unique leaf compact while deriving it from the full evidence identity.
    identity = f"{case.name}\0{run_id}\0{artifact}\0{platform.machine()}\0{os.getpid()}"
    digest = hashlib.sha256(identity.encode()).hexdigest()[:20]
    directory = f"py-{digest}-{'w' if artifact == 'wheel' else 's'}"
    path = parsed.path.rstrip("/") + "/" + directory
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["version"] = case.version
    return urlunsplit(parsed._replace(path=path, query=urlencode(query))), directory


def resident_bytes() -> int:
    try:
        pages = int(Path("/proc/self/statm").read_text(encoding="ascii").split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


def monitor_rss(operation: Callable[[], T]) -> tuple[T, int]:
    baseline = resident_bytes()
    maximum = baseline
    stop = threading.Event()

    def sample() -> None:
        nonlocal maximum
        while not stop.is_set():
            maximum = max(maximum, resident_bytes())
            time.sleep(0.005)

    sampler = threading.Thread(target=sample)
    sampler.start()
    try:
        result = operation()
    finally:
        stop.set()
        sampler.join()
        maximum = max(maximum, resident_bytes())
    return result, max(0, maximum - baseline)


def verified_remove(client: nfs_rs.Client, path: str) -> None:
    try:
        client.remove(path, missing_ok=True)
    except nfs_rs.NfsUncertainOutcomeError:
        if client.exists(path):
            raise


def verified_mkdir(client: nfs_rs.Client, path: str) -> None:
    try:
        client.mkdir(path)
    except nfs_rs.NfsUncertainOutcomeError:
        if not client.exists(path):
            raise


def verified_rmdir(client: nfs_rs.Client, path: str) -> None:
    try:
        client.rmdir(path)
    except nfs_rs.NfsUncertainOutcomeError:
        if client.exists(path):
            raise


def verified_rename(client: nfs_rs.Client, source: str, destination: str) -> None:
    try:
        client.rename(source, destination)
    except nfs_rs.NfsUncertainOutcomeError:
        if client.exists(source) or not client.exists(destination):
            raise


async def verified_remove_async(client: nfs_rs.AsyncClient, path: str) -> None:
    try:
        await client.remove(path, missing_ok=True)
    except nfs_rs.NfsUncertainOutcomeError:
        if await client.exists(path):
            raise


def parent_and_leaf(url: str) -> tuple[str, str]:
    parsed = urlsplit(url)
    parent_path, _, leaf = parsed.path.rstrip("/").rpartition("/")
    return urlunsplit(parsed._replace(path=parent_path or "/")), leaf


def prepare_run_directory(url: str) -> None:
    parent_url, leaf = parent_and_leaf(url)
    client = nfs_rs.Client.connect(parent_url)
    try:
        verified_mkdir(client, leaf)
    finally:
        client.close()


def cleanup_run_directory(url: str) -> None:
    parent_url, leaf = parent_and_leaf(url)
    client = nfs_rs.Client.connect(parent_url)
    try:
        for name in ("payload.bin", "renamed.bin", "async.bin"):
            verified_remove(client, f"{leaf}/{name}")
        verified_rmdir(client, leaf)
    finally:
        client.close()


def sync_scenario(url: str, version: str, require_pnfs: bool, payload: bytes) -> dict[str, float]:
    parent_url, leaf = parent_and_leaf(url)
    client = nfs_rs.Client.connect(parent_url)
    payload_path = f"{leaf}/payload.bin"
    renamed_path = f"{leaf}/renamed.bin"
    try:
        assert client.version.value == version
        if require_pnfs and not client.capabilities.pnfs:
            raise RuntimeError("required pNFS capability was not negotiated")
        tracemalloc.start()
        try:
            started = time.perf_counter()
            written, write_rss_growth = monitor_rss(
                lambda: client.write_bytes(payload_path, payload)
            )
            write_seconds = time.perf_counter() - started
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert written == len(payload)
        heartbeat = 0
        stop = threading.Event()
        ready = threading.Event()

        def beat() -> None:
            nonlocal heartbeat
            ready.set()
            while not stop.is_set():
                heartbeat += 1
                time.sleep(0.001)

        thread = threading.Thread(target=beat)
        thread.start()
        assert ready.wait(timeout=1)
        heartbeat_before_read = heartbeat
        heartbeat_after_read = heartbeat

        def read_payload() -> bytes:
            nonlocal heartbeat_before_read, heartbeat_after_read
            # Set the boundary only after the RSS sampler is running and
            # immediately before entering the native blocking operation.
            heartbeat_before_read = heartbeat
            result = client.read_bytes(payload_path)
            heartbeat_after_read = heartbeat
            return result

        started = time.perf_counter()
        try:
            data, read_rss_growth = monitor_rss(read_payload)
            read_seconds = time.perf_counter() - started
        finally:
            stop.set()
            thread.join(timeout=1)
            if thread.is_alive():
                raise RuntimeError("GIL heartbeat thread did not stop")
        heartbeats_during_read = heartbeat_after_read - heartbeat_before_read
        assert data == payload and heartbeats_during_read >= 2
        verified_rename(client, payload_path, renamed_path)
        assert "renamed.bin" in {name for name in client.listdir(leaf) if name not in {".", ".."}}
        verified_remove(client, renamed_path)
        return {
            "write_mib_s": len(payload) / write_seconds / 2**20,
            "read_mib_s": len(payload) / read_seconds / 2**20,
            "write_latency_ms": write_seconds * 1000,
            "read_latency_ms": read_seconds * 1000,
            "python_peak_bytes": float(peak),
            "process_buffer_peak_growth_bytes": float(max(write_rss_growth, read_rss_growth)),
            "gil_heartbeats": float(heartbeats_during_read),
        }
    finally:
        client.close()


async def async_scenario(url: str, version: str, payload: bytes) -> dict[str, float]:
    parent_url, leaf = parent_and_leaf(url)
    client = await nfs_rs.AsyncClient.connect(parent_url)
    payload_path = f"{leaf}/async.bin"
    heartbeat = 0
    max_lag = 0.0
    stop = False

    async def beat() -> None:
        nonlocal heartbeat, max_lag
        previous = time.perf_counter()
        while not stop:
            await asyncio.sleep(0)
            now = time.perf_counter()
            max_lag = max(max_lag, now - previous)
            previous = now
            heartbeat += 1

    task = asyncio.create_task(beat())
    try:
        assert client.version.value == version
        await client.write_bytes(payload_path, payload)
        assert await client.read_bytes(payload_path) == payload
        await verified_remove_async(client, payload_path)
    finally:
        stop = True
        await task
        await client.close()
    assert heartbeat > 0 and max_lag <= 0.25
    return {"event_loop_heartbeats": float(heartbeat), "event_loop_max_lag_ms": max_lag * 1000}


def validate_case_attempt(
    case: Case,
    run_id: str,
    artifact: str,
    payload: bytes,
    runs: int,
    minimum_valid_runs: int,
) -> dict[str, object]:
    samples: list[dict[str, float]] = []
    failures: list[str] = []
    cleanup_failures: list[str] = []
    rss_samples: list[int] = []
    for index in range(runs):
        url, _ = run_url(case, f"{run_id}-{index}", artifact)
        try:
            prepare_run_directory(url)
            sample = sync_scenario(url, case.version, case.require_pnfs, payload)
            sample.update(asyncio.run(async_scenario(url, case.version, payload)))
            samples.append(sample)
            rss_samples.append(resident_bytes())
        except Exception as error:
            failures.append(f"{type(error).__name__}: {error}")
        finally:
            try:
                cleanup_run_directory(url)
            except Exception as error:
                cleanup_failures.append(f"{type(error).__name__}: {error}")
    if cleanup_failures:
        raise RuntimeError(f"{case.name} cleanup failed: {cleanup_failures}")
    if len(samples) < minimum_valid_runs:
        raise RuntimeError(f"{case.name} produced only {len(samples)} valid performance runs: {failures}")
    if max(sample["python_peak_bytes"] for sample in samples) > len(payload) * 3 + 8 * 2**20:
        raise RuntimeError(f"{case.name} exceeded the bounded Python buffer-memory gate")
    if max(sample["process_buffer_peak_growth_bytes"] for sample in samples) > len(payload) * 3 + 8 * 2**20:
        raise RuntimeError(f"{case.name} exceeded the bounded process buffer-memory gate")
    if len(rss_samples) >= 3:
        tail = rss_samples[-3:]
        slope = statistics.linear_regression(range(3), tail).slope
        sustained_growth = tail[0] < tail[1] < tail[2] and slope > 4 * 2**20
        if max(tail) - min(tail) > 16 * 2**20 or sustained_growth:
            raise RssPlateauError(
                f"{case.name} RSS did not plateau: tail={tail}, slope={slope:.0f} bytes/run"
            )
    return {
        "case": asdict(case),
        "artifact": artifact,
        "valid_runs": len(samples),
        "invalid_runs": len(failures),
        "failures": failures,
        "samples": samples,
        "summary": {
            "write_mib_s_median": statistics.median(s["write_mib_s"] for s in samples),
            "read_mib_s_median": statistics.median(s["read_mib_s"] for s in samples),
            "write_latency_ms_p95": sorted(s["write_latency_ms"] for s in samples)[-1],
            "read_latency_ms_p95": sorted(s["read_latency_ms"] for s in samples)[-1],
            "rss_growth_bytes": rss_samples[-1] - min(rss_samples),
        },
    }


def validate_case(
    case: Case,
    run_id: str,
    artifact: str,
    payload: bytes,
    runs: int,
    minimum_valid_runs: int,
) -> dict[str, object]:
    rss_failures: list[str] = []
    for attempt in range(RSS_RETESTS + 1):
        attempt_run_id = run_id if attempt == 0 else f"{run_id}-rss-retest-{attempt}"
        try:
            result = validate_case_attempt(
                case,
                attempt_run_id,
                artifact,
                payload,
                runs,
                minimum_valid_runs,
            )
            result["rss_gate_attempts"] = attempt + 1
            result["rss_gate_failures"] = rss_failures
            return result
        except RssPlateauError as error:
            rss_failures.append(str(error))
    raise RssPlateauError(
        f"{case.name} failed initial RSS gate and all {RSS_RETESTS} retests: "
        f"{rss_failures}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", type=parse_case, action="append", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact", choices=("wheel", "sdist-wheel"), required=True)
    parser.add_argument("--payload-mib", type=int, default=4)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--minimum-valid-runs", type=int, default=4)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if not SAFE_ID.fullmatch(arguments.run_id):
        parser.error("unsafe run id")
    if not 1 <= arguments.payload_mib <= 64:
        parser.error("payload MiB must be between 1 and 64")
    if not 1 <= arguments.minimum_valid_runs <= arguments.runs <= 20:
        parser.error("runs must be 1..20 and minimum-valid-runs must be 1..runs")
    payload = bytes((index % 251 for index in range(arguments.payload_mib * 2**20)))
    result = {
        "schema_version": 1,
        "run_id": arguments.run_id,
        "nfs_rs_version": nfs_rs.__version__,
        "results": [
            validate_case(
                case,
                arguments.run_id,
                arguments.artifact,
                payload,
                arguments.runs,
                arguments.minimum_valid_runs,
            )
            for case in arguments.case
        ],
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
