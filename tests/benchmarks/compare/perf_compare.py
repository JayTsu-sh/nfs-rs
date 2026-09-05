#!/usr/bin/env python3
"""Python twin of `nfs-perf-compare`: same CLI, same suites, same JSON schema.

Backends:
  * posix  — kernel NFS mount via os.* (O_DIRECT or buffered), QD via threads
  * nfsrs  — nfs_rs.AsyncClient on a background event loop, QD via asyncio
"""
from __future__ import annotations

import argparse
import asyncio
import json
import mmap
import os
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit, urlunsplit

CHUNK = 1024 * 1024
ALIGN = 4096
SIZES = {"4k": 4096, "40m": 40 * CHUNK, "1g": 1024 * CHUNK}
PATTERN = bytes((i * 17 + 29) % 251 for i in range(CHUNK))
_PATTERN2 = PATTERN + PATTERN


def pattern_at(offset: int, n: int) -> bytes:
    """Pattern bytes for the absolute range [offset, offset+n); n <= CHUNK."""
    pos = offset % CHUNK
    return _PATTERN2[pos:pos + n]
METADATA_OPS = ("mkdir", "create", "stat", "access", "chmod", "rename", "remove", "rmdir")


class BenchError(RuntimeError):
    pass


def verify(offset: int, data: bytes | memoryview) -> bool:
    view = memoryview(data)
    pos = offset % CHUNK
    while len(view):
        n = min(len(view), CHUNK - pos)
        if view[:n] != PATTERN[pos:pos + n]:
            return False
        view = view[n:]
        pos = (pos + n) % CHUNK
    return True


def _aligned_pattern() -> mmap.mmap:
    block = mmap.mmap(-1, CHUNK)
    block.write(PATTERN)
    return block


# ----------------------------------------------------------------------------
# statistics / JSON
# ----------------------------------------------------------------------------

def percentile(samples: list[float], p: float) -> float:
    if not samples:
        return float("nan")
    ordered = sorted(samples)
    index = min(max(int(-(-len(ordered) * p // 1)) - 1, 0), len(ordered) - 1)
    return ordered[index]


def mibps(nbytes: int, seconds: float) -> float:
    return nbytes / 1048576 / seconds


class Series:
    def __init__(self, name: str, unit: str, reference_only: bool = False) -> None:
        self.name, self.unit, self.reference_only = name, unit, reference_only
        self.samples: list[float] = []

    def to_json(self) -> dict[str, Any]:
        base = {"name": self.name, "unit": self.unit, "reference_only": self.reference_only,
                "samples": self.samples}
        mean = statistics.fmean(self.samples) if self.samples else float("nan")
        if self.unit == "ms":
            base.update(p50=percentile(self.samples, 0.5), p95=percentile(self.samples, 0.95),
                        p99=percentile(self.samples, 0.99), mean=mean,
                        ops_s=1000.0 / mean if mean > 0 else float("nan"))
        else:
            base.update(median=percentile(self.samples, 0.5),
                        min=min(self.samples, default=float("nan")),
                        max=max(self.samples, default=float("nan")))
        return base


def peak_rss_kib() -> int:
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1])
    except OSError:
        pass
    return 0


# ----------------------------------------------------------------------------
# POSIX backend (kernel mount)
# ----------------------------------------------------------------------------

class PosixBackend:
    name = "posix"

    def __init__(self, root: str, io: str) -> None:
        self.root = root
        self.direct = io == "direct"
        self.protocol = None
        self.rsize = self.wsize = CHUNK
        self.supports_access = True
        self._pattern = _aligned_pattern()

    def _abs(self, path: str) -> str:
        return os.path.join(self.root, path)

    def mkdir(self, p: str) -> None: os.mkdir(self._abs(p))
    def create(self, p: str) -> None: os.close(os.open(self._abs(p), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644))
    def stat(self, p: str) -> None: os.stat(self._abs(p))
    def chmod(self, p: str, mode: int) -> None: os.chmod(self._abs(p), mode)
    def rename(self, a: str, b: str) -> None: os.rename(self._abs(a), self._abs(b))
    def readdir_count(self, p: str) -> int: return len(os.listdir(self._abs(p)))
    def remove(self, p: str) -> None: os.unlink(self._abs(p))
    def rmdir(self, p: str) -> None: os.rmdir(self._abs(p))

    def access(self, p: str) -> None:
        if not os.access(self._abs(p), os.R_OK):
            raise BenchError(f"access denied: {p}")

    def chunk_size(self) -> int:
        return CHUNK

    def _open(self, p: str, write: bool) -> int:
        flags = (os.O_WRONLY | os.O_CREAT | os.O_TRUNC) if write else os.O_RDONLY
        if self.direct:
            flags |= os.O_DIRECT
        return os.open(self._abs(p), flags, 0o644)

    def write_file(self, p: str, size: int, qd: int) -> float:
        fd = self._open(p, True)
        try:
            total = -(-size // CHUNK)
            pattern = memoryview(self._pattern)
            started = time.perf_counter()

            def worker(k: int) -> None:
                for i in range(k, total, qd):
                    offset = i * CHUNK
                    n = min(size - offset, CHUNK)
                    done = 0
                    while done < n:
                        w = os.pwritev(fd, [pattern[done:n]], offset + done)
                        if w <= 0:
                            raise BenchError("short write")
                        done += w

            _run_threads(worker, qd)
            os.fsync(fd)
            return time.perf_counter() - started
        finally:
            os.close(fd)

    def read_file(self, p: str, size: int, qd: int) -> float:
        fd = self._open(p, False)
        try:
            total = -(-size // CHUNK)
            verify_time = [0.0] * qd
            started = time.perf_counter()

            def worker(k: int) -> None:
                buf = mmap.mmap(-1, CHUNK) if self.direct else bytearray(CHUNK)
                view = memoryview(buf)
                for i in range(k, total, qd):
                    offset = i * CHUNK
                    n = min(size - offset, CHUNK)
                    done = 0
                    while done < n:
                        r = os.preadv(fd, [view[done:n]], offset + done)
                        if r <= 0:
                            break
                        done += r
                    v = time.perf_counter()
                    if done != n or not verify(offset, view[:n]):
                        raise BenchError(f"chunk at offset {offset} mismatch ({done} of {n} bytes)")
                    verify_time[k] += time.perf_counter() - v

            _run_threads(worker, qd)
            return time.perf_counter() - started - sum(verify_time) / qd
        finally:
            os.close(fd)

    def drop_caches(self) -> bool:
        if self.direct:
            return False
        os.sync()
        try:
            Path("/proc/sys/vm/drop_caches").write_text("3\n")
            return True
        except PermissionError:
            return False

    def shutdown(self) -> None:
        pass


def _run_threads(worker, qd: int) -> None:
    if qd == 1:
        worker(0)
        return
    with ThreadPoolExecutor(qd) as pool:
        for future in [pool.submit(worker, k) for k in range(qd)]:
            future.result()


# ----------------------------------------------------------------------------
# nfs-rs backend (userspace client)
# ----------------------------------------------------------------------------

class NfsRsBackend:
    name = "nfsrs"
    supports_access = False

    def __init__(self, url: str) -> None:
        import nfs_rs  # deferred so the posix backend works without the wheel

        parts = urlsplit(url)
        query = {k: v[-1] for k, v in parse_qs(parts.query).items()}
        options: dict[str, Any] = {}
        if "version" in query:
            options["versions"] = (query["version"],)
        for key in ("uid", "gid", "rsize", "wsize", "readahead", "writeback"):
            if key in query:
                options[key] = int(query[key])
        base = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._client = self._call(nfs_rs.AsyncClient.connect(base, **options))
        self.protocol = str(self._client.version.value)
        limits = self._client.io_limits
        self.rsize, self.wsize = limits.max_read, limits.max_write

    def _call(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def mkdir(self, p: str) -> None: self._call(self._client.mkdir(p, 0o755))
    def create(self, p: str) -> None: self._call(self._create(p))
    def stat(self, p: str) -> None: self._call(self._client.stat(p))
    def chmod(self, p: str, mode: int) -> None: self._call(self._client.chmod(p, mode))
    def rename(self, a: str, b: str) -> None: self._call(self._client.rename(a, b))
    def readdir_count(self, p: str) -> int:
        return sum(1 for name in self._call(self._client.listdir(p)) if name not in (".", ".."))
    def remove(self, p: str) -> None: self._call(self._client.remove(p))
    def rmdir(self, p: str) -> None: self._call(self._client.rmdir(p))

    def access(self, path: str) -> None:
        raise BenchError(f"access is not exposed by the Python API ({path})")

    async def _create(self, p: str) -> None:
        f = await self._client.open(p, "wb")
        await f.close()

    def chunk_size(self) -> int:
        return min(CHUNK, self.rsize, self.wsize)

    def write_file(self, p: str, size: int, qd: int) -> float:
        return self._call(self._write_file(p, size, qd))

    async def _write_file(self, p: str, size: int, qd: int) -> float:
        chunk = self.chunk_size()
        total = -(-size // chunk)
        f = await self._client.open(p, "wb")
        try:
            started = time.perf_counter()

            async def worker(k: int) -> None:
                for i in range(k, total, qd):
                    offset = i * chunk
                    n = min(size - offset, chunk)
                    done = 0
                    data = pattern_at(offset, n)
                    while done < n:
                        w = await f.write_at(data[done:n], offset + done)
                        if w <= 0:
                            raise BenchError("short write")
                        done += w

            await asyncio.gather(*(worker(k) for k in range(qd)))
            await f.flush()
            return time.perf_counter() - started
        finally:
            await f.close()

    def read_file(self, p: str, size: int, qd: int) -> float:
        return self._call(self._read_file(p, size, qd))

    async def _read_file(self, p: str, size: int, qd: int) -> float:
        chunk = self.chunk_size()
        total = -(-size // chunk)
        f = await self._client.open(p, "rb")
        try:
            verify_time = [0.0] * qd
            started = time.perf_counter()

            async def worker(k: int) -> None:
                for i in range(k, total, qd):
                    offset = i * chunk
                    n = min(size - offset, chunk)
                    data = await f.read_at(offset, n)
                    v = time.perf_counter()
                    if len(data) != n or not verify(offset, data):
                        raise BenchError(f"chunk at offset {offset} mismatch ({len(data)} of {n} bytes)")
                    verify_time[k] += time.perf_counter() - v

            await asyncio.gather(*(worker(k) for k in range(qd)))
            return time.perf_counter() - started - sum(verify_time) / qd
        finally:
            await f.close()

    def drop_caches(self) -> bool:
        return False

    def shutdown(self) -> None:
        self._call(self._client.close())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


# ----------------------------------------------------------------------------
# suites
# ----------------------------------------------------------------------------

def _timed(series: Series, fn, *args) -> None:
    started = time.perf_counter()
    fn(*args)
    series.samples.append((time.perf_counter() - started) * 1000.0)


def run_metadata(b, workdir: str, iters: int, readdir_entries: int, readdir_iters: int) -> list[Series]:
    ops = [op for op in METADATA_OPS if op != "access" or b.supports_access]
    series = {op: Series(op, "ms") for op in ops}
    m = f"{workdir}/m"
    b.mkdir(m)
    for i in range(iters):
        d, f, g = f"{m}/d{i}", f"{m}/d{i}/f", f"{m}/d{i}/g"
        _timed(series["mkdir"], b.mkdir, d)
        _timed(series["create"], b.create, f)
        _timed(series["stat"], b.stat, f)
        if "access" in series:
            _timed(series["access"], b.access, f)
        _timed(series["chmod"], b.chmod, f, 0o644)
        _timed(series["rename"], b.rename, f, g)
        _timed(series["remove"], b.remove, g)
        _timed(series["rmdir"], b.rmdir, d)
    b.rmdir(m)

    big = f"{workdir}/big"
    b.mkdir(big)
    for i in range(readdir_entries):
        b.create(f"{big}/e{i}")
    readdir = Series("readdir", "ms")
    seen = readdir_entries
    for _ in range(readdir_iters):
        started = time.perf_counter()
        seen = b.readdir_count(big)
        readdir.samples.append((time.perf_counter() - started) * 1000.0)
        if seen != readdir_entries:
            break
    for i in range(readdir_entries):
        b.remove(f"{big}/e{i}")
    b.rmdir(big)
    if seen != readdir_entries:
        raise BenchError(f"readdir saw {seen} entries, expected {readdir_entries}")
    return [*series.values(), readdir]


def run_data(b, workdir: str, size: int, qd: int, repeat: int, iters: int, hot_read: bool) -> tuple[list[Series], bool]:
    small = size <= b.chunk_size()
    count = iters if small else repeat
    paths = [f"{workdir}/f{i}.bin" for i in range(count)]
    unit = "ms" if small else "MiB/s"
    suffix = "_ms" if small else ""
    write, read = Series("write" + suffix, unit), Series("read" + suffix, unit)
    hot = Series("read_hot" + suffix, unit, reference_only=True)

    def record(series: Series, seconds: float) -> None:
        series.samples.append(seconds * 1000.0 if small else mibps(size, seconds))

    dropped = False
    try:
        for p in paths:
            record(write, b.write_file(p, size, qd))
        dropped = b.drop_caches()
        for p in paths:
            record(read, b.read_file(p, size, qd))
        out = [write, read]
        if hot_read:
            for p in paths:
                record(hot, b.read_file(p, size, qd))
            out.append(hot)
        return out, dropped
    finally:
        for p in paths:
            try:
                b.remove(p)
            except Exception:
                pass


def run_multiclient(b, args: argparse.Namespace, size: int, clients: int, mode: str, repeat: int) -> tuple[list[Series], int]:
    files = 1 if mode == "same" else clients
    paths = [f"{args.workdir}/mc{i}.bin" for i in range(files)]
    max_rss = 0
    try:
        for p in paths:
            b.write_file(p, size, 8)
        aggregate, per_client = Series("aggregate_read", "MiB/s"), Series("per_client_read", "MiB/s")
        for _ in range(repeat):
            b.drop_caches()
            started = time.perf_counter()
            children = [
                subprocess.Popen(
                    [sys.executable, __file__, "--target", args.target, "--io", args.io,
                     "--workdir", args.workdir, "worker-read", "--path", paths[c % files],
                     "--bytes", str(size), "--qd", "1"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                for c in range(clients)
            ]
            for child in children:
                out, err = child.communicate()
                if child.returncode != 0:
                    raise BenchError(f"worker failed: {err.strip()}")
                result = json.loads(out)
                per_client.samples.append(mibps(size, result["seconds"]))
                max_rss = max(max_rss, int(result.get("peak_rss_kib") or 0))
            aggregate.samples.append(mibps(size * clients, time.perf_counter() - started))
        return [aggregate, per_client], max_rss
    finally:
        for p in paths:
            try:
                b.remove(p)
            except Exception:
                pass


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="perf_compare.py")
    parser.add_argument("--target", required=True, help="nfs://... or absolute mount path")
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--json", default="/dev/stdout")
    parser.add_argument("--io", choices=("direct", "buffered"), default="direct")
    parser.add_argument("--smoke", action="store_true")
    sub = parser.add_subparsers(dest="suite", required=True)
    meta = sub.add_parser("metadata")
    meta.add_argument("--iters", type=int, default=200)
    meta.add_argument("--readdir-entries", type=int, default=1000)
    meta.add_argument("--readdir-iters", type=int, default=20)
    data = sub.add_parser("data")
    data.add_argument("--size", choices=sorted(SIZES), required=True)
    data.add_argument("--qd", type=int, choices=(1, 8), default=1)
    data.add_argument("--repeat", type=int, default=5)
    data.add_argument("--iters", type=int, default=200)
    multi = sub.add_parser("multiclient")
    multi.add_argument("--size", choices=sorted(SIZES), required=True)
    multi.add_argument("--clients", type=int, default=8)
    multi.add_argument("--mode", choices=("same", "distinct"), default="same")
    multi.add_argument("--repeat", type=int, default=3)
    worker = sub.add_parser("worker-read")
    worker.add_argument("--path", required=True)
    worker.add_argument("--bytes", type=int, required=True)
    worker.add_argument("--qd", type=int, default=1)
    args = parser.parse_args(argv)
    if not (args.target.startswith("nfs://") or args.target.startswith("/")):
        parser.error("--target must be nfs://... or an absolute path")
    if args.smoke:
        for name, value in (("iters", 1), ("repeat", 1), ("readdir_entries", 10), ("readdir_iters", 1)):
            if hasattr(args, name):
                setattr(args, name, value)
    return args


def connect(args: argparse.Namespace):
    if args.target.startswith("nfs://"):
        return NfsRsBackend(args.target)
    return PosixBackend(args.target, args.io)


def nfs_rs_version() -> str | None:
    try:
        import nfs_rs
        return nfs_rs.__version__
    except Exception:
        return None


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    b = connect(args)
    if args.suite == "worker-read":
        seconds = b.read_file(args.path, args.bytes, args.qd)
        b.shutdown()
        print(json.dumps({"seconds": seconds, "bytes": args.bytes, "peak_rss_kib": peak_rss_kib()}))
        return 0
    is_posix = b.name == "posix"
    b.mkdir(args.workdir)
    worker_rss, dropped = 0, False
    try:
        if args.suite == "metadata":
            series = run_metadata(b, args.workdir, args.iters, args.readdir_entries, args.readdir_iters)
            params = {"iters": args.iters, "readdir_entries": args.readdir_entries, "readdir_iters": args.readdir_iters}
        elif args.suite == "data":
            size = SIZES[args.size]
            series, dropped = run_data(b, args.workdir, size, args.qd, args.repeat, args.iters,
                                       is_posix and args.io == "buffered")
            params = {"size": args.size, "bytes": size, "qd": args.qd, "repeat": args.repeat, "iters": args.iters}
        else:
            size = SIZES[args.size]
            series, worker_rss = run_multiclient(b, args, size, args.clients, args.mode, args.repeat)
            params = {"size": args.size, "bytes": size, "clients": args.clients, "mode": args.mode, "repeat": args.repeat}
    finally:
        try:
            b.rmdir(args.workdir)
        except Exception:
            pass
    protocol = b.protocol or os.environ.get("PERF_PROTOCOL")
    b.shutdown()
    report = {
        "schema": 1,
        "harness": "python",
        "backend": b.name,
        "protocol": protocol,
        "target": args.target,
        "mount_variant": os.environ.get("PERF_MOUNT_VARIANT"),
        "io_mode": args.io if is_posix else None,
        "suite": args.suite,
        "smoke": args.smoke,
        "params": params,
        "env": {
            "hostname": os.uname().nodename,
            "kernel": os.uname().release,
            "python": sys.version.split()[0],
            "nfs_rs_version": nfs_rs_version(),
            "commit": os.environ.get("PERF_COMMIT"),
            "rsize": b.rsize,
            "wsize": b.wsize,
            "captured_at_unix": int(time.time()),
            "drop_caches": dropped,
        },
        "peak_rss_kib": max(peak_rss_kib(), worker_rss),
        "results": [s.to_json() for s in series],
    }
    text = json.dumps(report, indent=2)
    if args.json == "/dev/stdout":
        print(text)
    else:
        Path(args.json).write_text(text)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except BenchError as error:
        print(error, file=sys.stderr)
        sys.exit(1)
