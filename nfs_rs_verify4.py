"""Compare nfs-rs Python and local NFS mount scan/read/durable-write performance.

Creates one uniquely named temporary directory and fresh files for each case,
then removes only those objects. Existing dataset files are read-only samples.
Timings include Python/native adapter overhead and do not represent cold-cache
storage throughput. WRITE timings include per-buffer durable completion:
NFS batch commit for nfs-rs, fsync for the local mount. Scans collect attributes
on both sides and compare inventories before starting write measurements.

Example (the local path must already mount the same export):
    python nfs_rs_verify4.py --kernel-root /mnt/smartiq/NFSv3_7 \
        --buffer-mib 64 --file-mib 256 --json-output comparison.json

"""
from __future__ import annotations

import argparse
from collections import deque
import hashlib
import json
import os
import stat
import subprocess
from pathlib import Path
import statistics
import time
import uuid

from nfs_rs import Client, DirectoryRef, DirEntry, FileInfo, FileType

MIB = 1024 * 1024
HOST, EXPORT = "10.131.7.202", "/jsj-data-set/LN_NET"


class KernelFile:
    """Unbuffered Python I/O; one fsync after each full caller write buffer."""
    def __init__(self, path, mode):
        self.file = path.open(mode, buffering=0)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.file.close()

    def readinto(self, target):
        total = 0
        with memoryview(target) as view:
            while total < len(view):
                with view[total:] as remaining:
                    count = self.file.readinto(remaining)
                if not count:
                    break
                total += count
        return total

    def write(self, data):
        total = 0
        with memoryview(data) as view:
            while total < len(view):
                with view[total:] as remaining:
                    count = self.file.write(remaining)
                if not count:
                    raise OSError("local mount write made no progress")
                total += count
        os.fsync(self.file.fileno())
        return total


class KernelClient:
    """Present equivalent directory attributes; never follow directory symlinks."""
    def __init__(self, root):
        self.root = root.resolve(strict=True)

    def local_path(self, relative):
        relative = Path(str(relative).lstrip("/"))
        if ".." in relative.parts:
            raise ValueError("path escapes the benchmark root")
        return self.root / relative

    def open(self, path, mode):
        return KernelFile(self.local_path(path), mode)

    def scandir(self, target):
        parent = str(target.path)
        with os.scandir(self.local_path(parent)) as entries:
            for entry in entries:
                attrs = entry.stat(follow_symlinks=False)
                kind = next((kind for check, kind in (
                    (stat.S_ISREG, FileType.FILE), (stat.S_ISDIR, FileType.DIRECTORY),
                    (stat.S_ISLNK, FileType.SYMLINK), (stat.S_ISBLK, FileType.BLOCK_DEVICE),
                    (stat.S_ISCHR, FileType.CHARACTER_DEVICE), (stat.S_ISFIFO, FileType.FIFO),
                    (stat.S_ISSOCK, FileType.SOCKET),
                ) if check(attrs.st_mode)), FileType.UNKNOWN)
                path = entry.name if parent == "." else f"{parent}/{entry.name}"
                info = FileInfo(
                    path=path, type=kind, mode=stat.S_IMODE(attrs.st_mode),
                    nlink=attrs.st_nlink, uid=attrs.st_uid, gid=attrs.st_gid,
                    size=attrs.st_size, used=attrs.st_blocks * 512,
                    fsid=attrs.st_dev, fileid=attrs.st_ino,
                    atime=attrs.st_atime_ns, mtime=attrs.st_mtime_ns,
                    ctime=attrs.st_ctime_ns, owner=None, group=None,
                )
                yield DirEntry(entry.name, path, info)


def mount_information(root):
    if not root.is_dir():
        raise ValueError(f"NFS mount directory does not exist: {root}; supply --kernel-root")
    result = subprocess.run(
        ["findmnt", "--json", "--target", str(root), "--output", "TARGET,SOURCE,FSTYPE,OPTIONS"],
        check=True, capture_output=True, text=True,
    )
    info = json.loads(result.stdout)["filesystems"][0]
    if info["fstype"] not in {"nfs", "nfs4"}:
        raise ValueError(f"{root} is on {info['fstype']}, not an NFS mount")
    return info


def walk(client, root, *, use_fh, max_dirs):
    queue = deque([root])
    files, directories, records = [], [], []
    fh_scans = path_scans = entries_count = 0
    started = time.perf_counter()
    while queue and fh_scans + path_scans < max_dirs:
        directory = queue.popleft()
        target = directory if use_fh else DirectoryRef(directory.path)
        if target.fh is None:
            path_scans += 1
        else:
            fh_scans += 1
        entries = [entry for entry in client.scandir(target) if entry.name not in (".", "..")]
        # Stable BFS order means the bounded runs visit the same directories.
        entries.sort(key=lambda entry: entry.name)
        directories.append((directory, len(entries)))
        entries_count += len(entries)
        for entry in entries:
            records.append((entry.path, entry.info.type.value, entry.info.fileid, entry.info.size))
            if entry.info.type is FileType.DIRECTORY:
                queue.append(DirectoryRef(entry.path, entry.fh))
            elif entry.info.type is FileType.FILE:
                files.append(entry)
    elapsed = time.perf_counter() - started
    fingerprint = hashlib.sha256(json.dumps(sorted(records)).encode()).hexdigest()
    return {
        "seconds": elapsed,
        "directories": fh_scans + path_scans,
        "entries": entries_count,
        "files": len(files),
        "fh_scans": fh_scans,
        "path_lookups": None if isinstance(client, KernelClient) else path_scans,
        "truncated": bool(queue),
        "entries_per_second": entries_count / elapsed,
        "fingerprint": fingerprint,
    }, files, directories


def scan_benchmark(client, kernel, root, args):
    cases = {"nfs_rs_path": (client, False), "nfs_rs_fh": (client, True), "kernel": (kernel, False)}
    results = {name: [] for name in cases}
    names = list(cases)
    for round_index in range(args.scan_rounds):
        # Rotate first position to distribute cache/order effects across backends.
        shift = round_index % len(names)
        for name in names[shift:] + names[:shift]:
            backend, use_fh = cases[name]
            result, _, _ = walk(backend, root, use_fh=use_fh, max_dirs=args.max_dirs)
            results[name].append(result)
            print(f"SCAN round={round_index + 1} backend={name} "
                  f"dirs={result['directories']} entries={result['entries']} "
                  f"fh={result['fh_scans']} lookup={result['path_lookups']} "
                  f"time={result['seconds']:.4f}s rate={result['entries_per_second']:.0f} entries/s", flush=True)
    equivalent = len({run["fingerprint"] for runs in results.values() for run in runs}) == 1
    summary = {
        "root": str(root.path), "equivalent_entries": equivalent,
        "median_seconds": {name: statistics.median(run["seconds"] for run in runs)
                           for name, runs in results.items()},
        "runs": results,
    }
    print("SCAN median seconds:", json.dumps(summary["median_seconds"]), flush=True)
    if not equivalent:
        raise RuntimeError("scan inventories differ: verify that URL and --kernel-root refer to the same export, and that it is not changing")
    return summary


def read_file(open_file, size, buffer):
    """Fill a reusable caller-owned buffer; hash views without creating bytes."""
    digest = hashlib.sha256()
    total = calls = 0
    wall_started = time.time()
    started = time.perf_counter()
    with open_file() as file, memoryview(buffer) as view:
        while total < size:
            with view[:min(len(view), size - total)] as target:
                n = file.readinto(target)
                calls += 1
                if n != len(target):
                    raise RuntimeError(f"unexpected EOF/short fill: offset={total} got={n} expected={len(target)}")
                digest.update(target)
                total += n
    elapsed = time.perf_counter() - started
    return {"seconds": elapsed, "wall_started": wall_started, "wall_finished": time.time(),
            "bytes": total, "calls": calls,
            "mib_per_second": total / MIB / elapsed, "sha256": digest.hexdigest()}


def read_samples(client, kernel, files, buffer):
    samples = []
    for entry in sorted(files, key=lambda entry: entry.info.size, reverse=True)[:3]:
        result = read_file(lambda: client.open(entry.path, "rb"), entry.info.size, buffer)
        result.update(path=entry.path, mtime=entry.info.mtime)  # mtime: ns since Unix epoch.
        print(f"SAMPLE {entry.path!r}: {entry.info.size} bytes, "
              f"{result['seconds']:.4f}s, sha256={result['sha256']}", flush=True)
        result["kernel"] = read_file(lambda: kernel.open(entry.path, "rb"), entry.info.size, buffer)
        if result["kernel"]["sha256"] != result["sha256"]:
            raise RuntimeError(f"kernel/NFS content differs: {entry.path}")
        samples.append(result)
    return samples


def write_file(client, path, size, payload, buffer_bytes):
    calls = total = 0
    wall_started = time.time()
    started = time.perf_counter()
    with client.open(path, "wb") as file, memoryview(payload) as view:
        while total < size:
            with view[:min(buffer_bytes, size - total)] as data:
                n = file.write(data)  # nfs-rs batch commit or local fsync, once per buffer.
                if n != len(data):
                    raise RuntimeError(f"short write: offset={total} got={n} expected={len(data)}")
                total += n
                calls += 1
    elapsed = time.perf_counter() - started
    return {"seconds": elapsed, "wall_started": wall_started, "wall_finished": time.time(),
            "bytes": total, "calls": calls,
            "mib_per_second": total / MIB / elapsed}


def io_benchmark(client, kernel, args, read_buffer):
    size = args.file_mib * MIB
    backends = {"nfs_rs": client, "kernel": kernel}
    parent = args.scratch_parent.rstrip("/") or "."
    directory = f"{parent}/.nfs-rs-verify4-{uuid.uuid4().hex}"
    owned_paths = set()
    write_results, read_results = [], []
    print(f"SCRATCH {directory!r}: {args.file_mib} MiB per case, both backends; cleanup after test", flush=True)
    client.mkdir(directory)
    try:
        if not kernel.local_path(directory).is_dir():
            raise RuntimeError("new NFS directory is not visible through the kernel mount")
        cases = [(backend, buffer_mib) for backend in backends for buffer_mib in (1, args.buffer_mib)]
        for round_index in range(args.io_rounds):
            order = cases if round_index % 2 == 0 else list(reversed(cases))
            for case_index, (writer, buffer_mib) in enumerate(order):
                # Fresh filenames/content avoid stale cross-client data passing
                # validation just because it matches a previous case's payload.
                path = f"{directory}/{writer}-{round_index}-{buffer_mib}.bin"
                payload = os.urandom(MIB) * args.buffer_mib
                expected = hashlib.sha256()
                with memoryview(payload)[:MIB] as block:
                    for _ in range(args.file_mib):
                        expected.update(block)
                expected_hash = expected.hexdigest()
                owned_paths.add(path)
                written = write_file(backends[writer], path, size, payload, buffer_mib * MIB)
                written.update(backend=writer, round=round_index + 1, buffer_mib=buffer_mib)
                write_results.append(written)
                print(f"WRITE round={round_index + 1} backend={writer} buffer={buffer_mib}MiB "
                      f"{written['mib_per_second']:.2f}MiB/s ({written['calls']} durable buffer calls)", flush=True)
                # Both readers verify every writer's output. Time includes open,
                # buffer fills, SHA-256, and close, with identical buffer sizes.
                readers = ("nfs_rs", "kernel") if (round_index + case_index) % 2 == 0 else ("kernel", "nfs_rs")
                for reader in readers:
                    with memoryview(read_buffer)[:buffer_mib * MIB] as target:
                        read = read_file(lambda: backends[reader].open(path, "rb"), size, target)
                    if read["sha256"] != expected_hash:
                        raise RuntimeError(f"checksum mismatch: writer={writer}, reader={reader}, buffer={buffer_mib}MiB")
                    read.update(backend=reader, writer=writer, round=round_index + 1, buffer_mib=buffer_mib)
                    read_results.append(read)
                    print(f"READ round={round_index + 1} backend={reader} writer={writer} buffer={buffer_mib}MiB "
                          f"{read['mib_per_second']:.2f}MiB/s ({read['calls']} buffer calls) sha256=OK", flush=True)
                client.remove(path)
                owned_paths.remove(path)
    finally:
        for path in sorted(owned_paths):
            client.remove(path, missing_ok=True)
        client.rmdir(directory)
        print(f"SCRATCH removed {directory!r}", flush=True)
    summary = {
        backend: {
            str(buffer_mib): {
                "write": statistics.median(run["mib_per_second"] for run in write_results
                                           if run["backend"] == backend and run["buffer_mib"] == buffer_mib),
                "read_mixed_writers": statistics.median(run["mib_per_second"] for run in read_results
                                          if run["backend"] == backend and run["buffer_mib"] == buffer_mib),
                "read_by_writer": {
                    writer: statistics.median(run["mib_per_second"] for run in read_results
                                              if run["backend"] == backend and run["buffer_mib"] == buffer_mib
                                              and run["writer"] == writer)
                    for writer in backends
                },
            } for buffer_mib in (1, args.buffer_mib)
        } for backend in backends
    }
    print("IO median MiB/s:", json.dumps(summary), flush=True)
    print("Local writes include fsync per buffer. Reads include SHA-256 and use warm/mixed caches; "
          "kernel page-cache results are not pure network/storage throughput.", flush=True)
    return {"file_mib": args.file_mib, "median_mib_per_second": summary,
            "write_runs": write_results, "read_runs": read_results,
            "scratch_directory": directory, "cleanup_completed": True}


def positive(value):
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=f"nfs://{HOST}{EXPORT}?version=3&mountport=2050&nfsport=2049")
    parser.add_argument("--buffer-mib", type=positive, default=64)
    parser.add_argument("--file-mib", type=positive, default=256)
    parser.add_argument("--max-dirs", type=positive, default=300)
    parser.add_argument("--scan-rounds", type=positive, default=3)
    parser.add_argument("--io-rounds", type=positive, default=2)
    parser.add_argument("--scratch-parent", default=".")
    parser.add_argument("--skip-write", action="store_true", help="scan/read existing files only")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--scan-only", action="store_true", help="compare directory scans only; no file reads or writes")
    modes.add_argument("--io-only", action="store_true", help="compare temporary-file reads/writes only; no dataset scan")
    parser.add_argument("--kernel-root", type=Path, default=Path("/mnt/smartiq/NFSv3_7"))
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()
    if args.io_only and args.skip_write:
        parser.error("--io-only cannot be combined with --skip-write")
    if args.buffer_mib <= 1:
        parser.error("--buffer-mib must exceed the 1 MiB baseline")
    if args.file_mib < args.buffer_mib:
        parser.error("--file-mib must be at least --buffer-mib")
    try:
        kernel_mount = mount_information(args.kernel_root)
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        parser.error(str(error))
    kernel = KernelClient(args.kernel_root)
    print("KERNEL MOUNT:", json.dumps(kernel_mount), flush=True)
    from nfs_rs import _internal
    if hasattr(_internal, "_arm_open_test_barrier"):
        raise RuntimeError("Real-server benchmarks require a production extension, not python-test-support. "
                           "Build with: maturin develop --release --features python-extension")
    report = {"url": args.url, "scan_only": args.scan_only, "io_only": args.io_only, "buffer_mib": args.buffer_mib, "mtime_unit": "ns since Unix epoch UTC",
              "kernel_root": str(kernel.root), "kernel_mount": kernel_mount,
              "read_timing": "open + readinto + SHA256 + close; warm/mixed page caches",
              "write_timing": "open + write + durable completion per buffer + close"}
    try:
        with Client.connect(args.url, connect_timeout=10, operation_timeout=120) as client:
            limits = client.io_limits
            report["io_limits"] = {"max_read": limits.max_read, "max_write": limits.max_write}
            print(f"CONNECTED {client.version}: max_read={limits.max_read}, max_write={limits.max_write}; "
                  f"large buffer={args.buffer_mib} MiB", flush=True)
            print(f"Large-call potential concurrent chunks: read={min(8, (min(args.buffer_mib, args.file_mib) * MIB + limits.max_read - 1) // limits.max_read)}, "
                  f"write={min(8, (min(args.buffer_mib, args.file_mib) * MIB + limits.max_write - 1) // limits.max_write)} "
                  "(client limit, not observed wire concurrency)", flush=True)
            if not args.io_only:
                discovery, files, directories = walk(client, DirectoryRef("."), use_fh=True, max_dirs=args.max_dirs)
                report["discovery"] = discovery
                print("DISCOVERY", json.dumps(discovery), flush=True)
                print("MTIME samples (ns):", [(entry.path, entry.info.mtime) for entry in files[:5]], flush=True)
                if files:
                    largest = max(files, key=lambda entry: entry.info.size)
                    print(f"Largest existing file: {largest.path!r}, {largest.info.size} bytes; "
                          f"ceil(size/max_read)={(largest.info.size + limits.max_read - 1) // limits.max_read}", flush=True)
                print("Largest directories:", [(str(ref.path), count) for ref, count in sorted(directories, key=lambda item: -item[1])[:5]], flush=True)
                report["scan"] = scan_benchmark(client, kernel, DirectoryRef("."), args)
            if not args.scan_only:
                read_buffer = bytearray(args.buffer_mib * MIB)
                if not args.io_only:
                    report["samples"] = read_samples(client, kernel, files, read_buffer)
                if not args.skip_write:
                    report["io"] = io_benchmark(client, kernel, args, read_buffer)
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        if args.json_output:
            args.json_output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
