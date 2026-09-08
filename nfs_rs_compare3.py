"""Compare Python nfs-rs, Rust nfs-rs and Python on a kernel NFS mount.

Build: cargo build --release --example benchmark_three_way
Run with --rust-bin <target-dir>/release/examples/benchmark_three_way
and --kernel-root pointing to the same export as --url.
Scans use identical sorted BFS inventories. I/O uses fresh 8 MiB files, one
file at a time, and every writer is verified by every reader. Timings exclude
payload generation, verification, control IPC and connection setup. Kernel
writes fsync once per buffer; native writes finish their durable batch.
"""
from __future__ import annotations
import argparse
from collections import deque
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time
import uuid
from nfs_rs import Client, DirectoryRef, FileType, _internal
from nfs_rs_verify4 import KernelClient, mount_information, positive

SIZE = 8 * 1024 * 1024

class RustWorker:
    def __init__(self, binary, url):
        self.process = subprocess.Popen([str(binary), url], stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, text=True)
        self.limits = self.receive()
    def receive(self):
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError(f"Rust worker exited: {self.process.poll()}")
        result = json.loads(line)
        if "error" in result:
            raise RuntimeError(result["error"])
        return result
    def call(self, **request):
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        return self.receive()
    def close(self):
        if self.process.poll() is None:
            self.process.stdin.write('{"op":"quit"}\n')
            self.process.stdin.flush()
            self.process.stdin.close()
            self.process.wait(timeout=30)
        self.process.stdout.close()
        if self.process.returncode:
            raise RuntimeError(f"Rust worker exit code: {self.process.returncode}")

def fingerprint(records):
    return hashlib.sha256(json.dumps(sorted(records), ensure_ascii=False).encode()).hexdigest()

def scan(client, limit):
    queue = deque([DirectoryRef(".")])
    records = []
    dirs = 0
    start = time.perf_counter()
    while queue and dirs < limit:
        parent = queue.popleft()
        entries = sorted((e for e in client.scandir(parent) if e.name not in (".", "..")),
                         key=lambda e: e.name)
        for entry in entries:
            records.append((entry.path, entry.info.type.value, entry.info.fileid, entry.info.size))
            if entry.info.type is FileType.DIRECTORY:
                queue.append(DirectoryRef(entry.path, entry.fh))
        dirs += 1
    elapsed = time.perf_counter() - start
    return dict(seconds=elapsed, directories=dirs, entries=len(records),
                truncated=bool(queue), fingerprint=fingerprint(records))

def write(client, path, payload):
    start = time.perf_counter()
    with client.open(path, "wb") as f:
        n = f.write(payload)
        if n != len(payload):
            raise RuntimeError(f"short write: {n}")
    return dict(seconds=time.perf_counter()-start, bytes=n)

def read(client, path, buffer, payload):
    start = time.perf_counter()
    with client.open(path, "rb") as f:
        n = f.readinto(buffer)
    elapsed = time.perf_counter()-start
    if n != len(payload) or buffer != payload:
        raise RuntimeError(f"read content mismatch: {path}, bytes={n}")
    return dict(seconds=elapsed, bytes=n, verified=True)

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", default="nfs://10.131.7.214/data-500w-dj-5?version=3&mountport=20048&nfsport=2049")
    p.add_argument("--kernel-root", type=Path, required=True)
    p.add_argument("--rust-bin", type=Path, required=True)
    p.add_argument("--max-dirs", type=positive, default=300)
    p.add_argument("--scan-rounds", type=positive, default=3)
    p.add_argument("--io-rounds", type=positive, default=9)
    p.add_argument("--json-output", type=Path, required=True)
    args = p.parse_args()
    if hasattr(_internal, "_arm_open_test_barrier"):
        p.error("production extension required, rebuild with --features python-extension")
    report = dict(url=args.url, file_bytes=SIZE, buffer_bytes=SIZE, concurrency_limit=8,
                  kernel_mount=mount_information(args.kernel_root),
                  scan_timing="enumeration + attributes + sorted BFS + inventory construction; verification excluded",
                  io_timing="open + read/write + close; writes durable; verification excluded; no cache dropping",
                  rust_read_api="BufferedFile::read_at (includes its result allocation)",
                  scan_runs=[], write_runs=[], read_runs=[])
    kernel = KernelClient(args.kernel_root)
    worker = None
    try:
        worker = RustWorker(args.rust_bin, args.url)
        with tempfile.TemporaryDirectory(prefix="nfs-compare3-") as temp, Client.connect(args.url, operation_timeout=120) as client:
            temp = Path(temp)
            report['limits'] = dict(python=dict(max_read=client.io_limits.max_read, max_write=client.io_limits.max_write), rust=worker.limits)
            if report['limits']['python'] != worker.limits:
                raise RuntimeError("native clients negotiated different limits")
            print('LIMITS', report['limits'], flush=True)
            backends = dict(python=client, rust=worker, kernel=kernel)
            names = list(backends)
            for round_index in range(args.scan_rounds):
                order = names[round_index%3:] + names[:round_index%3]
                for name in order:
                    if name == 'rust':
                        local = temp/'inventory.json'
                        result = worker.call(op='scan', max_dirs=args.max_dirs, local=str(local))
                        result['fingerprint'] = fingerprint(json.loads(local.read_text()))
                    else:
                        result = scan(backends[name], args.max_dirs)
                    result.update(backend=name, round=round_index+1)
                    report['scan_runs'].append(result)
                    print('SCAN', json.dumps(result), flush=True)
            report['equivalent_scan_entries'] = len({v['fingerprint'] for v in report['scan_runs']}) == 1
            if not report['equivalent_scan_entries']:
                raise RuntimeError('scan inventories differ')
            directory = '.nfs-rs-compare3-' + uuid.uuid4().hex
            report['scratch_directory'] = directory
            owned = set()
            client.mkdir(directory)
            try:
                if not kernel.local_path(directory).is_dir():
                    raise RuntimeError('scratch directory not visible on local mount')
                buffer = bytearray(SIZE)
                for round_index in range(args.io_rounds):
                    order = names[round_index%3:] + names[:round_index%3]
                    for wi, writer in enumerate(order):
                        payload = os.urandom(SIZE)
                        local = temp/'payload.bin'
                        local.write_bytes(payload)
                        worker.call(op='load', local=str(local))
                        path = f'{directory}/{round_index}-{writer}.bin'
                        owned.add(path)
                        wall_started = time.time()
                        result = worker.call(op='write', path=path) if writer=='rust' else write(backends[writer], path, payload)
                        result.update(backend=writer, round=round_index+1, wall_started=wall_started, wall_finished=time.time())
                        report['write_runs'].append(result)
                        print('WRITE', json.dumps(result), flush=True)
                        shift=(2*round_index+wi)%3
                        for reader_position, reader in enumerate(names[shift:]+names[:shift]):
                            wall_started = time.time()
                            result = worker.call(op='read', path=path) if reader=='rust' else read(backends[reader], path, buffer, payload)
                            result.update(backend=reader, writer=writer, reader_position=reader_position, round=round_index+1, wall_started=wall_started, wall_finished=time.time())
                            report['read_runs'].append(result)
                            print('READ', json.dumps(result), flush=True)
                        client.remove(path)
                        owned.remove(path)
            finally:
                for path in sorted(owned):
                    client.remove(path, missing_ok=True)
                client.rmdir(directory)
                report['cleanup_removed'] = True
                report['cleanup_verified_native'] = not client.exists(directory)
                # A kernel dentry can remain visible until its attribute cache expires.
                report['cleanup_verified_kernel'] = not kernel.local_path(directory).exists()
                report['cleanup_verified'] = report['cleanup_verified_native'] and report['cleanup_verified_kernel']
            report['summary'] = {name: dict(
                scan_median_seconds=statistics.median(v['seconds'] for v in report['scan_runs'] if v['backend']==name),
                write_median_seconds=statistics.median(v['seconds'] for v in report['write_runs'] if v['backend']==name),
                read_by_writer_median_seconds={writer: statistics.median(v['seconds'] for v in report['read_runs'] if v['backend']==name and v['writer']==writer) for writer in names}
            ) for name in names}
            print('SUMMARY', json.dumps(report['summary']), flush=True)
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        try:
            if worker is not None:
                worker.close()
        finally:
            args.json_output.write_text(json.dumps(report, indent=2)+'\n')

if __name__ == '__main__':
    main()
