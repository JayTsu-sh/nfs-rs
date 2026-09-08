"""Compare 64 KiB / 256 KiB / 1 MiB WRITE RPCs, eight concurrent chunks,
one 8 MiB logical durable write. Uses the diagnostic Rust worker; production
negotiated sizes are unchanged. Unique scratch files are removed after checking.
"""
import argparse
import json
import os
from pathlib import Path
import statistics
import tempfile
import time
import uuid
from nfs_rs import Client
from nfs_rs_compare3 import RustWorker, SIZE
from nfs_rs_verify4 import positive

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--url', default='nfs://10.131.7.214/data-500w-dj-5?version=3&mountport=20048&nfsport=2049')
    p.add_argument('--rust-bin', type=Path, required=True)
    p.add_argument('--rounds', type=positive, default=9)
    p.add_argument('--json-output', type=Path, required=True)
    args = p.parse_args()
    report = dict(url=args.url, file_bytes=SIZE, caller_buffer_bytes=SIZE,
                  concurrency_limit=8, write_runs=[], read_runs=[],
                  timing='create/open + write + batch durability + close; preparation and verification excluded')
    worker = None
    try:
        worker = RustWorker(args.rust_bin, args.url)
        report['limits'] = worker.limits
        with tempfile.TemporaryDirectory(prefix='write-chunks-') as temp, Client.connect(args.url, operation_timeout=120) as c:
            directory = '.nfs-rs-write-chunks-' + uuid.uuid4().hex
            report['scratch_directory'] = directory
            owned = set()
            c.mkdir(directory)
            try:
                sizes = [64*1024, 256*1024, 1024*1024]
                target = bytearray(SIZE)
                for ri in range(args.rounds):
                    for chunk in sizes[ri%3:]+sizes[:ri%3]:
                        payload = os.urandom(SIZE)
                        local = Path(temp)/'payload.bin'
                        local.write_bytes(payload)
                        worker.call(op='load', local=str(local))
                        path = f'{directory}/{ri}-{chunk}.bin'
                        owned.add(path)
                        begin = time.time()
                        result = worker.call(op='write', path=path, chunk_bytes=chunk)
                        result.update(round=ri+1, backend='rust', wall_started=begin, wall_finished=time.time())
                        worker.call(op='read', path=path)
                        with c.open(path, 'rb') as f:
                            assert f.readinto(target)==SIZE and target==payload, 'content mismatch'
                        result['verified_by_rust_and_python'] = True
                        report['write_runs'].append(result)
                        print('WRITE', json.dumps(result), flush=True)
                        c.remove(path)
                        owned.remove(path)
            finally:
                for path in sorted(owned):
                    c.remove(path, missing_ok=True)
                c.rmdir(directory)
                report['cleanup_verified'] = not c.exists(directory)
            report['summary'] = {}
            for chunk in sizes:
                times = [r['seconds'] for r in report['write_runs'] if r['chunk_bytes']==chunk]
                median = statistics.median(times)
                report['summary'][str(chunk)] = dict(median_seconds=median, mib_per_second=8/median,
                                                      min_seconds=min(times), max_seconds=max(times))
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

if __name__=='__main__':
    main()
