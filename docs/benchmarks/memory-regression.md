# Memory regression diagnostics

`nfs-memory-benchmark` measures the current Rust file path on Linux. It uses
`BufferedFile::read_at`, including its short-read handling and bounded concurrent
I/O. Each response is fully verified and discarded before that worker requests
another range. It never stores the complete file in memory.

The payload is one shared allocation containing two identical 1 MiB periods.
Every WRITE takes a slice of that allocation, including negotiated 1020 KiB
NFSv4.1 chunks crossing the first period. No protocol gets an extra per-block
pattern allocation. WRITE uses bounded raw `Mount` batches, completes short
writes, retains acknowledgements, and settles each batch with
`commit_write_batch`. This prevents an entire file's uncommitted writes from
dominating the memory measurement. The measured READ path is the public
`BufferedFile` interface; the WRITE phase measures the transport and durability
path rather than `BufferedFile::write_at`.

## Deterministic checks

The normal Rust gate includes the single-response buffer-reuse regression,
short reads, EOF, ordered joins, cancellation and file-close ownership tests.
The diagnostic binary tests that both 1 MiB and 1020 KiB pattern slices share
one backing. CLI validation, report rejection and the concurrent-reader barrier
are also tested without contacting storage:

```sh
cargo test --locked --lib fileio::tests
cargo test --locked --bin nfs-memory-benchmark --test nfs_memory_benchmark_cli
```

The buffer-reuse assertion fails on the previous copying implementation. RSS
is not used as a deterministic unit-test threshold because allocator behavior,
runtime thread placement and kernel accounting affect it independently of
live payload ownership.

## Physical matrix

Build before collecting samples; do not compile during measurements. Use an
executor with access to the data LIFs and permission to mount the NFSv4.2
directory-preparation oracle. The wrapper explicitly retargets the existing
generic `afx` directory helper; that profile name does not identify the hardware.
The preparation export must be the fixture used by every supplied URL.

```sh
cargo build --release --locked --bin nfs-memory-benchmark
LAB_MEMORY_PREPARE_DATA=10.128.61.200 \
LAB_MEMORY_PREPARE_EXPORT=/deep_sync_nfs \
NFS_RS_MEMORY_MIB=1024 \
NFS_RS_MEMORY_ROUNDS=3 \
NFS_RS_MEMORY_REPEATS=3 \
  tests/lab/run-memory-benchmark.sh release-memory-UNIQUE-ID /tmp/memory-UNIQUE-ID \
  'nfs://10.128.61.200/deep_sync_nfs?version=3&uid=0&gid=0' \
  'nfs://10.128.61.200/deep_sync_nfs?version=4.0&uid=0&gid=0' \
  'nfs://10.128.61.200/deep_sync_nfs?version=4.1&uid=0&gid=0' \
  'nfs://10.128.61.201/deep_sync_nfs?version=3&uid=0&gid=0' \
  'nfs://10.128.61.201/deep_sync_nfs?version=4.0&uid=0&gid=0' \
  'nfs://10.128.61.201/deep_sync_nfs?version=4.1&uid=0&gid=0'
```

Use an existing export authorized for the executor. The example does not
create exports or change server configuration. All protocols use the default
reserved source port unless the supplied URL explicitly selects otherwise.

For each exact endpoint/protocol and repetition, the wrapper runs:

1. A new process creates and durably writes a seed file.
2. New READ-only processes use that seed at QD1 and QD8, excluding WRITE's
   allocator history from their address spaces.
3. New processes measure complete WRITE plus repeated READ at QD1 and QD8.
4. Eight new READ-only processes open the same seed, report readiness, and wait
   until the controller releases all readers. Each worker reports its own RSS;
   the sum of individual peaks is explicitly not a simultaneous machine peak.

The wrapper holds the tests lock before the performance lock, calls
`prepare-run.sh` with a unique `nightly-*` or `release-*` ID, and always calls
`cleanup-run.sh` after successful preparation. An existing case directory
causes WRITE to fail before touching its contents. Seed and combined-case files
remain under the run directory until unconditional cleanup. Raw output and
logs must remain outside the checkout.

On INT/TERM the wrapper stops and waits for the controller and its readers
before removing storage, and preserves a nonzero interruption status. If
interrupted during preparation it first waits for the helper's ownership result;
failed preparation never authorizes deleting an existing run directory.

`NFS_RS_MEMORY_BINARY` selects a prebuilt executable for an unchanged-source
baseline; `NFS_RS_MEMORY_LABEL` labels the result. Compare the same tool source
against baseline and candidate libraries, on the same executor, URLs, payload,
QD, rounds, process count and allocator environment. Each summary records the
binary SHA-256; never combine different endpoints or protocols into one baseline.
The driver leaves allocator and Tokio settings as supplied by the application.

The default per-process timeout is 300 seconds, configurable with
`NFS_RS_MEMORY_TIMEOUT`. Concurrent readers perform one complete READ each by
default; `NFS_RS_MEMORY_READER_ROUNDS` increases their rounds. Increase
`NFS_RS_MEMORY_ROUNDS` for longer single-process steady-state measurements.
Smaller payloads are useful for smoke tests but must be reported as such,
separately from the original 1 GiB workload.

## Metrics and verdicts

The executable's allocator wraps `System` only inside this diagnostic binary.
The library does not set a global allocator, change glibc settings, call
`malloc_trim`, or alter session replay protection.

Each WRITE/READ phase reports seconds, Rust live/peak requested bytes, cumulative
allocation requests and the equivalent counters for allocations of at least
512 KiB. Reallocation counts the new requested size; internal temporary
allocations inside libc's realloc, libc bookkeeping and thread stacks are not
tracked by these counters. READ time includes content verification and the EOF
check. RSS/VmHWM comes from `/proc/self/status`, whose approximate accounting
can yield slightly different high-water values between phase samples. The
summary uses the maximum sampled value.

After CLOSE, UMOUNT and dropping the mount/pattern, the executable allows up to
two seconds for background tasks to exit. New retained large Rust allocations
cause exit status 2. Errors, missing measurements and content mismatches fail;
they are not converted to unsupported-capability passes.

The matrix driver also checks that the last three idle rounds' live-byte range
and large-live-byte range are each at most 1 MiB by default. Checking the whole
live heap also covers negotiated chunks below the 512 KiB large-block cutoff.
This is an explicit diagnostic noise budget,
not an accepted RSS baseline. The driver's `--max-tail-growth-kib` can select a
different diagnostic budget, which is preserved in its summary. Fewer than
three rounds are reported as `not_evaluated_insufficient_rounds`, never as a
steady-state pass. A successful one-round reader still proves full content
verification and cleanup, not a long-running memory plateau.

These measurements and before/after throughput ratios are supplemental. The
manifest, per-environment accepted baselines and existing performance gate in
`tests/benchmarks/run-storage-benchmark-suite.sh` remain the release performance
acceptance decision. This memory matrix neither changes their thresholds nor
substitutes a measurement from another export. It also does not prove pNFS DS
routing or recovery under faults.
