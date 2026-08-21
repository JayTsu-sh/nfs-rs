# Terrasync integration lab

The lab is shared by `nfs-rs`, `data-mover-rs`, and `terrasync-rs`.

| Role | Management | Data | Services |
|---|---|---|---|
| Controller | 10.131.9.11 | 10.10.1.11 | GitHub Actions Runner |
| Source | 10.131.9.12 | 10.10.1.12 | NFSv3, NFSv4.0, NFSv4.1, RustFS |
| Destination | 10.131.9.13 | 10.10.1.13 | NFSv3, NFSv4.0, NFSv4.1, RustFS |
| Worker | 10.131.9.14 | 10.10.1.14 | RustFS, fault injection |
| NetApp pNFS MDS | 10.128.61.20 (management) | 10.128.56.160 | ONTAP 9.19.1, SVM `Test-y` |
| NetApp pNFS DS | — | 10.128.56.161 | Independent NFSv4.1 data LIF |
| DXN NFSv4.0 | — | 10.131.7.201 | NFSv4.0 export `/jay_nfs` |

Every run must call `prepare-run.sh` with a unique `nightly-*` or `release-*`
identifier and call `cleanup-run.sh` from an `always()` step.

Management traffic uses `10.131.9.0/20`. Test data uses `10.10.1.0/24`.
Credentials are provisioned on the self-hosted runner and must not be committed.

The NetApp baseline uses `/nfsrs_pnfs_a` and `/nfsrs_pnfs_b`, both dedicated
NFSv4-only FlexGroup exports. `run-netapp-v41-e2e.sh` first runs the ordinary
NFSv4.1 compatibility suite against both exports without requiring pNFS path
evidence. `run-pnfs-e2e.sh` then mounts the second export through
the `.160` MDS LIF, performs an 8 MiB+ multi-chunk write through `nfs-rs`, and
requires a new established connection to the `.161` DS LIF before accepting the
full-payload checksum. ONTAP management credentials are not needed by nightly.

The DXN baseline uses `10.131.7.201:/jay_nfs` with an exact NFSv4.0 client URL.
Nightly fails closed if TCP/2049 is unavailable, then exercises server I/O
limits, writable namespace and metadata semantics, chunked read/write/commit,
and concurrent I/O through one OPEN state. NetApp-specific dual-LIF,
delegation, and fault-injection assumptions are intentionally not applied to
DXN.

`fas2750-storage-check` is an independent NFSv4.0 storage-path diagnostic. It
does not invoke the lab test harness. For each FAS2750 LIF it samples CREATE,
WRITE, COMMIT, READ, and REMOVE, verifies the complete read-back payload, and
prints JSON. Thresholds are explicit so a diagnostic run cannot silently
redefine the accepted release baseline:

```bash
cargo run --release --locked --bin fas2750-storage-check -- \
  --url 'nfs://10.128.61.200/nfsrs_v40_test?version=4.0&noresvport=true&uid=0&gid=0' \
  --url 'nfs://10.128.61.201/nfsrs_v40_test?version=4.0&noresvport=true&uid=0&gid=0' \
  --samples 20 --payload-mib 4 \
  --max-metadata-p95-ms 10 --max-commit-p95-ms 10 \
  --min-write-mib-s 20 --min-read-mib-s 20
```

Exit status `0` means all requested thresholds and integrity checks passed,
`2` means a measurement crossed a threshold, and `1` means the probe itself
failed.

## Cross-environment performance baselines

`tests/benchmarks/baselines/manifest.json` is the authoritative list of every
real storage endpoint and protocol. Each entry owns a distinct baseline file;
sharing a baseline across LIFs, servers, or protocol versions is forbidden.
With accepted baselines committed, the scheduled `Performance baseline
capture` workflow runs at 02:00, 10:00, and 18:00 UTC, records five independent
captures for all eleven combinations, and uploads the raw JSON and generated
report. The reduced cadence monitors drift without retaining the temporary
20-minute bootstrap schedule.

Build candidate baselines from downloaded capture artifacts with:

```bash
python3 tests/benchmarks/build-performance-baselines.py \
  --manifest tests/benchmarks/baselines/manifest.json \
  --captures-root captures \
  --output-dir candidate-baselines
```

The release-validation performance gate runs five measurements per environment
and requires at least four valid runs. A missing, under-sampled, or regressed
baseline fails closed. Metadata and mount-control p95 latency use the greater
of the baseline-relative limit and a committed 10 ms absolute floor, avoiding
false regressions from sub-millisecond jitter. Data-path latency and throughput
retain their baseline-relative limits.
`tests/benchmarks/report/performance-baselines.{json,md,html}` is the generated
machine-readable and human-readable baseline status report. All three formats
include the data-derived baseline analysis summary.

`capability-report.sh` performs read-only discovery of the NFS implementation,
pNFS configuration, installed fault tools, repository-owned lab commands, and
the `ci-runner` sudo allow-list. Nightly uploads its output as an artifact. It
must not change service or network state.

The pNFS runner also creates 16 files whose writes cross the 1 GiB layout
refresh boundary. This validates independent per-file layout renewal plus
LAYOUTCOMMIT/LAYOUTRETURN cleanup in the real NetApp lab.

`run-e2e.sh` mounts the isolated run directory on both source and destination
over NFSv3 and NFSv4.1. For each endpoint it exercises server discovery,
directory and file creation, chunked write/commit/read, attributes, READDIR,
READDIRPLUS, rename, hard links, symbolic links, removal, and unmount.

`run-kernel-v40-e2e.sh` mounts each Linux knfsd export twice with the kernel
client and exact `vers=4.0`. It verifies independent local-oracle checksums for
small, large, and concurrent files, cross-mount metadata and namespace
visibility, and cross-mount lock exclusion. The audited privileged entry point
is `admin/nfsrs-lab-kernel-v40-mount`; the runner sudo allow-list must expose
only its installed `/usr/local/sbin/nfsrs-lab-kernel-v40-mount` copy.

The Rust integration test is ignored by default and requires both
`NFS_RS_LAB_E2E=1` and a whitespace-separated `NFS_RS_LAB_URLS` value. This
keeps normal CI from accidentally accessing the private lab.

The NFSv4.0 lease fault case uses `nfsrs-lab-v40-fault` on the runner. The
helper accepts only `.200` or `.201`, drops only runner-originated destination
TCP/2049 traffic, owns the fault by run ID, and is restored by both a shell trap
and an unconditional nightly cleanup step. The below-lease case preserves the
generation after reconnect; the above-lease case requires `LostState` and
rejection of the old state token. It never changes ONTAP LIF or SVM state.

The NFSv4.0 callback fault uses the same run-owned helper, but drops only new
inbound TCP connections whose source is the selected `.200` or `.201` LIF. It
arms after OPEN exposes the typed grant/no-grant outcome, verifies ordinary
public-API I/O, and restores with a shell trap before cross-LIF checksum checks.
