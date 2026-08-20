# Performance baseline report

Overall status: `complete`

| Environment | Endpoint | Protocol | Status | Capture runs | Baseline write median MiB/s | Baseline read median MiB/s | Current write MiB/s | Current read MiB/s |
|---|---|---:|---|---:|---:|---:|---:|---:|
| linux-source-v3 | `10.10.1.12:/srv/nfs/v3` | 3 | `accepted` | 45 | 28.030 | 1176.835 | — | — |
| linux-source-v40 | `10.10.1.12:/srv/nfs/v4` | 4.0 | `accepted` | 45 | 26.831 | 1150.028 | — | — |
| linux-source-v41 | `10.10.1.12:/srv/nfs/v4` | 4.1 | `accepted` | 45 | 23.094 | 941.643 | — | — |
| linux-destination-v3 | `10.10.1.13:/srv/nfs/v3` | 3 | `accepted` | 45 | 27.408 | 1118.429 | — | — |
| linux-destination-v40 | `10.10.1.13:/srv/nfs/v4` | 4.0 | `accepted` | 45 | 27.131 | 1110.925 | — | — |
| linux-destination-v41 | `10.10.1.13:/srv/nfs/v4` | 4.1 | `accepted` | 45 | 23.877 | 928.453 | — | — |
| dxn-v40 | `10.131.7.201:/jay_nfs` | 4.0 | `accepted` | 45 | 35.158 | 99.908 | — | — |
| fas2750-v40-lif-a | `10.128.61.200:/nfsrs_v40_test` | 4.0 | `accepted` | 45 | 46.417 | 52.299 | — | — |
| fas2750-v40-lif-b | `10.128.61.201:/nfsrs_v40_test` | 4.0 | `accepted` | 45 | 37.875 | 44.337 | — | — |
| netapp-pnfs-mds | `10.128.56.160:/nfsrs_pnfs_b` | 4.1 | `accepted` | 45 | 82.735 | 87.439 | — | — |
| netapp-pnfs-ds | `10.128.56.161:/nfsrs_pnfs_b` | 4.1 | `accepted` | 45 | 82.745 | 88.310 | — | — |

An environment remains `baseline_missing` until its independent baseline has the required number of accepted capture runs.

## Baseline analysis summary

### Key observations

- netapp-pnfs-ds has the highest median write throughput at 82.745 MiB/s, 258.3% above linux-source-v41 (23.094 MiB/s).
- linux-source-v3 has the highest median read throughput at 1176.835 MiB/s, 2554.3% above fas2750-v40-lif-b (44.337 MiB/s).
- FAS2750 LIF A exceeds LIF B by 22.6% for median write throughput and 18.0% for median read throughput; retain per-LIF baselines rather than combining them.
- linux-source: NFS 3 leads median writes (28.030 MiB/s), while NFS 3 leads median reads (1176.835 MiB/s).
- linux-destination: NFS 3 leads median writes (27.408 MiB/s), while NFS 3 leads median reads (1118.429 MiB/s).
- PATHCONF uses the interoperable case-insensitive default on fas2750-v40-lif-a, fas2750-v40-lif-b, netapp-pnfs-mds, netapp-pnfs-ds; this is an accepted capability difference, not a benchmark failure.

### Write-throughput ranking

| Rank | Environment | Write median MiB/s |
|---:|---|---:|
| 1 | netapp-pnfs-ds | 82.745 |
| 2 | netapp-pnfs-mds | 82.735 |
| 3 | fas2750-v40-lif-a | 46.417 |
| 4 | fas2750-v40-lif-b | 37.875 |
| 5 | dxn-v40 | 35.158 |
| 6 | linux-source-v3 | 28.030 |
| 7 | linux-destination-v3 | 27.408 |
| 8 | linux-destination-v40 | 27.131 |
| 9 | linux-source-v40 | 26.831 |
| 10 | linux-destination-v41 | 23.877 |
| 11 | linux-source-v41 | 23.094 |

### Read-throughput ranking

| Rank | Environment | Read median MiB/s |
|---:|---|---:|
| 1 | linux-source-v3 | 1176.835 |
| 2 | linux-source-v40 | 1150.028 |
| 3 | linux-destination-v3 | 1118.429 |
| 4 | linux-destination-v40 | 1110.925 |
| 5 | linux-source-v41 | 941.643 |
| 6 | linux-destination-v41 | 928.453 |
| 7 | dxn-v40 | 99.908 |
| 8 | netapp-pnfs-ds | 88.310 |
| 9 | netapp-pnfs-mds | 87.439 |
| 10 | fas2750-v40-lif-a | 52.299 |
| 11 | fas2750-v40-lif-b | 44.337 |

### Linux protocol comparison

| Site | Protocol | Write median MiB/s | Read median MiB/s |
|---|---:|---:|---:|
| linux-source | 3 | 28.030 | 1176.835 |
| linux-source | 4.0 | 26.831 | 1150.028 |
| linux-source | 4.1 | 23.094 | 941.643 |
| linux-destination | 3 | 27.408 | 1118.429 |
| linux-destination | 4.0 | 27.131 | 1110.925 |
| linux-destination | 4.1 | 23.877 | 928.453 |

### Highest baseline p95 latency observations

These are ranking observations across unlike operations, not causal diagnoses.

| Rank | Environment | Interface | p95 ms | p99 ms |
|---:|---|---|---:|---:|
| 1 | fas2750-v40-lif-b | READ | 388.999 | 680.464 |
| 2 | fas2750-v40-lif-a | READ | 347.594 | 633.940 |
| 3 | linux-destination-v41 | WRITE | 325.605 | 554.489 |
| 4 | linux-source-v41 | WRITE | 287.533 | 572.170 |
| 5 | linux-source-v41 | MOUNT | 265.356 | 395.567 |
| 6 | linux-source-v40 | WRITE | 262.842 | 486.825 |
| 7 | dxn-v40 | WRITE | 257.021 | 308.052 |
| 8 | linux-destination-v40 | WRITE | 232.206 | 456.930 |
| 9 | linux-destination-v3 | WRITE | 232.155 | 555.489 |
| 10 | linux-source-v3 | WRITE | 228.979 | 398.241 |

### PATHCONF capability groups

- `pass`: linux-source-v3, linux-source-v40, linux-source-v41, linux-destination-v3, linux-destination-v40, linux-destination-v41, dxn-v40
- `pass_with_defaults: case_insensitive`: fas2750-v40-lif-a, fas2750-v40-lif-b, netapp-pnfs-mds, netapp-pnfs-ds

## Per-interface latency

### linux-source-v3

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | 2.138 | — | — |
| UMOUNT | 0.801 | — | — |
| NULL | 0.130 | — | — |
| FSINFO | 0.129 | — | — |
| FSSTAT | 0.127 | — | — |
| MKDIR | 69.241 | — | — |
| CREATE | 50.067 | — | — |
| LOOKUP | 0.370 | — | — |
| GETATTR | 0.257 | — | — |
| ACCESS | 0.206 | — | — |
| PATHCONF | 0.159 | — | — |
| WRITE | 228.979 | — | — |
| COMMIT | 0.614 | — | — |
| CLOSE | 0.004 | — | — |
| OPEN | 0.428 | — | — |
| READ | 6.746 | — | — |
| RENAME | 51.093 | — | — |
| LINK | 51.847 | — | — |
| SYMLINK | 52.327 | — | — |
| READLINK | 0.389 | — | — |
| READDIR | 0.316 | — | — |
| REMOVE | 59.006 | — | — |
| RMDIR | 59.207 | — | — |

### linux-source-v40

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | 1.619 | — | — |
| UMOUNT | 0.192 | — | — |
| NULL | 0.129 | — | — |
| FSINFO | 0.137 | — | — |
| FSSTAT | 0.132 | — | — |
| MKDIR | 98.595 | — | — |
| CREATE | 113.160 | — | — |
| LOOKUP | 0.289 | — | — |
| GETATTR | 0.215 | — | — |
| ACCESS | 0.172 | — | — |
| PATHCONF | 0.154 | — | — |
| WRITE | 262.842 | — | — |
| COMMIT | 0.717 | — | — |
| CLOSE | 0.285 | — | — |
| OPEN | 0.529 | — | — |
| READ | 7.039 | — | — |
| RENAME | 53.460 | — | — |
| LINK | 50.882 | — | — |
| SYMLINK | 50.820 | — | — |
| READLINK | 0.383 | — | — |
| READDIR | 0.339 | — | — |
| REMOVE | 59.374 | — | — |
| RMDIR | 58.299 | — | — |

### linux-source-v41

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | 265.356 | — | — |
| UMOUNT | 132.029 | — | — |
| NULL | 0.129 | — | — |
| FSINFO | 0.137 | — | — |
| FSSTAT | 0.141 | — | — |
| MKDIR | 100.128 | — | — |
| CREATE | 92.014 | — | — |
| LOOKUP | 0.406 | — | — |
| GETATTR | 0.262 | — | — |
| ACCESS | 0.214 | — | — |
| PATHCONF | 0.167 | — | — |
| WRITE | 287.533 | — | — |
| COMMIT | 0.623 | — | — |
| CLOSE | 0.259 | — | — |
| OPEN | 0.392 | — | — |
| READ | 7.587 | — | — |
| RENAME | 66.463 | — | — |
| LINK | 48.925 | — | — |
| SYMLINK | 53.108 | — | — |
| READLINK | 0.385 | — | — |
| READDIR | 0.311 | — | — |
| REMOVE | 60.551 | — | — |
| RMDIR | 67.556 | — | — |

### linux-destination-v3

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | 2.148 | — | — |
| UMOUNT | 0.836 | — | — |
| NULL | 0.132 | — | — |
| FSINFO | 0.131 | — | — |
| FSSTAT | 0.131 | — | — |
| MKDIR | 70.869 | — | — |
| CREATE | 51.524 | — | — |
| LOOKUP | 0.362 | — | — |
| GETATTR | 0.266 | — | — |
| ACCESS | 0.206 | — | — |
| PATHCONF | 0.165 | — | — |
| WRITE | 232.155 | — | — |
| COMMIT | 0.598 | — | — |
| CLOSE | 0.004 | — | — |
| OPEN | 0.410 | — | — |
| READ | 6.887 | — | — |
| RENAME | 61.024 | — | — |
| LINK | 57.029 | — | — |
| SYMLINK | 53.598 | — | — |
| READLINK | 0.387 | — | — |
| READDIR | 0.305 | — | — |
| REMOVE | 67.425 | — | — |
| RMDIR | 61.546 | — | — |

### linux-destination-v40

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | 1.660 | — | — |
| UMOUNT | 0.203 | — | — |
| NULL | 0.128 | — | — |
| FSINFO | 0.137 | — | — |
| FSSTAT | 0.136 | — | — |
| MKDIR | 107.422 | — | — |
| CREATE | 102.458 | — | — |
| LOOKUP | 0.288 | — | — |
| GETATTR | 0.202 | — | — |
| ACCESS | 0.160 | — | — |
| PATHCONF | 0.147 | — | — |
| WRITE | 232.206 | — | — |
| COMMIT | 0.759 | — | — |
| CLOSE | 0.309 | — | — |
| OPEN | 0.542 | — | — |
| READ | 7.061 | — | — |
| RENAME | 59.720 | — | — |
| LINK | 52.965 | — | — |
| SYMLINK | 52.860 | — | — |
| READLINK | 0.378 | — | — |
| READDIR | 0.325 | — | — |
| REMOVE | 70.022 | — | — |
| RMDIR | 68.283 | — | — |

### linux-destination-v41

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | 166.446 | — | — |
| UMOUNT | 119.052 | — | — |
| NULL | 0.129 | — | — |
| FSINFO | 0.139 | — | — |
| FSSTAT | 0.138 | — | — |
| MKDIR | 99.719 | — | — |
| CREATE | 89.500 | — | — |
| LOOKUP | 0.426 | — | — |
| GETATTR | 0.291 | — | — |
| ACCESS | 0.227 | — | — |
| PATHCONF | 0.179 | — | — |
| WRITE | 325.605 | — | — |
| COMMIT | 0.637 | — | — |
| CLOSE | 0.250 | — | — |
| OPEN | 0.381 | — | — |
| READ | 7.993 | — | — |
| RENAME | 59.985 | — | — |
| LINK | 44.766 | — | — |
| SYMLINK | 55.933 | — | — |
| READLINK | 0.392 | — | — |
| READDIR | 0.319 | — | — |
| REMOVE | 69.643 | — | — |
| RMDIR | 64.032 | — | — |

### dxn-v40

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | 3.446 | — | — |
| UMOUNT | 0.215 | — | — |
| NULL | 0.246 | — | — |
| FSINFO | 0.274 | — | — |
| FSSTAT | 0.267 | — | — |
| MKDIR | 5.278 | — | — |
| CREATE | 4.791 | — | — |
| LOOKUP | 0.466 | — | — |
| GETATTR | 0.358 | — | — |
| ACCESS | 0.296 | — | — |
| PATHCONF | 0.265 | — | — |
| WRITE | 257.021 | — | — |
| COMMIT | 0.532 | — | — |
| CLOSE | 0.449 | — | — |
| OPEN | 1.369 | — | — |
| READ | 78.230 | — | — |
| RENAME | 4.276 | — | — |
| LINK | 4.016 | — | — |
| SYMLINK | 2.810 | — | — |
| READLINK | 0.347 | — | — |
| READDIR | 0.547 | — | — |
| REMOVE | 2.623 | — | — |
| RMDIR | 3.810 | — | — |

### fas2750-v40-lif-a

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | 41.591 | — | — |
| UMOUNT | 0.184 | — | — |
| NULL | 0.411 | — | — |
| FSINFO | 0.660 | — | — |
| FSSTAT | 0.539 | — | — |
| MKDIR | 1.705 | — | — |
| CREATE | 2.651 | — | — |
| LOOKUP | 0.668 | — | — |
| GETATTR | 0.552 | — | — |
| ACCESS | 0.476 | — | — |
| PATHCONF | 0.532 | — | — |
| WRITE | 128.479 | — | — |
| COMMIT | 0.574 | — | — |
| CLOSE | 0.727 | — | — |
| OPEN | 1.448 | — | — |
| READ | 347.594 | — | — |
| RENAME | 2.340 | — | — |
| LINK | 3.100 | — | — |
| SYMLINK | 1.581 | — | — |
| READLINK | 0.520 | — | — |
| READDIR | 0.629 | — | — |
| REMOVE | 1.532 | — | — |
| RMDIR | 2.731 | — | — |

### fas2750-v40-lif-b

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | 41.990 | — | — |
| UMOUNT | 0.189 | — | — |
| NULL | 0.393 | — | — |
| FSINFO | 0.671 | — | — |
| FSSTAT | 0.677 | — | — |
| MKDIR | 2.230 | — | — |
| CREATE | 3.461 | — | — |
| LOOKUP | 0.928 | — | — |
| GETATTR | 0.710 | — | — |
| ACCESS | 0.658 | — | — |
| PATHCONF | 0.706 | — | — |
| WRITE | 142.290 | — | — |
| COMMIT | 0.608 | — | — |
| CLOSE | 0.854 | — | — |
| OPEN | 1.958 | — | — |
| READ | 388.999 | — | — |
| RENAME | 3.066 | — | — |
| LINK | 4.412 | — | — |
| SYMLINK | 2.276 | — | — |
| READLINK | 0.709 | — | — |
| READDIR | 0.788 | — | — |
| REMOVE | 2.077 | — | — |
| RMDIR | 2.732 | — | — |

### netapp-pnfs-mds

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | 3.275 | — | — |
| UMOUNT | 16.518 | — | — |
| NULL | 0.387 | — | — |
| FSINFO | 0.649 | — | — |
| FSSTAT | 0.678 | — | — |
| MKDIR | 3.437 | — | — |
| CREATE | 6.080 | — | — |
| LOOKUP | 1.916 | — | — |
| GETATTR | 0.657 | — | — |
| ACCESS | 0.676 | — | — |
| PATHCONF | 0.685 | — | — |
| WRITE | 67.715 | — | — |
| COMMIT | 0.681 | — | — |
| CLOSE | 1.364 | — | — |
| OPEN | 2.745 | — | — |
| READ | 84.037 | — | — |
| RENAME | 3.508 | — | — |
| LINK | 5.511 | — | — |
| SYMLINK | 3.111 | — | — |
| READLINK | 0.659 | — | — |
| READDIR | 0.774 | — | — |
| REMOVE | 3.058 | — | — |
| RMDIR | 3.078 | — | — |

### netapp-pnfs-ds

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | 3.613 | — | — |
| UMOUNT | 7.987 | — | — |
| NULL | 0.421 | — | — |
| FSINFO | 0.563 | — | — |
| FSSTAT | 0.553 | — | — |
| MKDIR | 3.158 | — | — |
| CREATE | 3.226 | — | — |
| LOOKUP | 0.595 | — | — |
| GETATTR | 0.529 | — | — |
| ACCESS | 0.510 | — | — |
| PATHCONF | 0.505 | — | — |
| WRITE | 68.176 | — | — |
| COMMIT | 0.636 | — | — |
| CLOSE | 0.650 | — | — |
| OPEN | 1.999 | — | — |
| READ | 76.885 | — | — |
| RENAME | 2.574 | — | — |
| LINK | 3.187 | — | — |
| SYMLINK | 2.627 | — | — |
| READLINK | 0.521 | — | — |
| READDIR | 0.658 | — | — |
| REMOVE | 2.485 | — | — |
| RMDIR | 3.073 | — | — |
