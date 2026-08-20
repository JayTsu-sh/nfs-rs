# Performance baseline report

Overall status: `baseline_missing`

| Environment | Endpoint | Protocol | Status | Capture runs | Current write MiB/s | Current read MiB/s |
|---|---|---:|---|---:|---:|---:|
| linux-source-v3 | `10.10.1.12:/srv/nfs/v3` | 3 | `baseline_missing` | 0 | 40.339 | 299.707 |
| linux-source-v40 | `10.10.1.12:/srv/nfs/v4` | 4.0 | `baseline_missing` | 0 | 40.058 | 351.208 |
| linux-source-v41 | `10.10.1.12:/srv/nfs/v4` | 4.1 | `baseline_missing` | 0 | 23.304 | 310.391 |
| linux-destination-v3 | `10.10.1.13:/srv/nfs/v3` | 3 | `baseline_missing` | 0 | 30.304 | 397.703 |
| linux-destination-v40 | `10.10.1.13:/srv/nfs/v4` | 4.0 | `baseline_missing` | 0 | 39.320 | 383.931 |
| linux-destination-v41 | `10.10.1.13:/srv/nfs/v4` | 4.1 | `baseline_missing` | 0 | 24.130 | 262.008 |
| dxn-v40 | `10.131.7.201:/jay_nfs` | 4.0 | `baseline_missing` | 0 | 25.758 | 89.412 |
| fas2750-v40-lif-a | `10.128.61.200:/nfsrs_v40_test` | 4.0 | `baseline_missing` | 0 | 43.486 | 51.811 |
| fas2750-v40-lif-b | `10.128.61.201:/nfsrs_v40_test` | 4.0 | `baseline_missing` | 0 | 32.061 | 37.617 |
| netapp-pnfs-mds | `10.128.56.160:/nfsrs_pnfs_b` | 4.1 | `baseline_missing` | 0 | 40.579 | 72.810 |
| netapp-pnfs-ds | `10.128.56.161:/nfsrs_pnfs_b` | 4.1 | `baseline_missing` | 0 | 72.518 | 75.726 |

An environment remains `baseline_missing` until its independent baseline has the required number of accepted capture runs.

## Per-interface latency

### linux-source-v3

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | — | 1.760 | pass |
| UMOUNT | — | — | — |
| NULL | — | 0.095 | pass |
| FSINFO | — | 0.102 | pass |
| FSSTAT | — | 0.100 | pass |
| MKDIR | — | 36.080 | pass |
| CREATE | — | 20.574 | pass |
| LOOKUP | — | 0.372 | pass |
| GETATTR | — | 0.140 | pass |
| ACCESS | — | 0.118 | pass |
| PATHCONF | — | 0.112 | pass |
| WRITE | — | 24.790 | pass |
| COMMIT | — | 0.251 | pass |
| CLOSE | — | 0.001 | pass |
| OPEN | — | 0.261 | pass |
| READ | — | 3.337 | pass |
| RENAME | — | 21.016 | pass |
| LINK | — | 18.402 | pass |
| SYMLINK | — | 21.343 | pass |
| READLINK | — | 0.416 | pass |
| READDIR | — | 0.142 | pass |
| REMOVE | — | 20.769 | pass |
| RMDIR | — | 21.906 | pass |

### linux-source-v40

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | — | 1.046 | pass |
| UMOUNT | — | — | — |
| NULL | — | 0.098 | pass |
| FSINFO | — | 0.105 | pass |
| FSSTAT | — | 0.126 | pass |
| MKDIR | — | 43.187 | pass |
| CREATE | — | 97.753 | pass |
| LOOKUP | — | 0.145 | pass |
| GETATTR | — | 0.105 | pass |
| ACCESS | — | 0.098 | pass |
| PATHCONF | — | 0.103 | pass |
| WRITE | — | 24.964 | pass |
| COMMIT | — | 0.396 | pass |
| CLOSE | — | 0.221 | pass |
| OPEN | — | 0.454 | pass |
| READ | — | 2.847 | pass |
| RENAME | — | 20.990 | pass |
| LINK | — | 20.703 | pass |
| SYMLINK | — | 20.037 | pass |
| READLINK | — | 0.173 | pass |
| READDIR | — | 0.186 | pass |
| REMOVE | — | 23.086 | pass |
| RMDIR | — | 23.594 | pass |

### linux-source-v41

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | — | 67.464 | pass |
| UMOUNT | — | — | — |
| NULL | — | 0.115 | pass |
| FSINFO | — | 0.123 | pass |
| FSSTAT | — | 0.119 | pass |
| MKDIR | — | 43.439 | pass |
| CREATE | — | 41.478 | pass |
| LOOKUP | — | 0.232 | pass |
| GETATTR | — | 0.137 | pass |
| ACCESS | — | 0.120 | pass |
| PATHCONF | — | 0.105 | pass |
| WRITE | — | 42.911 | pass |
| COMMIT | — | 0.249 | pass |
| CLOSE | — | 0.155 | pass |
| OPEN | — | 0.298 | pass |
| READ | — | 3.222 | pass |
| RENAME | — | 19.149 | pass |
| LINK | — | 20.347 | pass |
| SYMLINK | — | 21.336 | pass |
| READLINK | — | 0.292 | pass |
| READDIR | — | 0.199 | pass |
| REMOVE | — | 21.032 | pass |
| RMDIR | — | 24.390 | pass |

### linux-destination-v3

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | — | 1.558 | pass |
| UMOUNT | — | — | — |
| NULL | — | 0.100 | pass |
| FSINFO | — | 0.109 | pass |
| FSSTAT | — | 0.097 | pass |
| MKDIR | — | 30.466 | pass |
| CREATE | — | 20.687 | pass |
| LOOKUP | — | 0.166 | pass |
| GETATTR | — | 0.161 | pass |
| ACCESS | — | 0.133 | pass |
| PATHCONF | — | 0.098 | pass |
| WRITE | — | 32.999 | pass |
| COMMIT | — | 0.237 | pass |
| CLOSE | — | 0.001 | pass |
| OPEN | — | 0.259 | pass |
| READ | — | 2.514 | pass |
| RENAME | — | 57.203 | pass |
| LINK | — | 96.797 | pass |
| SYMLINK | — | 89.013 | pass |
| READLINK | — | 0.183 | pass |
| READDIR | — | 0.160 | pass |
| REMOVE | — | 21.231 | pass |
| RMDIR | — | 20.970 | pass |

### linux-destination-v40

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | — | 1.094 | pass |
| UMOUNT | — | — | — |
| NULL | — | 0.128 | pass |
| FSINFO | — | 0.111 | pass |
| FSSTAT | — | 0.123 | pass |
| MKDIR | — | 48.375 | pass |
| CREATE | — | 83.811 | pass |
| LOOKUP | — | 0.122 | pass |
| GETATTR | — | 0.104 | pass |
| ACCESS | — | 0.111 | pass |
| PATHCONF | — | 0.098 | pass |
| WRITE | — | 25.432 | pass |
| COMMIT | — | 0.283 | pass |
| CLOSE | — | 0.181 | pass |
| OPEN | — | 0.412 | pass |
| READ | — | 2.605 | pass |
| RENAME | — | 18.680 | pass |
| LINK | — | 20.093 | pass |
| SYMLINK | — | 20.827 | pass |
| READLINK | — | 0.385 | pass |
| READDIR | — | 0.384 | pass |
| REMOVE | — | 20.566 | pass |
| RMDIR | — | 21.094 | pass |

### linux-destination-v41

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | — | 84.954 | pass |
| UMOUNT | — | — | — |
| NULL | — | 0.101 | pass |
| FSINFO | — | 0.109 | pass |
| FSSTAT | — | 0.110 | pass |
| MKDIR | — | 43.257 | pass |
| CREATE | — | 41.319 | pass |
| LOOKUP | — | 0.147 | pass |
| GETATTR | — | 0.123 | pass |
| ACCESS | — | 0.141 | pass |
| PATHCONF | — | 0.121 | pass |
| WRITE | — | 41.442 | pass |
| COMMIT | — | 0.567 | pass |
| CLOSE | — | 0.168 | pass |
| OPEN | — | 0.250 | pass |
| READ | — | 3.817 | pass |
| RENAME | — | 19.972 | pass |
| LINK | — | 20.549 | pass |
| SYMLINK | — | 20.564 | pass |
| READLINK | — | 0.142 | pass |
| READDIR | — | 0.163 | pass |
| REMOVE | — | 19.136 | pass |
| RMDIR | — | 20.795 | pass |

### dxn-v40

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | — | 1.433 | pass |
| UMOUNT | — | — | — |
| NULL | — | 0.178 | pass |
| FSINFO | — | 0.209 | pass |
| FSSTAT | — | 0.211 | pass |
| MKDIR | — | 6.075 | pass |
| CREATE | — | 4.822 | pass |
| LOOKUP | — | 0.350 | pass |
| GETATTR | — | 0.269 | pass |
| ACCESS | — | 0.203 | pass |
| PATHCONF | — | 0.209 | pass |
| WRITE | — | 38.824 | pass |
| COMMIT | — | 0.421 | pass |
| CLOSE | — | 0.296 | pass |
| OPEN | — | 0.942 | pass |
| READ | — | 11.184 | pass |
| RENAME | — | 3.428 | pass |
| LINK | — | 2.940 | pass |
| SYMLINK | — | 2.005 | pass |
| READLINK | — | 0.232 | pass |
| READDIR | — | 0.329 | pass |
| REMOVE | — | 2.055 | pass |
| RMDIR | — | 2.738 | pass |

### fas2750-v40-lif-a

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | — | 21.634 | pass |
| UMOUNT | — | — | — |
| NULL | — | 0.183 | pass |
| FSINFO | — | 0.278 | pass |
| FSSTAT | — | 0.278 | pass |
| MKDIR | — | 1.687 | pass |
| CREATE | — | 2.581 | pass |
| LOOKUP | — | 0.417 | pass |
| GETATTR | — | 0.337 | pass |
| ACCESS | — | 0.267 | pass |
| PATHCONF | — | — | unsupported: NFSv4.0 PATHCONF server omitted required attributes [16] |
| WRITE | — | 22.996 | pass |
| COMMIT | — | 0.272 | pass |
| CLOSE | — | 0.382 | pass |
| OPEN | — | 1.072 | pass |
| READ | — | 19.301 | pass |
| RENAME | — | 1.566 | pass |
| LINK | — | 2.459 | pass |
| SYMLINK | — | 1.222 | pass |
| READLINK | — | 0.265 | pass |
| READDIR | — | 0.467 | pass |
| REMOVE | — | 1.347 | pass |
| RMDIR | — | 0.763 | pass |

### fas2750-v40-lif-b

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | — | 21.984 | pass |
| UMOUNT | — | — | — |
| NULL | — | 0.195 | pass |
| FSINFO | — | 0.384 | pass |
| FSSTAT | — | 0.395 | pass |
| MKDIR | — | 1.501 | pass |
| CREATE | — | 2.302 | pass |
| LOOKUP | — | 0.611 | pass |
| GETATTR | — | 0.414 | pass |
| ACCESS | — | 0.387 | pass |
| PATHCONF | — | — | unsupported: NFSv4.0 PATHCONF server omitted required attributes [16] |
| WRITE | — | 31.191 | pass |
| COMMIT | — | 0.383 | pass |
| CLOSE | — | 0.637 | pass |
| OPEN | — | 1.477 | pass |
| READ | — | 26.583 | pass |
| RENAME | — | 2.180 | pass |
| LINK | — | 3.484 | pass |
| SYMLINK | — | 1.592 | pass |
| READLINK | — | 0.597 | pass |
| READDIR | — | 0.598 | pass |
| REMOVE | — | 1.610 | pass |
| RMDIR | — | 0.824 | pass |

### netapp-pnfs-mds

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | — | 2.501 | pass |
| UMOUNT | — | — | — |
| NULL | — | 0.270 | pass |
| FSINFO | — | 0.553 | pass |
| FSSTAT | — | 0.495 | pass |
| MKDIR | — | 1.611 | pass |
| CREATE | — | 2.151 | pass |
| LOOKUP | — | 0.615 | pass |
| GETATTR | — | 0.452 | pass |
| ACCESS | — | 0.392 | pass |
| PATHCONF | — | 0.365 | pass |
| WRITE | — | 24.643 | pass |
| COMMIT | — | 0.257 | pass |
| CLOSE | — | 0.765 | pass |
| OPEN | — | 1.703 | pass |
| READ | — | 13.734 | pass |
| RENAME | — | 2.132 | pass |
| LINK | — | 2.209 | pass |
| SYMLINK | — | 1.463 | pass |
| READLINK | — | 0.409 | pass |
| READDIR | — | 0.484 | pass |
| REMOVE | — | 1.387 | pass |
| RMDIR | — | 0.731 | pass |

### netapp-pnfs-ds

| Interface | Baseline p95 ms | Current p95 ms | Current status |
|---|---:|---:|---|
| MOUNT | — | 2.522 | pass |
| UMOUNT | — | — | — |
| NULL | — | 0.422 | pass |
| FSINFO | — | 0.358 | pass |
| FSSTAT | — | 0.253 | pass |
| MKDIR | — | 2.588 | pass |
| CREATE | — | 2.839 | pass |
| LOOKUP | — | 0.537 | pass |
| GETATTR | — | 0.349 | pass |
| ACCESS | — | 0.235 | pass |
| PATHCONF | — | 0.264 | pass |
| WRITE | — | 13.790 | pass |
| COMMIT | — | 0.219 | pass |
| CLOSE | — | 0.301 | pass |
| OPEN | — | 1.413 | pass |
| READ | — | 13.205 | pass |
| RENAME | — | 2.090 | pass |
| LINK | — | 2.764 | pass |
| SYMLINK | — | 2.329 | pass |
| READLINK | — | 0.310 | pass |
| READDIR | — | 0.429 | pass |
| REMOVE | — | 2.059 | pass |
| RMDIR | — | 1.733 | pass |
