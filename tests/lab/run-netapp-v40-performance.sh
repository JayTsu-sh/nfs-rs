#!/usr/bin/env bash
set -euo pipefail

run_id="${1:?run id required}"
lif_a="${NFS_RS_LAB_V40_LIF_A:-10.128.61.200}"
lif_b="${NFS_RS_LAB_V40_LIF_B:-10.128.61.201}"
export_path="${NFS_RS_LAB_V40_EXPORT:-/nfsrs_v40_test}"
export NFS_RS_LAB_V40_URLS="nfs://${lif_a}${export_path}?version=4.0&noresvport=true&uid=0&gid=0,nfs://${lif_b}${export_path}?version=4.0&noresvport=true&uid=0&gid=0"
export NFS_RS_LAB_V40_PERF_RUN_ID="$run_id"
export NFS_RS_LAB_V40_PERF_OUTPUT="${NFS_RS_LAB_V40_PERF_OUTPUT:-nfsv40-performance.json}"
export NFS_RS_LAB_V40_PERF_COMMIT="${NFS_RS_LAB_V40_PERF_COMMIT:-$(git rev-parse HEAD)}"

cargo test --release --locked --test lab_e2e \
  nfs_v40_small_large_single_multi_performance -- --ignored --exact --nocapture
tests/lab/check-nfsv40-performance.py \
  tests/lab/nfsv40-performance-baseline.json "$NFS_RS_LAB_V40_PERF_OUTPUT"
