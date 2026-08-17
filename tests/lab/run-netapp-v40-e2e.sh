#!/usr/bin/env bash
set -euo pipefail

lif_a="${NFS_RS_LAB_V40_LIF_A:-10.128.61.200}"
lif_b="${NFS_RS_LAB_V40_LIF_B:-10.128.61.201}"
export_path="${NFS_RS_LAB_V40_EXPORT:-/nfsrs_v40_test}"

export NFS_RS_LAB_V40_URLS="nfs://${lif_a}${export_path}?version=4.0&noresvport=true&uid=0&gid=0,nfs://${lif_b}${export_path}?version=4.0&noresvport=true&uid=0&gid=0"
export NFS_RS_LAB_V40_SMALL_FILE="${NFS_RS_LAB_V40_SMALL_FILE:-nfs-rs-small.bin}"
export NFS_RS_LAB_V40_LARGE_FILE="${NFS_RS_LAB_V40_LARGE_FILE:-nfs-rs-large.bin}"

for test_name in \
  nfs_v40_mount_null_and_traversal_on_both_lifs \
  nfs_v40_delegation_recall_across_both_lifs \
  nfs_v40_open_io_commit_close_on_both_lifs \
  nfs_v40_same_open_state_supports_concurrent_io; do
  cargo test --locked --test lab_e2e \
    "$test_name" \
    -- --ignored --exact --nocapture
done
