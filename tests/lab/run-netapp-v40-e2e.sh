#!/usr/bin/env bash
set -euo pipefail

lif_a="${NFS_RS_LAB_V40_LIF_A:-10.128.61.200}"
lif_b="${NFS_RS_LAB_V40_LIF_B:-10.128.61.201}"
export_path="${NFS_RS_LAB_V40_EXPORT:-/nfsrs_v40_test}"

export NFS_RS_LAB_V40_URLS="nfs://${lif_a}${export_path}?version=4.0&noresvport=true,nfs://${lif_b}${export_path}?version=4.0&noresvport=true"

cargo test --locked --test lab_e2e \
  nfs_v40_mount_null_and_traversal_on_both_lifs \
  -- --ignored --exact --nocapture
