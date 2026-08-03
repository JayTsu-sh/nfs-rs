#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
validate_ipv4 "$LAB_PNFS_MDS_DATA"
validate_export_path "$LAB_PNFS_SECONDARY_EXPORT"

export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_PNFS_URL="nfs://$LAB_PNFS_MDS_DATA$LAB_PNFS_SECONDARY_EXPORT?version=4.1&noresvport=true"
export NFS_RS_LAB_PNFS_RUN_ID="$run_id"

timeout 120 cargo test --locked --test lab_e2e \
  nfs_v41_pnfs_cleanup_run -- --ignored --nocapture
