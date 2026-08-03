#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
validate_ipv4 "$LAB_PNFS_MDS_DATA"
validate_ipv4 "$LAB_PNFS_DS_DATA"
validate_export_path "$LAB_PNFS_PRIMARY_EXPORT"
validate_export_path "$LAB_PNFS_SECONDARY_EXPORT"

export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_URLS="nfs://$LAB_PNFS_MDS_DATA$LAB_PNFS_PRIMARY_EXPORT?version=4.1&noresvport=true nfs://$LAB_PNFS_DS_DATA$LAB_PNFS_SECONDARY_EXPORT?version=4.1&noresvport=true"

timeout 300 cargo test --locked --test lab_e2e \
  nfs_v3_and_v41_end_to_end -- --ignored --nocapture
