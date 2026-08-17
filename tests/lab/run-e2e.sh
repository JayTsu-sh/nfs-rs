#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"

urls=(
  "nfs://$LAB_SOURCE_DATA$LAB_NFS3_EXPORT/ci/$run_id?version=3&noresvport=true"
  "nfs://$LAB_DEST_DATA$LAB_NFS3_EXPORT/ci/$run_id?version=3&noresvport=true"
  "nfs://$LAB_SOURCE_DATA$LAB_NFS41_EXPORT/ci/$run_id?version=4.1&noresvport=true"
  "nfs://$LAB_DEST_DATA$LAB_NFS41_EXPORT/ci/$run_id?version=4.1&noresvport=true"
)

export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_URLS="${urls[*]}"

cargo test --locked --test lab_e2e nfs_v3_and_v41_end_to_end -- --ignored --nocapture
