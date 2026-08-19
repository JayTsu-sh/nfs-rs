#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?usage: run-dxn-v40-e2e.sh RUN_ID}"
validate_run_id "$run_id"
validate_ipv4 "$LAB_DXN_V40_DATA"
validate_export_path "$LAB_DXN_V40_EXPORT"

timeout 5 bash -c 'exec 3<>/dev/tcp/$1/2049' _ "$LAB_DXN_V40_DATA" || {
  echo "DXN NFSv4.0 endpoint is unreachable: $LAB_DXN_V40_DATA:2049" >&2
  exit 1
}

export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_V40_URLS="nfs://$LAB_DXN_V40_DATA$LAB_DXN_V40_EXPORT?version=4.0&noresvport=true&uid=0&gid=0"
export NFS_RS_LAB_V40_SMALL_FILE="nfs-rs-dxn-small-$run_id.bin"
export NFS_RS_LAB_V40_LARGE_FILE="nfs-rs-dxn-large-$run_id.bin"
export NFS_RS_LAB_V40_RUN_ID="dxn-$run_id"

for test_name in \
  nfs_v40_server_max_io_attributes \
  nfs_v40_single_export_end_to_end \
  nfs_v40_same_open_state_supports_concurrent_io; do
  timeout 300 cargo test --locked --test lab_e2e \
    "$test_name" -- --ignored --exact --nocapture
done

echo "DXN NFSv4.0 E2E passed for $LAB_DXN_V40_DATA:$LAB_DXN_V40_EXPORT"
