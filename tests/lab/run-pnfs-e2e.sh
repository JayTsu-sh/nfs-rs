#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
validate_ipv4 "$LAB_PNFS_MDS_DATA"
validate_ipv4 "$LAB_PNFS_DS_DATA"
validate_export_path "$LAB_PNFS_SECONDARY_EXPORT"

ready_file="$(mktemp)"
done_file="$(mktemp)"
test_log="$(mktemp)"
rm -f "$ready_file" "$done_file"
test_pid=""

cleanup() {
  touch "$done_file" 2>/dev/null || true
  if [[ -n "$test_pid" ]]; then
    wait "$test_pid" 2>/dev/null || true
  fi
  rm -f "$ready_file" "$done_file" "$test_log"
}
trap cleanup EXIT

ds_connections() {
  ss -Hnt state established 2>/dev/null |
    grep -F -c "$LAB_PNFS_DS_DATA:2049" || true
}

baseline_connections="$(ds_connections)"
export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_PNFS_URL="nfs://$LAB_PNFS_MDS_DATA$LAB_PNFS_SECONDARY_EXPORT?version=4.1&noresvport=true"
export NFS_RS_LAB_PNFS_RUN_ID="$run_id"
export NFS_RS_LAB_PNFS_READY_FILE="$ready_file"
export NFS_RS_LAB_PNFS_DONE_FILE="$done_file"

timeout 300 cargo test --locked --test lab_e2e \
  nfs_v41_pnfs_write_uses_independent_ds -- --ignored --nocapture >"$test_log" 2>&1 &
test_pid=$!

for _ in $(seq 1 120); do
  [[ -e "$ready_file" ]] && break
  kill -0 "$test_pid" 2>/dev/null || {
    cat "$test_log" >&2
    echo "pNFS test exited before WRITE completed" >&2
    exit 1
  }
  sleep 1
done
[[ -e "$ready_file" ]] || {
  cat "$test_log" >&2
  echo "BLOCKED_CAPABILITY(netapp-pnfs-layout): WRITE did not become ready" >&2
  exit 1
}

observed_connections="$baseline_connections"
for _ in $(seq 1 30); do
  observed_connections="$(ds_connections)"
  ((observed_connections > baseline_connections)) && break
  sleep 1
done
if ((observed_connections <= baseline_connections)); then
  cat "$test_log" >&2
  echo "BLOCKED_CAPABILITY(netapp-pnfs-independent-ds): no new $LAB_PNFS_DS_DATA:2049 connection" >&2
  exit 1
fi

printf 'pnfs-ds-observed endpoint=%s:2049 baseline=%s observed=%s\n' \
  "$LAB_PNFS_DS_DATA" "$baseline_connections" "$observed_connections"
touch "$done_file"
wait "$test_pid"
test_pid=""
cat "$test_log"

# Exercise the proactive per-file layout lifecycle separately from the small
# full-payload checksum above.  The first test proves that this export reaches
# the independent DS; this test proves that multiple files can cross the
# refresh boundary and still commit, return, read, and remove cleanly.
timeout 300 cargo test --locked --test lab_e2e \
  nfs_v41_pnfs_multifile_active_layout_refresh -- --ignored --nocapture
