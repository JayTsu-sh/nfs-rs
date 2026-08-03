#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
validate_ipv4 "$LAB_PNFS_MDS_DATA"
validate_ipv4 "$LAB_PNFS_DS_DATA"
validate_export_path "$LAB_PNFS_SECONDARY_EXPORT"

ready_file="$(mktemp)"
applied_file="$(mktemp)"
done_file="$(mktemp)"
test_log="$(mktemp)"
rm -f "$ready_file" "$applied_file" "$done_file"
test_pid=""

restore_fault() {
  sudo -n /usr/local/sbin/nfsrs-lab-pnfs-fault restore-ds "$run_id" >/dev/null
}
cleanup() {
  restore_fault || true
  if [[ -n "$test_pid" ]]; then
    wait "$test_pid" 2>/dev/null || true
  fi
  rm -f "$ready_file" "$applied_file" "$done_file" "$test_log"
}
trap cleanup EXIT

ds_connections() {
  ss -Hnt state established 2>/dev/null |
    grep -F -c "$LAB_PNFS_DS_DATA:2049" || true
}

baseline_connections="$(ds_connections)"
maximum_connections="$baseline_connections"
export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_PNFS_URL="nfs://$LAB_PNFS_MDS_DATA$LAB_PNFS_SECONDARY_EXPORT?version=4.1&noresvport=true"
export NFS_RS_LAB_PNFS_RUN_ID="$run_id"
export NFS_RS_LAB_PNFS_READY_FILE="$ready_file"
export NFS_RS_LAB_PNFS_FAULT_APPLIED_FILE="$applied_file"
export NFS_RS_LAB_PNFS_DONE_FILE="$done_file"

timeout 300 cargo test --locked --test lab_e2e \
  nfs_v41_pnfs_ds_unreachable_before_write_falls_back_to_mds \
  -- --ignored --nocapture >"$test_log" 2>&1 &
test_pid=$!

for _ in $(seq 1 120); do
  [[ -e "$ready_file" ]] && break
  kill -0 "$test_pid" 2>/dev/null || {
    cat "$test_log" >&2
    echo "pNFS preflight test exited before the no-WRITE boundary" >&2
    exit 1
  }
  sleep 1
done
[[ -e "$ready_file" ]] || { cat "$test_log" >&2; exit 1; }

sudo -n /usr/local/sbin/nfsrs-lab-pnfs-fault isolate-ds "$run_id"
touch "$applied_file"

for _ in $(seq 1 120); do
  current_connections="$(ds_connections)"
  ((current_connections > maximum_connections)) && maximum_connections="$current_connections"
  [[ -e "$done_file" ]] && break
  kill -0 "$test_pid" 2>/dev/null || {
    cat "$test_log" >&2
    echo "pNFS preflight test exited before MDS checksum evidence" >&2
    exit 1
  }
  sleep 1
done
[[ -e "$done_file" ]] || { cat "$test_log" >&2; exit 1; }
if ((maximum_connections > baseline_connections)); then
  cat "$test_log" >&2
  echo "DS connection appeared after pre-WRITE isolation" >&2
  exit 1
fi

restore_fault
wait "$test_pid"
test_pid=""
cat "$test_log"
echo "pnfs-preflight preflight_failed=1 ds_write_sent=0 mds_fallback=1 restored=1 checksum=ok"
