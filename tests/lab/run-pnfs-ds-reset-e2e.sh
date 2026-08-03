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
uncertain_file="$(mktemp)"
restored_file="$(mktemp)"
test_log="$(mktemp)"
rm -f "$ready_file" "$applied_file" "$uncertain_file" "$restored_file"
test_pid=""

restore_fault() {
  sudo -n /usr/local/sbin/nfsrs-lab-pnfs-fault restore-ds "$run_id" >/dev/null
  touch "$restored_file"
}
cleanup() {
  restore_fault || true
  if [[ -n "$test_pid" ]]; then
    wait "$test_pid" 2>/dev/null || true
  fi
  rm -f "$ready_file" "$applied_file" "$uncertain_file" "$restored_file" "$test_log"
}
trap cleanup EXIT

export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_PNFS_URL="nfs://$LAB_PNFS_MDS_DATA$LAB_PNFS_SECONDARY_EXPORT?version=4.1&noresvport=true"
export NFS_RS_LAB_PNFS_RUN_ID="$run_id"
export NFS_RS_LAB_PNFS_READY_FILE="$ready_file"
export NFS_RS_LAB_PNFS_FAULT_APPLIED_FILE="$applied_file"
export NFS_RS_LAB_PNFS_UNCERTAIN_FILE="$uncertain_file"
export NFS_RS_LAB_PNFS_RESTORED_FILE="$restored_file"

timeout 360 cargo test --locked --test lab_e2e \
  nfs_v41_pnfs_ds_reset_returns_uncertain -- --ignored --nocapture >"$test_log" 2>&1 &
test_pid=$!

for _ in $(seq 1 120); do
  [[ -e "$ready_file" ]] && break
  kill -0 "$test_pid" 2>/dev/null || {
    cat "$test_log" >&2
    echo "pNFS fault test exited before DS session became ready" >&2
    exit 1
  }
  sleep 1
done
[[ -e "$ready_file" ]] || { cat "$test_log" >&2; exit 1; }

sudo -n /usr/local/sbin/nfsrs-lab-pnfs-fault isolate-ds "$run_id"
touch "$applied_file"

for _ in $(seq 1 240); do
  [[ -e "$uncertain_file" ]] && break
  kill -0 "$test_pid" 2>/dev/null || {
    cat "$test_log" >&2
    echo "pNFS fault test exited without an uncertain result" >&2
    exit 1
  }
  sleep 1
done
[[ -e "$uncertain_file" ]] || {
  cat "$test_log" >&2
  echo "pNFS DS fault did not produce an uncertain result" >&2
  exit 1
}

restore_fault
wait "$test_pid"
test_pid=""
cat "$test_log"
echo "pnfs-ds-reset uncertain=1 mds-fallback=0 restored=1 checksum=ok"
