#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
validate_ipv4 "$LAB_PNFS_MDS_DATA"
validate_export_path "$LAB_PNFS_SECONDARY_EXPORT"

ready_file="$(mktemp)"
applied_file="$(mktemp)"
test_log="$(mktemp)"
rm -f "$ready_file" "$applied_file"
test_pid=""

cleanup() {
  if [[ -n "$test_pid" ]]; then
    wait "$test_pid" 2>/dev/null || true
  fi
  rm -f "$ready_file" "$applied_file" "$test_log"
}
trap cleanup EXIT

export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_PNFS_URL="nfs://$LAB_PNFS_MDS_DATA$LAB_PNFS_SECONDARY_EXPORT?version=4.1&noresvport=true"
export NFS_RS_LAB_PNFS_RUN_ID="$run_id"
export NFS_RS_LAB_PNFS_READY_FILE="$ready_file"
export NFS_RS_LAB_PNFS_FAULT_APPLIED_FILE="$applied_file"
export RUST_LOG="nfs_rs::nfs41::mount=info"

timeout 360 cargo test --locked --test lab_e2e \
  nfs_v41_pnfs_layout_recall_during_write_and_close \
  -- --ignored --nocapture >"$test_log" 2>&1 &
test_pid=$!

for _ in $(seq 1 120); do
  [[ -e "$ready_file" ]] && break
  kill -0 "$test_pid" 2>/dev/null || { cat "$test_log" >&2; exit 1; }
  sleep 1
done
[[ -e "$ready_file" ]] || { cat "$test_log" >&2; exit 1; }

sudo -n /usr/local/sbin/nfsrs-lab-pnfs-fault trigger-layout-recall "$run_id"
touch "$applied_file"

if ! wait "$test_pid"; then
  test_pid=""
  cat "$test_log" >&2
  exit 1
fi
test_pid=""
cat "$test_log"
grep -Fq 'pnfs_layout_recall_received' "$test_log"
grep -Fq 'pnfs_layout_recall_returned' "$test_log"
echo "pnfs-layout-recall received=1 returned=1 write-serialized=1 close-ordered=1 checksum=ok"
