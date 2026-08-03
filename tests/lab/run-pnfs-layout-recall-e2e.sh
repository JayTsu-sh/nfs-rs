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
tmpdir="$(mktemp -d)"
trigger_file="$tmpdir/inject"
events="$tmpdir/events"
proxy_port=33049
proxy_pid=""
rm -f "$ready_file" "$applied_file" "$trigger_file" "$events"
test_pid=""

cleanup() {
  [[ -z "$proxy_pid" ]] || kill "$proxy_pid" 2>/dev/null || true
  if [[ -n "$test_pid" ]]; then
    wait "$test_pid" 2>/dev/null || true
  fi
  rm -f "$ready_file" "$applied_file" "$test_log"
  rm -rf "$tmpdir"
}
trap cleanup EXIT

python3 tests/lab/rpc-layout-recall-inject-proxy.py \
  --listen "$proxy_port" --upstream "$LAB_PNFS_MDS_DATA:2049" \
  --trigger "$trigger_file" --events "$events" &
proxy_pid=$!
sleep 1

export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_PNFS_URL="nfs://127.0.0.1$LAB_PNFS_SECONDARY_EXPORT?version=4.1&nfsport=$proxy_port&noresvport=true"
export NFS_RS_LAB_PNFS_RUN_ID="$run_id"
export NFS_RS_LAB_PNFS_READY_FILE="$ready_file"
export NFS_RS_LAB_PNFS_FAULT_APPLIED_FILE="$applied_file"

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

touch "$trigger_file"
touch "$applied_file"

if ! wait "$test_pid"; then
  test_pid=""
  cat "$test_log" >&2
  exit 1
fi
test_pid=""
cat "$test_log"
cat "$events"
grep -Fq 'pnfs-layout-recall received=' "$test_log"
grep -qxF 'layout-captured' "$events"
grep -qxF 'layout-recall-injected' "$events"
grep -qxF 'layout-recall-reply-status=0' "$events"
echo "pnfs-layout-recall received=1 returned=1 write-serialized=1 close-ordered=1 checksum=ok"
