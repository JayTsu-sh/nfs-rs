#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
validate_ipv4 "$LAB_PNFS_MDS_DATA"
validate_ipv4 "$LAB_PNFS_DS_DATA"
validate_export_path "$LAB_PNFS_SECONDARY_EXPORT"

require_independent_ds="${LAB_PNFS_REQUIRE_INDEPENDENT_DS:-1}"
[[ "$require_independent_ds" == 0 || "$require_independent_ds" == 1 ]] || {
  echo "LAB_PNFS_REQUIRE_INDEPENDENT_DS must be 0 or 1" >&2
  exit 2
}

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
  local connections
  connections="$(ss -Hnt state established)" || return 1
  # grep returns 1 for a valid observation with no matching connections.
  grep -F -c "$LAB_PNFS_DS_DATA:2049" <<<"$connections" || true
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
independent_ds_observed=false
if ((observed_connections > baseline_connections)); then
  independent_ds_observed=true
  printf 'pnfs-ds-observed endpoint=%s:2049 baseline=%s observed=%s\n' \
    "$LAB_PNFS_DS_DATA" "$baseline_connections" "$observed_connections"
fi

# Even in compatibility-only mode, finish CLOSE/reopen/full-payload verification.
touch "$done_file"
if wait "$test_pid"; then
  test_pid=""
else
  status=$?
  test_pid=""
  cat "$test_log" >&2
  exit "$status"
fi
cat "$test_log"

# Exercise per-file sparse I/O and cleanup separately from the small
# full-payload checksum above. Both tests also work through the MDS when the
# environment does not support pNFS; neither checksum alone proves DS routing.
timeout 300 cargo test --locked --test lab_e2e \
  nfs_v41_pnfs_multifile_sparse_io -- --ignored --nocapture

if [[ "$independent_ds_observed" == false && "$require_independent_ds" == 1 ]]; then
  echo "BLOCKED_CAPABILITY(netapp-pnfs-independent-ds): no new $LAB_PNFS_DS_DATA:2049 connection" >&2
  exit 1
fi

pnfs_status=OBSERVED
if [[ "$independent_ds_observed" == false ]]; then
  pnfs_status=NOT_OBSERVED
fi
printf 'NFSv4.1 integrity: PASS; pNFS independent DS: %s (required=%s)\n' \
  "$pnfs_status" "$require_independent_ds"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  printf 'NFSv4.1 integrity: PASS; pNFS independent DS: %s (required=%s). NOT_OBSERVED is not a pNFS pass.\n' \
    "$pnfs_status" "$require_independent_ds" >> "$GITHUB_STEP_SUMMARY"
fi
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  printf 'independent_ds_observed=%s\n' "$independent_ds_observed" >> "$GITHUB_OUTPUT"
fi
