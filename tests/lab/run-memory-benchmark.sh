#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?unique run id required}"
output="${2:?fresh evidence directory outside checkout required}"
shift 2
(( $# > 0 )) || { echo "at least one exact NFS URL is required" >&2; exit 2; }
validate_run_id "$run_id"
binary="${NFS_RS_MEMORY_BINARY:-target/release/nfs-memory-benchmark}"
[[ -x "$binary" ]] || { echo "build the release benchmark before running" >&2; exit 2; }
export LAB_PROFILE=afx
export LAB_AFX_DATA="${LAB_MEMORY_PREPARE_DATA:?directory preparation LIF required}"
export LAB_AFX_EXPORT="${LAB_MEMORY_PREPARE_EXPORT:?directory preparation export required}"
validate_ipv4 "$LAB_AFX_DATA"
validate_export_path "$LAB_AFX_EXPORT"
for url in "$@"; do
  "$binary" --url "$url" --run-id "$run_id" --case validation --validate-only >/dev/null
done
mkdir -m 700 "$output"
output="$(realpath "$output")"
case "$output/" in "$(git rev-parse --show-toplevel)/"*)
  echo "raw memory evidence must be outside checkout" >&2; rmdir "$output"; exit 2;;
esac

exec 8>/tmp/terrasync-lab-tests.lock
flock --wait 1800 8
exec 9>/tmp/terrasync-lab-performance.lock
flock --wait 1800 9
prepared=0
prepare_pid=''
controller_pid=''
finish() {
  local result=$?
  trap - EXIT
  trap '' INT TERM
  set +e
  if [[ -n "$prepare_pid" ]]; then
    # Let preparation establish whether this run owns the directory. Killing it
    # mid-mkdir would make cleanup indistinguishable from an ID collision.
    if wait "$prepare_pid"; then prepared=1; fi
  fi
  if [[ -n "$controller_pid" ]]; then
    # Its SIGTERM handler drains/terminates readers before storage is removed.
    kill -TERM "$controller_pid" 2>/dev/null
    wait "$controller_pid"
  fi
  if [[ "$prepared" == 1 ]]; then
    tests/lab/cleanup-run.sh "$run_id" >"$output/cleanup.log" 2>&1 || result=1
  fi
  printf 'FINAL_STATUS=%s\n' "$result"
  exit "$result"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
tests/lab/prepare-run.sh "$run_id" >"$output/prepare.log" 2>&1 &
prepare_pid=$!
prepare_status=0
wait "$prepare_pid" || prepare_status=$?
if [[ "$prepare_status" != 0 ]]; then
  prepare_pid=''
  exit "$prepare_status"
fi
prepared=1
prepare_pid=''
args=()
for url in "$@"; do args+=(--url "$url"); done
python3 tests/benchmarks/memory-benchmark.py \
  --binary "$binary" --run-id "$run_id" --output "$output/measurements" \
  --label "${NFS_RS_MEMORY_LABEL:-candidate}" \
  --mib "${NFS_RS_MEMORY_MIB:-1024}" \
  --rounds "${NFS_RS_MEMORY_ROUNDS:-3}" \
  --repeats "${NFS_RS_MEMORY_REPEATS:-3}" \
  --reader-rounds "${NFS_RS_MEMORY_READER_ROUNDS:-1}" \
  --timeout "${NFS_RS_MEMORY_TIMEOUT:-300}" "${args[@]}" &
controller_pid=$!
controller_status=0
wait "$controller_pid" || controller_status=$?
controller_pid=''
exit "$controller_status"
