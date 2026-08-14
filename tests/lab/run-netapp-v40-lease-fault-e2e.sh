#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
target_ip="${NFS_RS_LAB_V40_LIF_A:-10.128.61.200}"
export_path="${NFS_RS_LAB_V40_EXPORT:-/nfsrs_v40_test}"
validate_ipv4 "$target_ip"
validate_export_path "$export_path"
tmpdir="$(mktemp -d)"

restore_fault() {
  sudo -n /usr/local/sbin/nfsrs-lab-v40-fault restore "$run_id" "$target_ip" || true
}
cleanup() {
  restore_fault
  rm -rf -- "$tmpdir"
}
trap cleanup EXIT INT TERM

export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_V40_FAULT_URL="nfs://$target_ip$export_path?version=4.0&noresvport=true&uid=0&gid=0"

run_case() {
  local mode="$1"
  local case_dir="$tmpdir/$mode"
  mkdir -p "$case_dir"
  export NFS_RS_LAB_V40_FAULT_MODE="$mode"
  export NFS_RS_LAB_V40_FAULT_READY_FILE="$case_dir/ready"
  export NFS_RS_LAB_V40_FAULT_APPLIED_FILE="$case_dir/applied"
  export NFS_RS_LAB_V40_FAULT_RESTORED_FILE="$case_dir/restored"
  export NFS_RS_LAB_V40_FAULT_OBSERVED_FILE="$case_dir/observed"

  timeout 240 cargo test --locked --test lab_e2e \
    nfs_v40_destination_partition_respects_lease_generation -- --ignored --nocapture &
  local test_pid=$!
  for _ in $(seq 1 600); do
    [[ -s "$case_dir/ready" ]] && break
    kill -0 "$test_pid" 2>/dev/null || wait "$test_pid"
    sleep 0.1
  done
  [[ -s "$case_dir/ready" ]] || { echo "$mode case did not become ready" >&2; exit 1; }
  local lease_seconds
  lease_seconds="$(<"$case_dir/ready")"
  [[ "$lease_seconds" =~ ^[1-9][0-9]*$ ]] || { echo "invalid lease time" >&2; exit 1; }

  sudo -n /usr/local/sbin/nfsrs-lab-v40-fault isolate "$run_id" "$target_ip"
  : >"$case_dir/applied"
  if [[ "$mode" == below ]]; then
    sleep "$((lease_seconds / 2))"
  else
    for _ in $(seq 1 $(((lease_seconds + 60) * 10))); do
      [[ -e "$case_dir/observed" ]] && break
      kill -0 "$test_pid" 2>/dev/null || wait "$test_pid"
      sleep 0.1
    done
    [[ -e "$case_dir/observed" ]] || { echo "above-lease loss was not observed" >&2; exit 1; }
  fi
  restore_fault
  : >"$case_dir/restored"
  wait "$test_pid"
}

run_case below
run_case above
