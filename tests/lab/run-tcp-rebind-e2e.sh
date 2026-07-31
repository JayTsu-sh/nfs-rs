#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
tmpdir="$(mktemp -d)"
ready="$tmpdir/ready"
done_file="$tmpdir/done"
stage_file="$tmpdir/stage"
ack_file="$tmpdir/ack"
fault_host="$LAB_SOURCE_MGMT"

restore() {
  ssh_lab "$fault_host" \
    "sudo -n /usr/local/sbin/terrasync-lab-nfs-fault restore '$run_id'" || true
  rm -rf -- "$tmpdir"
}
trap restore EXIT INT TERM

export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_FAULT_URL="nfs://$LAB_SOURCE_DATA$LAB_NFS41_EXPORT/ci/$run_id?version=4.1&noresvport=true"
export NFS_RS_LAB_FAULT_READY_FILE="$ready"
export NFS_RS_LAB_FAULT_DONE_FILE="$done_file"
export NFS_RS_LAB_FAULT_STAGE_FILE="$stage_file"
export NFS_RS_LAB_FAULT_ACK_FILE="$ack_file"

timeout 180 cargo test --locked --test lab_e2e \
  nfs_v41_tcp_reset_rebind_checksum -- --ignored --nocapture &
test_pid=$!

for _ in $(seq 1 600); do
  [[ -e "$ready" ]] && break
  kill -0 "$test_pid" 2>/dev/null || wait "$test_pid"
  sleep 0.1
done
[[ -e "$ready" ]] || {
  echo "TCP rebind test did not become ready" >&2
  exit 1
}

for generation in 1 2; do
  ssh_lab "$fault_host" \
    "sudo -n /usr/local/sbin/terrasync-lab-nfs-fault apply-tcp-reset '$run_id'"
  printf '%s\n' "$generation" >"$stage_file"
  acknowledged=false
  for _ in $(seq 1 900); do
    if [[ -e "$ack_file" ]] && [[ "$(cat "$ack_file")" == "$generation" ]]; then
      acknowledged=true
      break
    fi
    kill -0 "$test_pid" 2>/dev/null || wait "$test_pid"
    sleep 0.1
  done
  [[ "$acknowledged" == true ]] || {
    echo "connection generation $generation was not acknowledged" >&2
    exit 1
  }
  ssh_lab "$fault_host" \
    "sudo -n /usr/local/sbin/terrasync-lab-nfs-fault restore '$run_id'"
done
: >"$done_file"
wait "$test_pid"
