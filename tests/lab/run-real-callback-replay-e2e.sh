#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
validate_run_id "$run_id"
tmpdir="$(mktemp -d)"
ready="$tmpdir/ready"
done_file="$tmpdir/done"
events="$tmpdir/proxy-events"
test_log="$tmpdir/test.log"
proxy_port=32049
proxy_pid=""
test_pid=""

cleanup() {
  [[ -z "$test_pid" ]] || kill "$test_pid" 2>/dev/null || true
  [[ -z "$proxy_pid" ]] || kill "$proxy_pid" 2>/dev/null || true
  rm -rf -- "$tmpdir"
}
trap cleanup EXIT INT TERM

python3 tests/lab/rpc-callback-drop-proxy.py \
  --listen "$proxy_port" --upstream "$LAB_SOURCE_DATA:2049" --events "$events" &
proxy_pid=$!
sleep 1

export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_FAULT_URL="nfs://127.0.0.1$LAB_NFS41_EXPORT/ci/$run_id?version=4.1&nfsport=$proxy_port&noresvport=true&retain-delegations=true"
export NFS_RS_LAB_FAULT_READY_FILE="$ready"
export NFS_RS_LAB_FAULT_DONE_FILE="$done_file"

timeout 240 cargo test --locked --test lab_e2e \
  nfs_v41_real_callback_reply_loss_checksum -- --ignored --nocapture >"$test_log" 2>&1 &
test_pid=$!
for _ in $(seq 1 600); do
  [[ -e "$ready" ]] && break
  kill -0 "$test_pid" 2>/dev/null || { cat "$test_log"; wait "$test_pid"; }
  sleep 0.1
done
[[ -e "$ready" ]] || { cat "$test_log"; echo "delegation test did not become ready" >&2; exit 1; }

ssh_lab "$LAB_SOURCE_MGMT" \
  "sudo -n /usr/local/sbin/terrasync-lab-nfs-fault trigger-delegation-recall '$run_id'"
for _ in $(seq 1 1200); do
  calls="$(grep -c '^callback-call$' "$events" 2>/dev/null || true)"
  dropped="$(grep -c '^callback-reply-dropped$' "$events" 2>/dev/null || true)"
  forwarded="$(grep -c '^callback-reply-forwarded$' "$events" 2>/dev/null || true)"
  injected="$(grep -c '^callback-retransmit-injected$' "$events" 2>/dev/null || true)"
  if [[ "$calls" -eq 2 && "$dropped" -eq 1 && "$injected" -eq 1 && "$forwarded" -eq 1 ]]; then
    : >"$done_file"
    break
  fi
  kill -0 "$test_pid" 2>/dev/null || { cat "$test_log"; wait "$test_pid"; }
  sleep 0.1
done
[[ -e "$done_file" ]] || { cat "$events" >&2; echo "callback replay evidence incomplete" >&2; exit 1; }
wait "$test_pid"
cat "$test_log"
printf 'callback calls=%s dropped=%s injected=%s forwarded=%s\n' \
  "$calls" "$dropped" "$injected" "$forwarded"
