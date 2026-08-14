#!/usr/bin/env bash
set -euo pipefail

run_id="${1:?run id required}"
lif_a="${NFS_RS_LAB_V40_LIF_A:-10.128.61.200}"
lif_b="${NFS_RS_LAB_V40_LIF_B:-10.128.61.201}"
export_path="${NFS_RS_LAB_V40_EXPORT:-/nfsrs_v40_test}"
export NFS_RS_LAB_V40_URLS="nfs://${lif_a}${export_path}?version=4.0&noresvport=true&uid=0&gid=0,nfs://${lif_b}${export_path}?version=4.0&noresvport=true&uid=0&gid=0"

restore_fault() {
  sudo -n /usr/local/sbin/nfsrs-lab-v40-fault restore-any "$run_id" || true
}
trap restore_fault EXIT INT TERM

for primary in 0 1; do
  target="$lif_a"
  [[ "$primary" == 1 ]] && target="$lif_b"
  armed="/tmp/nfsrs-${run_id}-${primary}.callback-armed"
  applied="/tmp/nfsrs-${run_id}-${primary}.callback-applied"
  trigger="/tmp/nfsrs-${run_id}-${primary}.callback-trigger"
  ready="/tmp/nfsrs-${run_id}-${primary}.callback-ready"
  restored="/tmp/nfsrs-${run_id}-${primary}.callback-restored"
  log="/tmp/nfsrs-${run_id}-${primary}.callback.log"
  rm -f "$armed" "$applied" "$trigger" "$ready" "$restored" "$log"
  NFS_RS_LAB_V40_FAULT_PRIMARY="$primary" \
    NFS_RS_LAB_V40_CALLBACK_FAULT_ARMED="$armed" \
    NFS_RS_LAB_V40_CALLBACK_FAULT_APPLIED="$applied" \
    NFS_RS_LAB_V40_CALLBACK_FAULT_TRIGGER="$trigger" \
    NFS_RS_LAB_V40_CALLBACK_FAULT_READY="$ready" \
    NFS_RS_LAB_V40_CALLBACK_FAULT_RESTORED="$restored" \
    cargo test --locked --test lab_e2e nfs_v40_unreachable_callback_preserves_base_io \
      -- --ignored --exact --nocapture >"$log" 2>&1 &
  test_pid=$!
  for _ in $(seq 1 45); do
    [[ -e "$armed" ]] && break
    kill -0 "$test_pid" 2>/dev/null || { cat "$log"; wait "$test_pid"; }
    sleep 1
  done
  [[ -e "$armed" ]] || { cat "$log"; echo "callback fault did not arm" >&2; exit 1; }
  sudo -n /usr/local/sbin/nfsrs-lab-v40-fault isolate-callback "$run_id" "$target"
  touch "$applied"
  for _ in $(seq 1 45); do
    [[ -e "$trigger" ]] && break
    [[ -e "$ready" ]] && break
    kill -0 "$test_pid" 2>/dev/null || { cat "$log"; wait "$test_pid"; }
    sleep 1
  done
  if [[ -e "$trigger" ]]; then
    for _ in $(seq 1 45); do
      sudo -n /usr/local/sbin/nfsrs-lab-v40-fault callback-evidence "$run_id" && break
      sleep 1
    done
    sudo -n /usr/local/sbin/nfsrs-lab-v40-fault callback-evidence "$run_id" || {
      cat "$log"
      echo "ONTAP callback SYN drop was not observed" >&2
      exit 1
    }
  fi
  restore_fault
  touch "$restored"
  for _ in $(seq 1 90); do
    [[ -e "$ready" ]] && break
    kill -0 "$test_pid" 2>/dev/null || { cat "$log"; wait "$test_pid"; }
    sleep 1
  done
  [[ -e "$ready" ]] || { cat "$log"; echo "callback fault evidence timed out" >&2; exit 1; }
  wait "$test_pid" || { cat "$log"; exit 1; }
  cat "$log"
  rm -f "$armed" "$applied" "$trigger" "$ready" "$restored" "$log"
done
