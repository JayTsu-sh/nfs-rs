#!/usr/bin/env bash
set -euo pipefail

run_id="${1:?run id required}"
evidence_dir="${2:?evidence directory required}"
[[ "$run_id" =~ ^(nightly|release)-[A-Za-z0-9._-]{1,80}$ ]] || {
  echo "unsafe run id: $run_id" >&2
  exit 2
}
mkdir -p "$evidence_dir"
[[ -z "$(find "$evidence_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]] || {
  echo "evidence directory is not fresh: $evidence_dir" >&2
  exit 1
}
export NFS_RS_LAB_V40_EVIDENCE_RUN_ID="$run_id"

record() {
  local name="$1"
  local fault_scope="$2"
  shift 2
  NFS_RS_LAB_V40_FAULT_SCOPE="$fault_scope" \
    tests/lab/record-nfsv40-evidence.sh "$evidence_dir" "$name" "$@"
}

record semantic none flock --wait 10800 /tmp/terrasync-lab-tests.lock \
  tests/lab/run-netapp-v40-e2e.sh
record callback-fault runner-inbound-source-lif-syn flock --wait 10800 \
  /tmp/terrasync-lab-tests.lock tests/lab/run-netapp-v40-callback-fault-e2e.sh "$run_id"
record lease-fault runner-outbound-destination-lif-tcp2049 flock --wait 10800 \
  /tmp/terrasync-lab-tests.lock tests/lab/run-netapp-v40-lease-fault-e2e.sh "$run_id"
performance_output="${NFS_RS_LAB_V40_PERF_OUTPUT:-nfsv40-performance.json}"
rm -f -- "$performance_output"
record performance none flock --wait 10800 /tmp/terrasync-lab-tests.lock \
  tests/lab/run-netapp-v40-performance.sh "$run_id"
record cleanup runner-run-owned tests/lab/verify-netapp-v40-cleanup.sh "$run_id"
NFS_RS_LAB_V40_EXPECTED_OUTCOME=EXCEPTION \
NFS_RS_LAB_V40_OUTCOME_REASON="dedicated restart fixture unavailable on shared lizy" \
  record grace-reclaim shared-svm-no-restart true
