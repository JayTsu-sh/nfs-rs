#!/usr/bin/env bash
set -euo pipefail

output="${1:?evidence directory required}"
name="${2:?evidence name required}"
shift 2
[[ "$name" =~ ^[a-z0-9-]+$ ]] || { echo "invalid evidence name" >&2; exit 2; }
(( $# > 0 )) || { echo "evidence command required" >&2; exit 2; }
mkdir -p "$output"
run_id="${NFS_RS_LAB_V40_EVIDENCE_RUN_ID:?evidence run id required}"
[[ "$run_id" =~ ^(nightly|release)-[A-Za-z0-9._-]{1,80}$ ]] || {
  echo "unsafe evidence run id: $run_id" >&2
  exit 2
}
commit="${NFS_RS_LAB_V40_EVIDENCE_COMMIT:-$(git rev-parse HEAD)}"
lif_a="${NFS_RS_LAB_V40_LIF_A:-10.128.61.200}"
lif_b="${NFS_RS_LAB_V40_LIF_B:-10.128.61.201}"
export_path="${NFS_RS_LAB_V40_EXPORT:-/nfsrs_v40_test}"
fault_scope="${NFS_RS_LAB_V40_FAULT_SCOPE:-none}"

started_at_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
set +e
"$@" 2>&1 | tee "$output/$name.log"
exit_code=${PIPESTATUS[0]}
set -e
finished_at_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
if (( exit_code == 0 )); then outcome=PASS
elif (( exit_code == 77 )); then outcome=SKIP
else outcome=FAIL
fi
expected_outcome="${NFS_RS_LAB_V40_EXPECTED_OUTCOME:-$outcome}"
[[ "$expected_outcome" == PASS || "$expected_outcome" == FAIL || \
   "$expected_outcome" == SKIP || "$expected_outcome" == EXCEPTION ]] || {
  echo "invalid expected evidence outcome: $expected_outcome" >&2
  exit 2
}
if [[ "$expected_outcome" == EXCEPTION && "$exit_code" == 0 ]]; then
  outcome=EXCEPTION
fi
reason="${NFS_RS_LAB_V40_OUTCOME_REASON:-}"
log_sha256="$(sha256sum "$output/$name.log" | cut -d' ' -f1)"

EVIDENCE_NAME="$name" STARTED_AT_UTC="$started_at_utc" \
FINISHED_AT_UTC="$finished_at_utc" OUTCOME="$outcome" EXIT_CODE="$exit_code" \
LOG_SHA256="$log_sha256" RUN_ID="$run_id" COMMIT="$commit" \
LIF_A="$lif_a" LIF_B="$lif_b" EXPORT_PATH="$export_path" \
REASON="$reason" FAULT_SCOPE="$fault_scope" \
python3 - "$output/$name.json" "$@" <<'PY'
import json
import os
import sys

document = {
    "schema_version": 1,
    "name": os.environ["EVIDENCE_NAME"],
    "run_id": os.environ["RUN_ID"],
    "commit": os.environ["COMMIT"],
    "topology": {
        "server": "FAS2750",
        "svm": "lizy",
        "export": os.environ["EXPORT_PATH"],
        "lifs": [os.environ["LIF_A"], os.environ["LIF_B"]],
        "protocol": "4.0",
    },
    "started_at_utc": os.environ["STARTED_AT_UTC"],
    "finished_at_utc": os.environ["FINISHED_AT_UTC"],
    "outcome": os.environ["OUTCOME"],
    "exit_code": int(os.environ["EXIT_CODE"]),
    "reason": os.environ["REASON"] or None,
    "fault_scope": os.environ["FAULT_SCOPE"],
    "command": sys.argv[2:],
    "log": f'{os.environ["EVIDENCE_NAME"]}.log',
    "sha256": os.environ["LOG_SHA256"],
}
with open(sys.argv[1], "w", encoding="utf-8") as stream:
    json.dump(document, stream, indent=2)
    stream.write("\n")
PY
exit "$exit_code"
