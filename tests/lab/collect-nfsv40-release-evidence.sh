#!/usr/bin/env bash
set -euo pipefail

run_id="${1:?run id required}"
output="${2:?output directory required}"
source_dir="${NFS_RS_LAB_V40_EVIDENCE_DIR:-$output/records}"
mkdir -p "$output"
commit="$(git rev-parse HEAD)"
timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
performance="${NFS_RS_LAB_V40_PERF_OUTPUT:-nfsv40-performance.json}"

required=(semantic callback-fault lease-fault performance cleanup)
for name in "${required[@]}"; do
  if [[ ! -f "$source_dir/$name.json" || ! -f "$source_dir/$name.log" ]]; then
    echo "missing required NFSv4.0 evidence: $name" >&2
    exit 1
  fi
  cp "$source_dir/$name.json" "$source_dir/$name.log" "$output/"
done
python3 - "$output" "${required[@]}" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
for name in sys.argv[2:]:
    record = json.loads((root / f"{name}.json").read_text(encoding="utf-8"))
    if record.get("outcome") != "PASS" or record.get("exit_code") != 0:
        raise SystemExit(f"NFSv4.0 evidence did not pass: {name}")
    digest = hashlib.sha256((root / f"{name}.log").read_bytes()).hexdigest()
    if digest != record.get("sha256"):
        raise SystemExit(f"NFSv4.0 evidence hash mismatch: {name}")
PY
if [[ -f "$performance" ]]; then
  cp "$performance" "$output/performance.json"
fi
cat >"$output/manifest.json" <<EOF
{
  "schema_version": 1,
  "run_id": "$run_id",
  "commit": "$commit",
  "timestamp_utc": "$timestamp",
  "runner": "$(hostname)",
  "server": "FAS2750",
  "svm": "lizy",
  "export": "/nfsrs_v40_test",
  "lifs": ["10.128.61.200", "10.128.61.201"],
  "protocol": "4.0",
  "security_profile": "AUTH_SYS",
  "classification": "experimental",
  "typed_outcomes": ["PASS", "FAIL", "SKIP", "EXCEPTION"],
  "grace_reclaim": {
    "outcome": "EXCEPTION",
    "reason": "dedicated restart fixture unavailable on shared lizy"
  },
  "fault_scope": "runner-side run-owned destination/direction scoped",
  "cleanup": "PASS"
}
EOF
find "$output" -maxdepth 1 -type f ! -name SHA256SUMS -print0 \
  | sort -z | xargs -0 sha256sum >"$output/SHA256SUMS"
