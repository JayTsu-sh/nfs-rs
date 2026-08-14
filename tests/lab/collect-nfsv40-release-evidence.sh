#!/usr/bin/env bash
set -euo pipefail

run_id="${1:?run id required}"
output="${2:?output directory required}"
mkdir -p "$output"
commit="$(git rev-parse HEAD)"
timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
performance="${NFS_RS_LAB_V40_PERF_OUTPUT:-nfsv40-performance.json}"

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
  "grace_reclaim_exception": "dedicated restart fixture unavailable on shared lizy",
  "fault_scope": "runner-side run-owned destination/direction scoped",
  "cleanup": "workflow always-steps required"
}
EOF
find "$output" -maxdepth 1 -type f ! -name SHA256SUMS -print0 \
  | sort -z | xargs -0 sha256sum >"$output/SHA256SUMS"
