#!/usr/bin/env bash
set -euo pipefail

run_id="${1:?run id required}"
output="${2:?output directory required}"
source_dir="${NFS_RS_LAB_V40_EVIDENCE_DIR:-$output/records}"
[[ "$run_id" =~ ^(nightly|release)-[A-Za-z0-9._-]{1,80}$ ]] || {
  echo "unsafe run id: $run_id" >&2
  exit 2
}
mkdir -p "$output"
commit="${NFS_RS_LAB_V40_EVIDENCE_COMMIT:-$(git rev-parse HEAD)}"
performance="${NFS_RS_LAB_V40_PERF_OUTPUT:-nfsv40-performance.json}"
lif_a="${NFS_RS_LAB_V40_LIF_A:-10.128.61.200}"
lif_b="${NFS_RS_LAB_V40_LIF_B:-10.128.61.201}"
export_path="${NFS_RS_LAB_V40_EXPORT:-/nfsrs_v40_test}"

# Preserve the raw measurement before validating the rest of the evidence. A
# performance-gate failure stops the matrix before cleanup records are written,
# but the measured values are still essential failure evidence.
if [[ -f "$performance" ]]; then
  cp "$performance" "$output/performance-report.json"
fi

required=(semantic callback-fault lease-fault performance cleanup grace-reclaim)
for name in "${required[@]}"; do
  if [[ ! -f "$source_dir/$name.json" || ! -f "$source_dir/$name.log" ]]; then
    echo "missing required NFSv4.0 evidence: $name" >&2
    exit 1
  fi
  cp "$source_dir/$name.json" "$source_dir/$name.log" "$output/"
done
[[ -f "$output/performance-report.json" ]] || {
  echo "missing required NFSv4.0 performance report" >&2
  exit 1
}

RUN_ID="$run_id" COMMIT="$commit" LIF_A="$lif_a" LIF_B="$lif_b" \
EXPORT_PATH="$export_path" python3 - "$output" "${required[@]}" <<'PY'
import datetime
import hashlib
import json
import os
import pathlib
import socket
import sys

root = pathlib.Path(sys.argv[1])
now = datetime.datetime.now(datetime.timezone.utc)
records = []
for name in sys.argv[2:]:
    record = json.loads((root / f"{name}.json").read_text(encoding="utf-8"))
    expected = "EXCEPTION" if name == "grace-reclaim" else "PASS"
    if record.get("outcome") != expected or record.get("exit_code") != 0:
        raise SystemExit(f"NFSv4.0 evidence has invalid typed outcome: {name}")
    if record.get("run_id") != os.environ["RUN_ID"] or record.get("commit") != os.environ["COMMIT"]:
        raise SystemExit(f"stale NFSv4.0 evidence identity: {name}")
    topology = record.get("topology", {})
    if topology.get("lifs") != [os.environ["LIF_A"], os.environ["LIF_B"]] or topology.get("export") != os.environ["EXPORT_PATH"]:
        raise SystemExit(f"NFSv4.0 evidence topology mismatch: {name}")
    started = datetime.datetime.fromisoformat(record["started_at_utc"].replace("Z", "+00:00"))
    finished = datetime.datetime.fromisoformat(record["finished_at_utc"].replace("Z", "+00:00"))
    if finished < started or now - finished > datetime.timedelta(hours=24):
        raise SystemExit(f"stale NFSv4.0 evidence timestamp: {name}")
    digest = hashlib.sha256((root / f"{name}.log").read_bytes()).hexdigest()
    if digest != record.get("sha256"):
        raise SystemExit(f"NFSv4.0 evidence hash mismatch: {name}")
    records.append(record)

performance = json.loads((root / "performance-report.json").read_text(encoding="utf-8"))
if performance.get("run_id") != os.environ["RUN_ID"] or performance.get("commit") != os.environ["COMMIT"]:
    raise SystemExit("stale NFSv4.0 performance report identity")
if performance.get("lifs") != [os.environ["LIF_A"], os.environ["LIF_B"]]:
    raise SystemExit("NFSv4.0 performance report topology mismatch")
if performance.get("protocol") != "4.0" or performance.get("liveness") != "pass":
    raise SystemExit("NFSv4.0 performance report did not prove liveness")
if len(performance.get("workloads", [])) != 4:
    raise SystemExit("NFSv4.0 performance report lacks the four workload quadrants")

manifest = {
    "schema_version": 1,
    "run_id": os.environ["RUN_ID"],
    "commit": os.environ["COMMIT"],
    "timestamp_utc": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "runner": socket.gethostname(),
    "topology": records[0]["topology"],
    "security_profile": "AUTH_SYS",
    "classification": "experimental",
    "typed_outcomes": ["PASS", "FAIL", "SKIP", "EXCEPTION"],
    "grace_reclaim": next(record for record in records if record["name"] == "grace-reclaim"),
    "cleanup": next(record for record in records if record["name"] == "cleanup")["outcome"],
    "records": [record["name"] for record in records],
}
(root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
PY
find "$output" -maxdepth 1 -type f ! -name SHA256SUMS -print0 \
  | sort -z | xargs -0 sha256sum >"$output/SHA256SUMS"
