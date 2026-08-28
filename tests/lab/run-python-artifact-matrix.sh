#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
artifact="${2:?wheel or sdist-wheel required}"
output="${3:?output path required}"
mode="${4:-gate}"
python="${PYTHON:-python3}"
validate_run_id "$run_id"
[[ "$artifact" == wheel || "$artifact" == sdist-wheel ]] || {
  echo "artifact must be wheel or sdist-wheel" >&2
  exit 2
}
[[ "$mode" == gate || "$mode" == smoke ]] || {
  echo "mode must be gate or smoke" >&2
  exit 2
}
validate_ipv4 "$LAB_SOURCE_DATA"
validate_ipv4 "$LAB_DXN_V40_DATA"
validate_ipv4 "$LAB_PNFS_MDS_DATA"
validate_export_path "$LAB_NFS3_EXPORT"
validate_export_path "$LAB_NFS41_EXPORT"
validate_export_path "$LAB_DXN_V40_EXPORT"
validate_export_path "$LAB_PNFS_SECONDARY_EXPORT"

baseline="tests/python/performance-baselines.json"
policy=$(python3 -c \
  'import json,sys; p=json.load(open(sys.argv[1], encoding="utf-8"))["policy"]; print(p["comparable_runs"], p["minimum_valid_runs"])' \
  "$baseline")
read -r comparable_runs minimum_valid_runs <<<"$policy"
run_options=(--runs "$comparable_runs" --minimum-valid-runs "$minimum_valid_runs")
if [[ "$mode" == smoke ]]; then
  run_options=(--runs 1 --minimum-valid-runs 1 --payload-mib 1)
fi

"$python" scripts/validate-python-real-protocols.py \
  "${run_options[@]}" \
  --run-id "$run_id" \
  --artifact "$artifact" \
  --output "$output" \
  --case "linux-source-v3|3|nfs://$LAB_SOURCE_DATA$LAB_NFS3_EXPORT/ci/$run_id?version=3&noresvport=true|false" \
  --case "dxn-v40|4.0|nfs://$LAB_DXN_V40_DATA$LAB_DXN_V40_EXPORT?version=4.0&noresvport=true&uid=0&gid=0|false" \
  --case "linux-source-v41|4.1|nfs://$LAB_SOURCE_DATA$LAB_NFS41_EXPORT/ci/$run_id?version=4.1&noresvport=true|false" \
  --case "netapp-pnfs-mds|4.1|nfs://$LAB_PNFS_MDS_DATA$LAB_PNFS_SECONDARY_EXPORT?version=4.1&noresvport=true|true"

if [[ "$mode" == gate ]]; then
  python3 scripts/check-python-performance.py \
    --baseline "$baseline" \
    --result "$output" \
    --output "${output%.json}-gate.json"
fi
