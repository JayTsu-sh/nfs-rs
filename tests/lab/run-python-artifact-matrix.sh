#!/usr/bin/env bash
set -euo pipefail
lab_dir="$(cd "$(dirname "$0")" && pwd -P)"
source "$lab_dir/common.sh"
source "$lab_dir/ots-executor.sh"
source "$lab_dir/python-ots.sh"

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
# The wheel under test: the built release wheel, or the wheel rebuilt from
# the final source artifact by the calling workflow step.
if [[ "$artifact" == wheel ]]; then
  wheel_candidates=(target/python-release-artifacts/*_x86_64.whl)
else
  wheel_candidates=("$RUNNER_TEMP"/python-sdist-wheel/*.whl)
fi
[[ ${#wheel_candidates[@]} -eq 1 ]] || {
  echo "expected exactly one matrix wheel, found ${#wheel_candidates[@]}" >&2
  exit 2
}
wheel="${wheel_candidates[0]}"
[[ -f "$wheel" ]] || { echo "matrix wheel missing: $wheel" >&2; exit 2; }

baseline="tests/python/performance-baselines.json"
policy=$(python3 -c \
  'import json,sys; p=json.load(open(sys.argv[1], encoding="utf-8"))["policy"]; print(p["comparable_runs"], p["minimum_valid_runs"])' \
  "$baseline")
read -r comparable_runs minimum_valid_runs <<<"$policy"
run_options=(--runs "$comparable_runs" --minimum-valid-runs "$minimum_valid_runs")
if [[ "$mode" == smoke ]]; then
  run_options=(--runs 1 --minimum-valid-runs 1 --payload-mib 1)
fi

run_python_lab_matrix "$python" "$run_id" "$output" "$wheel" \
  "$lab_dir/../../scripts/validate-python-real-protocols.py" --artifact "$artifact" "${run_options[@]}"

if [[ "$mode" == gate ]]; then
  python3 scripts/check-python-performance.py \
    --baseline "$baseline" \
    --result "$output" \
    --output "${output%.json}-gate.json"
fi
