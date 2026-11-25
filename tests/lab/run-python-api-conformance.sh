#!/usr/bin/env bash
set -euo pipefail
lab_dir="$(cd "$(dirname "$0")" && pwd -P)"
source "$lab_dir/common.sh"
source "$lab_dir/ots-executor.sh"
source "$lab_dir/python-ots.sh"

run_id="${1:?run id required}"
output="${2:?output path required}"
python="${PYTHON:-python3}"
wheel="${NFS_RS_PYTHON_CONFORMANCE_WHEEL:?current-checkout conformance wheel path required}"

run_python_lab_matrix "$python" "$run_id" "$output" "$wheel" \
  "$lab_dir/../../scripts/validate-python-real-api.py"
