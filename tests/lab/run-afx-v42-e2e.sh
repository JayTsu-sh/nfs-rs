#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
run_id="${1:?unique release-* or nightly-* run id required}"
validate_run_id "$run_id"
: "${NFS_RS_LAB_TEST_BINARY:?path to the current-checkout lab_e2e binary required}"
if [[ -n "${NFS_RS_LAB_PYTHON:-}" ]]; then
  "$NFS_RS_LAB_PYTHON" - <<'PYTHON'
import nfs_rs._internal as native
if hasattr(native, "_arm_open_test_barrier"):
    raise SystemExit("physical validation requires a wheel built without python-test-support")
PYTHON
fi
export LAB_PROFILE=afx
export LAB_AFX_DATA="${LAB_AFX_DATA:-192.168.9.101}"
export LAB_AFX_EXPORT="${LAB_AFX_EXPORT:-/pnfs_test2}"
validate_ipv4 "$LAB_AFX_DATA"
validate_export_path "$LAB_AFX_EXPORT"
# Run on the AFX data-network executor. Share the established AFX lab lock.
exec 9>/tmp/nfsrs-svm2-issue40.lock
flock -w 300 9
prepared=0
cleanup() {
  if [[ "$prepared" == 1 ]]; then "$(dirname "$0")/cleanup-run.sh" "$run_id"; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"$(dirname "$0")/prepare-run.sh" "$run_id"
prepared=1
export NFS_RS_LAB_E2E=1
export NFS_RS_LAB_V42_URLS=""
for address in ${LAB_AFX_LIFS:-192.168.9.101 192.168.9.102 192.168.9.103 192.168.9.104}; do
  validate_ipv4 "$address"
  NFS_RS_LAB_V42_URLS+="nfs://$address$LAB_AFX_EXPORT/$run_id?version=4.2&noresvport=true&uid=0&gid=0 "
done
timeout 600 "$NFS_RS_LAB_TEST_BINARY" nfs_v42_end_to_end --exact --ignored --nocapture

evidence_dir="${NFS_RS_LAB_EVIDENCE_DIR:-/tmp/nfsrs-$run_id}"
mkdir -p "$evidence_dir"
export NFS_RS_LAB_V42_OPS_EVIDENCE="$evidence_dir/rust-v42-operations.json"
timeout 600 "$NFS_RS_LAB_TEST_BINARY" nfs_v42_optional_operations --exact --ignored --nocapture

if [[ -n "${NFS_RS_LAB_PYTHON:-}" ]]; then
  evidence_dir="${NFS_RS_LAB_EVIDENCE_DIR:?evidence directory outside the checkout required}"
  mkdir -p "$evidence_dir"
  api_cases=()
  for address in ${LAB_AFX_LIFS:-192.168.9.101 192.168.9.102 192.168.9.103 192.168.9.104}; do
    api_cases+=(--case "afx-${address##*.}-v42|4.2|nfs://$address$LAB_AFX_EXPORT/$run_id?version=4.2&noresvport=true&uid=0&gid=0|true")
  done
  timeout 600 "$NFS_RS_LAB_PYTHON" "$(dirname "$0")/../../scripts/validate-python-real-api.py" \
    "${api_cases[@]}" --run-id "$run_id" --output "$evidence_dir/python-api.json"
  timeout 600 "$NFS_RS_LAB_PYTHON" "$(dirname "$0")/../../scripts/validate-python-real-protocols.py" \
    "${api_cases[@]}" --run-id "$run_id" --artifact wheel \
    --output "$evidence_dir/python-protocols.json"
else
  echo 'SKIP: Python AFX checks require NFS_RS_LAB_PYTHON pointing to an installed current-checkout wheel' >&2
fi
