#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
artifact="${2:?wheel or sdist-wheel required}"
output="${3:?output path required}"
mode="${4:-gate}"
python="${PYTHON:-python3}"
wheel="${NFS_RS_ARTIFACT_MATRIX_WHEEL:?release artifact wheel path required}"
repo_root="$(cd "$(dirname "$0")/../.." && pwd -P)"
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
validate_ipv4 "$LAB_AFX_PNFS_DATA"
validate_export_path "$LAB_NFS3_EXPORT"
validate_export_path "$LAB_NFS41_EXPORT"
validate_export_path "$LAB_DXN_V40_EXPORT"
validate_export_path "$LAB_AFX_PNFS_EXPORT"
[[ -f "$wheel" ]] || { echo "artifact wheel missing: $wheel" >&2; exit 2; }

# The AFX svm2 data network (192.168.9.x) is unreachable from the runner, so
# the netapp-pnfs-mds case runs on the isolated AFX executor, which sits on
# that network. The remaining cases run directly here on the runner with the
# artifact wheel already installed in $python. The executor pattern (key,
# known hosts, workspace naming) matches run-afx-t15-e2e.sh and
# run-python-api-conformance.sh.
host="${LAB_AFX_SSH_HOST:-10.128.56.17}"
user="${LAB_AFX_SSH_USER:-root}"
key="${LAB_AFX_SSH_KEY:-/home/github-runner/.ssh/nfsrs_afx_t15}"
known="${LAB_AFX_KNOWN_HOSTS:-/home/github-runner/.ssh/known_hosts_afx_t15}"
validate_ipv4 "$host"
[[ "$user" =~ ^[a-z_][a-z0-9_-]*$ ]] || exit 2
ssh_options=(-T -i "$key" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=10
             -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$known")
ssh_afx() { ssh "${ssh_options[@]}" "$user@$host" "$@"; }

baseline="tests/python/performance-baselines.json"
policy=$(python3 -c \
  'import json,sys; p=json.load(open(sys.argv[1], encoding="utf-8"))["policy"]; print(p["comparable_runs"], p["minimum_valid_runs"])' \
  "$baseline")
read -r comparable_runs minimum_valid_runs <<<"$policy"
run_options=(--runs "$comparable_runs" --minimum-valid-runs "$minimum_valid_runs")
if [[ "$mode" == smoke ]]; then
  run_options=(--runs 1 --minimum-valid-runs 1 --payload-mib 1)
fi

workdir="$(mktemp -d "${TMPDIR:-/tmp}/nfsrs-pymatrix.XXXXXX")"
remote=""
cleanup() {
  rc=$?
  trap - EXIT INT TERM
  if [[ -n "$remote" ]]; then
    ssh_afx "rm -rf -- '$remote'" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$workdir"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
chmod 700 "$workdir"
cp -f -- "$wheel" "$workdir/"
cp -f -- "$repo_root/scripts/validate-python-real-protocols.py" "$workdir/"

direct_rc=0
"$python" "$repo_root/scripts/validate-python-real-protocols.py" \
  "${run_options[@]}" \
  --run-id "$run_id" \
  --artifact "$artifact" \
  --output "$workdir/direct.json" \
  --case "linux-source-v3|3|nfs://$LAB_SOURCE_DATA$LAB_NFS3_EXPORT/ci/$run_id?version=3&noresvport=true|false" \
  --case "dxn-v40|4.0|nfs://$LAB_DXN_V40_DATA$LAB_DXN_V40_EXPORT?version=4.0&noresvport=true&uid=0&gid=0|false" \
  --case "linux-source-v41|4.1|nfs://$LAB_SOURCE_DATA$LAB_NFS41_EXPORT/ci/$run_id?version=4.1&noresvport=true|false" || direct_rc=$?

remote="$(ssh_afx "umask 077; mktemp -d /tmp/nfsrs-pymatrix-$run_id.XXXXXX")"
[[ "$remote" == "/tmp/nfsrs-pymatrix-$run_id."* ]] || {
  echo "invalid executor workspace: $remote" >&2; exit 1;
}
suffix="${remote#/tmp/nfsrs-pymatrix-$run_id.}"
[[ "$suffix" =~ ^[A-Za-z0-9]+$ ]] || {
  echo "invalid executor workspace: $remote" >&2; exit 1;
}
tar -cf - -C "$workdir" . | ssh_afx "tar -xf - -C '$remote'"

afx_rc=0
ssh_afx "bash -s" <<REMOTE || afx_rc=$?
set -euo pipefail
cd '$remote'
command -v /usr/bin/python3.11 >/dev/null || {
  echo 'AFX executor is missing /usr/bin/python3.11' >&2; exit 1;
}
/usr/bin/python3.11 -m venv --without-pip venv
./venv/bin/python - <<'PY'
import os, sysconfig, zipfile
wheel = next(n for n in os.listdir(".") if n.endswith(".whl"))
zipfile.ZipFile(wheel).extractall(sysconfig.get_paths()["purelib"])
PY
timeout 3600 ./venv/bin/python validate-python-real-protocols.py \
  ${run_options[@]} \
  --run-id '$run_id' \
  --artifact '$artifact' \
  --output results.json \
  --case "netapp-pnfs-mds|4.1|nfs://$LAB_AFX_PNFS_DATA$LAB_AFX_PNFS_EXPORT?version=4.1&noresvport=true|true"
REMOTE
ssh_afx "cat '$remote/results.json' ">"$workdir/afx.json" || {
  : >"$workdir/afx.json"
}

"$python" - "$workdir/direct.json" "$workdir/afx.json" "$output" <<'PY'
import json, sys
from pathlib import Path

def load(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None

direct = load(sys.argv[1])
afx = load(sys.argv[2])
if direct is None and afx is None:
    raise SystemExit("no real-protocol result available from either leg")
base = direct if direct is not None else afx
merged = {
    "schema_version": 1,
    "run_id": base["run_id"],
    "nfs_rs_version": base["nfs_rs_version"],
    "results": list((direct or {}).get("results", [])) + list((afx or {}).get("results", [])),
}
out = Path(sys.argv[3])
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(merged, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

gate_rc=0
if [[ "$mode" == gate ]]; then
  python3 "$repo_root/scripts/check-python-performance.py" \
    --baseline "$baseline" \
    --result "$output" \
    --output "${output%.json}-gate.json" || gate_rc=$?
fi

if [[ "$direct_rc" -ne 0 || "$afx_rc" -ne 0 || "$gate_rc" -ne 0 ]]; then
  exit 1
fi
