#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

run_id="${1:?run id required}"
output="${2:?output path required}"
python="${PYTHON:-python3}"
wheel="${NFS_RS_PYTHON_CONFORMANCE_WHEEL:?current-checkout conformance wheel path required}"
repo_root="$(cd "$(dirname "$0")/../.." && pwd -P)"
validate_run_id "$run_id"
validate_ipv4 "$LAB_SOURCE_DATA"
validate_ipv4 "$LAB_DXN_V40_DATA"
validate_ipv4 "$LAB_AFX_PNFS_DATA"
validate_export_path "$LAB_NFS3_EXPORT"
validate_export_path "$LAB_NFS41_EXPORT"
validate_export_path "$LAB_DXN_V40_EXPORT"
validate_export_path "$LAB_AFX_PNFS_EXPORT"
[[ -f "$wheel" ]] || { echo "conformance wheel missing: $wheel" >&2; exit 2; }

# The AFX svm2 data network (192.168.9.x) is unreachable from the runner; the
# netapp-pnfs-mds case therefore runs on the isolated AFX executor, which sits
# on that network. The remaining cases run directly here on the runner. The
# executor pattern (key, known hosts, workspace naming) matches
# run-afx-t15-e2e.sh.
host="${LAB_AFX_SSH_HOST:-10.128.56.17}"
user="${LAB_AFX_SSH_USER:-root}"
key="${LAB_AFX_SSH_KEY:-/home/github-runner/.ssh/nfsrs_afx_t15}"
known="${LAB_AFX_KNOWN_HOSTS:-/home/github-runner/.ssh/known_hosts_afx_t15}"
validate_ipv4 "$host"
[[ "$user" =~ ^[a-z_][a-z0-9_-]*$ ]] || exit 2
ssh_options=(-T -i "$key" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=10
             -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$known")
ssh_afx() { ssh "${ssh_options[@]}" "$user@$host" "$@"; }

workdir="$(mktemp -d "${TMPDIR:-/tmp}/nfsrs-pyconform.XXXXXX")"
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
cp -f -- "$repo_root/scripts/validate-python-real-api.py" "$workdir/"

direct_args=()
for case_spec in \
  "linux-source-v3|3|nfs://$LAB_SOURCE_DATA$LAB_NFS3_EXPORT/ci/$run_id?version=3&noresvport=true|false" \
  "dxn-v40|4.0|nfs://$LAB_DXN_V40_DATA$LAB_DXN_V40_EXPORT?version=4.0&noresvport=true&uid=0&gid=0|false" \
  "linux-source-v41|4.1|nfs://$LAB_SOURCE_DATA$LAB_NFS41_EXPORT/ci/$run_id?version=4.1&noresvport=true|false"; do
  direct_args+=(--case "$case_spec")
done

direct_rc=0
"$python" "$repo_root/scripts/validate-python-real-api.py" \
  --run-id "$run_id" \
  --output "$workdir/direct.json" \
  "${direct_args[@]}" || direct_rc=$?

remote="$(ssh_afx "umask 077; mktemp -d /tmp/nfsrs-pyconform-$run_id.XXXXXX")"
[[ "$remote" == "/tmp/nfsrs-pyconform-$run_id."* ]] || {
  echo "invalid executor workspace: $remote" >&2; exit 1;
}
suffix="${remote#/tmp/nfsrs-pyconform-$run_id.}"
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
timeout 1800 ./venv/bin/python validate-python-real-api.py \
  --run-id '$run_id' \
  --output results.json \
  --case "netapp-pnfs-mds|4.1|nfs://$LAB_AFX_PNFS_DATA$LAB_AFX_PNFS_EXPORT?version=4.1&noresvport=true|true"
REMOTE
ssh_afx "cat '$remote/results.json'" >"$workdir/afx.json" || {
  printf '{"results": [], "failures": ["netapp-pnfs-mds: TransportError: no executor result"], "run_id": "%s"}\n' \
    "$run_id" >"$workdir/afx.json"
}

"$python" - "$workdir/direct.json" "$workdir/afx.json" "$output" <<'PY'
import json, sys
from pathlib import Path

direct = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
try:
    afx = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
except (OSError, ValueError):
    afx = {"run_id": direct["run_id"], "results": [],
           "failures": ["netapp-pnfs-mds: TransportError: unreadable executor result"]}
if afx.get("run_id") != direct["run_id"]:
    raise SystemExit("remote AFX conformance result has a different run id")
failures = list(direct["failures"]) + list(afx.get("failures", []))
merged = dict(direct)
merged["results"] = list(direct["results"]) + list(afx.get("results", []))
merged["failures"] = failures
merged["status"] = "failed" if failures else "passed"
out = Path(sys.argv[3])
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(merged, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

if [[ "$direct_rc" -ne 0 || "$afx_rc" -ne 0 ]]; then
  "$python" -c 'import json, sys; sys.exit(0 if json.load(open(sys.argv[1]))["status"] == "passed" else 1)' \
    "$output"
fi
