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
validate_ipv4 "$LAB_OTS_PNFS_DATA" "$LAB_OTS_JUMP" "$LAB_OTS_NODE"
validate_export_path "$LAB_NFS3_EXPORT"
validate_export_path "$LAB_NFS41_EXPORT"
validate_export_path "$LAB_DXN_V40_EXPORT"
validate_export_path "$LAB_OTS_PNFS_EXPORT"
[[ -f "$wheel" ]] || { echo "conformance wheel missing: $wheel" >&2; exit 2; }

# The OTS data network (10.30.3.x) is unreachable from the runner; the
# ots-pnfs-mds case therefore runs on the DXN executor node (10.128.135.141,
# data IP 10.30.3.141) reached through the management jump
# (10.128.134.90); both hops authenticate with the same key. The remaining
# cases run directly here on the runner.
user="$LAB_OTS_SSH_USER"
jump="$LAB_OTS_JUMP"
node="$LAB_OTS_NODE"
key="${LAB_OTS_SSH_KEY:?OTS executor key required (provisioned by the workflow)}"
known="${LAB_OTS_KNOWN_HOSTS:?OTS executor known_hosts required (provisioned by the workflow)}"
[[ -f "$key" && -f "$known" ]] || { echo "OTS executor credentials missing" >&2; exit 2; }
[[ "$user" =~ ^[a-z_][a-z0-9_-]*$ ]] || exit 2
hop_options=(-T -i "$key" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=15
             -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$known")
ssh_ots() {
  ssh "${hop_options[@]}" \
    -o "ProxyCommand=ssh -i $key -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=yes -o UserKnownHostsFile=$known -W %h:%p $user@$jump" \
    "$user@$node" "$@"
}

workdir="$(mktemp -d "${TMPDIR:-/tmp}/nfsrs-pyconform.XXXXXX")"
remote=""
cleanup() {
  rc=$?
  trap - EXIT INT TERM
  if [[ -n "$remote" ]]; then
    ssh_ots "rm -rf -- '$remote'" >/dev/null 2>&1 || true
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

remote="$(ssh_ots "umask 077; mktemp -d /tmp/nfsrs-pyconform-$run_id.XXXXXX")"
[[ "$remote" == "/tmp/nfsrs-pyconform-$run_id."* ]] || {
  echo "invalid executor workspace: $remote" >&2; exit 1;
}
suffix="${remote#/tmp/nfsrs-pyconform-$run_id.}"
[[ "$suffix" =~ ^[A-Za-z0-9]+$ ]] || {
  echo "invalid executor workspace: $remote" >&2; exit 1;
}
tar -cf - -C "$workdir" . | ssh_ots "tar -xf - -C '$remote'"

ots_rc=0
ssh_ots "bash -s" <<REMOTE || ots_rc=$?
set -euo pipefail
cd '$remote'
command -v '$LAB_OTS_EXECUTOR_PYTHON' >/dev/null || {
  echo 'OTS executor is missing $LAB_OTS_EXECUTOR_PYTHON' >&2; exit 1;
}
'$LAB_OTS_EXECUTOR_PYTHON' -m venv --without-pip venv
./venv/bin/python - <<'PY'
import os, sysconfig, zipfile
wheel = next(n for n in os.listdir(".") if n.endswith(".whl"))
zipfile.ZipFile(wheel).extractall(sysconfig.get_paths()["purelib"])
PY
timeout 1800 ./venv/bin/python validate-python-real-api.py \
  --run-id '$run_id' \
  --output results.json \
  --case "ots-pnfs-mds|4.1|nfs://$LAB_OTS_PNFS_DATA$LAB_OTS_PNFS_EXPORT?version=4.1&noresvport=true|true"
REMOTE
ssh_ots "cat '$remote/results.json'" > "$workdir/ots.json" || {
  printf '{"results": [], "failures": ["ots-pnfs-mds: TransportError: no executor result"], "run_id": "%s"}\n' \
    "$run_id" >"$workdir/ots.json"
}

"$python" - "$workdir/direct.json" "$workdir/ots.json" "$output" <<'PY'
import json, sys
from pathlib import Path

direct = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
try:
    ots = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
except (OSError, ValueError):
    ots = {"run_id": direct["run_id"], "results": [],
           "failures": ["ots-pnfs-mds: TransportError: unreadable executor result"]}
if ots.get("run_id") != direct["run_id"]:
    raise SystemExit("remote OTS conformance result has a different run id")
failures = list(direct["failures"]) + list(ots.get("failures", []))
merged = dict(direct)
merged["results"] = list(direct["results"]) + list(ots.get("results", []))
merged["failures"] = failures
merged["status"] = "failed" if failures else "passed"
out = Path(sys.argv[3])
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(merged, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

if [[ "$direct_rc" -ne 0 || "$ots_rc" -ne 0 ]]; then
  "$python" -c 'import json, sys; sys.exit(0 if json.load(open(sys.argv[1]))["status"] == "passed" else 1)' \
    "$output"
fi