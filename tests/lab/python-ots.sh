#!/usr/bin/env bash
# Source common.sh and ots-executor.sh before loading this function.

run_python_lab_matrix() (
  local python="$1" run_id="$2" output="$3" wheel="$4" validator="$5"
  shift 5
  local lab_dir
  lab_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
  validate_run_id "$run_id"
  validate_ipv4 "$LAB_SOURCE_DATA"
  validate_ipv4 "$LAB_DXN_V40_DATA"
  validate_ipv4 "$LAB_OTS_PNFS_DATA"
  validate_export_path "$LAB_NFS3_EXPORT"
  validate_export_path "$LAB_NFS41_EXPORT"
  validate_export_path "$LAB_DXN_V40_EXPORT"
  validate_export_path "$LAB_OTS_PNFS_EXPORT"
  [[ -f "$wheel" ]] || { echo "lab wheel missing: $wheel" >&2; exit 2; }

  local workdir remote="" stage="" execute_status=0
  workdir="$(mktemp -d "${TMPDIR:-/tmp}/nfsrs-python-lab.XXXXXXXXXX")"
  cleanup_remote() {
    if [[ -n "$remote" ]]; then
      local path="$remote"
      remote=""
      ots_remove_workspace "$path"
    fi
  }
  trap 'cleanup_remote || :; rm -r -- "$workdir"' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM

  local direct_status=0 ots_status=0 cleanup_status=0
  "$python" "$validator" "$@" --run-id "$run_id" --output "$workdir/direct.json" \
    --case "linux-source-v3|3|nfs://$LAB_SOURCE_DATA$LAB_NFS3_EXPORT/ci/$run_id?version=3&noresvport=true|false" \
    --case "dxn-v40|4.0|nfs://$LAB_DXN_V40_DATA$LAB_DXN_V40_EXPORT?version=4.0&noresvport=true&uid=0&gid=0|false" \
    --case "linux-source-v41|4.1|nfs://$LAB_SOURCE_DATA$LAB_NFS41_EXPORT/ci/$run_id?version=4.1&noresvport=true|false" \
    || direct_status=$?

  run_ots_validator() {
    stage=ssh_config
    ots_configure_ssh || return
    stage=workspace
    remote="$(ots_create_workspace "nfsrs-python-$run_id")" || return
    stage=upload
    cp -- "$wheel" "$workdir/artifact.whl" || return
    cp -- "$validator" "$workdir/validate.py" || return
    tar -cf - -C "$workdir" artifact.whl validate.py | ots_ssh "tar -xf - -C '$remote'" || return

    local command
    printf -v command '%q ' "$remote" "$LAB_OTS_EXECUTOR_PYTHON" "$@" \
      --run-id "$run_id" --output results.json \
      --case "ots-pnfs-mds|4.1|nfs://$LAB_OTS_PNFS_DATA$LAB_OTS_PNFS_EXPORT?version=4.1&noresvport=true|true"
    stage=execute
    ots_ssh "bash -s -- $command" <<'REMOTE' || execute_status=$?
set -euo pipefail
cd -- "$1" || exit 78
executor_python="$2"
shift 2
"$executor_python" -m venv --without-pip venv || exit 78
./venv/bin/python - <<'PY' || exit 78
import sysconfig
import zipfile

with zipfile.ZipFile("artifact.whl") as wheel:
    wheel.extractall(sysconfig.get_paths()["purelib"])
PY
exec timeout --kill-after=10s 1800 ./venv/bin/python validate.py "$@"
REMOTE
    if ((execute_status == 78)); then
      stage=venv
      return "$execute_status"
    fi
    if ((execute_status == 255)); then
      return "$execute_status"
    fi

    stage=fetch
    local fetch_status=0
    ots_exec "cat '$remote/results.json'" > "$workdir/ots.json" || fetch_status=$?
    if ((fetch_status != 0)); then
      rm -f -- "$workdir/ots.json"
      return "$fetch_status"
    fi
    stage=validate
    return "$execute_status"
  }

  run_ots_validator "$@" || ots_status=$?
  cleanup_remote || cleanup_status=$?
  "$python" "$lab_dir/merge-python-lab-results.py" \
    "$run_id" "$workdir" "$output" "$direct_status" "$stage" "$ots_status" \
    "$execute_status" "$cleanup_status"
)
