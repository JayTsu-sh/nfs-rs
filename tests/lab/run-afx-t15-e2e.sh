#!/usr/bin/env bash
# Controller transport only. Privileged faults run on the isolated AFX executor.
set -euo pipefail
umask 077
source "$(dirname "$0")/common.sh"
action="${1:?run or cleanup required}"
run_id="${2:?run id required}"
validate_run_id "$run_id"
[[ "$action" == run || "$action" == cleanup ]] || exit 2
: "${NFS_RS_AFX_T15_EVIDENCE_DIR:?private evidence directory outside checkout required}"
evidence="$(realpath -m -- "$NFS_RS_AFX_T15_EVIDENCE_DIR")"
repo_root="$(cd "$(dirname "$0")/../.." && pwd -P)"
if [[ "$evidence" == "$repo_root" || "$evidence" == "$repo_root/"* ]]; then
  echo 'raw AFX evidence must be outside the checkout' >&2; exit 2
fi
host="${LAB_AFX_SSH_HOST:-10.128.56.17}"
user="${LAB_AFX_SSH_USER:-root}"
key="${LAB_AFX_SSH_KEY:-/home/github-runner/.ssh/nfsrs_afx_t15}"
known="${LAB_AFX_KNOWN_HOSTS:-/home/github-runner/.ssh/known_hosts_afx_t15}"
validate_ipv4 "$host"
[[ "$user" =~ ^[a-z_][a-z0-9_-]*$ ]] || exit 2
ssh_options=(-T -i "$key" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=10
             -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$known")
ssh_afx() { ssh "${ssh_options[@]}" "$user@$host" "$@"; }
remote=""
load_remote() {
  [[ -e "$evidence/remote-workspace" ]] || return 1
  read -r remote <"$evidence/remote-workspace"
  [[ "$remote" == "/tmp/nfsrs-afx-t15-$run_id."* ]] || return 2
  local suffix="${remote#"/tmp/nfsrs-afx-t15-$run_id."}"
  [[ "$suffix" =~ ^[A-Za-z0-9]+$ ]] || {
    echo 'invalid recorded executor workspace' >&2; return 2;
  }
}
cleanup_remote() {
  [[ -e "$evidence/remote-cleaned" ]] && return 0
  if [[ ! -e "$evidence/remote-workspace" ]]; then return 0; fi
  load_remote || { remote=""; return 2; }
  # No state means the uploaded controller never started: no fixture is owned.
  local state_status=0
  ssh_afx "test -f '$remote/state.json'" || state_status=$?
  if [[ "$state_status" == 0 ]]; then
    ssh_afx "python3 '$remote/tools/afx_t15.py' cleanup --workspace '$remote' --run-id '$run_id'"
  elif [[ "$state_status" != 1 ]]; then
    return "$state_status"
  fi
}
if [[ "$action" == cleanup ]]; then
  cleanup_remote
  exit
fi
: "${NFS_RS_AFX_T15_BINARY:?current-checkout static musl probe required}"
: "${NFS_RS_AFX_T15_COMMIT:?exact source SHA required}"
[[ "$NFS_RS_AFX_T15_COMMIT" =~ ^[0-9a-f]{40}$ ]] || exit 2
[[ -x "$NFS_RS_AFX_T15_BINARY" ]] || exit 2
mkdir -m 0700 "$evidence"
bundle="$(mktemp -d "$evidence/bundle.XXXXXX")"
finish() {
  local rc=$?
  trap - EXIT INT TERM
  local cleanup_ok=1
  cleanup_remote || { rc=1; cleanup_ok=0; }
  if [[ -n "$remote" ]]; then
    # Preserve failure evidence too. Only public/result.json is uploaded by CI;
    # packet bodies and file handles remain in this private archive.
    if ssh_afx "tar -czf - -C '$remote' ." >"$evidence/executor-private.tar.gz"; then
      mkdir -p "$evidence/public"
      if ssh_afx "tar -cf - -C '$remote/public' ." | tar -xf - -C "$evidence/public"; then
        # Keep the workspace if the cleanup report does not confirm restoration.
        if [[ "$cleanup_ok" == 1 ]] && python3 -c 'import json,sys; sys.exit(json.load(open(sys.argv[1]))["cleanup"] != "pass")' "$evidence/public/result.json"; then
          ssh_afx "rm -rf -- '$remote'" && touch "$evidence/remote-cleaned" || rc=1
        else
          rc=1
        fi
      else
        rc=1
      fi
    else
      rc=1
    fi
  fi
  if [[ "$cleanup_ok" != 1 ]]; then
    mkdir -p "$evidence/public"
    python3 - "$evidence/public/result.json" <<'PYTHON'
import json,sys
from pathlib import Path
p=Path(sys.argv[1]);value=json.loads(p.read_text()) if p.exists() else {}
value.update(status="failed",transport_cleanup="unconfirmed")
p.write_text(json.dumps(value,indent=2)+"\n")
PYTHON
  fi
  if [[ "$rc" == 0 ]]; then
    local probe_sha
    probe_sha="$(sha256sum "$NFS_RS_AFX_T15_BINARY")"
    if ! python3 "$(dirname "$0")/afx_t15_wire.py" "$evidence/public/result.json" \
      "$NFS_RS_AFX_T15_COMMIT" "${probe_sha%% *}"; then
      rc=1
      python3 - "$evidence/public/result.json" <<'PYTHON'
import json,sys
from pathlib import Path
p=Path(sys.argv[1]);value=json.loads(p.read_text())
value.update(status="failed",transport_validation="failed")
p.write_text(json.dumps(value,indent=2)+"\n")
PYTHON
    fi
  fi
  rm -rf -- "$bundle"
  exit "$rc"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
remote="$(ssh_afx "umask 077; mktemp -d /tmp/nfsrs-afx-t15-$run_id.XXXXXX")"
printf '%s\n' "$remote" >"$evidence/remote-workspace"
load_remote
mkdir "$bundle/tools"
for tool in common.sh prepare-run.sh cleanup-run.sh afx-run-dir.sh afx_t15.py afx_t15_wire.py; do
  install -m 0700 "$(dirname "$0")/$tool" "$bundle/tools/$tool"
done
install -m 0700 "$NFS_RS_AFX_T15_BINARY" "$bundle/pnfs_partial_failure"
tar -cf - -C "$bundle" . | ssh_afx "tar -xf - -C '$remote'"
ssh_afx "timeout --signal=TERM --kill-after=60 1500 python3 '$remote/tools/afx_t15.py' run --workspace '$remote' --run-id '$run_id' --commit '$NFS_RS_AFX_T15_COMMIT'"
