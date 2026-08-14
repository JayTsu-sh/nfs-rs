#!/usr/bin/env bash
set -euo pipefail

run_id="${1:?run id required}"
sudo -n /usr/local/sbin/nfsrs-lab-v40-fault restore-any "$run_id"
status="$(sudo -n /usr/local/sbin/nfsrs-lab-v40-fault status "$run_id")"
[[ "$status" == restored ]] || {
  echo "NFSv4.0 fault cleanup assertion failed: $status" >&2
  exit 1
}
echo "NFSv4.0 fault state: restored"
