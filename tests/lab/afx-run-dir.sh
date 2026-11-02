#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
action="${1:?prepare or cleanup required}"
run_id="${2:?run id required}"
validate_run_id "$run_id"
[[ "$action" == prepare || "$action" == cleanup ]] || exit 2
address="${LAB_AFX_DATA:-192.168.9.101}"
export_path="${LAB_AFX_EXPORT:-/pnfs_test2}"
validate_ipv4 "$address"
validate_export_path "$export_path"
mount_dir="$(mktemp -d "/tmp/nfsrs-afx-$run_id.XXXXXX")"
finish() {
  if mountpoint -q "$mount_dir"; then umount "$mount_dir"; fi
  rmdir "$mount_dir"
}
trap finish EXIT
mount -t nfs -o vers=4.2,proto=tcp,noresvport,soft,timeo=30,retrans=2 \
  "$address:$export_path" "$mount_dir"
if [[ "$action" == prepare ]]; then
  # Fail on collisions; never adopt another run's directory.
  mkdir -m 0777 -- "$mount_dir/$run_id"
else
  rm -rf -- "$mount_dir/$run_id"
  [[ ! -e "$mount_dir/$run_id" ]]
fi
