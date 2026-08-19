#!/usr/bin/env bash
set -euo pipefail

LAB_SSH_USER="${LAB_SSH_USER:-ci-runner}"
LAB_SSH_KEY="${LAB_SSH_KEY:-/home/github-runner/.ssh/terrasync_lab}"
LAB_SOURCE_MGMT="${LAB_SOURCE_MGMT:-10.131.9.12}"
LAB_DEST_MGMT="${LAB_DEST_MGMT:-10.131.9.13}"
LAB_WORKER_MGMT="${LAB_WORKER_MGMT:-10.131.9.14}"
LAB_SOURCE_DATA="${LAB_SOURCE_DATA:-10.10.1.12}"
LAB_DEST_DATA="${LAB_DEST_DATA:-10.10.1.13}"
LAB_WORKER_DATA="${LAB_WORKER_DATA:-10.10.1.14}"
LAB_NFS3_EXPORT="${LAB_NFS3_EXPORT:-/srv/nfs/v3}"
LAB_NFS41_EXPORT="${LAB_NFS41_EXPORT:-/srv/nfs/v4}"
LAB_PNFS_MDS_DATA="${LAB_PNFS_MDS_DATA:-10.128.56.160}"
LAB_PNFS_DS_DATA="${LAB_PNFS_DS_DATA:-10.128.56.161}"
LAB_PNFS_PRIMARY_EXPORT="${LAB_PNFS_PRIMARY_EXPORT:-/nfsrs_pnfs_a}"
LAB_PNFS_SECONDARY_EXPORT="${LAB_PNFS_SECONDARY_EXPORT:-/nfsrs_pnfs_b}"
LAB_DXN_V40_DATA="${LAB_DXN_V40_DATA:-10.131.7.201}"
LAB_DXN_V40_EXPORT="${LAB_DXN_V40_EXPORT:-/jay_nfs}"

ssh_lab() {
  local host="$1"
  shift
  ssh -i "$LAB_SSH_KEY" \
    -o BatchMode=yes \
    -o ConnectTimeout=10 \
    -o StrictHostKeyChecking=accept-new \
    "$LAB_SSH_USER@$host" "$@"
}

validate_run_id() {
  local run_id="$1"
  [[ "$run_id" =~ ^(nightly|release)-[A-Za-z0-9._-]{1,80}$ ]] || {
    echo "unsafe run id: $run_id" >&2
    return 2
  }
}

validate_ipv4() {
  local address="$1"
  [[ "$address" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || {
    echo "unsafe IPv4 address: $address" >&2
    return 2
  }
  local octet
  IFS=. read -r -a octets <<<"$address"
  for octet in "${octets[@]}"; do
    ((10#$octet <= 255)) || {
      echo "unsafe IPv4 address: $address" >&2
      return 2
    }
  done
}

validate_export_path() {
  local path="$1"
  [[ "$path" =~ ^/[A-Za-z0-9._/-]{1,200}$ ]] && [[ "$path" != *".."* ]] || {
    echo "unsafe export path: $path" >&2
    return 2
  }
}
