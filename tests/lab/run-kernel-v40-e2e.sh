#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

run_id="${1:?usage: run-kernel-v40-e2e.sh RUN_ID}"
validate_run_id "$run_id"

server_a="${NFS_RS_LAB_KERNEL_V40_SERVER_A:-$LAB_SOURCE_DATA}"
server_b="${NFS_RS_LAB_KERNEL_V40_SERVER_B:-$LAB_DEST_DATA}"
export_path="${NFS_RS_LAB_KERNEL_V40_EXPORT:-$LAB_NFS41_EXPORT}"
validate_ipv4 "$server_a"
validate_ipv4 "$server_b"
validate_export_path "$export_path"

source_mount="$(mktemp -d /tmp/nfsrs-kernel-v40-s1.XXXXXX)"
source_peer_mount="$(mktemp -d /tmp/nfsrs-kernel-v40-s2.XXXXXX)"
dest_mount="$(mktemp -d /tmp/nfsrs-kernel-v40-d1.XXXXXX)"
dest_peer_mount="$(mktemp -d /tmp/nfsrs-kernel-v40-d2.XXXXXX)"
local_oracle="$(mktemp -d /tmp/nfsrs-kernel-v40-oracle.XXXXXX)"
test_name="kernel-v40-${run_id}"
mount_helper="${NFS_RS_LAB_KERNEL_V40_MOUNT_HELPER:-/usr/local/sbin/nfsrs-lab-kernel-v40-mount}"
mounted_source=false
mounted_source_peer=false
mounted_dest=false
mounted_dest_peer=false

[[ -x "$mount_helper" ]] || {
  echo "missing privileged kernel NFSv4.0 mount helper: $mount_helper" >&2
  exit 1
}

cleanup() {
  status=$?
  trap - EXIT INT TERM
  set +e
  if [[ "$mounted_source" == true ]] && [[ -d "$source_mount/$test_name" ]]; then
    sudo -n "$mount_helper" cleanup "$source_mount" "$test_name"
  fi
  if [[ "$mounted_dest" == true ]] && [[ -d "$dest_mount/$test_name" ]]; then
    sudo -n "$mount_helper" cleanup "$dest_mount" "$test_name"
  fi
  for entry in \
    "$mounted_dest_peer:$dest_peer_mount" \
    "$mounted_dest:$dest_mount" \
    "$mounted_source_peer:$source_peer_mount" \
    "$mounted_source:$source_mount"; do
    state="${entry%%:*}"
    mountpoint="${entry#*:}"
    if [[ "$state" == true ]]; then
      sudo -n "$mount_helper" umount "$mountpoint"
    fi
  done
  rmdir "$source_mount" "$source_peer_mount" "$dest_mount" "$dest_peer_mount" 2>/dev/null || true
  find "$local_oracle" -mindepth 1 -delete
  rmdir "$local_oracle" 2>/dev/null || true
  return "$status"
}
trap cleanup EXIT INT TERM

sudo -n "$mount_helper" mount-source "$source_mount" "$server_a" "$export_path"
mounted_source=true
sudo -n "$mount_helper" mount-source "$source_peer_mount" "$server_a" "$export_path"
mounted_source_peer=true
sudo -n "$mount_helper" mount-dest "$dest_mount" "$server_b" "$export_path"
mounted_dest=true
sudo -n "$mount_helper" mount-dest "$dest_peer_mount" "$server_b" "$export_path"
mounted_dest_peer=true

for mountpoint in "$source_mount" "$source_peer_mount" "$dest_mount" "$dest_peer_mount"; do
  [[ "$(findmnt -n -o FSTYPE --target "$mountpoint")" == "nfs4" ]]
  findmnt -n -o OPTIONS --target "$mountpoint" | tr ',' '\n' | grep -qx 'vers=4.0'
done

sudo -n "$mount_helper" prepare "$source_mount" "$test_name"
sudo -n "$mount_helper" prepare "$dest_mount" "$test_name"

exercise_server() {
  local primary_mount="$1"
  local peer_mount="$2"
  local label="$3"
  local test_dir="$primary_mount/$test_name"
  local peer_dir="$peer_mount/$test_name"
  local oracle_dir="$local_oracle/$label"
  local manifest="$oracle_dir/concurrent-manifest.sha256"

  mkdir "$oracle_dir"
  printf 'nfs-rs kernel NFSv4.0 %s\n' "$label" >"$oracle_dir/small.bin"
  dd if=/dev/urandom of="$oracle_dir/large.bin" bs=1M count=8 status=none
  cp "$oracle_dir/small.bin" "$test_dir/small.bin"
  cp "$oracle_dir/large.bin" "$test_dir/large.bin"
  sync "$test_dir/small.bin" "$test_dir/large.bin"
  (
    cd "$oracle_dir"
    sha256sum small.bin large.bin >payload-manifest.sha256
  )
  (
    cd "$peer_dir"
    sha256sum --check "$oracle_dir/payload-manifest.sha256"
  )

  chmod 0640 "$test_dir/small.bin"
  touch -m -d '@1700000000' "$test_dir/small.bin"
  [[ "$(stat -c '%a' "$peer_dir/small.bin")" == "640" ]]
  [[ "$(stat -c '%Y' "$peer_dir/small.bin")" == "1700000000" ]]

  mv "$test_dir/small.bin" "$test_dir/renamed.bin"
  ln "$test_dir/renamed.bin" "$test_dir/hardlink.bin"
  ln -s renamed.bin "$test_dir/symlink.bin"
  [[ "$(readlink "$peer_dir/symlink.bin")" == "renamed.bin" ]]
  [[ "$(stat -c '%i' "$peer_dir/renamed.bin")" == \
     "$(stat -c '%i' "$peer_dir/hardlink.bin")" ]]

  : >"$manifest"
  for index in $(seq -w 1 16); do
    dd if=/dev/urandom of="$oracle_dir/concurrent-$index.bin" bs=64K count=4 status=none
    (
      cd "$oracle_dir"
      sha256sum "concurrent-$index.bin" >>"$manifest"
    )
  done
  export oracle_dir test_dir
  seq -w 1 16 | xargs -P 8 -I '{}' sh -c \
    'cp "$oracle_dir/concurrent-{}.bin" "$test_dir/concurrent-{}.bin" && sync "$test_dir/concurrent-{}.bin"'
  [[ "$(find "$peer_dir" -maxdepth 1 -type f -name 'concurrent-*.bin' -size 256k | wc -l)" -eq 16 ]]
  (
    cd "$peer_dir"
    sha256sum --check "$manifest"
  )

  : >"$test_dir/lock.bin"
  flock "$test_dir/lock.bin" sh -c \
    'if flock -n "$1" true; then echo "cross-mount NFS lock unexpectedly succeeded" >&2; exit 1; fi' \
    sh "$peer_dir/lock.bin"

  if command -v getfacl >/dev/null 2>&1; then
    getfacl -cp "$peer_dir/renamed.bin" | grep -q '^user::rw-'
  fi
}

exercise_server "$source_mount" "$source_peer_mount" source
exercise_server "$dest_mount" "$dest_peer_mount" destination

echo "kernel NFSv4.0 release-grade E2E passed for $server_a,$server_b:$export_path"
