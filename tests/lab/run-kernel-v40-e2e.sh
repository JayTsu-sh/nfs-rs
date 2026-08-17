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

mount_a="$(mktemp -d /tmp/nfsrs-kernel-v40-a.XXXXXX)"
mount_b="$(mktemp -d /tmp/nfsrs-kernel-v40-b.XXXXXX)"
test_name="kernel-v40-${run_id}"
test_dir_a="$mount_a/$test_name"
test_dir_b="$mount_b/$test_name"
mounted_a=false
mounted_b=false
mount_helper="${NFS_RS_LAB_KERNEL_V40_MOUNT_HELPER:-/usr/local/sbin/nfsrs-lab-kernel-v40-mount}"

[[ -x "$mount_helper" ]] || {
  echo "missing privileged kernel NFSv4.0 mount helper: $mount_helper" >&2
  exit 1
}

cleanup() {
  status=$?
  trap - EXIT INT TERM
  set +e
  if [[ "$mounted_a" == true ]] && [[ -d "$test_dir_a" ]]; then
    sudo -n "$mount_helper" cleanup "$mount_a" "$test_name"
  fi
  if [[ "$mounted_b" == true ]] && [[ -d "$test_dir_b" ]]; then
    sudo -n "$mount_helper" cleanup "$mount_b" "$test_name"
  fi
  if [[ "$mounted_b" == true ]]; then
    sudo -n "$mount_helper" umount "$mount_b"
  fi
  if [[ "$mounted_a" == true ]]; then
    sudo -n "$mount_helper" umount "$mount_a"
  fi
  rmdir "$mount_a" "$mount_b" 2>/dev/null || true
  return "$status"
}
trap cleanup EXIT INT TERM

sudo -n "$mount_helper" mount-a "$mount_a" "$server_a" "$export_path"
mounted_a=true
sudo -n "$mount_helper" mount-b "$mount_b" "$server_b" "$export_path"
mounted_b=true

for mountpoint in "$mount_a" "$mount_b"; do
  [[ "$(findmnt -n -o FSTYPE --target "$mountpoint")" == "nfs4" ]]
  findmnt -n -o OPTIONS --target "$mountpoint" | tr ',' '\n' | grep -qx 'vers=4.0'
done

sudo -n "$mount_helper" prepare "$mount_a" "$test_name"
sudo -n "$mount_helper" prepare "$mount_b" "$test_name"
[[ -d "$test_dir_b" ]]

exercise_server() {
  local test_dir="$1"
  local expected_small expected_large

  printf 'nfs-rs kernel NFSv4.0\n' >"$test_dir/small.bin"
  dd if=/dev/urandom of="$test_dir/large.bin" bs=1M count=8 status=none
  sync "$test_dir/small.bin" "$test_dir/large.bin"
  expected_small="$(printf 'nfs-rs kernel NFSv4.0\n' | sha256sum | cut -d' ' -f1)"
  expected_large="$(sha256sum "$test_dir/large.bin" | cut -d' ' -f1)"
  [[ "$(sha256sum "$test_dir/small.bin" | cut -d' ' -f1)" == "$expected_small" ]]
  [[ "$(sha256sum "$test_dir/large.bin" | cut -d' ' -f1)" == "$expected_large" ]]

  chmod 0640 "$test_dir/small.bin"
  touch -m -d '@1700000000' "$test_dir/small.bin"
  [[ "$(stat -c '%a' "$test_dir/small.bin")" == "640" ]]
  [[ "$(stat -c '%Y' "$test_dir/small.bin")" == "1700000000" ]]

  mv "$test_dir/small.bin" "$test_dir/renamed.bin"
  ln "$test_dir/renamed.bin" "$test_dir/hardlink.bin"
  ln -s renamed.bin "$test_dir/symlink.bin"
  [[ "$(readlink "$test_dir/symlink.bin")" == "renamed.bin" ]]
  [[ "$(stat -c '%i' "$test_dir/renamed.bin")" == \
     "$(stat -c '%i' "$test_dir/hardlink.bin")" ]]

  export test_dir
  seq 1 16 | xargs -P 8 -I '{}' sh -c \
    'dd if=/dev/zero of="$test_dir/concurrent-{}.bin" bs=64K count=4 conv=fsync status=none'
  [[ "$(find "$test_dir" -maxdepth 1 -type f -name 'concurrent-*.bin' -size 256k | wc -l)" -eq 16 ]]

  : >"$test_dir/lock.bin"
  flock "$test_dir/lock.bin" sh -c \
    'if flock -n "$1" true; then echo "conflicting NFS lock unexpectedly succeeded" >&2; exit 1; fi' \
    sh "$test_dir/lock.bin"

  if command -v getfacl >/dev/null 2>&1; then
    getfacl -cp "$test_dir/renamed.bin" | grep -q '^user::rw-'
  fi
}

exercise_server "$test_dir_a"
exercise_server "$test_dir_b"

echo "kernel NFSv4.0 E2E passed for $server_a,$server_b:$export_path"
