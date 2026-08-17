#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

run_id="${1:?usage: run-kernel-v40-e2e.sh RUN_ID}"
validate_run_id "$run_id"

lif_a="${NFS_RS_LAB_V40_LIF_A:-10.128.61.200}"
lif_b="${NFS_RS_LAB_V40_LIF_B:-10.128.61.201}"
export_path="${NFS_RS_LAB_V40_EXPORT:-/nfsrs_v40_test}"
validate_ipv4 "$lif_a"
validate_ipv4 "$lif_b"
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
  if [[ -d "$test_dir_a" ]]; then
    find "$test_dir_a" -mindepth 1 -delete
    rmdir "$test_dir_a"
  elif [[ -d "$test_dir_b" ]]; then
    find "$test_dir_b" -mindepth 1 -delete
    rmdir "$test_dir_b"
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

sudo -n "$mount_helper" mount-a "$mount_a" "$lif_a" "$export_path"
mounted_a=true
sudo -n "$mount_helper" mount-b "$mount_b" "$lif_b" "$export_path"
mounted_b=true

for mountpoint in "$mount_a" "$mount_b"; do
  [[ "$(findmnt -n -o FSTYPE --target "$mountpoint")" == "nfs4" ]]
  findmnt -n -o OPTIONS --target "$mountpoint" | tr ',' '\n' | grep -qx 'vers=4.0'
done

sudo -n "$mount_helper" prepare "$mount_a" "$test_name"
[[ -d "$test_dir_b" ]]

printf 'nfs-rs kernel NFSv4.0\n' >"$test_dir_a/small.bin"
dd if=/dev/urandom of="$test_dir_a/large.bin" bs=1M count=8 status=none
sync "$test_dir_a/small.bin" "$test_dir_a/large.bin"
[[ "$(sha256sum "$test_dir_a/small.bin" | cut -d' ' -f1)" == \
   "$(sha256sum "$test_dir_b/small.bin" | cut -d' ' -f1)" ]]
[[ "$(sha256sum "$test_dir_a/large.bin" | cut -d' ' -f1)" == \
   "$(sha256sum "$test_dir_b/large.bin" | cut -d' ' -f1)" ]]

chmod 0640 "$test_dir_a/small.bin"
touch -m -d '@1700000000' "$test_dir_a/small.bin"
[[ "$(stat -c '%a' "$test_dir_b/small.bin")" == "640" ]]
[[ "$(stat -c '%Y' "$test_dir_b/small.bin")" == "1700000000" ]]

mv "$test_dir_a/small.bin" "$test_dir_a/renamed.bin"
ln "$test_dir_a/renamed.bin" "$test_dir_a/hardlink.bin"
ln -s renamed.bin "$test_dir_a/symlink.bin"
[[ "$(readlink "$test_dir_b/symlink.bin")" == "renamed.bin" ]]
[[ "$(stat -c '%i' "$test_dir_b/renamed.bin")" == \
   "$(stat -c '%i' "$test_dir_b/hardlink.bin")" ]]

export test_dir_a
seq 1 16 | xargs -P 8 -I '{}' sh -c \
  'dd if=/dev/zero of="$test_dir_a/concurrent-{}.bin" bs=64K count=4 conv=fsync status=none'
[[ "$(find "$test_dir_b" -maxdepth 1 -type f -name 'concurrent-*.bin' -size 256k | wc -l)" -eq 16 ]]

lock_file="$test_dir_a/lock.bin"
: >"$lock_file"
flock "$lock_file" sh -c \
  'if flock -n "$1" true; then echo "conflicting NFS lock unexpectedly succeeded" >&2; exit 1; fi' \
  sh "$test_dir_b/lock.bin"

if command -v getfacl >/dev/null 2>&1; then
  getfacl -cp "$test_dir_b/renamed.bin" | grep -q '^user::rw-'
fi

echo "kernel NFSv4.0 E2E passed for $lif_a,$lif_b:$export_path"
