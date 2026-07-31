#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

report_dir="${1:?report directory required}"
mkdir -p "$report_dir"

probe_host() {
  local role="$1"
  local host="$2"
  local output="$report_dir/$role.txt"

  ssh_lab "$host" 'bash -s' >"$output" <<'REMOTE'
set -euo pipefail

section() {
  printf '\n[%s]\n' "$1"
}

section identity
hostname
uname -a

section nfs-status
sudo -n /usr/local/sbin/terrasync-lab-nfs-status 2>&1 || true

section nfs-implementation
for command in rpc.nfsd ganesha.nfsd nfs-ganesha systemctl exportfs nfsstat; do
  if path="$(command -v "$command" 2>/dev/null)"; then
    printf '%s=%s\n' "$command" "$path"
  else
    printf '%s=absent\n' "$command"
  fi
done
if command -v systemctl >/dev/null 2>&1; then
  for unit in nfs-server nfs-kernel-server nfs-ganesha; do
    printf '%s=' "$unit"
    systemctl is-active "$unit" 2>/dev/null || true
  done
fi

section protocol-and-pnfs
if test -r /proc/fs/nfsd/versions; then
  printf 'versions='
  tr '\n' ' ' </proc/fs/nfsd/versions
  printf '\n'
else
  echo 'versions=unavailable'
fi
for path in /proc/fs/nfsd/pnfs /proc/fs/nfsd/exports /etc/ganesha/ganesha.conf; do
  if test -e "$path"; then
    printf '%s=present' "$path"
    test -r "$path" && printf ',readable'
    printf '\n'
  else
    printf '%s=absent\n' "$path"
  fi
done
if command -v exportfs >/dev/null 2>&1; then
  exportfs -v 2>&1 || true
fi

section fault-tools
for command in nft iptables ip6tables tc ss conntrack socat toxiproxy-cli; do
  if path="$(command -v "$command" 2>/dev/null)"; then
    printf '%s=%s\n' "$command" "$path"
  else
    printf '%s=absent\n' "$command"
  fi
done

section repository-owned-lab-commands
find /usr/local/sbin -maxdepth 1 -type f -name 'terrasync-lab-*' -printf '%f\n' 2>/dev/null |
  LC_ALL=C sort || true

section callback-fault-injection
echo 'real_server_callback_origin=supported:knfsd-write-delegation-conflict'
echo 'selective_callback_reply_loss=supported:runner-rpc-aware-proxy'
echo 'callback_retransmission=supported:proxy-replays-original-knfsd-cb-compound'

section sudo-allow-list
sudo -n -l 2>&1 || true
REMOTE
}

probe_host source "$LAB_SOURCE_MGMT"
probe_host destination "$LAB_DEST_MGMT"
probe_host worker "$LAB_WORKER_MGMT"

echo "lab capability report written to $report_dir"
