#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
source "$(dirname "$0")/ots-executor.sh"

: "${OTS_SSH_KEY:?NFS_RS_OTS_SSH_KEY is required}"
umask 077
key_dir="$(mktemp -d "$RUNNER_TEMP/ots-executor-ssh.XXXXXXXXXX")"
trap 'rm -r -- "$key_dir"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

LAB_OTS_SSH_KEY="$key_dir/key"
LAB_OTS_KNOWN_HOSTS="$key_dir/known_hosts"
printf '%s\n' "$OTS_SSH_KEY" > "$LAB_OTS_SSH_KEY"
unset OTS_SSH_KEY
touch "$LAB_OTS_KNOWN_HOSTS"
ots_configure_ssh
# Enroll both hosts for this job, then require the recorded keys in lab commands.
ssh "${ots_ssh_options[@]}" -o StrictHostKeyChecking=accept-new "$ots_jump_host" true </dev/null
ssh "${ots_ssh_options[@]}" -o StrictHostKeyChecking=accept-new \
  -o "ProxyCommand=$ots_proxy_command" "$ots_node_host" true </dev/null

printf 'LAB_OTS_SSH_KEY=%s\nLAB_OTS_KNOWN_HOSTS=%s\n' \
  "$LAB_OTS_SSH_KEY" "$LAB_OTS_KNOWN_HOSTS" >> "$GITHUB_ENV"
# The runner clears RUNNER_TEMP at the end of the job.
trap - EXIT
