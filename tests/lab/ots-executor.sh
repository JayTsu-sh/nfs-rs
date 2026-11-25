#!/usr/bin/env bash
# Source common.sh before loading the executor functions.

ots_configure_ssh() {
  validate_ipv4 "$LAB_OTS_JUMP" && validate_ipv4 "$LAB_OTS_NODE" || return
  if [[ ! "$LAB_OTS_SSH_USER" =~ ^[a-z_][a-z0-9_-]*$ ]]; then
    echo "invalid OTS executor SSH user" >&2
    return 2
  fi
  local key="${LAB_OTS_SSH_KEY:-}" known="${LAB_OTS_KNOWN_HOSTS:-}"
  if [[ ! -f "$key" || ! -r "$key" || ! -s "$key" || ! -f "$known" || ! -r "$known" ]]; then
    echo "OTS executor key and known_hosts are required" >&2
    return 2
  fi

  ots_jump_host="$LAB_OTS_SSH_USER@$LAB_OTS_JUMP"
  ots_node_host="$LAB_OTS_SSH_USER@$LAB_OTS_NODE"
  ots_ssh_options=(-T -i "$key" -o IdentitiesOnly=yes -o BatchMode=yes
    -o ConnectTimeout=15 -o ServerAliveInterval=15 -o ServerAliveCountMax=2
    -o "UserKnownHostsFile=$known")

  # OpenSSH expands % tokens before passing ProxyCommand to the shell.
  local proxy
  # Quote each option as a shell word while leaving the forwarding tokens intact.
  printf -v proxy '%q ' ssh "${ots_ssh_options[@]//%/%%}" -o StrictHostKeyChecking=yes
  ots_proxy_command="exec ${proxy}-W %h:%p $ots_jump_host"
}

ots_ssh() {
  ssh "${ots_ssh_options[@]}" -o StrictHostKeyChecking=yes \
    -o "ProxyCommand=$ots_proxy_command" "$ots_node_host" "$@"
}

ots_exec() {
  ots_ssh "$@" </dev/null
}

ots_create_workspace() {
  local prefix="$1" path suffix
  [[ "$prefix" =~ ^nfsrs-[A-Za-z0-9._-]+$ ]] || return 2
  path="$(ots_exec "umask 077; mktemp -d /tmp/$prefix.XXXXXXXXXX")" || return
  suffix="${path#"/tmp/$prefix."}"
  if [[ "$suffix" == "$path" || ! "$suffix" =~ ^[A-Za-z0-9]{10}$ ]]; then
    echo "invalid OTS executor workspace path" >&2
    return 2
  fi
  printf '%s\n' "$path"
}

ots_remove_workspace() {
  local path="$1"
  [[ "$path" =~ ^/tmp/nfsrs-[A-Za-z0-9._-]+\.[A-Za-z0-9]{10}$ ]] || return 2
  ots_exec "if test -d '$path'; then rm -r -- '$path'; fi"
}
