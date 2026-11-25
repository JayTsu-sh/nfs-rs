#!/usr/bin/env bash
set -euo pipefail

for zig in /opt/zig/0.13.0/zig "$HOME/.local/zig/0.13.0/zig" "$(command -v zig || true)"; do
  if [[ -x "$zig" && "$("$zig" version)" == "0.13.0" ]]; then
    printf '%s\n' "$zig"
    exit 0
  fi
done
echo "pre-provisioned Zig 0.13.0 not found" >&2
exit 1
