#!/usr/bin/env bash
# Sync the repo and the local cargo registry to the benchmark client, build the
# Rust harness offline (crates.io is blocked there), and install the nfs-rs
# wheel into a venv for the Python harness.
#
# usage: tests/benchmarks/compare/deploy.sh [host] [nfs-rs wheel version]
set -euo pipefail

host="${1:-10.131.6.181}"
wheel_version="${2:-0.6.1}"
remote="/root/nfs-rs-perf"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

cd "$repo_root"
cargo fetch --locked
git rev-parse --short HEAD > COMMIT
ssh "root@$host" "mkdir -p $remote/repo $remote/results /root/.cargo/registry"
rsync -az --delete --exclude target --exclude .git --exclude '__pycache__' --exclude '.pytest_cache' \
  "$repo_root/" "root@$host:$remote/repo/"
rsync -az "$HOME/.cargo/registry/" "root@$host:/root/.cargo/registry/"
ssh "root@$host" bash -s <<EOF
set -euo pipefail
cd $remote/repo
cargo build --release --offline --bin nfs-perf-compare
[ -x $remote/venv/bin/python ] || python3 -m venv $remote/venv
$remote/venv/bin/pip install -q "nfs-rs==$wheel_version"
$remote/venv/bin/python -c 'import nfs_rs; print("nfs_rs", nfs_rs.__version__)'
target/release/nfs-perf-compare --target /tmp --workdir perfcmp-deploy-check --io buffered --smoke metadata >/dev/null
echo "deploy ok: commit $(cat COMMIT)"
EOF
