#!/usr/bin/env bash
# Sync the repo and the local cargo registry to the benchmark client, build the
# Rust harness offline (crates.io is blocked there), and install the nfs-rs
# wheel into a venv for the Python harness.
#
# usage: tests/benchmarks/compare/deploy.sh [host] [nfs-rs wheel version | local]
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
if [ "$wheel_version" = local ]; then
  # Build the wheel from this checkout on the client itself (its glibc is older than ours).
  $remote/venv/bin/pip install -q "maturin>=1,<2"
  rm -rf target/wheels
  $remote/venv/bin/maturin build --release --offline --locked --out target/wheels >/dev/null
  $remote/venv/bin/pip install -q --force-reinstall --no-deps target/wheels/nfs_rs-*.whl
else
  $remote/venv/bin/pip install -q "nfs-rs==$wheel_version"
fi
$remote/venv/bin/python -c 'import nfs_rs; print("nfs_rs", nfs_rs.__version__)'
target/release/nfs-perf-compare --target /tmp --workdir perfcmp-deploy-check --io buffered --smoke metadata >/dev/null
echo "deploy ok: commit $(cat COMMIT)"
EOF
