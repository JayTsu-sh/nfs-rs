#!/usr/bin/env bash
set -euo pipefail

mode="${1:?capture or gate mode required}"
run_id="${2:?run id required}"
output="${3:?output directory required}"
samples="${NFS_RS_BENCHMARK_SAMPLES:-20}"
payload_mib="${NFS_RS_BENCHMARK_PAYLOAD_MIB:-4}"
manifest="tests/benchmarks/baselines/manifest.json"
commit="$(git rev-parse HEAD)"
[[ "$mode" == capture || "$mode" == gate ]] || {
  echo "benchmark mode must be capture or gate" >&2
  exit 2
}

[[ "$run_id" =~ ^(nightly|release)-[A-Za-z0-9._-]{1,80}$ ]] || {
  echo "unsafe benchmark run id: $run_id" >&2
  exit 2
}
[[ "$samples" =~ ^[1-9][0-9]*$ && "$payload_mib" =~ ^[1-9][0-9]*$ ]] || {
  echo "benchmark samples and payload must be positive integers" >&2
  exit 2
}
mkdir -p "$output"
[[ -z "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit)" ]] || {
  echo "benchmark output directory is not empty: $output" >&2
  exit 1
}

# Benchmark traffic must exclude ordinary lab I/O as well as other benchmark
# suites. Keep the acquisition order identical in every caller.
exec 8>/tmp/terrasync-lab-tests.lock
flock --wait 1800 8
exec 9>/tmp/terrasync-lab-performance.lock
flock --wait 1800 9

run_environment() {
  local environment="$1"
  local template="$2"
  local runs="$3"
  local target="$4"
  url="${template//\$\{RUN_ID\}/$run_id}"
  for run in $(seq 1 "$runs"); do
    suffix="-capture-$run"
    [[ "$mode" == gate ]] && suffix="-run-$run"
    cargo run --release --locked --bin nfs-storage-benchmark -- \
      --environment "$environment" \
      --run-id "$run_id-$run" \
      --window-id "$run_id" \
      --commit "$commit" \
      --url "$url" \
      --samples "$samples" \
      --payload-mib "$payload_mib" \
      >"$target/$environment$suffix.json"
  done
}

# Executor-marked environments point at storage the runner cannot reach
# directly. The OTS data network (10.30.3.x) is only reachable through the
# management jump (10.128.134.90) onto the DXN node (10.128.135.141, data IP
# 10.30.3.141); the benchmark binary is cross-built for glibc 2.17 (the node
# is glibc 2.28) with the same zig cross-compile path the manylinux2014 wheel
# uses, shipped to the node, and run there. Without LAB_OTS_SSH_KEY the
# environments stay executor_pending in the gate report.
ots_executor_available() {
  [[ -n "${LAB_OTS_SSH_KEY:-}" && -f "${LAB_OTS_SSH_KEY:-}" ]]
}

if ots_executor_available; then
  ots_user="${LAB_OTS_SSH_USER:-root}"
  ots_jump="${LAB_OTS_JUMP:-10.128.134.90}"
  ots_node="${LAB_OTS_NODE:-10.128.135.141}"
  ots_key="$LAB_OTS_SSH_KEY"
  ots_known="${LAB_OTS_KNOWN_HOSTS:-}"
  hop_options=(-T -i "$ots_key" -o IdentitiesOnly=yes -o BatchMode=yes
    -o ConnectTimeout=20)
  if [[ -n "$ots_known" && -f "$ots_known" ]]; then
    hop_options+=(-o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$ots_known")
    proxy_host_opts="-o StrictHostKeyChecking=yes -o UserKnownHostsFile=$ots_known"
  else
    hop_options+=(-o StrictHostKeyChecking=accept-new)
    proxy_host_opts="-o StrictHostKeyChecking=accept-new"
  fi
  remote_dir=""
  remote_binary_ready=0
  ssh_ots() {
    ssh "${hop_options[@]}" \
      -o "ProxyCommand=ssh -i $ots_key -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=20 $proxy_host_opts -W %h:%p $ots_user@$ots_jump" \
      "$ots_user@$ots_node" "$@"
  }
  ots_cleanup() {
    if [[ -n "$remote_dir" ]]; then
      ssh_ots "rm -r -- '$remote_dir'" >/dev/null 2>&1 || true
    fi
  }
  trap ots_cleanup EXIT

  build_ots_benchmark_binary() {
    local output="$1"
    local zig_bin="" candidate
    for candidate in /opt/zig/0.13.0 "$HOME/.local/zig/0.13.0"; do
      if [[ -x "$candidate/zig" ]]; then
        zig_bin="$candidate/zig"
        break
      fi
    done
    if [[ -z "$zig_bin" ]] && command -v zig >/dev/null 2>&1; then
      zig_bin="$(command -v zig)"
    fi
    [[ -n "$zig_bin" ]] || {
      echo "zig not found; cannot cross-build the OTS benchmark binary" >&2
      return 1
    }
    local target_dir wrapper
    target_dir="$(cargo metadata --no-deps --format-version 1 | jq -r .target_directory)"
    wrapper="$(mktemp "${TMPDIR:-/tmp}/nfsrs-zig-cc.XXXXXX")"
    printf '#!/usr/bin/env bash\nexec "%s" cc --target=x86_64-linux-gnu.2.17 "$@"\n' \
      "$zig_bin" > "$wrapper"
    chmod +x "$wrapper"
    # glibc 2.17 keeps the binary loadable on the node (glibc 2.28) and any
    # manylinux2014 host; the runner's own glibc 2.39 build is not portable.
    RUSTFLAGS="-C linker=$wrapper" cargo build --release --locked \
      --target x86_64-unknown-linux-gnu --bin nfs-storage-benchmark
    rm -f -- "$wrapper"
    cp -f -- "$target_dir/x86_64-unknown-linux-gnu/release/nfs-storage-benchmark" "$output"
  }

  run_ots_environment() {
    local environment="$1"
    local template="$2"
    local runs="$3"
    local target="$4"
    local url="${template//\$\{RUN_ID\}/$run_id}"
    if ((remote_binary_ready == 0)); then
      remote_dir="/tmp/nfsrs-bench-$run_id"
      local binary
      binary="$(mktemp -d "${TMPDIR:-/tmp}/nfsrs-ots-bench.XXXXXX")/nfs-storage-benchmark"
      build_ots_benchmark_binary "$binary"
      ssh_ots "rm -r -- '$remote_dir' && mkdir -p -- '$remote_dir'"
      ssh_ots "cat > '$remote_dir/nfs-storage-benchmark'" < "$binary"
      ssh_ots "chmod 755 '$remote_dir/nfs-storage-benchmark'"
      rm -r -- "$(dirname "$binary")"
      remote_binary_ready=1
    fi
    local run suffix
    for run in $(seq 1 "$runs"); do
      suffix="-capture-$run"
      [[ "$mode" == gate ]] && suffix="-run-$run"
      ssh_ots "$remote_dir/nfs-storage-benchmark --environment $environment \
        --run-id $run_id-$run --window-id $run_id --commit $commit \
        --url \"$url\" --samples $samples --payload-mib $payload_mib" \
        > "$target/$environment$suffix.json"
    done
  }
fi

while IFS=$'\t' read -r environment template executor; do
  runs="${NFS_RS_BENCHMARK_CAPTURE_RUNS:-1}"
  [[ "$mode" == gate ]] && runs="${NFS_RS_BENCHMARK_GATE_RUNS:-5}"
  if [[ "$executor" == "ots-dxn-node" ]] && ots_executor_available; then
    run_ots_environment "$environment" "$template" "$runs" "$output"
  else
    echo "skipping benchmark environment $environment (executor acquisition not wired on this runner)"
  fi
done < <(jq -r '.environments[] | select(.executor != null) | [.id, .url_template, .executor] | @tsv' "$manifest")

while IFS=$'\t' read -r environment template; do
  runs="${NFS_RS_BENCHMARK_CAPTURE_RUNS:-1}"
  [[ "$mode" == gate ]] && runs="${NFS_RS_BENCHMARK_GATE_RUNS:-5}"
  run_environment "$environment" "$template" "$runs" "$output"
done < <(jq -r '.environments[] | select(.executor == null) | [.id, .url_template] | @tsv' "$manifest")

if [[ "$mode" == gate ]]; then
  python3 tests/benchmarks/check-performance-baselines.py \
    --manifest "$manifest" --results-dir "$output" --output "$output/gate-initial.json" || initial_gate_status=$?
  mapfile -t supplemental_environments < <(
    jq -r '.environments[] | select(.supplemental_eligible) | .environment' "$output/gate-initial.json"
  )
  if ((${#supplemental_environments[@]})); then
    supplemental_args=()
    for round in 1 2 3; do
      supplemental="$output/supplemental-$round"
      mkdir -p "$supplemental"
      supplemental_args+=(--supplemental-results-dir "$supplemental")
      for environment in "${supplemental_environments[@]}"; do
        template="$(jq -r --arg environment "$environment" \
          '.environments[] | select(.id == $environment) | .url_template' "$manifest")"
        run_environment "$environment" "$template" "${NFS_RS_BENCHMARK_GATE_RUNS:-5}" "$supplemental"
      done
    done
    python3 tests/benchmarks/check-performance-baselines.py \
      --manifest "$manifest" --results-dir "$output" \
      "${supplemental_args[@]}" --output "$output/gate.json" || gate_status=$?
  else
    cp "$output/gate-initial.json" "$output/gate.json"
    gate_status="${initial_gate_status:-0}"
  fi
fi

report_args=(--manifest "$manifest" --results-dir "$output" --output-dir "$output/report")
if [[ "$mode" == gate ]]; then
  report_args+=(--gate-result "$output/gate.json")
  for supplemental in "$output"/supplemental-*; do
    [[ -d "$supplemental" ]] && report_args+=(--supplemental-results-dir "$supplemental")
  done
fi
python3 tests/benchmarks/generate-baseline-report.py "${report_args[@]}" || report_status=$?
if [[ "$mode" == gate ]]; then
  [[ "${gate_status:-0}" -eq 0 && "${report_status:-0}" -eq 0 ]] || exit 2
else
  [[ "${report_status:-0}" -eq 0 || "${report_status:-0}" -eq 2 ]] || exit "${report_status}"
fi
