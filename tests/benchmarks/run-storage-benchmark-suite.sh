#!/usr/bin/env bash
set -euo pipefail
lab_dir="$(dirname "$0")/../lab"
source "$lab_dir/common.sh"
source "$lab_dir/ots-executor.sh"

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

remote_dir=""
local_workspace=""
cleanup_ots_benchmark() {
  local status=$? cleanup_status=0
  if [[ -n "$remote_dir" ]]; then
    ots_remove_workspace "$remote_dir" || cleanup_status=$?
  fi
  if [[ -n "$local_workspace" ]]; then
    rm -r -- "$local_workspace" || cleanup_status=$?
  fi
  if ((status == 0)); then
    status=$cleanup_status
  fi
  exit "$status"
}
trap cleanup_ots_benchmark EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

build_ots_benchmark_binary() {
  local output="$1"
  local zig_bin target_dir wrapper="$local_workspace/zig-cc"
  zig_bin="$("$lab_dir/find-runner-zig.sh")"
  target_dir="$(cargo metadata --no-deps --format-version 1 | jq -r .target_directory)"
  printf '#!/usr/bin/env bash\nexec "%s" cc --target=x86_64-linux-gnu.2.17 "$@"\n' \
    "$zig_bin" > "$wrapper"
  chmod +x "$wrapper"
  # The runner's native glibc build is too new for the DXN executor.
  RUSTFLAGS="-C linker=$wrapper" cargo build --release --locked \
    --target x86_64-unknown-linux-gnu --bin nfs-storage-benchmark
  cp -- "$target_dir/x86_64-unknown-linux-gnu/release/nfs-storage-benchmark" "$output"
}

prepare_ots_benchmark() {
  if [[ -n "$remote_dir" ]]; then
    return
  fi
  ots_configure_ssh
  local_workspace="$(mktemp -d "${TMPDIR:-/tmp}/nfsrs-ots-bench.XXXXXX")"
  local binary="$local_workspace/nfs-storage-benchmark"
  build_ots_benchmark_binary "$binary"
  remote_dir="$(ots_create_workspace "nfsrs-bench-$run_id")"
  ots_ssh "cat > '$remote_dir/nfs-storage-benchmark'" < "$binary"
  ots_exec "chmod 700 '$remote_dir/nfs-storage-benchmark'"
}

run_environment() {
  local environment="$1" template="$2" runs="$3" target="$4" executor="$5"
  local url="${template//\$\{RUN_ID\}/$run_id}"
  local run suffix remote_command
  if [[ -n "$executor" ]]; then
    if [[ "$executor" != "ots-dxn-node" || -z "${LAB_OTS_SSH_KEY:-}" ]]; then
      echo "skipping benchmark environment $environment (executor unavailable)"
      return
    fi
    prepare_ots_benchmark
  fi

  for run in $(seq 1 "$runs"); do
    suffix="-capture-$run"
    [[ "$mode" == gate ]] && suffix="-run-$run"
    local -a arguments=(
      --environment "$environment" --run-id "$run_id-$run" --window-id "$run_id"
      --commit "$commit" --url "$url" --samples "$samples" --payload-mib "$payload_mib"
    )
    if [[ -n "$executor" ]]; then
      printf -v remote_command '%q ' "$remote_dir/nfs-storage-benchmark" "${arguments[@]}"
      ots_exec "$remote_command" > "$target/$environment$suffix.json"
    else
      cargo run --release --locked --bin nfs-storage-benchmark -- "${arguments[@]}" \
        > "$target/$environment$suffix.json"
    fi
  done
}

runs="${NFS_RS_BENCHMARK_CAPTURE_RUNS:-1}"
[[ "$mode" == gate ]] && runs="${NFS_RS_BENCHMARK_GATE_RUNS:-5}"
while IFS=$'\t' read -r environment template executor; do
  run_environment "$environment" "$template" "$runs" "$output" "$executor"
done < <(jq -r '.environments | sort_by(.executor == null)[] | [.id, .url_template, .executor // ""] | @tsv' "$manifest")

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
        IFS=$'\t' read -r template executor < <(jq -r --arg environment "$environment" \
          '.environments[] | select(.id == $environment) | [.url_template, .executor // ""] | @tsv' "$manifest")
        run_environment "$environment" "$template" "$runs" "$supplemental" "$executor"
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
