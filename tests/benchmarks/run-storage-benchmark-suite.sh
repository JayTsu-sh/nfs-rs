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

while IFS=$'\t' read -r environment template; do
  url="${template//\$\{RUN_ID\}/$run_id}"
  runs="${NFS_RS_BENCHMARK_CAPTURE_RUNS:-1}"
  [[ "$mode" == gate ]] && runs="${NFS_RS_BENCHMARK_GATE_RUNS:-5}"
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
      >"$output/$environment$suffix.json"
  done
done < <(jq -r '.environments[] | [.id, .url_template] | @tsv' "$manifest")

if [[ "$mode" == gate ]]; then
  python3 tests/benchmarks/check-performance-baselines.py \
    --manifest "$manifest" --results-dir "$output" --output "$output/gate.json" || gate_status=$?
fi

report_args=(--manifest "$manifest" --results-dir "$output" --output-dir "$output/report")
[[ "$mode" == gate ]] && report_args+=(--gate-result "$output/gate.json")
python3 tests/benchmarks/generate-baseline-report.py "${report_args[@]}" || report_status=$?
if [[ "$mode" == gate ]]; then
  [[ "${gate_status:-0}" -eq 0 && "${report_status:-0}" -eq 0 ]] || exit 2
else
  [[ "${report_status:-0}" -eq 0 || "${report_status:-0}" -eq 2 ]] || exit "${report_status}"
fi
