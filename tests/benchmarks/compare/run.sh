#!/usr/bin/env bash
# Matrix driver for nfs-rs vs kernel mount comparison. Runs on the benchmark
# client as root (needs mount/umount and /proc/sys/vm/drop_caches).
#
# usage: run.sh RUN_ID [protocol ...]        (default protocols: 3 4.0 4.1)
# env:   LIF (10.128.61.200) LIF_B (10.128.61.201) EXPORT (/nfsrs_perf)
#        MNT (/mnt/nfsrs_perf) BASE (/root/nfs-rs-perf) SMOKE=1 for a quick pass
#        SKIP_LIF_B=1 to skip the cross-check on the second LIF
#        DATA_REPEAT (5) / MC_REPEAT (3) repeats for large-file and multiclient cases
set -uo pipefail

run_id="${1:?run id required}"
shift
protocols=("$@")
[[ ${#protocols[@]} -gt 0 ]] || protocols=(3 4.0 4.1)

LIF="${LIF:-10.128.61.200}"
LIF_B="${LIF_B:-10.128.61.201}"
EXPORT="${EXPORT:-/nfsrs_perf}"
MNT="${MNT:-/mnt/nfsrs_perf}"
BASE="${BASE:-/root/nfs-rs-perf}"
BIN="$BASE/repo/target/release/nfs-perf-compare"
PY="$BASE/venv/bin/python $BASE/repo/tests/benchmarks/compare/perf_compare.py"
OUT="$BASE/results/$run_id"
DATA_REPEAT="${DATA_REPEAT:-5}"
MC_REPEAT="${MC_REPEAT:-3}"
SMOKE_FLAG=""
[[ "${SMOKE:-0}" == 1 ]] && SMOKE_FLAG="--smoke"
export PERF_COMMIT="${PERF_COMMIT:-$(cat "$BASE/repo/COMMIT" 2>/dev/null || echo unknown)}"

mkdir -p "$OUT" "$MNT"
log() { echo "[$(date +%T)] $*" | tee -a "$OUT/progress.log"; }

mounted=0
cleanup() {
  if [[ $mounted == 1 ]]; then
    umount "$MNT" 2>/dev/null || umount -l "$MNT" 2>/dev/null
    mounted=0
  fi
}
trap cleanup EXIT

mount_kernel() {   # proto variant
  local proto="$1" variant="$2" opts="vers=$1,rsize=1048576,wsize=1048576,hard,proto=tcp"
  [[ "$variant" == nolookup ]] && opts="$opts,lookupcache=none"
  cleanup
  if ! mount -t nfs -o "$opts" "$LIF:$EXPORT" "$MNT"; then
    log "MOUNT FAILED vers=$proto ($variant)"
    return 1
  fi
  mounted=1
  rm -rf "$MNT/$run_id"-* 2>/dev/null   # leftovers from an interrupted run
  export PERF_MOUNT_VARIANT="$variant" PERF_PROTOCOL="$proto"
  log "mounted $LIF:$EXPORT vers=$proto ($variant): $(grep " $MNT " /proc/mounts | sed 's/.*(//;s/)//' | cut -c1-120)"
}

harness_cmd() {   # rust|python
  if [[ "$1" == rust ]]; then echo "$BIN"; else echo "$PY"; fi
}

# invoke PROTO HARNESS BACKEND VARIANT IO SUITE PARAMS... ; target must be in $TARGET
invoke() {
  local proto="$1" harness="$2" backend="$3" variant="$4" io="$5" suite="$6"
  shift 6
  local params="$*"
  local tag="${params// /}"; tag="${tag//--/-}"
  local name="$harness-$backend-$variant-$io-$suite${tag}"
  local dir="$OUT/$proto"
  mkdir -p "$dir"
  local json="$dir/$name.json"
  [[ -s "$json" ]] && { log "skip (exists) $proto/$name"; return 0; }
  local io_args=()
  [[ "$backend" == posix && "$io" != na ]] && io_args=(--io "$io")
  log "run $proto/$name"
  # shellcheck disable=SC2086
  if ! $(harness_cmd "$harness") --target "$TARGET" "${io_args[@]}" --workdir "$run_id-$harness-$backend" \
       --json "$json" $SMOKE_FLAG "$suite" "$@" 2>"$dir/$name.stderr"; then
    local reason
    reason="$(tail -1 "$dir/$name.stderr" 2>/dev/null)"
    log "FAIL $proto/$name: $reason"
    printf '%s\t%s\n' "$name.json" "$reason" >> "$OUT/failures.txt"
    rm -f "$json"
    return 1
  fi
  rm -f "$dir/$name.stderr"
}

data_matrix() {   # proto harness backend variant io
  for size in 4k 40m 1g; do
    for qd in 1 8; do
      [[ "$size" == 4k && "$qd" == 8 ]] && continue
      invoke "$@" data --size "$size" --qd "$qd" --repeat "$DATA_REPEAT"
    done
  done
}

multiclient_matrix() {   # proto harness backend variant io
  for mode in same distinct; do
    invoke "$@" multiclient --size 1g --clients 8 --mode "$mode" --repeat "$MC_REPEAT"
  done
}

for proto in "${protocols[@]}"; do
  log "=== protocol $proto ==="
  # kernel mount, default options
  if mount_kernel "$proto" default; then
    TARGET="$MNT"
    for harness in rust python; do
      invoke "$proto" "$harness" posix default na metadata
      for io in direct buffered; do
        data_matrix "$proto" "$harness" posix default "$io"
        multiclient_matrix "$proto" "$harness" posix default "$io"
      done
    done
    cleanup
  fi
  # kernel mount, lookupcache=none (metadata only)
  if mount_kernel "$proto" nolookup; then
    TARGET="$MNT"
    for harness in rust python; do
      invoke "$proto" "$harness" posix nolookup na metadata
    done
    cleanup
  fi
  # nfs-rs userspace client
  TARGET="nfs://$LIF$EXPORT?version=$proto&rsize=1048576&wsize=1048576&uid=0&gid=0"
  unset PERF_MOUNT_VARIANT PERF_PROTOCOL
  for harness in rust python; do
    invoke "$proto" "$harness" nfsrs na na metadata
    data_matrix "$proto" "$harness" nfsrs na na
    multiclient_matrix "$proto" "$harness" nfsrs na na
  done
done

if [[ "${SKIP_LIF_B:-0}" != 1 ]]; then
  log "=== cross-check on $LIF_B ==="
  saved_lif="$LIF"
  LIF="$LIF_B"
  for proto in "${protocols[@]}"; do
    if mount_kernel "$proto" default; then
      TARGET="$MNT"
      export PERF_MOUNT_VARIANT="lif-b"
      invoke "lif-b-$proto" rust posix lif-b direct data --size 1g --qd 8 --repeat "$DATA_REPEAT"
      cleanup
    fi
    TARGET="nfs://$LIF$EXPORT?version=$proto&rsize=1048576&wsize=1048576&uid=0&gid=0"
    unset PERF_MOUNT_VARIANT PERF_PROTOCOL
    invoke "lif-b-$proto" rust nfsrs na na data --size 1g --qd 8 --repeat "$DATA_REPEAT"
  done
  LIF="$saved_lif"
fi

log "=== done: $(find "$OUT" -name '*.json' | wc -l) results, $(wc -l < "$OUT/failures.txt" 2>/dev/null || echo 0) failures ==="
