#!/usr/bin/env bash
set -euo pipefail

output="${1:?evidence directory required}"
name="${2:?evidence name required}"
shift 2
[[ "$name" =~ ^[a-z0-9-]+$ ]] || { echo "invalid evidence name" >&2; exit 2; }
(( $# > 0 )) || { echo "evidence command required" >&2; exit 2; }
mkdir -p "$output"

started_at_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
set +e
"$@" 2>&1 | tee "$output/$name.log"
exit_code=${PIPESTATUS[0]}
set -e
finished_at_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
if (( exit_code == 0 )); then outcome=PASS; else outcome=FAIL; fi
log_sha256="$(sha256sum "$output/$name.log" | cut -d' ' -f1)"

EVIDENCE_NAME="$name" STARTED_AT_UTC="$started_at_utc" \
FINISHED_AT_UTC="$finished_at_utc" OUTCOME="$outcome" EXIT_CODE="$exit_code" \
LOG_SHA256="$log_sha256" python3 - "$output/$name.json" "$@" <<'PY'
import json
import os
import sys

document = {
    "schema_version": 1,
    "name": os.environ["EVIDENCE_NAME"],
    "started_at_utc": os.environ["STARTED_AT_UTC"],
    "finished_at_utc": os.environ["FINISHED_AT_UTC"],
    "outcome": os.environ["OUTCOME"],
    "exit_code": int(os.environ["EXIT_CODE"]),
    "command": sys.argv[2:],
    "log": f'{os.environ["EVIDENCE_NAME"]}.log',
    "sha256": os.environ["LOG_SHA256"],
}
with open(sys.argv[1], "w", encoding="utf-8") as stream:
    json.dump(document, stream, indent=2)
    stream.write("\n")
PY
exit "$exit_code"
