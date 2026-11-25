"""Combine runner and OTS validator evidence and decide the job status."""

import json
import sys
from pathlib import Path


def error(case, stage, kind, message, exit_code=None):
    detail = {"case": case, "stage": stage, "type": kind, "message": message}
    if exit_code is not None:
        detail["exit_code"] = exit_code
    return detail


def read_result(path, run_id, location, cases, failures, details):
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        if document["run_id"] != run_id:
            raise ValueError("unexpected run ID")
        names = [entry["case"]["name"] for entry in document["results"]]
        case_failures = document.get("failures", [])
        case_details = document.get("failure_details", [])
        if not case_failures and (set(names) != cases or len(names) != len(cases)):
            raise ValueError(f"unexpected result cases: {names!r}")
        if document.get("status") == "failed" and not case_failures:
            raise ValueError("validator reported failure without details")
    except (OSError, ValueError, KeyError, TypeError) as caught:
        message = f"{location}: invalid validator evidence: {caught}"
        failures.append(message)
        details.append(error(location, "result", type(caught).__name__, str(caught)))
        return {}
    failures.extend(case_failures)
    details.extend(case_details)
    return document


def main():
    run_id, workdir, output, direct_status, stage, ots_status, execute_status, cleanup_status = sys.argv[1:]
    direct_status, ots_status, execute_status, cleanup_status = map(
        int, (direct_status, ots_status, execute_status, cleanup_status)
    )
    workdir = Path(workdir)
    failures = []
    details = []

    def record_exit(case, phase, code):
        kind = "ProcessError"
        if case == "ots-pnfs-mds" and code == 255:
            kind = "TransportError"
        elif phase == "validate" and code == 124:
            kind = "TimeoutError"
        message = f"{phase} exited with code {code}"
        failures.append(f"{case}: {kind}: {message}")
        details.append(error(case, phase, kind, message, code))

    direct = {}
    if direct_status == 0 or (workdir / "direct.json").is_file():
        direct = read_result(
            workdir / "direct.json", run_id, "runner", {"linux-source-v3", "dxn-v40", "linux-source-v41"},
            failures, details,
        )
    if direct_status and not direct.get("failures"):
        record_exit("runner", "validate", direct_status)

    ots = {}
    if stage == "validate":
        ots = read_result(workdir / "ots.json", run_id, "ots-pnfs-mds", {"ots-pnfs-mds"}, failures, details)
        if ots_status and not ots.get("failures"):
            record_exit("ots-pnfs-mds", stage, ots_status)
    else:
        if execute_status and stage == "fetch":
            record_exit("ots-pnfs-mds", "validate", execute_status)
        record_exit("ots-pnfs-mds", stage, ots_status)
    if cleanup_status:
        record_exit("ots-pnfs-mds", "cleanup", cleanup_status)

    merged = dict(direct or ots)
    merged.update(
        schema_version=1,
        run_id=run_id,
        results=direct.get("results", []) + ots.get("results", []),
        failures=failures,
        failure_details=details,
        status="failed" if failures else "passed",
    )
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(merged, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for failure in failures:
        print(failure, file=sys.stderr)
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
