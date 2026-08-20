#!/usr/bin/env python3
import argparse
import json
import statistics
from pathlib import Path


def percentile(values, fraction):
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * fraction + 0.999999) - 1))
    return ordered[index]


def operation_values(captures, name):
    if name in ("mount_ms", "umount_ms"):
        return [capture["lifs"][0][name] for capture in captures]
    return [
        value
        for capture in captures
        for sample in capture["lifs"][0]["samples"]
        for value in [sample.get(name)]
        if isinstance(value, (int, float))
    ]


parser = argparse.ArgumentParser()
parser.add_argument("--manifest", required=True)
parser.add_argument("--captures-root", required=True)
parser.add_argument("--output-dir", required=True)
args = parser.parse_args()
manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
captures_root = Path(args.captures_root)
output = Path(args.output_dir)
output.mkdir(parents=True, exist_ok=True)

incomplete = []
for environment in manifest["environments"]:
    captures_by_run = {}
    for path in captures_root.rglob(f'{environment["id"]}-capture-*.json'):
        document = json.loads(path.read_text(encoding="utf-8"))
        if (
            document.get("environment") == environment["id"]
            and document.get("protocol") == environment["protocol"]
            and document.get("status") == "pass"
            and len(document.get("lifs", [])) == 1
        ):
            run_id = document.get("run_id")
            if run_id:
                captures_by_run[run_id] = document
    captures = list(captures_by_run.values())
    windows = {}
    for capture in captures:
        window_id = capture.get("window_id")
        if window_id:
            windows.setdefault(window_id, []).append(capture)
    if len(captures) < manifest["minimum_capture_runs"]:
        incomplete.append(f'{environment["id"]}: {len(captures)} captures')
        continue
    if len(windows) < manifest["minimum_capture_windows"]:
        incomplete.append(f'{environment["id"]}: {len(windows)} capture windows')
        continue
    if any(len(window) > manifest["maximum_runs_per_window"] for window in windows.values()):
        incomplete.append(f'{environment["id"]}: too many captures in one window')
        continue
    operation_samples = {name: [] for name in [
        "mount_ms", "umount_ms", "null_ms", "fsinfo_ms", "fsstat_ms", "mkdir_ms", "create_ms",
        "lookup_ms", "getattr_ms", "access_ms", "pathconf_ms", "write_ms",
        "commit_ms", "close_ms", "open_ms", "read_ms", "rename_ms",
        "link_ms", "symlink_ms", "readlink_ms", "readdir_ms", "remove_ms",
        "rmdir_ms", "write_mib_s", "read_mib_s"
    ]}
    for name in operation_samples:
        operation_samples[name] = operation_values(captures, name)
    metrics = {}
    for name, values in operation_samples.items():
        if not values:
            continue
        metrics[name] = {
            "p50": percentile(values, 0.50),
            "p95": percentile(values, 0.95),
            "p99": percentile(values, 0.99),
            "median": statistics.median(values),
            "mean": statistics.fmean(values),
            "sample_count": len(values),
        }
        window_p95_values = [
            percentile(window_values, 0.95)
            for window in windows.values()
            for window_values in [operation_values(window, name)]
            if window_values
        ]
        metrics[name]["window_p95"] = {
            "p50": percentile(window_p95_values, 0.50),
            "p95": percentile(window_p95_values, 0.95),
            "p99": percentile(window_p95_values, 0.99),
            "sample_count": len(window_p95_values),
        }
    pathconf_statuses = sorted({
        sample.get("pathconf_status", "pass")
        for capture in captures
        for sample in capture["lifs"][0]["samples"]
    })
    if len(pathconf_statuses) != 1:
        incomplete.append(f'{environment["id"]}: inconsistent PATHCONF capability')
        continue
    baseline = {
        "schema_version": 1,
        "environment": environment["id"],
        "endpoint": environment["endpoint"],
        "protocol": environment["protocol"],
        "status": "accepted",
        "capture_runs": len(captures),
        "capture_windows": len(windows),
        "captured_commits": sorted({capture.get("commit", "unknown") for capture in captures}),
        "runners": sorted({capture.get("runner", "unknown") for capture in captures}),
        "capabilities": {"pathconf": pathconf_statuses[0]},
        "thresholds": {
            "throughput_regression_percent": 15,
            "p95_latency_regression_percent": 30,
            "p99_latency_regression_percent": 50,
        },
        "benchmarks": {"storage_path": metrics},
    }
    (output / f'{environment["id"]}.json').write_text(
        json.dumps(baseline, indent=2) + "\n", encoding="utf-8"
    )

if incomplete:
    raise SystemExit("insufficient baseline captures:\n" + "\n".join(incomplete))
