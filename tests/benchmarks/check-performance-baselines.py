#!/usr/bin/env python3
import argparse
import json
import statistics
from pathlib import Path


def percentile(values, fraction):
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * fraction + 0.999999) - 1))
    return ordered[index]


parser = argparse.ArgumentParser()
parser.add_argument("--manifest", required=True)
parser.add_argument("--results-dir", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()
manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
results_root = Path(args.results_dir)
rows = []
complete = True

for environment in manifest["environments"]:
    baseline = json.loads(Path(environment["baseline"]).read_text(encoding="utf-8"))
    accepted = (
        baseline.get("status") == "accepted"
        and baseline.get("capture_runs", 0) >= manifest["minimum_capture_runs"]
        and baseline.get("capture_windows", 0) >= manifest["minimum_capture_windows"]
    )
    run_paths = sorted(results_root.glob(f'{environment["id"]}-run-*.json'))
    valid_runs = []
    for path in run_paths:
        result = json.loads(path.read_text(encoding="utf-8"))
        if result.get("status") == "pass" and len(result.get("lifs", [])) == 1:
            valid_runs.append(result)
    status = "baseline_missing"
    violations = []
    if accepted and len(valid_runs) >= 4:
        status = "pass"
        reference = baseline["benchmarks"]["storage_path"]
        thresholds = baseline["thresholds"]
        samples = [sample for run in valid_runs for sample in run["lifs"][0]["samples"]]
        expected_pathconf = baseline.get("capabilities", {}).get("pathconf", "pass")
        actual_pathconf = {sample.get("pathconf_status", "pass") for sample in samples}
        if actual_pathconf != {expected_pathconf}:
            violations.append({
                "metric": "pathconf_capability",
                "actual": sorted(actual_pathconf),
                "expected": expected_pathconf,
            })
        metadata_latency_metrics = [
            "null_ms", "fsinfo_ms", "fsstat_ms", "mkdir_ms", "create_ms",
            "lookup_ms", "getattr_ms", "access_ms", "pathconf_ms", "rename_ms",
            "link_ms", "symlink_ms", "readlink_ms", "readdir_ms", "remove_ms",
            "rmdir_ms", "mount_ms", "umount_ms",
        ]
        metadata_latency_metric_set = set(metadata_latency_metrics)
        latency_metrics = metadata_latency_metrics + [
            "write_ms", "commit_ms", "close_ms", "open_ms", "read_ms",
        ]
        checks = [
            ("write_mib_s", statistics.median(row["write_mib_s"] for row in samples), "minimum"),
            ("read_mib_s", statistics.median(row["read_mib_s"] for row in samples), "minimum"),
        ]
        checks.extend(
            (metric, percentile(values, 0.95), "maximum")
            for metric in latency_metrics
            if metric in reference
            for values in [[row[metric] for row in samples if isinstance(row.get(metric), (int, float))]]
            if values
        )
        for metric in ["mount_ms", "umount_ms"]:
            if metric in reference:
                values = [run["lifs"][0][metric] for run in valid_runs]
                checks.append((metric, percentile(values, 0.95), "maximum"))
        for metric, actual, direction in checks:
            if direction == "minimum":
                reference_value = reference[metric]["median"]
            else:
                reference_value = (
                    reference[metric].get("window_p95", {}).get("p95")
                    or reference[metric]["p95"]
                )
            budget = thresholds[
                "throughput_regression_percent" if direction == "minimum"
                else "p95_latency_regression_percent"
            ] / 100
            limit = reference_value * (1 - budget if direction == "minimum" else 1 + budget)
            if metric in metadata_latency_metric_set:
                limit = max(limit, thresholds["metadata_p95_absolute_floor_ms"])
            violated = actual < limit if direction == "minimum" else actual > limit
            if violated:
                violations.append({"metric": metric, "actual": actual, "limit": limit})
        if violations:
            status = "fail"
    elif accepted:
        status = "insufficient_valid_runs"
    if status != "pass":
        complete = False
    rows.append({
        "environment": environment["id"],
        "status": status,
        "valid_runs": len(valid_runs),
        "violations": violations,
    })

document = {"schema_version": 1, "status": "pass" if complete else "fail", "environments": rows}
Path(args.output).write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
raise SystemExit(0 if complete else 2)
