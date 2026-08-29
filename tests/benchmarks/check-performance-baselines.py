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
parser.add_argument("--supplemental-results-dir", action="append", default=[])
parser.add_argument("--output", required=True)
args = parser.parse_args()
manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
soft_policy = manifest.get("soft_threshold_policy", {})
throughput_soft_factor = soft_policy.get("throughput_hard_limit_factor", 1.0)
latency_soft_factor = soft_policy.get("latency_hard_limit_factor", 1.0)
results_root = Path(args.results_dir)
supplemental_roots = [Path(path) for path in args.supplemental_results_dir]
rows = []
complete = True
has_warnings = False


def evaluate_runs(baseline, run_paths):
    valid_runs = []
    for path in run_paths:
        result = json.loads(path.read_text(encoding="utf-8"))
        if result.get("status") == "pass" and len(result.get("lifs", [])) == 1:
            valid_runs.append(result)
    status = "insufficient_valid_runs"
    violations = []
    warnings = []
    if len(valid_runs) >= 4:
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
            metric_policy = baseline.get("metric_thresholds", {}).get(metric, {})
            if direction == "minimum":
                reference_value = reference[metric]["median"]
            else:
                reference_value = (
                    reference[metric].get("window_p95", {}).get("p95")
                    or reference[metric]["p95"]
                )
            default_budget_name = (
                "throughput_regression_percent"
                if direction == "minimum"
                else "p95_latency_regression_percent"
            )
            budget = metric_policy.get(
                "regression_percent", thresholds[default_budget_name]
            ) / 100
            limit = reference_value * (1 - budget if direction == "minimum" else 1 + budget)
            if metric in metadata_latency_metric_set:
                limit = max(limit, thresholds["metadata_p95_absolute_floor_ms"])
            violated = actual < limit if direction == "minimum" else actual > limit
            if violated:
                default_soft_factor = (
                    throughput_soft_factor if direction == "minimum" else latency_soft_factor
                )
                soft_limit = limit * metric_policy.get(
                    "soft_limit_factor", default_soft_factor
                )
                soft_violated = actual < soft_limit if direction == "minimum" else actual > soft_limit
                finding = {
                    "metric": metric,
                    "actual": actual,
                    "hard_limit": limit,
                    "soft_limit": soft_limit,
                    "deviation_percent": abs(actual - limit) / limit * 100,
                }
                if soft_violated:
                    violations.append(finding)
                else:
                    warnings.append(finding)
        if violations:
            status = "fail"
        elif warnings:
            status = "warning"
    return {
        "status": status,
        "valid_runs": len(valid_runs),
        "violations": violations,
        "warnings": warnings,
    }


for environment in manifest["environments"]:
    baseline = json.loads(Path(environment["baseline"]).read_text(encoding="utf-8"))
    accepted = (
        baseline.get("status") == "accepted"
        and baseline.get("capture_runs", 0) >= manifest["minimum_capture_runs"]
        and baseline.get("capture_windows", 0) >= manifest["minimum_capture_windows"]
    )
    if accepted:
        row = evaluate_runs(
            baseline,
            sorted(results_root.glob(f'{environment["id"]}-run-*.json')),
        )
    else:
        row = {"status": "baseline_missing", "valid_runs": 0, "violations": [], "warnings": []}
    retryable = (
        row["status"] == "fail"
        and row["violations"]
        and all("hard_limit" in finding for finding in row["violations"])
    )
    row["supplemental_eligible"] = retryable
    if supplemental_roots and retryable:
        supplemental_tests = [
            evaluate_runs(
                baseline,
                sorted(root.glob(f'{environment["id"]}-run-*.json')),
            )
            for root in supplemental_roots
        ]
        row["initial_status"] = row["status"]
        row["initial_violations"] = row["violations"]
        row["initial_warnings"] = row["warnings"]
        row["supplemental_tests"] = supplemental_tests
        accepted_test = next(
            (test for test in supplemental_tests if test["status"] in ("pass", "warning")),
            None,
        )
        if accepted_test is not None:
            row.update(accepted_test)
    row["environment"] = environment["id"]
    status = row["status"]
    if status == "warning":
        has_warnings = True
    if status not in ("pass", "warning"):
        complete = False
    rows.append(row)

document = {
    "schema_version": 1,
    "status": "pass_with_warnings" if complete and has_warnings else "pass" if complete else "fail",
    "soft_threshold_policy": soft_policy,
    "environments": rows,
}
Path(args.output).write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
raise SystemExit(0 if complete else 2)
