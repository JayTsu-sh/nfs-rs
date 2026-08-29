#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


THROUGHPUT_METRICS = ("write_mib_s_median", "read_mib_s_median")
LATENCY_METRICS = ("write_latency_ms_p95", "read_latency_ms_p95")


def evaluate(baseline: dict, result: dict) -> tuple[list[str], list[str]]:
    violations: list[str] = []
    warnings: list[str] = []
    if baseline.get("status") != "accepted":
        return ["Python performance baseline is not accepted"], warnings
    policy = baseline.get("policy", {})
    comparable_runs = policy.get("comparable_runs")
    minimum_valid_runs = policy.get("minimum_valid_runs")
    maximum_regression_percent = policy.get("maximum_regression_percent")
    if (
        not isinstance(comparable_runs, int)
        or not isinstance(minimum_valid_runs, int)
        or not isinstance(maximum_regression_percent, (int, float))
        or comparable_runs < 1
        or not 1 <= minimum_valid_runs <= comparable_runs
        or not 0 < maximum_regression_percent < 100
    ):
        return ["Python performance baseline policy is invalid"], warnings
    throughput_factor = 1 - maximum_regression_percent / 100
    latency_factor = 1 + maximum_regression_percent / 100
    expected = baseline.get("cases", {})
    observed = {entry["case"]["name"]: entry for entry in result.get("results", [])}
    for name, reference in expected.items():
        entry = observed.get(name)
        if entry is None:
            violations.append(f"{name}: required result is missing")
            continue
        valid = entry.get("valid_runs", 0)
        total = valid + entry.get("invalid_runs", 0)
        if valid < minimum_valid_runs or total != comparable_runs:
            violations.append(
                f"{name}: requires {comparable_runs} runs with at least "
                f"{minimum_valid_runs} valid; got {valid}/{total}"
            )
            continue
        summary = entry["summary"]
        for metric in THROUGHPUT_METRICS:
            limit = reference[metric] * throughput_factor
            actual = summary[metric]
            if actual < limit:
                warnings.append(
                    f"{name} {metric}: {actual:.6f} below "
                    f"{maximum_regression_percent:g}% regression limit {limit:.6f}"
                )
        for metric in LATENCY_METRICS:
            limit = reference[metric] * latency_factor
            actual = summary[metric]
            if actual > limit:
                warnings.append(
                    f"{name} {metric}: {actual:.6f} exceeds "
                    f"{maximum_regression_percent:g}% regression limit {limit:.6f}"
                )
    extra = set(observed) - set(expected)
    if extra:
        violations.append(f"unexpected result cases: {sorted(extra)}")
    return violations, warnings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    baseline = json.loads(arguments.baseline.read_text(encoding="utf-8"))
    result = json.loads(arguments.result.read_text(encoding="utf-8"))
    violations, warnings = evaluate(baseline, result)
    status = "fail" if violations else "pass_with_warnings" if warnings else "pass"
    report = {"status": status, "violations": violations, "warnings": warnings}
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if violations:
        raise SystemExit("\n".join(violations))
    if warnings:
        print("\n".join(warnings), file=sys.stderr)


if __name__ == "__main__":
    main()
