#!/usr/bin/env python3
import json
import sys


def load(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def fail(message):
    print(message, file=sys.stderr)
    raise SystemExit(1)


def fail_metric(workload, metric, baseline, actual, limit, direction):
    if direction == "minimum":
        regression = (baseline - actual) / baseline * 100
    else:
        regression = (actual - baseline) / baseline * 100
    fail(
        f"{workload}: {metric} regression exceeds budget; "
        f"baseline={baseline:.6f} actual={actual:.6f} limit={limit:.6f} "
        f"regression_percent={regression:.2f}"
    )


if len(sys.argv) != 3:
    fail("usage: check-nfsv40-performance.py BASELINE CURRENT")

baseline = load(sys.argv[1])
current = load(sys.argv[2])
if baseline.get("status") != "accepted":
    fail("performance baseline is not accepted")
if current.get("liveness") != "pass":
    fail("performance run reported a liveness failure")

thresholds = baseline["thresholds"]
expected_names = {entry["name"] for entry in baseline["workloads"]}
observed = {entry["name"]: entry for entry in current["workloads"]}
if set(observed) != expected_names:
    fail("performance workload matrix is incomplete")

for reference in baseline["workloads"]:
    result = observed[reference["name"]]
    throughput_floor = reference["throughput_mib_s"] * (
        1 - thresholds["throughput_regression_percent"] / 100
    )
    latency_ceiling = reference["write_p95_latency_ms"] * (
        1 + thresholds["p95_latency_regression_percent"] / 100
    )
    workload_latency_ceiling = reference["workload_p95_latency_ms"] * (
        1 + thresholds["p95_latency_regression_percent"] / 100
    )
    rss_ceiling = reference["peak_rss_kib"] * (
        1 + thresholds["peak_rss_regression_percent"] / 100
    )
    if result["throughput_mib_s"] < throughput_floor:
        fail_metric(
            reference["name"], "throughput_mib_s", reference["throughput_mib_s"],
            result["throughput_mib_s"], throughput_floor, "minimum"
        )
    if result["write_p95_latency_ms"] > latency_ceiling:
        fail_metric(
            reference["name"], "write_p95_latency_ms", reference["write_p95_latency_ms"],
            result["write_p95_latency_ms"], latency_ceiling, "maximum"
        )
    if result["workload_p95_latency_ms"] > workload_latency_ceiling:
        fail_metric(
            reference["name"], "workload_p95_latency_ms",
            reference["workload_p95_latency_ms"], result["workload_p95_latency_ms"],
            workload_latency_ceiling, "maximum"
        )
    if result["peak_rss_kib"] > rss_ceiling:
        fail_metric(
            reference["name"], "peak_rss_kib", reference["peak_rss_kib"],
            result["peak_rss_kib"], rss_ceiling, "maximum"
        )

print("NFSv4.0 performance gate passed")
