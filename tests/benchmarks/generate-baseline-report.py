#!/usr/bin/env python3
import argparse
import json
import statistics
from pathlib import Path


def load(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * fraction + 0.999999) - 1))
    return ordered[index]


parser = argparse.ArgumentParser()
parser.add_argument("--manifest", required=True)
parser.add_argument("--results-dir")
parser.add_argument("--gate-result")
parser.add_argument("--output-dir", required=True)
args = parser.parse_args()

manifest = load(args.manifest)
output = Path(args.output_dir)
output.mkdir(parents=True, exist_ok=True)
results_root = Path(args.results_dir) if args.results_dir else None
gate = load(args.gate_result) if args.gate_result else None
gate_by_environment = {
    row["environment"]: row for row in (gate or {}).get("environments", [])
}
rows = []
complete = True

for environment in manifest["environments"]:
    baseline = load(environment["baseline"])
    accepted = (
        baseline.get("status") == "accepted"
        and baseline.get("capture_runs", 0) >= manifest["minimum_capture_runs"]
        and baseline.get("capture_windows", 0) >= manifest["minimum_capture_windows"]
    )
    if not accepted:
        complete = False
    current_runs = []
    if results_root:
        result_paths = sorted(results_root.glob(f'{environment["id"]}*.json'))
        current_runs = [load(path) for path in result_paths]
    gate_row = gate_by_environment.get(environment["id"])
    row_status = "accepted" if accepted else "baseline_missing"
    if gate_row:
        row_status = gate_row["status"]
        if row_status != "pass":
            complete = False
    rows.append({
        "id": environment["id"],
        "endpoint": environment["endpoint"],
        "protocol": environment["protocol"],
        "status": row_status,
        "capture_runs": baseline.get("capture_runs", 0),
        "baseline": baseline,
        "current_runs": current_runs,
        "gate": gate_row,
    })

document = {
    "schema_version": 1,
    "status": "complete" if complete else "baseline_missing",
    "minimum_capture_runs": manifest["minimum_capture_runs"],
    "minimum_capture_windows": manifest["minimum_capture_windows"],
    "environments": rows,
}
(output / "performance-baselines.json").write_text(
    json.dumps(document, indent=2) + "\n", encoding="utf-8"
)

lines = [
    "# Performance baseline report",
    "",
    f'Overall status: `{document["status"]}`',
    "",
    "| Environment | Endpoint | Protocol | Status | Capture runs | Current write MiB/s | Current read MiB/s |",
    "|---|---|---:|---|---:|---:|---:|",
]
for row in rows:
    summaries = [lif.get("summary", {}) for run in row["current_runs"] for lif in run.get("lifs", [])]
    write_values = [summary["write_median_mib_s"] for summary in summaries if "write_median_mib_s" in summary]
    read_values = [summary["read_median_mib_s"] for summary in summaries if "read_median_mib_s" in summary]
    write = statistics.median(write_values) if write_values else "—"
    read = statistics.median(read_values) if read_values else "—"
    write = f"{write:.3f}" if isinstance(write, (int, float)) else write
    read = f"{read:.3f}" if isinstance(read, (int, float)) else read
    lines.append(
        f'| {row["id"]} | `{row["endpoint"]}` | {row["protocol"]} | '
        f'`{row["status"]}` | {row["capture_runs"]} | {write} | {read} |'
    )
lines.extend([
    "",
    "An environment remains `baseline_missing` until its independent baseline "
    "has the required number of accepted capture runs.",
    "",
])
latency_metrics = [
    "mount_ms", "umount_ms", "null_ms", "fsinfo_ms", "fsstat_ms", "mkdir_ms", "create_ms",
    "lookup_ms", "getattr_ms", "access_ms", "pathconf_ms", "write_ms",
    "commit_ms", "close_ms", "open_ms", "read_ms", "rename_ms",
    "link_ms", "symlink_ms", "readlink_ms", "readdir_ms", "remove_ms",
    "rmdir_ms",
]
lines.extend(["## Per-interface latency", ""])
for row in rows:
    baseline_metrics = row["baseline"].get("benchmarks", {}).get("storage_path", {})
    current_samples = [
        sample
        for run in row["current_runs"]
        for lif in run.get("lifs", [])
        for sample in lif.get("samples", [])
    ]
    lines.extend([
        f'### {row["id"]}', "",
        "| Interface | Baseline p95 ms | Current p95 ms | Current status |",
        "|---|---:|---:|---|",
    ])
    for metric in latency_metrics:
        reference = baseline_metrics.get(metric, {}).get("p95")
        if metric in ("mount_ms", "umount_ms"):
            current_values = [
                lif[metric]
                for run in row["current_runs"]
                for lif in run.get("lifs", [])
                if metric in lif
            ]
        else:
            current_values = [
                sample[metric]
                for sample in current_samples
                if isinstance(sample.get(metric), (int, float))
            ]
        current = percentile(current_values, 0.95)
        reference_text = f"{reference:.3f}" if isinstance(reference, (int, float)) else "—"
        current_text = f"{current:.3f}" if isinstance(current, (int, float)) else "—"
        status_key = f"{metric.removesuffix('_ms')}_status"
        statuses = sorted({sample[status_key] for sample in current_samples if status_key in sample})
        status_text = "; ".join(statuses) if statuses else ("pass" if current is not None else "—")
        lines.append(
            f"| {metric.removesuffix('_ms').upper()} | {reference_text} | "
            f"{current_text} | {status_text} |"
        )
    lines.append("")
(output / "performance-baselines.md").write_text("\n".join(lines), encoding="utf-8")
raise SystemExit(0 if complete else 2)
