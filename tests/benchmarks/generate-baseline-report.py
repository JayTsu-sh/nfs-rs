#!/usr/bin/env python3
import argparse
import json
import statistics
from html import escape
from pathlib import Path


def load(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * fraction + 0.999999) - 1))
    return ordered[index]


def baseline_metric(row, metric, statistic="median"):
    value = (
        row["baseline"]
        .get("benchmarks", {})
        .get("storage_path", {})
        .get(metric, {})
        .get(statistic)
    )
    return value if isinstance(value, (int, float)) else None


def percent_faster(faster, slower):
    if not isinstance(faster, (int, float)) or not isinstance(slower, (int, float)) or slower == 0:
        return None
    return ((faster / slower) - 1) * 100


def display_number(value):
    return f"{value:.3f}" if isinstance(value, (int, float)) else "—"


parser = argparse.ArgumentParser()
parser.add_argument("--manifest", required=True)
parser.add_argument("--results-dir")
parser.add_argument("--supplemental-results-dir", action="append", default=[])
parser.add_argument("--gate-result")
parser.add_argument("--output-dir", required=True)
args = parser.parse_args()

manifest = load(args.manifest)
output = Path(args.output_dir)
output.mkdir(parents=True, exist_ok=True)
results_root = Path(args.results_dir) if args.results_dir else None
supplemental_roots = [Path(path) for path in args.supplemental_results_dir]
gate = load(args.gate_result) if args.gate_result else None
gate_by_environment = {
    row["environment"]: row for row in (gate or {}).get("environments", [])
}
rows = []
complete = True
has_warnings = False
latency_metrics = [
    "mount_ms", "umount_ms", "null_ms", "fsinfo_ms", "fsstat_ms", "mkdir_ms", "create_ms",
    "lookup_ms", "getattr_ms", "access_ms", "pathconf_ms", "write_ms",
    "commit_ms", "close_ms", "open_ms", "read_ms", "rename_ms",
    "link_ms", "symlink_ms", "readlink_ms", "readdir_ms", "remove_ms",
    "rmdir_ms",
]

for environment in manifest["environments"]:
    baseline = load(environment["baseline"])
    accepted = (
        baseline.get("status") == "accepted"
        and baseline.get("capture_runs", 0) >= manifest["minimum_capture_runs"]
        and baseline.get("capture_windows", 0) >= manifest["minimum_capture_windows"]
    )
    if not accepted:
        complete = False
    gate_row = gate_by_environment.get(environment["id"])
    current_runs = []
    if results_root:
        accepted_round = next(
            (
                index for index, test in enumerate((gate_row or {}).get("supplemental_tests", []))
                if test.get("status") in ("pass", "warning")
            ),
            None,
        )
        current_root = (
            supplemental_roots[accepted_round]
            if accepted_round is not None and accepted_round < len(supplemental_roots)
            else results_root
        )
        result_paths = sorted(current_root.glob(f'{environment["id"]}*.json'))
        current_runs = [load(path) for path in result_paths]
    row_status = "accepted" if accepted else "baseline_missing"
    if gate_row:
        row_status = gate_row["status"]
        if row_status == "warning":
            has_warnings = True
        elif row_status != "pass":
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

write_ranking = sorted(
    [
        {"environment": row["id"], "median_mib_s": baseline_metric(row, "write_mib_s")}
        for row in rows
        if baseline_metric(row, "write_mib_s") is not None
    ],
    key=lambda item: item["median_mib_s"],
    reverse=True,
)
read_ranking = sorted(
    [
        {"environment": row["id"], "median_mib_s": baseline_metric(row, "read_mib_s")}
        for row in rows
        if baseline_metric(row, "read_mib_s") is not None
    ],
    key=lambda item: item["median_mib_s"],
    reverse=True,
)
tail_hotspots = sorted(
    [
        {
            "environment": row["id"],
            "interface": metric.removesuffix("_ms").upper(),
            "p95_ms": baseline_metric(row, metric, "p95"),
            "p99_ms": baseline_metric(row, metric, "p99"),
        }
        for row in rows
        for metric in latency_metrics
        if baseline_metric(row, metric, "p95") is not None
    ],
    key=lambda item: item["p95_ms"],
    reverse=True,
)[:10]
protocol_comparisons = []
for site, environment_ids in [
    ("linux-source", ["linux-source-v3", "linux-source-v40", "linux-source-v41"]),
    (
        "linux-destination",
        ["linux-destination-v3", "linux-destination-v40", "linux-destination-v41"],
    ),
]:
    site_rows = [row for row in rows if row["id"] in environment_ids]
    protocol_comparisons.append({
        "site": site,
        "protocols": [
            {
                "environment": row["id"],
                "protocol": row["protocol"],
                "write_median_mib_s": baseline_metric(row, "write_mib_s"),
                "read_median_mib_s": baseline_metric(row, "read_mib_s"),
            }
            for row in site_rows
        ],
    })
pathconf_groups = {}
for row in rows:
    capability = row["baseline"].get("capabilities", {}).get("pathconf", "unknown")
    pathconf_groups.setdefault(capability, []).append(row["id"])

observations = []
if write_ranking:
    fastest, slowest = write_ranking[0], write_ranking[-1]
    difference = percent_faster(fastest["median_mib_s"], slowest["median_mib_s"])
    if difference is not None:
        observations.append(
            f'{fastest["environment"]} has the highest median write throughput at '
            f'{fastest["median_mib_s"]:.3f} MiB/s, {difference:.1f}% above '
            f'{slowest["environment"]} ({slowest["median_mib_s"]:.3f} MiB/s).'
        )
if read_ranking:
    fastest, slowest = read_ranking[0], read_ranking[-1]
    difference = percent_faster(fastest["median_mib_s"], slowest["median_mib_s"])
    if difference is not None:
        observations.append(
            f'{fastest["environment"]} has the highest median read throughput at '
            f'{fastest["median_mib_s"]:.3f} MiB/s, {difference:.1f}% above '
            f'{slowest["environment"]} ({slowest["median_mib_s"]:.3f} MiB/s).'
        )
rows_by_id = {row["id"]: row for row in rows}
fas_a = rows_by_id.get("fas2750-v40-lif-a")
fas_b = rows_by_id.get("fas2750-v40-lif-b")
if fas_a and fas_b:
    fas_write_difference = percent_faster(
        baseline_metric(fas_a, "write_mib_s"), baseline_metric(fas_b, "write_mib_s")
    )
    fas_read_difference = percent_faster(
        baseline_metric(fas_a, "read_mib_s"), baseline_metric(fas_b, "read_mib_s")
    )
    if fas_write_difference is not None and fas_read_difference is not None:
        observations.append(
            f'FAS2750 LIF A exceeds LIF B by {fas_write_difference:.1f}% for median '
            f'write throughput and {fas_read_difference:.1f}% for median read throughput; '
            "retain per-LIF baselines rather than combining them."
        )
for comparison in protocol_comparisons:
    write_protocols = [
        item for item in comparison["protocols"] if item["write_median_mib_s"] is not None
    ]
    read_protocols = [
        item for item in comparison["protocols"] if item["read_median_mib_s"] is not None
    ]
    if not write_protocols or not read_protocols:
        continue
    fastest_write = max(write_protocols, key=lambda item: item["write_median_mib_s"])
    fastest_read = max(read_protocols, key=lambda item: item["read_median_mib_s"])
    observations.append(
        f'{comparison["site"]}: NFS {fastest_write["protocol"]} leads median writes '
        f'({fastest_write["write_median_mib_s"]:.3f} MiB/s), while NFS '
        f'{fastest_read["protocol"]} leads median reads '
        f'({fastest_read["read_median_mib_s"]:.3f} MiB/s).'
    )
defaulted_pathconf = pathconf_groups.get("pass_with_defaults: case_insensitive", [])
if defaulted_pathconf:
    observations.append(
        "PATHCONF uses the interoperable case-insensitive default on "
        + ", ".join(defaulted_pathconf)
        + "; this is an accepted capability difference, not a benchmark failure."
    )

analysis = {
    "observations": observations,
    "write_throughput_ranking": write_ranking,
    "read_throughput_ranking": read_ranking,
    "protocol_comparisons": protocol_comparisons,
    "tail_latency_hotspots": tail_hotspots,
    "pathconf_capabilities": pathconf_groups,
}

document = {
    "schema_version": 1,
    "status": "complete_with_warnings" if complete and has_warnings else "complete" if complete else "baseline_missing",
    "minimum_capture_runs": manifest["minimum_capture_runs"],
    "minimum_capture_windows": manifest["minimum_capture_windows"],
    "analysis": analysis,
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
    "| Environment | Endpoint | Protocol | Status | Capture runs | Baseline write median MiB/s | Baseline read median MiB/s | Current write MiB/s | Current read MiB/s |",
    "|---|---|---:|---|---:|---:|---:|---:|---:|",
]
for row in rows:
    baseline_metrics = row["baseline"].get("benchmarks", {}).get("storage_path", {})
    baseline_write = baseline_metrics.get("write_mib_s", {}).get("median", "—")
    baseline_read = baseline_metrics.get("read_mib_s", {}).get("median", "—")
    baseline_write = (
        f"{baseline_write:.3f}" if isinstance(baseline_write, (int, float)) else baseline_write
    )
    baseline_read = (
        f"{baseline_read:.3f}" if isinstance(baseline_read, (int, float)) else baseline_read
    )
    summaries = [lif.get("summary", {}) for run in row["current_runs"] for lif in run.get("lifs", [])]
    write_values = [summary["write_median_mib_s"] for summary in summaries if "write_median_mib_s" in summary]
    read_values = [summary["read_median_mib_s"] for summary in summaries if "read_median_mib_s" in summary]
    write = statistics.median(write_values) if write_values else "—"
    read = statistics.median(read_values) if read_values else "—"
    write = f"{write:.3f}" if isinstance(write, (int, float)) else write
    read = f"{read:.3f}" if isinstance(read, (int, float)) else read
    lines.append(
        f'| {row["id"]} | `{row["endpoint"]}` | {row["protocol"]} | '
        f'`{row["status"]}` | {row["capture_runs"]} | {baseline_write} | '
        f'{baseline_read} | {write} | {read} |'
    )
gate_warnings = [
    (row["id"], warning)
    for row in rows
    for warning in (row.get("gate") or {}).get("warnings", [])
]
supplemental_tests = [
    (row["id"], row["gate"])
    for row in rows
    if (row.get("gate") or {}).get("supplemental_tests")
]
if supplemental_tests:
    lines.extend([
        "",
        "## Supplemental performance tests",
        "",
        "Only environments with retryable numeric failures are sampled again; the initial findings remain recorded below.",
        "",
        "| Environment | Round | Initial status | Supplemental status | Final status | Supplemental valid runs |",
        "|---|---:|---|---|---|---:|",
    ])
    for environment, gate_row in supplemental_tests:
        for round_number, supplemental in enumerate(gate_row["supplemental_tests"], 1):
            lines.append(
                f'| {environment} | {round_number} | {gate_row["initial_status"]} | '
                f'{supplemental["status"]} | {gate_row["status"]} | '
                f'{supplemental["valid_runs"]} |'
            )
        for finding in gate_row.get("initial_violations", []):
            lines.append(
                f'| ↳ {finding["metric"]} | — | actual={finding.get("actual")} | '
                f'hard_limit={finding.get("hard_limit", "—")} | '
                f'soft_limit={finding.get("soft_limit", "—")} | — |'
            )
if gate_warnings:
    lines.extend([
        "",
        "## Performance gate warnings",
        "",
        "Values outside the hard limit but inside the 10% environment-jitter soft limit are accepted with warning.",
        "",
        "| Environment | Metric | Actual | hard_limit | soft_limit | deviation_percent |",
        "|---|---|---:|---:|---:|---:|",
    ])
    for environment, warning in gate_warnings:
        lines.append(
            f'| {environment} | {warning["metric"]} | {warning["actual"]:.6f} | '
            f'{warning["hard_limit"]:.6f} | {warning["soft_limit"]:.6f} | '
            f'{warning["deviation_percent"]:.2f}% |'
        )
lines.extend([
    "",
    "An environment remains `baseline_missing` until its independent baseline "
    "has the required number of accepted capture runs.",
    "",
    "## Baseline analysis summary",
    "",
    "### Key observations",
    "",
])
for observation in observations:
    lines.append(f"- {observation}")
lines.extend([
    "",
    "### Write-throughput ranking",
    "",
    "| Rank | Environment | Write median MiB/s |",
    "|---:|---|---:|",
])
for rank, item in enumerate(write_ranking, start=1):
    lines.append(
        f'| {rank} | {item["environment"]} | {item["median_mib_s"]:.3f} |'
    )
lines.extend([
    "",
    "### Read-throughput ranking",
    "",
    "| Rank | Environment | Read median MiB/s |",
    "|---:|---|---:|",
])
for rank, item in enumerate(read_ranking, start=1):
    lines.append(
        f'| {rank} | {item["environment"]} | {item["median_mib_s"]:.3f} |'
    )
lines.extend([
    "",
    "### Linux protocol comparison",
    "",
    "| Site | Protocol | Write median MiB/s | Read median MiB/s |",
    "|---|---:|---:|---:|",
])
for comparison in protocol_comparisons:
    for protocol in comparison["protocols"]:
        lines.append(
            f'| {comparison["site"]} | {protocol["protocol"]} | '
            f'{display_number(protocol["write_median_mib_s"])} | '
            f'{display_number(protocol["read_median_mib_s"])} |'
        )
lines.extend([
    "",
    "### Highest baseline p95 latency observations",
    "",
    "These are ranking observations across unlike operations, not causal diagnoses.",
    "",
    "| Rank | Environment | Interface | p95 ms | p99 ms |",
    "|---:|---|---|---:|---:|",
])
for rank, hotspot in enumerate(tail_hotspots, start=1):
    lines.append(
        f'| {rank} | {hotspot["environment"]} | {hotspot["interface"]} | '
        f'{display_number(hotspot["p95_ms"])} | {display_number(hotspot["p99_ms"])} |'
    )
lines.extend(["", "### PATHCONF capability groups", ""])
for capability, environments in sorted(pathconf_groups.items()):
    lines.append(f'- `{capability}`: {", ".join(environments)}')
lines.append("")
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

html_lines = [
    "<!doctype html>",
    '<html lang="en">',
    "<head>",
    '  <meta charset="utf-8">',
    '  <meta name="viewport" content="width=device-width, initial-scale=1">',
    "  <title>Performance baseline report</title>",
    "  <style>",
    "    :root { color-scheme: light dark; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }",
    "    body { margin: 0; background: #0b1020; color: #e8edf7; }",
    "    main { width: min(1500px, calc(100% - 32px)); margin: 0 auto; padding: 32px 0 64px; }",
    "    h1 { margin-bottom: 8px; } h2 { margin-top: 40px; }",
    "    .summary { color: #aebbd0; margin: 0 0 24px; }",
    "    .status { display: inline-block; padding: 3px 9px; border-radius: 999px; font-weight: 700; }",
    "    .complete, .accepted, .pass { background: #153d2b; color: #70e1a1; }",
    "    .complete_with_warnings, .warning { background: #4b3b12; color: #ffd166; }",
    "    .baseline_missing { background: #4a251d; color: #ff9b82; }",
    "    .table-wrap { overflow-x: auto; border: 1px solid #26324a; border-radius: 10px; }",
    "    table { width: 100%; border-collapse: collapse; background: #11182a; }",
    "    th, td { padding: 9px 12px; border-bottom: 1px solid #26324a; text-align: left; white-space: nowrap; }",
    "    th { background: #172138; color: #b8c7df; position: sticky; top: 0; }",
    "    td.number, th.number { text-align: right; font-variant-numeric: tabular-nums; }",
    "    code { color: #9fc5ff; }",
    "    section.environment { margin-top: 28px; padding: 20px; background: #10182a; border: 1px solid #26324a; border-radius: 12px; }",
    "    .metadata { color: #aebbd0; margin-top: -8px; }",
    "    @media print { body { background: white; color: black; } main { width: 100%; } table, section.environment { background: white; } th { background: #eee; color: black; } }",
    "  </style>",
    "</head>",
    "<body>",
    "<main>",
    "  <h1>Performance baseline report</h1>",
    f'  <p class="summary">Overall status: <span class="status {escape(document["status"])}">{escape(document["status"])}</span> · '
    f'{manifest["minimum_capture_windows"]} windows · {manifest["minimum_capture_runs"]} captures per environment</p>',
    '  <div class="table-wrap"><table>',
    "    <thead><tr><th>Environment</th><th>Endpoint</th><th>Protocol</th><th>Status</th><th class=\"number\">Captures</th><th class=\"number\">Write median MiB/s</th><th class=\"number\">Read median MiB/s</th><th>PATHCONF</th></tr></thead>",
    "    <tbody>",
]
for row in rows:
    baseline_metrics = row["baseline"].get("benchmarks", {}).get("storage_path", {})
    baseline_write = baseline_metrics.get("write_mib_s", {}).get("median")
    baseline_read = baseline_metrics.get("read_mib_s", {}).get("median")
    write_text = f"{baseline_write:.3f}" if isinstance(baseline_write, (int, float)) else "—"
    read_text = f"{baseline_read:.3f}" if isinstance(baseline_read, (int, float)) else "—"
    pathconf = row["baseline"].get("capabilities", {}).get("pathconf", "—")
    html_lines.append(
        "      <tr>"
        f'<td><code>{escape(row["id"])}</code></td>'
        f'<td><code>{escape(row["endpoint"])}</code></td>'
        f'<td>{escape(row["protocol"])}</td>'
        f'<td><span class="status {escape(row["status"])}">{escape(row["status"])}</span></td>'
        f'<td class="number">{row["capture_runs"]}</td>'
        f'<td class="number">{write_text}</td><td class="number">{read_text}</td>'
        f'<td>{escape(pathconf)}</td></tr>'
    )
html_lines.extend([
    "    </tbody>",
    "  </table></div>",
])
if supplemental_tests:
    html_lines.extend([
        "  <h2>Supplemental performance tests</h2>",
        "  <p>Only environments with retryable numeric failures are sampled again; initial findings remain recorded.</p>",
        '  <div class="table-wrap"><table>',
        "    <thead><tr><th>Environment</th><th>Round</th><th>Initial status</th><th>Supplemental status</th><th>Final status</th><th>Supplemental valid runs</th></tr></thead>",
        "    <tbody>",
    ])
    for environment, gate_row in supplemental_tests:
        for round_number, supplemental in enumerate(gate_row["supplemental_tests"], 1):
            html_lines.append(
                f'<tr><td>{escape(environment)}</td><td>{round_number}</td>'
                f'<td>{escape(gate_row["initial_status"])}</td>'
                f'<td>{escape(supplemental["status"])}</td>'
                f'<td>{escape(gate_row["status"])}</td>'
                f'<td>{supplemental["valid_runs"]}</td></tr>'
            )
        for finding in gate_row.get("initial_violations", []):
            html_lines.append(
                f'<tr><td>↳ {escape(finding["metric"])}</td><td>—</td><td>actual={finding.get("actual")}</td>'
                f'<td>hard_limit={finding.get("hard_limit", "—")}</td>'
                f'<td>soft_limit={finding.get("soft_limit", "—")}</td><td>—</td><td>—</td></tr>'
            )
    html_lines.extend(["    </tbody>", "  </table></div>"])
if gate_warnings:
    html_lines.extend([
        "  <h2>Performance gate warnings</h2>",
        "  <p>Values outside the hard limit but inside the 10% environment-jitter soft limit are accepted with warning.</p>",
        '  <div class="table-wrap"><table>',
        "    <thead><tr><th>Environment</th><th>Metric</th><th>Actual</th><th>hard_limit</th><th>soft_limit</th><th>deviation_percent</th></tr></thead>",
        "    <tbody>",
    ])
    for environment, warning in gate_warnings:
        html_lines.append(
            f'<tr><td>{escape(environment)}</td><td>{escape(warning["metric"])}</td>'
            f'<td>{warning["actual"]:.6f}</td><td>{warning["hard_limit"]:.6f}</td>'
            f'<td>{warning["soft_limit"]:.6f}</td><td>{warning["deviation_percent"]:.2f}%</td></tr>'
        )
    html_lines.extend(["    </tbody>", "  </table></div>"])
html_lines.extend([
    "  <h2>Baseline analysis summary</h2>",
    '  <section class="environment">',
    "    <h3>Key observations</h3>",
    "    <ul>",
])
for observation in observations:
    html_lines.append(f"      <li>{escape(observation)}</li>")
html_lines.extend([
    "    </ul>",
    "  </section>",
    '  <section class="environment">',
    "    <h3>Write-throughput ranking</h3>",
    '    <div class="table-wrap"><table>',
    '      <thead><tr><th class="number">Rank</th><th>Environment</th><th class="number">Write median MiB/s</th></tr></thead>',
    "      <tbody>",
])
for rank, item in enumerate(write_ranking, start=1):
    html_lines.append(
        f'        <tr><td class="number">{rank}</td><td><code>{escape(item["environment"])}</code></td>'
        f'<td class="number">{display_number(item["median_mib_s"])}</td></tr>'
    )
html_lines.extend([
    "      </tbody>",
    "    </table></div>",
    "  </section>",
    '  <section class="environment">',
    "    <h3>Read-throughput ranking</h3>",
    '    <div class="table-wrap"><table>',
    '      <thead><tr><th class="number">Rank</th><th>Environment</th><th class="number">Read median MiB/s</th></tr></thead>',
    "      <tbody>",
])
for rank, item in enumerate(read_ranking, start=1):
    html_lines.append(
        f'        <tr><td class="number">{rank}</td><td><code>{escape(item["environment"])}</code></td>'
        f'<td class="number">{display_number(item["median_mib_s"])}</td></tr>'
    )
html_lines.extend([
    "      </tbody>",
    "    </table></div>",
    "  </section>",
    '  <section class="environment">',
    "    <h3>Linux protocol comparison</h3>",
    '    <div class="table-wrap"><table>',
    '      <thead><tr><th>Site</th><th>Protocol</th><th class="number">Write median MiB/s</th><th class="number">Read median MiB/s</th></tr></thead>',
    "      <tbody>",
])
for comparison in protocol_comparisons:
    for protocol in comparison["protocols"]:
        html_lines.append(
            f'        <tr><td>{escape(comparison["site"])}</td><td>{escape(protocol["protocol"])}</td>'
            f'<td class="number">{display_number(protocol["write_median_mib_s"])}</td>'
            f'<td class="number">{display_number(protocol["read_median_mib_s"])}</td></tr>'
        )
html_lines.extend([
    "      </tbody>",
    "    </table></div>",
    "  </section>",
    '  <section class="environment">',
    "    <h3>Highest baseline p95 latency observations</h3>",
    '    <p class="metadata">These are ranking observations across unlike operations, not causal diagnoses.</p>',
    '    <div class="table-wrap"><table>',
    '      <thead><tr><th class="number">Rank</th><th>Environment</th><th>Interface</th><th class="number">p95 ms</th><th class="number">p99 ms</th></tr></thead>',
    "      <tbody>",
])
for rank, hotspot in enumerate(tail_hotspots, start=1):
    html_lines.append(
        f'        <tr><td class="number">{rank}</td><td><code>{escape(hotspot["environment"])}</code></td>'
        f'<td>{escape(hotspot["interface"])}</td><td class="number">{display_number(hotspot["p95_ms"])}</td>'
        f'<td class="number">{display_number(hotspot["p99_ms"])}</td></tr>'
    )
html_lines.extend([
    "      </tbody>",
    "    </table></div>",
    "  </section>",
    '  <section class="environment">',
    "    <h3>PATHCONF capability groups</h3>",
    "    <ul>",
])
for capability, environments in sorted(pathconf_groups.items()):
    html_lines.append(
        f"      <li><code>{escape(capability)}</code>: {escape(', '.join(environments))}</li>"
    )
html_lines.extend([
    "    </ul>",
    "  </section>",
    "  <h2>Per-interface latency</h2>",
])
for row in rows:
    baseline_metrics = row["baseline"].get("benchmarks", {}).get("storage_path", {})
    current_samples = [
        sample
        for run in row["current_runs"]
        for lif in run.get("lifs", [])
        for sample in lif.get("samples", [])
    ]
    html_lines.extend([
        '  <section class="environment">',
        f'    <h3>{escape(row["id"])}</h3>',
        f'    <p class="metadata"><code>{escape(row["endpoint"])}</code> · NFS {escape(row["protocol"])} · {row["capture_runs"]} captures</p>',
        '    <div class="table-wrap"><table>',
        '      <thead><tr><th>Interface</th><th class="number">Baseline p50 ms</th><th class="number">Baseline p95 ms</th><th class="number">Baseline p99 ms</th><th class="number">Current p95 ms</th><th>Current status</th></tr></thead>',
        "      <tbody>",
    ])
    for metric in latency_metrics:
        reference = baseline_metrics.get(metric, {})
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
        current_text = f"{current:.3f}" if isinstance(current, (int, float)) else "—"
        status_key = f"{metric.removesuffix('_ms')}_status"
        statuses = sorted({sample[status_key] for sample in current_samples if status_key in sample})
        status_text = "; ".join(statuses) if statuses else ("pass" if current is not None else "—")
        percentiles = [
            f'{reference[name]:.3f}' if isinstance(reference.get(name), (int, float)) else "—"
            for name in ("p50", "p95", "p99")
        ]
        html_lines.append(
            f'        <tr><td>{escape(metric.removesuffix("_ms").upper())}</td>'
            f'<td class="number">{percentiles[0]}</td><td class="number">{percentiles[1]}</td>'
            f'<td class="number">{percentiles[2]}</td><td class="number">{current_text}</td>'
            f'<td>{escape(status_text)}</td></tr>'
        )
    html_lines.extend(["      </tbody>", "    </table></div>", "  </section>"])
html_lines.extend(["</main>", "</body>", "</html>", ""])
(output / "performance-baselines.html").write_text("\n".join(html_lines), encoding="utf-8")
raise SystemExit(0 if complete else 2)
