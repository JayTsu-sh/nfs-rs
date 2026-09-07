#!/usr/bin/env python3
"""Render nfs-perf-compare JSON results into a Markdown and an HTML report."""
from __future__ import annotations

import argparse
import html
import json
import math
import statistics
from collections import Counter
from pathlib import Path
from typing import Any

PROTOCOLS = ("3", "4.0", "4.1")
HARNESSES = ("rust", "python")
METADATA_OPS = ("mkdir", "create", "stat", "access", "chmod", "rename", "remove", "rmdir", "readdir")


# ----------------------------------------------------------------------------
# loading / lookup
# ----------------------------------------------------------------------------

def load_results(results_dir: Path) -> tuple[list[dict[str, Any]], dict[str, str]]:
    reports = []
    for path in sorted(results_dir.rglob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(data, dict) and data.get("schema") == 1:
            data["_file"] = path.name
            data["_host"] = _host(data.get("target", ""))
            reports.append(data)
    failures: dict[str, str] = {}
    for path in results_dir.rglob("failures.txt"):
        for line in path.read_text(encoding="utf-8").splitlines():
            name, _, reason = line.partition("\t")
            if name:
                failures[name.strip()] = reason.strip()
    return reports, failures


def _host(target: str) -> str:
    if target.startswith("nfs://"):
        return target[6:].split("/", 1)[0].split("?", 1)[0]
    return "mount"


def select(reports: list[dict[str, Any]], **filters: Any) -> list[dict[str, Any]]:
    out = []
    for r in reports:
        ok = True
        for key, value in filters.items():
            if key.startswith("p_"):
                actual = r.get("params", {}).get(key[2:])
            else:
                actual = r.get(key)
            if actual != value:
                ok = False
                break
        if ok:
            out.append(r)
    return out


def series(report: dict[str, Any] | None, name: str) -> dict[str, Any] | None:
    if report is None:
        return None
    for s in report.get("results", []):
        if s.get("name") == name:
            return s
    return None


def one(reports: list[dict[str, Any]]) -> dict[str, Any] | None:
    return reports[0] if reports else None


def fmt(value: float | None, digits: int = 2) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "N/A"
    return f"{value:.{digits}f}"


def ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    if math.isnan(numerator) or math.isnan(denominator):
        return None
    return numerator / denominator


# ----------------------------------------------------------------------------
# column definitions
# ----------------------------------------------------------------------------

def posix_filters(harness: str, variant: str, io: str | None) -> dict[str, Any]:
    f: dict[str, Any] = {"harness": harness, "backend": "posix", "mount_variant": variant}
    if io is not None:
        f["io_mode"] = io
    return f


def metadata_columns() -> list[tuple[str, dict[str, Any]]]:
    cols = []
    for h in HARNESSES:
        cols.append((f"{h}-nfsrs", {"harness": h, "backend": "nfsrs"}))
        cols.append((f"{h}-kernel default", posix_filters(h, "default", None)))
        cols.append((f"{h}-kernel lookupcache=none", posix_filters(h, "nolookup", None)))
    return cols


def data_columns() -> list[tuple[str, dict[str, Any], str]]:
    """(label, filters, series-name-suffix) — suffix 'hot' selects read_hot."""
    cols = []
    for h in HARNESSES:
        cols.append((f"{h}-nfsrs", {"harness": h, "backend": "nfsrs"}, ""))
        cols.append((f"{h}-kernel O_DIRECT", posix_filters(h, "default", "direct"), ""))
        cols.append((f"{h}-kernel buffered 冷", posix_filters(h, "default", "buffered"), ""))
        cols.append((f"{h}-kernel buffered 热*", posix_filters(h, "default", "buffered"), "hot"))
    return cols


# ----------------------------------------------------------------------------
# document model
# ----------------------------------------------------------------------------

class Doc:
    def __init__(self, title: str) -> None:
        self.title = title
        self.blocks: list[tuple[str, Any]] = []

    def h(self, level: int, text: str) -> None:
        self.blocks.append(("h", (level, text)))

    def p(self, text: str) -> None:
        self.blocks.append(("p", text))

    def table(self, header: list[str], rows: list[list[str]]) -> None:
        self.blocks.append(("table", (header, rows)))

    def raw_md(self, text: str) -> None:
        self.blocks.append(("md", text))

    def to_markdown(self) -> str:
        out = [f"# {self.title}", ""]
        for kind, payload in self.blocks:
            if kind == "h":
                level, text = payload
                out += ["#" * level + " " + text, ""]
            elif kind == "p":
                out += [payload, ""]
            elif kind == "table":
                header, rows = payload
                out.append("| " + " | ".join(header) + " |")
                out.append("|" + "|".join("---" for _ in header) + "|")
                out += ["| " + " | ".join(r) + " |" for r in rows]
                out.append("")
            elif kind == "md":
                out += [payload.rstrip(), ""]
        return "\n".join(out)

    def to_html(self) -> str:
        parts = [
            "<!doctype html><html lang=\"zh\"><head><meta charset=\"utf-8\">",
            f"<title>{html.escape(self.title)}</title>",
            "<style>body{font-family:system-ui,sans-serif;max-width:1200px;margin:2rem auto;padding:0 1rem;color:#222}"
            "table{border-collapse:collapse;margin:1rem 0;font-size:0.9rem}th,td{border:1px solid #ccc;padding:4px 8px;text-align:right}"
            "th:first-child,td:first-child{text-align:left}th{background:#f3f3f3}h2{border-bottom:2px solid #ddd;padding-bottom:4px}"
            "code{background:#f5f5f5;padding:1px 4px}</style></head><body>",
            f"<h1>{html.escape(self.title)}</h1>",
        ]
        for kind, payload in self.blocks:
            if kind == "h":
                level, text = payload
                parts.append(f"<h{level}>{html.escape(text)}</h{level}>")
            elif kind == "p":
                parts.append(f"<p>{_inline(payload)}</p>")
            elif kind == "table":
                header, rows = payload
                parts.append("<table><thead><tr>" + "".join(f"<th>{_inline(c)}</th>" for c in header) + "</tr></thead><tbody>")
                for r in rows:
                    parts.append("<tr>" + "".join(f"<td>{_inline(c)}</td>" for c in r) + "</tr>")
                parts.append("</tbody></table>")
            elif kind == "md":
                parts.append(_md_to_html(payload))
        parts.append("</body></html>")
        return "\n".join(parts)


def _inline(text: str) -> str:
    escaped = html.escape(text)
    while "`" in escaped:
        start = escaped.find("`")
        end = escaped.find("`", start + 1)
        if end < 0:
            break
        escaped = escaped[:start] + "<code>" + escaped[start + 1:end] + "</code>" + escaped[end + 1:]
    while "**" in escaped:
        start = escaped.find("**")
        end = escaped.find("**", start + 2)
        if end < 0:
            break
        escaped = escaped[:start] + "<strong>" + escaped[start + 2:end] + "</strong>" + escaped[end + 2:]
    return escaped


def _md_to_html(text: str) -> str:
    out: list[str] = []
    in_list = False
    in_table: list[str] = []

    def flush_table() -> None:
        nonlocal in_table
        if in_table:
            rows = [r.strip().strip("|").split("|") for r in in_table if not set(r.strip().strip("|").replace("|", "")) <= set("-: ")]
            if rows:
                out.append("<table><thead><tr>" + "".join(f"<th>{_inline(c.strip())}</th>" for c in rows[0]) + "</tr></thead><tbody>")
                for r in rows[1:]:
                    out.append("<tr>" + "".join(f"<td>{_inline(c.strip())}</td>" for c in r) + "</tr>")
                out.append("</tbody></table>")
            in_table = []

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("|"):
            in_table.append(stripped)
            continue
        flush_table()
        if stripped.startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{_inline(stripped[2:])}</li>")
            continue
        if in_list:
            out.append("</ul>")
            in_list = False
        if stripped.startswith("#"):
            level = len(stripped) - len(stripped.lstrip("#"))
            out.append(f"<h{level}>{_inline(stripped[level:].strip())}</h{level}>")
        elif stripped:
            out.append(f"<p>{_inline(stripped)}</p>")
    flush_table()
    if in_list:
        out.append("</ul>")
    return "\n".join(out)


# ----------------------------------------------------------------------------
# sections
# ----------------------------------------------------------------------------

def metadata_ratio(reports: list[dict[str, Any]], protocol: str, harness: str, variant: str) -> float | None:
    """Geometric mean over ops of kernel_p50 / nfsrs_p50 (1.0 = parity, >1 = nfs-rs faster)."""
    nfs = one(select(reports, protocol=protocol, harness=harness, backend="nfsrs", suite="metadata"))
    ker = one(select(reports, protocol=protocol, suite="metadata", **posix_filters(harness, variant, None)))
    logs = []
    for op in METADATA_OPS:
        a, b = series(nfs, op), series(ker, op)
        if a and b and a.get("p50", 0) > 0 and b.get("p50", 0) > 0:
            logs.append(math.log(b["p50"] / a["p50"]))
    return math.exp(statistics.fmean(logs)) if logs else None


def data_value(reports: list[dict[str, Any]], protocol: str, filters: dict[str, Any], size: str, qd: int,
               direction: str, hot: bool = False) -> float | None:
    r = one(select(reports, protocol=protocol, suite="data", p_size=size, p_qd=qd, **filters))
    if r is None:
        return None
    if size == "4k":
        name = f"{direction}_hot_ms" if hot else f"{direction}_ms"
        s = series(r, name)
        return s.get("p50") if s else None
    name = f"{direction}_hot" if hot else direction
    s = series(r, name)
    return s.get("median") if s else None


def multiclient_value(reports: list[dict[str, Any]], protocol: str, filters: dict[str, Any], mode: str) -> float | None:
    r = one(select(reports, protocol=protocol, suite="multiclient", p_mode=mode, **filters))
    s = series(r, "aggregate_read")
    return s.get("median") if s else None


def summary_section(doc: Doc, reports: list[dict[str, Any]]) -> None:
    doc.h(2, "执行摘要")
    doc.p("所有比值定义为 **nfs-rs 相对内核挂载的相对性能**：1.00 = 持平，0.50 = nfs-rs 慢一倍，2.00 = nfs-rs 快一倍。"
          "元数据取 9 种操作 p50 延迟比（内核/nfs-rs）的几何平均；数据路径取吞吐比（nfs-rs/内核 O_DIRECT）；"
          "多客户端取 8 路聚合吞吐比（nfs-rs/内核 O_DIRECT）。")
    header = ["协议", "元数据 (rust)", "元数据 (python)", "1 GiB 写 QD8 (rust)", "1 GiB 读 QD8 (rust)",
              "1 GiB 写 QD8 (python)", "1 GiB 读 QD8 (python)", "8 客户端同文件 (rust)", "8 客户端不同文件 (rust)"]
    rows = []
    for proto in PROTOCOLS:
        if not select(reports, protocol=proto):
            continue
        row = [proto]
        row.append(fmt(metadata_ratio(reports, proto, "rust", "default")))
        row.append(fmt(metadata_ratio(reports, proto, "python", "default")))
        for h in HARNESSES:
            for direction in ("write", "read"):
                n = data_value(reports, proto, {"harness": h, "backend": "nfsrs"}, "1g", 8, direction)
                k = data_value(reports, proto, posix_filters(h, "default", "direct"), "1g", 8, direction)
                row.append(fmt(ratio(n, k)))
        for mode in ("same", "distinct"):
            n = multiclient_value(reports, proto, {"harness": "rust", "backend": "nfsrs"}, mode)
            k = multiclient_value(reports, proto, posix_filters("rust", "default", "direct"), mode)
            row.append(fmt(ratio(n, k)))
        rows.append(row)
    doc.table(header, rows)


def environment_section(doc: Doc, reports: list[dict[str, Any]]) -> None:
    doc.h(2, "环境")
    first = one(reports)
    env = first.get("env", {}) if first else {}
    hosts = Counter(r["_host"] for r in reports if r["backend"] == "nfsrs")
    versions = {r.get("env", {}).get("nfs_rs_version") for r in reports}
    rows = [
        ["客户端", f"{env.get('hostname', 'N/A')} / kernel {env.get('kernel', 'N/A')}"],
        ["nfs-rs 版本", ", ".join(sorted(str(v) for v in versions if v)) or "N/A"],
        ["提交", str(env.get("commit") or "N/A")],
        ["NFS 数据 LIF", ", ".join(f"{h} ({n} 次)" for h, n in hosts.most_common()) or "N/A"],
        ["协商 rsize / wsize", f"{env.get('rsize', 'N/A')} / {env.get('wsize', 'N/A')}"],
        ["用例数", f"{len(reports)} 个 JSON"],
    ]
    doc.table(["项", "值"], rows)


def metadata_section(doc: Doc, reports: list[dict[str, Any]], proto: str) -> None:
    doc.h(3, "元数据操作延迟（ms，p50 / p95）")
    cols = metadata_columns()
    header = ["操作"] + [c[0] for c in cols]
    rows = []
    for op in METADATA_OPS:
        row = [op]
        for _, filters in cols:
            s = series(one(select(reports, protocol=proto, suite="metadata", **filters)), op)
            row.append(f"{fmt(s['p50'], 3)} / {fmt(s['p95'], 3)}" if s else "N/A")
        rows.append(row)
    doc.table(header, rows)


def data_section(doc: Doc, reports: list[dict[str, Any]], proto: str) -> None:
    doc.h(3, "数据读写（4 KiB：p50 延迟 ms；40 MiB / 1 GiB：中位吞吐 MiB/s）")
    cols = data_columns()
    header = ["负载"] + [c[0] for c in cols]
    rows = []
    for size in ("4k", "40m", "1g"):
        for qd in (1, 8):
            if size == "4k" and qd == 8:
                continue
            for direction in ("write", "read"):
                label = f"{size} QD{qd} {direction}"
                row = [label]
                for _, filters, suffix in cols:
                    hot = suffix == "hot"
                    if hot and direction == "write":
                        row.append("—")
                        continue
                    row.append(fmt(data_value(reports, proto, filters, size, qd, direction, hot)))
                rows.append(row)
    doc.table(header, rows)
    doc.p("\\* 热读：同一文件紧接冷读之后再读一次，内核侧命中 page cache，仅作参考，不作为结论依据。")


def multiclient_section(doc: Doc, reports: list[dict[str, Any]], proto: str) -> None:
    doc.h(3, "8 路独立客户端并发读 1 GiB（聚合 MiB/s，中位数）")
    cols = []
    for h in HARNESSES:
        cols.append((f"{h}-nfsrs", {"harness": h, "backend": "nfsrs"}))
        cols.append((f"{h}-kernel O_DIRECT", posix_filters(h, "default", "direct")))
        cols.append((f"{h}-kernel buffered", posix_filters(h, "default", "buffered")))
    header = ["模式"] + [c[0] for c in cols]
    rows = []
    for mode, label in (("same", "同一文件"), ("distinct", "各自文件")):
        row = [label]
        for _, filters in cols:
            row.append(fmt(multiclient_value(reports, proto, filters, mode)))
        rows.append(row)
    doc.table(header, rows)


def rss_section(doc: Doc, reports: list[dict[str, Any]]) -> None:
    doc.h(2, "峰值内存（VmHWM，MiB；1 GiB QD8 数据用例 / 8 客户端同文件 worker 最大值）")
    header = ["配置"] + [f"v{p}" for p in PROTOCOLS]
    rows = []
    for h in HARNESSES:
        for label, filters in ((f"{h}-nfsrs", {"harness": h, "backend": "nfsrs"}),
                               (f"{h}-kernel O_DIRECT", posix_filters(h, "default", "direct")),
                               (f"{h}-kernel buffered", posix_filters(h, "default", "buffered"))):
            row = [label]
            for proto in PROTOCOLS:
                d = one(select(reports, protocol=proto, suite="data", p_size="1g", p_qd=8, **filters))
                m = one(select(reports, protocol=proto, suite="multiclient", p_mode="same", **filters))
                dv = d["peak_rss_kib"] / 1024 if d else None
                mv = m["peak_rss_kib"] / 1024 if m else None
                row.append(f"{fmt(dv, 0)} / {fmt(mv, 0)}")
            rows.append(row)
    doc.table(header, rows)


def crosscheck_section(doc: Doc, reports: list[dict[str, Any]], main_host: str) -> None:
    others = [r for r in reports if r["_host"] not in (main_host, "mount") or r.get("mount_variant") == "lif-b"]
    if not others:
        return
    doc.h(2, "交叉验证（第二个 LIF）")
    header = ["协议", "配置", "1 GiB QD8 写", "1 GiB QD8 读"]
    rows = []
    for r in sorted(others, key=lambda r: (r.get("protocol") or "", r["harness"], r["backend"])):
        if r.get("suite") != "data":
            continue
        w, rd = series(r, "write"), series(r, "read")
        rows.append([str(r.get("protocol")), f"{r['harness']}-{r['backend']} ({r['_host']})",
                     fmt(w.get("median") if w else None), fmt(rd.get("median") if rd else None)])
    doc.table(header, rows)


def failures_section(doc: Doc, failures: dict[str, str]) -> None:
    if not failures:
        return
    doc.h(2, "失败用例（表中显示为 N/A）")
    doc.table(["用例文件", "原因"], [[k, v] for k, v in sorted(failures.items())])


def build(reports: list[dict[str, Any]], failures: dict[str, str], title: str, notes: str | None) -> Doc:
    doc = Doc(title)
    summary_section(doc, reports)
    environment_section(doc, reports)
    main_host = Counter(r["_host"] for r in reports if r["backend"] == "nfsrs").most_common(1)
    main_host_name = main_host[0][0] if main_host else ""
    main = [r for r in reports if r["_host"] in (main_host_name, "mount") and r.get("mount_variant") != "lif-b"]
    doc.h(2, "结果")
    for proto in PROTOCOLS:
        if not select(main, protocol=proto):
            continue
        doc.h(2, f"NFSv{proto}")
        metadata_section(doc, main, proto)
        data_section(doc, main, proto)
        multiclient_section(doc, main, proto)
    rss_section(doc, main)
    crosscheck_section(doc, reports, main_host_name)
    failures_section(doc, failures)
    if notes:
        doc.raw_md(notes)
    return doc


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--md", type=Path, required=True)
    parser.add_argument("--html", type=Path)
    parser.add_argument("--title", default="nfs-rs vs 内核 NFS 挂载性能对比（FAS2750）")
    parser.add_argument("--notes", type=Path, help="Markdown appended verbatim (analysis, limitations)")
    args = parser.parse_args()
    reports, failures = load_results(args.results_dir)
    notes = args.notes.read_text(encoding="utf-8") if args.notes else None
    doc = build(reports, failures, args.title, notes)
    args.md.write_text(doc.to_markdown(), encoding="utf-8")
    if args.html:
        args.html.write_text(doc.to_html(), encoding="utf-8")
    print(f"{len(reports)} results, {len(failures)} failures -> {args.md}")


if __name__ == "__main__":
    main()
