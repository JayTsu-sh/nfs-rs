#!/usr/bin/env python3
"""Designed single-file HTML page for the nfs-rs vs kernel comparison.

Reads the same result JSON as report.py and renders inline SVG charts.
usage: report_page.py --results-dir DIR --out page.html [--status "..."] [--notes notes.html]
"""
from __future__ import annotations

import argparse
import html
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import report as R  # noqa: E402

PROTO_LABEL = {"3": "NFSv3", "4.0": "NFSv4.0", "4.1": "NFSv4.1"}
LINK_CEILING = 111.0  # MiB/s, measured 1 Gbps end-to-end ceiling


def e(s: object) -> str:
    return html.escape(str(s))


def fmt(v: float | None, d: int = 1) -> str:
    return "—" if v is None else f"{v:.{d}f}"


# ----------------------------------------------------------------------------
# charts
# ----------------------------------------------------------------------------

def hbar_chart(rows: list[tuple[str, list[float | None]]], series: list[tuple[str, str]], vmax: float,
               unit: str, ceiling: float | None = None, width: int = 720) -> str:
    """Grouped horizontal bars. rows: (label, [value per series]); series: (name, css-class)."""
    label_w, bar_h, gap, group_gap, top = 150, 14, 3, 14, 26
    n = len(series)
    group_h = n * bar_h + (n - 1) * gap
    height = top + len(rows) * (group_h + group_gap) + 24
    plot_w = width - label_w - 70
    x0 = label_w

    def sx(v: float) -> float:
        return x0 + plot_w * min(v, vmax) / vmax

    out = [f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="bar chart">']
    # axis ticks
    ticks = 4
    for i in range(ticks + 1):
        v = vmax * i / ticks
        x = sx(v)
        out.append(f'<line class="grid" x1="{x:.1f}" y1="{top - 6}" x2="{x:.1f}" y2="{height - 20}"/>')
        out.append(f'<text class="tick" x="{x:.1f}" y="{height - 6}" text-anchor="middle">{v:g}</text>')
    out.append(f'<text class="tick unit" x="{x0 + plot_w}" y="{top - 12}" text-anchor="end">{e(unit)}</text>')
    if ceiling is not None and ceiling <= vmax:
        x = sx(ceiling)
        out.append(f'<line class="ceiling" x1="{x:.1f}" y1="{top - 6}" x2="{x:.1f}" y2="{height - 20}"/>')
        out.append(f'<text class="tick ceiling-label" x="{x + 4:.1f}" y="{top - 12}">链路上限 ≈ {ceiling:g}</text>')
    y = top
    for label, values in rows:
        out.append(f'<text class="rowlabel" x="{x0 - 10}" y="{y + group_h / 2 + 4:.1f}" text-anchor="end">{e(label)}</text>')
        for (name, cls), v in zip(series, values):
            if v is None:
                out.append(f'<text class="na" x="{x0 + 4}" y="{y + bar_h - 3}">N/A</text>')
            else:
                w = sx(v) - x0
                out.append(f'<rect class="bar {cls}" x="{x0}" y="{y}" width="{max(w, 1):.1f}" height="{bar_h}"><title>{e(name)}: {v:.2f} {e(unit)}</title></rect>')
                out.append(f'<text class="val" x="{x0 + w + 5:.1f}" y="{y + bar_h - 3}">{v:.{0 if v >= 100 else 1}f}</text>')
            y += bar_h + gap
        y += group_gap
    out.append("</svg>")
    return "\n".join(out)


def legend(series: list[tuple[str, str]]) -> str:
    return '<div class="legend">' + "".join(
        f'<span><i class="sw {cls}"></i>{e(name)}</span>' for name, cls in series) + "</div>"


# ----------------------------------------------------------------------------
# sections
# ----------------------------------------------------------------------------

def ratio_tiles(reports, proto: str) -> str:
    nf = {"harness": "rust", "backend": "nfsrs"}
    kd = R.posix_filters("rust", "default", "direct")
    tiles = [
        ("元数据 p50", R.metadata_ratio(reports, proto, "rust", "default"), "内核 default 挂载，几何平均"),
        ("元数据 p50 vs lookupcache=none", R.metadata_ratio(reports, proto, "rust", "nolookup"), "对齐生产挂载选项"),
        ("1 GiB 写 QD8", R.ratio(R.data_value(reports, proto, nf, "1g", 8, "write"), R.data_value(reports, proto, kd, "1g", 8, "write")), "吞吐比，内核 O_DIRECT"),
        ("1 GiB 读 QD8", R.ratio(R.data_value(reports, proto, nf, "1g", 8, "read"), R.data_value(reports, proto, kd, "1g", 8, "read")), "吞吐比，内核 O_DIRECT"),
        ("1 GiB 读 QD1", R.ratio(R.data_value(reports, proto, nf, "1g", 1, "read"), R.data_value(reports, proto, kd, "1g", 1, "read")), "单路顺序读"),
        ("8 客户端同文件", R.ratio(R.multiclient_value(reports, proto, nf, "same"), R.multiclient_value(reports, proto, R.posix_filters("rust", "default", "buffered"), "same")), "vs 内核 buffered（page cache 共享）"),
    ]
    out = ['<div class="tiles">']
    for name, v, note in tiles:
        cls = "na" if v is None else ("good" if v >= 0.9 else "warn" if v >= 0.5 else "bad")
        out.append(f'<div class="tile {cls}"><div class="tile-name">{e(name)}</div><div class="tile-val">{fmt(v, 2)}</div><div class="tile-note">{e(note)}</div></div>')
    out.append("</div>")
    return "\n".join(out)


def data_chart(reports, proto: str) -> str:
    series = [("nfs-rs (Rust)", "s-nfs"), ("内核 O_DIRECT", "s-ker"), ("内核 buffered 冷读", "s-buf"), ("nfs-rs (Python)", "s-py")]
    filt = [{"harness": "rust", "backend": "nfsrs"}, R.posix_filters("rust", "default", "direct"),
            R.posix_filters("rust", "default", "buffered"), {"harness": "python", "backend": "nfsrs"}]
    rows = []
    for size, sl in (("40m", "40 MiB"), ("1g", "1 GiB")):
        for qd in (1, 8):
            for d, dl in (("write", "写"), ("read", "读")):
                rows.append((f"{sl} QD{qd} {dl}", [R.data_value(reports, proto, f, size, qd, d) for f in filt]))
    return legend(series) + hbar_chart(rows, series, 125, "MiB/s", ceiling=LINK_CEILING)


def small_io_table(reports, proto: str) -> str:
    cols = [("nfs-rs Rust", {"harness": "rust", "backend": "nfsrs"}), ("内核 O_DIRECT", R.posix_filters("rust", "default", "direct")),
            ("内核 buffered", R.posix_filters("rust", "default", "buffered")), ("nfs-rs Python", {"harness": "python", "backend": "nfsrs"}),
            ("内核 O_DIRECT (py)", R.posix_filters("python", "default", "direct"))]
    out = ['<table class="data"><thead><tr><th>4 KiB 单次操作 (ms, p50)</th>' + "".join(f"<th>{e(c)}</th>" for c, _ in cols) + "</tr></thead><tbody>"]
    for d, dl in (("write", "写 + COMMIT/fsync"), ("read", "读（冷）")):
        out.append(f"<tr><td>{dl}</td>" + "".join(f"<td>{fmt(R.data_value(reports, proto, f, '4k', 1, d), 2)}</td>" for _, f in cols) + "</tr>")
    out.append("</tbody></table>")
    return "\n".join(out)


def metadata_chart(reports, proto: str) -> str:
    series = [("nfs-rs (Rust)", "s-nfs"), ("内核 default", "s-ker"), ("内核 lookupcache=none", "s-buf")]
    cols = [R.one(R.select(reports, protocol=proto, harness="rust", backend="nfsrs", suite="metadata")),
            R.one(R.select(reports, protocol=proto, suite="metadata", **R.posix_filters("rust", "default", None))),
            R.one(R.select(reports, protocol=proto, suite="metadata", **R.posix_filters("rust", "nolookup", None)))]
    rows = []
    vmax = 0.0
    for op in R.METADATA_OPS:
        vals = []
        for r in cols:
            s = R.series(r, op)
            v = s.get("p50") if s else None
            vals.append(v)
            if v is not None and op != "readdir":
                vmax = max(vmax, v)
        rows.append((op, vals))
    vmax = max(3.0, round(vmax + 0.5))
    return legend(series) + hbar_chart(rows, series, vmax, "ms (p50)")


def metadata_table(reports, proto: str) -> str:
    cols = R.metadata_columns()
    out = ['<table class="data"><thead><tr><th>操作 (ms, p50 / p95)</th>' + "".join(f"<th>{e(c)}</th>" for c, _ in cols) + "</tr></thead><tbody>"]
    for op in R.METADATA_OPS:
        cells = []
        for _, f in cols:
            s = R.series(R.one(R.select(reports, protocol=proto, suite="metadata", **f)), op)
            cells.append(f"<td>{R.fmt(s['p50'], 2)} <span class='p95'>/ {R.fmt(s['p95'], 2)}</span></td>" if s else "<td>—</td>")
        out.append(f"<tr><td>{op}</td>{''.join(cells)}</tr>")
    out.append("</tbody></table>")
    return "\n".join(out)


def multiclient_chart(reports, proto: str) -> str:
    series = [("nfs-rs (Rust)", "s-nfs"), ("内核 O_DIRECT", "s-ker"), ("内核 buffered", "s-buf"), ("nfs-rs (Python)", "s-py")]
    filt = [{"harness": "rust", "backend": "nfsrs"}, R.posix_filters("rust", "default", "direct"),
            R.posix_filters("rust", "default", "buffered"), {"harness": "python", "backend": "nfsrs"}]
    rows = [(lab, [R.multiclient_value(reports, proto, f, mode) for f in filt]) for mode, lab in (("same", "8 进程读同一文件"), ("distinct", "8 进程各读自己的文件"))]
    vmax = max([v for _, vs in rows for v in vs if v is not None] + [120.0])
    return legend(series) + hbar_chart(rows, series, round(vmax / 100 + 0.5) * 100, "聚合 MiB/s", ceiling=LINK_CEILING)


def memory_table(reports, protos: list[str]) -> str:
    rows = [("nfs-rs Rust", {"harness": "rust", "backend": "nfsrs"}), ("内核 O_DIRECT (Rust)", R.posix_filters("rust", "default", "direct")),
            ("内核 buffered (Rust)", R.posix_filters("rust", "default", "buffered")), ("nfs-rs Python", {"harness": "python", "backend": "nfsrs"}),
            ("内核 (Python)", R.posix_filters("python", "default", "direct"))]
    out = ['<table class="data"><thead><tr><th>峰值 RSS (MiB)，1 GiB QD8 用例</th>' + "".join(f"<th>{PROTO_LABEL[p]}</th>" for p in protos) + "</tr></thead><tbody>"]
    for name, f in rows:
        cells = []
        for p in protos:
            d = R.one(R.select(reports, protocol=p, suite="data", p_size="1g", p_qd=8, **f))
            cells.append(f"<td>{fmt(d['peak_rss_kib'] / 1024, 0) if d else '—'}</td>")
        out.append(f"<tr><td>{e(name)}</td>{''.join(cells)}</tr>")
    out.append("</tbody></table>")
    return "\n".join(out)


def crosscheck(reports) -> str:
    rows = [r for r in reports if r.get("mount_variant") == "lif-b" or (r["backend"] == "nfsrs" and r["_host"].endswith(".201"))]
    if not rows:
        return ""
    out = ['<table class="data"><thead><tr><th>LIF 10.128.61.201 交叉验证</th><th>1 GiB QD8 写</th><th>1 GiB QD8 读</th></tr></thead><tbody>']
    for r in sorted(rows, key=lambda r: (str(r.get("protocol")), r["backend"])):
        w, rd = R.series(r, "write"), R.series(r, "read")
        out.append(f"<tr><td>{PROTO_LABEL.get(str(r.get('protocol')), r.get('protocol'))} · {'nfs-rs' if r['backend']=='nfsrs' else '内核 O_DIRECT'}</td><td>{fmt(w.get('median') if w else None)}</td><td>{fmt(rd.get('median') if rd else None)}</td></tr>")
    out.append("</tbody></table>")
    return "\n".join(out)


CSS = """
:root{--bg:#F4F6F8;--surface:#FFFFFF;--ink:#17202A;--muted:#5B6B7B;--rule:#D9E0E6;--nfs:#0E7C86;--ker:#C4772E;--buf:#8A96A3;--py:#5FA8B0;
--good:#1E7F4F;--warn:#B7791F;--bad:#B23A3A;--tile:#EEF2F5;--ceiling:#B23A3A;--code:#EEF2F5}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#0F1519;--surface:#161D23;--ink:#E4EAEF;--muted:#94A3B1;--rule:#2A343D;--nfs:#3FB4BE;--ker:#E29A55;--buf:#6C7A88;--py:#7FCBD2;--good:#4CC38A;--warn:#E0A94A;--bad:#E06C6C;--tile:#1C252D;--ceiling:#E06C6C;--code:#1C252D}}
:root[data-theme="dark"]{--bg:#0F1519;--surface:#161D23;--ink:#E4EAEF;--muted:#94A3B1;--rule:#2A343D;--nfs:#3FB4BE;--ker:#E29A55;--buf:#6C7A88;--py:#7FCBD2;--good:#4CC38A;--warn:#E0A94A;--bad:#E06C6C;--tile:#1C252D;--ceiling:#E06C6C;--code:#1C252D}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font-family:"IBM Plex Sans","Noto Sans SC",system-ui,sans-serif;font-size:15px;line-height:1.55}
.wrap{max-width:1080px;margin:0 auto;padding:32px 24px 64px}
header{display:grid;grid-template-columns:1fr auto;gap:16px 32px;align-items:end;border-bottom:2px solid var(--ink);padding-bottom:18px;margin-bottom:28px}
h1{font-family:"IBM Plex Sans Condensed","IBM Plex Sans",sans-serif;font-weight:600;font-size:40px;line-height:1.05;margin:0;letter-spacing:-0.01em;text-wrap:balance}
h1 small{display:block;font-size:16px;font-weight:400;color:var(--muted);margin-top:8px;letter-spacing:0}
.meta{font-family:"IBM Plex Mono",monospace;font-size:12.5px;color:var(--muted);display:grid;gap:3px;text-align:right}
.meta b{color:var(--ink);font-weight:500}
.status{display:inline-block;font-family:"IBM Plex Mono",monospace;font-size:12px;padding:3px 8px;border:1px solid var(--rule);border-radius:3px;margin-top:6px}
h2{font-family:"IBM Plex Sans Condensed","IBM Plex Sans",sans-serif;font-weight:600;font-size:26px;margin:44px 0 12px;letter-spacing:-0.005em}
h3{font-size:16px;font-weight:600;margin:28px 0 8px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
p{max-width:68ch}
.lede{font-size:17px;max-width:70ch}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:14px 0 4px}
.tile{background:var(--tile);padding:12px 14px;border-left:3px solid var(--muted)}
.tile.good{border-color:var(--good)}.tile.warn{border-color:var(--warn)}.tile.bad{border-color:var(--bad)}
.tile-name{font-size:12.5px;color:var(--muted)}
.tile-val{font-family:"IBM Plex Mono",monospace;font-size:30px;font-weight:500;line-height:1.15;margin:4px 0 2px;font-variant-numeric:tabular-nums}
.tile-note{font-size:11.5px;color:var(--muted)}
.chartbox{background:var(--surface);border:1px solid var(--rule);padding:14px 16px 8px;overflow-x:auto}
.chart{width:100%;height:auto;display:block;font-family:"IBM Plex Mono",monospace}
.chart .grid{stroke:var(--rule);stroke-width:1}
.chart .tick{fill:var(--muted);font-size:11px}
.chart .unit{font-size:11px}
.chart .rowlabel{fill:var(--ink);font-size:12px;font-family:"IBM Plex Sans","Noto Sans SC",sans-serif}
.chart .val{fill:var(--ink);font-size:11px}
.chart .na{fill:var(--muted);font-size:10px}
.chart .ceiling{stroke:var(--ceiling);stroke-dasharray:4 3;stroke-width:1.2}
.chart .ceiling-label{fill:var(--ceiling)}
.bar{fill:var(--buf)}.s-nfs{fill:var(--nfs)}.s-ker{fill:var(--ker)}.s-buf{fill:var(--buf)}.s-py{fill:var(--py)}
.legend{display:flex;flex-wrap:wrap;gap:6px 18px;font-size:12.5px;color:var(--muted);margin:0 0 8px}
.legend .sw{display:inline-block;width:12px;height:12px;margin-right:6px;vertical-align:-1px;background:var(--buf)}
.legend .s-nfs{background:var(--nfs)}.legend .s-ker{background:var(--ker)}.legend .s-buf{background:var(--buf)}.legend .s-py{background:var(--py)}
.tablebox{overflow-x:auto;margin:10px 0}
table.data{border-collapse:collapse;width:100%;font-size:13.5px;font-variant-numeric:tabular-nums}
table.data th,table.data td{padding:7px 10px;border-bottom:1px solid var(--rule);text-align:right;white-space:nowrap}
table.data th:first-child,table.data td:first-child{text-align:left}
table.data th{font-weight:500;color:var(--muted);font-size:12px;letter-spacing:.03em;border-bottom:1px solid var(--ink)}
table.data td{font-family:"IBM Plex Mono",monospace;font-size:13px}
table.data td:first-child{font-family:"IBM Plex Sans","Noto Sans SC",sans-serif}
.p95{color:var(--muted);font-size:11.5px}
.findings{display:grid;gap:12px;padding:0;margin:12px 0;list-style:none;max-width:78ch}
.findings li{padding:12px 16px;background:var(--surface);border:1px solid var(--rule);border-left:3px solid var(--ker)}
.findings li.ok{border-left-color:var(--good)}
.findings b{display:block;margin-bottom:3px}
code{font-family:"IBM Plex Mono",monospace;font-size:.92em;background:var(--code);padding:1px 5px;border-radius:2px}
.method{columns:2;column-gap:36px;font-size:14px;max-width:none}
.method p{break-inside:avoid;margin:0 0 10px}
.proto-nav{display:flex;gap:8px;flex-wrap:wrap;margin:6px 0 0}
.proto-nav a{font-family:"IBM Plex Mono",monospace;font-size:12.5px;padding:4px 10px;border:1px solid var(--rule);color:var(--ink);text-decoration:none;border-radius:3px}
.proto-nav a:hover,.proto-nav a:focus-visible{border-color:var(--nfs);outline:none}
footer{margin-top:48px;padding-top:14px;border-top:1px solid var(--rule);font-size:12.5px;color:var(--muted);font-family:"IBM Plex Mono",monospace}
@media (max-width:720px){header{grid-template-columns:1fr}.meta{text-align:left}h1{font-size:30px}.method{columns:1}}
@media (prefers-reduced-motion:reduce){*{transition:none!important}}
"""


def build_page(reports, failures, status: str, notes_html: str) -> str:
    protos = [p for p in R.PROTOCOLS if R.select(reports, protocol=p)]
    env = (R.one(reports) or {}).get("env", {})
    parts = [
        '<title>nfs-rs vs 内核挂载</title>',
        '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Condensed:wght@500;600&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&family=Noto+Sans+SC:wght@400;500&display=swap">',
        f"<style>{CSS}</style>",
        '<div class="wrap">',
        "<header><div>",
        '<h1>nfs-rs vs 内核挂载<small>FAS2750 · NFSv3 / v4.0 / v4.1 · 元数据 · 4 KiB / 40 MiB / 1 GiB · Rust 与 Python</small></h1>',
        f'<span class="status">{e(status)}</span>',
        '<nav class="proto-nav">' + "".join(f'<a href="#p{p.replace(".", "")}">{PROTO_LABEL[p]}</a>' for p in protos) + '<a href="#findings">发现</a><a href="#method">方法与限制</a></nav>',
        "</div>",
        f'<div class="meta"><span>客户端 <b>node181</b> · Rocky 9.4 · {e(env.get("kernel", ""))}</span>'
        f'<span>存储 <b>FAS2750</b> ONTAP 9.19.1 · SVM lizy · LIF 10.128.61.200</span>'
        f'<span>nfs-rs <b>{e(env.get("nfs_rs_version", ""))}</b> · rsize/wsize <b>1 MiB</b> · RTT 0.28 ms</span>'
        f'<span>链路实测上限 <b>≈ 110 MiB/s</b>（1 Gbps 端到端）</span>'
        f'<span>2026-09-04 · {len(reports)} 个用例 · {len(failures)} 失败</span></div>',
        "</header>",
        '<p class="lede">同一台客户端、同一个导出、同一套操作序列和计时点：nfs-rs 用户态客户端（Rust API 与 Python 封装）直连 FAS2750，与 Linux 内核 NFS 挂载逐项对比。'
        '所有比值定义为 <b>nfs-rs 相对内核的相对性能</b>：1.00 持平，0.50 表示 nfs-rs 慢一倍。数据路径以内核 <code>O_DIRECT</code> 为对照（与用户态同口径），buffered 冷/热读作参考。</p>',
    ]
    for p in protos:
        pid = "p" + p.replace(".", "")
        parts += [
            f'<h2 id="{pid}">{PROTO_LABEL[p]}</h2>',
            ratio_tiles(reports, p),
            "<h3>数据读写 · 中位吞吐（3 次）</h3>",
            f'<div class="chartbox">{data_chart(reports, p)}</div>',
            f'<div class="tablebox">{small_io_table(reports, p)}</div>',
            "<h3>元数据 · 200 次唯一名字操作，readdir 为 1000 项目录</h3>",
            f'<div class="chartbox">{metadata_chart(reports, p)}</div>',
            f'<div class="tablebox">{metadata_table(reports, p)}</div>',
            "<h3>多客户端 · 8 个独立进程各自完整读 1 GiB（2 次）</h3>",
            f'<div class="chartbox">{multiclient_chart(reports, p)}</div>',
        ]
    parts += [
        '<h2 id="findings">发现</h2>',
        notes_html,
        "<h3>峰值内存</h3>",
        f'<div class="tablebox">{memory_table(reports, protos)}</div>',
    ]
    cc = crosscheck(reports)
    if cc:
        parts += ["<h3>交叉验证</h3>", f'<div class="tablebox">{cc}</div>']
    if failures:
        parts += ["<h3>失败用例</h3>", '<div class="tablebox"><table class="data"><thead><tr><th>用例</th><th>原因</th></tr></thead><tbody>'
                  + "".join(f"<tr><td>{e(k)}</td><td style='text-align:left'>{e(v)}</td></tr>" for k, v in sorted(failures.items())) + "</tbody></table></div>"]
    parts += [
        '<h2 id="method">方法与限制</h2>',
        '<div class="method">',
        '<p><b>Harness。</b><code>src/bin/nfs-perf-compare</code>（Rust）与 <code>tests/benchmarks/compare/perf_compare.py</code>（Python）同 CLI、同 JSON；'
        '<code>--target nfs://…</code> 走 nfs-rs 直连，<code>--target /mnt/…</code> 走内核挂载的 POSIX 调用。内核挂载选项 <code>vers=X,rsize=1048576,wsize=1048576,hard,proto=tcp</code>，元数据另跑一遍 <code>lookupcache=none</code>。</p>'
        '<p><b>数据路径。</b>按 1 MiB 分块写满后 COMMIT / <code>fsync</code>，计时 create→sync；读回按块校验模式数据，校验耗时已扣除。QD8 = 同一文件 8 路 in-flight（nfs-rs 单连接多路 RPC，内核 8 线程 pread/pwrite）。冷读前 <code>sync; echo 3 &gt; drop_caches</code>。</p>'
        '<p><b>元数据。</b>每次迭代用唯一路径（mkdir → create → stat → access → chmod → rename → remove → rmdir），避免内核缓存把重复路径变成零成本；nfs-rs 的 <code>_path</code> 方法每次从根逐级 LOOKUP、无缓存，这是设计差异，如实呈现而不做惩罚性配置。</p>'
        '<p><b>链路。</b>node181 网卡为 10 GbE，但到 FAS 端到端实测 ≈ 110 MiB/s（内核挂载、<code>dd</code>、nfs-rs 一致），因此 QD8 场景三者都打满链路，差异主要看 QD1 与元数据。</p>'
        '<p><b>噪声。</b>node181 同时运行 k3s / ClickHouse / Prometheus（空闲 load ≈ 1.5）；SVM lizy 为多人共用。吞吐取 3 次中位数，延迟报 p50/p95。</p>'
        '<p><b>NFSv4.1 内核挂载。</b>SVM lizy 还有 <code>192.168.13.x</code> 私网 NFS LIF，Linux v4.1 客户端做 session trunking 发现时会尝试连接它们并长时间挂起；测试时在 node181 加了 <code>unreachable</code> 路由让其快速失败。nfs-rs 只连 URL 指定的 LIF，不受影响。</p>'
        '<p><b>存储准备。</b>专用卷 <code>/nfsrs_perf</code>（50 GB，导出策略仅放行 node181，nfs3+nfs4，sec=sys）；SVM <code>tcp_max_transfer_size</code> 由 64 KiB 提到 1 MiB。</p>'
        "</div>",
        f'<footer>nfs-rs {e(env.get("nfs_rs_version", ""))} · commit {e(env.get("commit", ""))} · 原始 JSON：tests/benchmarks/compare/results/2026-09-04/ · 生成：report_page.py</footer>',
        "</div>",
    ]
    return "\n".join(parts)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--status", default="")
    ap.add_argument("--notes", type=Path, help="HTML fragment for the findings section")
    a = ap.parse_args()
    reports, failures = R.load_results(a.results_dir)
    notes = a.notes.read_text(encoding="utf-8") if a.notes else ""
    a.out.write_text(build_page(reports, failures, a.status, notes), encoding="utf-8")
    print(f"{len(reports)} results -> {a.out}")


if __name__ == "__main__":
    main()
