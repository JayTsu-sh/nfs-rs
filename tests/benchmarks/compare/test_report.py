from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import report  # noqa: E402


def _write(dir: Path, name: str, **fields) -> None:
    base = {"schema": 1, "harness": "rust", "protocol": "3", "target": "nfs://10.0.0.1/x?version=3",
            "mount_variant": None, "io_mode": None, "suite": "data", "params": {}, "env": {"hostname": "h"},
            "peak_rss_kib": 2048, "results": []}
    base.update(fields)
    (dir / name).write_text(json.dumps(base))


def _data(write: float, read: float, **fields) -> dict:
    return dict(
        suite="data", params={"size": "1g", "qd": 8},
        results=[{"name": "write", "unit": "MiB/s", "median": write},
                 {"name": "read", "unit": "MiB/s", "median": read}], **fields)


def _meta(p50: float, **fields) -> dict:
    return dict(suite="metadata", params={"iters": 2},
                results=[{"name": op, "unit": "ms", "p50": p50, "p95": 2.0} for op in report.METADATA_OPS],
                **fields)


def test_summary_ratios_and_missing_cells(tmp_path: Path) -> None:
    _write(tmp_path, "a.json", backend="nfsrs", **_data(100.0, 300.0))
    _write(tmp_path, "b.json", backend="posix", target="/mnt/x", mount_variant="default", io_mode="direct",
           **_data(200.0, 150.0))
    _write(tmp_path, "c.json", backend="nfsrs", **_meta(p50=2.0))
    _write(tmp_path, "d.json", backend="posix", target="/mnt/x", mount_variant="default", **_meta(p50=1.0))
    (tmp_path / "failures.txt").write_text("rust-nfsrs-na-na-multiclient-same.json\tworker failed\n")

    reports, failures = report.load_results(tmp_path)
    assert len(reports) == 4 and failures
    doc = report.build(reports, failures, "T", "## 分析\n\n- 一条备注\n")
    md = doc.to_markdown()
    assert "| 3 | 0.50 | N/A | 0.50 | 2.00 |" in md
    assert "1g QD8 write | 100.00 | 200.00 | N/A | — |" in md
    assert "worker failed" in md and "一条备注" in md
    html_text = doc.to_html()
    assert "<table>" in html_text and "<li>一条备注</li>" in html_text
    assert "<script" not in html_text


def test_percentile_free_helpers() -> None:
    assert report.fmt(None) == "N/A"
    assert report.ratio(1.0, 0.0) is None
    assert report._host("nfs://10.1.2.3/exp?version=3") == "10.1.2.3"
    assert report._host("/mnt/x") == "mount"
