from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HARNESS = Path(__file__).with_name("perf_compare.py")
sys.path.insert(0, str(HARNESS.parent))


def _run(tmp_path: Path, *suite: str) -> dict:
    out = tmp_path / "out.json"
    common = [sys.executable, str(HARNESS), "--target", str(tmp_path), "--io", "buffered",
              "--workdir", "w", "--json", str(out), "--smoke"]
    subprocess.run(common + list(suite), check=True, capture_output=True, text=True)
    return json.loads(out.read_text())


def test_metadata_suite_series_names(tmp_path: Path) -> None:
    report = _run(tmp_path, "metadata")
    assert report["harness"] == "python" and report["backend"] == "posix"
    assert [s["name"] for s in report["results"]] == [
        "mkdir", "create", "stat", "access", "chmod", "rename", "remove", "rmdir", "readdir"]
    assert report["results"][0]["samples"] and report["results"][0]["ops_s"] > 0


def test_data_suite_small_and_large(tmp_path: Path) -> None:
    small = _run(tmp_path, "data", "--size", "4k", "--qd", "1")
    assert small["results"][0]["name"] == "write_ms"
    large = _run(tmp_path, "data", "--size", "40m", "--qd", "8")
    assert [s["name"] for s in large["results"]] == ["write", "read", "read_hot"]
    assert large["results"][1]["median"] > 0
    assert large["results"][2]["reference_only"] is True


def test_multiclient_and_cleanup(tmp_path: Path) -> None:
    report = _run(tmp_path, "multiclient", "--size", "40m", "--clients", "2", "--mode", "distinct")
    assert report["results"][0]["name"] == "aggregate_read"
    assert len(report["results"][1]["samples"]) == 2
    assert report["peak_rss_kib"] > 0
    assert not any(p.name == "w" for p in tmp_path.iterdir())


def test_pattern_matches_rust_convention() -> None:
    import perf_compare

    assert perf_compare.PATTERN[0] == 29 and perf_compare.PATTERN[1] == 46
    assert perf_compare.verify(perf_compare.CHUNK * 3 + 10, perf_compare.PATTERN[10:100])
    assert not perf_compare.verify(1, perf_compare.PATTERN[:10])
    assert perf_compare.percentile([5.0, 1.0, 3.0, 2.0, 4.0], 0.5) == 3.0
