from __future__ import annotations

import json
import pathlib
import subprocess
import sys


ROOT = pathlib.Path(__file__).parents[2]


def _result(value: float, *, valid: int = 5, invalid: int = 0) -> dict:
    return {
        "results": [{
            "case": {"name": "v3"},
            "valid_runs": valid,
            "invalid_runs": invalid,
            "summary": {
                "write_mib_s_median": value,
                "read_mib_s_median": value,
                "write_latency_ms_p95": 10,
                "read_latency_ms_p95": 10,
            },
        }]
    }


def _run(tmp_path: pathlib.Path, result: dict) -> subprocess.CompletedProcess[str]:
    baseline = tmp_path / "baseline.json"
    candidate = tmp_path / "result.json"
    report = tmp_path / "report.json"
    baseline.write_text(json.dumps({
        "status": "accepted",
        "policy": {
            "comparable_runs": 5,
            "minimum_valid_runs": 4,
            "maximum_regression_percent": 10,
        },
        "cases": {"v3": {
            "write_mib_s_median": 10,
            "read_mib_s_median": 10,
            "write_latency_ms_p95": 10,
            "read_latency_ms_p95": 10,
        }},
    }), encoding="utf-8")
    candidate.write_text(json.dumps(result), encoding="utf-8")
    return subprocess.run(
        [sys.executable, str(ROOT / "scripts/check-python-performance.py"),
         "--baseline", str(baseline), "--result", str(candidate), "--output", str(report)],
        capture_output=True, text=True,
    )


def test_python_performance_gate_requires_five_runs_and_four_valid(tmp_path) -> None:
    assert _run(tmp_path, _result(10, valid=4, invalid=1)).returncode == 0
    failed = _run(tmp_path, _result(10, valid=3, invalid=2))
    assert failed.returncode != 0
    assert "at least 4 valid" in failed.stderr


def test_python_performance_gate_blocks_more_than_ten_percent_regression(tmp_path) -> None:
    assert _run(tmp_path, _result(9)).returncode == 0
    failed = _run(tmp_path, _result(8.99))
    assert failed.returncode != 0
    assert "10% regression limit" in failed.stderr
