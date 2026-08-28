from __future__ import annotations

import json
import pathlib


ROOT = pathlib.Path(__file__).parents[2]


def test_real_protocol_runner_is_fail_closed_and_complete() -> None:
    runner = (ROOT / "tests/lab/run-python-artifact-matrix.sh").read_text(encoding="utf-8")
    expected_cases = {
        "linux-source-v3|3|",
        "dxn-v40|4.0|",
        "linux-source-v41|4.1|",
        "netapp-pnfs-mds|4.1|",
    }
    assert all(case in runner for case in expected_cases)
    assert "netapp-pnfs-mds|4.1|" in runner and "|true\"" in runner
    assert "check-python-performance.py" in runner
    assert "continue-on-error" not in runner


def test_release_validation_requires_both_x86_artifacts_and_aarch64() -> None:
    workflow = (ROOT / ".github/workflows/release-validation.yml").read_text(encoding="utf-8")
    assert workflow.count("run-python-artifact-matrix.sh") == 3
    assert "x86_64-wheel.json" in workflow
    assert "x86_64-sdist-wheel.json" in workflow
    assert "runs-on: [self-hosted, linux, ARM64, terrasync-lab, nfs-rs]" in workflow
    assert "aarch64-wheel.json" in workflow
    assert 'aarch64-wheel.json" smoke' in workflow
    assert "continue-on-error" not in workflow


def test_accepted_baseline_encodes_five_of_four_and_ten_percent_policy() -> None:
    baseline = json.loads(
        (ROOT / "tests/python/performance-baselines.json").read_text(encoding="utf-8")
    )
    assert baseline["status"] == "accepted"
    assert baseline["policy"] == {
        "comparable_runs": 5,
        "minimum_valid_runs": 4,
        "maximum_regression_percent": 10,
    }
    assert set(baseline["cases"]) == {
        "linux-source-v3",
        "dxn-v40",
        "linux-source-v41",
        "netapp-pnfs-mds",
    }
