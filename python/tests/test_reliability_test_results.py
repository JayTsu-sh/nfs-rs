from pathlib import Path
import runpy

import pytest

check_results = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/check-reliability-test-results.py"))["check_results"]


def manifest(status="mapped", names=None):
    return {"tests": [{"id": "T01", "ci_status": status, "ci_tests": ["module::case"] if names is None else names, "ci": "wire behavior"}]}


@pytest.mark.parametrize("result", ["ignored", "FAILED", "", "ok-but-not-a-real-result"])
def test_mapping_requires_exact_successful_execution(result):
    with pytest.raises(ValueError):
        check_results(manifest(), f"test unrelated ... ok\ntest module::case ... {result}\n")


def test_mapping_detects_renamed_test_and_reports_unmapped_work():
    assert check_results(manifest(), "test module::case ... ok\n") == []
    with pytest.raises(ValueError, match="did not pass"):
        check_results(manifest(), "test module::renamed ... ok\n")
    assert "unmapped" in check_results(manifest("unmapped", []), "test unrelated ... ok\n")[0]
    with pytest.raises(ValueError, match="concrete"):
        check_results(manifest("mapped", []), "test unrelated ... ok\n")
