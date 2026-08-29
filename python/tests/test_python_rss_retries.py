from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


def load_validator():
    script = Path(__file__).parents[2] / "scripts" / "validate-python-real-protocols.py"
    sys.modules.setdefault("nfs_rs", types.ModuleType("nfs_rs"))
    spec = importlib.util.spec_from_file_location("validate_python_real_protocols", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_rss_failure_is_retried_at_the_case_seam(monkeypatch):
    validator = load_validator()
    outcomes = iter(
        [
            validator.RssPlateauError("first sample did not plateau"),
            {"case": "passed-on-retest"},
        ]
    )
    run_ids: list[str] = []

    def attempt(case, run_id, artifact, payload, runs, minimum_valid_runs):
        run_ids.append(run_id)
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(validator, "validate_case_attempt", attempt)
    result = validator.validate_case(
        validator.Case("linux-source-v3", "3", "nfs://server/export", False),
        "release-test",
        "wheel",
        b"payload",
        5,
        4,
    )

    assert result == {
        "case": "passed-on-retest",
        "rss_gate_attempts": 2,
        "rss_gate_failures": ["first sample did not plateau"],
    }
    assert run_ids == ["release-test", "release-test-rss-retest-1"]


def test_rss_failure_blocks_after_three_independent_retests(monkeypatch):
    validator = load_validator()
    run_ids: list[str] = []

    def attempt(case, run_id, artifact, payload, runs, minimum_valid_runs):
        run_ids.append(run_id)
        raise validator.RssPlateauError(f"{run_id} did not plateau")

    monkeypatch.setattr(validator, "validate_case_attempt", attempt)

    try:
        validator.validate_case(
            validator.Case("linux-source-v3", "3", "nfs://server/export", False),
            "release-test",
            "wheel",
            b"payload",
            5,
            4,
        )
    except validator.RssPlateauError as error:
        assert "failed initial RSS gate and all 3 retests" in str(error)
    else:
        raise AssertionError("RSS gate unexpectedly passed")

    assert run_ids == [
        "release-test",
        "release-test-rss-retest-1",
        "release-test-rss-retest-2",
        "release-test-rss-retest-3",
    ]
