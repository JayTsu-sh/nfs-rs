from __future__ import annotations

import json

from contract_artifacts import write_failure_artifact


def test_failure_artifact_is_sanitized_and_reproducible(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NFS_RS_CONTRACT_ENVIRONMENT", "ci runner / secret? no")
    monkeypatch.setenv("NFS_RS_TEST_SEED", "seed:42")
    artifact = write_failure_artifact(
        tmp_path,
        "python/tests/test_contract.py::test_fault[token/value]",
        {"protocol": "4.1", "phase": "after-send-before-response", "operation": "rename"},
        "server=10.1.2.3 token=do not retain",
    )
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    assert payload["protocol"] == "4.1"
    assert payload["environment"] == "ci-runner-secret-no"
    assert payload["seed"] == "seed-42"
    assert payload["barrier_phase"] == "after-send-before-response"
    assert payload["operation"] == "rename"
    assert payload["result"] == "failed"
    assert "failure" not in payload
    assert "10.1.2.3" not in artifact.read_text(encoding="utf-8")
    assert "do not retain" not in artifact.read_text(encoding="utf-8")
    assert "/" not in artifact.name
