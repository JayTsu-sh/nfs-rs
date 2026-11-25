from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import sys

import pytest


ROOT = pathlib.Path(__file__).parents[2]


def test_real_protocol_runner_is_fail_closed_and_complete() -> None:
    runner = (ROOT / "tests/lab/run-python-artifact-matrix.sh").read_text(encoding="utf-8")
    expected_cases = {
        "linux-source-v3|3|",
        "dxn-v40|4.0|",
        "linux-source-v41|4.1|",
        "ots-pnfs-mds|4.1|",
    }
    matrix = (ROOT / "tests/lab/python-ots.sh").read_text(encoding="utf-8")
    assert "run_python_lab_matrix" in runner
    assert all(case in matrix for case in expected_cases)
    assert "ots-pnfs-mds|4.1|" in matrix and "|true\"" in matrix
    assert "check-python-performance.py" in runner
    assert "continue-on-error" not in runner


def test_release_validation_requires_both_x86_artifacts_without_an_arm_lab() -> None:
    workflow = (ROOT / ".github/workflows/release-validation.yml").read_text(encoding="utf-8")
    assert workflow.count("run-python-artifact-matrix.sh") == 2
    assert "x86_64-wheel.json" in workflow
    assert "x86_64-sdist-wheel.json" in workflow
    assert "runs-on: [self-hosted, linux, ARM64, terrasync-lab, nfs-rs]" not in workflow
    assert "aarch64-wheel.json" not in workflow
    assert "continue-on-error" not in workflow


def test_build_and_release_workflows_are_x86_64_only() -> None:
    files = [
        ROOT / ".github/workflows/ci.yml",
        ROOT / ".github/workflows/release-validation.yml",
        ROOT / ".github/workflows/release.yml",
        ROOT / "scripts/verify-python-artifacts.py",
        ROOT / "README.md",
    ]
    combined = "\n".join(path.read_text(encoding="utf-8") for path in files).lower()
    assert "aarch64" not in combined
    assert "arm64" not in combined
    assert "ubuntu-24.04-arm" not in combined


def test_nightly_runs_public_python_api_conformance_on_every_real_protocol() -> None:
    workflow = (ROOT / ".github/workflows/nightly.yml").read_text(encoding="utf-8")
    runner = (ROOT / "tests/lab/run-python-api-conformance.sh").read_text(
        encoding="utf-8"
    )

    assert "run-python-api-conformance.sh" in workflow
    assert "python-api-conformance" in workflow
    assert "--features python-test-support" not in workflow
    assert "continue-on-error" not in workflow
    assert "run_python_lab_matrix" in runner
    assert "maturin build --release --locked --compatibility manylinux2014 --zig" in workflow
    assert "NFS_RS_PYTHON_CONFORMANCE_WHEEL" in runner
    assert "NFS_RS_PYTHON_CONFORMANCE_WHEEL" in workflow


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
        "ots-pnfs-mds",
    }


@pytest.mark.parametrize(
    ("direct_exit", "stage", "ots_exit", "protocol_failure", "expected_stage", "expected_type"),
    [
        (0, "validate", 0, False, None, None),
        (0, "workspace", 255, False, "workspace", "TransportError"),
        (0, "upload", 255, False, "upload", "TransportError"),
        (0, "fetch", 255, False, "fetch", "TransportError"),
        (0, "validate", 1, True, "validate", "RuntimeError"),
        (0, "validate", 0, True, "validate", "RuntimeError"),
        (1, "validate", 0, False, "validate", "ProcessError"),
    ],
)
def test_matrix_requires_successful_evidence(
    tmp_path, direct_exit, stage, ots_exit, protocol_failure, expected_stage, expected_type
):
    run_id = "release-test"
    direct = {"run_id": run_id, "results": [
        {"case": {"name": name}, "valid_runs": 4, "failures": ["timeout in one sample"]}
        for name in ("linux-source-v3", "dxn-v40", "linux-source-v41")
    ]}
    (tmp_path / "direct.json").write_text(json.dumps(direct), encoding="utf-8")
    if stage == "validate":
        ots = {"run_id": run_id, "results": [{"case": {"name": "ots-pnfs-mds"}}]}
        if protocol_failure:
            ots.update(
                results=[],
                failures=["ots-pnfs-mds: required pNFS capability was not negotiated"],
                failure_details=[{
                    "case": "ots-pnfs-mds",
                    "stage": "validate",
                    "type": "RuntimeError",
                    "message": "required pNFS capability was not negotiated",
                }],
            )
        (tmp_path / "ots.json").write_text(json.dumps(ots), encoding="utf-8")
    output = tmp_path / "merged.json"
    result = subprocess.run(
        [
            sys.executable, str(ROOT / "tests/lab/merge-python-lab-results.py"),
            run_id, str(tmp_path), str(output), str(direct_exit), stage, str(ots_exit),
            str(int(protocol_failure)), "0",
        ],
        check=False, capture_output=True, text=True,
    )

    evidence = json.loads(output.read_text(encoding="utf-8"))
    assert evidence["results"][:3] == direct["results"]
    assert result.returncode == int(expected_stage is not None)
    if expected_stage is None:
        assert evidence["status"] == "passed"
        assert not evidence["failure_details"]
        assert len(evidence["results"]) == 4
    else:
        assert evidence["status"] == "failed"
        (detail,) = evidence["failure_details"]
        assert detail["stage"] == expected_stage
        assert detail["type"] == expected_type


def test_ots_proxy_command_launches_under_openssh(tmp_path):
    openssh = shutil.which("ssh")
    assert openssh is not None
    key = tmp_path / "key"
    key.write_text("test key", encoding="utf-8")
    known_hosts = tmp_path / "known_hosts"
    known_hosts.touch()
    proxy_log = tmp_path / "proxy-arguments"
    proxy = tmp_path / "ssh"
    proxy.write_text('#!/bin/bash\nprintf "%s\\n" "$@" > "$PROXY_ARGUMENTS"\n', encoding="utf-8")
    proxy.chmod(0o700)

    subprocess.run(
        [
            "bash", "-c",
            """
            set -euo pipefail
            source "$1/tests/lab/common.sh"
            source "$1/tests/lab/ots-executor.sh"
            ots_configure_ssh
            "$2" -F /dev/null "${ots_ssh_options[@]}" -o "ProxyCommand=$ots_proxy_command" "$ots_node_host" true
            """,
            "test", str(ROOT), openssh,
        ],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "LAB_OTS_JUMP": "127.0.0.1",
            "LAB_OTS_NODE": "127.0.0.2",
            "LAB_OTS_SSH_USER": "root",
            "LAB_OTS_SSH_KEY": str(key),
            "LAB_OTS_KNOWN_HOSTS": str(known_hosts),
            "PROXY_ARGUMENTS": str(proxy_log),
        },
        check=False, capture_output=True, timeout=10,
    )
    proxy_arguments = proxy_log.read_text(encoding="utf-8").splitlines()
    assert proxy_arguments[-4:] == ["StrictHostKeyChecking=yes", "-W", "127.0.0.2:22", "root@127.0.0.1"]
