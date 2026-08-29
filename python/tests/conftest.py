from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from contract_artifacts import write_failure_artifact


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[Any]):
    outcome = yield
    report = outcome.get_result()
    if report.when != "call" or not report.failed:
        return
    parameters = dict(getattr(getattr(item, "callspec", None), "params", {}))
    artifact_root = Path(
        os.environ.get("NFS_RS_FAILURE_ARTIFACT_DIR", "target/python-failure-artifacts")
    )
    write_failure_artifact(artifact_root, item.nodeid, parameters, str(report.longrepr))
