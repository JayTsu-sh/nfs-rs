from __future__ import annotations

import json
import os
import platform
import re
import sys
from pathlib import Path
from typing import Any


_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")


def sanitized(value: object, *, fallback: str) -> str:
    result = _SAFE.sub("-", str(value)).strip("-.")
    return result[:160] or fallback


def failure_context(nodeid: str, parameters: dict[str, Any]) -> dict[str, str]:
    return {
        "test": sanitized(nodeid, fallback="unknown-test"),
        "protocol": sanitized(parameters.get("protocol", "4.1"), fallback="unknown"),
        "environment": sanitized(
            os.environ.get("NFS_RS_CONTRACT_ENVIRONMENT", "test-support-wheel"),
            fallback="unknown",
        ),
        "seed": sanitized(os.environ.get("NFS_RS_TEST_SEED", "0"), fallback="0"),
        "barrier_phase": sanitized(parameters.get("phase", "none"), fallback="none"),
        "operation": sanitized(parameters.get("operation", "none"), fallback="none"),
        "python": sanitized(platform.python_version(), fallback="unknown"),
        "platform": sanitized(sys.platform, fallback="unknown"),
        "architecture": sanitized(platform.machine(), fallback="unknown"),
    }


def write_failure_artifact(
    directory: Path, nodeid: str, parameters: dict[str, Any], _failure: str,
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    context = failure_context(nodeid, parameters)
    # Never persist pytest's free-form failure text: assertion values, URLs,
    # headers, and third-party errors can all contain credentials. The
    # allowlisted fields above are sufficient to reproduce deterministic gates.
    context["result"] = "failed"
    destination = directory / f"{context['test']}.json"
    destination.write_text(json.dumps(context, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return destination
