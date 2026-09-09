#!/usr/bin/env python3
"""Validate concrete reliability mappings against successful Rust test execution."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re


def check_results(manifest: dict, output: str) -> list[str]:
    passed = set(re.findall(r"^test (\S+) \.\.\. ok$", output, re.MULTILINE))
    if not passed:
        raise ValueError("no successful Rust tests found in execution log")
    outstanding = []
    for entry in manifest["tests"] + manifest.get("review_regressions", []):
        status = entry["ci_status"]
        names = entry["ci_tests"]
        if status not in {"mapped", "partial", "unmapped"}:
            raise ValueError(f"{entry['id']}: invalid ci_status {status!r}")
        if not isinstance(names, list) or any(not isinstance(name, str) or not name for name in names):
            raise ValueError(f"{entry['id']}: ci_tests must contain test names")
        if status != "unmapped" and not names:
            raise ValueError(f"{entry['id']}: mapped coverage requires concrete tests")
        if status == "unmapped" and names:
            raise ValueError(f"{entry['id']}: unmapped coverage cannot list concrete tests")
        for name in names:
            if name not in passed:
                raise ValueError(f"{entry['id']}: mapped test did not pass (missing, renamed, ignored or failed): {name}")
        if status != "mapped":
            outstanding.append(f"{entry['id']}: {status} — {entry['ci']}")
    return outstanding


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("tests/nfs41-reliability-coverage.json"))
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    try:
        outstanding = check_results(json.loads(args.manifest.read_text()), args.results.read_text())
    except (ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"reliability mapping failed: {error}\n")
    print("All concrete reliability test mappings passed.")
    for item in outstanding:
        print(f"Outstanding coverage: {item}")


if __name__ == "__main__":
    main()
