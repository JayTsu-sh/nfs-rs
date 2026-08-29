#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import urllib.error
import urllib.request
from pathlib import Path


def fetch(url: str) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--github-output", type=Path)
    arguments = parser.parse_args()
    python_files = sorted(arguments.artifacts.glob("*.whl")) + sorted(
        arguments.artifacts.glob("*.tar.gz")
    )
    crate_files = sorted(arguments.artifacts.glob("*.crate"))
    if not python_files or len(crate_files) != 1:
        raise SystemExit("complete Python and Rust artifact sets are required")

    pypi = fetch(f"https://pypi.org/pypi/nfs-rs/{arguments.version}/json")
    if pypi is None:
        publish_pypi = True
    else:
        published = {item["filename"]: item["digests"]["sha256"] for item in pypi["urls"]}
        expected = {path.name: digest(path) for path in python_files}
        conflicts = set(published) ^ set(expected)
        conflicts.update(
            name for name, value in expected.items() if published.get(name) != value
        )
        if conflicts:
            raise SystemExit(f"PyPI version exists with missing/conflicting bytes: {sorted(conflicts)}")
        publish_pypi = False

    crates = fetch(f"https://crates.io/api/v1/crates/nfs-rs/{arguments.version}")
    if crates is None:
        publish_crate = True
    else:
        expected_checksum = digest(crate_files[0])
        if crates["version"]["checksum"] != expected_checksum:
            raise SystemExit("crates.io version exists with conflicting bytes")
        publish_crate = False

    output = arguments.github_output or (
        Path(os.environ["GITHUB_OUTPUT"]) if "GITHUB_OUTPUT" in os.environ else None
    )
    values = {
        "publish_pypi": str(publish_pypi).lower(),
        "publish_crate": str(publish_crate).lower(),
    }
    if output:
        with output.open("a", encoding="utf-8") as stream:
            for name, value in values.items():
                stream.write(f"{name}={value}\n")
    print(json.dumps(values, sort_keys=True))


if __name__ == "__main__":
    main()
