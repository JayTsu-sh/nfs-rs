#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
import tomllib
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    arguments = parser.parse_args()
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:[a-zA-Z0-9.-]+)?", arguments.tag):
        parser.error("release tag must be v<semver>")
    version = arguments.tag.removeprefix("v")
    cargo = tomllib.loads((arguments.root / "Cargo.toml").read_text(encoding="utf-8"))
    lock = tomllib.loads((arguments.root / "Cargo.lock").read_text(encoding="utf-8"))
    root_lock = next(
        package for package in lock["package"]
        if package["name"] == cargo["package"]["name"]
    )
    if cargo["package"]["version"] != version or root_lock["version"] != version:
        raise SystemExit("tag, Cargo.toml, and Cargo.lock versions must match")
    changelog = (arguments.root / "CHANGELOG.md").read_text(encoding="utf-8")
    if f"## [{version}]" not in changelog:
        raise SystemExit("CHANGELOG.md has no section for the release version")
    exact_tag = subprocess.run(
        ["git", "describe", "--exact-match", "--tags", "HEAD"],
        cwd=arguments.root, capture_output=True, text=True, check=True,
    ).stdout.strip()
    if exact_tag != arguments.tag:
        raise SystemExit(f"checked-out commit is {exact_tag}, expected {arguments.tag}")
    print(version)


if __name__ == "__main__":
    main()
