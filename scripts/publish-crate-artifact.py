#!/usr/bin/env python3
"""Upload an already packaged .crate without rebuilding it."""
from __future__ import annotations

import argparse
import json
import os
import struct
import subprocess
import urllib.request
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("crate", type=Path)
    parser.add_argument("--api", default="https://crates.io/api/v1/crates/new")
    arguments = parser.parse_args()
    token = os.environ.get("CARGO_REGISTRY_TOKEN")
    if not token:
        raise SystemExit("CARGO_REGISTRY_TOKEN is required")
    metadata = json.loads(subprocess.run(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
        check=True, capture_output=True, text=True,
    ).stdout)
    package = next(item for item in metadata["packages"] if item["name"] == "nfs-rs")
    publish = {
        "name": package["name"], "vers": package["version"],
        "deps": [{
            "name": dependency["name"], "version_req": dependency["req"],
            "features": dependency["features"], "optional": dependency["optional"],
            "default_features": dependency["uses_default_features"],
            "target": dependency["target"], "kind": dependency["kind"] or "normal",
            "registry": dependency["registry"],
            "explicit_name_in_toml": dependency.get("rename"),
        } for dependency in package["dependencies"]],
        "features": package["features"], "authors": package["authors"],
        "description": package["description"], "documentation": package["documentation"],
        "homepage": package["homepage"],
        "readme": Path(package["readme"]).read_text(encoding="utf-8") if package["readme"] else None,
        "keywords": package["keywords"], "categories": package["categories"],
        "license": package["license"], "license_file": package["license_file"],
        "repository": package["repository"], "badges": {}, "links": package["links"],
        "rust_version": package["rust_version"],
    }
    encoded = json.dumps(publish, separators=(",", ":")).encode()
    crate = arguments.crate.read_bytes()
    body = struct.pack("<I", len(encoded)) + encoded + struct.pack("<I", len(crate)) + crate
    request = urllib.request.Request(
        arguments.api, data=body, method="PUT",
        headers={"Authorization": token, "Content-Type": "application/octet-stream",
                 "User-Agent": "nfs-rs-release"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        if response.status not in {200, 201}:
            raise SystemExit(f"crates.io upload failed with HTTP {response.status}")


if __name__ == "__main__":
    main()
