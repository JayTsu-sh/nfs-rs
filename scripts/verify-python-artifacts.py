#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
import tarfile
import tempfile
import zipfile
from pathlib import Path


ALLOWED_MANYLINUX_LIBRARIES = {
    "libc.so.6",
    "libdl.so.2",
    "libgcc_s.so.1",
    "libm.so.6",
    "libpthread.so.0",
    "librt.so.1",
    "libutil.so.1",
    "ld-linux-x86-64.so.2",
}


def cargo_version(root: Path) -> str:
    manifest = (root / "Cargo.toml").read_text(encoding="utf-8")
    package = manifest.split("[package]", 1)[1].split("[", 1)[0]
    match = re.search(r'^version\s*=\s*"([^"]+)"', package, re.MULTILINE)
    if match is None:
        raise AssertionError("Cargo package version is missing")
    return match.group(1)


def verify_wheel(root: Path, wheel: Path) -> None:
    version = cargo_version(root)
    name = wheel.name
    assert f"-{version}-cp310-abi3-" in name, f"wheel version/ABI mismatch: {name}"
    assert "manylinux_2_17" in name or "manylinux2014" in name, f"not manylinux2014: {name}"
    assert name.endswith("_x86_64.whl"), f"unsupported architecture: {name}"
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        for required in ("nfs_rs/__init__.pyi", "nfs_rs/py.typed", "nfs_rs/_client.py"):
            assert required in names, f"wheel is missing {required}"
        assert not any(name.endswith("_internal.pyi") for name in names)
        extension = next(name for name in names if name.startswith("nfs_rs/_internal") and name.endswith(".so"))
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = archive.read(metadata_name).decode("utf-8")
        assert f"Version: {version}\n" in metadata
        assert "Requires-Dist:" not in metadata, "unexpected mandatory Python dependency"
        with tempfile.TemporaryDirectory() as directory:
            extension_path = Path(directory) / Path(extension).name
            extension_path.write_bytes(archive.read(extension))
            dynamic = subprocess.run(
                ["readelf", "-d", extension_path], check=True, capture_output=True, text=True
            ).stdout
    needed = set(re.findall(r"Shared library: \[([^]]+)]", dynamic))
    unexpected = needed - ALLOWED_MANYLINUX_LIBRARIES
    assert not unexpected, f"unexpected native libraries: {sorted(unexpected)}"


def verify_sdist(root: Path, sdist: Path) -> None:
    version = cargo_version(root)
    assert sdist.name == f"nfs_rs-{version}.tar.gz"
    with tarfile.open(sdist, "r:gz") as archive:
        names = archive.getnames()
    prefix = f"nfs_rs-{version}/"
    required = {
        "Cargo.toml",
        "Cargo.lock",
        "pyproject.toml",
        "build.rs",
        "python/nfs_rs/__init__.py",
        "python/nfs_rs/__init__.pyi",
        "python/nfs_rs/py.typed",
        "src/lib.rs",
    }
    missing = [name for name in required if prefix + name not in names]
    assert not missing, f"sdist is incomplete: {missing}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wheel", type=Path, action="append", default=[])
    parser.add_argument("--sdist", type=Path, action="append", default=[])
    parser.add_argument("--root", type=Path, default=Path.cwd())
    arguments = parser.parse_args()
    assert arguments.wheel or arguments.sdist, "at least one artifact is required"
    for wheel in arguments.wheel:
        verify_wheel(arguments.root, wheel)
    for sdist in arguments.sdist:
        verify_sdist(arguments.root, sdist)


if __name__ == "__main__":
    main()
