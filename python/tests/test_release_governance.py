from __future__ import annotations

import os
import pathlib
import struct
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


ROOT = pathlib.Path(__file__).parents[2]


def test_tag_release_publishes_only_downloaded_immutable_artifacts() -> None:
    workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    assert 'tags: ["v[0-9]+.[0-9]+.[0-9]+"]' in workflow
    assert "uses: ./.github/workflows/release-validation.yml" in workflow
    assert "actions/download-artifact@v5" in workflow
    assert "actions/attest-build-provenance@v3" in workflow
    assert "gh-action-pypi-publish@release/v1" in workflow
    assert "rust-lang/crates-io-auth-action@v1" in workflow
    assert "publish-crate-artifact.py" in workflow
    publish_section = workflow.split("  publish-pypi:", 1)[1]
    assert "maturin build" not in publish_section
    assert "cargo package" not in publish_section
    assert "cargo publish" not in publish_section
    assert "continue-on-error" not in workflow


def test_release_preflight_covers_audits_checksums_and_partial_recovery() -> None:
    workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    for required in (
        "verify-release-version.py",
        "cargo audit",
        "sha256sum",
        "check-release-registries.py",
        "publish_pypi",
        "publish_crate",
    ):
        assert required in workflow
    registry = (ROOT / "scripts/check-release-registries.py").read_text(encoding="utf-8")
    assert "conflicting bytes" in registry
    assert 'publish_pypi = False' in registry
    assert 'publish_crate = False' in registry


def test_release_gates_rust_quality_and_intermediate_python_smoke() -> None:
    validation = (ROOT / ".github/workflows/release-validation.yml").read_text(
        encoding="utf-8"
    )
    for required in (
        "cargo fmt --all -- --check",
        "cargo clippy --all-targets --locked -- -D warnings",
        'python-version: ["3.11", "3.12", "3.13"]',
        "python-test-support-x86_64",
        "scripts/smoke-python-artifact.py",
    ):
        assert required in validation


def test_python_documentation_covers_first_release_contract() -> None:
    guide = (ROOT / "docs/python-api.md").read_text(encoding="utf-8").lower()
    for required in (
        "synchronous workflow",
        "async workflow",
        "py.typed",
        "binary modes",
        "privileged source port",
        "large transfers",
        "cancellation",
        "nfsuncertainoutcomeerror",
        "recovery_events",
        "experimental",
        "kerberos",
        "rpcsec_gss",
    ):
        assert required in guide
    metadata = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert '"Typing :: Typed"' in metadata
    assert '"Operating System :: POSIX :: Linux"' in metadata


def test_crates_publisher_uploads_the_supplied_bytes_without_repackaging(tmp_path) -> None:
    crate = tmp_path / "nfs-rs-0.5.1.crate"
    crate.write_bytes(b"exact-tested-crate-bytes")
    captured: dict[str, bytes] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self) -> None:  # noqa: N802
            captured["body"] = self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    try:
        environment = os.environ.copy()
        environment["CARGO_REGISTRY_TOKEN"] = "redacted-test-token"
        environment["NO_PROXY"] = "127.0.0.1"
        environment["no_proxy"] = "127.0.0.1"
        subprocess.run(
            [sys.executable, str(ROOT / "scripts/publish-crate-artifact.py"), str(crate),
             "--api", f"http://127.0.0.1:{server.server_port}/api/v1/crates/new"],
            cwd=ROOT, env=environment, check=True,
        )
    finally:
        thread.join(timeout=2)
        server.server_close()
    body = captured["body"]
    metadata_size = struct.unpack("<I", body[:4])[0]
    crate_size_offset = 4 + metadata_size
    crate_size = struct.unpack("<I", body[crate_size_offset:crate_size_offset + 4])[0]
    assert body[crate_size_offset + 4:] == crate.read_bytes()
    assert crate_size == crate.stat().st_size
