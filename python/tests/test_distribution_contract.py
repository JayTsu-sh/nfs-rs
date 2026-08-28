from __future__ import annotations

import ast
import importlib.metadata
import pathlib
import re
import sys
import sysconfig
import platform

import pytest


ROOT = pathlib.Path(__file__).parents[2]


def test_public_stub_and_typing_marker_cover_every_stable_export() -> None:
    import nfs_rs

    marker = ROOT / "python/nfs_rs/py.typed"
    stub = ROOT / "python/nfs_rs/__init__.pyi"
    assert marker.is_file()
    tree = ast.parse(stub.read_text(encoding="utf-8"))
    stub_names = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    }
    stub_names.update(
        target.id
        for node in tree.body
        if isinstance(node, ast.AnnAssign) and isinstance((target := node.target), ast.Name)
    )
    assert set(nfs_rs.__all__) == stub_names
    assert not any(name.startswith("_") and name != "__version__" for name in nfs_rs.__all__)

    for class_name in ("Client", "AsyncClient", "File", "AsyncFile"):
        class_node = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name)
        constructor = next(
            node for node in class_node.body if isinstance(node, ast.FunctionDef) and node.name == "__init__"
        )
        assert [argument.arg for argument in constructor.args.args] == ["self", "_private"]
        assert ast.unparse(constructor.args.args[1].annotation) == "NoReturn"


def test_python_distribution_version_is_coupled_to_rust_package() -> None:
    import nfs_rs

    manifest = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    package = manifest.split("[package]", 1)[1].split("[", 1)[0]
    rust_version = re.search(r'^version\s*=\s*"([^"]+)"', package, re.MULTILINE)
    assert rust_version is not None
    try:
        installed_version = importlib.metadata.version("nfs-rs")
    except importlib.metadata.PackageNotFoundError:
        assert nfs_rs.__version__ == "0+unknown"
    else:
        assert nfs_rs.__version__ == installed_version == rust_version.group(1)


def test_extension_load_error_preserves_cause_and_platform_guidance(monkeypatch: pytest.MonkeyPatch) -> None:
    from nfs_rs import _client

    cause = OSError("ELFCLASS mismatch")

    def fail_import(name: str):
        assert name == "nfs_rs._internal"
        raise cause

    monkeypatch.setattr(_client, "_import_module", fail_import)
    with pytest.raises(ImportError) as caught:
        _client._adapter()
    assert caught.value.__cause__ is cause
    message = str(caught.value)
    for context in (
        "CPython 3.10+", "Linux/glibc", "source distribution",
        platform.python_implementation(), platform.python_version(), sys.platform,
        platform.machine(), str(sysconfig.get_config_var("SOABI")),
    ):
        assert context in message


def test_private_extension_has_no_public_stub_or_export() -> None:
    import nfs_rs

    assert "_internal" not in nfs_rs.__all__
    assert not (ROOT / "python/nfs_rs/_internal.pyi").exists()
