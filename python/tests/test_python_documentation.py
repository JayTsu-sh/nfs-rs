from __future__ import annotations

import ast
import re
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _python_blocks(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    return re.findall(r"```python\n(.*?)```", text, flags=re.DOTALL)


def test_pypi_uses_python_specific_readme() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["project"]["readme"] == "README-PYPI.md"

    readme = (ROOT / "README-PYPI.md").read_text(encoding="utf-8")
    assert "```rust" not in readme
    assert "from nfs_rs import Client" in readme
    assert "from nfs_rs import AsyncClient" in readme
    assert 'versions=["4.1", "4.0", "3"]' in readme
    assert "NFSv3 is the default" in readme
    assert "NFSv4.2 are rejected" in readme


def test_python_documentation_examples_parse() -> None:
    paths = (ROOT / "README-PYPI.md", ROOT / "docs/python-api.md")
    blocks = [block for path in paths for block in _python_blocks(path)]
    assert len(blocks) >= 12
    for block in blocks:
        ast.parse(block)


def test_documented_client_methods_exist_in_public_stub() -> None:
    stub = ast.parse((ROOT / "python/nfs_rs/__init__.pyi").read_text(encoding="utf-8"))
    classes = {
        node.name: {item.name for item in node.body if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))}
        for node in stub.body
        if isinstance(node, ast.ClassDef)
    }
    expected = {
        "Client": {"connect", "stat", "scandir", "mkdir", "open", "setxattr", "drain_recovery_events"},
        "AsyncClient": {"connect", "scandir", "mkdir", "open", "write_bytes", "drain_recovery_events"},
        "File": {"read", "read_at", "write", "write_at", "flush"},
        "AsyncFile": {"read", "read_at", "write", "write_at", "flush"},
    }
    for class_name, methods in expected.items():
        assert methods <= classes[class_name]
