from __future__ import annotations

import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _python_blocks(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    return re.findall(r"```python\n(.*?)```", text, flags=re.DOTALL)


def test_pypi_uses_python_specific_readme() -> None:
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert re.search(r'^readme = "README-PYPI\.md"$', project, flags=re.MULTILINE)

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
        "AsyncClient": {"connect", "scandir", "mkdir", "open", "drain_recovery_events"},
        "File": {"read", "read_at", "readinto", "readinto_at", "write", "write_at", "flush"},
        "AsyncFile": {"read", "read_at", "readinto", "readinto_at", "write", "write_at", "flush"},
    }
    for class_name, methods in expected.items():
        assert methods <= classes[class_name]

    for class_name in ("Client", "AsyncClient"):
        assert not {"read_bytes", "write_bytes"} & classes[class_name]


def test_complete_reference_covers_public_stub_and_signatures() -> None:
    """New public symbols/members must have a reference entry and current types."""
    reference = ROOT / "python/nfs_rs/API.md"
    text = reference.read_text(encoding="utf-8")
    stub = ast.parse((ROOT / "python/nfs_rs/__init__.pyi").read_text(encoding="utf-8"))
    headings = set(re.findall(r"^#{3,4} (\S+)$", text, re.MULTILINE))
    sections = re.split(r"^#{3,4} (\S+)\n", text, flags=re.MULTILINE)
    bodies = dict(zip(sections[1::2], sections[2::2]))

    implementations = {}
    for filename in ("_client.py", "_errors.py"):
        tree = ast.parse((ROOT / "python/nfs_rs" / filename).read_text(encoding="utf-8"))
        for definition in tree.body:
            if isinstance(definition, ast.ClassDef):
                for method in definition.body:
                    if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        implementations[f"{definition.name}.{method.name}"] = method

    def argument_defaults(function):
        args = function.args
        pairs = list(zip(args.args[-len(args.defaults):], args.defaults))
        pairs += list(zip(args.kwonlyargs, args.kw_defaults))
        return {arg.arg: ast.dump(value) for arg, value in pairs if value is not None}

    def check_function(node, qualified):
        assert qualified in headings
        code = re.search(r"```python\n(.*?)```", bodies[qualified], re.DOTALL)
        assert code is not None, qualified
        documented = ast.parse(code.group(1)).body[0]
        assert type(documented) is type(node), qualified
        assert ast.dump(documented.returns) == ast.dump(node.returns), qualified
        implementation_key = qualified
        if node.name in ("connect", "list_exports", "list_exports_async"):
            implementation_key = "_ClientOptions._connection_options"
        elif node.name == "unlink":
            implementation_key = qualified.replace(".unlink", ".remove")
        implementation = implementations.get(implementation_key)
        if implementation is not None and node.name != "__init__":
            assert argument_defaults(documented) == argument_defaults(implementation), qualified
        # Defaults are concrete in documentation and ellipses in stubs.
        for arguments in (documented.args, node.args):
            arguments.defaults = []
            arguments.kw_defaults = []
        assert ast.dump(documented.args) == ast.dump(node.args), qualified
        assert len(bodies[qualified].split("```", 2)[-1].strip()) > 30, qualified

    for node in stub.body:
        if isinstance(node, ast.AnnAssign) and node.target.id == "__version__":
            assert node.target.id in headings
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            check_function(node, node.name)
        elif isinstance(node, ast.ClassDef):
            assert node.name in headings
            for member in node.body:
                if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    check_function(member, f"{node.name}.{member.name}")
                elif isinstance(member, ast.AnnAssign):
                    assert f"`{node.name}.{member.target.id}`" in bodies[node.name]
                elif isinstance(member, ast.Assign):
                    for target in member.targets:
                        assert f"`{node.name}.{target.id}`" in bodies[node.name]


def test_packaged_guide_matches_repository_guide() -> None:
    assert (ROOT / "python/nfs_rs/GUIDE.md").read_bytes() == (ROOT / "docs/python-api.md").read_bytes()
    for name in ("API.md", "GUIDE.md"):
        for block in _python_blocks(ROOT / "python/nfs_rs" / name):
            ast.parse(block)


def test_reference_covers_runtime_exports_and_is_packaged() -> None:
    import nfs_rs
    from importlib.resources import files

    # This also exercises the installed wheel in CI/release validation.
    reference = files("nfs_rs").joinpath("API.md").read_text(encoding="utf-8")
    guide = files("nfs_rs").joinpath("GUIDE.md").read_text(encoding="utf-8")
    headings = set(re.findall(r"^### (\S+)$", reference, re.MULTILINE))
    assert set(nfs_rs.__all__) <= headings
    assert guide.startswith("# Python user guide")
