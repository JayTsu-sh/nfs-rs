from __future__ import annotations

import ast
import pathlib


ROOT = pathlib.Path(__file__).parents[2]


def public_methods(class_name: str) -> set[str]:
    module = ast.parse((ROOT / "python/nfs_rs/__init__.pyi").read_text(encoding="utf-8"))
    class_node = next(
        node for node in module.body if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    return {
        node.name
        for node in class_node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name != "__init__"
    }


def declared_coverage() -> dict[str, set[str]]:
    script = ast.parse(
        (ROOT / "scripts/validate-python-real-api.py").read_text(encoding="utf-8")
    )
    assignment = next(
        node
        for node in script.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "PUBLIC_API_COVERAGE" for target in node.targets)
    )
    value = ast.literal_eval(assignment.value)
    return {name: set(methods) for name, methods in value.items()}


def literal_assignment(name: str):
    script = ast.parse(
        (ROOT / "scripts/validate-python-real-api.py").read_text(encoding="utf-8")
    )
    assignment = next(
        node
        for node in script.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == name for target in node.targets)
    )
    return ast.literal_eval(assignment.value)


def test_real_conformance_declares_every_public_object_method() -> None:
    coverage = declared_coverage()
    assert {name: methods for name, methods in coverage.items() if name != "module"} == {
        class_name: public_methods(class_name)
        for class_name in ("Client", "AsyncClient", "File", "AsyncFile")
    }


def test_real_conformance_covers_export_discovery_functions() -> None:
    coverage = declared_coverage()
    assert coverage["module"] == {"list_exports", "list_exports_async"}


def test_real_conformance_makes_every_unavailable_api_explicit() -> None:
    assert set(literal_assignment("EXPECTED_UNAVAILABLE")) == {
        f"{class_name}.{method}"
        for class_name in ("Client", "AsyncClient")
        for method in (
            "setxattr", "getxattr", "listxattr", "removexattr",
            "getdacl", "setdacl", "getsacl", "setsacl",
        )
    }
