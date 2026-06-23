"""Stub docstring contract checks (stdlib; write/render needs libcst at project level)."""

from __future__ import annotations

import ast
import importlib
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig


def _doc_of(obj: object) -> str | None:
    doc = getattr(obj, '__doc__', None)
    if not doc or not doc.strip():
        return None
    return doc.strip('\n')


def _has_docstring(node: ast.FunctionDef) -> bool:
    if not node.body:
        return False
    first = node.body[0]
    return isinstance(first, ast.Expr) and isinstance(
        first.value, (ast.Constant, ast.Str)
    )


def collect_doc_contract_errors(cfg: StubConfig) -> list[str]:
    """Flag public runtime symbols missing docs and stub overrides without prose."""
    runtime = importlib.import_module(cfg.module)
    tree = ast.parse(cfg.stub_path.read_text())
    missing: list[str] = []
    class_stack: list[str] = []

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            class_stack.append(node.name)
            obj = getattr(runtime, node.name, None)
            if obj is None:
                class_stack.pop()
                continue
            doc = _doc_of(obj)
            if not doc and not node.name.startswith('_'):
                missing.append(f'{node.name}: runtime docstring missing or empty')
            class_stack.pop()
        elif isinstance(node, ast.FunctionDef):
            _check_function(node, runtime, class_stack, missing)

    return missing


def _is_overload(node: ast.FunctionDef) -> bool:
    for dec in node.decorator_list:
        target = dec.func if isinstance(dec, ast.Call) else dec
        if isinstance(target, ast.Name) and target.id == 'overload':
            return True
        if isinstance(target, ast.Attribute) and target.attr == 'overload':
            return True
    return False


def _check_function(
    node: ast.FunctionDef,
    runtime: object,
    class_stack: list[str],
    missing: list[str],
) -> None:
    if _is_overload(node):
        return
    name = node.name
    if class_stack:
        qualname = f'{class_stack[-1]}.{name}'
        cls = getattr(runtime, class_stack[-1], None)
        if cls is None:
            return
        if name not in vars(cls):
            if not _has_docstring(node) and not name.startswith('__'):
                missing.append(f'{qualname}: stub override needs its own docstring')
            return
        obj = getattr(cls, name, None)
    else:
        qualname = name
        obj = getattr(runtime, name, None)
    if obj is None:
        return
    doc = _doc_of(obj)
    if not doc and not name.startswith('_'):
        missing.append(f'{qualname}: runtime docstring missing or empty')


def collect_errors(cfg: StubConfig) -> list[str]:
    """Alias for :func:`collect_doc_contract_errors`."""
    return collect_doc_contract_errors(cfg)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point — requires a project shim to build :class:`StubConfig`."""
    _ = argv
    print(
        'pyo3stubs.stub_docs: supply a project shim that builds StubConfig',
        file=sys.stderr,
    )
    return 2
