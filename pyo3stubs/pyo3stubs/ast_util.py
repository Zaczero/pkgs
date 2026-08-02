"""Shared AST helpers used by multiple gates (one definition each)."""

from __future__ import annotations

import ast


def doc_of(obj: object) -> str | None:
    """Non-empty ``__doc__`` stripped of leading/trailing newlines, or ``None``."""
    doc = getattr(obj, '__doc__', None)
    if not isinstance(doc, str) or not doc.strip():
        return None
    return doc.strip('\n')


def decorator_names(
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
) -> set[str]:
    """Bare names of a def's decorators (`@x.setter` contributes ``setter``)."""
    names = set()
    for dec in node.decorator_list:
        target = dec.func if isinstance(dec, ast.Call) else dec
        if isinstance(target, ast.Attribute):
            names.add(target.attr)
        elif isinstance(target, ast.Name):
            names.add(target.id)
    return names


def function_groups(body: list[ast.stmt]) -> dict[str, list[ast.FunctionDef]]:
    """Group top-level function defs in a body by name (preserves order)."""
    groups: dict[str, list[ast.FunctionDef]] = {}
    for stmt in body:
        if isinstance(stmt, ast.FunctionDef):
            groups.setdefault(stmt.name, []).append(stmt)
    return groups
