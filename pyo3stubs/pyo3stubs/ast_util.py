"""Shared AST helpers used by multiple gates (one definition each)."""

from __future__ import annotations

import ast


def doc_of(obj: object) -> str | None:
    """Non-empty ``__doc__`` stripped of leading/trailing newlines, or ``None``."""
    doc = getattr(obj, '__doc__', None)
    if not doc or not doc.strip():
        return None
    return doc.strip('\n')


def function_groups(body: list[ast.stmt]) -> dict[str, list[ast.FunctionDef]]:
    """Group top-level function defs in a body by name (preserves order)."""
    groups: dict[str, list[ast.FunctionDef]] = {}
    for stmt in body:
        if isinstance(stmt, ast.FunctionDef):
            groups.setdefault(stmt.name, []).append(stmt)
    return groups
