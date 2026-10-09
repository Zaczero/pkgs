"""Shared AST helpers used by multiple gates (one definition each)."""

from __future__ import annotations

import ast
import inspect
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


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


def annotated_data_descriptors(
    cls: type,
    body: list[ast.stmt],
) -> Iterator[tuple[str, ast.AnnAssign, str | None, object]]:
    """Enumerate locally declared annotated data descriptors and their prose.

    A bare string expression immediately following a class annotation is that
    attribute's docstring. Only a data descriptor in the class's own namespace
    qualifies; reading it through the class or an instance would invoke it.
    """
    namespace = vars(cls)
    for index, stmt in enumerate(body):
        if not (isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)):
            continue
        name = stmt.target.id
        descriptor = namespace.get(name)
        if not inspect.isdatadescriptor(descriptor):
            continue
        docstring = None
        if index + 1 < len(body):
            following = body[index + 1]
            if (
                isinstance(following, ast.Expr)
                and isinstance(following.value, ast.Constant)
                and isinstance(following.value.value, str)
            ):
                docstring = following.value.value
        yield name, stmt, docstring, descriptor
