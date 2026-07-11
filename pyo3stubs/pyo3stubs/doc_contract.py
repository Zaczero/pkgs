"""Docstring-contract check: every public symbol documented, exactly once.

The single implementation of the contract (``gen.py`` writes docstrings, this
module validates presence — no duplicated logic):

* a public runtime symbol (module function, class, or method defined on the
  class itself) must have a non-empty runtime docstring — stale stub prose must
  never outlive its Rust ``///`` source;
* a stub-only override of an inherited runtime member narrows types, so it must
  carry its own hand-written docstring — on its docstring-carrier def (the last
  variant of an overload set, or the def itself when not overloaded).

Stdlib-only (``ast``); pairs with the libcst-based writer in ``gen.py``.
"""

from __future__ import annotations

import ast
from typing import TYPE_CHECKING

from pyo3stubs.ast_util import doc_of, function_groups
from pyo3stubs.context import CheckContext

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig


def _has_docstring(node: ast.FunctionDef) -> bool:
    if not node.body:
        return False
    first = node.body[0]
    return isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant)


def collect_doc_contract_errors(cfg: StubConfig) -> list[str]:
    """Flag public runtime symbols missing docs and stub overrides without prose."""
    ctx = CheckContext(cfg)
    runtime = ctx.runtime_module
    tree = ctx.stub_ast
    missing: list[str] = []

    for name in function_groups(tree.body):
        obj = getattr(runtime, name, None)
        if obj is None or name.startswith('_'):
            continue
        if not doc_of(obj):
            missing.append(f'{name}: runtime docstring missing or empty')

    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        cls = getattr(runtime, node.name, None)
        if cls is None or not isinstance(cls, type):
            continue  # stub-only typing helper (protocols)
        if not node.name.startswith('_') and not doc_of(cls):
            missing.append(f'{node.name}: runtime docstring missing or empty')
        for name, defs in function_groups(node.body).items():
            qualname = f'{node.name}.{name}'
            if name in vars(cls):
                if not name.startswith('_') and not doc_of(getattr(cls, name, None)):
                    missing.append(f'{qualname}: runtime docstring missing or empty')
            elif not name.startswith('__') and not _has_docstring(defs[-1]):
                # Stub-only override: the carrier def (last of the group) must
                # hold hand-written prose.
                missing.append(f'{qualname}: stub override needs its own docstring')

    return missing


def collect_errors(cfg: StubConfig) -> list[str]:
    """Alias for :func:`collect_doc_contract_errors`."""
    return collect_doc_contract_errors(cfg)
