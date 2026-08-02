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
from pyo3stubs.report import Findings, loc

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig


def _has_docstring(node: ast.FunctionDef) -> bool:
    """Whether the def carries prose.

    Not "the first statement is a constant": in a `.pyi` every body is `...`,
    which parses to `Expr(Constant(Ellipsis))`. Accepting that made this
    return True for every stub def, so the contract it guards -- a stub-only
    override must document itself -- could never fire.
    """
    return ast.get_docstring(node) is not None


def collect_errors(cfg: StubConfig) -> Findings:
    """Flag public runtime symbols missing docs and stub overrides without prose."""
    ctx = CheckContext(cfg)
    runtime = ctx.runtime_module
    path = cfg.stub_path
    missing: list[str] = []
    examined = 0

    for name, defs in function_groups(ctx.stub_ast.body).items():
        obj = getattr(runtime, name, None)
        if obj is None or name.startswith('_'):
            continue
        examined += 1
        if not doc_of(obj):
            missing.append(
                f'{loc(path, defs[-1])}: {name}: runtime docstring missing or empty'
            )

    for node in ctx.stub_classes.values():
        cls = getattr(runtime, node.name, None)
        if not isinstance(cls, type):
            continue  # stub-only typing helper (protocols)
        if not node.name.startswith('_') and not doc_of(cls):
            missing.append(
                f'{loc(path, node)}: {node.name}: runtime docstring missing or empty'
            )
        for name, defs in function_groups(node.body).items():
            qualname = f'{node.name}.{name}'
            carrier = defs[-1]
            examined += 1
            if name in vars(cls):
                if not name.startswith('_') and not doc_of(getattr(cls, name, None)):
                    missing.append(
                        f'{loc(path, carrier)}: {qualname}: runtime docstring '
                        f'missing or empty'
                    )
            elif not name.startswith('__') and not _has_docstring(carrier):
                # Stub-only override: the carrier def (last of the group) must
                # hold hand-written prose.
                missing.append(
                    f'{loc(path, carrier)}: {qualname}: stub override needs its '
                    f'own docstring'
                )

    return Findings(missing, examined=examined)
