"""Docstring-contract check: every public symbol documented, exactly once.

The single implementation of the contract (``gen.py`` writes docstrings, this
module validates presence — no duplicated logic):

* a public runtime symbol (module function, class, method defined on the class,
  or annotated data descriptor) must have a non-empty runtime docstring —
  stale stub prose must never outlive its Rust ``///`` source;
* a stub-only override of an inherited runtime member narrows types, so it must
  carry its own hand-written docstring — on its docstring-carrier def (the last
  variant of an overload set, or the def itself when not overloaded).

Stdlib-only (``ast``); pairs with the libcst-based writer in ``gen.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyo3stubs.context import CheckContext
from pyo3stubs.doc_structure import iter_doc_subjects
from pyo3stubs.report import Findings

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig


def collect_errors(cfg: StubConfig) -> Findings:
    """Flag public runtime symbols missing docs and stub overrides without prose."""
    ctx = CheckContext(cfg)
    missing: list[str] = []
    examined = 0
    for subject in iter_doc_subjects(ctx):
        examined += 1
        if not (subject.prose or '').strip():
            missing.append(
                f'{subject.source.path}:{subject.source.line}: {subject.qualname}: '
                'runtime docstring missing or empty'
            )

    return Findings(missing, examined=examined)
