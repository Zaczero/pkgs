"""Docstring-contract check: every public symbol documented, exactly once.

The single implementation of the contract (``gen.py`` writes docstrings, this
module validates presence — no duplicated logic):

* a public runtime symbol (module function, class, method defined on the class,
  or annotated data descriptor) must have a non-empty runtime docstring —
  stale stub prose must never outlive its Rust ``///`` source;
* a stub-only override of an inherited runtime member narrows types, so it must
  carry its own hand-written docstring — on its docstring-carrier def (the last
  variant of an overload set, or the def itself when not overloaded).

Pairs with the libcst-based writer in ``gen.py`` and reads ``#[new]`` prose
through the retained Rust syntax tree when Python exposes only boilerplate.
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
    from pyo3stubs.rust_scan import constructor_doc_sources

    ctx = CheckContext(cfg)
    constructor_sources = constructor_doc_sources(cfg)
    missing: list[str] = []
    examined = 0
    for subject in iter_doc_subjects(
        ctx,
        constructor_sources=constructor_sources.by_class,
    ):
        examined += 1
        class_name = subject.qualname.rpartition('.')[0]
        source = (
            constructor_sources.by_class.get(class_name)
            if subject.kind == 'constructor'
            else None
        )
        if source is not None and source.reason is not None:
            if source not in constructor_sources.issues:
                missing.append(
                    f'{source.path}:{source.line}: {subject.qualname}: {source.reason}'
                )
            continue
        if not (subject.prose or '').strip():
            missing.append(
                f'{subject.source.path}:{subject.source.line}: {subject.qualname}: '
                'runtime docstring missing or empty'
            )

    missing.extend(
        f'{source.path}:{source.line}: '
        f'{source.python_class + ".__new__" if source.python_class else "constructor-doc macro"}: '
        f'{source.reason}'
        for source in constructor_sources.issues
        if source.reason is not None
    )
    return Findings(missing, examined=examined)
