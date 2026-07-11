"""Inject Rust-authored docstrings into the ``.pyi`` stub (the generation half of
the toolkit; the check modules validate, this one writes).

Docstrings live once, in the Rust ``///`` doc comments on the PyO3 surface, and
become the compiled extension's ``__doc__``. :func:`render_stub_with_docs` copies
each symbol's runtime ``__doc__`` into the hand-authored stub so IDE hover and the
docs site render the prose, while preserving the hand-written signatures,
overloads, and typed annotations untouched.

Placement contract: **exactly one docstring per symbol.** A non-overloaded def
carries it directly; an overload set carries it on the *last* variant — the
canonical union signature (stubs must not have a bare implementation def per
PEP 484, and duplicating prose across variants is drift surface). Earlier
variants are stripped. Stub-only overrides of inherited runtime members keep
their hand-written prose untouched (they narrow types, so they document
themselves — the doc-contract check enforces their presence).

Requires ``libcst``. The runtime module and stub path come from the project's
:class:`~pyo3stubs.config.StubConfig`.
"""

from __future__ import annotations

import ast
from collections import Counter
from typing import TYPE_CHECKING

import libcst as cst

from pyo3stubs.ast_util import doc_of
from pyo3stubs.context import CheckContext

if TYPE_CHECKING:
    import types

    from pyo3stubs.config import StubConfig


def _docstring_statement(doc: str, indent: str) -> cst.BaseStatement:
    """Build an indented triple-quoted docstring statement line."""
    esc = doc.replace('\\', '\\\\').replace('"""', '\\"\\"\\"')
    lines = esc.split('\n')
    if len(lines) == 1:
        literal = f'"""{lines[0]}"""'
    else:
        body = '\n'.join((indent + ln) if ln else '' for ln in lines[1:])
        literal = f'"""{lines[0]}\n{body}\n{indent}"""'
    return cst.parse_statement(literal, config=cst.PartialParserConfig())


def _is_string_expr(stmt: object) -> bool:
    return (
        isinstance(stmt, cst.SimpleStatementLine)
        and len(stmt.body) == 1
        and isinstance(stmt.body[0], cst.Expr)
        and isinstance(stmt.body[0].value, (cst.SimpleString, cst.ConcatenatedString))
    )


def _apply(node: cst.CSTNode, doc: str, indent: str):
    """Return ``node`` with the docstring as the first body statement.

    A stub body that carries a docstring uses the docstring *as* its body — no
    trailing ``...`` (two statements would trip ruff's ``PYI048``). Class members
    are preserved after the docstring; an otherwise-empty function's leading
    ``...`` is dropped.
    """
    docline = _docstring_statement(doc, indent)
    body = node.body  # type: ignore[attr-defined]
    rest: list = []
    if isinstance(body, cst.IndentedBlock):
        stmts = list(body.body)
        if stmts and _is_string_expr(stmts[0]):
            stmts = stmts[1:]
        rest = [
            s
            for s in stmts
            if not (
                isinstance(s, cst.SimpleStatementLine)
                and len(s.body) == 1
                and isinstance(s.body[0], cst.Expr)
                and isinstance(s.body[0].value, cst.Ellipsis)
            )
        ]
    return node.with_changes(body=cst.IndentedBlock(body=[docline, *rest]))


def _without_docstring(node: cst.FunctionDef) -> cst.FunctionDef:
    body = node.body
    if not isinstance(body, cst.IndentedBlock):
        return node
    stmts = list(body.body)
    if stmts and _is_string_expr(stmts[0]):
        stmts = stmts[1:]
    if not stmts:
        stmts = [cst.parse_statement('...')]
    return node.with_changes(body=cst.IndentedBlock(body=stmts))


def _def_totals(stub_source: str) -> Counter[tuple[str, str]]:
    """``(scope, name) -> def count`` for module- and class-level functions."""
    totals: Counter[tuple[str, str]] = Counter()
    for node in ast.parse(stub_source).body:
        if isinstance(node, ast.FunctionDef):
            totals[('', node.name)] += 1
        elif isinstance(node, ast.ClassDef):
            for stmt in node.body:
                if isinstance(stmt, ast.FunctionDef):
                    totals[(node.name, stmt.name)] += 1
    return totals


class DocInjector(cst.CSTTransformer):
    """Copy ``runtime.__doc__`` onto each symbol's docstring-carrier def."""

    def __init__(
        self,
        runtime: types.ModuleType,
        totals: Counter[tuple[str, str]],
    ) -> None:
        self._runtime = runtime
        self._totals = totals
        self._seen: Counter[tuple[str, str]] = Counter()
        self._stack: list[str] = []

    def visit_ClassDef(self, node: cst.ClassDef) -> None:  # noqa: N802
        self._stack.append(node.name.value)

    def leave_ClassDef(self, original, updated):  # noqa: N802
        self._stack.pop()
        name = original.name.value
        obj = getattr(self._runtime, name, None)
        if obj is None:
            return updated  # stub-only typing helper (protocols)
        doc = doc_of(obj)
        if doc:
            return _apply(updated, doc, '    ' * (len(self._stack) + 1))
        return updated

    def leave_FunctionDef(self, original, updated):  # noqa: N802
        name = original.name.value
        scope = self._stack[-1] if self._stack else ''
        key = (scope, name)
        self._seen[key] += 1
        carrier = self._seen[key] == self._totals[key]
        if self._stack:
            indent = '    ' * (len(self._stack) + 1)
            cls = getattr(self._runtime, scope, None)
            if cls is None:
                return updated  # stub-only typing helper (protocols)
            if name not in vars(cls):
                # Stub-only override of an inherited runtime member: it narrows
                # types, so it documents itself — leave the group untouched.
                return updated
            obj = getattr(cls, name, None)
        else:
            indent = '    '
            obj = getattr(self._runtime, name, None)
        if obj is None:
            return updated  # stub-only typing helper
        if not carrier:
            return _without_docstring(updated)
        doc = doc_of(obj)
        if doc:
            return _apply(updated, doc, indent)
        return updated


def render_stub_with_docs(cfg: StubConfig) -> str:
    """Return the stub source with runtime docstrings injected; does not write.

    Docstring-presence violations are the doc-contract check's job
    (:func:`pyo3stubs.doc_contract.collect_errors`) — the CLI runs both.
    """
    ctx = CheckContext(cfg)
    source = ctx.stub_text
    injector = DocInjector(ctx.runtime_module, _def_totals(source))
    return cst.parse_module(source).visit(injector).code
