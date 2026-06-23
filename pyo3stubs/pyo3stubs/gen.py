"""Inject Rust-authored docstrings into the ``.pyi`` stub (the generation half of
the toolkit; the check modules validate, this one writes).

Docstrings live once, in the Rust ``///`` doc comments on the PyO3 surface, and
become the compiled extension's ``__doc__``. :func:`render_stub_with_docs` copies
each symbol's runtime ``__doc__`` into the hand-authored stub so IDE hover and the
docs site render the prose, while preserving the hand-written signatures,
overloads, and typed annotations untouched. Two contracts are enforced (returned
as ``missing``):

* a public runtime symbol with a deleted/empty docstring fails (stale stub prose
  must never outlive its source);
* a stub-only override of an inherited runtime member keeps — and must carry — its
  own hand-written docstring.

Requires ``libcst``. The runtime module and stub path come from the project's
:class:`~pyo3stubs.config.StubConfig`.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

import libcst as cst

if TYPE_CHECKING:
    import types

    from pyo3stubs.config import StubConfig


def _doc_of(obj: object) -> str | None:
    doc = getattr(obj, '__doc__', None)
    if not doc or not doc.strip():
        return None
    return doc.strip('\n')


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


def _is_overload(node: cst.FunctionDef) -> bool:
    for decorator in node.decorators:
        value = decorator.decorator
        if isinstance(value, cst.Name) and value.value == 'overload':
            return True
        if isinstance(value, cst.Attribute) and value.attr.value == 'overload':
            return True
    return False


def _has_docstring(node: cst.FunctionDef) -> bool:
    body = node.body
    return isinstance(body, cst.IndentedBlock) and _is_string_expr(body.body[0])


class DocInjector(cst.CSTTransformer):
    """Copy ``runtime.__doc__`` onto each matching stub symbol; record contract
    violations in :attr:`missing`.
    """

    def __init__(self, runtime: types.ModuleType) -> None:
        self._runtime = runtime
        self._stack: list[str] = []
        self.missing: list[str] = []

    def visit_ClassDef(self, node: cst.ClassDef) -> None:  # noqa: N802
        self._stack.append(node.name.value)

    def leave_ClassDef(self, original, updated):  # noqa: N802
        self._stack.pop()
        name = original.name.value
        obj = getattr(self._runtime, name, None)
        if obj is None:
            return updated  # stub-only typing helper (protocols)
        doc = _doc_of(obj)
        if doc:
            return _apply(updated, doc, '    ' * (len(self._stack) + 1))
        if not name.startswith('_'):
            self.missing.append(f'{name}: runtime docstring missing or empty')
        return updated

    def leave_FunctionDef(self, original, updated):  # noqa: N802
        if _is_overload(original):
            return _without_docstring(updated)
        name = original.name.value
        if self._stack:
            qualname = f'{self._stack[-1]}.{name}'
            indent = '    ' * (len(self._stack) + 1)
            cls = getattr(self._runtime, self._stack[-1], None)
            if cls is None:
                return updated  # stub-only typing helper (protocols)
            if name not in vars(cls):
                # Stub-only override of an inherited runtime member: it narrows
                # types, so it documents itself — require prose, leave untouched.
                if not _has_docstring(original) and not name.startswith('__'):
                    self.missing.append(
                        f'{qualname}: stub override needs its own docstring'
                    )
                return updated
            obj = getattr(cls, name, None)
        else:
            qualname = name
            indent = '    '
            obj = getattr(self._runtime, name, None)
        if obj is None:
            return updated  # stub-only typing helper
        doc = _doc_of(obj)
        if doc:
            return _apply(updated, doc, indent)
        if not name.startswith('_'):
            self.missing.append(f'{qualname}: runtime docstring missing or empty')
        return updated


def render_stub_with_docs(cfg: StubConfig) -> tuple[str, list[str]]:
    """Return ``(rendered_stub_code, contract_violations)`` for ``cfg``.

    Imports ``cfg.module`` for its runtime docstrings and injects them into
    ``cfg.stub_path``; does not write. An empty ``violations`` list means the
    docstring contract holds.
    """
    runtime = importlib.import_module(cfg.module)
    injector = DocInjector(runtime)
    code = (
        cst.parse_module(cfg.stub_path.read_text(encoding='utf-8')).visit(injector).code
    )
    return code, injector.missing
