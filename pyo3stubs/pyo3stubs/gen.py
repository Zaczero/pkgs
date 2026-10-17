"""Inject Rust-authored docstrings into the ``.pyi`` stub (the generation half of
the toolkit; the check modules validate, this one writes).

Docstrings live once, in the Rust ``///`` doc comments on the PyO3 surface, and
become the compiled extension's ``__doc__``. :func:`render_stub_with_docs` copies
each symbol's runtime ``__doc__`` into the hand-authored stub so IDE hover and the
docs site render the prose, while preserving hand-written signatures, overloads,
typed annotations, and annotated descriptor declarations.

Placement contract: **exactly one docstring per symbol.** A non-overloaded def
carries it directly; an overload set carries it on the *last* authored variant
as the prose carrier (stubs must not have a bare implementation def per PEP 484,
and duplicating prose across variants is drift surface). Earlier variants are
stripped. Reference pages show every accepted variant in source order; the
carrier is not a universal signature. Stub-only overrides of inherited runtime
members keep their hand-written prose untouched (they narrow types, so they
document themselves — the doc-contract check enforces their presence).

Requires ``libcst``. The runtime module and stub path come from the project's
:class:`~pyo3stubs.config.StubConfig`.
"""

from __future__ import annotations

import ast
import inspect
from collections import Counter
from typing import TYPE_CHECKING, TypeVar

import libcst as cst

from pyo3stubs.ast_util import (
    annotated_data_descriptors,
    decorator_names,
    passive_class_member,
    runtime_doc_for_stub,
)
from pyo3stubs.context import CheckContext
from pyo3stubs.rust_scan import constructor_doc_sources

if TYPE_CHECKING:
    import types
    from collections.abc import Iterator, Mapping

    from pyo3stubs.config import StubConfig
    from pyo3stubs.rust_scan import ConstructorDocSource

#: The two node kinds that can carry a docstring in a stub.
_Def = TypeVar('_Def', cst.ClassDef, cst.FunctionDef)


def _escape(text: str) -> str:
    """Docstring text made safe as the body of a triple-quoted literal.

    Escaping the backslash and the triple quote was not enough, and both
    survivors were quiet rather than loud. A doc ending in one quote closed the
    literal a character early — an unterminated string, so ``gen-docs``
    aborted. A doc ending in two parsed as a *shorter* literal, and the stub
    kept the truncated text with ``gen-docs-sync`` calling it in sync forever.
    Escaping a trailing quote fixes both. A carriage return needs escaping too:
    raw, the tokenizer folds it into a newline and the prose silently changes.
    """
    text = '\n'.join('' if not line.strip() else line for line in text.split('\n'))
    # The body of a `"""` literal is safe exactly when it contains no run of
    # three quotes and does not end in one. Escape only the quotes that break
    # those two rules, so ordinary quoted prose stays readable in the stub.
    out: list[str] = []
    run = 0
    for char in text.replace('\\', '\\\\').replace('\r', '\\r'):
        run = run + 1 if char == '"' else 0
        if run == 3:
            out.append('\\"')
            run = 0
        else:
            out.append(char)
    if out and out[-1] == '"':
        out[-1] = '\\"'
    return ''.join(out)


def _docstring_statement(doc: str, indent: str) -> cst.SimpleStatementLine:
    """Build an indented triple-quoted docstring statement line."""
    lines = _escape(inspect.cleandoc(doc)).split('\n')
    if len(lines) == 1:
        literal = f'"""{lines[0]}"""'
    else:
        body = '\n'.join((indent + line) if line.strip() else '' for line in lines[1:])
        literal = f'"""{lines[0]}\n{body}\n{indent}"""'
    return cst.SimpleStatementLine(body=[cst.Expr(cst.SimpleString(literal))])


def _is_string_expr(stmt: object) -> bool:
    return (
        isinstance(stmt, cst.SimpleStatementLine)
        and len(stmt.body) == 1
        and isinstance(stmt.body[0], cst.Expr)
        and isinstance(stmt.body[0].value, (cst.SimpleString, cst.ConcatenatedString))
    )


def _apply(node: _Def, doc: str, indent: str) -> _Def:
    """Return ``node`` with the docstring as the first body statement.

    A stub body that carries a docstring uses the docstring *as* its body — no
    trailing ``...`` (two statements would trip ruff's ``PYI048``). Class members
    are preserved after the docstring; an otherwise-empty function's leading
    ``...`` is dropped.
    """
    docline = _docstring_statement(doc, indent)
    body = node.body
    rest: list[cst.BaseStatement] = []
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


#: Statements that hold defs the transformer will still visit. A stub gates
#: members behind `if sys.version_info >= ...:` and `if TYPE_CHECKING:`; a def
#: the count misses is a def the transformer strips the docstring from.
_NESTING = (ast.If, ast.Try)


def _counted_defs(
    body: list[ast.stmt],
) -> Iterator[ast.FunctionDef | ast.AsyncFunctionDef]:
    """Every def in a suite, through the blocks a stub nests them in.

    `async def` is a distinct node to `ast` but the same node to libcst, so
    omitting it meant the transformer treated one as an uncounted extra and
    deleted its docstring.
    """
    for stmt in body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield stmt
        elif isinstance(stmt, _NESTING):
            yield from _counted_defs(stmt.body)
            yield from _counted_defs(stmt.orelse)


def _def_totals(module: ast.Module) -> Counter[tuple[str, str]]:
    """``(scope, name) -> def count`` for module- and class-level functions.

    A `@x.setter` is not an overload variant — it shares the getter's name by
    design — so it is not counted, which keeps the getter the carrier.
    """
    totals: Counter[tuple[str, str]] = Counter()

    def count(scope: str, body: list[ast.stmt]) -> None:
        for node in _counted_defs(body):
            if 'setter' not in decorator_names(node):
                totals[scope, node.name] += 1

    count('', module.body)
    for node in module.body:
        if isinstance(node, ast.ClassDef):
            count(node.name, node.body)
    return totals


def _descriptor_docs(
    classes: Mapping[str, ast.ClassDef],
    runtime: types.ModuleType,
) -> dict[str, dict[str, str]]:
    """Native prose for annotated data descriptors, keyed by class/member."""
    docs: dict[str, dict[str, str]] = {}
    for class_name, node in classes.items():
        cls = getattr(runtime, class_name, None)
        if not isinstance(cls, type):
            continue
        for member_name, _, _, descriptor in annotated_data_descriptors(cls, node.body):
            if doc := runtime_doc_for_stub(member_name, descriptor):
                docs.setdefault(class_name, {})[member_name] = doc
    return docs


def _inject_attribute_docs(
    body: cst.IndentedBlock,
    docs: dict[str, str],
    continuation_indent: str,
) -> cst.IndentedBlock:
    """Replace adjacent docstrings of native annotated data descriptors."""
    statements = list(body.body)
    updated: list[cst.BaseStatement] = []
    index = 0
    while index < len(statements):
        statement = statements[index]
        attribute = None
        if (
            isinstance(statement, cst.SimpleStatementLine)
            and len(statement.body) == 1
            and isinstance(statement.body[0], cst.AnnAssign)
            and isinstance(statement.body[0].target, cst.Name)
        ):
            attribute = statement.body[0].target.value
        doc = docs.get(attribute) if attribute is not None else None
        updated.append(statement)
        index += 1
        if doc is None:
            continue
        if index < len(statements) and _is_string_expr(statements[index]):
            index += 1
        updated.append(_docstring_statement(doc, continuation_indent))
    return body.with_changes(body=updated)


class DocInjector(cst.CSTTransformer):
    """Copy ``runtime.__doc__`` onto each symbol's docstring-carrier def."""

    def __init__(
        self,
        runtime: types.ModuleType,
        totals: Counter[tuple[str, str]],
        constructors: Mapping[str, ConstructorDocSource],
        attribute_docs: dict[str, dict[str, str]] | None = None,
    ) -> None:
        self._runtime = runtime
        self._totals = totals
        self._constructors = constructors
        self._attribute_docs = attribute_docs or {}
        self._seen: Counter[tuple[str, str]] = Counter()
        self._stack: list[str] = []

    def visit_ClassDef(self, node: cst.ClassDef) -> None:  # ruff: ignore[invalid-function-name]
        self._stack.append(node.name.value)

    def leave_ClassDef(  # ruff: ignore[invalid-function-name]
        self, original_node: cst.ClassDef, updated_node: cst.ClassDef
    ) -> cst.ClassDef:
        self._stack.pop()
        name = original_node.name.value
        obj = getattr(self._runtime, name, None)
        if obj is None:
            return updated_node  # stub-only typing helper (protocols)
        if (attribute_docs := self._attribute_docs.get(name)) and isinstance(
            updated_node.body, cst.IndentedBlock
        ):
            updated_node = updated_node.with_changes(
                body=_inject_attribute_docs(
                    updated_node.body,
                    attribute_docs,
                    '    ' * (len(self._stack) + 1),
                )
            )
        doc = runtime_doc_for_stub(name, obj)
        if doc:
            return _apply(updated_node, doc, '    ' * (len(self._stack) + 1))
        return updated_node

    def leave_FunctionDef(  # ruff: ignore[invalid-function-name]
        self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef
    ) -> cst.FunctionDef:
        name = original_node.name.value
        if any(
            isinstance(d.decorator, cst.Attribute)
            and d.decorator.attr.value == 'setter'
            for d in original_node.decorators
        ):
            # A property setter shares the getter's name but is not a variant of
            # it: the property's `__doc__` is the getter's, and treating the
            # pair as an overload set moved the prose onto the setter.
            return updated_node
        scope = self._stack[-1] if self._stack else ''
        key = (scope, name)
        self._seen[key] += 1
        carrier = self._seen[key] == self._totals[key]
        if self._stack:
            indent = '    ' * (len(self._stack) + 1)
            cls = getattr(self._runtime, scope, None)
            if cls is None:
                return updated_node  # stub-only typing helper (protocols)
            constructor = self._constructors.get(scope) if name == '__new__' else None
            if name not in vars(cls) and constructor is None:
                # Stub-only override of an inherited runtime member: it narrows
                # types, so it documents itself — leave the group untouched.
                return updated_node
            obj = passive_class_member(cls, name)
        else:
            indent = '    '
            constructor = None
            obj = getattr(self._runtime, name, None)
        if obj is None and constructor is None:
            return updated_node  # stub-only typing helper
        if not carrier:
            return _without_docstring(updated_node)
        if constructor is not None:
            return (
                _apply(updated_node, constructor.prose, indent)
                if constructor.prose is not None
                else _without_docstring(updated_node)
            )
        doc = runtime_doc_for_stub(name, obj)
        if doc:
            return _apply(updated_node, doc, indent)
        return updated_node


def render_stub_with_docs(cfg: StubConfig) -> str:
    """Return the stub source with runtime docstrings injected; does not write.

    Docstring-presence violations are the doc-contract check's job
    (:func:`pyo3stubs.doc_contract.collect_errors`) — the CLI runs both.
    """
    ctx = CheckContext(cfg)
    source = ctx.stub_text
    module = ctx.stub_ast
    runtime = ctx.runtime_module
    injector = DocInjector(
        runtime,
        _def_totals(module),
        constructor_doc_sources(cfg).by_class,
        _descriptor_docs(ctx.stub_classes, runtime),
    )
    return cst.parse_module(source).visit(injector).code
