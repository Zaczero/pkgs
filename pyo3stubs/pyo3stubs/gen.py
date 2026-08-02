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
import types
from collections import Counter
from typing import TYPE_CHECKING, TypeVar

import libcst as cst

from pyo3stubs.ast_util import decorator_names, doc_of
from pyo3stubs.context import CheckContext
from pyo3stubs.rust_scan import constructor_docs

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyo3stubs.config import StubConfig

# PyO3 registers protocol methods as type slots, and CPython synthesises the
# wrapper — taking the docstring with it. `LRUCache.__getitem__` reads "Return
# self[key]." no matter what the `///` comment above the Rust said, so injecting
# the runtime doc would overwrite the stub's own prose with boilerplate.
#
# The tell is the descriptor kind, not the text. Everything PyO3 documents
# itself arrives as a `method_descriptor` (plain methods), `getset_descriptor`
# (getters) or `classmethod_descriptor`; only CPython-synthesised slots are
# `types.WrapperDescriptorType` (the type of `object.__lt__`).

#: Docstrings CPython supplies for members a class never wrote. Two dunders slip
#: past the descriptor-kind test above: `__new__` is a
#: `builtin_function_or_method` carrying `object.__new__`'s text (PyO3 does not
#: attach a `#[new]`'s `///` to it at all), and the `__class_getitem__` that
#: `#[pyclass(generic)]` synthesises carries `dict.__class_getitem__`'s "See PEP
#: 585". Both were being published into shipped stubs over the Rust prose.
#:
#: Read out of the running interpreter rather than written down, so it tracks
#: CPython's wording instead of pinning one release's phrasing — and consulted
#: only for dunders, where a hand-written `///` cannot plausibly collide.
_BUILTIN_DOCS = frozenset(
    doc
    for owner in (object, type, dict, list, tuple, str, int)
    for name in dir(owner)
    if (doc := doc_of(getattr(owner, name, None))) is not None
)


def _runtime_doc_for_stub(name: str, obj: object) -> str | None:
    """Runtime ``__doc__`` suitable for stub injection, or ``None`` to leave stub.

    Returning None hands the symbol to the stub-only-override branch, so the
    hand-written prose survives and `doc-contract` requires it to exist.
    """
    if isinstance(obj, types.WrapperDescriptorType):
        return None
    doc = doc_of(obj)
    if doc is not None and name.startswith('__') and doc in _BUILTIN_DOCS:
        return None
    return doc


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


def _docstring_statement(doc: str, indent: str) -> cst.BaseStatement:
    """Build an indented triple-quoted docstring statement line."""
    lines = _escape(doc).split('\n')
    if len(lines) == 1:
        literal = f'"""{lines[0]}"""'
    else:
        body = '\n'.join((indent + ln) if ln.strip() else '' for ln in lines[1:])
        literal = f'"""{lines[0]}\n{body}\n{indent}"""'
    return cst.parse_statement(literal, config=cst.PartialParserConfig())


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


def _def_totals(stub_source: str) -> Counter[tuple[str, str]]:
    """``(scope, name) -> def count`` for module- and class-level functions.

    A `@x.setter` is not an overload variant — it shares the getter's name by
    design — so it is not counted, which keeps the getter the carrier.
    """
    totals: Counter[tuple[str, str]] = Counter()

    def count(scope: str, body: list[ast.stmt]) -> None:
        for node in _counted_defs(body):
            if 'setter' not in decorator_names(node):
                totals[(scope, node.name)] += 1

    module = ast.parse(stub_source)
    count('', module.body)
    for node in module.body:
        if isinstance(node, ast.ClassDef):
            count(node.name, node.body)
    return totals


class DocInjector(cst.CSTTransformer):
    """Copy ``runtime.__doc__`` onto each symbol's docstring-carrier def."""

    def __init__(
        self,
        runtime: types.ModuleType,
        totals: Counter[tuple[str, str]],
        constructors: dict[str, str],
    ) -> None:
        self._runtime = runtime
        self._totals = totals
        self._constructors = constructors
        self._seen: Counter[tuple[str, str]] = Counter()
        self._stack: list[str] = []

    def visit_ClassDef(self, node: cst.ClassDef) -> None:  # noqa: N802
        self._stack.append(node.name.value)

    def leave_ClassDef(  # noqa: N802
        self, original: cst.ClassDef, updated: cst.ClassDef
    ) -> cst.ClassDef:
        self._stack.pop()
        name = original.name.value
        obj = getattr(self._runtime, name, None)
        if obj is None:
            return updated  # stub-only typing helper (protocols)
        doc = _runtime_doc_for_stub(name, obj)
        if doc:
            return _apply(updated, doc, '    ' * (len(self._stack) + 1))
        return updated

    def leave_FunctionDef(  # noqa: N802
        self, original: cst.FunctionDef, updated: cst.FunctionDef
    ) -> cst.FunctionDef:
        name = original.name.value
        if any(
            isinstance(d.decorator, cst.Attribute)
            and d.decorator.attr.value == 'setter'
            for d in original.decorators
        ):
            # A property setter shares the getter's name but is not a variant of
            # it: the property's `__doc__` is the getter's, and treating the
            # pair as an overload set moved the prose onto the setter.
            return updated
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
        doc = _runtime_doc_for_stub(name, obj)
        if doc is None and name == '__new__':
            # PyO3 leaves a `#[new]`'s doc comment unreachable at runtime, so
            # the only place the constructor's prose exists is the Rust source.
            doc = self._constructors.get(scope)
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
    injector = DocInjector(
        ctx.runtime_module, _def_totals(source), constructor_docs(cfg)
    )
    return cst.parse_module(source).visit(injector).code
