"""Rust-source nullability oracle: ``Option<T>`` surfaces must be ``| None``.

Neither runtime introspection nor stubtest can see this class of stub lie:
PyO3 emits no return annotations, so a ``#[getter] fn crs(&self) ->
Option<Crs>`` whose stub says ``crs: CRS`` type-checks everywhere while
returning ``None`` at runtime. The Rust signature is the single source of
truth, and it is cheap to read.

Scanned surfaces (best-effort, additive — macro-generated getters and custom
``IntoPyObject`` impls are invisible, which only costs coverage, never a false
positive):

* ``#[getter]`` methods in ``#[pymethods]`` blocks returning ``Option<...>``
  (through ``PyResult``), honoring ``#[getter(name)]`` overrides and PyO3's
  ``get_``-prefix stripping;
* ``#[pyo3(get)]`` struct fields typed ``Option<...>``.

The owning Python class is resolved through the same ``#[pyclass]`` scan the
leak gate uses; the stub's property return / attribute annotation must then
admit ``None``.
"""

from __future__ import annotations

import ast
import re
from typing import TYPE_CHECKING

from pyo3stubs.context import CheckContext
from pyo3stubs.report import Findings, loc
from pyo3stubs.rust_scan import (
    iter_sources,
    pyclass_exports,
    rust_class_map,
    unquote,
)

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig
    from pyo3stubs.rust_scan import Attr, Item

_PYRESULT = re.compile(r'^PyResult\s*<(.*)>$', re.DOTALL)


def _returns_option(rust_type: str) -> bool:
    """True when the (possibly ``PyResult``-wrapped) type is ``Option<...>``."""
    text = rust_type.strip()
    inner = _PYRESULT.match(text)
    if inner:
        text = inner.group(1).strip()
    return text.startswith(('Option<', 'Option <'))


def _getter_name(attr: Attr, item: Item) -> str:
    """The Python attribute a ``#[getter]`` exposes.

    PyO3 strips a `get_` prefix only when deriving the name from the function;
    an explicit `#[getter(name)]` is used verbatim.
    """
    if attr.params:
        return unquote(attr.params[0][0])
    return item.name.removeprefix('get_')


def _optional_attrs(cfg: StubConfig) -> dict[tuple[str, str], str]:
    """``(python class, attribute) -> defining path`` for Option-typed surfaces."""
    classes = rust_class_map(cfg)
    found: dict[tuple[str, str], str] = {}
    for source in iter_sources(cfg):
        for item in source.walk():
            # Keyed on the member's `#[getter]`, not the impl's `#[pymethods]`:
            # a project macro may supply the latter, which hid 29 of gometry's
            # 195 getters from this gate. `#[getter]` does not compile outside
            # `#[pymethods]`, so it is the sound signal anyway.
            if item.kind != 'impl_item':
                continue
            py_class = classes.get(item.name)
            if py_class is None:
                continue
            for child in item.children:
                attr = child.attr('getter')
                if attr is None or not _returns_option(child.ty):
                    continue
                found.setdefault((py_class, _getter_name(attr, child)), source.rel)
    for export in pyclass_exports(cfg):
        for field in export.item.children:
            attr = field.attr('pyo3')
            if attr is not None and attr.has('get') and _returns_option(field.ty):
                found.setdefault((export.python, field.name), export.source.rel)
    return found


def _admits_none(annotation: ast.expr) -> bool:
    """Whether the annotation admits `None` *itself*.

    Three things this deliberately does not do. It does not look for the text
    `None` inside a forward reference -- `"NoneCheck"` is a class name, so the
    reference is parsed and re-examined. It does not treat a container of
    optionals as optional: `list[int | None]` never is `None`. And it matches
    `Optional`/`Union` as resolved names, so `typing.Optional[int]` counts.
    """
    if isinstance(annotation, ast.Constant):
        if annotation.value is None:
            return True
        if isinstance(annotation.value, str):
            try:
                inner = ast.parse(annotation.value, mode='eval').body
            except SyntaxError:
                return False
            return _admits_none(inner)
        return False
    if isinstance(annotation, ast.BinOp) and isinstance(annotation.op, ast.BitOr):
        return _admits_none(annotation.left) or _admits_none(annotation.right)
    if isinstance(annotation, ast.Subscript):
        head = _qualified_name(annotation.value)
        if head in ('Optional', 'typing.Optional'):
            return True
        if head in ('Union', 'typing.Union'):
            elts = (
                annotation.slice.elts
                if isinstance(annotation.slice, ast.Tuple)
                else [annotation.slice]
            )
            return any(_admits_none(elt) for elt in elts)
        # Any other subscript is a container; its parameters are not the
        # annotation's own nullability.
        return False
    return isinstance(annotation, ast.Name) and annotation.id == 'None'


def _qualified_name(node: ast.expr) -> str | None:
    """Dotted name of a `Name`/`Attribute` chain, or None."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _qualified_name(node.value)
        return f'{base}.{node.attr}' if base else None
    return None


def _annotation_of(node: ast.ClassDef, attr: str) -> ast.expr | None:
    """The stub's annotation for one attribute: an assignment or a property."""
    for stmt in node.body:
        if (
            isinstance(stmt, ast.AnnAssign)
            and isinstance(stmt.target, ast.Name)
            and stmt.target.id == attr
        ):
            return stmt.annotation
        if isinstance(stmt, ast.FunctionDef) and stmt.name == attr:
            return stmt.returns
    return None


def collect_errors(cfg: StubConfig) -> Findings:
    """Stub annotations that deny ``None`` for Option-typed Rust surfaces."""
    optional = _optional_attrs(cfg)
    if not optional:
        return Findings(examined=0)
    ctx = CheckContext(cfg)
    errors: list[str] = []
    for (py_class, attr), rel in sorted(optional.items()):
        node = ctx.stub_classes.get(py_class)
        if node is None:
            continue  # leak gate owns missing classes
        annotation = _annotation_of(node, attr)
        if annotation is None or _admits_none(annotation):
            continue
        errors.append(
            f'{loc(cfg.stub_path, annotation)}: {py_class}.{attr}: Rust surface '
            f'is Option<..> ({cfg.src_root.name}/{rel}) but the stub type '
            f'`{ast.unparse(annotation)}` does not admit None'
        )
    return Findings(errors, examined=len(optional))
