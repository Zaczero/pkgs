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
from pyo3stubs.rust_scan import DEFAULT_PYCLASS, PYCLASS_NAME_ARG, rust_class_map, sanitize

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig

PYMETHODS_IMPL = re.compile(r'#\s*\[\s*pymethods\s*\]\s*impl\s+(?:<[^>]*>\s*)?(\w+)')
# `#[getter]`, `#[getter(name)]`, and the quoted `#[getter("name")]` form
GETTER_FN = re.compile(
    r'#\s*\[\s*getter\s*(?:\(\s*"?([\w.]+)"?\s*\))?\s*\]'
    r'(?:\s*#\s*\[[^\]]*\])*'
    r'\s*(?:pub(?:\([^)]*\))?\s+)?fn\s+(\w+)\s*(?:<[^>]*>)?\s*\([^)]*\)\s*->\s*([^{;]+)',
)
GET_FIELD = re.compile(
    r'#\s*\[\s*pyo3\s*\(\s*get[^)]*\)\s*\]'
    r'(?:\s*#\s*\[[^\]]*\])*'
    r'\s*(?:pub(?:\([^)]*\))?\s+)?(\w+)\s*:\s*([^,}]+)',
)


def _returns_option(rust_type: str) -> bool:
    """True when the (possibly ``PyResult``-wrapped) type is ``Option<...>``."""
    text = rust_type.strip()
    inner = re.match(r'PyResult\s*<(.*)>\s*$', text)
    if inner:
        text = inner.group(1).strip()
    return text.startswith(('Option<', 'Option <'))


def _impl_body(text: str, start: int) -> str:
    """The brace-balanced body of the impl block opening at/after ``start``.

    Braces are counted over the sanitized text (comments and string contents
    blanked) so a ``{`` inside a doc comment or literal cannot derail the
    scan; the returned slice is from the ORIGINAL text.
    """
    clean = sanitize(text)
    open_brace = clean.index('{', start)
    depth, index = 1, open_brace + 1
    while depth and index < len(clean):
        if clean[index] == '{':
            depth += 1
        elif clean[index] == '}':
            depth -= 1
        index += 1
    return text[open_brace:index]


def _optional_attrs(cfg: StubConfig) -> dict[tuple[str, str], str]:
    """``(python class, attribute) -> defining path`` for Option-typed surfaces."""
    classes = rust_class_map(cfg)
    found: dict[tuple[str, str], str] = {}
    for path in sorted(cfg.src_root.rglob('*.rs')):
        text = path.read_text()
        rel = path.relative_to(cfg.src_root).as_posix()
        for match in PYMETHODS_IMPL.finditer(text):
            py_class = classes.get(match.group(1))
            if py_class is None:
                continue
            body = _impl_body(text, match.end())
            for getter in GETTER_FN.finditer(body):
                if not _returns_option(getter.group(3)):
                    continue
                name = getter.group(1) or getter.group(2)
                name = name.removeprefix('get_')
                found.setdefault((py_class, name), rel)
        for match in DEFAULT_PYCLASS.finditer(text):
            name_arg = PYCLASS_NAME_ARG.search(match.group('args') or '')
            py_class = name_arg.group(1) if name_arg else match.group('ident')
            body = _impl_body(text, match.end())
            for field in GET_FIELD.finditer(body):
                if _returns_option(field.group(2)):
                    found.setdefault((py_class, field.group(1)), rel)
    return found


def _admits_none(annotation: ast.expr) -> bool:
    if isinstance(annotation, ast.Constant):
        return annotation.value is None or (
            isinstance(annotation.value, str) and 'None' in annotation.value
        )
    if isinstance(annotation, ast.BinOp):
        return _admits_none(annotation.left) or _admits_none(annotation.right)
    if isinstance(annotation, ast.Subscript):
        value = annotation.value
        if isinstance(value, ast.Name) and value.id == 'Optional':
            return True
        if isinstance(annotation.slice, ast.Tuple):
            return any(_admits_none(elt) for elt in annotation.slice.elts)
        return _admits_none(annotation.slice)
    if isinstance(annotation, ast.Name):
        return annotation.id == 'None'
    return False


def collect_errors(cfg: StubConfig) -> list[str]:
    """Stub annotations that deny ``None`` for Option-typed Rust surfaces."""
    ctx = CheckContext(cfg)
    optional = _optional_attrs(cfg)
    if not optional:
        return []
    stub_classes: dict[str, ast.ClassDef] = {
        node.name: node
        for node in ctx.stub_ast.body
        if isinstance(node, ast.ClassDef)
    }
    errors: list[str] = []
    for (py_class, attr), rel in sorted(optional.items()):
        node = stub_classes.get(py_class)
        if node is None:
            continue  # leak gate owns missing classes
        for stmt in node.body:
            annotation: ast.expr | None = None
            lineno = stmt.lineno
            if (
                isinstance(stmt, ast.AnnAssign)
                and isinstance(stmt.target, ast.Name)
                and stmt.target.id == attr
            ):
                annotation = stmt.annotation
            elif isinstance(stmt, ast.FunctionDef) and stmt.name == attr:
                annotation = stmt.returns
            if annotation is None:
                continue
            if not _admits_none(annotation):
                errors.append(
                    f'{cfg.stub_path}:{lineno}: {py_class}.{attr}: Rust surface '
                    f'is Option<..> (src/{rel}) but the stub type '
                    f'`{ast.unparse(annotation)}` does not admit None'
                )
            break
    return errors
