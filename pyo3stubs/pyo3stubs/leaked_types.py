"""Leak gate: reachable PyO3 types must be registered and stubbed."""

from __future__ import annotations

import ast
import inspect
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pyo3stubs.config import StubConfig

# Any `#[pyclass]` attribute (with or without arguments) followed by its
# struct/enum declaration, tolerating other attributes in between. The Python
# name is the `name = "..."` argument when present, else the Rust identifier —
# an unnamed `#[pyclass]` exports under its struct name and leaks just the same.
from pyo3stubs.rust_scan import (  # noqa: F401 — re-exported
    DEFAULT_PYCLASS,
    PYCLASS_NAME_ARG,
)
from pyo3stubs.rust_scan import pyclass_names as collect_pyclass_names

DEFAULT_IGNORED_TYPE_NAMES: frozenset[str] = frozenset({
    'Any',
    'Buffer',
    'Callable',
    'ClassVar',
    'Final',
    'Generic',
    'Iterable',
    'Iterator',
    'Literal',
    'Mapping',
    'MutableMapping',
    'Protocol',
    'Self',
    'Sequence',
    'TypedDict',
    'TypeVar',
    'Union',
    'bool',
    'bytes',
    'dict',
    'float',
    'frozenset',
    'int',
    'list',
    'object',
    'override',
    'set',
    'str',
    'tuple',
    'type',
    'npt',
    'np',
    'numpy',
    'NDArray',
    'BaseException',
    'Exception',
    'ValueError',
    'TypeError',
    'RuntimeError',
    'IndexError',
    'StopIteration',
    'BufferError',
})


def _registered_class_names(runtime_module: object) -> set[str]:
    return {
        name
        for name in dir(runtime_module)
        if inspect.isclass(getattr(runtime_module, name, None))
    }


def _stub_class_names(stub_path: Path) -> set[str]:
    tree = ast.parse(stub_path.read_text())
    return {node.name for node in tree.body if isinstance(node, ast.ClassDef)}


def _public_stub_class_names(stub_path: Path) -> set[str]:
    return {name for name in _stub_class_names(stub_path) if not name.startswith('_')}


def _annotation_type_names(node: ast.expr | None) -> set[str]:
    if node is None:
        return set()
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        # Quoted forward reference: parse and recurse.
        try:
            return _annotation_type_names(ast.parse(node.value, mode='eval').body)
        except SyntaxError:
            return set()
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, ast.Attribute):
        if isinstance(node.value, ast.Name):
            return {node.attr}
        return _annotation_type_names(node.value)
    if isinstance(node, ast.Subscript):
        names = _annotation_type_names(node.value)
        if isinstance(node.slice, ast.Tuple):
            for elt in node.slice.elts:
                names |= _annotation_type_names(elt)
        else:
            names |= _annotation_type_names(node.slice)
        return names
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        return _annotation_type_names(node.left) | _annotation_type_names(node.right)
    if isinstance(node, ast.Tuple):
        names: set[str] = set()
        for elt in node.elts:
            names |= _annotation_type_names(elt)
        return names
    return set()


def _collect_stub_signature_leaks(
    stub_path: Path,
    pyclass_names: set[str],
    public_stub_classes: set[str],
    *,
    leak_allowlist: dict[str, str],
    ignored_type_names: frozenset[str],
) -> list[str]:
    tree = ast.parse(stub_path.read_text())
    errors: list[str] = []

    def check_refs(symbol: str, refs: set[str]) -> None:
        for ref in sorted(refs):
            if ref.startswith('_'):
                continue
            if ref in ignored_type_names:
                continue
            if ref in leak_allowlist:
                continue
            if ref not in pyclass_names:
                continue
            if ref in public_stub_classes:
                continue
            errors.append(
                f'{symbol}: annotation references leaked pyclass {ref!r} — '
                f'add a public stub class or register the type'
            )

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            if node.name.startswith('_'):
                continue
            for base in node.bases:
                check_refs(f'class {node.name}', _annotation_type_names(base))
            for child in node.body:
                if isinstance(child, ast.FunctionDef) and not child.name.startswith(
                    '_'
                ):
                    refs = _annotation_type_names(child.returns)
                    for arg in child.args.args:
                        refs |= _annotation_type_names(arg.annotation)
                    for arg in child.args.kwonlyargs:
                        refs |= _annotation_type_names(arg.annotation)
                    check_refs(f'{node.name}.{child.name}', refs)
                elif isinstance(child, ast.AnnAssign) and isinstance(
                    child.target, ast.Name
                ):
                    check_refs(
                        f'{node.name}.{child.target.id}',
                        _annotation_type_names(child.annotation),
                    )
        elif isinstance(node, ast.FunctionDef) and not node.name.startswith('_'):
            refs = _annotation_type_names(node.returns)
            for arg in node.args.args:
                refs |= _annotation_type_names(arg.annotation)
            for arg in node.args.kwonlyargs:
                refs |= _annotation_type_names(arg.annotation)
            check_refs(node.name, refs)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            check_refs(node.target.id, _annotation_type_names(node.annotation))

    return errors


def collect_errors(cfg: StubConfig) -> list[str]:
    """Flag registration leaks and stub reachability leaks."""
    import importlib

    runtime = importlib.import_module(cfg.module)
    pyclass_map = collect_pyclass_names(cfg)
    pyclass_names = set(pyclass_map)
    registered = _registered_class_names(runtime)
    public_stub_classes = _public_stub_class_names(cfg.stub_path)
    ignored = cfg.ignored_type_names or DEFAULT_IGNORED_TYPE_NAMES
    errors: list[str] = []

    for name, rel_path in sorted(pyclass_map.items()):
        if name.startswith('_'):
            continue
        if name in cfg.leak_allowlist:
            continue
        if name in registered:
            continue
        reason = cfg.leak_allowlist.get(name)
        detail = f' ({reason})' if reason else ''
        errors.append(
            f'{rel_path}: pyclass {name!r} is not registered on {cfg.module}{detail}'
        )

    errors.extend(
        _collect_stub_signature_leaks(
            cfg.stub_path,
            pyclass_names,
            public_stub_classes,
            leak_allowlist=cfg.leak_allowlist,
            ignored_type_names=ignored,
        )
    )
    return errors
