"""Leak gate: reachable PyO3 types must be registered and stubbed.

Two ways a ``#[pyclass]`` leaks. It is declared but never registered on the
module, so it exists at runtime and cannot be named or imported; or a public
signature mentions it while the stub has no public class of that name, so the
type a caller receives has no reachable spelling.
"""

from __future__ import annotations

import ast
from typing import TYPE_CHECKING

from pyo3stubs.context import CheckContext
from pyo3stubs.report import Findings, loc, unused_allowlist_errors
from pyo3stubs.rust_scan import pyclass_names

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig

#: Annotation names that are never a PyO3 class: typing vocabulary, builtins,
#: the exception hierarchy PyO3 raises through, and the numpy spellings that
#: appear in array signatures. Projects add to this through
#: ``StubConfig.extra_ignored_type_names``.
DEFAULT_IGNORED_TYPE_NAMES: frozenset[str] = frozenset({
    'Any',
    'BaseException',
    'BufferError',
    'Buffer',
    'Callable',
    'ClassVar',
    'Exception',
    'Final',
    'Generic',
    'IndexError',
    'Iterable',
    'Iterator',
    'Literal',
    'Mapping',
    'MutableMapping',
    'NDArray',
    'Protocol',
    'RuntimeError',
    'Self',
    'Sequence',
    'StopIteration',
    'TypeError',
    'TypedDict',
    'TypeVar',
    'Union',
    'ValueError',
    'bool',
    'bytes',
    'dict',
    'float',
    'frozenset',
    'int',
    'list',
    'np',
    'npt',
    'numpy',
    'object',
    'override',
    'set',
    'str',
    'tuple',
    'type',
})


def _all_args(args: ast.arguments) -> list[ast.arg]:
    """Every annotated parameter, including the kinds a partial walk misses.

    `posonlyargs`, `*args` and `**kwargs` were skipped, so a leaked pyclass in
    a positional-only parameter was invisible -- and gometry's stub alone has
    136 `, /` markers. `structural._parameters` already walks all five kinds;
    the two modules disagreed about what a parameter list is.
    """
    optional = [args.vararg, args.kwarg]
    return [
        *args.posonlyargs,
        *args.args,
        *args.kwonlyargs,
        *[arg for arg in optional if arg is not None],
    ]


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
        names = set()
        for elt in node.elts:
            names |= _annotation_type_names(elt)
        return names
    return set()


def _signature_type_names(node: ast.FunctionDef) -> set[str]:
    names = _annotation_type_names(node.returns)
    for arg in _all_args(node.args):
        names |= _annotation_type_names(arg.annotation)
    return names


def collect_errors(cfg: StubConfig) -> Findings:
    """Flag registration leaks, stub reachability leaks, and allowlist rot."""
    ctx = CheckContext(cfg)
    declared = pyclass_names(cfg)
    runtime = ctx.runtime_module
    registered = {
        name for name in dir(runtime) if isinstance(getattr(runtime, name, None), type)
    }
    public_stub_classes = {
        name for name in ctx.stub_classes if not name.startswith('_')
    }
    ignored = DEFAULT_IGNORED_TYPE_NAMES | cfg.extra_ignored_type_names
    used_allowlist: set[str] = set()
    errors: list[str] = []

    for name, rel in sorted(declared.items()):
        if name.startswith('_') or name in registered:
            continue
        if name in cfg.leak_allowlist:
            used_allowlist.add(name)
            continue
        errors.append(
            f'{cfg.src_root.name}/{rel}: pyclass {name!r} is not registered on '
            f'{cfg.module}'
        )

    def check(symbol: str, node: ast.stmt, refs: set[str]) -> None:
        for ref in sorted(refs):
            if ref.startswith('_') or ref in ignored or ref not in declared:
                continue
            if ref in cfg.leak_allowlist:
                used_allowlist.add(ref)
                continue
            if ref in public_stub_classes:
                continue
            errors.append(
                f'{loc(cfg.stub_path, node)}: {symbol}: annotation references '
                f'leaked pyclass {ref!r} — add a public stub class or register '
                f'the type'
            )

    for node in ctx.stub_ast.body:
        if isinstance(node, ast.ClassDef):
            if node.name.startswith('_'):
                continue
            for base in node.bases:
                check(f'class {node.name}', node, _annotation_type_names(base))
            for child in node.body:
                if isinstance(child, ast.FunctionDef) and not child.name.startswith(
                    '_'
                ):
                    check(
                        f'{node.name}.{child.name}',
                        child,
                        _signature_type_names(child),
                    )
                elif isinstance(child, ast.AnnAssign) and isinstance(
                    child.target, ast.Name
                ):
                    check(
                        f'{node.name}.{child.target.id}',
                        child,
                        _annotation_type_names(child.annotation),
                    )
        elif isinstance(node, ast.FunctionDef) and not node.name.startswith('_'):
            check(node.name, node, _signature_type_names(node))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            check(node.target.id, node, _annotation_type_names(node.annotation))

    errors += unused_allowlist_errors('leak', cfg.leak_allowlist, used_allowlist)
    public = [name for name in declared if not name.startswith('_')]
    return Findings(errors, examined=len(public))
