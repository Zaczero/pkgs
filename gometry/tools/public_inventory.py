"""Source-only inventory of gometry's declared public API.

The inventory reads the literal facade ``__all__``, top-level declarations in
the generated native stub, and Python classes directly re-exported by that
facade.  Exported class members include public methods, properties, annotated
attributes, and members inherited from declared bases.  It never imports the
extension or optional dependencies, and it does not attempt to infer names
created by arbitrary execution.
"""

from __future__ import annotations

import ast
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding='utf-8'), filename=str(path))


def load_public_all(init_path: Path) -> frozenset[str]:
    """Read the literal top-level export list from ``__init__.py``."""
    tree = _tree(init_path)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = (node.target,)
        else:
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == '__all__'
            for target in targets
        ):
            continue
        if node.value is None:
            raise RuntimeError('__all__ has no value')
        try:
            value = ast.literal_eval(node.value)
        except Exception as exc:
            raise RuntimeError(f'cannot eval __all__: {exc}') from exc
        if not isinstance(value, (list, tuple)):
            raise TypeError('__all__ is not a list')
        if any(not isinstance(item, str) for item in value):
            raise RuntimeError('__all__ must contain only strings')
        return frozenset(value)
    raise RuntimeError('__all__ not found in __init__.py')


def _is_public_name(name: str) -> bool:
    return not name.startswith('_') or (name.startswith('__') and name.endswith('__'))


def _base_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Subscript):
        return _base_name(node.value)
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _class_declarations(tree: ast.Module) -> dict[str, ast.ClassDef]:
    return {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}


def _class_members(
    name: str,
    declarations: dict[str, ast.ClassDef],
    visiting: set[str] | None = None,
) -> set[str]:
    """Collect declared public members through a cycle-safe class hierarchy."""
    if visiting is None:
        visiting = set()
    if name in visiting:
        return set()
    cls = declarations.get(name)
    if cls is None:
        return set()

    visiting.add(name)
    members: set[str] = set()
    for node in cls.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if _is_public_name(node.name):
                members.add(node.name)
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and _is_public_name(node.target.id)
        ):
            members.add(node.target.id)
    for base in cls.bases:
        base_name = _base_name(base)
        if base_name is not None:
            members.update(_class_members(base_name, declarations, visiting))
    visiting.remove(name)
    return members


def _module_path(
    module: str | None,
    *,
    init_path: Path,
    stub_path: Path,
) -> Path | None:
    if not module:
        return None
    package_name = init_path.parent.name
    prefix = f'{package_name}.'
    if not module.startswith(prefix):
        return None
    if module == f'{package_name}.{stub_path.stem}' and stub_path.is_file():
        return stub_path

    relative = module[len(prefix) :].replace('.', '/')
    candidates = (
        init_path.parent / f'{relative}.py',
        init_path.parent / f'{relative}.pyi',
        init_path.parent / relative / '__init__.py',
    )
    return next((path for path in candidates if path.is_file()), None)


def _facade_imports(
    init_path: Path,
    stub_path: Path,
    public_all: frozenset[str],
) -> list[tuple[str, Path, str]]:
    """Return ``(public name, source path, declared name)`` facade imports."""
    imports: list[tuple[str, Path, str]] = []
    for node in _tree(init_path).body:
        if not isinstance(node, ast.ImportFrom):
            continue
        source_path = _module_path(
            node.module, init_path=init_path, stub_path=stub_path
        )
        if source_path is None:
            continue
        for alias in node.names:
            public_name = alias.asname or alias.name
            if public_name in public_all and alias.name != '*':
                imports.append((public_name, source_path, alias.name))
    return imports


def load_public_symbols(stub_path: Path, init_path: Path) -> frozenset[str]:
    """Return facade names plus declared members of every exported class."""
    public_all = load_public_all(init_path)
    symbols: set[str] = set(public_all)

    stub_tree = _tree(stub_path)
    stub_declarations = _class_declarations(stub_tree)
    for name in public_all:
        if name in stub_declarations:
            symbols.update(
                f'{name}.{member}' for member in _class_members(name, stub_declarations)
            )

    source_declarations: dict[Path, dict[str, ast.ClassDef]] = {}
    for public_name, source_path, declared_name in _facade_imports(
        init_path, stub_path, public_all
    ):
        declarations = source_declarations.setdefault(
            source_path, _class_declarations(_tree(source_path))
        )
        if declared_name not in declarations:
            continue
        symbols.update(
            f'{public_name}.{member}'
            for member in _class_members(declared_name, declarations)
        )
    return frozenset(symbols)


def unknown_public_symbols(
    symbols: Iterable[str], public_symbols: frozenset[str]
) -> tuple[str, ...]:
    """Return sorted symbols absent from the canonical inventory."""
    return tuple(sorted(set(symbols) - public_symbols))
