"""Source-only inventory of gometry's public API symbols.

The inventory intentionally reads ``__all__`` and the generated stub instead
of importing the extension.  Both documentation and benchmark gates use this
same parser so class members cannot disappear from one contract while staying
visible in the other.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable
from pathlib import Path


def load_public_all(init_path: Path) -> frozenset[str]:
    """Read the literal top-level export list from ``__init__.py``."""
    tree = ast.parse(init_path.read_text(encoding='utf-8'), filename=str(init_path))
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
            raise RuntimeError('__all__ is not a list')
        return frozenset(str(item) for item in value)
    raise RuntimeError('__all__ not found in __init__.py')


def load_public_symbols(stub_path: Path, init_path: Path) -> frozenset[str]:
    """Return ``__all__`` symbols plus public members of exported classes."""
    public_all = load_public_all(init_path)
    tree = ast.parse(stub_path.read_text(encoding='utf-8'), filename=str(stub_path))
    symbols: set[str] = set(public_all)

    def is_public_member(node: ast.AST) -> bool:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return False
        return not node.name.startswith('_') or (
            node.name.startswith('__') and node.name.endswith('__')
        )

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name in public_all:
                symbols.add(node.name)
        elif isinstance(node, ast.ClassDef) and node.name in public_all:
            symbols.add(node.name)
            symbols.update(
                f'{node.name}.{member.name}'
                for member in node.body
                if is_public_member(member)
            )
    return frozenset(symbols)


def unknown_public_symbols(
    symbols: Iterable[str], public_symbols: frozenset[str]
) -> tuple[str, ...]:
    """Return sorted symbols absent from the canonical inventory."""
    return tuple(sorted(set(symbols) - public_symbols))
