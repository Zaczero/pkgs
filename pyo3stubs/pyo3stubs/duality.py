"""Config-activated gate: scalar↔array return duality.

Array returns are DERIVED from the scalar contract. For each configured
``(scalar_class, array_class)`` pair and every public method both classes
define: when every scalar return is kind-preserving (``Self`` or an element
TypeVar), the array method must return ``Self``; otherwise it must return
``ArrayClass[<union of scalar leaf returns>]`` (a scalar return of
``ArrayClass[X]`` — an expansion op — contributes ``X``).

That union binds the catch-all array overload, the last of the group. An
earlier overload narrows by element kind, so each of its elements must be a
stub subclass of a union member, or an element TypeVar when the scalar
method has a kind-preserving return.

Activated by ``StubConfig.duality``; a reported no-op when unset.
"""

from __future__ import annotations

import ast
from typing import TYPE_CHECKING

from pyo3stubs.context import CheckContext
from pyo3stubs.report import Findings, loc, unused_allowlist_errors

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig


def _flatten_union(expr: ast.expr) -> list[ast.expr]:
    if isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.BitOr):
        return [*_flatten_union(expr.left), *_flatten_union(expr.right)]
    return [expr]


def _return_atoms(node: ast.FunctionDef) -> frozenset[str] | None:
    if node.returns is None:
        return None
    return frozenset(ast.unparse(atom) for atom in _flatten_union(node.returns))


def _class_methods(
    tree: ast.Module, class_name: str
) -> dict[str, list[ast.FunctionDef]]:
    for top in tree.body:
        if isinstance(top, ast.ClassDef) and top.name == class_name:
            methods: dict[str, list[ast.FunctionDef]] = {}
            for node in top.body:
                if isinstance(node, ast.FunctionDef):
                    methods.setdefault(node.name, []).append(node)
            return methods
    return {}


def _stub_bases(tree: ast.Module) -> dict[str, list[str]]:
    return {
        top.name: [ast.unparse(base) for base in top.bases]
        for top in tree.body
        if isinstance(top, ast.ClassDef)
    }


def _type_vars(tree: ast.Module) -> frozenset[str]:
    return frozenset(
        target.id
        for top in tree.body
        if isinstance(top, ast.Assign)
        and isinstance(top.value, ast.Call)
        and ast.unparse(top.value.func) == 'TypeVar'
        for target in top.targets
        if isinstance(target, ast.Name)
    )


def _is_subclass(name: str, ancestor: str, bases: dict[str, list[str]]) -> bool:
    return name == ancestor or any(
        _is_subclass(base, ancestor, bases) for base in bases.get(name, ())
    )


def _unwrap_array(atom: str, array_class: str) -> str | None:
    """``ArrayClass[X]`` -> ``X`` source; None when not that shape."""
    prefix = f'{array_class}['
    if atom.startswith(prefix) and atom.endswith(']'):
        return atom[len(prefix) : -1]
    return None


def collect_errors(cfg: StubConfig) -> Findings:
    """Scalar↔array method return parity for configured class pairs."""
    conf = cfg.duality
    if conf is None:
        return Findings()
    ctx = CheckContext(cfg)
    path = cfg.stub_path
    errors: list[str] = []
    used_exempt: set[str] = set()
    compared = 0
    bases = _stub_bases(ctx.stub_ast)
    type_vars = _type_vars(ctx.stub_ast)
    for scalar_class, array_class in conf.pairs:
        scalar_methods = _class_methods(ctx.stub_ast, scalar_class)
        array_methods = _class_methods(ctx.stub_ast, array_class)
        for name, array_defs in sorted(array_methods.items()):
            if name.startswith('_'):
                continue
            scalar_defs = scalar_methods.get(name)
            if not scalar_defs:
                continue
            if name in conf.exempt:
                used_exempt.add(name)
                continue
            compared += 1
            array_returns = [
                (node, atoms)
                for node in array_defs
                if (atoms := _return_atoms(node)) is not None
                and all(
                    atom == 'Self' or _unwrap_array(atom, array_class) is not None
                    for atom in atoms
                )
            ]
            if not array_returns:
                continue
            scalar_atom_sets = [
                atoms for node in scalar_defs if (atoms := _return_atoms(node))
            ]
            if not scalar_atom_sets:
                continue
            scalar_atoms = frozenset().union(*scalar_atom_sets)
            kind_preserving = {'Self', *conf.self_atoms}
            preserving = scalar_atoms <= kind_preserving
            if preserving:
                expected_label = 'Self'
                expected_atoms = frozenset({'Self'})
            else:
                elements: set[str] = set()
                for atom in scalar_atoms:
                    inner = _unwrap_array(atom, array_class)
                    elements.update(e.strip() for e in (inner or atom).split('|'))
                # `Self` is a subclass of the scalar class, so a wider member
                # already covers it.
                if any(_is_subclass(scalar_class, e, bases) for e in elements):
                    elements -= kind_preserving
                expected_label = f'{array_class}[{" | ".join(sorted(elements))}]'
                expected_atoms = frozenset(elements)
            catch_all = array_defs[-1]
            for node, atoms in array_returns:
                if preserving:
                    actual_ok = atoms == {'Self'}
                else:
                    inners: set[str] = set()
                    actual_ok = True
                    for atom in atoms:
                        inner = _unwrap_array(atom, array_class)
                        if inner is None:
                            actual_ok = False
                            break
                        inners.update(e.strip() for e in inner.split('|'))
                    inners.discard('None')
                    if node is catch_all:
                        actual_ok = actual_ok and inners == expected_atoms
                    else:
                        actual_ok = actual_ok and all(
                            any(_is_subclass(e, x, bases) for x in expected_atoms)
                            or (e in type_vars and bool(scalar_atoms & kind_preserving))
                            for e in inners
                        )
                if not actual_ok:
                    scalar_label = ' | '.join(sorted(scalar_atoms))
                    errors.append(
                        f'{loc(path, node)}: {array_class}.{name}: return '
                        f'`{" | ".join(sorted(atoms))}` breaks scalar<->array '
                        f'duality — scalar returns `{scalar_label}`, so the '
                        f'array form must return `{expected_label}` (exempt '
                        f'via DualityConfig.exempt with a reason if deliberate)'
                    )
    return Findings(
        errors + unused_allowlist_errors('duality exempt', conf.exempt, used_exempt),
        examined=compared,
    )
