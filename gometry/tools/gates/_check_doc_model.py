#!/usr/bin/env python3
"""Validate the transformed Griffe model used by the documentation build."""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Any

import griffe

ROOT = Path(__file__).resolve().parents[2]
STUB = ROOT / 'python/gometry/_lib.pyi'
sys.path.insert(0, str(ROOT / 'tools/docs'))

from griffe_expand_aliases import ExpandTokenAliases, PromoteStubOverloads

_PUBLIC_TYPEVAR_NAMES = {
    '_CellT': 'CellT',
    '_CellT_co': 'CellT_co',
    '_DefaultT': 'DefaultT',
    '_ExtremeT': 'ExtremeT',
    '_GeometryOtherT': 'GeometryOtherT',
    '_GeometryT': 'GeometryT',
    '_GeometryT_co': 'GeometryT_co',
    '_GroupKeyT': 'GroupKeyT',
    '_GroupValuesT_co': 'GroupValuesT_co',
}


def _is_never(function: griffe.Function) -> bool:
    returns = function.returns
    return isinstance(returns, griffe.ExprName) and returns.name in {
        'Never',
        'NoReturn',
    }


def _public_annotation(annotation: str) -> str:
    for private, public in _PUBLIC_TYPEVAR_NAMES.items():
        annotation = annotation.replace(private, public)
    return annotation


def _expressions(expr: Any) -> list[griffe.ExprName]:
    if isinstance(expr, griffe.ExprName):
        return [expr]
    found: list[griffe.ExprName] = []
    for attr in ('left', 'right', 'slice', 'elements'):
        child = getattr(expr, attr, None)
        if isinstance(child, list):
            for item in child:
                found.extend(_expressions(item))
        elif child is not None:
            found.extend(_expressions(child))
    return found


def _functions(obj: Any, seen: set[int]) -> list[griffe.Function]:
    if id(obj) in seen or isinstance(obj, griffe.Alias):
        return []
    seen.add(id(obj))
    result = [obj] if isinstance(obj, griffe.Function) else []
    for member in getattr(obj, 'members', {}).values():
        result.extend(_functions(member, seen))
    return result


def _stub_overloads() -> dict[str, tuple[str, tuple[str, ...]]]:
    """Final non-Never overload return and parameters by public model path."""
    tree = ast.parse(STUB.read_text(encoding='utf-8'))
    groups: list[tuple[str, list[ast.FunctionDef]]] = []

    def collect(prefix: str, body: list[ast.stmt]) -> None:
        by_name: dict[str, list[ast.FunctionDef]] = {}
        for node in body:
            if isinstance(node, ast.FunctionDef):
                by_name.setdefault(node.name, []).append(node)
            elif isinstance(node, ast.ClassDef):
                collect(f'{prefix}{node.name}.', node.body)
        groups.extend((f'{prefix}{name}', defs) for name, defs in by_name.items())

    collect('gometry.', tree.body)
    result: dict[str, tuple[str, tuple[str, ...]]] = {}
    for path, defs in groups:
        overloads = [
            node
            for node in defs
            if any(
                isinstance(decorator, ast.Name) and decorator.id == 'overload'
                for decorator in node.decorator_list
            )
        ]
        if not overloads:
            continue
        concrete = [
            node
            for node in overloads
            if ast.unparse(node.returns) not in {'Never', 'NoReturn'}
        ]
        if not concrete:
            continue
        final = concrete[-1]
        params = tuple(
            argument.arg
            for argument in [
                *final.args.posonlyargs,
                *final.args.args,
                *final.args.kwonlyargs,
            ]
            if argument.arg not in {'self', 'cls'}
        )
        result[path] = (ast.unparse(final.returns), params)
    return result


def collect_errors() -> list[str]:
    import gometry as gm

    loader = griffe.GriffeLoader(
        search_paths=[str(ROOT / 'python')],
        extensions=griffe.Extensions(PromoteStubOverloads(), ExpandTokenAliases()),
        allow_inspection=False,
    )
    pkg = loader.load('gometry')
    loader.resolve_aliases(external=False)
    lib = pkg.members.get('_lib')
    errors: list[str] = []
    stub_overloads = _stub_overloads()
    if not isinstance(lib, griffe.Module):
        return ['transformed model has no gometry._lib module']

    # The import inventory is authoritative: every flat public function/class
    # must still be represented by its same-named native member.
    for name in gm.__all__:
        public = pkg.members.get(name)
        native = lib.members.get(name)
        if public is None:
            errors.append(f'{name}: missing public/native model member')
            continue
        # Structured result classes are intentionally private-stub-owned; they
        # have no native class and are still public canonical anchors.
        stub_owned = name in {'Extremes', 'Features', 'PolygonizeResult'}
        if public.canonical_path != f'gometry.{name}' and not stub_owned:
            errors.append(f'{name}: canonical path is {public.canonical_path!r}')
        if isinstance(public, griffe.Function):
            if not isinstance(native, griffe.Function):
                errors.append(f'{name}: missing native function provenance')
        elif isinstance(public, griffe.Class):
            targets = {
                alias.target_path
                for alias in public.aliases.values()
                if isinstance(alias, griffe.Alias)
            }
            if f'gometry._lib.{name}' not in targets and (
                name == 'Cell' and f'gometry._types.{name}' not in targets
            ):
                errors.append(f'{name}: missing native class provenance')

    for function in _functions(pkg, set()):
        if function.name.startswith('_'):
            continue
        variants = [function, *(function.overloads or ())]
        if any(_is_never(variant) for variant in variants):
            errors.append(f'{function.canonical_path}: public Never overload')
        if expected := stub_overloads.get(function.canonical_path):
            expected_return, expected_params = expected
            actual_params = tuple(
                parameter.name
                for parameter in function.parameters
                if parameter.name not in {'self', 'cls'}
            )
            if (
                _public_annotation(str(function.returns))
                != _public_annotation(expected_return)
                or actual_params != expected_params
            ):
                errors.append(
                    f'{function.canonical_path}: canonical overload differs from '
                    'the final non-Never stub overload'
                )
        for variant in variants:
            annotations = [parameter.annotation for parameter in variant.parameters]
            annotations.append(variant.returns)
            private = sorted({
                expr.name
                for annotation in annotations
                for expr in _expressions(annotation)
                if expr.name.startswith('_')
            })
            if private:
                errors.append(
                    f'{function.canonical_path}: private annotations {private}'
                )

    for module_name in ('_lib', '_types'):
        module = pkg.members.get(module_name)
        if not isinstance(module, griffe.Module):
            continue
        for name in gm.__all__:
            member = module.members.get(name)
            if (
                isinstance(member, griffe.Alias)
                and name == 'Cell'
                and member.canonical_path != f'gometry.{name}'
            ):
                errors.append(
                    f'{module_name}.{name}: canonical path is {member.canonical_path!r}'
                )
    return errors


def main() -> int:
    errors = collect_errors()
    if errors:
        print(f'doc model: {len(errors)} issue(s):', file=sys.stderr)
        for error in errors:
            print(f'  {error}', file=sys.stderr)
        return 1
    print('doc model: OK')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
