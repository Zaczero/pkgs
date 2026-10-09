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

from griffe_expand_aliases import (
    ExpandTokenAliases,
    PromoteStubOverloads,
    annotation_aliases,
    expand_annotation,
)

_PUBLIC_TYPEVAR_NAMES = {
    '_CellT': 'CellT',
    '_CellT_co': 'CellT_co',
    '_DefaultT': 'DefaultT',
    '_ExtremeT': 'ExtremeT',
    '_GeometryOtherT': 'GeometryOtherT',
    '_GeometryRowT': 'GeometryRowT',
    '_GeometryT': 'GeometryT',
    '_GeometryT_co': 'GeometryT_co',
    '_GroupKeyT': 'GroupKeyT',
    '_GroupValuesT_co': 'GroupValuesT_co',
}

ParameterForm = tuple[str | None, str, str | None, str | None]
CallForm = tuple[tuple[ParameterForm, ...], str | None]


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


def _expression_text(expression: Any) -> str | None:
    if expression is None:
        return None
    source = str(expression)
    try:
        return ast.unparse(ast.parse(source, mode='eval').body)
    except SyntaxError:
        return f'<unparsed:{source}>'


def _stub_annotation(
    expression: ast.expr | None,
    stub_module: griffe.Module,
    aliases: dict[str, Any],
) -> str | None:
    if expression is None:
        return None
    parsed = griffe.get_expression(expression, stub_module, parse_strings=True)
    if parsed is None:
        return None
    return _public_annotation(str(expand_annotation(parsed, aliases)))


def _model_annotation(expression: Any) -> str | None:
    return _public_annotation(str(expression)) if expression is not None else None


def _stub_is_never(
    expression: ast.expr | None,
    stub_module: griffe.Module,
    aliases: dict[str, Any],
) -> bool:
    annotation = _stub_annotation(expression, stub_module, aliases)
    return annotation is not None and annotation.rsplit('.', 1)[-1] in {
        'Never',
        'NoReturn',
    }


def _stub_call_form(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
    stub_module: griffe.Module,
    aliases: dict[str, Any],
) -> CallForm:
    args = function.args
    positional = [*args.posonlyargs, *args.args]
    defaults = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
    parameters: list[ParameterForm] = []
    for index, (argument, default) in enumerate(zip(positional, defaults, strict=True)):
        kind = (
            'positional-only'
            if index < len(args.posonlyargs)
            else 'positional or keyword'
        )
        parameters.append((
            kind,
            argument.arg,
            _stub_annotation(argument.annotation, stub_module, aliases),
            _expression_text(ast.unparse(default)) if default is not None else None,
        ))
    if args.vararg is not None:
        parameters.append((
            'variadic positional',
            args.vararg.arg,
            _stub_annotation(args.vararg.annotation, stub_module, aliases),
            None,
        ))
    for argument, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        parameters.append((
            'keyword-only',
            argument.arg,
            _stub_annotation(argument.annotation, stub_module, aliases),
            _expression_text(ast.unparse(default)) if default is not None else None,
        ))
    if args.kwarg is not None:
        parameters.append((
            'variadic keyword',
            args.kwarg.arg,
            _stub_annotation(args.kwarg.annotation, stub_module, aliases),
            None,
        ))
    return tuple(parameters), _stub_annotation(function.returns, stub_module, aliases)


def _griffe_call_form(function: griffe.Function) -> CallForm:
    parameters: list[ParameterForm] = []
    for parameter in function.parameters:
        kind = parameter.kind.value if parameter.kind is not None else None
        default = (
            None
            if kind in {'variadic positional', 'variadic keyword'}
            else _expression_text(parameter.default)
        )
        parameters.append((
            kind,
            parameter.name,
            _model_annotation(parameter.annotation),
            default,
        ))
    return tuple(parameters), _model_annotation(function.returns)


def _stub_overloads(
    stub_module: griffe.Module, aliases: dict[str, Any]
) -> dict[str, tuple[CallForm, ...]]:
    """All non-Never overload forms by model path, in authored order."""
    tree = ast.parse(STUB.read_text(encoding='utf-8'))
    groups: list[tuple[str, list[ast.FunctionDef | ast.AsyncFunctionDef]]] = []

    def collect(prefix: str, body: list[ast.stmt]) -> None:
        by_name: dict[str, list[ast.FunctionDef | ast.AsyncFunctionDef]] = {}
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                by_name.setdefault(node.name, []).append(node)
            elif isinstance(node, ast.ClassDef):
                collect(f'{prefix}{node.name}.', node.body)
        groups.extend((f'{prefix}{name}', defs) for name, defs in by_name.items())

    collect('gometry.', tree.body)
    result: dict[str, tuple[CallForm, ...]] = {}
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
            if not _stub_is_never(node.returns, stub_module, aliases)
        ]
        if not concrete:
            # Never-only groups have no accepted forms to publish.
            continue
        result[path] = tuple(
            _stub_call_form(function, stub_module, aliases) for function in concrete
        )
    return result


def _public_overload_paths(
    overloads: dict[str, tuple[CallForm, ...]], public_names: set[str]
) -> set[str]:
    return {
        path
        for path in overloads
        if (parts := path.split('.'))[1] in public_names
        and not parts[-1].startswith('_')
        and not any(part.startswith('_') for part in parts[2:-1])
    }


def _python_function_sources(
    function: griffe.Function,
    pkg: griffe.Module,
    native: griffe.Module,
) -> list[tuple[griffe.Alias, griffe.Function]]:
    package_prefix = f'{pkg.path}.'
    sources: list[tuple[griffe.Alias, griffe.Function]] = []
    for alias in function.aliases.values():
        if not isinstance(alias, griffe.Alias) or alias.parent is not pkg:
            continue
        target_path = getattr(alias, 'target_path', '')
        if not target_path.startswith(package_prefix):
            continue
        try:
            target = alias.target
        except (griffe.AliasResolutionError, griffe.CyclicAliasError):
            continue
        parent = getattr(target, 'parent', None)
        if (
            isinstance(target, griffe.Function)
            and isinstance(parent, griffe.Module)
            and parent.path != native.path
            and parent.path.startswith(package_prefix)
        ):
            sources.append((alias, target))
    return sources


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
    if not isinstance(lib, griffe.Module):
        return ['transformed model has no gometry._lib module']
    aliases = annotation_aliases(pkg)
    stub_overloads = _stub_overloads(lib, aliases)
    public_overloads = _public_overload_paths(stub_overloads, set(gm.__all__))

    # The import inventory is authoritative: every flat public native
    # function/class must still be represented by its same-named native member;
    # Python-defined functions carry provenance through their facade alias.
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
            python_sources = _python_function_sources(public, pkg, lib)
            if python_sources:
                for alias, source in python_sources:
                    if source.canonical_path != alias.target_path:
                        errors.append(
                            f'{name}: Python target canonical path is '
                            f'{source.canonical_path!r}'
                        )
            elif not isinstance(native, griffe.Function):
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

    model_overloads: set[str] = set()
    for function in _functions(pkg, set()):
        if function.name.startswith('_'):
            continue
        path = function.canonical_path
        if path in stub_overloads:
            if path in public_overloads:
                model_overloads.add(path)
            actual_forms = tuple(
                _griffe_call_form(variant) for variant in function.overloads or ()
            )
            if actual_forms != stub_overloads[path]:
                errors.append(
                    f'{path}: accepted call forms differ from the authored overload sequence'
                )
        variants = [function, *(function.overloads or ())]
        if any(_is_never(variant) for variant in variants):
            errors.append(f'{function.canonical_path}: public Never overload')
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

    errors.extend(
        f'{path}: accepted overload group missing from the Griffe model'
        for path in sorted(public_overloads - model_overloads)
        if stub_overloads[path]
    )

    cell_array = pkg.members.get('CellArray')
    if isinstance(cell_array, griffe.Class):
        for name, geometry_type in (('center', 'Point'), ('polygon', 'Polygon')):
            attribute = cell_array.members.get(name)
            expected = f'GeometryArray[{geometry_type} | None]'
            if not isinstance(attribute, griffe.Attribute):
                errors.append(f'gometry.CellArray.{name}: property reference missing')
                continue
            if (
                attribute.canonical_path != f'gometry.CellArray.{name}'
                or _public_annotation(str(attribute.annotation)) != expected
            ):
                errors.append(
                    f'gometry.CellArray.{name}: property annotation/path is not '
                    f'{expected} at its public anchor'
                )
            if not {'property', 'readonly'}.issubset(attribute.labels):
                errors.append(
                    f'gometry.CellArray.{name}: property/read-only labels missing'
                )
            if attribute.docstring is None or not attribute.docstring.value.strip():
                errors.append(f'gometry.CellArray.{name}: property prose missing')

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
