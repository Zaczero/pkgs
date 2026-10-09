#!/usr/bin/env python3
"""Validate the transformed Griffe model used by the documentation build."""

from __future__ import annotations

import ast
import inspect
import re
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import griffe

ROOT = Path(__file__).resolve().parents[2]
STUB = ROOT / 'python/gometry/_lib.pyi'
_TOOLS_ROOT = ROOT / 'tools'
if str(_TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TOOLS_ROOT))

from _gatelib import prepend_rust_source_import_paths

prepend_rust_source_import_paths()

from pyo3stubs.ast_util import annotated_data_descriptors
from pyo3stubs.cli import load_config
from pyo3stubs.context import CheckContext
from pyo3stubs.doc_structure import (
    DocSubject,
    attribute_entries,
    iter_doc_subjects,
    parameter_entries,
    parse_doc,
    return_entries,
)

from griffe_expand_aliases import ExpandTokenAliases, PromoteStubOverloads

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
_ARRAY_DOCUMENTATION_OWNERS = frozenset({'GeometryArray', 'CellArray'})
_NDARRAY_TYPE = re.compile(r'(?<![\w.])(?:np\.ndarray|ndarray)(?![\w.])')
_PER_ELEMENT = re.compile(r'\bper element\b', re.IGNORECASE)
_PLANAR = re.compile(r'\(planar\)', re.IGNORECASE)
_AUTO_SPLIT_OPS = frozenset({
    'contains',
    'within',
    'covers',
    'covered_by',
    'intersects',
    'disjoint',
    'touches',
    'crosses',
    'overlaps',
    'equals',
    'relate',
    'relate_pattern',
    'intersection',
    'union',
    'difference',
    'symmetric_difference',
    'clip_by_rect',
})
_GENUINELY_PLANAR_OPS = frozenset({
    'simplify',
    'smooth',
    'convex_hull',
    'buffer',
    'offset_curve',
    'segmentize',
})


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


def _mentions_type(type_text: str, name: str) -> bool:
    """Find a type name as a parsed identifier, not a substring."""
    normalized = type_text.replace('`', '').replace(' or ', ' | ')
    try:
        expression = ast.parse(normalized, mode='eval')
    except SyntaxError:
        return False
    return any(
        isinstance(node, ast.Name) and node.id == name
        for node in ast.walk(expression)
    )


def _scalar_array_return_errors(stub: Path, runtime: object) -> list[str]:
    tree = ast.parse(stub.read_text(encoding='utf-8'), filename=str(stub))
    errors: list[str] = []
    array_classes = {'GeometryArray', 'CellArray'}
    for cls in tree.body:
        if not isinstance(cls, ast.ClassDef) or cls.name in array_classes:
            continue
        runtime_cls = getattr(runtime, cls.name, None)
        descriptor_docs = {}
        if isinstance(runtime_cls, type):
            descriptor_docs = {
                id(statement): doc
                for _name, statement, doc, _descriptor
                in annotated_data_descriptors(runtime_cls, cls.body)
            }
        for member in cls.body:
            if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                doc = ast.get_docstring(member, clean=True)
                if doc and member.returns is not None:
                    returns = return_entries(parse_doc(doc))
                    if any(
                        _mentions_type(entry.type_text, 'GeometryArray')
                        for entry in returns
                    ) and not any(
                        isinstance(node, ast.Name) and node.id == 'GeometryArray'
                        for node in ast.walk(member.returns)
                    ):
                        errors.append(
                            f'{stub.name}:{member.lineno}: {cls.name}.{member.name}: '
                            'Returns type names GeometryArray but the binding '
                            f'annotation is {ast.unparse(member.returns)}'
                        )
                continue
            if not isinstance(member, ast.AnnAssign) or not isinstance(
                runtime_cls, type
            ):
                continue
            if not isinstance(member.target, ast.Name):
                continue
            doc = descriptor_docs.get(id(member))
            if doc is None:
                continue
            if any(
                _mentions_type(entry.type_text, 'GeometryArray')
                for entry in return_entries(parse_doc(doc))
            ) and not any(
                isinstance(node, ast.Name) and node.id == 'GeometryArray'
                for node in ast.walk(member.annotation)
            ):
                errors.append(
                    f'{stub.name}:{member.lineno}: {cls.name}.{member.target.id}: '
                    'Returns type names GeometryArray but the binding '
                    f'annotation is {ast.unparse(member.annotation)}'
                )
    return errors


def _has_unquoted_enum(type_text: str) -> bool:
    for match in re.finditer(r'\{([^{}]*)\}', type_text):
        try:
            expression = ast.parse('{' + match.group(1) + '}', mode='eval').body
        except SyntaxError:
            continue
        if isinstance(expression, ast.Set) and any(
            isinstance(node, (ast.Name, ast.Attribute))
            for element in expression.elts
            for node in ast.walk(element)
        ):
            return True
    return False


def _doc_content_errors(subjects: Iterable[DocSubject]) -> list[str]:
    """Retain factual doc claims while leaving editorial prose to the author."""
    errors: list[str] = []
    for subject in subjects:
        prose = subject.prose
        if not prose:
            continue
        qualname = subject.qualname
        bare_name = qualname.rsplit('.', 1)[-1]
        owner = qualname.rpartition('.')[0]
        parsed = parse_doc(prose)
        if (
            bare_name in _ARRAY_DOCUMENTATION_OWNERS
            or owner in _ARRAY_DOCUMENTATION_OWNERS
        ) and _PER_ELEMENT.search(prose):
            errors.append(
                f'{qualname}: array documentation must describe row-aligned '
                'results, not "per element"'
            )

        if (
            bare_name in _AUTO_SPLIT_OPS
            and bare_name not in _GENUINELY_PLANAR_OPS
        ):
            lines = inspect.cleandoc(prose).splitlines()
            summary_end = parsed.sections[0].line - 1 if parsed.sections else len(lines)
            if _PLANAR.search('\n'.join(lines[:summary_end])):
                errors.append(
                    f'{qualname}: antimeridian-splitting operation is described '
                    'as planar'
                )

        named_entries = (*parameter_entries(parsed), *attribute_entries(parsed))
        type_entries = (*named_entries, *return_entries(parsed))
        for entry in type_entries:
            if _NDARRAY_TYPE.search(entry.type_text):
                errors.append(
                    f'{qualname}: type fields must use numpy.ndarray'
                )
        for entry in named_entries:
            if _has_unquoted_enum(entry.type_text):
                errors.append(
                    f'{qualname}: enum values in a type field must be quoted'
                )
    return errors


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
    stub_overloads = _stub_overloads()
    errors.extend(_scalar_array_return_errors(STUB, gm))
    cfg = load_config(ROOT / 'tools' / 'stubs' / 'stubconfig.py')
    errors.extend(
        _doc_content_errors(
            iter_doc_subjects(CheckContext(cfg), runtime_module=gm)
        )
    )
    if not isinstance(lib, griffe.Module):
        return ['transformed model has no gometry._lib module']

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
