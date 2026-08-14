"""Minimal Griffe repairs for a handwritten extension-module stub.

Griffe does not create members for overload-only ``.pyi`` definitions and
cannot resolve aliases to those missing members.  The docs also expand private
annotation aliases so reference signatures describe accepted values rather
than exposing implementation names.  All prose remains untouched.
"""

from __future__ import annotations

import copy
import re
from typing import Any

import griffe

_STUB_LOCAL = (
    '_AffineMatrix',
    '_ArealLike',
    '_ArrowInput',
    '_BoolLane',
    '_Bounds2D',
    '_Bounds3D',
    '_GeoJsonScalar',
    '_GeometryLike',
    '_IndexLane',
    '_IntInput',
    '_WktOutputDimension',
)
_ERROR_CLASSES = (
    'CRSError',
    'CRSMismatchError',
    'GeometryError',
    'GeometryTypeError',
    'InvalidGeometryError',
    'ParseError',
    'TransformError',
)
_GEOMETRY_LEAVES = frozenset({
    'GeometryCollection',
    'LineString',
    'MultiLineString',
    'MultiPoint',
    'MultiPolygon',
    'Point',
    'Polygon',
})
_PRIVATE_TYPEVAR_RENAMES = {
    '_CellT': 'CellT',
    '_DefaultT': 'DefaultT',
    '_ExtremeT': 'ExtremeT',
    '_CellT_co': 'CellT_co',
    '_GeometryOtherT': 'GeometryOtherT',
    '_GeometryT': 'GeometryT',
    '_GeometryT_co': 'GeometryT_co',
    '_GroupKeyT': 'GroupKeyT',
    '_GroupValuesT_co': 'GroupValuesT_co',
}
_SEE_ALSO_ENTRY = re.compile(
    r'(?:^|(?<=\. ))(?P<ticks>`?)'
    r'(?P<name>(?:gm\.)?[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)'
    r'(?P=ticks)(?P<sep>[ \t]*(?:\n[ \t]*)?:)(?=[ \t]|$)',
    re.MULTILINE,
)


def _is_never(function: griffe.Function) -> bool:
    returns = function.returns
    return isinstance(returns, griffe.ExprName) and returns.name in {
        'Never',
        'NoReturn',
    }


def _promote_overloads(obj: Any, seen: set[int]) -> None:
    if id(obj) in seen or isinstance(obj, griffe.Alias):
        return
    seen.add(id(obj))
    stranded = getattr(obj, 'overloads', None)
    if isinstance(stranded, dict):
        for name, variants in list(stranded.items()):
            if not variants or name in obj.members:
                continue
            usable = [variant for variant in variants if not _is_never(variant)]
            if not usable:
                continue
            # The stub's final concrete definition is Griffe's canonical
            # implementation.  Arity is not a reliable proxy: overloads can
            # deliberately widen or narrow parameters independently.
            canonical = usable[-1]
            canonical.overloads = [
                variant
                for variant in variants
                if variant is not canonical and not _is_never(variant)
            ]
            canonical.overloads.append(copy.copy(canonical))
            obj.set_member(name, canonical)
            del stranded[name]
    for member in list(getattr(obj, 'members', {}).values()):
        _promote_overloads(member, seen)


def _top_level_alias_target(member: Any) -> str | None:
    if isinstance(member, griffe.Alias):
        target_path = getattr(member, 'target_path', '')
        if target_path.startswith('gometry._lib.'):
            return target_path.rsplit('.', 1)[-1]
        return None
    if not isinstance(member, griffe.Attribute):
        return None
    parts = getattr(getattr(member, 'value', None), 'values', None)
    if (
        parts
        and len(parts) == 2
        and getattr(parts[0], 'name', None) == '_lib'
        and isinstance(getattr(parts[1], 'name', None), str)
    ):
        return parts[1].name
    return None


def _materialize_function_aliases(pkg: griffe.Module) -> None:
    lib = pkg.members.get('_lib')
    if not isinstance(lib, griffe.Module):
        return
    for alias_name, member in list(pkg.members.items()):
        target_name = _top_level_alias_target(member)
        if target_name is None:
            continue
        target = lib.members.get(target_name)
        if not isinstance(target, griffe.Function):
            continue
        local = copy.copy(target)
        local.name = alias_name
        local.parent = pkg
        local.overloads = [
            copy.copy(overload)
            for overload in target.overloads or ()
            if not _is_never(overload)
        ]
        for overload in local.overloads:
            overload.name = alias_name
            overload.parent = pkg
        pkg.set_member(alias_name, local)


def _materialize_class_aliases(pkg: griffe.Module) -> None:
    """Materialize native class aliases so per-page member options apply."""
    lib = pkg.members.get('_lib')
    if not isinstance(lib, griffe.Module):
        return
    for alias_name, member in list(pkg.members.items()):
        target_name = _top_level_alias_target(member)
        if target_name is None:
            continue
        target = lib.members.get(target_name)
        if not isinstance(target, griffe.Class):
            continue
        local = copy.copy(target)
        local.aliases = dict(target.aliases)
        local.name = alias_name
        local.parent = pkg
        if target_name in _GEOMETRY_LEAVES:
            # The leaf reference page documents only type-specific members;
            # Geometry is the canonical home for their shared surface.
            local.bases = []
        local.members = {
            member_name: copy.copy(child)
            for member_name, child in target.members.items()
        }
        for child in local.members.values():
            child.parent = local
        pkg.set_member(alias_name, local)


def _materialize_classes(
    pkg: griffe.Module,
    module_name: str,
    names: tuple[str, ...],
) -> None:
    module = pkg.members.get(module_name)
    if not isinstance(module, griffe.Module):
        return
    for name in names:
        target = module.members.get(name)
        if not isinstance(target, griffe.Class):
            continue
        local = copy.copy(target)
        local.aliases = dict(target.aliases)
        local.name = name
        local.parent = pkg
        local.members = {
            member_name: copy.copy(member)
            for member_name, member in target.members.items()
        }
        for member in local.members.values():
            member.parent = local
        pkg.set_member(name, local)


def _canonicalize_public_classes(pkg: griffe.Module) -> None:
    """Make public materialized classes canonical in the docs model.

    Private ``_lib``/``_types`` classes become aliases of the public
    materialization so annotation crossrefs (signatures and numpydoc type
    columns) resolve to ``gometry.X`` anchors instead of unresolved private
    paths.
    """
    for module_name in ('_lib', '_types'):
        module = pkg.members.get(module_name)
        if not isinstance(module, griffe.Module):
            continue
        for name, public in list(pkg.members.items()):
            private = module.members.get(name)
            if not isinstance(public, griffe.Class) or not isinstance(
                private, griffe.Class
            ):
                continue
            module.set_member(
                name,
                griffe.Alias(name, public, parent=module),
            )


class PromoteStubOverloads(griffe.Extension):
    """Make overload-only functions and their public aliases renderable."""

    def on_package(self, *, pkg: griffe.Module, **kwargs: Any) -> None:
        if pkg.name != 'gometry':
            return
        _promote_overloads(pkg, set())
        _materialize_class_aliases(pkg)
        _materialize_classes(pkg, '_lib', _ERROR_CLASSES)
        _materialize_classes(pkg, '_types', ('Cell', 'Coverage'))
        _canonicalize_public_classes(pkg)
        _materialize_function_aliases(pkg)


def _expand(expr: Any, aliases: dict[str, Any]) -> Any:
    if expr is None or isinstance(expr, str):
        return expr
    if isinstance(expr, griffe.ExprName):
        if expr.name in _PRIVATE_TYPEVAR_RENAMES:
            return griffe.ExprName(_PRIVATE_TYPEVAR_RENAMES[expr.name])
        if expr.name == '_CoverageIterator':
            return griffe.ExprName('Iterator')
        if expr.name in {'_Coverage', '_NumericCell', '_Cell'}:
            return griffe.ExprName('object')
        return aliases.get(expr.name, expr)
    if isinstance(expr, griffe.ExprBinOp):
        expr.left = _expand(expr.left, aliases)
        expr.right = _expand(expr.right, aliases)
    elif isinstance(expr, griffe.ExprSubscript):
        expr.left = _expand(expr.left, aliases)
        expr.slice = _expand(expr.slice, aliases)
    elif isinstance(expr, griffe.ExprTuple):
        expr.elements = [_expand(element, aliases) for element in expr.elements]
    return expr


def _expand_base(expr: Any, aliases: dict[str, Any]) -> Any:
    if isinstance(expr, griffe.ExprSubscript) and isinstance(
        expr.left, griffe.ExprName
    ):
        if expr.left.name == '_Coverage':
            return griffe.ExprName('Sequence')
        if expr.left.name == '_CoverageIterator':
            return griffe.ExprName('Iterator')
    return _expand(expr, aliases)


def _walk_annotations(obj: Any, aliases: dict[str, Any], seen: set[int]) -> None:
    if id(obj) in seen or isinstance(obj, griffe.Alias):
        return
    seen.add(id(obj))
    if isinstance(obj, griffe.Function):
        for function in (obj, *(obj.overloads or ())):
            for parameter in function.parameters:
                parameter.annotation = _expand(parameter.annotation, aliases)
            function.returns = _expand(function.returns, aliases)
    elif isinstance(obj, griffe.Class):
        obj.bases = [_expand_base(base, aliases) for base in obj.bases]
    elif isinstance(obj, griffe.Attribute):
        obj.annotation = _expand(obj.annotation, aliases)
    for member in getattr(obj, 'members', {}).values():
        _walk_annotations(member, aliases, seen)


def _public_members(pkg: griffe.Module) -> tuple[set[str], dict[str, set[str]]]:
    top = {name for name in pkg.members if not name.startswith('_')}
    classes: dict[str, set[str]] = {}
    for name, member in pkg.members.items():
        if isinstance(member, griffe.Class):
            classes[name] = set(member.members)
    return top, classes


def _link_see_also(
    obj: Any,
    top: set[str],
    classes: dict[str, set[str]],
    seen: set[int],
) -> None:
    if id(obj) in seen or isinstance(obj, griffe.Alias):
        return
    seen.add(id(obj))
    parent = getattr(obj, 'parent', None)
    owner = parent.name if isinstance(parent, griffe.Class) else None

    def replace(match: re.Match[str]) -> str:
        shown = match.group('name')
        target = shown.removeprefix('gm.')
        if '.' in target:
            head, _, tail = target.partition('.')
            path = f'gometry.{head}.{tail}' if tail in classes.get(head, ()) else None
        elif owner and target in classes.get(owner, ()):
            path = f'gometry.{owner}.{target}'
        else:
            path = f'gometry.{target}' if target in top else None
        if path is None:
            return match.group(0)
        ticks = match.group('ticks')
        return f'[{ticks}{shown}{ticks}][{path}]{match.group("sep")}'

    docstring = getattr(obj, 'docstring', None)
    if docstring is not None:
        for section in getattr(docstring, 'parsed', ()):
            if section.kind is not griffe.DocstringSectionKind.admonition:
                continue
            admonition = section.value
            if getattr(admonition, 'kind', None) != 'see-also':
                continue
            contents = getattr(admonition, 'contents', None)
            if isinstance(contents, str):
                admonition.contents = _SEE_ALSO_ENTRY.sub(replace, contents)
    for member in getattr(obj, 'members', {}).values():
        _link_see_also(member, top, classes, seen)


class ExpandTokenAliases(griffe.Extension):
    """Expand private Literal/input aliases in rendered annotations only."""

    def on_package(self, *, pkg: griffe.Module, **kwargs: Any) -> None:
        if pkg.name != 'gometry':
            return
        aliases: dict[str, Any] = {}
        types_module = pkg.members.get('_types')
        if isinstance(types_module, griffe.Module):
            for name, member in types_module.members.items():
                if isinstance(member, griffe.Alias):
                    continue
                value = getattr(member, 'value', None)
                if value is not None and str(value).startswith('Literal['):
                    aliases[name] = value
        lib = pkg.members.get('_lib')
        if isinstance(lib, griffe.Module):
            for name in _STUB_LOCAL:
                value = getattr(lib.members.get(name), 'value', None)
                if value is not None:
                    aliases[name] = value
        seen: set[int] = set()
        if isinstance(lib, griffe.Module):
            _walk_annotations(lib, aliases, seen)
        _walk_annotations(pkg, aliases, seen)
        top, classes = _public_members(pkg)
        _link_see_also(pkg, top, classes, set())
