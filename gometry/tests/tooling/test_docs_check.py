"""Unit tests for built-site checks and Griffe public canonicalization.

Drives the real shipped helpers in ``tools/docs/``:
* ``check.collect_errors`` unresolved-public-type + rendered double-period rules
* ``griffe_expand_aliases`` private→public class alias installation so annotation
  crossrefs resolve to ``gometry.X`` anchors
* Python-defined adapter re-exports resolve to their top-level facade anchors
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import griffe

from tests._support import GOMETRY_ROOT, load_tool

if TYPE_CHECKING:
    from pathlib import Path


def _check():
    return load_tool('docs_check', GOMETRY_ROOT / 'tools/docs/check.py')


def _write_site(tmp_path: Path, pages: dict[str, str]) -> Path:
    site = tmp_path / 'site'
    for relative, body in pages.items():
        path = site / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding='utf-8')
    return site


def test_unresolved_public_type_span_is_flagged(tmp_path: Path) -> None:
    """A tooltip span for a runtime public class must fail the built-site gate."""
    check = _check()
    # Minimal page set: gate still needs expected API pages; feed only one HTML
    # under site/ and assert our unresolved-type rule fires regardless of
    # missing-page noise from the generator inventory.
    site = _write_site(
        tmp_path,
        {
            'api/toplevel/coverage-polygonal/index.html': (
                '<html><body>'
                '<span title="gometry._lib.GeometryArray">GeometryArray</span>'
                '</body></html>'
            ),
        },
    )
    errors = check.collect_errors(site)
    unresolved = [e for e in errors if 'unresolved public type links' in e]
    assert unresolved, f'expected unresolved-type failure, got: {errors[:5]}'
    assert any('GeometryArray' in e for e in unresolved)


def test_public_type_link_is_not_flagged(tmp_path: Path) -> None:
    """A real anchor link to the public class must not trip the unresolved gate."""
    check = _check()
    site = _write_site(
        tmp_path,
        {
            'ok.html': (
                '<html><body>'
                '<a href="geometryarray/#gometry.GeometryArray">GeometryArray</a>'
                '</body></html>'
            ),
        },
    )
    errors = check.collect_errors(site)
    assert not any('unresolved public type links' in e for e in errors)


def test_rendered_raises_double_period_is_flagged(tmp_path: Path) -> None:
    """Render-injected ``..`` in a Raises description fails; ellipsis does not."""
    check = _check()
    doubled = (
        '<html><body>'
        '<p><span class="doc-section-title">Raises:</span></p>'
        '<table><tr><td>'
        '<div class="doc-md-description"><p>If the rows\' metadata differ.</p>\n.</div>'
        '</td></tr></table>'
        '</body></html>'
    )
    # site/api must exist for collect_errors to proceed past the empty-site guard
    site = _write_site(tmp_path / 'doubled', {'api/raises.html': doubled})
    errors = check.collect_errors(site)
    assert any('doubled terminal period' in e for e in errors), errors

    ellipsis = (
        '<html><body>'
        '<p><span class="doc-section-title">Raises:</span></p>'
        '<table><tr><td>'
        '<div class="doc-md-description"><p>May raise for ... reasons.</p></div>'
        '</td></tr></table>'
        '</body></html>'
    )
    site2 = _write_site(tmp_path / 'ellipsis', {'api/ellipsis.html': ellipsis})
    errors2 = check.collect_errors(site2)
    assert not any('doubled terminal period' in e for e in errors2), errors2


def test_griffe_canonicalizes_private_classes_to_public_paths() -> None:
    """``_lib.X`` / ``_types.X`` become aliases of public ``gometry.X``.

    This is the root-cause fix for unresolved signature/numpydoc type links:
    annotation ``canonical_path`` must be the public anchor path.
    """
    expand = load_tool('griffe_expand_aliases')
    loader = griffe.GriffeLoader(
        search_paths=[str(GOMETRY_ROOT / 'python')],
        extensions=griffe.Extensions(
            expand.PromoteStubOverloads(),
            expand.ExpandTokenAliases(),
        ),
        allow_inspection=False,
    )
    pkg = loader.load('gometry')
    assert isinstance(pkg, griffe.Module)
    loader.resolve_aliases(external=False)

    lib = pkg.members['_lib']
    types = pkg.members['_types']
    assert isinstance(lib, griffe.Module)
    assert isinstance(types, griffe.Module)
    public_ga = pkg.members['GeometryArray']
    private_ga = lib.members['GeometryArray']
    assert isinstance(public_ga, griffe.Class)
    assert isinstance(private_ga, griffe.Alias)
    assert private_ga.canonical_path == 'gometry.GeometryArray'
    assert private_ga.target_path == 'gometry.GeometryArray'

    public_cell = pkg.members['Cell']
    private_cell = types.members['Cell']
    assert isinstance(public_cell, griffe.Class)
    assert isinstance(private_cell, griffe.Alias)
    assert private_cell.canonical_path == 'gometry.Cell'

    # coverage_clean return annotation must resolve publicly after materialize.
    # Absence of the symbol or its returns is a hard failure (not a skip).
    assert 'coverage_clean' in pkg.members, (
        'coverage_clean missing from stub model; public API surface regressed'
    )
    coverage_clean = pkg.members['coverage_clean']
    returns = getattr(coverage_clean, 'returns', None)
    assert returns is not None, (
        'coverage_clean returns annotation missing from stub model'
    )
    # Walk expression names for GeometryArray → public path
    found: list[str] = []

    def walk(expr: object) -> None:
        if isinstance(expr, griffe.ExprName):
            found.append(getattr(expr, 'canonical_path', expr.name))
        for attr in ('left', 'right', 'slice', 'elements'):
            child = getattr(expr, attr, None)
            if isinstance(child, list):
                for item in child:
                    walk(item)
            elif child is not None:
                walk(child)

    walk(returns)
    assert any(
        path == 'gometry.GeometryArray' or path.endswith('.GeometryArray')
        for path in found
    ), found


def test_griffe_presents_cell_descriptors_as_nullable_geometry_properties() -> None:
    expand = load_tool('griffe_expand_aliases')
    loader = griffe.GriffeLoader(
        search_paths=[str(GOMETRY_ROOT / 'python')],
        extensions=griffe.Extensions(
            expand.PromoteStubOverloads(),
            expand.ExpandTokenAliases(),
        ),
        allow_inspection=False,
    )
    pkg = loader.load('gometry')
    loader.resolve_aliases(external=False)
    cell_array = pkg.members['CellArray']
    assert isinstance(cell_array, griffe.Class)

    for name, geometry_type in (('center', 'Point'), ('polygon', 'Polygon')):
        attribute = cell_array.members[name]
        assert isinstance(attribute, griffe.Attribute)
        assert attribute.canonical_path == f'gometry.CellArray.{name}'
        assert str(attribute.annotation) == f'GeometryArray[{geometry_type} | None]'
        assert {'property', 'readonly'} <= attribute.labels
        assert attribute.docstring is not None
        assert attribute.docstring.value.strip()
        assert '_CellGeometryProjection' not in str(attribute.annotation)


def _public_python_function_aliases(
    pkg: griffe.Module,
) -> dict[str, tuple[griffe.Alias, griffe.Function]]:
    native = pkg.members['_lib']
    assert isinstance(native, griffe.Module)
    package_prefix = f'{pkg.path}.'
    aliases: dict[str, tuple[griffe.Alias, griffe.Function]] = {}
    for name, member in pkg.members.items():
        if not isinstance(member, griffe.Alias) or member.parent is not pkg:
            continue
        target_path = getattr(member, 'target_path', '')
        if not target_path.startswith(package_prefix):
            continue
        try:
            target = member.target
        except (griffe.AliasResolutionError, griffe.CyclicAliasError):
            continue
        parent = getattr(target, 'parent', None)
        if (
            isinstance(target, griffe.Function)
            and isinstance(parent, griffe.Module)
            and parent.path != native.path
            and parent.path.startswith(package_prefix)
        ):
            aliases[name] = (member, target)
    return aliases


def test_griffe_canonicalizes_python_function_aliases_to_public_paths() -> None:
    """Python-defined facade aliases resolve to their public routes."""
    expand = load_tool('griffe_expand_aliases')
    raw_loader = griffe.GriffeLoader(
        search_paths=[str(GOMETRY_ROOT / 'python')],
        allow_inspection=False,
    )
    raw_pkg = raw_loader.load('gometry')
    aliases = _public_python_function_aliases(raw_pkg)
    assert aliases

    loader = griffe.GriffeLoader(
        search_paths=[str(GOMETRY_ROOT / 'python')],
        extensions=griffe.Extensions(
            expand.PromoteStubOverloads(),
            expand.ExpandTokenAliases(),
        ),
        allow_inspection=False,
    )
    pkg = loader.load('gometry')
    loader.resolve_aliases(external=False)

    for name, (raw_alias, raw_target) in aliases.items():
        public = pkg.members[name]
        assert isinstance(public, griffe.Function)
        assert public.canonical_path == f'gometry.{name}'
        assert raw_target.canonical_path == raw_alias.target_path
        private_alias = next(
            alias
            for alias in public.aliases.values()
            if (
                isinstance(alias, griffe.Alias)
                and alias.parent is pkg
                and alias.target_path == raw_alias.target_path
            )
        )
        private = private_alias.target
        assert isinstance(private, griffe.Function)
        assert private.canonical_path == raw_alias.target_path


def test_function_alias_materializer_ignores_broken_external_and_class_targets() -> (
    None
):
    """Only resolvable package functions enter the public model."""
    expand = load_tool('griffe_expand_aliases')
    loader = griffe.GriffeLoader(
        search_paths=[str(GOMETRY_ROOT / 'python')],
        allow_inspection=False,
    )
    pkg = loader.load('gometry')
    aliases = {
        name: griffe.Alias(name, target, parent=pkg)
        for name, target in {
            'broken': 'gometry._missing.function',
            'external': 'other_package.function',
            'class_alias': pkg.members['CRS'].target,
        }.items()
    }
    for alias in aliases.values():
        pkg.set_member(alias.name, alias)

    expand._materialize_function_aliases(pkg)

    for name, alias in aliases.items():
        assert pkg.members[name] is alias
        assert isinstance(pkg.members[name], griffe.Alias)


def test_griffe_uses_final_concrete_overload_as_canonical() -> None:
    """The rendered implementation is the final non-Never overload."""
    expand = load_tool('griffe_expand_aliases')
    loader = griffe.GriffeLoader(
        search_paths=[str(GOMETRY_ROOT / 'python')],
        extensions=griffe.Extensions(
            expand.PromoteStubOverloads(),
            expand.ExpandTokenAliases(),
        ),
        allow_inspection=False,
    )
    pkg = loader.load('gometry')
    loader.resolve_aliases(external=False)
    doc_model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    expected = doc_model._stub_overloads()
    for member in pkg.members.values():
        if not isinstance(member, griffe.Function) or not member.overloads:
            continue
        expected_return, expected_params = expected[member.canonical_path]
        assert doc_model._public_annotation(
            str(member.returns)
        ) == doc_model._public_annotation(expected_return), member.name
        assert (
            tuple(
                parameter.name
                for parameter in member.parameters
                if parameter.name not in {'self', 'cls'}
            )
            == expected_params
        ), member.name
        assert all(
            getattr(getattr(variant, 'returns', None), 'name', None)
            not in {'Never', 'NoReturn'}
            for variant in (member, *member.overloads)
        ), member.name
