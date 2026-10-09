"""Unit tests for built-site checks and Griffe public canonicalization.

Drives the real shipped helpers in ``tools/docs/``:
* ``check.collect_errors`` unresolved-public-type + public See Also targets
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


def test_rendered_see_also_links_target_public_symbols(tmp_path: Path) -> None:
    check = _check()
    page = (
        '<html><body><details class="see-also"><p>'
        '<a href="#gometry.Geometry.buffer">buffer</a>'
        '</p></details></body></html>'
    )
    valid = _write_site(tmp_path / 'valid', {'api/shape/index.html': page})
    valid_errors = check.collect_errors(valid)
    assert not any('does not target a public API symbol' in e for e in valid_errors)

    private_page = page.replace(
        '#gometry.Geometry.buffer',
        '#gometry._lib.Geometry.buffer',
    )
    invalid = _write_site(tmp_path / 'invalid', {'api/shape/index.html': private_page})
    invalid_errors = check.collect_errors(invalid)
    assert any(
        'does not target a public API symbol' in e for e in invalid_errors
    ), invalid_errors


def test_shared_doc_parser_checks_stub_and_raw_rust_duplicates(tmp_path: Path) -> None:
    from types import ModuleType

    integrity = load_tool(
        'doc_integrity', GOMETRY_ROOT / 'tools/gates/_check_doc_integrity.py'
    )
    from pyo3stubs.config import StubConfig

    stub_path = tmp_path / 'python' / 'gometry' / '_lib.pyi'
    stub_path.parent.mkdir(parents=True)
    stub_path.write_text(
        '''\
class Widget:
    """A widget.

    Args:
        size (int): The size.

    Arguments:
        size (int): The repeated size.
    """
'''
    )
    src_root = tmp_path / 'src'
    src_root.mkdir()
    (src_root / 'lib.rs').write_text(
        '''\
docs! {
    #[cfg(test)]
    /// Build a widget.
    ///
    /// Parameters
    /// ----------
    /// size : int
    ///     The size.
    ///
    /// Parameters
    /// ----------
    /// size : int
    ///     The repeated size.
    pub fn build(size: usize) {}
}
'''
    )
    runtime = ModuleType('fakepkg._lib')
    runtime.__all__ = ['Widget']
    setattr(runtime, 'Widget', type('Widget', (), {'__doc__': 'A widget.'}))
    cfg = StubConfig(
        module='fakepkg._lib',
        stub_path=stub_path,
        src_root=src_root,
    )

    errors = integrity.collect_errors(cfg, runtime)

    assert any('_lib.pyi' in error and "'Parameters'" in error for error in errors)
    assert any('src/lib.rs' in error and "'Parameters'" in error for error in errors)


def test_scalar_geometry_array_returns_cover_overloads_and_properties(
    tmp_path: Path,
) -> None:
    model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    stub = tmp_path / '_lib.pyi'
    stub.write_text(
        '''\
class Geometry:
    @overload
    def buffer(self, distance: float) -> Point:
        """Buffer geometry.

        Returns
        -------
        GeometryArray[Point]
        """
    @overload
    def buffer(self, distance: float, *, array: bool) -> GeometryArray[Point]:
        """Buffer geometry as an array.

        Returns
        -------
        GeometryArray[Point]
        """
    @property
    def bounds(self) -> tuple[float, float, float, float]:
        """The scalar bounds.

        Returns
        -------
        GeometryArray[float]
        """

class GeometryArray:
    def buffer(self, distance: float) -> GeometryArray[Point]:
        """Buffer each row.

        Returns
        -------
        GeometryArray[Point]
        """
'''
    )

    errors = model._scalar_array_return_errors(stub, object())

    assert len(errors) == 2, errors
    assert any('Geometry.buffer' in error for error in errors)
    assert any('Geometry.bounds' in error for error in errors)


def test_doc_model_keeps_row_units_planarity_and_literal_value_claims() -> None:
    model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )

    def subject(name: str, prose: str):
        return model.DocSubject(
            name,
            'callable',
            prose,
            (),
            model.DocLocation('gometry/_lib.pyi', 1),
        )

    subjects = (
        subject(
            'GeometryArray.buffer',
            """\
            Buffer each element.

            Returns
            -------
            GeometryArray[Geometry]
            """,
        ),
        subject(
            'Geometry.intersection',
            """\
            Intersect the geometries (planar).

            Returns
            -------
            Geometry
            """,
        ),
        subject(
            'Geometry.simplify',
            """\
            Simplify the geometry (planar).

            Returns
            -------
            Geometry
            """,
        ),
        subject(
            'Geometry.buffer',
            """\
            Buffer with a quoted unit.

            Parameters
            ----------
            unit : {'planar', 'meters'}
                Measurement unit.

            Returns
            -------
            Geometry
            """,
        ),
        subject(
            'Geometry.buffer',
            """\
            Buffer with an ambiguous unit.

            Parameters
            ----------
            unit : {planar, meters}
                Measurement unit.

            Returns
            -------
            Geometry
            """,
        ),
        subject(
            'from_array',
            """\
            Return the array representation.

            Returns
            -------
            ndarray of float64
            """,
        ),
    )

    errors = model._doc_content_errors(subjects)

    assert any(
        'GeometryArray.buffer' in error and 'per element' in error for error in errors
    )
    assert any(
        'Geometry.intersection' in error and 'planar' in error for error in errors
    )
    assert not any(
        'Geometry.simplify' in error and 'planar' in error for error in errors
    )
    assert not any('quoted unit' in error and 'enum' in error for error in errors)
    assert any(
        'ambiguous unit' not in error and 'enum values' in error for error in errors
    )
    assert any('from_array' in error and 'numpy.ndarray' in error for error in errors)


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
