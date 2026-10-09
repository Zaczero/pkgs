"""Unit tests for built-site checks and Griffe public canonicalization.

Drives the real shipped helpers in ``tools/docs/``:
* ``check.collect_errors`` unresolved-public-type + public See Also targets
* ``griffe_expand_aliases`` private→public class alias installation so annotation
  crossrefs resolve to ``gometry.X`` anchors
* Python-defined adapter re-exports resolve to their top-level facade anchors
"""

from __future__ import annotations

import ast
from html.parser import HTMLParser
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Literal

import griffe
from jinja2 import (
    ChoiceLoader,
    DictLoader,
    Environment,
    FileSystemLoader,
    TemplateNotFound,
    pass_context,
)
from markupsafe import Markup, escape

from tests._support import GOMETRY_ROOT, load_tool

if TYPE_CHECKING:
    from collections.abc import Iterable


def _check():
    return load_tool('docs_check', GOMETRY_ROOT / 'tools/docs/check.py')


def _write_site(tmp_path: Path, pages: dict[str, str]) -> Path:
    site = tmp_path / 'site'
    for relative, body in pages.items():
        path = site / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding='utf-8')
    return site


def _render_signature_block(function: Any) -> tuple[str, tuple[Any, ...]]:
    forms = tuple(function.overloads or ()) or (function,)
    markers = {id(form): f'form-{index}' for index, form in enumerate(forms)}

    def format_signature(_body: str, form: Any, *_args: Any, **_kwargs: Any) -> Markup:
        return Markup('<span data-form="{}"></span>').format(markers[id(form)])

    template_dir = GOMETRY_ROOT / 'overrides/mkdocstrings/python/material'
    environment = Environment(
        loader=ChoiceLoader([
            FileSystemLoader(str(template_dir)),
            DictLoader({
                '_base/function.html.jinja': (
                    '{% block signature scoped %}{% endblock signature %}'
                ),
            }),
        ]),
        autoescape=True,
    )
    environment.filters['format_signature'] = format_signature
    template = environment.get_template('function.html.jinja')
    rendered = template.render(
        function=function,
        config=SimpleNamespace(
            separate_signature=True,
            line_length=88,
            signature_crossrefs=True,
        ),
    )
    return rendered, forms


def _load_griffe_model() -> griffe.Module:
    expand = load_tool('griffe_expand_aliases')
    loader = griffe.GriffeLoader(
        search_paths=[str(GOMETRY_ROOT / 'python')],
        extensions=griffe.Extensions(
            expand.PromoteStubOverloads(),
            expand.ExpandTokenAliases(),
        ),
        allow_inspection=False,
        docstring_parser=griffe.Parser.numpy,
        docstring_options={'warn_unknown_params': False},
    )
    pkg = loader.load('gometry')
    assert isinstance(pkg, griffe.Module)
    loader.resolve_aliases(external=False)
    return pkg


class _ClassReference(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.ids: list[str] = []
        self.disclosures: list[dict[str, Any]] = []
        self.disclosure: dict[str, Any] | None = None
        self.summary: list[str] | None = None
        self.signature: list[str] | None = None
        self.rows: list[list[str]] = []
        self.row: list[str] | None = None
        self.cell: list[str] | None = None

    def handle_starttag(
        self, tag: str, attributes: list[tuple[str, str | None]]
    ) -> None:
        attrs = dict(attributes)
        if ident := attrs.get('id'):
            self.ids.append(ident)
        classes = set((attrs.get('class') or '').split())
        if tag == 'details' and 'doc-overloads-details' in classes:
            self.disclosure = {
                'open': 'open' in attrs,
                'summary': [],
                'signatures': [],
            }
            self.disclosures.append(self.disclosure)
        elif tag == 'summary' and self.disclosure is not None:
            self.summary = self.disclosure['summary']
        elif (
            tag == 'pre' and 'doc-signature' in classes and self.disclosure is not None
        ):
            self.signature = []
            self.disclosure['signatures'].append(self.signature)
        elif tag == 'tr' and 'doc-section-item' in classes:
            self.row = []
        elif tag == 'td' and self.row is not None:
            self.cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag == 'summary':
            self.summary = None
        elif tag == 'pre':
            self.signature = None
        elif tag == 'td' and self.cell is not None and self.row is not None:
            self.row.append(' '.join(''.join(self.cell).split()))
            self.cell = None
        elif tag == 'tr' and self.row is not None:
            self.rows.append(self.row)
            self.row = None
        elif tag == 'details':
            self.disclosure = None

    def handle_data(self, data: str) -> None:
        if self.summary is not None:
            self.summary.append(data)
        if self.signature is not None:
            self.signature.append(data)
        if self.cell is not None:
            self.cell.append(data)


def _render_class_reference(class_obj: griffe.Class) -> str:
    from mkdocstrings_handlers.python._internal.config import PythonOptions
    from mkdocstrings_handlers.python._internal.rendering import (
        do_format_signature,
        do_split_path,
        do_stash_crossref,
    )

    template_root = (
        Path(do_format_signature.__code__.co_filename).parents[1] / 'templates/material'
    )
    loader = ChoiceLoader([
        FileSystemLoader(str(GOMETRY_ROOT / 'overrides/mkdocstrings/python/material')),
        DictLoader({
            '_base/class.html.jinja': (
                '<h2 id="{{ class.path }}">{{ class.name }}</h2>'
                '{% block signature scoped %}{% endblock signature %}'
                '{% block docstring scoped %}{% endblock docstring %}'
            ),
        }),
        FileSystemLoader(str(template_root)),
    ])
    environment = Environment(loader=loader, autoescape=True)
    environment.filters['split_path'] = do_split_path
    environment.filters['stash_crossref'] = do_stash_crossref
    environment.filters['any'] = any
    environment.filters['convert_markdown'] = lambda value, *_args, **_kwargs: escape(
        str(value)
    )
    environment.filters['heading'] = lambda *_args, **_kwargs: ''

    def highlight(
        source: Any, *, classes: Iterable[str] = (), **_kwargs: Any
    ) -> Markup:
        class_names = ' '.join(classes)
        # The formatter escapes signature text before this test highlighter wraps it.
        rendered = str(source)
        name, _, tail = rendered.partition('(')
        parameters, separator, suffix = tail.rpartition(')')
        if not separator:
            parameters, suffix = tail, ''
        # Preserve escaped formatter text and expose its terminal punctuation.
        code = Markup(  # noqa: S704
            f'<span class="nf">{name}</span><span class="p">(</span>'
            f'<span class="n">{parameters}</span><span class="p">)</span>{suffix}'
        )
        return Markup('<div class="highlight"><pre class="{}">{}</pre></div>').format(
            class_names,
            code,
        )

    @pass_context
    def format_signature(
        context: Any,
        callable_path: Markup,
        function: griffe.Function,
        line_length: int,
        **kwargs: Any,
    ) -> Markup:
        # The installed formatter returns its trusted highlighted signature HTML.
        return Markup(  # noqa: S704
            do_format_signature(
                context,
                callable_path,
                function,
                line_length,
                **kwargs,
            )
        )

    environment.filters['highlight'] = highlight
    environment.filters['format_signature'] = format_signature
    environment.globals['AutorefsHook'] = lambda *_args: object()
    environment.globals['log'] = SimpleNamespace(debug=lambda *_args: '')
    environment.globals['locale'] = 'en'
    environment.tests['existing_template'] = lambda name: _template_exists(
        environment,
        name,
    )
    options = PythonOptions.from_data(
        merge_init_into_class=True,
        separate_signature=True,
        show_signature_annotations=True,
        show_signature_type_parameters=False,
        signature_crossrefs=False,
        line_length=4096,
        parameter_headings=False,
        docstring_section_style='table',
    )
    return environment.get_template('class.html.jinja').render(**{
        'class': class_obj,
        'class_name': class_obj.name,
        'config': options,
        'heading_level': 2,
        'html_id': class_obj.path,
        'locale': 'en',
        'root': True,
        'root_members': True,
    })


def _template_exists(environment: Environment, name: str) -> bool:
    try:
        environment.loader.get_source(environment, name)
    except TemplateNotFound:
        return False
    return True


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
    assert any('does not target a public API symbol' in e for e in invalid_errors), (
        invalid_errors
    )


def test_doc_integrity_checks_only_resolved_public_doc_carriers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import importlib

    integrity = load_tool(
        'doc_integrity', GOMETRY_ROOT / 'tools/gates/_check_doc_integrity.py'
    )
    from pyo3stubs.config import StubConfig

    stub_path = tmp_path / 'python' / 'gometry' / '_lib.pyi'
    stub_path.parent.mkdir(parents=True)
    stub_path.write_text(
        '''\
class Widget:
    """Stale stub prose.

    Notes
    -----
    This is not the resolved runtime carrier.

    Notes
    -----
    The stale duplicate is not inspected.
    """
'''
    )
    src_root = tmp_path / 'src'
    src_root.mkdir()
    (src_root / 'lib.rs').write_text(
        """\
/// A duplicate on an unexported Rust item is not a public doc carrier.
///
/// Raises
/// ------
/// GeometryError
///     First entry.
/// GeometryError
///     Duplicate entry.
fn private_helper() {}
"""
    )
    runtime_package = tmp_path / 'runtime' / 'fakepkg'
    runtime_package.mkdir(parents=True)
    (runtime_package / '__init__.py').write_text('')
    (runtime_package / '_lib.py').write_text(
        '''\
__all__ = ['Widget']

class Widget:
    """A widget.

    Parameters
    ----------
    size : int
        The size.

    Parameters
    ----------
    size : int
        The duplicate parameter.
    """
'''
    )
    monkeypatch.syspath_prepend(str(runtime_package.parent))
    runtime = importlib.import_module('fakepkg._lib')
    cfg = StubConfig(
        module='fakepkg._lib',
        stub_path=stub_path,
        src_root=src_root,
    )

    errors = integrity.collect_errors(cfg, runtime)

    assert len(errors) == 1, errors
    assert errors[0].endswith("Widget: duplicate 'Parameters' section")
    assert str(stub_path) in errors[0]
    assert 'Notes' not in errors[0]
    assert 'src/lib.rs' not in errors[0]


def test_shared_subject_return_check_covers_overloads_and_properties(
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
        ...

    @overload
    def buffer(self, distance: float, *, array: bool) -> GeometryArray[Point]:
        """Buffer geometry.

        Returns
        -------
        Point or GeometryArray[Point]
        """

    @overload
    def centroid(self, *, array: bool = False) -> Point: ...

    @overload
    def centroid(self, *, array: bool) -> Point:
        """Compute a representative point.

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

    tree = ast.parse(stub.read_text(encoding='utf-8'))
    classes = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
    from pyo3stubs.doc_structure import DocLocation

    def subject(
        class_name: str,
        member_name: str,
        kind: Literal['callable', 'property'] = 'callable',
    ):
        class_node = classes[class_name]
        declarations = tuple(
            node
            for node in class_node.body
            if isinstance(node, ast.FunctionDef) and node.name == member_name
        )
        carrier = declarations[-1]
        return model.DocSubject(
            f'{class_name}.{member_name}',
            kind,
            ast.get_docstring(carrier),
            (),
            DocLocation(str(stub), carrier.lineno),
            declarations,
        )

    subjects = (
        subject('Geometry', 'buffer'),
        subject('Geometry', 'centroid'),
        subject('Geometry', 'bounds', 'property'),
        subject('GeometryArray', 'buffer'),
    )
    errors = model._scalar_array_return_errors(subjects)

    assert len(errors) == 2, errors
    assert any('Geometry.centroid' in error for error in errors)
    assert any('Geometry.bounds' in error for error in errors)


def test_doc_model_requires_quoted_literal_values_in_type_entries() -> None:
    from pyo3stubs.doc_structure import DocLocation

    model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )

    def subject(name: str, prose: str):
        return model.DocSubject(
            name,
            'callable',
            prose,
            (),
            DocLocation('gometry/_lib.pyi', 1),
        )

    subjects = (
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
            Buffer with an unquoted enum-like type.

            Parameters
            ----------
            unit : {planar, meters}
                Measurement unit.

            Returns
            -------
            Geometry
            """,
        ),
    )

    errors = model._doc_type_expression_errors(subjects)

    assert errors == ['Geometry.buffer: enum values in a type field must be quoted']


def test_griffe_canonicalizes_private_classes_to_public_paths() -> None:
    """``_lib.X`` / ``_types.X`` become aliases of public ``gometry.X``.

    This is the root-cause fix for unresolved signature/numpydoc type links:
    annotation ``canonical_path`` must be the public anchor path.
    """
    pkg = _load_griffe_model()

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
    pkg = _load_griffe_model()
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
    raw_loader = griffe.GriffeLoader(
        search_paths=[str(GOMETRY_ROOT / 'python')],
        allow_inspection=False,
    )
    raw_pkg = raw_loader.load('gometry')
    aliases = _public_python_function_aliases(raw_pkg)
    assert aliases

    pkg = _load_griffe_model()

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


def test_griffe_preserves_all_accepted_forms_in_authored_order() -> None:
    pkg = _load_griffe_model()
    doc_model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    lib = pkg.members['_lib']
    assert isinstance(lib, griffe.Module)
    expected = doc_model._stub_overloads(
        lib,
        doc_model.annotation_aliases(pkg),
    )
    functions = {
        function.canonical_path: function
        for function in doc_model._functions(pkg, set())
    }
    paths = (
        'gometry.Point.destination',
        'gometry.Geometry.segmentize',
        'gometry.Geometry.line_interpolate',
    )

    for path in paths:
        function = functions[path]
        forms = tuple(function.overloads or ())
        assert forms
        assert (
            tuple(doc_model._griffe_call_form(form) for form in forms) == expected[path]
        )


def test_function_template_renders_every_accepted_form_once_in_order() -> None:
    pkg = _load_griffe_model()
    point = pkg.members['Point']
    geometry = pkg.members['Geometry']
    assert isinstance(point, griffe.Class)
    assert isinstance(geometry, griffe.Class)
    assert point.canonical_path == 'gometry.Point'
    assert geometry.canonical_path == 'gometry.Geometry'
    functions = (
        point.members['destination'],
        geometry.members['segmentize'],
        geometry.members['line_interpolate'],
    )

    for function in functions:
        rendered, forms = _render_signature_block(function)
        markers = [
            f'<span data-form="form-{index}"></span>' for index, _ in enumerate(forms)
        ]
        assert function.overloads
        assert '<details open class="doc-overloads-details">' in rendered
        assert '<summary>Accepted call forms</summary>' in rendered
        assert 'doc-canonical-signature' not in rendered
        assert rendered.count('<span data-form=') == len(forms)
        assert all(rendered.count(marker) == 1 for marker in markers)
        assert [rendered.index(marker) for marker in markers] == sorted(
            rendered.index(marker) for marker in markers
        )


def test_function_template_keeps_ordinary_and_single_form_signatures() -> None:
    pkg = _load_griffe_model()
    geometry = pkg.members['Geometry']
    assert isinstance(geometry, griffe.Class)

    ordinary = geometry.members['minimum_clearance']
    assert isinstance(ordinary, griffe.Function)
    assert ordinary.canonical_path == 'gometry.Geometry.minimum_clearance'
    assert not ordinary.overloads
    ordinary_html, ordinary_forms = _render_signature_block(ordinary)
    assert ordinary_forms == (ordinary,)
    assert '<div class="doc-canonical-signature">' in ordinary_html
    assert '<details' not in ordinary_html
    assert ordinary_html.count('<span data-form=') == 1

    accepted_form = SimpleNamespace(name='sample', returns='Point')
    single_form = SimpleNamespace(name='sample', returns='Point')
    single_form.overloads = [accepted_form]
    single_html, single_forms = _render_signature_block(single_form)
    assert single_forms == (accepted_form,)
    assert '<details open class="doc-overloads-details">' in single_html
    assert 'doc-canonical-signature' not in single_html
    assert single_html.count('<span data-form="form-0"></span>') == 1


def test_griffe_preserves_public_constructor_forms_in_authored_order() -> None:
    pkg = _load_griffe_model()
    doc_model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    expand = load_tool('griffe_expand_aliases')
    names = {
        'Point',
        'LineString',
        'Polygon',
        'CellArray',
        'GeometryArray',
        'CRS',
        'GeohashCell',
    }
    classes = {
        name: pkg.members[name]
        for name in names
        if isinstance(pkg.members.get(name), griffe.Class)
    }
    lib = pkg.members['_lib']
    assert isinstance(lib, griffe.Module)
    contracts = doc_model._stub_constructor_contracts(
        lib,
        doc_model.annotation_aliases(pkg),
        names,
    )
    assert set(contracts) == {f'gometry.{name}' for name in names}
    expected_counts = {
        'GeohashCell': 3,
        'Point': 2,
        'LineString': 3,
        'Polygon': 3,
        'CellArray': 16,
        'GeometryArray': 3,
        'CRS': 1,
    }

    for name, count in expected_counts.items():
        path = f'gometry.{name}'
        class_obj = classes[name]
        contract = contracts[path]
        binding = expand.constructor_binding(class_obj)
        assert contract.error is None, path
        assert len(contract.forms) == count, path
        assert binding is not None, path
        assert (
            tuple(doc_model._griffe_call_form(form) for form in binding.overloads or ())
            == contract.forms
        ), path
        assert binding not in class_obj.members.values(), path


def test_constructor_parameter_tables_show_authored_defaults() -> None:
    pkg = _load_griffe_model()
    expected = {
        'Point': {
            'x': 'None',
            'y': 'None',
            'z': 'None',
            'm': 'None',
            'crs': 'None',
            'epoch': 'None',
        },
        'LineString': {
            'coordinates': 'None',
            'x': 'None',
            'y': 'None',
            'z': 'None',
            'm': 'None',
            'crs': 'None',
            'epoch': 'None',
        },
        'Polygon': {
            'shell': 'None',
            'holes': 'None',
            'x': 'None',
            'y': 'None',
            'z': 'None',
            'm': 'None',
            'crs': 'None',
            'epoch': 'None',
        },
        'CellArray': {'values': 'required', 'type': 'None'},
        'CRS': {'value': 'required'},
    }

    for name, expected_defaults in expected.items():
        class_obj = pkg.members[name]
        assert isinstance(class_obj, griffe.Class)
        parser = _ClassReference()
        parser.feed(_render_class_reference(class_obj))
        rows = [row for row in parser.rows if row and row[0] in expected_defaults]
        assert len(rows) == len(expected_defaults), name
        assert {row[0]: row[-1] for row in rows} == expected_defaults, name
        assert len(parser.disclosures) == 1, name
        assert (
            ''.join(parser.disclosures[0]['summary']).strip() == 'Accepted call forms'
        )


def test_constructor_default_projection_keeps_differing_accepted_forms() -> None:
    model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    forms = (
        ((('keyword-only', 'value', 'int', None),), None),
        ((('keyword-only', 'value', 'int', '1'),), None),
        ((('keyword-only', 'value', 'int', '2'),), None),
    )

    defaults, conflicts = model._call_form_defaults(forms)

    assert defaults == {'value': '1'}
    assert conflicts == []


def test_constructor_class_template_uses_the_handler_formatter_once_per_form() -> None:
    pkg = _load_griffe_model()
    expand = load_tool('griffe_expand_aliases')
    for name in (
        'Point',
        'LineString',
        'Polygon',
        'CellArray',
        'GeometryArray',
        'CRS',
        'GeohashCell',
    ):
        class_obj = pkg.members[name]
        assert isinstance(class_obj, griffe.Class)
        binding = expand.constructor_binding(class_obj)
        assert binding is not None, name
        parser = _ClassReference()
        html = _render_class_reference(class_obj)
        parser.feed(html)
        assert parser.ids.count(class_obj.path) == 1, name
        assert len(parser.disclosures) == 1, name
        assert parser.disclosures[0]['open'], name
        rendered = [
            ' '.join(''.join(signature).split()).replace('np.', '')
            for signature in parser.disclosures[0]['signatures']
        ]
        expected = [
            ' '.join(form.signature(name=name).split()).replace('np.', '')
            for form in binding.overloads or ()
        ]
        assert rendered == expected, (
            f'{name}: rendered={rendered!r}, expected={expected!r}, html={html!r}'
        )
        assert f'{class_obj.path}.__new__' not in html, name
        assert f'{class_obj.path}.__init__' not in html, name


def test_constructor_inheritance_rebinds_without_mutating_base_and_honors_never(
    tmp_path: Path,
) -> None:
    package_dir = tmp_path / 'constructors'
    package_dir.mkdir()
    (package_dir / '__init__.pyi').write_text(
        """from typing import Never, Self

class Base:
    def __new__(cls, value: int = 1) -> Self: ...

class Inherited(Base):
    pass

class Blocked(Base):
    def __new__(cls, forbidden: Never, /) -> Self: ...
""",
        encoding='utf-8',
    )
    loader = griffe.GriffeLoader(
        search_paths=[str(tmp_path)],
        allow_inspection=False,
    )
    module = loader.load('constructors')
    loader.resolve_aliases(external=False)
    expand = load_tool('griffe_expand_aliases')
    base = module.members['Base']
    inherited = module.members['Inherited']
    blocked = module.members['Blocked']
    assert isinstance(base, griffe.Class)
    assert isinstance(inherited, griffe.Class)
    assert isinstance(blocked, griffe.Class)

    source = base.members['__new__']
    assert isinstance(source, griffe.Function)
    binding = expand._constructor_binding(inherited, module.path)
    assert binding is not None
    assert binding.overloads and len(binding.overloads) == 1
    form = binding.overloads[0]
    assert form.name == '__call__'
    assert form.parent is inherited
    assert [parameter.name for parameter in form.parameters] == ['value']
    assert str(form.parameters['value'].default) == '1'
    assert str(form.returns) == 'Inherited'
    assert [parameter.name for parameter in source.parameters] == ['cls', 'value']
    assert source.parent is base
    assert expand.constructor_binding(base) is None
    assert expand._constructor_binding(blocked, module.path) is None


def test_inherited_generic_constructor_self_and_specialized_returns_match_stub_ast(
    tmp_path: Path,
) -> None:
    source = """from typing import Generic, Self, TypeVar, overload

_GeometryT_co = TypeVar('_GeometryT_co', covariant=True)

class GenericBase(Generic[_GeometryT_co]):
    @overload
    def __new__(cls, value: _GeometryT_co, /) -> Self: ...
    @overload
    def __new__(cls, value: int, /) -> GenericBase[int]: ...

class Inherited(GenericBase[_GeometryT_co]):
    pass
    """
    package_dir = tmp_path / 'gometry'
    package_dir.mkdir()
    (package_dir / '__init__.pyi').write_text(source, encoding='utf-8')
    expand = load_tool('griffe_expand_aliases')
    loader = griffe.GriffeLoader(
        search_paths=[str(tmp_path)],
        extensions=griffe.Extensions(
            expand.PromoteStubOverloads(),
            expand.ExpandTokenAliases(),
        ),
        allow_inspection=False,
    )
    package = loader.load('gometry')
    loader.resolve_aliases(external=False)
    model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    inherited = package.members['Inherited']
    assert isinstance(inherited, griffe.Class)
    binding = expand.constructor_binding(inherited)
    assert binding is not None

    contract = model._stub_constructor_contracts(
        package,
        {},
        {'Inherited'},
        ast.parse(source),
    )['gometry.Inherited']
    assert contract.error is None
    actual = tuple(model._griffe_call_form(form) for form in binding.overloads or ())
    assert actual == contract.forms
    assert [form[1] for form in actual] == [
        'Inherited[GeometryT_co]',
        'GenericBase[int]',
    ]
