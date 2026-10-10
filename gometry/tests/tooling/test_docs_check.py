"""Unit tests for built-site checks and Griffe public canonicalization.

Drives the real shipped helpers in ``tools/docs/``:
* ``check.collect_errors`` unresolved-public-type + public See Also targets
* ``griffe_expand_aliases`` private→public class alias installation so annotation
  crossrefs resolve to ``gometry.X`` anchors
* Python-defined adapter re-exports resolve to their top-level facade anchors
"""

from __future__ import annotations

import ast
import copy
import html
import inspect
import re
from html.parser import HTMLParser
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Literal

import gometry as gm
import griffe
import pytest
from jinja2 import (
    ChoiceLoader,
    DictLoader,
    Environment,
    FileSystemLoader,
    TemplateNotFound,
)
from markupsafe import Markup, escape

from tests._support import GOMETRY_ROOT, load_tool

if TYPE_CHECKING:
    from collections.abc import Iterable


_CHECK: Any = None


def _check():
    global _CHECK
    if _CHECK is None:
        _CHECK = load_tool('docs_check', GOMETRY_ROOT / 'tools/docs/check.py')
    return _CHECK


def _write_site(tmp_path: Path, pages: dict[str, str]) -> Path:
    site = tmp_path / 'site'
    for relative, body in pages.items():
        path = site / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding='utf-8')
    return site


_EXPAND: Any = None


def _expand():
    global _EXPAND
    if _EXPAND is None:
        _EXPAND = load_tool('griffe_expand_aliases')
    return _EXPAND


def _load_griffe_model() -> griffe.Module:
    expand = _expand()
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


def _handler_environment(*, line_length: int = 88) -> tuple[Environment, Any]:
    from mkdocstrings_handlers.python._internal.config import PythonOptions
    from mkdocstrings_handlers.python._internal.rendering import (
        do_format_signature,
        do_split_path,
        do_stash_crossref,
    )
    from pygments import highlight
    from pygments.formatters import HtmlFormatter
    from pygments.lexers import get_lexer_by_name

    template_root = (
        Path(do_format_signature.__code__.co_filename).parents[1] / 'templates/material'
    )
    template_dir = GOMETRY_ROOT / 'overrides/mkdocstrings/python/material'
    environment = Environment(
        loader=ChoiceLoader([
            FileSystemLoader(str(template_dir)),
            DictLoader({
                '_base/class.html.jinja': (
                    '<div class="doc doc-object doc-class">'
                    '<h2 id="{{ class.path }}">{{ class.name }}</h2>'
                    '{% block signature scoped %}{% endblock signature %}'
                    '{% block docstring scoped %}{% endblock docstring %}'
                    '</div>'
                ),
                '_base/function.html.jinja': (
                    '<div class="doc doc-object doc-function">'
                    '<h3 id="{{ function.path }}">{{ function.name }}</h3>'
                    '{% block signature scoped %}{% endblock signature %}'
                    '</div>'
                ),
            }),
            FileSystemLoader(str(template_root)),
        ]),
        autoescape=True,
    )
    environment.filters['split_path'] = do_split_path
    environment.filters['stash_crossref'] = do_stash_crossref
    environment.filters['any'] = any
    environment.filters['convert_markdown'] = lambda value, *_args, **_kwargs: escape(
        str(value)
    )
    environment.filters['heading'] = lambda *_args, **_kwargs: ''

    def pygments_highlight(
        source: Any,
        *,
        language: str,
        inline: bool = False,
        classes: Iterable[str] = (),
        linenums: bool = False,
        **_kwargs: Any,
    ) -> Markup:
        del inline
        return Markup(  # noqa: S704
            highlight(
                html.unescape(str(source)),
                get_lexer_by_name(language),
                HtmlFormatter(
                    cssclass=' '.join(classes) or 'highlight',
                    linenos=linenums,
                ),
            )
        )

    environment.filters['highlight'] = pygments_highlight
    environment.filters['format_signature'] = do_format_signature
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
        show_signature_type_parameters=True,
        signature_crossrefs=True,
        parameter_headings=False,
        line_length=line_length,
        docstring_section_style='table',
    )
    return environment, options


def _render_function_reference(
    function: griffe.Function,
    *,
    line_length: int = 88,
) -> str:
    from mkdocstrings_handlers.python._internal.rendering import do_stash_crossref

    environment, options = _handler_environment(line_length=line_length)
    do_stash_crossref.stash.clear()
    try:
        return environment.get_template('function.html.jinja').render(
            function=function,
            config=options,
            heading_level=2,
            root=True,
            root_members=True,
        )
    finally:
        do_stash_crossref.stash.clear()


class _ClassReference(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.ids: list[str] = []
        self.disclosures: list[dict[str, Any]] = []
        self.disclosure: dict[str, Any] | None = None
        self.summary: list[str] | None = None
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
            }
            self.disclosures.append(self.disclosure)
        elif tag == 'summary' and self.disclosure is not None:
            self.summary = self.disclosure['summary']
        elif tag == 'tr' and 'doc-section-item' in classes:
            self.row = []
        elif tag == 'td' and self.row is not None:
            self.cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag == 'summary':
            self.summary = None
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
        if self.cell is not None:
            self.cell.append(data)


def _render_class_reference(
    class_obj: griffe.Class,
    *,
    line_length: int = 88,
) -> str:
    from mkdocstrings_handlers.python._internal.rendering import do_stash_crossref

    environment, options = _handler_environment(line_length=line_length)
    do_stash_crossref.stash.clear()
    try:
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
    finally:
        do_stash_crossref.stash.clear()


def _source_class_call_forms(
    stub: Path,
    class_name: str,
) -> tuple[Any, ...]:
    check = _check()
    tree = ast.parse(stub.read_text(encoding='utf-8'))
    class_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    constructor_name = next(
        (
            name
            for name in ('__new__', '__init__')
            if any(
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == name
                for node in class_node.body
            )
        ),
        None,
    )
    assert constructor_name is not None
    definitions = [
        node
        for node in class_node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == constructor_name
    ]
    overloads = [
        node
        for node in definitions
        if any(
            isinstance(decorator, ast.Name) and decorator.id == 'overload'
            for decorator in node.decorator_list
        )
    ]
    forms = overloads or definitions
    result = []
    for function in forms:
        args = function.args
        positional = [*args.posonlyargs, *args.args]
        defaults: list[ast.expr | None] = [None] * (
            len(positional) - len(args.defaults)
        ) + list(args.defaults)
        positional = positional[1:]
        defaults = defaults[1:]
        parameters = []
        for index, (argument, default) in enumerate(
            zip(positional, defaults, strict=True)
        ):
            parameters.append((
                'positional-only'
                if index < max(0, len(args.posonlyargs) - 1)
                else 'positional or keyword',
                argument.arg,
                ast.unparse(argument.annotation)
                if argument.annotation is not None
                else None,
                ast.unparse(default) if default is not None else None,
            ))
        if args.vararg is not None:
            parameters.append((
                'variadic positional',
                args.vararg.arg,
                ast.unparse(args.vararg.annotation)
                if args.vararg.annotation is not None
                else None,
                None,
            ))
        for argument, default in zip(args.kwonlyargs, args.kw_defaults, strict=True):
            parameters.append((
                'keyword-only',
                argument.arg,
                ast.unparse(argument.annotation)
                if argument.annotation is not None
                else None,
                ast.unparse(default) if default is not None else None,
            ))
        if args.kwarg is not None:
            parameters.append((
                'variadic keyword',
                args.kwarg.arg,
                ast.unparse(args.kwarg.annotation)
                if args.kwarg.annotation is not None
                else None,
                None,
            ))
        returns = (
            class_name
            if constructor_name == '__init__'
            or (
                isinstance(function.returns, ast.Name) and function.returns.id == 'Self'
            )
            else (
                ast.unparse(function.returns) if function.returns is not None else None
            )
        )
        result.append(check.RenderedCallForm(tuple(parameters), returns))
    return tuple(result)


def _form_shape(
    form: Any,
) -> tuple[tuple[tuple[str, str, str | None], ...], str | None]:
    return (
        tuple(
            (kind, name, default)
            for kind, name, _annotation, default in form.parameters
        ),
        form.returns,
    )


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


def test_rendered_signature_parser_keeps_tokens_and_parameter_kinds() -> None:
    check = _check()
    source = """\
<div class="doc doc-object doc-class">
  <h2 id="gometry.Widget">Widget</h2>
  <div class="doc doc-object doc-function">
    <h3 id="gometry.Widget.build">build</h3>
    <div class="doc-signature highlight"><pre>
      <span class="n">build</span><span class="p">(</span><span class="n">in</span><span class="n">t</span><span class="p">:</span>
      <span class="n">int</span><span class="p">,</span> <span class="o">/</span><span class="p">,</span>
      <span class="n">flag</span><span class="p">:</span> <span class="n">bool</span> <span class="o">=</span>
      <span class="kc">None</span><span class="p">,</span> <span class="o">*</span><span class="p">,</span>
      <span class="n">name</span><span class="p">:</span> <span class="n">str</span> <span class="o">=</span>
      <span class="s1">&#39;a/b&#39;</span><span class="p">)</span><span class="o">-&gt;</span> <span class="nb">str</span>
    </pre></div>
  </div>
</div>
"""
    parsed = check.rendered_call_forms(source)
    assert set(parsed) == {'gometry.Widget.build'}
    signature = parsed['gometry.Widget.build'][0]
    assert signature.form == check.RenderedCallForm(
        (
            ('positional-only', 'int', 'int', None),
            ('positional or keyword', 'flag', 'bool', 'None'),
            ('keyword-only', 'name', 'str', "'a/b'"),
        ),
        'str',
    )

    missing_slash = check._signature_form(signature.source.replace(', /', '', 1))
    assert missing_slash != signature.form
    missing_star = check._signature_form(signature.source.replace('*,', '', 1))
    assert missing_star != signature.form


def test_rendered_property_signature_is_not_parsed_as_a_call() -> None:
    check = _check()
    source = """\
<div class="doc doc-object doc-attribute">
  <h3 id="gometry.CRS.area_of_use">
    <code class="doc-symbol doc-symbol-heading doc-symbol-attribute"></code>
  </h3>
  <div class="language-python doc-signature highlight"><pre><span class="n">area_of_use</span><span class="p">:</span> <span class="n">CrsAreaOfUse</span> <span class="o">|</span> <span class="kc">None</span></pre></div>
</div>
"""

    signature = check.rendered_call_forms(source)['gometry.CRS.area_of_use'][0]

    assert signature.source == 'area_of_use: CrsAreaOfUse | None'
    assert signature.form is None


def test_real_handler_renders_parameter_kinds_and_default_states(
    tmp_path: Path,
) -> None:
    source = """\
def positional_only(value: int, /) -> str: ...
def mixed(value: int, /, label: str = 'x/y', *, limit: int = 7, pairs: tuple[int, int] = (1, 2)) -> bool: ...
def variadic(*args: int, flag: bool = False, **kwargs: str) -> None: ...
def keyword_variadic(**kwargs: str) -> int: ...
def empty() -> None: ...

class Methods:
    def method(self, value: int, /) -> str: ...
"""
    package = tmp_path / 'gometry'
    package.mkdir()
    stub = package / '__init__.pyi'
    stub.write_text(source, encoding='utf-8')
    loader = griffe.GriffeLoader(search_paths=[str(tmp_path)], allow_inspection=False)
    module = loader.load('gometry')
    check = _check()
    expected = {
        'positional_only': check.RenderedCallForm(
            (('positional-only', 'value', 'int', None),),
            'str',
        ),
        'mixed': check.RenderedCallForm(
            (
                ('positional-only', 'value', 'int', None),
                ('positional or keyword', 'label', 'str', "'x/y'"),
                ('keyword-only', 'limit', 'int', '7'),
                ('keyword-only', 'pairs', 'tuple[int, int]', '(1, 2)'),
            ),
            'bool',
        ),
        'variadic': check.RenderedCallForm(
            (
                ('variadic positional', 'args', 'int', None),
                ('keyword-only', 'flag', 'bool', 'False'),
                ('variadic keyword', 'kwargs', 'str', None),
            ),
            'None',
        ),
        'keyword_variadic': check.RenderedCallForm(
            (('variadic keyword', 'kwargs', 'str', None),),
            'int',
        ),
        'empty': check.RenderedCallForm((), 'None'),
    }

    for name, expected_form in expected.items():
        function = module.members[name]
        assert isinstance(function, griffe.Function)
        rendered = _render_function_reference(
            function,
            line_length=48 if name == 'mixed' else 88,
        )
        signatures = check.rendered_call_forms(rendered)
        path = f'gometry.{name}'
        assert tuple(item.form for item in signatures[path]) == (expected_form,)
        assert 'class="doc-signature"' in rendered
        if expected_form.parameters:
            assert '<autoref identifier=' in rendered

    method = module.members['Methods'].members['method']
    assert isinstance(method, griffe.Function)
    rendered_method = _render_function_reference(method)
    method_form = check.rendered_call_forms(rendered_method)['gometry.Methods.method'][
        0
    ]
    assert method_form.form == check.RenderedCallForm(
        (('positional-only', 'value', 'int', None),),
        'str',
    )

    rendered_variadic = check.rendered_call_forms(
        _render_function_reference(module.members['variadic'])
    )['gometry.variadic'][0]
    assert (
        check._signature_form(rendered_variadic.source.replace('*args', 'args', 1))
        != expected['variadic']
    )
    assert (
        check._signature_form(rendered_variadic.source.replace('**kwargs', 'kwargs', 1))
        != expected['variadic']
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
    errors = model._scalar_array_return_errors(subjects, tree)

    assert len(errors) == 2, errors
    assert any('Geometry.centroid' in error for error in errors)
    assert any('Geometry.bounds' in error for error in errors)


def test_shared_subject_return_check_resolves_generic_classes_and_descriptors(
    tmp_path: Path,
) -> None:
    model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    stub = tmp_path / '_lib.pyi'
    stub.write_text(
        '''\
from typing import Generic, Self, TypeVar

_GroupValuesT_co = TypeVar('_GroupValuesT_co', covariant=True)
U = TypeVar('U')
V = TypeVar('V')
W = TypeVar('W')
X = TypeVar('X')

class GeometryArray(Generic[W]):
    def restore(self) -> Self:
        """Restore the array.

        Returns
        -------
        GeometryArray[W]
            This array.
        """

class _CellGeometryProjection(Generic[W]):
    def __get__(
        self,
        instance: CellArray,
        owner: type[CellArray] | None = None,
    ) -> GeometryArray[W]: ...

    def __get__(self, instance: None, owner: type[CellArray]) -> Self: ...

class CellArray:
    center: _CellGeometryProjection[Point]
    """The center geometries.

    Returns
    -------
    GeometryArray[Point]
        The projected geometry values.
    """

class Groups(Generic[_GroupValuesT_co]):
    @property
    def values(self) -> _GroupValuesT_co:
        """The flat backing column.

        Returns
        -------
        numpy.ndarray or GeometryArray or CellArray
            The selected backing column.
        """

class CellGroups(Generic[U]):
    @property
    def values(self) -> U:
        """The cell backing column.

        Returns
        -------
        GeometryArray
            The documented but unsupported backing type.
        """

class Deferred(Generic[V]):
    @property
    def values(self) -> V:
        """The externally specialized backing column.

        Returns
        -------
        GeometryArray
            A type that has no in-stub specialization.
        """

class PayloadBox(Generic[X]):
    @property
    def payload(self) -> X:
        """The wrapped payload.

        Returns
        -------
        GeometryArray
            The specialized payload.
        """

class Geometry:
    @property
    def bounds(self) -> tuple[float, float, float, float]:
        """The scalar bounds.

        Returns
        -------
        GeometryArray
            An incompatible concrete binding type.
        """

def geometry_groups() -> Groups[GeometryArray]: ...
def cell_groups() -> Groups[CellArray]: ...
def numpy_groups() -> Groups[npt.NDArray[np.int64]]: ...
def cell_only_groups() -> CellGroups[CellArray]: ...
def geometry_box() -> PayloadBox[GeometryArray[Point]]: ...
'''
    )
    tree = ast.parse(stub.read_text(encoding='utf-8'))
    classes = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
    from pyo3stubs.doc_structure import DocLocation

    def subject(
        class_name: str,
        member_name: str,
        kind: Literal['callable', 'property'] = 'property',
    ):
        class_node = classes[class_name]
        declaration = next(
            node
            for node in class_node.body
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == member_name
            )
            or (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id == member_name
            )
        )
        if isinstance(declaration, (ast.FunctionDef, ast.AsyncFunctionDef)):
            prose = ast.get_docstring(declaration)
        else:
            index = class_node.body.index(declaration)
            following = class_node.body[index + 1]
            prose = following.value.value
        return model.DocSubject(
            f'{class_name}.{member_name}',
            kind,
            prose,
            (),
            DocLocation(str(stub), declaration.lineno),
            (declaration,),
        )

    errors = model._scalar_array_return_errors(
        (
            subject('Groups', 'values'),
            subject('CellGroups', 'values'),
            subject('Deferred', 'values'),
            subject('PayloadBox', 'payload'),
            subject('GeometryArray', 'restore', 'callable'),
            subject('CellArray', 'center'),
            subject('Geometry', 'bounds'),
        ),
        tree,
    )

    assert len(errors) == 2, errors
    assert any('CellGroups.values' in error for error in errors)
    assert any('Geometry.bounds' in error for error in errors)
    assert not any('Deferred.values' in error for error in errors)


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


def test_griffe_expands_private_aliases_from_their_source_identity() -> None:
    pkg = _load_griffe_model()
    expand = _expand()
    lib = pkg.members['_lib']
    assert isinstance(lib, griffe.Module)
    aliases = expand.annotation_aliases(pkg)
    expression = griffe.ExprName(
        '_FloatScalarInput',
        parent=lib,
        member=lib.members['_FloatScalarInput'],
    )

    expanded = expand.expand_annotation(expression, aliases)

    assert isinstance(expanded, griffe.ExprBinOp)
    assert str(expanded) == 'FloatScalar | ScalarFloatArray'
    assert expanded.left.canonical_path == 'gometry._types.FloatScalar'
    assert expanded.right.canonical_path == 'gometry._types.ScalarFloatArray'


def test_class_materialization_preserves_source_relationships_and_copy_isolation(
    tmp_path: Path,
) -> None:
    package = tmp_path / 'gometry'
    package.mkdir()
    (package / '__init__.pyi').write_text(
        'from ._types import Record\n',
        encoding='utf-8',
    )
    (package / '_types.pyi').write_text(
        """\
from typing import overload

class Base: ...

class Record(Base):
    @overload
    def choose(self, value: int) -> int: ...
    @overload
    def choose(self, value: str) -> str: ...
""",
        encoding='utf-8',
    )
    loader = griffe.GriffeLoader(search_paths=[str(tmp_path)], allow_inspection=False)
    pkg = loader.load('gometry')
    loader.resolve_aliases(external=False)
    expand = _expand()
    alias = pkg.members['Record']
    assert isinstance(alias, griffe.Alias)
    source = alias.target
    assert isinstance(source, griffe.Class)
    expand._promote_overloads(pkg, set())
    source_method = source.members['choose']
    assert isinstance(source_method, griffe.Function)
    assert source_method.overloads

    expand._materialize_class_aliases(pkg)

    materialized = pkg.members['Record']
    assert isinstance(materialized, griffe.Class)
    assert materialized is not source
    assert materialized.bases == source.bases
    assert materialized.members['choose'] is not source_method
    assert alias.path in materialized.aliases
    copied_method = materialized.members['choose']
    assert isinstance(copied_method, griffe.Function)
    assert copied_method.overloads is not source_method.overloads
    copied_method.overloads[0].parameters['value'].annotation = griffe.ExprName('bytes')
    assert str(source_method.overloads[0].parameters['value'].annotation) == 'int'


def test_class_provenance_accepts_native_and_facade_owners_and_rejects_wrong_owner() -> (
    None
):
    pkg = _load_griffe_model()
    model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    inventory = model.load_public_symbols(
        model.STUB,
        GOMETRY_ROOT / 'python/gometry/__init__.py',
    )
    point = pkg.members['Point']
    features = pkg.members['Features']
    assert isinstance(point, griffe.Class)
    assert isinstance(features, griffe.Class)
    assert model._class_provenance_error('Point', point, inventory) is None
    assert model._class_provenance_error('Features', features, inventory) is None

    wrong_owner = copy.copy(features)
    point_source = point.extra['gometry']['class_source']
    wrong_owner.extra = {
        **features.extra,
        'gometry': {
            **features.extra['gometry'],
            'class_source': type(point_source)(
                'gometry._types.Features',
                point_source.path,
                point_source.line,
            ),
        },
    }
    error = model._class_provenance_error('Features', wrong_owner, inventory)
    assert error is not None
    assert 'does not match' in error
    assert 'gometry._types.Features' in error


def test_exported_namedtuple_forms_match_runtime_and_construct() -> None:
    pkg = _load_griffe_model()
    model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    expand = _expand()
    inventory = model.load_public_symbols(
        model.STUB,
        GOMETRY_ROOT / 'python/gometry/__init__.py',
    )
    native_classes = model._stub_class_nodes(
        ast.parse(model.STUB.read_text(encoding='utf-8'))
    )
    source_classes = dict(native_classes)
    source_classes.update({
        name: facade.declaration.node for name, facade in inventory.classes.items()
    })
    namedtuple_classes = {
        name: node
        for name, node in source_classes.items()
        if name in gm.__all__
        and any(model._stub_base_name(base) == 'NamedTuple' for base in node.bases)
    }
    assert set(namedtuple_classes) == {'Extremes', 'PolygonizeResult'}

    classifications = {}
    for name in gm.__all__:
        runtime_class = getattr(gm, name, None)
        node = source_classes.get(name)
        if not isinstance(runtime_class, type):
            continue
        assert node is not None, f'{name}: public class has no source declaration'
        if model._stub_constructor_declarations(
            node, '__new__'
        ) or model._stub_constructor_declarations(node, '__init__'):
            continue
        class_obj = pkg.members.get(name)
        assert isinstance(class_obj, griffe.Class), name
        outcome = expand.constructor_outcome(class_obj)
        if name in namedtuple_classes:
            assert isinstance(outcome, expand.RenderableConstructor), name
            classification = 'NamedTuple fields'
        elif isinstance(outcome, expand.UnavailableConstructor):
            classification = f'unavailable: {outcome.reason}'
        elif isinstance(outcome, expand.RenderableConstructor):
            classification = f'inherited: {outcome.source.qualname}'
        else:
            pytest.fail(f'{name}: missing constructor classification')
        classifications[name] = classification

    expected_unavailable = {
        'AccuracyWarning',
        'CRSError',
        'CRSMismatchError',
        'Cell',
        'GeometryError',
        'GeometryTypeError',
        'InvalidGeometryError',
        'ParseError',
        'TransformError',
    }
    assert set(classifications) == expected_unavailable | set(namedtuple_classes), (
        classifications
    )
    assert classifications['Cell'].startswith('unavailable:')
    assert all(
        classifications[name].startswith('unavailable:')
        for name in expected_unavailable - {'Cell'}
    ), classifications
    assert {
        name
        for name, classification in classifications.items()
        if classification == 'NamedTuple fields'
    } == set(namedtuple_classes), classifications
    for name, node in namedtuple_classes.items():
        runtime_class = getattr(gm, name)
        runtime_parameters = tuple(inspect.signature(runtime_class).parameters.values())
        class_obj = pkg.members[name]
        outcome = expand.constructor_outcome(class_obj)
        assert isinstance(outcome, expand.RenderableConstructor)
        forms = expand.ordered_call_forms(outcome)
        assert len(forms) == 1, name
        form = forms[0]
        source_fields = [
            member
            for member in node.body
            if isinstance(member, ast.AnnAssign)
            and isinstance(member.target, ast.Name)
            and not member.target.id.startswith('_')
            and not (
                isinstance(member.annotation, ast.Subscript)
                and model._stub_base_name(member.annotation.value) == 'ClassVar'
            )
        ]
        assert len(runtime_parameters) == len(source_fields), name
        assert [parameter.name for parameter in runtime_parameters] == [
            member.target.id for member in source_fields
        ], name
        assert [parameter.name for parameter in form.parameters] == [
            parameter.name for parameter in runtime_parameters
        ], name
        for parameter, source_field, form_parameter in zip(
            runtime_parameters,
            source_fields,
            form.parameters,
            strict=True,
        ):
            expected_default = model._expression_text(source_field.value)
            runtime_default = (
                None
                if parameter.default is inspect.Parameter.empty
                else str(parameter.default)
            )
            assert runtime_default == expected_default, (name, parameter.name)
            assert form_parameter.kind is griffe.ParameterKind.positional_or_keyword
            assert (
                None if form_parameter.default is None else str(form_parameter.default)
            ) == expected_default, (name, parameter.name)

    extremes = gm.Extremes(1, 2, 3, 4)
    assert isinstance(extremes, gm.Extremes)
    assert tuple(extremes) == (1, 2, 3, 4)
    result_fields = tuple(object() for _ in gm.PolygonizeResult._fields)
    result = gm.PolygonizeResult(*result_fields)
    assert isinstance(result, gm.PolygonizeResult)
    assert tuple(result) == result_fields


def test_generic_namedtuple_constructor_keeps_field_default_and_type_context(
    tmp_path: Path,
) -> None:
    package = tmp_path / 'gometry'
    package.mkdir()
    source_path = package / '__init__.pyi'
    source_path.write_text(
        """\
from typing import Generic, NamedTuple, TypeVar

ItemT = TypeVar('ItemT', default=int)

class Record(NamedTuple, Generic[ItemT]):
    required: str
    value: ItemT = 7
""",
        encoding='utf-8',
    )
    expand = _expand()
    loader = griffe.GriffeLoader(
        search_paths=[str(tmp_path)],
        extensions=griffe.Extensions(
            expand.PromoteStubOverloads(),
            expand.ExpandTokenAliases(),
        ),
        allow_inspection=False,
    )
    pkg = loader.load('gometry')
    loader.resolve_aliases(external=False)
    record = pkg.members['Record']
    assert isinstance(record, griffe.Class)
    outcome = expand.constructor_outcome(record)
    assert isinstance(outcome, expand.RenderableConstructor)
    forms = expand.ordered_call_forms(outcome)
    assert len(forms) == 1
    assert tuple(
        (parameter.name, parameter.kind.value, str(parameter.default))
        for parameter in forms[0].parameters
    ) == (
        ('required', 'positional or keyword', 'None'),
        ('value', 'positional or keyword', '7'),
    )
    assert str(forms[0].returns) == 'Record[ItemT]'
    assert outcome.source.path == str(source_path)

    model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    source_node = next(
        node
        for node in ast.parse(source_path.read_text(encoding='utf-8')).body
        if isinstance(node, ast.ClassDef) and node.name == 'Record'
    )
    expected = model._source_namedtuple_form(source_node, pkg, {})
    actual = model._griffe_call_form(forms[0])
    assert actual == expected

    reordered = copy.deepcopy(source_node)
    field_positions = [
        index
        for index, node in enumerate(reordered.body)
        if isinstance(node, ast.AnnAssign)
    ]
    first, second = field_positions
    reordered.body[first], reordered.body[second] = (
        reordered.body[second],
        reordered.body[first],
    )
    assert actual != model._source_namedtuple_form(reordered, pkg, {})

    without_default = copy.deepcopy(source_node)
    next(
        node
        for node in without_default.body
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == 'value'
    ).value = None
    assert actual != model._source_namedtuple_form(without_default, pkg, {})


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
    expand = _expand()
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
    check = _check()
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
        rendered = _render_function_reference(function)
        actual = check.rendered_call_forms(rendered)[function.canonical_path]
        doc_model = load_tool(
            'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
        )
        lib = pkg.members['_lib']
        assert isinstance(lib, griffe.Module)
        aliases = doc_model.annotation_aliases(pkg)
        class_name, method_name = function.canonical_path.rsplit('.', 2)[-2:]
        stub_tree = ast.parse(doc_model.STUB.read_text(encoding='utf-8'))
        class_node = next(
            node
            for node in stub_tree.body
            if isinstance(node, ast.ClassDef) and node.name == class_name
        )
        definitions = [
            node
            for node in class_node.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == method_name
            and any(
                isinstance(decorator, ast.Name) and decorator.id == 'overload'
                for decorator in node.decorator_list
            )
            and not doc_model._stub_is_never(node.returns, lib, aliases)
        ]
        source_forms = tuple(
            doc_model._stub_call_form(
                node,
                lib,
                aliases,
                receiver='self',
            )
            for node in definitions
        )
        assert source_forms and all(form is not None for form in source_forms)
        expected = tuple(
            check.RenderedCallForm(*form) for form in source_forms if form is not None
        )
        assert function.overloads
        assert '<details open class="doc-overloads-details">' in rendered
        assert '<summary>Accepted call forms</summary>' in rendered
        assert 'doc-canonical-signature' not in rendered
        assert tuple(_form_shape(item.form) for item in actual) == tuple(
            _form_shape(form) for form in expected
        )
        assert '<autoref identifier=' in rendered
        blocks = re.findall(
            r'<div class="doc-signature(?: highlight)?">.*?</div>',
            rendered,
            flags=re.DOTALL,
        )
        assert len(blocks) == len(expected)
        duplicated = rendered.replace(blocks[1], blocks[0], 1)
        mutated = check.rendered_call_forms(duplicated)[function.canonical_path]
        assert tuple(item.form for item in mutated) != expected


def test_function_template_keeps_ordinary_and_single_form_signatures() -> None:
    check = _check()
    pkg = _load_griffe_model()
    geometry = pkg.members['Geometry']
    assert isinstance(geometry, griffe.Class)

    ordinary = geometry.members['minimum_clearance']
    assert isinstance(ordinary, griffe.Function)
    assert ordinary.canonical_path == 'gometry.Geometry.minimum_clearance'
    assert not ordinary.overloads
    ordinary_html = _render_function_reference(ordinary)
    ordinary_forms = check.rendered_call_forms(ordinary_html)[ordinary.canonical_path]
    assert '<div class="doc-canonical-signature">' in ordinary_html
    assert '<details' not in ordinary_html
    assert len(ordinary_forms) == 1
    assert ordinary_forms[0].form == check.RenderedCallForm(
        (('keyword-only', 'unit', "Literal['planar', 'meters'] | None", 'None'),),
        'float',
    )


def test_griffe_preserves_public_constructor_forms_in_authored_order() -> None:
    pkg = _load_griffe_model()
    doc_model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    expand = _expand()
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


def test_constructor_parameter_tables_report_each_row_applicability() -> None:
    pkg = _load_griffe_model()
    model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    lib = pkg.members['_lib']
    assert isinstance(lib, griffe.Module)
    names = {'Point', 'LineString', 'Polygon', 'CellArray', 'CRS'}
    contracts = model._stub_constructor_contracts(
        lib,
        model.annotation_aliases(pkg),
        names,
    )
    for name in names:
        class_obj = pkg.members[name]
        assert isinstance(class_obj, griffe.Class)
        rendered = _render_class_reference(class_obj)
        parser = _ClassReference()
        parser.feed(rendered)
        expected_rows = model._call_form_parameter_rows(
            contracts[f'gometry.{name}'].forms
        )
        expected_values = {
            parameter: (
                value
                if status in {'defaulted', 'varies'}
                else 'not applicable'
                if status == 'variadic'
                else 'required'
            )
            for parameter, (status, value) in expected_rows.items()
        }
        rows = [row for row in parser.rows if row and row[0] in expected_values]
        assert len(rows) == len(expected_values), name
        assert {row[0]: row[-1] for row in rows} == expected_values, name
        assert len(parser.disclosures) == 1, name
        assert (
            ''.join(parser.disclosures[0]['summary']).strip() == 'Accepted call forms'
        )

    point_rows = model._call_form_parameter_rows(contracts['gometry.Point'].forms)
    assert point_rows['x'] == ('varies', 'varies by call form')
    assert point_rows['y'] == ('varies', 'varies by call form')
    cell_rows = model._call_form_parameter_rows(contracts['gometry.CellArray'].forms)
    assert cell_rows['type'] == ('varies', 'varies by call form')

    point = pkg.members['Point']
    assert isinstance(point, griffe.Class)
    point_html = _render_class_reference(point)
    universal_default = point_html.replace('varies by call form', 'None', 1)
    mutated_parser = _ClassReference()
    mutated_parser.feed(universal_default)
    mutated_values = {
        row[0]: row[-1] for row in mutated_parser.rows if row and row[0] in point_rows
    }
    assert mutated_values != {
        name: value if status in {'defaulted', 'varies'} else 'required'
        for name, (status, value) in point_rows.items()
    }


def test_constructor_default_projection_keeps_differing_accepted_forms() -> None:
    model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    forms = (
        (
            (
                ('keyword-only', 'value', 'int', None),
                ('keyword-only', 'stable', 'bool', 'False'),
            ),
            None,
        ),
        (
            (
                ('keyword-only', 'value', 'int', '1'),
                ('keyword-only', 'stable', 'bool', 'False'),
                ('keyword-only', 'only_here', 'str', None),
            ),
            None,
        ),
        (
            (
                ('keyword-only', 'value', 'int', '2'),
                ('keyword-only', 'stable', 'bool', 'False'),
            ),
            None,
        ),
    )

    assert model._call_form_parameter_rows(forms) == {
        'value': ('varies', 'varies by call form'),
        'stable': ('defaulted', 'False'),
        'only_here': ('varies', 'varies by call form'),
    }


def test_constructor_class_template_renders_the_source_ast_form_sequence() -> None:
    pkg = _load_griffe_model()
    check = _check()
    doc_model = load_tool(
        'check_doc_model', GOMETRY_ROOT / 'tools/gates/_check_doc_model.py'
    )
    expand = _expand()
    lib = pkg.members['_lib']
    assert isinstance(lib, griffe.Module)
    source_contracts = doc_model._stub_constructor_contracts(
        lib,
        doc_model.annotation_aliases(pkg),
        {
            'Point',
            'LineString',
            'Polygon',
            'CellArray',
            'GeometryArray',
            'CRS',
            'GeohashCell',
        },
    )
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
        html = _render_class_reference(class_obj)
        parser = _ClassReference()
        parser.feed(html)
        actual = check.rendered_call_forms(html)[class_obj.canonical_path]
        expected = tuple(
            check.RenderedCallForm(parameters, returns)
            for parameters, returns in source_contracts[class_obj.canonical_path].forms
        )
        assert parser.ids.count(class_obj.path) == 1, name
        assert len(parser.disclosures) == 1, name
        assert parser.disclosures[0]['open'], name
        assert tuple(_form_shape(item.form) for item in actual) == tuple(
            _form_shape(form) for form in expected
        ), name
        assert len(actual) == len(binding.overloads or (binding,)), name
        assert '<autoref identifier=' in html, name
        assert f'{class_obj.path}.__new__' not in html, name
        assert f'{class_obj.path}.__init__' not in html, name
        blocks = re.findall(
            r'<div class="doc-signature(?: highlight)?">.*?</div>',
            html,
            flags=re.DOTALL,
        )
        assert len(blocks) == len(expected), name
        if len(expected) > 1:
            duplicated = html.replace(blocks[1], blocks[0], 1)
            mutated = check.rendered_call_forms(duplicated)[class_obj.canonical_path]
            assert tuple(item.form for item in mutated) != expected, name


def test_constructor_receiver_is_projected_once_from_source_ast(tmp_path: Path) -> None:
    source = """\
from typing import Self, overload

class Widget:
    @overload
    def __new__(cls, self: int, /) -> Self: ...

    @overload
    def __new__(cls, value: str, *, enabled: bool = False) -> Widget: ...
"""
    package = tmp_path / 'gometry'
    package.mkdir()
    stub = package / '__init__.pyi'
    stub.write_text(source, encoding='utf-8')
    expand = _expand()
    loader = griffe.GriffeLoader(
        search_paths=[str(tmp_path)],
        extensions=griffe.Extensions(expand.PromoteStubOverloads()),
        allow_inspection=False,
    )
    module = loader.load('gometry')
    loader.resolve_aliases(external=False)
    widget = module.members['Widget']
    assert isinstance(widget, griffe.Class)
    outcome = expand.constructor_outcome(widget)
    assert isinstance(outcome, expand.RenderableConstructor)
    expected = _source_class_call_forms(stub, 'Widget')
    assert expand.ordered_call_forms(widget) == tuple(outcome.binding.overloads)

    rendered = _render_class_reference(widget)
    check = _check()
    signatures = check.rendered_call_forms(rendered)['gometry.Widget']
    assert tuple(item.form for item in signatures) == expected
    assert [parameter[1] for parameter in signatures[0].form.parameters] == ['self']
    assert [parameter[1] for parameter in signatures[1].form.parameters] == [
        'value',
        'enabled',
    ]
    assert (
        check._signature_form(signatures[0].source.replace('self', 'value', 1))
        != (expected[0])
    )


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

class Default:
    pass

class OpaqueError(ValueError):
    pass
""",
        encoding='utf-8',
    )
    loader = griffe.GriffeLoader(
        search_paths=[str(tmp_path)],
        allow_inspection=False,
    )
    module = loader.load('constructors')
    loader.resolve_aliases(external=False)
    expand = _expand()
    base = module.members['Base']
    inherited = module.members['Inherited']
    blocked = module.members['Blocked']
    default = module.members['Default']
    opaque_error = module.members['OpaqueError']
    assert isinstance(base, griffe.Class)
    assert isinstance(inherited, griffe.Class)
    assert isinstance(blocked, griffe.Class)
    assert isinstance(default, griffe.Class)
    assert isinstance(opaque_error, griffe.Class)

    source = base.members['__new__']
    assert isinstance(source, griffe.Function)
    outcome = expand._constructor_outcome(inherited, module.path)
    assert isinstance(outcome, expand.RenderableConstructor)
    binding = outcome.binding
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
    blocked_outcome = expand._constructor_outcome(blocked, module.path)
    assert isinstance(blocked_outcome, expand.UnavailableConstructor)
    assert 'non-constructible' in blocked_outcome.reason
    assert isinstance(
        expand._constructor_outcome(default, module.path),
        expand.UnavailableConstructor,
    )
    opaque_outcome = expand._constructor_outcome(opaque_error, module.path)
    assert isinstance(opaque_outcome, expand.UnavailableConstructor)
    assert 'opaque inherited built-in exception constructor' in opaque_outcome.reason


def test_unsupported_exported_constructor_fails_with_its_source_location(
    tmp_path: Path,
) -> None:
    package = tmp_path / 'gometry'
    package.mkdir()
    (package / '__init__.pyi').write_text(
        """\
class Broken:
    def __init__(wrong: int) -> None: ...
""",
        encoding='utf-8',
    )
    expand = _expand()
    loader = griffe.GriffeLoader(
        search_paths=[str(tmp_path)],
        extensions=griffe.Extensions(expand.PromoteStubOverloads()),
        allow_inspection=False,
    )
    with pytest.raises(
        expand.UnsupportedConstructorError,
        match=r':2: gometry\.Broken\.__init__',
    ):
        loader.load('gometry')


def test_inherited_generic_constructor_self_and_specialized_returns_match_stub_ast(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    expand = _expand()
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
    check = _check()
    rendered = _render_class_reference(inherited)
    signatures = check.rendered_call_forms(rendered)['gometry.Inherited']
    expected_visible = tuple(
        check.RenderedCallForm(parameters, returns)
        for parameters, returns in contract.forms
    )
    assert tuple(item.form for item in signatures) == expected_visible

    first_form = (binding.overloads or ())[0]
    monkeypatch.setattr(first_form, 'returns', 'object')
    mutated = check.rendered_call_forms(_render_class_reference(inherited))[
        'gometry.Inherited'
    ]
    assert tuple(item.form for item in mutated) != expected_visible
