"""The generation half: what gets written into the stub, and what must not.

The whole of ``gen.py`` used to be covered only through the fixture package,
whose every docstring is single-line, top-level and quote-free — so the
idempotence test proved a fixed point over one path. These exercise the
escaper and the def counter directly, which is where the defects were.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import types
from array import array
from typing import Self

import libcst as cst
import pytest

from pyo3stubs.ast_util import annotated_data_descriptors
from pyo3stubs.gen import (
    DocInjector,
    _def_totals,
    _descriptor_docs,
    _docstring_statement,
)
from tests._support import make_config

Q = '"'


def rendered(doc: str, indent: str = '    ') -> str:
    """The value a reader of the generated stub would see."""
    return eval(cst.Module(body=[_docstring_statement(doc, indent)]).code.strip())  # ruff: ignore[suspicious-eval-usage]


@pytest.mark.parametrize(
    'doc',
    [
        pytest.param(f'He said {Q}hi{Q}', id='ends-with-a-quote'),
        pytest.param(f'abc{Q * 2}', id='ends-with-two-quotes'),
        pytest.param(f'abc{Q * 3}', id='ends-with-three-quotes'),
        pytest.param(f'say {Q * 3}this{Q * 3} ok', id='triple-quote-inside'),
        pytest.param(f'a{Q * 4}b', id='four-quotes-inside'),
        pytest.param(Q, id='only-a-quote'),
        pytest.param('path C:\\', id='trailing-backslash'),
        pytest.param(r'a\nb', id='literal-backslash-n'),
        pytest.param('a\rb', id='carriage-return'),
        pytest.param('emoji \U0001f30d', id='non-bmp'),
    ],
)
def test_a_single_line_docstring_round_trips_exactly(doc):
    """Whatever the Rust `///` said is what the stub means.

    A doc ending in one quote closed the literal early and `gen-docs` aborted
    with a parser error; ending in two silently produced a *shorter* literal,
    so the stub kept truncated prose and `gen-docs-sync` called it in sync
    forever. A `\\r` was folded into a newline.
    """
    assert rendered(doc) == doc


@pytest.mark.parametrize(
    'doc',
    [
        pytest.param('first\nsecond\n\nfourth', id='blank-line'),
        pytest.param('first\n  \nthird', id='whitespace-only-line'),
        pytest.param(f'first\nends {Q}quoted{Q}', id='quoted-last-line'),
    ],
)
def test_a_multi_line_docstring_keeps_its_text(doc):
    """Continuation lines are re-indented; none of the prose changes."""

    def strip(text: str) -> str:
        return '\n'.join(line.strip() for line in text.split('\n')).strip()

    assert strip(rendered(doc)) == strip(doc)


def test_no_generated_line_carries_trailing_whitespace():
    code = cst.Module(body=[_docstring_statement('first\n  \nthird', '    ')]).code
    assert [line for line in code.split('\n') if line != line.rstrip()] == []


def _inject(stub: str, runtime: types.ModuleType) -> str:
    module = ast.parse(stub)
    classes = {
        node.name: node for node in module.body if isinstance(node, ast.ClassDef)
    }
    return (
        cst
        .parse_module(stub)
        .visit(
            DocInjector(
                runtime,
                _def_totals(module),
                {},
                _descriptor_docs(classes, runtime),
            )
        )
        .code
    )


def test_a_property_setter_is_not_an_overload_variant():
    """The property's `__doc__` is the getter's, so the getter is the carrier.

    Counting the pair as a same-name group of two made the *setter* the
    carrier: the getter's prose was stripped and the setter received it.
    `structural._check_overload_group` has always special-cased the pair;
    the writer did not.
    """
    stub = (
        'class C:\n'
        '    @property\n'
        '    def size(self) -> int:\n'
        '        """Getter prose."""\n'
        '    @size.setter\n'
        '    def size(self, v: int) -> None:\n'
        '        """Setter prose, hand-written."""\n'
    )

    class C:
        """Class prose."""

        @property
        def size(self) -> int:
            """Getter prose."""
            return 0

    module = types.ModuleType('m')
    module.C = C  # type: ignore[attr-defined]
    out = _inject(stub, module)

    assert out.count('Getter prose.') == 1
    # The setter is left alone entirely -- it is not a stale overload variant
    # whose prose should be stripped, and the runtime has nothing to put there.
    assert 'Setter prose, hand-written.' in out


@pytest.mark.parametrize(
    ('owner', 'name'),
    [
        pytest.param(array, 'itemsize', id='native-getset'),
        pytest.param(complex, 'real', id='native-member'),
    ],
)
def test_native_property_prose_is_refreshed(owner, name):
    stub = f'''\
class C:
    @property
    def {name}(self) -> int:
        """Stale property prose."""
'''
    runtime = types.ModuleType('m')
    runtime.C = owner  # type: ignore[attr-defined]

    generated = _inject(stub, runtime)
    class_node = ast.parse(generated).body[0]
    assert isinstance(class_node, ast.ClassDef)
    member = class_node.body[-1]
    assert isinstance(member, ast.FunctionDef)
    assert ast.get_docstring(member) == vars(owner)[name].__doc__
    assert _inject(generated, runtime) == generated


def test_annotated_data_descriptor_prose_is_replaced_idempotently() -> None:
    stub = '''\
class C:
    value: int
    """Stale descriptor prose."""
    constant: int
    """Hand-written constant prose."""
'''

    class NativeDescriptor:
        """Native descriptor prose."""

        def __get__(self, instance, owner=None):
            raise AssertionError(
                'descriptor getter must not run while finding native prose'
            )

        def __set__(self, instance, value):
            raise AssertionError('descriptor assignment is not part of generation')

    class C:
        value = NativeDescriptor()
        constant = 1

    runtime = types.ModuleType('m')
    runtime.C = C  # type: ignore[attr-defined]
    class_node = ast.parse(stub).body[0]
    assert isinstance(class_node, ast.ClassDef)
    descriptors = list(annotated_data_descriptors(C, class_node.body))
    assert [(name, prose) for name, _, prose, _ in descriptors] == [
        ('value', 'Stale descriptor prose.'),
    ]

    once = _inject(stub, runtime)
    twice = _inject(once, runtime)
    assert once == twice
    assert once.count('Native descriptor prose.') == 1
    assert 'Stale descriptor prose.' not in once
    assert 'Hand-written constant prose.' in once
    before = ast.parse(stub).body[0]
    after = ast.parse(once).body[0]

    def without_docstrings(node):
        return [
            statement
            for statement in node.body
            if not (
                isinstance(statement, ast.Expr)
                and isinstance(statement.value, ast.Constant)
                and isinstance(statement.value.value, str)
            )
        ]

    assert [ast.dump(stmt) for stmt in without_docstrings(before)] == [
        ast.dump(stmt) for stmt in without_docstrings(after)
    ]


def test_doc_injector_reads_static_and_class_method_declarations():
    stub = """\
class C:
    @staticmethod
    def parse(value: str) -> C: ...

    @classmethod
    def from_value(cls, value: str) -> C: ...
"""

    class C:
        @staticmethod
        def parse(value: str) -> C:
            """Static method prose."""
            raise NotImplementedError

        @classmethod
        def from_value(cls, value: str) -> C:
            """Class method prose."""
            raise NotImplementedError

    runtime = types.ModuleType('m')
    runtime.C = C  # type: ignore[attr-defined]

    rendered = _inject(stub, runtime)

    assert 'Static method prose.' in rendered
    assert 'Class method prose.' in rendered


@pytest.mark.parametrize(
    ('stub_body', 'label'),
    [
        pytest.param(
            '    async def go(self) -> None:\n        """Prose."""\n', 'async'
        ),
        pytest.param(
            '    if True:\n        def go(self) -> None:\n            """Prose."""\n',
            'gated',
        ),
    ],
)
def test_a_def_the_counter_misses_loses_its_docstring(stub_body, label):
    """Every def libcst visits has to be counted, or it is treated as a stray.

    `ast` models `async def` as its own node and libcst does not, and the
    counter walked only direct children — so an `async def`, or a def behind
    `if sys.version_info >= …:`, was never the carrier and was stripped.
    """
    stub = f'class C:\n{stub_body}'

    class C:
        """Class prose."""

        def go(self) -> None:
            """Prose."""

    module = types.ModuleType('m')
    module.C = C  # type: ignore[attr-defined]
    assert 'Prose.' in _inject(stub, module), label


@pytest.mark.parametrize(
    ('before', 'after'),
    [
        pytest.param('if sys.platform == "linux":\n', '', id='platform'),
        pytest.param('try:\n', 'except ImportError:\n    pass\n', id='try'),
        pytest.param('try:\n    pass\nexcept ImportError:\n', '', id='except'),
        pytest.param('try:\n    pass\nexcept* ImportError:\n', '', id='except-star'),
        pytest.param('try:\n    pass\nfinally:\n', '', id='finally'),
    ],
)
def test_guarded_class_and_function_keep_their_indented_prose(tmp_path, before, after):
    from pyo3stubs.gen import render_stub_with_docs

    stub = (
        'import sys\n' + before + '    class Sealed:\n'
        '        maybe: int | None\n'
        '        def method(self, value: int, flag: bool = True) -> int: ...\n'
        '    def documented(a: int, b: str = "x") -> str: ...\n' + after
    )
    cfg = make_config(tmp_path, stub_text=stub)
    runtime = importlib.import_module(cfg.module)
    rendered = render_stub_with_docs(cfg)
    nodes = tuple(ast.walk(ast.parse(rendered)))
    cls = next(node for node in nodes if isinstance(node, ast.ClassDef))
    function = next(
        node
        for node in nodes
        if isinstance(node, ast.FunctionDef) and node.name == 'documented'
    )
    method = next(node for node in cls.body if isinstance(node, ast.FunctionDef))
    assert ast.get_docstring(method) == inspect.getdoc(runtime.Sealed.method)
    attribute_doc = next(
        node.value.value
        for node in cls.body
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
        and 'Optional payload' in node.value.value
    )
    assert inspect.cleandoc(attribute_doc) == inspect.getdoc(runtime.Sealed.maybe)
    assert ast.get_docstring(function) == inspect.getdoc(runtime.documented)
    assert '\n            Parameters\n' in rendered
    assert '\n        Parameters\n' in rendered
    assert '\n            """\n' in rendered
    assert render_stub_with_docs(make_config(tmp_path, stub_text=rendered)) == rendered


def test_cpython_boilerplate_never_reaches_the_stub():
    """`__new__` carries CPython's text, not the `#[new]`'s `///`.

    PyO3 does not attach a constructor's doc comment to anything Python can
    read, so injecting `__new__.__doc__` published "Create and return a new
    object.  See help(type) for accurate signature." over the Rust prose — 21
    times in gometry's shipped stub, and once each in h2corn's and
    lrucache-rs's.
    """
    from pyo3stubs.ast_util import runtime_doc_for_stub

    assert runtime_doc_for_stub('__new__', object.__new__) is None
    assert runtime_doc_for_stub('__class_getitem__', dict.__class_getitem__) is None
    # The same text on a normal member is prose, not boilerplate.
    assert runtime_doc_for_stub('make', object.__new__) is not None


@pytest.mark.parametrize('newline', ['\n', '\r\n'], ids=('lf', 'crlf'))
def test_the_constructor_falls_back_to_the_rust_doc_comment(tmp_path, newline):
    """The `#[new]`'s `///` is the only place the prose exists — read it there."""
    from pyo3stubs.rust_scan import constructor_doc_sources

    src = (
        '#[pyclass(name = "Widget")]\n'
        'pub struct PyWidget;\n'
        '\n'
        '#[pymethods]\n'
        'impl PyWidget {\n'
        '    /// Build a widget of the requested size.\n'
        '    ///\n'
        '    /// Parameters\n'
        '    /// ----------\n'
        '    /// size : int, default 3\n'
        '    ///     Requested size.\n'
        '    ///\n'
        '    /// Returns\n'
        '    /// -------\n'
        '    /// Widget\n'
        '    #[new]\n'
        '    #[pyo3(signature = (size = 3))]\n'
        '    fn new(size: usize) -> Self { Self }\n'  # ruff: ignore[missing-f-string-syntax] - Rust fixture.
        '}\n'
    )
    src = src.replace('\n', newline)
    cfg = make_config(tmp_path, src_text=src)
    expected_doc = """\
Build a widget of the requested size.

Parameters
----------
size : int, default 3
    Requested size.

Returns
-------
Widget"""
    constructor_sources = constructor_doc_sources(cfg)
    assert constructor_sources.by_class['Widget'].prose == expected_doc
    assert constructor_sources.by_class['Widget'].reason is None

    stub = 'class Widget:\n    def __new__(cls, size: int = 3) -> Widget: ...\n'

    class Widget:
        """Class prose."""

        def __new__(cls, size: int = 3) -> Self:
            del size
            return super().__new__(cls)

    module = types.ModuleType('m')
    module.Widget = Widget  # type: ignore[attr-defined]
    out = (
        cst
        .parse_module(stub)
        .visit(
            DocInjector(
                module,
                _def_totals(ast.parse(stub)),
                constructor_doc_sources(cfg).by_class,
            )
        )
        .code
    )
    assert 'Build a widget of the requested size.' in out
    assert 'See help(type)' not in out

    from pyo3stubs.context import CheckContext
    from pyo3stubs.doc_structure import check_doc_subject, iter_doc_subjects

    generated_cfg = make_config(tmp_path, stub_text=out, src_text=src)
    subjects = tuple(
        iter_doc_subjects(CheckContext(generated_cfg), runtime_module=module)
    )
    constructor = next(subject for subject in subjects if subject.kind == 'constructor')
    assert check_doc_subject(constructor) == []

    mutated = out.replace('default 3', 'default 4', 1)
    assert mutated != out
    mutated_cfg = make_config(tmp_path, stub_text=mutated, src_text=src)
    mutated_subjects = tuple(
        iter_doc_subjects(CheckContext(mutated_cfg), runtime_module=module)
    )
    mutated_constructor = next(
        subject for subject in mutated_subjects if subject.kind == 'constructor'
    )
    assert mutated_constructor.prose == expected_doc


def test_an_impl_inside_a_project_macro_is_still_scanned(tmp_path):
    """A macro-wrapped `impl` hid 29 of gometry's 195 getters from every scan.

    The grammar hands a macro body back as opaque tokens, so
    `frozen_pymethods! { impl PyWidget { … } }` was invisible. Re-parsing the
    body recovers it — and the recovered items keep their real line numbers.
    """
    from pyo3stubs.rust_scan import constructor_doc_sources, iter_sources

    src = (
        '#[pyclass(name = "Widget")]\n'
        'pub struct PyWidget;\n'
        '\n'
        'frozen_pymethods! {\n'
        'impl PyWidget {\n'
        '    /// Build a widget.\n'
        '    #[new]\n'
        '    fn new() -> Self { Self }\n'  # ruff: ignore[missing-f-string-syntax] - Rust fixture.
        '}\n'
        '}\n'
    )
    cfg = make_config(tmp_path, src_text=src)
    assert constructor_doc_sources(cfg).by_class['Widget'].prose == 'Build a widget.'

    inner = [
        item
        for source in iter_sources(cfg)
        for item in source.walk()
        if item.attr('new') is not None
    ]
    assert [item.line for item in inner] == [8], 'line numbers survive the re-parse'


def test_configured_constructor_macro_uses_its_impl_doc_carrier(tmp_path):
    """A macro-generated `#[new]` can carry prose on its class impl input."""
    from pyo3stubs.rust_scan import constructor_doc_sources, iter_sources

    src = (
        '#[pyclass(name = "RenamedWidget")]\n'
        'pub struct PyWidget;\n'
        '#[pyclass(name = "OtherWidget")]\n'
        'pub struct PyOtherWidget;\n'
        '\n'
        'make_array! {\n'
        '    /// Build a renamed widget array.\n'
        '    impl PyWidget {\n'
        '        iter: PyWidgetIterator,\n'
        '        scalar: PyWidget,\n'
        '        label: "widget",\n'
        '        class_name: "RenamedWidget",\n'
        '        build_scalar: |id| PyWidget { id },\n'
        '    }\n'
        '}\n'
        '\n'
        'make_array! {\n'
        '    /// This non-impl error must not become a constructor carrier.\n'
        '    @ PyOtherWidget { value: "nested identifier" }\n'
        '}\n'
        '\n'
        'make_array! {\n'
        '    /// This class has no exported PyO3 binding.\n'
        '    impl PyGhost {}\n'
        '}\n'
    )
    cfg = make_config(
        tmp_path,
        src_text=src,
        constructor_doc_macros=frozenset({'make_array'}),
    )

    macro_calls = [
        item
        for rust_source in iter_sources(cfg)
        for item in rust_source.walk()
        if item.kind == 'macro_invocation' and item.name == 'make_array'
    ]
    assert len(macro_calls) == 3
    negative_items = tuple(
        child
        for child in macro_calls[1].children
        if child.kind == 'ERROR' and child.doc
    )
    assert len(negative_items) == 1
    (negative,) = negative_items
    assert negative.doc == 'This non-impl error must not become a constructor carrier.'
    assert negative.node.children[0].type != 'impl'
    assert 'PyOtherWidget' in negative.text

    inventory = constructor_doc_sources(cfg)
    source = inventory.by_class['RenamedWidget']
    assert source.prose == 'Build a renamed widget array.'
    assert source.reason is None
    assert set(inventory.by_class) == {'RenamedWidget'}
    assert (source.path, source.line) == ('src/lib.rs', 8)
    (unsupported_item,) = tuple(
        issue
        for issue in inventory.issues
        if issue.python_class is None and issue.prose == negative.doc
    )
    assert unsupported_item.reason is not None
    assert 'unsupported documented item' in unsupported_item.reason
    (unexported,) = tuple(
        issue
        for issue in inventory.issues
        if issue.reason is not None and 'PyGhost' in issue.reason
    )
    assert unexported.python_class is None
    assert unexported.reason is not None
    assert 'exported PyO3 class' in unexported.reason

    unregistered = constructor_doc_sources(
        make_config(tmp_path, src_text=src, constructor_doc_macros=frozenset())
    )
    assert unregistered.by_class == {}
    assert unregistered.issues == ()


def test_h3_array_macro_recovers_each_authored_constructor_doc(tmp_path):
    """Each configured macro invocation carries prose for its own class."""
    from pyo3stubs.rust_scan import constructor_doc_sources

    source = """\
#[pyclass(name = "H3VertexArray")]
pub struct PyH3VertexArray;
#[pyclass(name = "H3EdgeArray")]
pub struct PyH3EdgeArray;

h3_index_array_common! {
    /// Build an array of H3 vertices.
    impl PyH3VertexArray {
        class_name: "H3VertexArray",
    }
}

h3_index_array_common! {
    /// Build an array of H3 directed edges.
    impl PyH3EdgeArray {
        class_name: "H3EdgeArray",
    }
}
"""
    cfg = make_config(
        tmp_path,
        src_text=source,
        constructor_doc_macros=frozenset({'h3_index_array_common'}),
    )
    sources = constructor_doc_sources(cfg).by_class
    vertex = sources['H3VertexArray']
    edge = sources['H3EdgeArray']

    assert vertex.path == 'src/lib.rs'
    assert edge.path == vertex.path
    assert vertex.prose == 'Build an array of H3 vertices.'
    assert edge.prose == 'Build an array of H3 directed edges.'
    assert vertex.prose != edge.prose
    assert vertex.line < edge.line


@pytest.mark.parametrize(
    ('source', 'macro_names', 'expected_reason', 'expected_issue'),
    [
        pytest.param(
            (
                '#[pyclass(name = "Widget")]\n'
                'pub struct PyWidget;\n'
                '#[pymethods]\n'
                'impl PyWidget {\n'
                '    #[new]\n'
                '    fn new(size: usize) -> Self { Self }\n'  # ruff: ignore[missing-f-string-syntax] - Rust fixture.
                '}\n'
                'make_array! {\n'
                '    /// Macro prose cannot replace the direct carrier.\n'
                '    impl PyWidget {}\n'
                '}\n'
            ),
            frozenset({'make_array'}),
            'direct #[new] carrier has no authored prose',
            None,
            id='direct-absence-precedes-macro-prose',
        ),
        pytest.param(
            (
                '#[pyclass(name = "Widget")]\n'
                'pub struct PyWidget;\n'
                'make_array! {\n'
                '    impl PyWidget {}\n'
                '}\n'
                'make_array! {\n'
                '    /// Unsupported documented macro item.\n'
                '    let _ = PyGhost { value: "nested identifier" };\n'
                '}\n'
            ),
            frozenset({'make_array'}),
            'configured constructor-doc macro carrier has no authored prose',
            'unsupported documented item',
            id='configured-macro-absence-and-unsupported-item',
        ),
    ],
)
def test_missing_constructor_prose_overrides_stale_stub_and_fails_sync(
    tmp_path,
    source,
    macro_names,
    expected_reason,
    expected_issue,
):
    from pyo3stubs.context import CheckContext
    from pyo3stubs.doc_contract import collect_errors
    from pyo3stubs.doc_structure import iter_doc_subjects
    from pyo3stubs.gates import Status, gate
    from pyo3stubs.gen import render_stub_with_docs
    from pyo3stubs.rust_scan import constructor_doc_sources

    stub = '''\
from typing import Self

class Widget:
    """A widget."""

    def __new__(cls, size: int = 3) -> Self:
        """Stale constructor prose."""
'''
    cfg = make_config(
        tmp_path,
        stub_text=stub,
        src_text=source,
        module='fakepkg.constructor_only',
        constructor_doc_macros=macro_names,
    )
    inventory = constructor_doc_sources(cfg)
    constructor_source = inventory.by_class['Widget']
    assert constructor_source.prose is None
    assert constructor_source.reason == expected_reason

    subjects = tuple(
        iter_doc_subjects(
            CheckContext(cfg),
            constructor_sources=inventory.by_class,
        )
    )
    constructor = next(subject for subject in subjects if subject.kind == 'constructor')
    assert constructor.prose is None

    rendered = render_stub_with_docs(cfg)
    assert 'Stale constructor prose.' not in rendered

    contract_errors = collect_errors(cfg)
    assert any(expected_reason in error for error in contract_errors)
    sync = gate('gen-docs-sync').run(cfg)
    assert sync.status is Status.VIOLATIONS
    assert any(expected_reason in error for error in sync.errors)
    if expected_issue is not None:
        assert any(expected_issue in error for error in contract_errors)
        assert any(expected_issue in error for error in sync.errors)
