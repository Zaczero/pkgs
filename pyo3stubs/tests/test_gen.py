"""The generation half: what gets written into the stub, and what must not.

The whole of ``gen.py`` used to be covered only through the fixture package,
whose every docstring is single-line, top-level and quote-free — so the
idempotence test proved a fixed point over one path. These exercise the
escaper and the def counter directly, which is where the defects were.
"""

from __future__ import annotations

import types
from typing import Self

import libcst as cst
import pytest

from pyo3stubs.gen import DocInjector, _def_totals, _docstring_statement

Q = '"'


def rendered(doc: str, indent: str = '    ') -> str:
    """The value a reader of the generated stub would see."""
    return eval(cst.Module(body=[_docstring_statement(doc, indent)]).code.strip())  # noqa: S307


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
    return (
        cst.parse_module(stub).visit(DocInjector(runtime, _def_totals(stub), {})).code
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


def test_cpython_boilerplate_never_reaches_the_stub():
    """`__new__` carries CPython's text, not the `#[new]`'s `///`.

    PyO3 does not attach a constructor's doc comment to anything Python can
    read, so injecting `__new__.__doc__` published "Create and return a new
    object.  See help(type) for accurate signature." over the Rust prose — 21
    times in gometry's shipped stub, and once each in h2corn's and
    lrucache-rs's.
    """
    from pyo3stubs.gen import _runtime_doc_for_stub

    assert _runtime_doc_for_stub('__new__', object.__new__) is None
    assert _runtime_doc_for_stub('__class_getitem__', dict.__class_getitem__) is None
    # The same text on a normal member is prose, not boilerplate.
    assert _runtime_doc_for_stub('make', object.__new__) is not None


def test_the_constructor_falls_back_to_the_rust_doc_comment(tmp_path):
    """The `#[new]`'s `///` is the only place the prose exists — read it there."""
    from conftest import make_config

    from pyo3stubs.rust_scan import constructor_docs

    src = (
        '#[pyclass(name = "Widget")]\n'
        'pub struct PyWidget;\n'
        '\n'
        '#[pymethods]\n'
        'impl PyWidget {\n'
        '    /// Build a widget of the requested size.\n'
        '    #[new]\n'
        '    fn new(size: usize) -> Self { Self }\n'
        '}\n'
    )
    cfg = make_config(tmp_path, src_text=src)
    assert constructor_docs(cfg) == {'Widget': 'Build a widget of the requested size.'}

    stub = 'class Widget:\n    def __new__(cls, size: int) -> Widget: ...\n'

    class Widget:
        """Class prose."""

        # A PyO3 class installs its own `tp_new`, so `__new__` is in
        # `vars(cls)` and is not treated as a stub-only override.
        def __new__(cls, size: int) -> Self:
            del size
            return super().__new__(cls)

    module = types.ModuleType('m')
    module.Widget = Widget  # type: ignore[attr-defined]
    out = (
        cst
        .parse_module(stub)
        .visit(DocInjector(module, _def_totals(stub), constructor_docs(cfg)))
        .code
    )
    assert 'Build a widget of the requested size.' in out
    assert 'See help(type)' not in out


def test_an_impl_inside_a_project_macro_is_still_scanned(tmp_path):
    """A macro-wrapped `impl` hid 29 of gometry's 195 getters from every scan.

    The grammar hands a macro body back as opaque tokens, so
    `frozen_pymethods! { impl PyWidget { … } }` was invisible. Re-parsing the
    body recovers it — and the recovered items keep their real line numbers.
    """
    from conftest import make_config

    from pyo3stubs.rust_scan import constructor_docs, iter_sources

    src = (
        '#[pyclass(name = "Widget")]\n'
        'pub struct PyWidget;\n'
        '\n'
        'frozen_pymethods! {\n'
        'impl PyWidget {\n'
        '    /// Build a widget.\n'
        '    #[new]\n'
        '    fn new() -> Self { Self }\n'
        '}\n'
        '}\n'
    )
    cfg = make_config(tmp_path, src_text=src)
    assert constructor_docs(cfg) == {'Widget': 'Build a widget.'}

    inner = [
        item
        for source in iter_sources(cfg)
        for item in source.walk()
        if item.attr('new') is not None
    ]
    assert [item.line for item in inner] == [8], 'line numbers survive the re-parse'
