"""The doc-structure rules, against both docstring dialects.

Explicit ``DocSubject`` values exercise the same structure consumer used by the
gate without a second test-only validation path.
"""

from __future__ import annotations

import ast
import inspect
from typing import Literal, cast

import pytest

from pyo3stubs.doc_structure import (
    DocLocation,
    DocSubject,
    _stub_parameters,
    check_doc_subject,
    duplicate_section_errors,
    parameter_entries,
    parse_doc,
    raises_entries,
    return_entries,
)

NUMPYDOC = """\
Do the thing.

Parameters
----------
value : int
    The input.
scale : float, default 2.0
    How much.

Returns
-------
int
    The output.
"""

GOOGLE = """\
Do the thing.

Args:
    value (int): The input.
    scale (float): How much, defaults to 2.0.

Returns:
    int: The output.
"""


def sample(value: int, scale: float = 2.0) -> int:
    return int(value * scale)


def _subject(
    qualname: str,
    prose: str,
    parameters: tuple[inspect.Parameter, ...],
    *,
    kind: Literal['callable', 'property', 'constructor', 'class'] = 'callable',
) -> DocSubject:
    return DocSubject(
        qualname,
        kind,
        prose,
        parameters,
        DocLocation('test.pyi', 1),
    )


@pytest.mark.parametrize(
    ('dialect', 'doc'),
    [pytest.param('numpydoc', NUMPYDOC), pytest.param('google', GOOGLE)],
)
def test_a_complete_docstring_passes_in_either_dialect(dialect, doc):
    subject = _subject(
        'sample',
        doc,
        tuple(inspect.signature(sample).parameters.values()),
    )
    assert check_doc_subject(subject) == [], dialect


def test_an_undocumented_parameter_is_flagged():
    doc = NUMPYDOC.replace('scale : float, default 2.0\n    How much.\n', '')
    subject = _subject(
        'sample',
        doc,
        tuple(inspect.signature(sample).parameters.values()),
    )
    assert check_doc_subject(subject) == [
        "sample: parameter 'scale' is not documented"
    ]


def test_a_documented_parameter_that_left_the_signature_is_flagged():
    doc = NUMPYDOC.replace('value : int', 'value : int\ngone : str')
    subject = _subject(
        'sample',
        doc,
        tuple(inspect.signature(sample).parameters.values()),
    )
    assert any(
        'gone' in error and 'not in the signature' in error
        for error in check_doc_subject(subject)
    )


def test_a_missing_returns_section_is_flagged():
    doc = NUMPYDOC.split('Returns', maxsplit=1)[0]
    subject = _subject(
        'sample',
        doc,
        tuple(inspect.signature(sample).parameters.values()),
    )
    assert any(
        'no Returns section' in error for error in check_doc_subject(subject)
    )


def test_a_documented_default_must_match_the_runtime():
    """Prose that states a default is a claim the runtime has to honour."""
    doc = NUMPYDOC.replace('default 2.0', 'default 3.0')
    subject = _subject(
        'sample',
        doc,
        tuple(inspect.signature(sample).parameters.values()),
    )
    assert check_doc_subject(subject) == [
        'sample.scale: documented default 3.0 != runtime 2.0'
    ]


def test_google_description_default_must_match_the_runtime():
    doc = GOOGLE.replace('defaults to 2.0', 'defaults to 3.0')
    subject = _subject(
        'sample',
        doc,
        tuple(inspect.signature(sample).parameters.values()),
    )
    assert check_doc_subject(subject) == [
        'sample.scale: documented default 3.0 != runtime 2.0'
    ]


@pytest.mark.parametrize(
    ('doc', 'dialect'),
    [
        pytest.param(
            """\
            Use a value.

            Parameters
            ----------
            value : int, default 3

            Returns
            -------
            int
            """,
            'numpydoc',
            id='numpydoc',
        ),
        pytest.param(
            """\
            Use a value.

            Args:
                value (int): defaults to 3.

            Returns:
                int: The value.
            """,
            'google',
            id='google',
        ),
    ],
)
def test_stated_default_is_rejected_for_a_required_parameter(doc, dialect):
    def required(value: int) -> int:
        return value

    subject = _subject(
        'required',
        doc,
        tuple(inspect.signature(required).parameters.values()),
    )
    assert check_doc_subject(subject) == [
        'required.value: documented default 3 but the signature has no default'
    ], dialect


def test_parsed_entries_preserve_numpy_and_google_default_forms():
    numpy = parse_doc(
        """\
        Construct a value.

        Parameters
        ----------
        name : str, default 'wkb'
            Codec name.
        bounds : tuple[float, float], default (0.0, 1.0)
            Lower and upper bounds.
        count : int, default=3
            Number of values.
        """
    )
    name, bounds, count = parameter_entries(numpy)
    assert (name.names, name.type_text, name.default_text, name.description) == (
        ('name',),
        'str',
        "'wkb'",
        'Codec name.',
    )
    assert (
        bounds.names,
        bounds.type_text,
        bounds.default_text,
        bounds.line,
    ) == (('bounds',), 'tuple[float, float]', '(0.0, 1.0)', 7)
    assert (count.names, count.type_text, count.default_text) == (
        ('count',),
        'int',
        '3',
    )

    google = parse_doc(
        """\
        Construct a value.

        Args:
            mode (str, default='auto'): Output encoding.
            count (int): Number of rows, defaults to 3.

        Raises:
            ValueError: If the value is invalid.
        """
    )
    mode, count = parameter_entries(google)
    (error,) = raises_entries(google)
    assert (mode.type_text, mode.default_text) == ('str', "'auto'")
    assert (count.type_text, count.default_text) == ('int', '3')
    assert (error.names, error.description) == (
        ('ValueError',),
        'If the value is invalid.',
    )


def test_google_returns_parse_full_types_and_keep_their_descriptions():
    doc = """\
    Compute several public result forms.

    Returns:
        list[str]: A sequence of names.
        dict[str, int]: A map of identifiers to indexes.
        Geometry | None: A geometry when one was found.
        numpy.ndarray: A dense coordinate array.
        Literal['EPSG:4326']: The selected coordinate reference.
        float
            One numeric measurement.

    Yields:
        Iterator[Geometry]: The generated geometries.

    Receives:
        Geometry | None: The optional next geometry.
    """
    parsed = parse_doc(doc)
    entries = return_entries(parsed)

    assert [
        (entry.type_text, entry.description)
        for entry in entries
    ] == [
        ('list[str]', 'A sequence of names.'),
        ('dict[str, int]', 'A map of identifiers to indexes.'),
        ('Geometry | None', 'A geometry when one was found.'),
        ('numpy.ndarray', 'A dense coordinate array.'),
        ("Literal['EPSG:4326']", 'The selected coordinate reference.'),
        ('float', 'One numeric measurement.'),
        ('Iterator[Geometry]', 'The generated geometries.'),
        ('Geometry | None', 'The optional next geometry.'),
    ]
    assert check_doc_subject(_subject('measure', doc, ())) == []


def test_duplicate_google_section_aliases_remain_visible():
    parsed = parse_doc(
        """\
        Construct a value.

        Args:
            value (int): The value.

        Arguments:
            value (int): The same value.
        """
    )
    assert [section.name for section in parsed.sections] == [
        'Parameters',
        'Parameters',
    ]
    assert duplicate_section_errors(parsed, 'sample') == [
        "sample: duplicate 'Parameters' section"
    ]


def test_class_overview_does_not_require_callable_returns():
    subject = _subject(
        'Widget',
        """\
        A widget.

        Parameters
        ----------
        size : int
            Widget size.
        """,
        (inspect.Parameter('size', inspect.Parameter.POSITIONAL_OR_KEYWORD),),
        kind='class',
    )
    assert check_doc_subject(subject) == []


STUB_OVERRIDE = cast(
    'ast.FunctionDef',
    ast.parse(
        '''\
@pytest.overload
def inherited(self, value: int, flag: bool = False) -> int: ...
def inherited(self, value: int, flag: bool = False) -> int:
    """An inherited override.

    Parameters
    ----------
    value : int
        The value.
    flag : bool, default False
        Whether to keep the sign.

    Returns
    -------
    int
        The result.
    """
'''
    ).body[-1],
)


def test_stub_only_inherited_override_uses_final_stub_signature() -> None:
    subject = _subject(
        'Child.inherited',
        ast.get_docstring(STUB_OVERRIDE) or '',
        tuple(_stub_parameters(STUB_OVERRIDE)),
    )
    assert check_doc_subject(subject) == []


def test_stub_only_inherited_override_flags_stub_default_mismatch() -> None:
    doc = (ast.get_docstring(STUB_OVERRIDE) or '').replace(
        'default False', 'default True'
    )
    subject = _subject(
        'Child.inherited',
        doc,
        tuple(_stub_parameters(STUB_OVERRIDE)),
    )
    assert check_doc_subject(subject) == [
        'Child.inherited.flag: documented default True != runtime False'
    ]
