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
    assert check_doc_subject(subject) == ["sample: parameter 'scale' is not documented"]


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
    assert any('no Returns section' in error for error in check_doc_subject(subject))


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


@pytest.mark.parametrize(
    ('style', 'section', 'header', 'expected_type', 'expected_default'),
    [
        pytest.param(
            'numpy',
            'Parameters',
            "literal : Literal['x, default 3']",
            "Literal['x, default 3']",
            None,
            id='numpy-literal-without-default',
        ),
        pytest.param(
            'numpy',
            'Parameters',
            "literal : Literal['x, default 3'], default None",
            "Literal['x, default 3']",
            'None',
            id='numpy-literal-with-outer-default',
        ),
        pytest.param(
            'google',
            'Args',
            "literal (Literal['x, default 3']): A literal value.",
            "Literal['x, default 3']",
            None,
            id='google-literal-without-default',
        ),
        pytest.param(
            'google',
            'Args',
            "literal (Literal['x, default 3'], default=None): A literal value.",
            "Literal['x, default 3']",
            'None',
            id='google-literal-with-outer-default',
        ),
        pytest.param(
            'numpy',
            'Returns',
            "Literal['x, default 3']",
            "Literal['x, default 3']",
            None,
            id='numpy-literal-return',
        ),
        pytest.param(
            'google',
            'Returns',
            "Literal['x, default 3']: A literal result.",
            "Literal['x, default 3']",
            None,
            id='google-literal-return',
        ),
    ],
)
def test_default_markers_inside_literal_values_remain_type_text(
    style,
    section,
    header,
    expected_type,
    expected_default,
):
    if style == 'numpy':
        underline = '-' * len(section)
        doc = f"""\
        Use a literal value.

        {section}
        {underline}
        {header}
            Literal field.
        """
    else:
        doc = f"""\
        Use a literal value.

        {section}:
            {header}
        """
    parsed = parse_doc(doc)
    entries = (
        parameter_entries(parsed)
        if section in {'Parameters', 'Args'}
        else return_entries(parsed)
    )

    assert len(entries) == 1
    assert (entries[0].type_text, entries[0].default_text) == (
        expected_type,
        expected_default,
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
        Literal['x, default 3']: A literal value that contains default-like text.
        float
            One numeric measurement.

    Yields:
        Iterator[Geometry]: The generated geometries.

    Receives:
        Geometry | None: The optional next geometry.
    """
    parsed = parse_doc(doc)
    entries = return_entries(parsed)

    assert [(entry.type_text, entry.description) for entry in entries] == [
        ('list[str]', 'A sequence of names.'),
        ('dict[str, int]', 'A map of identifiers to indexes.'),
        ('Geometry | None', 'A geometry when one was found.'),
        ('numpy.ndarray', 'A dense coordinate array.'),
        ("Literal['EPSG:4326']", 'The selected coordinate reference.'),
        (
            "Literal['x, default 3']",
            'A literal value that contains default-like text.',
        ),
        ('float', 'One numeric measurement.'),
        ('Iterator[Geometry]', 'The generated geometries.'),
    ]
    assert check_doc_subject(_subject('measure', doc, ())) == []


@pytest.mark.parametrize(
    ('header', 'expected_name', 'expected_type', 'expected_default'),
    [
        pytest.param(
            'pair (tuple[int, int], default=(0, 1)): Bounds.',
            'pair',
            'tuple[int, int]',
            '(0, 1)',
            id='nested-parentheses-in-default',
        ),
        pytest.param(
            "token (Literal[')']): Token.",
            'token',
            "Literal[')']",
            None,
            id='quoted-close-paren-in-type',
        ),
        pytest.param(
            "token (Literal['default=None']): Token.",
            'token',
            "Literal['default=None']",
            None,
            id='quoted-default-like-type-literal',
        ),
    ],
)
def test_google_parameter_headers_scan_the_complete_type_wrapper(
    header,
    expected_name,
    expected_type,
    expected_default,
):
    parsed = parse_doc(
        f"""\
        Read a value.

        Args:
            {header}
        """
    )
    (entry,) = parameter_entries(parsed)

    assert (entry.names, entry.type_text, entry.default_text) == (
        (expected_name,),
        expected_type,
        expected_default,
    )


def test_numpy_untyped_parameter_headers_cover_named_varargs_without_prose():
    doc = """\
    Accept values.

    Parameters
    ----------
    value
        The primary value.
    *args
        Additional positional values.
    **kwargs
        Additional named values.
    provide an integer
        This sentence is prose, not a parameter name.

    Returns
    -------
    object
    """
    parsed = parse_doc(doc)
    entries = parameter_entries(parsed)

    assert [
        (entry.names, entry.type_text, entry.default_text) for entry in entries
    ] == [
        (('value',), '', None),
        (('args',), '', None),
        (('kwargs',), '', None),
    ]
    parameters = (
        inspect.Parameter('value', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        inspect.Parameter('args', inspect.Parameter.VAR_POSITIONAL),
        inspect.Parameter('kwargs', inspect.Parameter.VAR_KEYWORD),
    )
    assert check_doc_subject(_subject('accept', doc, parameters)) == []

    missing_kwargs = doc.replace(
        '    **kwargs\n        Additional named values.\n',
        '',
    )
    assert check_doc_subject(_subject('accept', missing_kwargs, parameters)) == [
        "accept: parameter 'kwargs' is not documented"
    ]


@pytest.mark.parametrize(
    'default_marker',
    [
        pytest.param('default=True', id='equals'),
        pytest.param('default True', id='space'),
        pytest.param('default: True', id='colon'),
    ],
)
def test_numpy_default_marker_forms_parse_and_check_against_the_signature(
    default_marker,
):
    doc = f"""\
    Copy a value.

    Parameters
    ----------
    copy : bool, {default_marker}
        Whether to copy.

    Returns
    -------
    object
    """
    (entry,) = parameter_entries(parse_doc(doc))

    assert (entry.names, entry.type_text, entry.default_text) == (
        ('copy',),
        'bool',
        'True',
    )
    parameter = inspect.Parameter(
        'copy',
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        default=False,
    )
    assert check_doc_subject(_subject('copy_value', doc, (parameter,))) == [
        'copy_value.copy: documented default True != runtime False'
    ]


def test_receives_does_not_satisfy_the_output_contract():
    doc = """\
    Consume a value.

    Receives
    --------
    value : object
        The value sent into the generator.
    """

    assert return_entries(parse_doc(doc)) == ()
    assert check_doc_subject(_subject('consume', doc, ())) == [
        'consume: no Returns section — document what it gives back'
    ]


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
        """,
        (),
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
