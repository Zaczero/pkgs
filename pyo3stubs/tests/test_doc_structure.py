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
    SignatureParameter,
    _stub_parameters,
    check_doc_subject,
    doc_style_errors,
    duplicate_section_errors,
    parameter_entries,
    parse_doc,
    raises_entries,
    resolve_parameter_rows,
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


@pytest.mark.parametrize(
    ('prose', 'style', 'invalid'),
    [
        pytest.param(NUMPYDOC, 'numpy', False, id='numpy'),
        pytest.param(GOOGLE, 'google', False, id='google'),
        pytest.param(GOOGLE, 'numpy', True, id='google-in-numpy'),
        pytest.param(
            'Consume a value.\n\nargs:\n    value: Input.',
            'numpy',
            True,
            id='lowercase-google',
        ),
        pytest.param(NUMPYDOC, 'google', True, id='numpy-in-google'),
        pytest.param(
            'Consume a value.\n\nArgs:\n    value: Input.',
            'numpy',
            True,
            id='args-only',
        ),
        pytest.param(
            NUMPYDOC + '\nRaises:\n    ValueError: Invalid.', 'numpy', True, id='mixed'
        ),
        pytest.param('Return a coordinate.', 'numpy', False, id='short'),
        pytest.param('Return a coordinate.', 'google', False, id='short-google'),
        pytest.param(
            'Example:\n\n```text\nArgs:\n    value: Input.\n```',
            'numpy',
            False,
            id='fenced-example',
        ),
    ],
)
def test_docstring_section_convention(prose, style, invalid):
    assert bool(doc_style_errors(prose, style)) is invalid


def sample(value: int, scale: float = 2.0) -> int:
    return int(value * scale)


def _subject(
    qualname: str,
    prose: str,
    parameters: tuple[inspect.Parameter, ...],
    *,
    kind: Literal[
        'callable',
        'property',
        'constructor',
        'class',
        'attribute',
        'schema',
    ] = 'callable',
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
        'gone' in error and 'not in any call form' in error
        for error in check_doc_subject(subject)
    )


def test_bound_method_receivers_are_removed_but_static_self_is_a_parameter() -> None:
    tree = ast.parse(
        """\
class Widget:
    def move(self, value: int) -> int: ...

    @staticmethod
    def build(self: str) -> str: ...

    @classmethod
    def from_value(cls, value: int) -> int: ...
"""
    )
    class_node = tree.body[0]
    assert isinstance(class_node, ast.ClassDef)
    methods = {
        node.name: node for node in class_node.body if isinstance(node, ast.FunctionDef)
    }
    docs = {
        'move': """\
Move the receiver.

Parameters
----------
value : int
    Translation amount.

Returns
-------
int
    The translated value.
""",
        'build': """\
Build from a string.

Parameters
----------
self : str
    The explicit string argument.

Returns
-------
str
    The built value.
""",
        'from_value': """\
Build from a value.

Parameters
----------
value : int
    The value.

Returns
-------
int
    The built value.
""",
    }

    for name, method in methods.items():
        subject = DocSubject(
            f'Widget.{name}',
            'callable',
            docs[name],
            None,
            DocLocation('widget.pyi', method.lineno),
            (method,),
        )
        assert check_doc_subject(subject) == [], name


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


def test_default_comparison_preserves_boolean_and_integer_types() -> None:
    def boolean(flag: bool = True) -> bool:
        return flag

    doc = """\
Return the flag.

Parameters
----------
flag : bool, default 1
    The flag.

Returns
-------
bool
    The flag value.
"""
    subject = _subject(
        'boolean',
        doc,
        tuple(inspect.signature(boolean).parameters.values()),
    )
    assert check_doc_subject(subject) == [
        'boolean.flag: documented default 1 != runtime True'
    ]


def test_unquoted_enum_default_matches_its_quoted_string_value() -> None:
    def choose(mode: Literal['auto', 'wkb'] = 'auto') -> str:
        return mode

    doc = """\
Choose an encoding.

Parameters
----------
mode : {'auto', 'wkb'}, default auto
    Encoding mode.

Returns
-------
str
    The chosen encoding.
"""
    subject = _subject(
        'choose',
        doc,
        tuple(inspect.signature(choose).parameters.values()),
    )
    assert check_doc_subject(subject) == []


def test_parameter_rows_reconcile_absence_required_defaults_and_variadics() -> None:
    prose = """\
Construct a value.

Parameters
----------
value : int
    Value in either form.
flag : bool, default False
    Whether to enable the option.
args : int
    Additional positional values.
optional : str
    Optional text value.

Returns
-------
object
    The result.
"""
    forms = (
        (
            SignatureParameter('value', 'positional-only', 'int', None),
            SignatureParameter('flag', 'keyword-only', 'bool', 'False'),
            SignatureParameter('args', 'variadic positional', 'int', None),
        ),
        (
            SignatureParameter('value', 'keyword-only', 'int', '1'),
            SignatureParameter('flag', 'keyword-only', 'bool', 'False'),
            SignatureParameter('optional', 'keyword-only', 'str', 'None'),
            SignatureParameter('args', 'variadic positional', 'int', None),
        ),
    )
    carriers = ((prose, DocLocation('widget.pyi', 10)),)

    resolution = resolve_parameter_rows('Widget.construct', carriers, forms)
    rows = {row.name: row for row in resolution.rows}

    assert resolution.errors == ()
    assert (rows['value'].status, rows['value'].default) == (
        'varies',
        'varies by call form',
    )
    required, defaulted = rows['value'].occurrences
    assert required is not None
    assert defaulted is not None
    assert [required.status, defaulted.status] == [
        'required',
        'defaulted',
    ]
    assert [required.kind, defaulted.kind] == [
        'positional-only',
        'keyword-only',
    ]
    assert [required.annotation, defaulted.annotation] == [
        'int',
        'int',
    ]
    assert [required.default, defaulted.default] == [
        None,
        '1',
    ]
    assert (rows['flag'].status, rows['flag'].default) == ('defaulted', 'False')
    assert (rows['args'].status, rows['args'].default) == (
        'variadic',
        'not applicable',
    )
    assert (rows['optional'].status, rows['optional'].default) == (
        'varies',
        'varies by call form',
    )
    assert rows['optional'].occurrences[0] is None
    optional = rows['optional'].occurrences[1]
    assert optional is not None
    assert optional.default == 'None'

    fixed_default_claim = prose.replace(
        'value : int\n',
        'value : int, default 1\n',
        1,
    )
    conflicting = resolve_parameter_rows(
        'Widget.construct',
        ((fixed_default_claim, DocLocation('widget.pyi', 10)),),
        forms,
    )
    assert any(
        'documented default 1' in error and 'applicability varies' in error
        for error in conflicting.errors
    )


def test_duplicate_prose_claims_coalesce_and_conflicts_keep_both_sources() -> None:
    forms = ((SignatureParameter('value', 'positional or keyword', 'int', '1'),),)
    equivalent = resolve_parameter_rows(
        'Widget.build',
        (
            (
                'Parameters\n----------\nvalue : int\n    The first claim.',
                DocLocation('first.py', 10),
            ),
            (
                'Other Parameters\n----------------\nvalue : int\n    The second claim.',
                DocLocation('second.rs', 20),
            ),
        ),
        forms,
    )
    assert equivalent.errors == ()
    assert len(equivalent.rows) == 1
    assert equivalent.rows[0].description == 'The first claim.\n\nThe second claim.'

    complementary = resolve_parameter_rows(
        'Widget.build',
        (
            (
                'Parameters\n----------\nvalue : int\n    The value.',
                DocLocation('first.py', 10),
            ),
            (
                'Other Parameters\n----------------\nvalue : int\n    The value. Extra constraints apply.',
                DocLocation('second.rs', 20),
            ),
        ),
        forms,
    )
    assert complementary.errors == ()
    assert complementary.rows[0].description == (
        'The value.\n\nThe value. Extra constraints apply.'
    )

    conflicting = resolve_parameter_rows(
        'Widget.build',
        (
            (
                'Parameters\n----------\nvalue : int, default 1\n    The value.',
                DocLocation('first.py', 10),
            ),
            (
                'Other Parameters\n----------------\nvalue : float, default 2\n    The value.',
                DocLocation('second.rs', 20),
            ),
        ),
        forms,
    )
    assert len(conflicting.errors) == 2
    assert 'first.py:' in conflicting.errors[0]
    assert 'second.rs:' in conflicting.errors[0]
    assert 'first.py:' in conflicting.errors[1]
    assert 'second.rs:' in conflicting.errors[1]


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


@pytest.mark.parametrize('kind', ['attribute', 'schema'])
def test_typed_field_subjects_do_not_require_callable_returns(kind):
    subject = _subject('Features.ids', 'One id per feature.', (), kind=kind)

    assert check_doc_subject(subject) == []


def test_source_facade_class_subjects_keep_member_provenance_and_one_init_carrier(
    tmp_path,
):
    import types

    from pyo3stubs.context import CheckContext
    from pyo3stubs.doc_structure import (
        DocClassSource,
        DocInitializationSource,
        DocLocation,
        DocMemberSource,
        iter_doc_subjects,
    )
    from tests._support import make_config

    source = ast.parse(
        '''\
class Base:
    @property
    def origin(self) -> str:
        """The inherited source name.

        Returns
        -------
        str
        """

class Public(Base):
    """A public source-defined class.

    Parameters
    ----------
    value : int
        The value to retain.

    Returns
    -------
    Public
    """

    def __init__(self, value: int) -> None:
        """A competing initializer carrier that is not selected."""

    @property
    def label(self) -> str:
        """The overridden label.

        Returns
        -------
        str
        """

    value: int
    """The retained value."""

    @overload
    def convert(self, value: int) -> int: ...
    @overload
    def convert(self, value: str) -> str: ...
    def convert(self, value):
        """Convert a value without changing its type."""
'''
    )
    base_node, public_node = source.body
    assert isinstance(base_node, ast.ClassDef)
    assert isinstance(public_node, ast.ClassDef)
    origin_node = base_node.body[0]
    init_node = next(
        node
        for node in public_node.body
        if isinstance(node, ast.FunctionDef) and node.name == '__init__'
    )
    label_node = next(
        node
        for node in public_node.body
        if isinstance(node, ast.FunctionDef) and node.name == 'label'
    )
    value_node = next(
        node for node in public_node.body if isinstance(node, ast.AnnAssign)
    )
    convert_nodes = [
        node
        for node in public_node.body
        if isinstance(node, ast.FunctionDef) and node.name == 'convert'
    ]
    assert isinstance(origin_node, ast.FunctionDef)
    assert isinstance(value_node.target, ast.Name)
    source_class = DocClassSource(
        node=public_node,
        source=DocLocation('src/models.py', public_node.body[0].lineno),
        members={
            'convert': DocMemberSource(
                node=convert_nodes[-1],
                kind='callable',
                prose=ast.get_docstring(convert_nodes[-1]),
                source=DocLocation('src/models.py', convert_nodes[-1].lineno),
            ),
            '__init__': DocMemberSource(
                node=init_node,
                kind='callable',
                prose=ast.get_docstring(init_node),
                source=DocLocation('src/models.py', init_node.body[0].lineno),
            ),
            'origin': DocMemberSource(
                node=origin_node,
                kind='property',
                prose=ast.get_docstring(origin_node),
                source=DocLocation('src/base.py', origin_node.body[0].lineno),
            ),
            'label': DocMemberSource(
                node=label_node,
                kind='property',
                prose=ast.get_docstring(label_node),
                source=DocLocation('src/models.py', label_node.body[0].lineno),
            ),
            'value': DocMemberSource(
                node=value_node,
                kind='attribute',
                prose='The retained value.',
                source=DocLocation('src/models.py', value_node.lineno + 1),
            ),
        },
        initialization=DocInitializationSource(
            node=public_node,
            prose=ast.get_docstring(public_node),
            source=DocLocation('src/models.py', public_node.body[0].lineno),
        ),
    )

    class Base:
        @property
        def origin(self) -> str:
            """The inherited source name.

            Returns
            -------
            str
            """
            return 'base'

    class Public(Base):
        def __init__(self, value: int) -> None:
            self.value = value

        @property
        def label(self) -> str:
            """The overridden label.

            Returns
            -------
            str
            """
            return str(self.value)

    runtime = types.ModuleType('fakepkg.facade')
    vars(runtime).update(__all__=['Public'], Public=Public)
    cfg = make_config(tmp_path)
    ctx = CheckContext(cfg, _runtime_module=runtime)
    subjects = tuple(
        iter_doc_subjects(
            ctx,
            runtime_module=runtime,
            source_classes={'Public': source_class},
        )
    )

    (constructor,) = tuple(
        subject for subject in subjects if subject.kind == 'constructor'
    )
    (field,) = tuple(
        subject for subject in subjects if subject.qualname == 'Public.value'
    )
    (label,) = tuple(
        subject for subject in subjects if subject.qualname == 'Public.label'
    )
    (inherited,) = tuple(
        subject for subject in subjects if subject.qualname == 'Public.origin'
    )
    (converted,) = tuple(
        subject for subject in subjects if subject.qualname == 'Public.convert'
    )
    assert [
        ast.unparse(node.returns) if node.returns is not None else None
        for node in converted.declarations
    ] == ['int', 'str', None]
    assert constructor.source == DocLocation(
        'src/models.py',
        public_node.body[0].lineno,
    )
    assert constructor.prose == ast.get_docstring(public_node)
    assert constructor.declarations == (public_node,)
    assert check_doc_subject(constructor) == []
    assert (field.kind, field.prose, field.source.path) == (
        'attribute',
        'The retained value.',
        'src/models.py',
    )
    assert field.declarations == (value_node,)
    assert check_doc_subject(field) == []
    assert label.source.path == 'src/models.py'
    assert inherited.source.path == 'src/base.py'
    assert len([s for s in subjects if s.qualname == 'Public.label']) == 1
    assert len([s for s in subjects if s.qualname == 'Public.origin']) == 1


@pytest.mark.parametrize('prefix', ['', 'async '])
@pytest.mark.parametrize('package', ['fakepkg', 'fakepkg.nested'])
def test_python_facade_owns_documentation_when_native_stub_shares_name(
    tmp_path, prefix, package
):
    import importlib.util
    import types
    from dataclasses import replace

    from pyo3stubs.context import CheckContext
    from pyo3stubs.doc_structure import iter_doc_subjects
    from tests._support import make_config

    cfg = make_config(
        tmp_path,
        stub_text='def from_arrow(data: object, *, crs: str | None = None) -> object: ...\n',
    )
    if package == 'fakepkg.nested':
        nested = cfg.stub_path.parent / 'nested'
        nested.mkdir()
        stub = nested / '_lib.pyi'
        stub.write_text(cfg.stub_path.read_text())
        cfg = replace(cfg, module=f'{package}._lib', stub_path=stub)
    facade_path = cfg.stub_path.parent / '_facade.py'
    facade_source = '''\
from typing import overload

@overload
def from_arrow(data: bytes, *, geometry: str | None = None) -> int: ...
@overload
def from_arrow(data: str, *, geometry: str | None = None) -> str: ...
def from_arrow(data, *, geometry=None):
    """Import one value.

    Parameters
    ----------
    data : bytes or str
        The value to import.
    geometry : str, optional
        The selected geometry.

    Returns
    -------
    int or str
        The imported value.
    """
    return data
'''
    facade_path.write_text(
        facade_source.replace('def from_arrow', f'{prefix}def from_arrow')
    )
    spec = importlib.util.spec_from_file_location(f'{package}._facade', facade_path)
    assert spec is not None and spec.loader is not None
    facade = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(facade)
    runtime = types.ModuleType(package)
    runtime.__all__ = ['from_arrow']
    runtime.from_arrow = facade.from_arrow
    ctx = CheckContext(cfg, _runtime_module=runtime)
    subject = next(
        subject
        for subject in iter_doc_subjects(
            ctx, runtime_module=runtime, constructor_sources={}
        )
        if subject.qualname == 'from_arrow'
    )
    assert subject.source.path == str(facade_path)
    assert len(subject.declarations) == 3
    assert all(
        'geometry' in {argument.arg for argument in declaration.args.kwonlyargs}
        for declaration in subject.declarations
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
