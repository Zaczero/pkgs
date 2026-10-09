"""Gometry's exception policy over the shared documentation subjects."""

from __future__ import annotations

import inspect
from typing import Literal

import pytest
from pyo3stubs.doc_structure import DocLocation

from tests._support import GOMETRY_ROOT, load_tool

coverage = load_tool('doc_coverage', GOMETRY_ROOT / 'tools/stubs/_doc_coverage.py')


def _subject(
    qualname: str,
    prose: str,
    parameters: tuple[inspect.Parameter, ...] = (),
    *,
    kind: Literal['callable', 'property', 'constructor', 'class'] = 'callable',
):
    return coverage.DocSubject(
        qualname,
        kind,
        prose,
        parameters,
        DocLocation('src/test.rs', 1),
    )


def test_empty_raises_description_is_reported_from_parsed_entries() -> None:
    subject = _subject(
        'coverage_clean',
        """\
        Clean coverage.

        Raises
        ------
        GeometryError
        InvalidGeometryError
            If the coverage contents are invalid.
        GeometryTypeError
            If an input has the wrong geometry type.
        CRSMismatchError
            If the input coordinate systems differ.
        """,
    )

    assert coverage.check_subject_raises(subject) == [
        "coverage_clean: Raises 'GeometryError' requires a non-empty description"
    ]


def test_from_arrow_provider_errors_remain_allowed_and_required() -> None:
    parameters = tuple(
        inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY)
        for name in ('data', 'crs', 'epoch')
    )
    expected = {
        'CRSError',
        'GeometryError',
        'ParseError',
        'MemoryError',
        'OSError',
    }
    assert expected <= coverage.required_raises('from_arrow', {'data', 'crs', 'epoch'})
    subject = _subject(
        'from_arrow',
        """\
        Import Arrow geometry.

        Raises
        ------
        CRSError
            If the CRS cannot be resolved.
        CRSMismatchError
            If the supplied frame conflicts.
        GeometryError
            If the declared dimensions are invalid.
        ParseError
            If an extension or coordinate is malformed.
        TypeError
            If the input does not expose the Arrow protocol.
        OSError
            If the foreign stream reports an I/O failure.
        MemoryError
            If the foreign provider reports an allocation failure.
        OverflowError
            If a declared length exceeds the representable range.
        """,
        parameters,
    )

    assert coverage.check_subject_raises(subject) == []


def test_native_geometry_failures_do_not_invent_memoryerror() -> None:
    assert 'MemoryError' not in coverage.required_raises(
        'intersection', {'left', 'right'}
    )


def test_required_exception_missing_from_raises_is_reported() -> None:
    subject = _subject(
        'intersects',
        """\
        Test whether geometries intersect.

        Raises
        ------
        GeometryError
            If a predicate input is invalid.
        """,
        (
            inspect.Parameter('left', inspect.Parameter.POSITIONAL_OR_KEYWORD),
            inspect.Parameter('right', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ),
    )

    assert coverage.check_subject_raises(subject) == [
        'intersects: Raises must document CRSMismatchError (fallible-family contract)'
    ]


def test_constructor_uses_the_existing_crs_error_policy() -> None:
    subject = _subject(
        'CRS.__new__',
        """\
        Build a CRS.

        Raises
        ------
        CRSError
            If the input is not recognized.
        """,
        kind='constructor',
    )

    assert coverage.check_subject_raises(subject) == []


def test_unknown_exception_and_bare_valueerror_are_rejected() -> None:
    subject = _subject(
        'custom_operation',
        """\
        Perform a custom operation.

        Raises
        ------
        ValueError
            If the input is not supported.
        MysteryError
            If the backend fails.
        """,
    )

    assert coverage.check_subject_raises(subject) == [
        'custom_operation: Raises names bare ValueError — name the specific '
        'gometry exception class (GeometryError if generic)',
        "custom_operation: Raises names unknown exception 'MysteryError'",
    ]


@pytest.mark.parametrize(
    ('qualname', 'exception'),
    [
        pytest.param('Point.x', 'AttributeError', id='empty-point-property'),
        pytest.param(
            'GeometryCollection.index', 'ValueError', id='collection-sequence-index'
        ),
        pytest.param(
            'MultiLineString.index', 'ValueError', id='multiline-sequence-index'
        ),
        pytest.param('MultiPoint.index', 'ValueError', id='multipoint-sequence-index'),
        pytest.param(
            'MultiPolygon.index', 'ValueError', id='multipolygon-sequence-index'
        ),
    ],
)
def test_existing_python_protocol_exceptions_remain_allowed(
    qualname: str,
    exception: str,
) -> None:
    subject = _subject(
        qualname,
        f"""\
        Inspect {qualname}.

        Raises
        ------
        {exception}
            If the requested value cannot be returned.
        """,
    )

    assert coverage.check_subject_raises(subject) == []


def test_free_aggregate_keeps_its_frame_error_without_assigning_it_to_array() -> None:
    free = coverage.required_raises('union_all', {'geometries'})
    array_method = coverage.required_raises('GeometryArray.union_all', {'geometries'})

    assert 'CRSMismatchError' in free
    assert 'CRSMismatchError' not in array_method


def test_mirrored_raises_allow_only_the_free_function_frame_error() -> None:
    free = _subject(
        'union_all',
        """\
        Union all geometries.

        Raises
        ------
        GeometryError
            If an input is invalid.
        CRSMismatchError
            If input frames differ.
        """,
    )
    array = _subject(
        'GeometryArray.union_all',
        """\
        Union all rows.

        Raises
        ------
        GeometryError
            If an input is invalid.
        """,
    )

    assert coverage.check_mirrored_raises((free, array)) == []

    extra = _subject(
        'GeometryArray.union_all',
        """\
        Union all rows.

        Raises
        ------
        GeometryError
            If an input is invalid.
        ParseError
            If parsing fails.
        """,
    )
    errors = coverage.check_mirrored_raises((free, extra))

    assert len(errors) == 1
    assert 'union_all / GeometryArray.union_all' in errors[0]
    assert "expected ['GeometryError']" in errors[0]
