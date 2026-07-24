"""Scalar-or-array parameter validation matrix.

Covers the magnitude family that ``test_scalar_or_array_params`` exercises only
on the happy path: wrong-length vectors, NaN, ±inf, and negative-where-invalid
for ``offset_curve``, ``line_interpolate(s)``, ``line_substring`` (both
bounds), ``interpolate_m``, ``concave_hull``, ``polylabel``, and
``maximum_inscribed_circle``.

Scalar surfaces raise. Array surfaces raise on parameter errors (not the
geometry-data degrade lane) — including ``line_interpolate``, whose
packed lane validates finiteness at the parameter boundary.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import gometry as gm
import numpy as np
import pytest

NAN = float('nan')
POS_INF = float('inf')
NEG_INF = float('-inf')


def _three_lines() -> gm.GeometryArray:
    return gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (10.0, 0.0)]),
        gm.LineString([(0.0, 10.0), (10.0, 10.0)]),
        gm.LineString([(0.0, 20.0), (10.0, 20.0)]),
    ])


def _three_boxes() -> gm.GeometryArray:
    return gm.GeometryArray([
        gm.box(0, 0, 1, 1),
        gm.box(0, 0, 2, 2),
        gm.box(0, 0, 3, 3),
    ])


def _three_multipoints() -> gm.GeometryArray:
    return gm.GeometryArray([
        gm.MultiPoint([(0, 0), (1, 0), (0, 1), (1, 1)]),
        gm.MultiPoint([(0, 0), (2, 0), (0, 2), (2, 2)]),
        gm.MultiPoint([(0, 0), (3, 0), (0, 3), (3, 3)]),
    ])


def _three_plain_lines() -> gm.GeometryArray:
    return gm.GeometryArray([
        gm.from_wkt('LINESTRING (0 0, 10 0)'),
        gm.from_wkt('LINESTRING (0 10, 10 10)'),
        gm.from_wkt('LINESTRING (0 20, 10 20)'),
    ])


# (id, array_callable, match)
WrongLength = tuple[str, Callable[[], Any], str]

WRONG_LENGTH_CASES: list[WrongLength] = [
    (
        'offset_curve',
        lambda: _three_lines().offset_curve([1.0, 2.0]),
        'distance must be a float or a length-3',
    ),
    (
        'line_interpolate',
        lambda: _three_lines().line_interpolate([1.0, 2.0]),
        'at must be a float or a length-3',
    ),
    (
        'line_substring_start',
        lambda: _three_lines().line_substring([0.0, 1.0], [5.0, 5.0, 5.0]),
        'start must be a float or a length-3',
    ),
    (
        'line_substring_end',
        lambda: _three_lines().line_substring([0.0, 0.0, 0.0], [5.0, 5.0]),
        'end must be a float or a length-3',
    ),
    (
        'interpolate_m_start',
        lambda: _three_plain_lines().interpolate_m([0.0, 1.0], [100.0, 100.0, 100.0]),
        'start_m must be a float or a length-3',
    ),
    (
        'interpolate_m_end',
        lambda: _three_plain_lines().interpolate_m([0.0, 0.0, 0.0], [100.0, 100.0]),
        'end_m must be a float or a length-3',
    ),
    (
        'concave_hull_concavity',
        lambda: _three_multipoints().concave_hull(concavity=[1.0, 2.0]),
        'concavity must be a float or a length-3',
    ),
    (
        'concave_hull_length_threshold',
        lambda: _three_multipoints().concave_hull(length_threshold=[0.0, 1.0]),
        'length_threshold must be a float or a length-3',
    ),
    (
        'polylabel',
        lambda: _three_boxes().polylabel(tolerance=[0.1, 0.2]),
        'tolerance must be a float or a length-3',
    ),
    (
        'maximum_inscribed_circle',
        lambda: _three_boxes().maximum_inscribed_circle(tolerance=[0.1, 0.2]),
        'tolerance must be a float or a length-3',
    ),
]


@pytest.mark.parametrize(
    ('case_id', 'call', 'match'),
    WRONG_LENGTH_CASES,
    ids=[c[0] for c in WRONG_LENGTH_CASES],
)
def test_array_wrong_length_raises(
    case_id: str, call: Callable[[], Any], match: str
) -> None:
    with pytest.raises(gm.GeometryError, match=match):
        call()


# (id, scalar_call, array_call, match, err_type)
NonFinite = tuple[str, Callable[[], Any], Callable[[], Any], str, type[BaseException]]

NON_FINITE_CASES: list[NonFinite] = [
    (
        'offset_curve_nan',
        lambda: _three_lines()[0].offset_curve(NAN),
        lambda: _three_lines().offset_curve([1.0, NAN, 2.0]),
        'distance must be finite',
        gm.GeometryError,
    ),
    (
        'offset_curve_inf',
        lambda: _three_lines()[0].offset_curve(POS_INF),
        lambda: _three_lines().offset_curve([1.0, POS_INF, 2.0]),
        'distance must be finite',
        gm.GeometryError,
    ),
    (
        'offset_curve_ninf',
        lambda: _three_lines()[0].offset_curve(NEG_INF),
        lambda: _three_lines().offset_curve([1.0, NEG_INF, 2.0]),
        'distance must be finite',
        gm.GeometryError,
    ),
    (
        'line_interpolate_nan_scalar',
        lambda: _three_lines()[0].line_interpolate(NAN),
        # array path is special-cased below
        lambda: (_ for _ in ()).throw(AssertionError('use dedicated test')),
        'distance must be finite',
        gm.GeometryError,
    ),
    (
        'line_substring_start_nan',
        lambda: _three_lines()[0].line_substring(NAN, 5.0),
        lambda: _three_lines().line_substring([NAN, 0.0, 0.0], [5.0, 5.0, 5.0]),
        'start_distance must be finite',
        gm.GeometryError,
    ),
    (
        'line_substring_end_nan',
        lambda: _three_lines()[0].line_substring(0.0, NAN),
        lambda: _three_lines().line_substring([0.0, 0.0, 0.0], [5.0, NAN, 5.0]),
        'end_distance must be finite',
        gm.GeometryError,
    ),
    (
        'line_substring_end_inf',
        lambda: _three_lines()[0].line_substring(0.0, POS_INF),
        lambda: _three_lines().line_substring([0.0, 0.0, 0.0], [5.0, POS_INF, 5.0]),
        'end_distance must be finite',
        gm.GeometryError,
    ),
    (
        'interpolate_m_start_nan',
        lambda: _three_plain_lines()[0].interpolate_m(NAN, 100.0),
        lambda: _three_plain_lines().interpolate_m(
            [NAN, 0.0, 0.0], [100.0, 100.0, 100.0]
        ),
        'start_m must be finite',
        gm.GeometryError,
    ),
    (
        'interpolate_m_end_nan',
        lambda: _three_plain_lines()[0].interpolate_m(0.0, NAN),
        lambda: _three_plain_lines().interpolate_m(
            [0.0, 0.0, 0.0], [100.0, NAN, 100.0]
        ),
        'end_m must be finite',
        gm.GeometryError,
    ),
    (
        'interpolate_m_end_inf',
        lambda: _three_plain_lines()[0].interpolate_m(0.0, POS_INF),
        lambda: _three_plain_lines().interpolate_m(
            [0.0, 0.0, 0.0], [100.0, POS_INF, 100.0]
        ),
        'end_m must be finite',
        gm.GeometryError,
    ),
    (
        'concave_hull_concavity_nan',
        lambda: _three_multipoints()[0].concave_hull(concavity=NAN),
        lambda: _three_multipoints().concave_hull(concavity=[1.0, NAN, 2.0]),
        'concavity must be a non-negative finite number, got NaN',
        gm.GeometryError,
    ),
    (
        'concave_hull_concavity_inf',
        lambda: _three_multipoints()[0].concave_hull(concavity=POS_INF),
        lambda: _three_multipoints().concave_hull(concavity=[1.0, POS_INF, 2.0]),
        'concavity must be a non-negative finite number, got inf',
        gm.GeometryError,
    ),
    (
        'concave_hull_length_threshold_nan',
        lambda: _three_multipoints()[0].concave_hull(length_threshold=NAN),
        lambda: _three_multipoints().concave_hull(length_threshold=[0.0, NAN, 0.0]),
        'length_threshold must be a non-negative finite number, got NaN',
        gm.GeometryError,
    ),
    (
        'polylabel_nan',
        lambda: _three_boxes()[0].polylabel(tolerance=NAN),
        lambda: _three_boxes().polylabel(tolerance=[0.1, NAN, 0.2]),
        'tolerance must be a positive finite number, got NaN',
        gm.GeometryError,
    ),
    (
        'polylabel_inf',
        lambda: _three_boxes()[0].polylabel(tolerance=POS_INF),
        lambda: _three_boxes().polylabel(tolerance=[0.1, POS_INF, 0.2]),
        'tolerance must be a positive finite number, got inf',
        gm.GeometryError,
    ),
    (
        'maximum_inscribed_circle_nan',
        lambda: _three_boxes()[0].maximum_inscribed_circle(tolerance=NAN),
        lambda: _three_boxes().maximum_inscribed_circle(tolerance=[0.1, NAN, 0.2]),
        'tolerance must be a positive finite number, got NaN',
        gm.GeometryError,
    ),
    (
        'maximum_inscribed_circle_inf',
        lambda: _three_boxes()[0].maximum_inscribed_circle(tolerance=POS_INF),
        lambda: _three_boxes().maximum_inscribed_circle(tolerance=[0.1, POS_INF, 0.2]),
        'tolerance must be a positive finite number, got inf',
        gm.GeometryError,
    ),
    (
        'line_interpolate_at_nan',
        lambda: _three_lines()[0].line_interpolate([1.0, NAN]),
        lambda: _three_lines().line_interpolate([1.0, NAN, 2.0]),
        'finite',
        gm.GeometryError,
    ),
    (
        'segmentize_nan',
        lambda: _three_lines()[0].segmentize(NAN),
        lambda: _three_lines().segmentize([1.0, NAN, 2.0]),
        'max_length must be finite',
        gm.GeometryError,
    ),
    (
        'hausdorff_densify_nan',
        lambda: gm.hausdorff_distance(_three_lines()[0], _three_lines()[1], densify=NAN),
        lambda: gm.hausdorff_distance(_three_lines(), _three_lines(), densify=[0.0, NAN, 0.0]),
        'densify must be finite',
        gm.GeometryError,
    ),
    (
        'frechet_densify_nan',
        lambda: gm.frechet_distance(_three_lines()[0], _three_lines()[1], densify=NAN),
        lambda: gm.frechet_distance(_three_lines(), _three_lines(), densify=[0.0, NAN, 0.0]),
        'densify must be finite',
        gm.GeometryError,
    ),
]


@pytest.mark.parametrize(
    ('case_id', 'scalar_call', 'array_call', 'match', 'err_type'),
    NON_FINITE_CASES,
    ids=[c[0] for c in NON_FINITE_CASES],
)
def test_non_finite_raises_scalar_and_array(
    case_id: str,
    scalar_call: Callable[[], Any],
    array_call: Callable[[], Any],
    match: str,
    err_type: type[BaseException] | tuple[type[BaseException], ...],
) -> None:
    if case_id == 'line_interpolate_nan_scalar':
        with pytest.raises(err_type, match=match):
            scalar_call()
        return
    with pytest.raises(err_type, match=match):
        scalar_call()
    with pytest.raises(err_type, match=match):
        array_call()


def test_line_interpolate_non_finite_raises_on_both_surfaces() -> None:
    """A non-finite distance is a parameter error, so it raises on the scalar
    AND the array surface (the packed row loop clamps only in-range
    stationing; parameter validation happens at the lane boundary).
    """
    lines = _three_lines()
    for bad in (NAN, POS_INF, NEG_INF):
        with pytest.raises(gm.GeometryError, match='distance must be finite'):
            lines[0].line_interpolate(bad)
        with pytest.raises(gm.GeometryError, match='distance must be finite'):
            lines.line_interpolate([1.0, bad, 2.0])


# (id, scalar_call, array_call, match)
NegativeInvalid = tuple[str, Callable[[], Any], Callable[[], Any], str]

NEGATIVE_CASES: list[NegativeInvalid] = [
    (
        'polylabel',
        lambda: _three_boxes()[0].polylabel(tolerance=-0.1),
        lambda: _three_boxes().polylabel(tolerance=[-0.1, 0.1, 0.1]),
        'tolerance must be a positive finite number',
    ),
    (
        'maximum_inscribed_circle',
        lambda: _three_boxes()[0].maximum_inscribed_circle(tolerance=-0.1),
        lambda: _three_boxes().maximum_inscribed_circle(tolerance=[-0.1, 0.1, 0.1]),
        'tolerance must be a positive finite number',
    ),
    (
        'concave_hull_concavity',
        lambda: _three_multipoints()[0].concave_hull(concavity=-1.0),
        lambda: _three_multipoints().concave_hull(concavity=[-1.0, 1.0, 1.0]),
        'concavity must be a non-negative finite number',
    ),
    (
        'concave_hull_length_threshold',
        lambda: _three_multipoints()[0].concave_hull(length_threshold=-1.0),
        lambda: _three_multipoints().concave_hull(length_threshold=[-1.0, 0.0, 0.0]),
        'length_threshold must be a non-negative finite number',
    ),
    (
        'line_interpolate_count_zero',
        lambda: _three_lines()[0].line_interpolate(count=0),
        lambda: _three_lines().line_interpolate(count=0),
        'count must be >= 1',
    ),
    (
        'line_interpolate_count_neg',
        lambda: _three_lines()[0].line_interpolate(count=-1),
        lambda: _three_lines().line_interpolate(count=-1),
        'count must be >= 1',
    ),
]


@pytest.mark.parametrize(
    ('case_id', 'scalar_call', 'array_call', 'match'),
    NEGATIVE_CASES,
    ids=[c[0] for c in NEGATIVE_CASES],
)
def test_negative_where_invalid_raises(
    case_id: str,
    scalar_call: Callable[[], Any],
    array_call: Callable[[], Any],
    match: str,
) -> None:
    with pytest.raises(gm.GeometryError, match=match):
        scalar_call()
    with pytest.raises(gm.GeometryError, match=match):
        array_call()


def test_offset_curve_accepts_signed_negative_distance() -> None:
    """Negative distance is valid for ``offset_curve`` (side selection)."""
    lines = _three_lines()
    scalar = lines[0].offset_curve(-1.0)
    array = lines.offset_curve([-1.0, 1.0, 2.0])
    assert scalar.geometry_type in ('LineString', 'MultiLineString')
    assert len(array) == 3
    assert array[0].to_wkt() == scalar.to_wkt()


def test_boundary_valid_values_still_work() -> None:
    """Smoke: the valid edge of each domain still produces results."""
    lines = _three_lines()
    boxes = _three_boxes()
    mps = _three_multipoints()
    plains = _three_plain_lines()

    assert len(lines.offset_curve(0.0)) == 3
    assert len(lines.line_interpolate(0.0)) == 3
    assert len(lines.line_substring([0.0, 0.0, 0.0], [0.0, 1.0, 2.0])) == 3
    assert len(plains.interpolate_m(0.0, 0.0)) == 3
    assert len(mps.concave_hull(concavity=0.0, length_threshold=0.0)) == 3
    # polylabel/mic require positive tolerance
    assert len(boxes.polylabel(tolerance=1e-3)) == 3
    assert len(boxes.maximum_inscribed_circle(tolerance=1e-3)) == 3
    assert len(lines.line_interpolate(count=1)) == 3
    # ndarray path for a representative vector param
    out = lines.offset_curve(np.array([1.0, 2.0, 3.0]))
    assert len(out) == 3
