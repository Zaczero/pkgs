"""Focused scalar/array predicate-executor regressions.

These tests cover the side-aware point fast paths independently from the broad
topology matrix in ``test_predicates.py``.
"""

import gometry as gm
import numpy as np
import pytest
from conftest import bools


@pytest.mark.parametrize(
    'predicate',
    [
        'contains',
        'within',
        'covers',
        'covered_by',
        'intersects',
        'disjoint',
        'touches',
        'crosses',
    ],
)
def test_point_scalar_predicate_executor_preserves_both_orientations(
    predicate: str,
) -> None:
    operation = getattr(gm, predicate)
    point = gm.Point(0, 0)
    values = [
        gm.box(-1, -1, 1, 1),
        gm.LineString([(-1, 0), (1, 0)]),
        gm.Point(2, 2),
        None,
    ]
    array = gm.GeometryArray(values)

    expected_left = [
        operation(point, value) if value is not None else False for value in values
    ]
    expected_right = [
        operation(value, point) if value is not None else False for value in values
    ]
    np.testing.assert_array_equal(operation(point, array), expected_left)
    np.testing.assert_array_equal(operation(array, point), expected_right)

    packed_points = gm.points([0, 1, 3], [0, 1, 3])
    polygon = gm.box(-1, -1, 2, 2)
    packed_rows = [gm.Point(0, 0), gm.Point(1, 1), gm.Point(3, 3)]
    np.testing.assert_array_equal(
        operation(polygon, packed_points),
        [operation(polygon, value) for value in packed_rows],
    )
    np.testing.assert_array_equal(
        operation(packed_points, polygon),
        [operation(value, polygon) for value in packed_rows],
    )


def test_point_scalar_predicate_executor_handles_antimeridian_and_missing_rows() -> (
    None
):
    crossing = gm.Polygon(
        [(179, -1), (-179, -1), (-179, 1), (179, 1), (179, -1)],
        crs=4326,
    )
    rows = [gm.Point(180, 0, crs=4326), None, gm.Point(170, 0, crs=4326)]
    points = gm.GeometryArray(rows)
    expected = [True, False, False]
    np.testing.assert_array_equal(gm.covers(crossing, points), expected)
    np.testing.assert_array_equal(gm.covered_by(points, crossing), expected)


def test_equals_exact_rejects_non_finite_or_negative_tolerance() -> None:
    a, b = (gm.Point(0, 0), gm.Point(1, 1))
    for bad in (float('inf'), float('nan'), -1.0):
        with pytest.raises(ValueError, match='tolerance'):
            gm.equals_exact(a, b, bad)
        with pytest.raises(ValueError, match='tolerance'):
            gm.equals_exact(a, b, bad)
        with pytest.raises(ValueError, match='tolerance'):
            gm.equals_exact(gm.GeometryArray([a]), b, bad)


def test_collection_of_empty_members_is_empty() -> None:
    gc = gm.from_wkt('GEOMETRYCOLLECTION (POINT EMPTY)')
    assert gc.is_empty
    assert gm.equals(gc, gm.from_wkt('POINT EMPTY'))


def test_empty_geometry_predicate_matrix() -> None:
    """Every broadcast path follows the same GEOS-style EMPTY policy."""
    empty = gm.from_wkt('POLYGON EMPTY')
    box = gm.box(0, 0, 1, 1)
    falsy = [
        'contains',
        'contains_properly',
        'covers',
        'covered_by',
        'within',
        'intersects',
        'touches',
        'crosses',
        'overlaps',
    ]
    for name in falsy:
        assert getattr(gm, name)(empty, box) is False, name
        assert getattr(gm, name)(box, empty) is False, name
        assert getattr(gm, name)(empty, empty) is False, name
    for left, right in [(empty, box), (box, empty), (empty, empty)]:
        assert gm.disjoint(left, right) is True
    assert gm.equals(empty, box) is False
    assert gm.equals(box, empty) is False
    assert gm.equals(empty, empty) is True


def test_every_predicate_spelling_agrees_across_dispatch_strategies() -> None:
    """Free fn and prepared paths agree below and above the batch threshold."""
    scalar = gm.Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
    base = [
        gm.Point(5, 5),
        gm.Point(0, 0),
        gm.Point(20, 20),
        gm.box(2, 2, 4, 4),
        gm.box(0, 0, 4, 4),
        gm.box(5, 5, 15, 15),
        gm.box(10, 0, 20, 10),
        gm.box(30, 30, 40, 40),
        gm.LineString([(2, 2), (8, 8)]),
        gm.LineString([(0, 0), (10, 10)]),
        gm.LineString([(-5, 5), (15, 5)]),
        gm.Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]),
        gm.from_wkt('POINT EMPTY'),
        gm.from_wkt('POLYGON EMPTY'),
    ]
    predicates = [
        'intersects',
        'disjoint',
        'contains',
        'contains_properly',
        'within',
        'covers',
        'covered_by',
        'touches',
        'crosses',
        'overlaps',
        'equals',
    ]
    prepared = scalar.prepare()
    for batch in (base, base * 2):
        values = gm.GeometryArray(batch)
        for name in predicates:
            free = getattr(gm, name)
            expected = [free(scalar, item) for item in batch]
            assert bools(free(scalar, values)) == expected, name
            assert bools(getattr(prepared, name)(values)) == expected, name
            reversed_expected = [free(item, scalar) for item in batch]
            assert bools(free(values, scalar)) == reversed_expected, name
