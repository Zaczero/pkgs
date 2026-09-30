"""Focused scalar/array predicate-executor regressions.

These tests cover the side-aware point fast paths independently from the broad
topology matrix in ``test_predicates.py``.
"""

import gometry as gm
import numpy as np
import pytest

from tests._support import bools


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


def test_relate_scalar_array_uses_logical_rows_for_compact_nulls() -> None:
    scalar = gm.Point(0.0, 0.0)
    values = gm.GeometryArray([
        gm.Point(float(row), 0.0) if row % 2 == 0 else None for row in range(65)
    ])
    expected_left = [
        None if row % 2 else gm.relate(scalar, gm.Point(float(row), 0.0))
        for row in range(65)
    ]
    expected_right = [
        None if row % 2 else gm.relate(gm.Point(float(row), 0.0), scalar)
        for row in range(65)
    ]

    present = gm.GeometryArray([gm.Point(float(row), 0.0) for row in range(0, 65, 2)])
    present_before = present.__sizeof__()
    gm.relate(scalar, present)
    present_growth = present.__sizeof__() - present_before
    values_before = values.__sizeof__()
    gm.relate(scalar, values)
    assert values.__sizeof__() - values_before > present_growth

    assert values.is_missing.tolist() == [row % 2 == 1 for row in range(65)]
    assert gm.relate(scalar, values) == expected_left
    assert gm.relate(values, scalar) == expected_right
    assert gm.relate(values, values) == [
        None if row % 2 else '0FFFFFFF2' for row in range(65)
    ]


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
    """Every broadcast path follows the same empty-geometry policy."""
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
    for batch in (base, base * 2):
        values = gm.GeometryArray(batch)
        for name in predicates:
            free = getattr(gm, name)
            expected = [free(scalar, item) for item in batch]
            assert bools(free(scalar, values)) == expected, name
            assert bools(free(scalar, values)) == expected, name
            reversed_expected = [free(item, scalar) for item in batch]
            assert bools(free(values, scalar)) == reversed_expected, name
            assert bools(free(values, scalar)) == reversed_expected, name


def test_fully_missing_pairwise_skips_materialization():
    """All-missing partner must leave the receiver's retained size unchanged.

    The observable consequence of skipping missing-row materialization and
    cache warming: ``__sizeof__`` of a mixed receiver is bit-stable across an
    all-missing pairwise predicate. Boolean all-False alone would stay green
    under a path that still warms every left-hand ShapeData cache — a present
    partner on the same left grows retained size substantially, so the flat
    all-missing result is the property under test.
    """
    # Mixed storage (heterogeneous kinds) so a present partner materializes
    # per-row ShapeData handles; packed homogeneous columns would not.
    rows = []
    for i in range(200):
        if i % 2 == 0:
            rows.append(gm.box(float(i), 0.0, float(i) + 1.0, 1.0))
        else:
            rows.append(
                gm.LineString([(float(i), 0.0), (float(i), 1.0), (float(i) + 1.0, 1.0)])
            )
    left = gm.GeometryArray(rows)
    miss = gm.GeometryArray([None] * 200)
    before = left.__sizeof__()
    out = gm.intersects(left, miss)
    after = left.__sizeof__()
    assert len(out) == 200
    assert not out.any()
    assert after == before, (
        f'all-missing partner grew receiver retained size {before} -> {after} '
        '(missing rows must not materialize / warm caches)'
    )
    # Control: a present partner on the same left MUST grow retained size
    # (otherwise the flat all-missing result is vacuous).
    present = gm.GeometryArray(rows)
    _ = gm.intersects(left, present)
    assert left.__sizeof__() > before, 'present partner should materialize caches'
    # Fresh left: second all-missing call still stable.
    left2 = gm.GeometryArray(rows)
    b2 = left2.__sizeof__()
    _ = gm.intersects(left2, miss)
    assert left2.__sizeof__() == b2


def test_partial_missing_pairwise_elementwise():
    left = gm.GeometryArray([gm.Point(0, 0), None, gm.Point(1, 1), gm.Point(2, 2)])
    right = gm.GeometryArray([gm.Point(0, 0), gm.Point(9, 9), None, gm.Point(2, 2)])
    out = list(gm.intersects(left, right))
    assert out == [True, False, False, True]
