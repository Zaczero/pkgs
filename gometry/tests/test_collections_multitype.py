"""Predicates, metrics, and IO round-trips on nested / empty-member / mixed
collections and multi-hole polygons (complex-structure coverage).
"""

import gometry as gm
import numpy as np
import pytest


def test_nested_geometry_collection_predicates_metrics_and_roundtrip() -> None:
    inner = gm.GeometryCollection([gm.Point(0, 0), gm.box(0, 0, 1, 1)])
    outer = gm.GeometryCollection([inner, gm.Point(2, 2)])
    assert gm.contains(outer, gm.Point(0.5, 0.5))
    assert gm.intersects(outer, gm.box(-1, -1, 3, 3))
    assert outer.area == pytest.approx(1.0)
    assert outer.length == pytest.approx(4.0)
    wkt_back = gm.from_wkt(outer.to_wkt())
    assert gm.contains(wkt_back, gm.Point(0.5, 0.5))
    geojson_back = gm.from_geojson(outer.to_geojson())
    assert geojson_back.geometry_type == 'GeometryCollection'
    assert geojson_back.parts[0].geometry_type == 'GeometryCollection'


def test_geometry_collection_with_empty_member_predicates_and_metrics() -> None:
    collection = gm.GeometryCollection([gm.from_wkt('POINT EMPTY'), gm.box(0, 0, 1, 1)])
    assert gm.contains(collection, gm.Point(0.5, 0.5))
    assert collection.area == pytest.approx(1.0)
    assert gm.intersects(collection, gm.Point(0, 0))
    assert not gm.intersects(collection, gm.Point(5, 5))


@pytest.mark.parametrize(
    ('wkt', 'expected'),
    [
        pytest.param(
            'GEOMETRYCOLLECTION ('
            'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), '
            '(1 1, 3 1, 3 3, 1 3, 1 1)), '
            'LINESTRING (-1 1, 5 3), LINESTRING (1 0, 3 0), POINT (0 0))',
            12.0,
            id='hole-with-rational-crossing-and-boundary-subdivision',
        ),
        pytest.param(
            'GEOMETRYCOLLECTION ('
            'POLYGON ((0 0, 4 0, 2 0, 0 0)), '
            'POLYGON ((-2 -2, 2 -2, 2 2, -2 2, -2 -2)))',
            16.0,
            id='collapsed-shell-crosses-an-area',
        ),
        pytest.param(
            'GEOMETRYCOLLECTION ('
            'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), '
            '(2 0, 8 0, 5 0, 2 0)))',
            100.0,
            id='collapsed-hole-on-shell-boundary',
        ),
        pytest.param(
            'GEOMETRYCOLLECTION ('
            'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)), '
            'POLYGON ((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0)))',
            1.5,
            id='distinct-overlapping-polygon-owners',
        ),
        pytest.param(
            'GEOMETRYCOLLECTION ('
            'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), '
            '((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))))',
            1.5,
            id='overlapping-multipolygon-member',
        ),
        pytest.param(
            'GEOMETRYCOLLECTION ('
            'GEOMETRYCOLLECTION (POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))), '
            'GEOMETRYCOLLECTION ('
            'POLYGON ((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))))',
            1.5,
            id='distinct-overlapping-collection-owners',
        ),
        pytest.param(
            'GEOMETRYCOLLECTION ('
            'POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0)))',
            2.0,
            id='invalid-bow-tie-polygon',
        ),
    ],
)
def test_geometry_collection_area_uses_occupied_polygon_faces(
    wkt: str, expected: float
) -> None:
    collection = gm.from_wkt(wkt)
    assert collection.geometry_type == 'GeometryCollection'
    assert collection.area == expected
    np.testing.assert_array_equal(gm.GeometryArray([collection]).area, [expected])


def test_geometry_collection_area_without_areal_members_is_zero() -> None:
    collection = gm.GeometryCollection([
        gm.Point(8, 1),
        gm.LineString([(0, 0), (2, 0)]),
        gm.GeometryCollection([gm.Point(3, 4)]),
    ])
    assert collection.area == 0.0
    np.testing.assert_array_equal(gm.GeometryArray([collection]).area, [0.0])


def test_repeated_shared_collection_dag_keeps_occupied_area() -> None:
    shape = gm.box(0, 0, 1, 1)
    for _ in range(16):
        shape = gm.GeometryCollection([shape, shape])
    assert shape.area == 1.0


def test_mixed_geometry_array_predicates() -> None:
    rows = gm.GeometryArray([gm.Point(0, 0), gm.box(0, 0, 2, 2)])
    target = gm.Point(1, 1)
    np.testing.assert_array_equal(gm.contains(rows, target), [False, True])
    np.testing.assert_array_equal(gm.intersects(rows, target), [False, True])


def test_multipolygon_multiple_holes_area_contains_and_overlay() -> None:
    holed = gm.MultiPolygon([
        [
            [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
            [(2, 2), (4, 2), (4, 4), (2, 4), (2, 2)],
            [(6, 6), (8, 6), (8, 8), (6, 8), (6, 6)],
        ],
        [[(20, 0), (30, 0), (30, 10), (20, 10), (20, 0)]],
    ])
    assert holed.area == pytest.approx(192.0)
    assert gm.contains(holed, gm.Point(1, 1))
    assert not gm.contains(holed, gm.Point(3, 3))
    hole_box = gm.box(3, 3, 4, 4)
    assert gm.difference(holed, hole_box).area == pytest.approx(192.0)
