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
    assert gm.parts(geojson_back)[0].geometry_type == 'GeometryCollection'


def test_geometry_collection_with_empty_member_predicates_and_metrics() -> None:
    collection = gm.GeometryCollection([gm.from_wkt('POINT EMPTY'), gm.box(0, 0, 1, 1)])
    assert gm.contains(collection, gm.Point(0.5, 0.5))
    assert collection.area == pytest.approx(1.0)
    assert gm.intersects(collection, gm.Point(0, 0))
    assert not gm.intersects(collection, gm.Point(5, 5))


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
