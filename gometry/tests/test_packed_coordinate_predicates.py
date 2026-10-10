"""Packed polygon coordinate predicates — storage-twin parity."""

from __future__ import annotations

import gometry as gm

from tests._support import bools, polygon_storage_twins


def test_packed_polygons_point_predicate_matches_mixed_storage() -> None:
    packed, mixed = polygon_storage_twins()
    point = gm.Point(0.5, 0.5, crs=3857)
    assert bools(gm.within(point, packed)) == bools(gm.within(point, mixed))
    assert bools(gm.covers(point, packed)) == bools(gm.covers(point, mixed))
    assert bools(gm.intersects(point, packed)) == bools(gm.intersects(point, mixed))
    assert bools(gm.contains(packed, point)) == bools(gm.contains(mixed, point))
    assert bools(gm.covers(packed, point)) == bools(gm.covers(mixed, point))


def test_packed_polygons_coordinate_predicates_match_mixed_storage() -> None:
    packed, mixed = polygon_storage_twins()
    xs = [0.5, 1.5, 3.0]
    ys = [0.5, 1.5, 1.0]
    for poly_packed, poly_mixed in zip(packed, mixed, strict=True):
        assert bools(gm.contains(poly_packed, x=xs, y=ys)) == bools(
            gm.contains(poly_mixed, x=xs, y=ys)
        )
        assert bools(gm.intersects(poly_packed, x=xs, y=ys)) == bools(
            gm.intersects(poly_mixed, x=xs, y=ys)
        )


def test_point_vs_packed_polygon_array_batch() -> None:
    packed, mixed = polygon_storage_twins()
    points = gm.points([0.5, 1.5], [0.5, 1.5], crs=3857)
    assert bools(gm.within(points, packed)) == bools(gm.within(points, mixed))
    assert bools(gm.intersects(points, packed)) == bools(gm.intersects(points, mixed))
    assert bools(gm.contains(packed, points)) == bools(gm.contains(mixed, points))
