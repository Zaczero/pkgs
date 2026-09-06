"""point_on_surface scanline + degenerate fallbacks — covers() invariant."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import cast

import gometry as gm
import pytest


@dataclass(frozen=True)
class SurfaceCase:
    name: str
    geom: gm.Geometry


def _holed_donut() -> gm.Geometry:
    return gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
        holes=[[(3, 3), (7, 3), (7, 7), (3, 7), (3, 3)]],
    )


SURFACE_CASES = [
    SurfaceCase('thin_sliver', gm.box(0, 0, 100, 0.001)),
    SurfaceCase('holed_donut', _holed_donut()),
    SurfaceCase('zero_area_collinear', gm.Polygon([(0, 0), (1, 0), (2, 0), (0, 0)])),
    SurfaceCase('tall_aspect', gm.box(0, 0, 1, 1000)),
    SurfaceCase('wide_aspect', gm.box(0, 0, 1000, 1)),
]


def _assert_covers_invariant(geom: gm.Geometry, point: gm.Point) -> None:
    assert gm.covered_by(point, geom)
    if geom.geometry_type == 'Polygon':
        poly = cast('gm.Polygon', geom)
        assert gm.covers(poly, point)
        for hole in poly.interiors:
            assert not gm.covers(hole, point)


@pytest.mark.parametrize('case', SURFACE_CASES, ids=lambda c: c.name)
def test_point_on_surface_satisfies_covers_invariant(case: SurfaceCase) -> None:
    point = (case.geom).point_on_surface()
    assert point.geometry_type == 'Point'
    assert math.isfinite(point.x)
    assert math.isfinite(point.y)
    _assert_covers_invariant(case.geom, point)


def test_holed_point_on_surface_is_inside_shell_outside_holes() -> None:
    donut = _holed_donut()
    point = (donut).point_on_surface()
    assert gm.covers(donut, point)
    hole = gm.box(3, 3, 7, 7)
    assert not gm.contains(hole, point)


@pytest.mark.parametrize(
    ('tag', 'ordinates'),
    [('Z', '7'), ('M', '7'), ('ZM', '7 8')],
    ids=['xyz', 'xym', 'xyzm'],
)
def test_mixed_axis_polygon_collection_uses_xy_scan(tag: str, ordinates: str) -> None:
    collection = gm.from_wkt(
        'GEOMETRYCOLLECTION ('
        'POLYGON ((0 0, 2 0, 2 4, 0 4, 0 0)), '
        f'POLYGON {tag} ((10 0 {ordinates}, 14 0 {ordinates}, '
        f'14 4 {ordinates}, 10 4 {ordinates}, 10 0 {ordinates}))'
        ')'
    )

    for value in (collection, gm.GeometryArray([collection])[0]):
        point = value.point_on_surface()
        assert point.to_wkt() == 'POINT (12 2)'
        assert point.coordinate_axes == 'XY'
        assert gm.covered_by(point, value)


def test_mixed_axis_point_collection_uses_xy_selector() -> None:
    collection = gm.GeometryCollection([
        gm.from_wkt('POINT Z (0 0 7)'),
        gm.Point(2, 0),
        gm.Point(10, 0),
    ])

    for value in (collection, gm.GeometryArray([collection])[0]):
        point = value.point_on_surface()
        assert point.to_wkt() == 'POINT (2 0)'
        assert point.coordinate_axes == 'XY'
        assert gm.covered_by(point, value)


def test_sparse_multipoint_member_is_skipped_by_collection_surface_point() -> None:
    collection = gm.GeometryCollection([gm.MultiPoint([gm.Point(), gm.Point(1, 2)])])

    for value in (collection, gm.GeometryArray([collection])[0]):
        point = value.point_on_surface()
        assert point.to_wkt() == 'POINT (1 2)'
        assert gm.covered_by(point, value)


def test_mixed_axis_degenerate_polygon_collection_uses_boundary_fallback() -> None:
    collection = gm.from_wkt(
        'GEOMETRYCOLLECTION ('
        'POLYGON ((0 0, 1 0, 3 0, 0 0)), '
        'POLYGON Z ((10 0 1, 11 0 1, 10 0 1, 10 0 1))'
        ')'
    )

    for value in (collection, gm.GeometryArray([collection])[0]):
        point = value.point_on_surface()
        assert point.to_wkt() == 'POINT (3 0)'
        assert point.coordinate_axes == 'XY'
        assert gm.covered_by(point, value)
