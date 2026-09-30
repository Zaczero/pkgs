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


@pytest.mark.parametrize(
    'wkt',
    [
        'LINESTRING (0 0, 1 2, 4 3)',
        'LINESTRING (10000000000000000 1, 10000000000000002 2)',
        'LINESTRING (5e-324 -1, 5e-324 2)',
        'MULTILINESTRING ((0 0, 1 2, 4 3), (5 0, 5 3))',
        ('GEOMETRYCOLLECTION (LINESTRING (0 0, 1 2, 4 3), LINESTRING (5 0, 5 3))'),
    ],
    ids=[
        'rounded_line_midpoint',
        'off_line_single_segment_midpoint',
        'subnormal_ordinate',
        'multiline',
        'collection',
    ],
)
def test_line_point_on_surface_is_exactly_on_linework(wkt: str) -> None:
    linework = gm.from_wkt(wkt)
    point = linework.point_on_surface()

    assert point.coordinate_axes == 'XY'
    assert (point.crs, point.epoch) == (linework.crs, linework.epoch)
    assert gm.covers(linework, point)
    assert gm.distance(linework, point, unit='planar') == 0.0


def test_holed_point_on_surface_is_inside_shell_outside_holes() -> None:
    donut = _holed_donut()
    point = (donut).point_on_surface()
    assert gm.covers(donut, point)
    hole = gm.box(3, 3, 7, 7)
    assert not gm.covers(hole, point)


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
        ')',
        crs=3857,
        epoch=2020.0,
    )
    array_result = gm.GeometryArray(
        [collection, None], crs=3857, epoch=2020.0
    ).point_on_surface()
    assert array_result.is_missing.tolist() == [False, True]
    assert (array_result.crs, array_result.epoch) == (collection.crs, collection.epoch)
    scalar_point = collection.point_on_surface()
    array_point = array_result[0]
    assert gm.equals(scalar_point, array_point)

    for point in (scalar_point, array_point):
        assert type(point) is gm.Point
        assert point.coordinate_axes == 'XY'
        assert gm.covered_by(point, collection)
        assert (point.crs, point.epoch) == (collection.crs, collection.epoch)


def test_mixed_axis_point_collection_uses_xy_selector() -> None:
    collection = gm.GeometryCollection(
        [
            gm.from_wkt('POINT Z (0 0 7)'),
            gm.Point(2, 0),
            gm.Point(10, 0),
        ],
        crs=3857,
        epoch=2020.0,
    )
    array_point = gm.GeometryArray(
        [collection], crs=3857, epoch=2020.0
    ).point_on_surface()[0]
    scalar_point = collection.point_on_surface()
    assert gm.equals(scalar_point, array_point)

    for point in (scalar_point, array_point):
        assert type(point) is gm.Point
        assert point.coordinate_axes == 'XY'
        assert gm.covered_by(point, collection)
        assert (point.crs, point.epoch) == (collection.crs, collection.epoch)


def test_sparse_multipoint_member_is_skipped_by_collection_surface_point() -> None:
    collection = gm.GeometryCollection([gm.MultiPoint([gm.Point(), gm.Point(1, 2)])])
    array_point = gm.GeometryArray([collection]).point_on_surface()[0]
    scalar_point = collection.point_on_surface()
    assert gm.equals(scalar_point, array_point)

    for point in (scalar_point, array_point):
        assert type(point) is gm.Point
        assert (point.x, point.y) == (1.0, 2.0)
        assert point.coordinate_axes == 'XY'
        assert gm.covered_by(point, collection)


def test_mixed_axis_degenerate_polygon_collection_uses_boundary_fallback() -> None:
    collection = gm.from_wkt(
        'GEOMETRYCOLLECTION ('
        'POLYGON ((0 0, 1 0, 3 0, 0 0)), '
        'POLYGON Z ((10 0 1, 11 0 1, 10 0 1, 10 0 1))'
        ')',
        crs=3857,
        epoch=2020.0,
    )
    array_point = gm.GeometryArray(
        [collection], crs=3857, epoch=2020.0
    ).point_on_surface()[0]
    scalar_point = collection.point_on_surface()
    assert gm.equals(scalar_point, array_point)

    for point in (scalar_point, array_point):
        assert type(point) is gm.Point
        assert point.coordinate_axes == 'XY'
        assert gm.covered_by(point, collection)
        assert (point.crs, point.epoch) == (collection.crs, collection.epoch)


def test_random_line_3_point_on_surface_is_exactly_on_linework() -> None:
    linework = gm.LineString(
        [
            (-1.050900940215378, 1.0201872904998037),
            (-1.4304915211489437, 2.063103840252438),
            (-2.391648612451435, 2.618028145649449),
            (-3.4846361290312164, 2.425304956898893),
            (-4.198033064677433, 1.5751115958968152),
            (-4.198033064677433, 0.46526298510279274),
            (-3.4846361290312178, -0.38493037589928525),
            (-2.391648612451436, -0.577653564649842),
            (-1.4304915211489442, -0.02272925925283098),
            (-1.050900940215378, 1.0201872904998037),
        ],
        crs=3857,
    )
    point = linework.point_on_surface()

    assert point.coordinate_axes == 'XY'
    assert (point.crs, point.epoch) == (linework.crs, linework.epoch)
    assert gm.covers(linework, point)
    assert gm.distance(linework, point, unit='planar') == 0.0
