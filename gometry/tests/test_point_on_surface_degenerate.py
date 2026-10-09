"""point_on_surface scanline + degenerate fallbacks — covers() invariant."""

from __future__ import annotations

import math
from dataclasses import dataclass
from fractions import Fraction
from itertools import pairwise
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


def _ring_location_exact(
    ring: list[tuple[float, float]], point: tuple[float, float]
) -> tuple[bool, bool]:
    """Return (inside, boundary) using exact rational orientation and parity."""
    exact_ring = [(Fraction(x), Fraction(y)) for x, y in ring]
    px, py = (Fraction(value) for value in point)
    inside = False
    for (ax, ay), (bx, by) in pairwise(exact_ring):
        orientation = (bx - ax) * (py - ay) - (by - ay) * (px - ax)
        if (
            orientation == 0
            and min(ax, bx) <= px <= max(ax, bx)
            and min(ay, by) <= py <= max(ay, by)
        ):
            return False, True
        if (ay <= py < by and orientation > 0) or (by <= py < ay and orientation < 0):
            inside = not inside
    return inside, False


def _polygon_covers_exact(polygon: gm.Polygon, point: tuple[float, float]) -> bool:
    shell_inside, shell_boundary = _ring_location_exact(
        list(polygon.exterior.coords), point
    )
    if not shell_inside and not shell_boundary:
        return False
    return all(
        not _ring_location_exact(list(hole.coords), point)[0]
        for hole in polygon.interiors
    )


def _geometry_covers_exact(geometry: gm.Geometry, point: tuple[float, float]) -> bool:
    if geometry.geometry_type == 'Polygon':
        return _polygon_covers_exact(cast('gm.Polygon', geometry), point)
    return any(_geometry_covers_exact(part, point) for part in geometry.parts)


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


def test_adjacent_float_polygon_candidates_are_exactly_covered() -> None:
    unit = 2.0**-52
    vertices = [(1.0, 0.0), (1.0 + unit, 2.0), (1.0 + unit, 3.0), (1.0, 0.0)]
    polygons = [
        gm.Polygon(vertices),
        gm.Polygon([(-x, y) for x, y in vertices]),
        gm.Polygon([(x, y + 8.0) for x, y in vertices]),
        gm.Polygon([(-x, 8.0 - y) for x, y in vertices]),
    ]

    for polygon in polygons:
        point = polygon.point_on_surface()
        assert _polygon_covers_exact(cast('gm.Polygon', polygon), (point.x, point.y))
        collection = gm.GeometryCollection([polygon])
        collection_point = collection.point_on_surface()
        assert _geometry_covers_exact(
            collection, (collection_point.x, collection_point.y)
        )


def test_surface_scanline_preserves_hole_and_tie_rules_with_exact_membership() -> None:
    donut = _holed_donut()
    donut_point = donut.point_on_surface()
    assert (donut_point.x, donut_point.y) == (8.5, 5.0)
    assert _polygon_covers_exact(
        cast('gm.Polygon', donut), (donut_point.x, donut_point.y)
    )

    first = gm.box(0, 0, 10, 10)
    second = gm.box(20, 0, 30, 10)
    multipolygon = gm.MultiPolygon([first, second])
    component_tie = multipolygon.point_on_surface()
    assert (component_tie.x, component_tie.y) == (5.0, 5.0)
    assert _geometry_covers_exact(multipolygon, (component_tie.x, component_tie.y))

    equal_chords = gm.Polygon([
        (0, 0),
        (2, 0),
        (2, 6),
        (6, 6),
        (6, 0),
        (8, 0),
        (8, 8),
        (0, 8),
        (0, 0),
    ])
    chord_tie = equal_chords.point_on_surface()
    assert (chord_tie.x, chord_tie.y) == (7.0, 4.0)
    assert _polygon_covers_exact(
        cast('gm.Polygon', equal_chords), (chord_tie.x, chord_tie.y)
    )


def test_typed_empty_and_nullable_reversed_duplicate_polygon_gather() -> None:
    empty = gm.from_wkt('POLYGON EMPTY')
    assert empty.point_on_surface().is_empty

    unit = 2.0**-52
    triangle = gm.Polygon([
        (1.0, 0.0),
        (1.0 + unit, 2.0),
        (1.0 + unit, 3.0),
        (1.0, 0.0),
    ])
    translated = gm.Polygon([
        (1.0, 8.0),
        (1.0 + unit, 10.0),
        (1.0 + unit, 11.0),
        (1.0, 8.0),
    ])
    source = gm.GeometryArray([triangle, None, translated])
    gather = source[[2, 1, 0, 2]]
    result = gather.point_on_surface()
    assert result.is_missing.tolist() == [False, True, False, False]

    expected = [translated, None, triangle, translated]
    for index, source_polygon in enumerate(expected):
        if source_polygon is not None:
            point = result[index]
            assert _polygon_covers_exact(
                cast('gm.Polygon', source_polygon), (point.x, point.y)
            )


def test_geographic_split_point_on_surface_is_covered_by_source_and_split_parts() -> (
    None
):
    source = gm.Polygon([(170, 40), (-170, 40), (-170, 50), (170, 50)], crs=4326)
    split = source.split_antimeridian()

    point = source.point_on_surface()

    assert gm.covered_by(point, source)
    assert _geometry_covers_exact(split, (point.x, point.y))


def test_equal_width_polygon_components_keep_first_input_order() -> None:
    first = gm.box(170, 0, 180, 10)
    adjacent = gm.box(180, 0, 190, 10)
    disjoint = gm.box(190, 0, 200, 10)
    forward_nested = gm.GeometryCollection([first, adjacent])
    reverse_nested = gm.GeometryCollection([adjacent, first])
    forward_shared = gm.GeometryCollection([forward_nested, forward_nested])
    reverse_shared = gm.GeometryCollection([reverse_nested, reverse_nested])
    cases = [
        (gm.GeometryCollection([first, adjacent]), (175.0, 5.0)),
        (gm.GeometryCollection([adjacent, first]), (185.0, 5.0)),
        (gm.MultiPolygon([first, disjoint]), (175.0, 5.0)),
        (gm.MultiPolygon([disjoint, first]), (195.0, 5.0)),
        (forward_shared, (175.0, 5.0)),
        (reverse_shared, (185.0, 5.0)),
    ]

    for geometry, expected in cases:
        point = geometry.point_on_surface()
        assert (point.x, point.y) == expected
        assert gm.covered_by(point, geometry)

    geometries = [geometry for geometry, _ in cases]
    result = gm.GeometryArray([*geometries, None]).point_on_surface()
    assert result.is_missing.tolist() == [
        False,
        False,
        False,
        False,
        False,
        False,
        True,
    ]
    assert [(result[index].x, result[index].y) for index in range(len(cases))] == [
        expected for _, expected in cases
    ]


def test_equal_width_polygon_component_tie_keeps_each_holes_parity() -> None:
    donut = _holed_donut()
    equal_width = gm.box(20, 0, 23, 10)
    collection = gm.GeometryCollection([donut, equal_width])

    point = collection.point_on_surface()

    assert (point.x, point.y) == (8.5, 5.0)
    assert gm.covered_by(point, collection)
    assert not gm.covers(gm.box(3, 3, 7, 7), point)


def test_vertical_gap_uses_global_bisector_boundary_fallback() -> None:
    collection = gm.GeometryCollection([gm.box(0, 0, 4, 2), gm.box(10, 8, 14, 10)])

    point = collection.point_on_surface()

    assert type(point) is gm.Point
    assert gm.covered_by(point, collection.boundary())


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
