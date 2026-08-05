"""Topological predicates — planar measurements, DE-9IM, structural checks,
hole/concavity cases, and strict array broadcasting.
"""

import math
from collections.abc import Iterable
from typing import cast

import gometry as gm
import numpy as np
import pytest

from tests._support import ids, pair_rows


def test_polygon_planar_measurements_and_predicates() -> None:
    polygon = gm.box(-1, -1, 1, 1)
    point = gm.Point(0, 0)
    boundary = gm.Point(1, 0)
    outside = gm.Point(2, 0)
    assert polygon.area == 4
    assert polygon.length == 8
    assert gm.contains(polygon, point)
    assert not gm.contains(polygon, boundary)
    assert gm.covers(polygon, boundary)
    assert not gm.within(boundary, polygon)
    assert gm.covered_by(boundary, polygon)
    assert not gm.disjoint(polygon, boundary)
    assert gm.disjoint(polygon, outside)
    assert gm.equals(polygon, gm.box(-1, -1, 1, 1))
    assert gm.within(point, polygon)
    assert gm.covers(polygon, boundary)
    assert gm.covered_by(boundary, polygon)
    assert gm.disjoint(polygon, outside)
    assert gm.equals(polygon, gm.box(-1, -1, 1, 1))
    assert gm.touches(polygon, boundary)
    assert gm.touches(polygon, boundary)
    assert not gm.contains_xy(polygon, 1, 0)
    assert gm.intersects_xy(polygon, 1, 0)
    assert gm.contains_xy(polygon, 0, 0)
    np.testing.assert_array_equal(
        gm.contains_xy(polygon, [0, 1, 2], [0, 0, 0]), [True, False, False]
    )
    np.testing.assert_array_equal(gm.contains_xy(polygon, 0, [0, 2]), [True, False])
    np.testing.assert_array_equal(gm.contains_xy(polygon, [0, 2], 0), [True, False])
    np.testing.assert_array_equal(
        gm.intersects_xy(polygon, [0, 1, 2], [0, 0, 0]), [True, True, False]
    )
    np.testing.assert_array_equal(gm.intersects_xy(polygon, 0, [0, 2]), [True, False])
    with pytest.raises(ValueError, match='x and y must have the same length'):
        gm.contains_xy(polygon, [0, 1], [0])

    boxes = gm.GeometryArray([gm.box(0, 0, 2, 2), None, gm.box(10, 10, 12, 12)])
    np.testing.assert_array_equal(
        gm.contains_xy(boxes, [1.0, 1.0, 11.0], [1.0, 1.0, 11.0]),
        [True, False, True],
    )
    np.testing.assert_array_equal(
        gm.intersects_xy(boxes, 0.0, [1.0, 1.0, 11.0]),
        [True, False, False],
    )
    with pytest.raises(gm.InvalidGeometryError, match='GeometryArray and x'):
        gm.contains_xy(boxes, [1.0, 2.0], 1.0)

    crossing = gm.Polygon(
        [(170, -10), (-170, -10), (-170, 10), (170, 10), (170, -10)],
        crs=4326,
    )
    np.testing.assert_array_equal(
        gm.contains_xy(gm.GeometryArray([crossing, crossing]), [178.0, 0.0], 0.0),
        [True, False],
    )
    multipolygon = gm.MultiPolygon([
        [
            [(0, 0), (3, 0), (3, 3), (0, 3), (0, 0)],
            [(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)],
        ]
    ])
    collection = gm.GeometryCollection([gm.Point(5, 5), polygon])
    np.testing.assert_array_equal(
        gm.contains_xy(multipolygon, [0.5, 1.5, 3.0], [0.5, 1.5, 1.0]),
        [True, False, False],
    )
    np.testing.assert_array_equal(
        gm.intersects_xy(multipolygon, [0.5, 1.5, 3.0], [0.5, 1.5, 1.0]),
        [True, False, True],
    )
    np.testing.assert_array_equal(
        gm.contains_xy(collection, [0, 5, 2], [0, 5, 2]), [True, True, False]
    )
    square = gm.Polygon([(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)])
    assert gm.contains_xy(square, 2.0, 2.0)
    assert not gm.contains_xy(square, 2.0, 0.0)
    assert gm.contains_xy(square, 2.0, 1e-12)
    assert not gm.contains_xy(square, 2.0, -1e-12)
    holed = gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
        [[(3, 3), (7, 3), (7, 7), (3, 7), (3, 3)]],
    )
    assert gm.contains_xy(holed, 1.0, 1.0)
    assert not gm.contains_xy(holed, 5.0, 5.0)
    assert not gm.contains_xy(holed, 3.0, 5.0)
    assert not gm.contains_xy(gm.LineString([(0, 0), (1, 0)]), 0, 0)
    assert gm.intersects_xy(gm.LineString([(0, 0), (1, 0)]), 0, 0)
    assert gm.contains_xy(gm.LineString([(0, 0), (1, 0)]), 0.5, 0)
    multiline = gm.MultiLineString([[(0, 0), (1, 0)], [(1, 0), (2, 0)]])
    assert not gm.contains_xy(multiline, 0, 0)
    assert gm.contains_xy(multiline, 1, 0)
    assert gm.contains_xy(multiline, 0.5, 0)
    assert gm.intersects_xy(multiline, 2, 0)
    assert not gm.contains(polygon, outside)
    with pytest.raises(
        gm.CRSMismatchError, match='contains requires matching CRS metadata'
    ):
        gm.contains(gm.box(-1, -1, 1, 1, crs=4326), gm.Point(0, 0, crs=3857))
    with pytest.raises(
        gm.CRSMismatchError, match='intersects requires matching CRS metadata'
    ):
        gm.intersects(gm.box(-1, -1, 1, 1, crs=4326), gm.Point(0, 0, crs=3857))
    with pytest.raises(
        gm.CRSMismatchError, match='relate requires matching CRS metadata'
    ):
        gm.relate(
            gm.GeometryArray([gm.box(-1, -1, 1, 1, crs=4326)]), gm.Point(0, 0, crs=3857)
        )


def test_relate_array_value_dispatch_preserves_operand_order_and_missing_masks() -> (
    None
):
    polygon = gm.box(0, 0, 2, 2)
    values = gm.GeometryArray([gm.Point(1, 1), None, gm.Point(3, 3)])

    scalar_left = gm.relate(polygon, values)
    scalar_right = gm.relate(values, polygon)
    assert scalar_left == [
        gm.relate(polygon, values[0]),
        None,
        gm.relate(polygon, values[2]),
    ]
    assert scalar_right == [
        gm.relate(values[0], polygon),
        None,
        gm.relate(values[2], polygon),
    ]
    assert scalar_left[0] != scalar_right[0]

    left = gm.GeometryArray([polygon, None, gm.Point(3, 3)])
    right = gm.GeometryArray([gm.Point(1, 1), gm.Point(0, 0), None])
    left_right = gm.relate(left, right)
    right_left = gm.relate(right, left)
    assert left_right == [gm.relate(left[0], right[0]), None, None]
    assert right_left == [gm.relate(right[0], left[0]), None, None]
    assert left_right[0] != right_left[0]


def test_planar_similarity_metrics_are_rust_backed() -> None:
    left = gm.LineString([(0, 0), (1, 1), (2, 1)])
    right = gm.LineString([(0, 1), (1, 2), (2, 2)])
    point = gm.Point(0, 0)
    values = gm.GeometryArray([right, gm.LineString([(0, 0), (2, 0)])])
    lonlat = gm.LineString([(0, 0), (1, 1)], crs=4326)
    peak_line = gm.LineString([(0, 0), (10, 0)])
    far_peak = gm.LineString([(0, 1), (5, 8), (10, 1)])
    assert gm.hausdorff_distance(peak_line, far_peak) == pytest.approx(8)
    geo_peak = gm.LineString([(0, 0), (10, 0)], crs=4326)
    geo_far = gm.LineString([(0, 1), (5, 8), (10, 1)], crs=4326)
    assert gm.hausdorff_distance(geo_peak, geo_far) > 500000
    assert gm.hausdorff_distance(geo_peak, geo_far, densify=0.5) > 500000
    assert gm.hausdorff_distance(geo_peak, geo_far, densify=0.5) == pytest.approx(
        gm.hausdorff_distance(geo_peak, geo_far)
    )
    assert gm.hausdorff_distance(left, right) == pytest.approx(1)
    assert gm.hausdorff_distance(left, right) == pytest.approx(1)
    assert gm.hausdorff_distance(
        point, gm.MultiPoint([(0, 0), (1, 2)])
    ) == pytest.approx(5**0.5)
    assert gm.hausdorff_distance(left, values) == pytest.approx([1, 1])
    assert gm.hausdorff_distance(values, left) == pytest.approx([1, 1])
    assert gm.hausdorff_distance(values, values) == pytest.approx([0, 0])
    assert gm.frechet_distance(left, right) == pytest.approx(1)
    assert gm.frechet_distance(left, values) == pytest.approx([1, 2**0.5])
    assert gm.frechet_distance(values, left) == pytest.approx([1, 2**0.5])
    assert gm.frechet_distance(values, values) == pytest.approx([0, 0])
    geo_line = gm.LineString([(0, 1), (1, 2)], crs=4326)
    assert gm.hausdorff_distance(lonlat, geo_line) == pytest.approx(
        110575.06, rel=0.0001
    )
    assert gm.hausdorff_distance(lonlat, geo_line, unit='planar') == pytest.approx(1)
    assert gm.frechet_distance(lonlat, geo_line, unit='planar') == pytest.approx(1)
    assert gm.frechet_distance(lonlat, geo_line) == pytest.approx(110575.06, rel=0.0001)
    with pytest.raises(ValueError, match='same length'):
        gm.hausdorff_distance(values, gm.GeometryArray([right]))
    with pytest.raises(TypeError, match='Frechet distance requires'):
        gm.frechet_distance(left, gm.box(0, 0, 1, 1))


def test_array_detached_map_and_metric_lanes_match_scalar_results() -> None:
    polygon = gm.Polygon([(0, 0), (4, 0), (4, 2), (2, 2), (2, 4), (0, 4), (0, 0)])
    shifted = gm.Polygon([(1, 0), (5, 0), (5, 2), (3, 2), (3, 4), (1, 4), (1, 0)])
    polygons = [polygon, shifted]
    polygon_array = gm.GeometryArray(polygons)

    def wkts(values: Iterable[gm.Geometry]) -> list[str]:
        return [geom.to_wkt() for geom in values]

    assert polygon_array.simplify(0.25).to_wkt() == [
        geom.simplify(0.25).to_wkt() for geom in polygons
    ]
    assert polygon_array.simplify(0.25, method='vw').to_wkt() == [
        geom.simplify(0.25, method='vw').to_wkt() for geom in polygons
    ]
    assert polygon_array.normalize().to_wkt() == [
        geom.normalize().to_wkt() for geom in polygons
    ]
    assert polygon_array.quantize(1).to_wkt() == [
        geom.quantize(1).to_wkt() for geom in polygons
    ]
    assert polygon_array.snap_to_grid(0.5).to_wkt() == [
        geom.snap_to_grid(0.5).to_wkt() for geom in polygons
    ]
    assert polygon_array.convex_hull().to_wkt() == [
        geom.convex_hull().to_wkt() for geom in polygons
    ]
    assert polygon_array.minimum_bounding_circle().to_wkt() == [
        geom.minimum_bounding_circle().to_wkt() for geom in polygons
    ]
    assert polygon_array.minimum_rotated_rectangle().to_wkt() == [
        geom.minimum_rotated_rectangle().to_wkt() for geom in polygons
    ]
    assert polygon_array.polylabel(tolerance=0.1).to_wkt() == [
        geom.polylabel(tolerance=0.1).to_wkt() for geom in polygons
    ]
    repeated = gm.LineString([(0, 0), (1, 0), (1, 0), (2, 0)])
    bent = gm.LineString([(0, 0), (0.5, 0.25), (1, 0)])
    lines = [repeated, bent]
    line_array = gm.GeometryArray(lines)
    assert line_array.remove_repeated_points().to_wkt() == [
        geom.remove_repeated_points().to_wkt() for geom in lines
    ]
    assert line_array.segmentize(0.5).to_wkt() == [
        geom.segmentize(0.5).to_wkt() for geom in lines
    ]
    assert line_array.set_z(7).set_m(3).to_wkt() == [
        geom.set_z(7).set_m(3).to_wkt() for geom in lines
    ]
    merged = gm.MultiLineString([[(0, 0), (1, 0)], [(1, 0), (2, 0)]])
    merge_array = gm.GeometryArray([merged, merged])
    assert merge_array.line_merge().to_wkt() == [merged.line_merge().to_wkt()] * 2
    assert wkts(polygon_array.subdivide(max_vertices=8).values) == [
        part.to_wkt() for geom in polygons for part in geom.subdivide(max_vertices=8)
    ]
    assert wkts(polygon_array.triangulate(method='earcut').values) == [
        part.to_wkt() for geom in polygons for part in geom.triangulate(method='earcut')
    ]
    right = gm.LineString([(0, 1), (1, 1), (2, 1)])
    assert list(gm.hausdorff_distance(line_array, right)) == pytest.approx([
        gm.hausdorff_distance(geom, right) for geom in lines
    ])
    assert list(gm.frechet_distance(line_array, right)) == pytest.approx([
        gm.frechet_distance(geom, right) for geom in lines
    ])


def test_structural_predicates_and_minimum_clearance_are_rust_backed() -> None:
    open_line = gm.LineString([(0, 0), (1, 0)])
    ring = gm.LineString([(0, 0), (1, 0), (1, 1), (0, 0)])
    duplicate_line = gm.LineString([(0, 0), (1, 0), (0, 0)])
    crossing_line = gm.LineString([(0, 0), (1, 1), (1, 0), (0, 1)])
    polygon = gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])
    empty = gm.GeometryCollection([])
    array = gm.GeometryArray([open_line, ring, crossing_line, empty])
    assert not open_line.is_empty
    assert empty.is_empty
    assert not open_line.is_closed
    assert ring.is_closed
    assert ring.is_ring
    assert not duplicate_line.is_ring
    assert open_line.is_simple
    assert not crossing_line.is_simple
    assert not gm.MultiPoint([(0, 0), (0, 0)]).is_simple
    assert polygon.is_simple
    assert open_line.minimum_clearance() == pytest.approx(1.0)
    assert polygon.minimum_clearance() == pytest.approx(2 ** (-0.5))
    assert gm.Point(0, 0).minimum_clearance() == math.inf
    np.testing.assert_array_equal(array.is_closed, [False, True, False, False])
    np.testing.assert_array_equal(array.is_ring, [False, True, False, False])
    np.testing.assert_array_equal(array.is_simple, [True, True, False, False])
    clearance = cast('list[float]', array.minimum_clearance())
    assert clearance[:3] == pytest.approx([1.0, 2 ** (-0.5), 2 ** (-0.5)])


def test_de9im_predicates_are_exposed_for_scalar_array_index_and_join() -> None:
    horizontal = gm.LineString([(-1, 0), (1, 0)])
    vertical = gm.LineString([(0, -1), (0, 1)])
    left = gm.box(0, 0, 2, 2)
    right = gm.box(1, 1, 3, 3)
    separate = gm.box(5, 5, 6, 6)
    assert gm.crosses(horizontal, vertical)
    np.testing.assert_array_equal(
        gm.crosses(horizontal, gm.GeometryArray([vertical, separate])), [True, False]
    )
    assert gm.relate(horizontal, vertical) == '0F1FF0102'
    assert gm.relate(horizontal, gm.GeometryArray([vertical, separate])) == [
        '0F1FF0102',
        'FF1FF0212',
    ]
    assert gm.relate_pattern(horizontal, vertical, '0********')
    np.testing.assert_array_equal(
        gm.relate_pattern(
            horizontal, gm.GeometryArray([vertical, separate]), '0********'
        ),
        [True, False],
    )
    assert gm.overlaps(left, right)
    np.testing.assert_array_equal(
        gm.overlaps(left, gm.GeometryArray([right, separate])), [True, False]
    )
    assert gm.relate(left, right) == '212101212'
    assert gm.relate_pattern(left, right, 'T*T***T**')
    with pytest.raises(gm.GeometryError, match='invalid DE-9IM pattern'):
        gm.relate_pattern(left, right, 'bad')
    assert ids(
        gm.SpatialIndex([vertical, right, separate]).query(
            horizontal, predicate='crosses'
        )
    ) == [0]
    assert ids(
        gm.SpatialIndex([vertical, right, separate]).query(left, predicate='overlaps')
    ) == [1]
    assert pair_rows(
        gm.join([horizontal, left], [vertical, right, separate], predicate='crosses')
    ) == [(0, 0)]
    assert pair_rows(
        gm.join([horizontal, left], [vertical, right, separate], predicate='overlaps')
    ) == [(1, 1)]


def test_de9im_reviewer_correctness_cases_match_shapely() -> None:
    shapely = pytest.importorskip('shapely')
    cases = [
        (
            'GEOMETRYCOLLECTION (POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)), POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0)))',
            'POINT (1 0.5)',
            '0F2FF1FF2',
        ),
        (
            'GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), POINT (1 0))',
            'POINT (1 0)',
            'FF20F1FF2',
        ),
        (
            'GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), LINESTRING (0 0, 2 0))',
            'LINESTRING (0.5 0, 1.5 0)',
            'FF2101FF2',
        ),
        (
            'GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)))',
            'LINESTRING (-1 1, 3 1)',
            '1F20F1102',
        ),
        (
            'GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)))',
            'LINESTRING (1 1, 3 1)',
            '1020F1102',
        ),
        (
            'GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)))',
            'POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))',
            '212101212',
        ),
        (
            'GEOMETRYCOLLECTION (POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0)), LINESTRING (0 0, 2 0))',
            'LINESTRING (0 0, 2 0)',
            'FF2101FF2',
        ),
        ('LINESTRING (0 0, 0 0)', 'POINT (1 0)', 'FF0FFF0F2'),
        ('POLYGON ((0 0, 0 0, 0 0, 0 0))', 'LINESTRING (0 0, 1 0)', 'FF2F0F102'),
    ]
    for left_wkt, right_wkt, matrix in cases:
        left = gm.from_wkt(left_wkt)
        right = gm.from_wkt(right_wkt)
        oracle_left = shapely.from_wkt(left_wkt)
        oracle_right = shapely.from_wkt(right_wkt)
        assert shapely.relate(oracle_left, oracle_right) == matrix
        assert gm.relate(left, right) == matrix
        assert gm.relate(left, right) == matrix
        assert gm.relate_pattern(left, right, matrix)
        assert gm.relate_pattern(left, right, matrix)
    assert gm.contains(gm.from_wkt(cases[0][0]), gm.from_wkt(cases[0][1]))
    assert not gm.touches(gm.from_wkt(cases[0][0]), gm.from_wkt(cases[0][1]))
    assert not gm.contains(gm.from_wkt(cases[1][0]), gm.from_wkt(cases[1][1]))
    assert gm.touches(gm.from_wkt(cases[1][0]), gm.from_wkt(cases[1][1]))
    assert (
        gm.relate(gm.from_wkt('LINESTRING (0 0, 2 0)'), gm.Point(1, 0)) == '0F1FF0FF2'
    )
    assert (
        gm.relate(gm.Point(1, 0), gm.from_wkt('LINESTRING (0 0, 2 0)')) == '0FFFFF102'
    )


def test_intersects_detects_line_polygon_and_line_line_crossings() -> None:
    polygon = gm.box(-1, -1, 1, 1)
    crossing_line = gm.LineString([(-2, 0), (2, 0)])
    outside_line = gm.LineString([(-2, 2), (2, 2)])
    diagonal = gm.LineString([(-1, -1), (1, 1)])
    other_diagonal = gm.LineString([(-1, 1), (1, -1)])
    assert gm.intersects(polygon, crossing_line)
    assert gm.intersects(crossing_line, polygon)
    assert not gm.intersects(polygon, outside_line)
    assert gm.intersects(diagonal, other_diagonal)


def test_topology_does_not_treat_near_collinear_points_as_collinear() -> None:
    offset = math.ulp(1.0) / 4
    base = gm.LineString([(0, 0), (1, 0)])
    near = gm.LineString([(0.5, offset), (0.5, 1)])
    assert not gm.contains_xy(base, 0.5, offset)
    assert not gm.intersects(base, near)


def test_polygon_hole_excludes_points() -> None:
    polygon = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4)], holes=[[(1, 1), (3, 1), (3, 3), (1, 3)]]
    )
    assert gm.contains_xy(polygon, 0.5, 0.5)
    assert not gm.contains_xy(polygon, 2, 2)


def test_intersects_detects_polygon_hole_boundaries() -> None:
    polygon = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4)], holes=[[(1, 1), (3, 1), (3, 3), (1, 3)]]
    )
    assert gm.intersects(polygon, gm.LineString([(0.5, 2), (3.5, 2)]))
    assert not gm.contains(polygon, gm.LineString([(1.5, 2), (2.5, 2)]))


def test_polygon_contains_rejects_lines_that_leave_concave_area() -> None:
    polygon = gm.Polygon([(0, 0), (2, 0), (2, 2), (1, 1), (0, 2)])
    crossing_notch = gm.LineString([(0.5, 1.5), (1.5, 1.5)])
    inside = gm.LineString([(0.5, 0.5), (1.5, 0.5)])
    assert gm.intersects(polygon, crossing_notch)
    assert not gm.contains(polygon, crossing_notch)
    assert gm.contains(polygon, inside)


def test_polygon_contains_rejects_polygons_inside_holes() -> None:
    polygon = gm.Polygon(
        [(0, 0), (5, 0), (5, 5), (0, 5)], holes=[[(1, 1), (4, 1), (4, 4), (1, 4)]]
    )
    assert not gm.contains(polygon, gm.box(2, 2, 3, 3))
    assert gm.contains(polygon, gm.box(0.1, 0.1, 0.9, 0.9))


def test_binary_predicates_support_strict_array_broadcasting() -> None:
    polygons = gm.GeometryArray([gm.box(-1, -1, 1, 1), gm.box(10, 10, 11, 11)])
    points = gm.points([0, 20], [0, 20])
    crossing = gm.GeometryArray([
        gm.LineString([(-2, 0), (2, 0)]),
        gm.LineString([(10, 10), (11, 11)]),
    ])
    np.testing.assert_array_equal(gm.contains(polygons, points), [True, False])
    np.testing.assert_array_equal(gm.contains(polygons, points), [True, False])
    np.testing.assert_array_equal(gm.contains(polygons, gm.Point(0, 0)), [True, False])
    np.testing.assert_array_equal(gm.contains(polygons, gm.Point(0, 0)), [True, False])
    np.testing.assert_array_equal(gm.within(points, polygons), [True, False])
    np.testing.assert_array_equal(gm.within(points, polygons), [True, False])
    np.testing.assert_array_equal(gm.covers(polygons, points), [True, False])
    np.testing.assert_array_equal(gm.covers(polygons, points), [True, False])
    np.testing.assert_array_equal(gm.covered_by(points, polygons), [True, False])
    np.testing.assert_array_equal(gm.covered_by(points, polygons), [True, False])
    np.testing.assert_array_equal(gm.intersects(polygons, crossing), [True, True])
    np.testing.assert_array_equal(gm.intersects(polygons, crossing), [True, True])
    np.testing.assert_array_equal(gm.disjoint(polygons, points), [False, True])
    np.testing.assert_array_equal(gm.disjoint(polygons, points), [False, True])
    np.testing.assert_array_equal(
        gm.touches(polygons, gm.GeometryArray([gm.Point(1, 0), gm.Point(10, 10)])),
        [True, True],
    )
    np.testing.assert_array_equal(
        gm.touches(polygons, gm.GeometryArray([gm.Point(1, 0), gm.Point(10, 10)])),
        [True, True],
    )
    np.testing.assert_array_equal(gm.crosses(polygons, crossing), [True, False])
    np.testing.assert_array_equal(gm.crosses(polygons, crossing), [True, False])
    np.testing.assert_array_equal(
        gm.overlaps(
            polygons, gm.GeometryArray([gm.box(0, 0, 2, 2), gm.box(20, 20, 21, 21)])
        ),
        [True, False],
    )
    np.testing.assert_array_equal(
        gm.overlaps(
            polygons, gm.GeometryArray([gm.box(0, 0, 2, 2), gm.box(20, 20, 21, 21)])
        ),
        [True, False],
    )
    np.testing.assert_array_equal(
        gm.equals(
            gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)]),
            gm.points([0, 2], [0, 2]),
        ),
        [True, False],
    )
    np.testing.assert_array_equal(
        gm.equals_exact(
            gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(0, 0, 1, 1)]).normalize(),
            gm.GeometryArray([
                gm.Polygon([(1, 1), (0, 1), (0, 0), (1, 0), (1, 1)]),
                gm.box(0, 0, 2, 2),
            ]).normalize(),
        ),
        [True, False],
    )
    assert gm.relate(polygons, points) == ['0F2FF1FF2', 'FF2FF10F2']
    np.testing.assert_array_equal(
        gm.relate_pattern(polygons, points, 'T********'), [True, False]
    )
    with pytest.raises(ValueError, match='same length'):
        gm.contains(polygons, gm.points([0], [0]))
    np.testing.assert_array_equal(
        gm.equals(
            gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)]),
            gm.points([0, 2], [0, 2]),
        ),
        [True, False],
    )


def test_contains_properly_excludes_boundary_contact() -> None:
    square = gm.box(0, 0, 10, 10)
    inner = gm.box(2, 2, 8, 8)
    touching = gm.box(0, 0, 5, 5)
    point_in = gm.Point(5, 5)
    point_on = gm.Point(0, 5)
    assert gm.contains(square, inner) and gm.contains_properly(square, inner)
    assert gm.contains(square, touching) and (
        not gm.contains_properly(square, touching)
    )
    assert gm.contains_properly(square, point_in)
    assert not gm.contains_properly(square, point_on)
    for other in (inner, touching, point_in, point_on):
        assert gm.contains_properly(square, other) == gm.relate_pattern(
            square, other, 'T**FF*FF*'
        )
    assert not gm.contains_properly(square, gm.from_wkt('POINT EMPTY'))
    assert not gm.contains_properly(gm.from_wkt('POLYGON EMPTY'), square)
    arr = gm.GeometryArray([inner, touching])
    np.testing.assert_array_equal(gm.contains_properly(square, arr), [True, False])
    np.testing.assert_array_equal(gm.contains_properly(arr, square), [False, False])
    np.testing.assert_array_equal(
        square.prepare().contains_properly(arr), [True, False]
    )
    idx = gm.SpatialIndex(arr)
    assert ids(idx.query(square, predicate='contains_properly')) == [0]
    assert pair_rows(gm.join([square], arr, predicate='contains_properly')) == [(0, 0)]


def test_xy_predicates_are_vectorized_on_every_surface() -> None:
    square = gm.box(0, 0, 10, 10)
    xs, ys = ([5.0, 0.0, 20.0], [5.0, 5.0, 20.0])
    assert gm.contains_xy(square, 5, 5) is True
    np.testing.assert_array_equal(gm.contains_xy(square, xs, ys), [True, False, False])
    np.testing.assert_array_equal(gm.intersects_xy(square, xs, ys), [True, True, False])
    prepared = square.prepare()
    np.testing.assert_array_equal(prepared.contains_xy(xs, ys), [True, False, False])
    np.testing.assert_array_equal(prepared.intersects_xy(xs, ys), [True, True, False])
    assert prepared.intersects_xy(0, 5) is True
    np.testing.assert_array_equal(gm.contains_xy(square, xs, ys), [True, False, False])
    np.testing.assert_array_equal(gm.intersects_xy(square, xs, ys), [True, True, False])


def test_dispatch_strategy_seams_cannot_change_results() -> None:
    """Results are identical at every batch-strategy boundary.

    15/16/17 elements straddle the per-pair -> prepared threshold; the
    255/256/257-coordinate scalars straddle the cached-geo -> prepared
    escalation for ``intersects``. A divergence between strategies fails here.
    """
    import math

    probe = gm.Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
    cases = [gm.box(2, 2, 4, 4), gm.box(0, 0, 5, 5), gm.box(20, 20, 30, 30)]
    for n in (15, 16, 17):
        batch = (cases * 6)[:n]
        arr = gm.GeometryArray(batch)
        expected = [gm.contains(probe, b) for b in batch]
        np.testing.assert_array_equal(gm.contains(probe, arr), expected, err_msg=str(n))
        np.testing.assert_array_equal(
            probe.prepare().contains(arr), expected, err_msg=str(n)
        )
    cells = gm.GeometryArray([
        gm.box(i, j, i + 0.9, j + 0.9) for i in range(6) for j in range(6)
    ])
    for vertices in (254, 255, 256):
        ring = [
            (
                3 + 4 * math.cos(2 * math.pi * k / vertices),
                3 + 4 * math.sin(2 * math.pi * k / vertices),
            )
            for k in range(vertices)
        ]
        scalar = gm.Polygon(ring)
        expected = [gm.intersects(scalar, cell) for cell in list(cells)]
        np.testing.assert_array_equal(
            gm.intersects(scalar, cells), expected, err_msg=str(vertices)
        )


def test_geometry_collections_flow_through_every_strategy() -> None:
    gc = gm.from_wkt('GEOMETRYCOLLECTION (POINT (5 5), LINESTRING (0 0, 1 1))')
    square = gm.box(0, 0, 10, 10)
    assert gm.contains(square, gc)
    np.testing.assert_array_equal(
        gm.contains(square, gm.GeometryArray([gc] * 3)), [True] * 3
    )
    np.testing.assert_array_equal(
        gm.contains(square, gm.GeometryArray([gc] * 20)), [True] * 20
    )
    np.testing.assert_array_equal(
        square.prepare().contains(gm.GeometryArray([gc] * 20)), [True] * 20
    )


def test_prepared_empty_geometry_answers_through_kernels() -> None:
    prepared = gm.from_wkt('POLYGON EMPTY').prepare()
    assert prepared.contains(gm.Point(0, 0)) is False
    np.testing.assert_array_equal(
        prepared.contains(gm.GeometryArray([gm.Point(0, 0)] * 20)), [False] * 20
    )
    assert prepared.equals(gm.from_wkt('POINT EMPTY')) is True


def test_multiline_mod2_junction_rule_holds_on_every_spelling() -> None:
    """An odd-degree junction vertex is boundary; an even-degree one is interior."""
    junction = gm.Point(1, 1)
    degree3 = gm.from_wkt('MULTILINESTRING ((0 0, 1 1), (2 0, 1 1), (1 2, 1 1))')
    degree2 = gm.from_wkt('MULTILINESTRING ((0 0, 1 1), (2 2, 1 1))')
    for lines, contained in ((degree3, False), (degree2, True)):
        assert gm.contains(lines, junction) is contained
        np.testing.assert_array_equal(
            gm.contains(lines, gm.GeometryArray([junction] * 20)), [contained] * 20
        )
        assert lines.prepare().contains(junction) is contained
        assert ids(gm.SpatialIndex([junction]).query(lines, predicate='contains')) == (
            [0] if contained else []
        )


def test_equals_bounds_gate_never_false_rejects() -> None:
    square = gm.box(0, 0, 10, 10)
    diagonal = gm.LineString([(0, 0), (10, 10)])
    assert not gm.equals(square, diagonal)
    np.testing.assert_array_equal(
        gm.equals(square, gm.GeometryArray([diagonal] * 20)), [False] * 20
    )
    rotations = gm.GeometryArray(
        [
            gm.box(0, 0, 10, 10, ccw=False),
            gm.Polygon([(10, 0), (10, 10), (0, 10), (0, 0), (10, 0)]),
        ]
        * 10
    )
    np.testing.assert_array_equal(gm.equals(square, rotations), [True] * 20)
    np.testing.assert_array_equal(
        gm.equals(gm.Point(-0.0, 0.0), gm.GeometryArray([gm.Point(0.0, 0.0)] * 20)),
        [True] * 20,
    )


def test_contains_properly_on_one_dimensional_containers() -> None:
    line = gm.LineString([(0, 0), (10, 0)])
    cases = [
        (gm.Point(5, 0), True),
        (gm.Point(0, 0), False),
        (gm.LineString([(2, 0), (8, 0)]), True),
        (gm.LineString([(0, 0), (5, 0)]), False),
    ]
    for other, expected in cases:
        assert gm.contains_properly(line, other) is expected
        assert gm.relate_pattern(line, other, 'T**FF*FF*') is expected


def test_closed_lines_have_no_boundary() -> None:
    ring = gm.LineString([(0, 0), (1, 0), (1, 1), (0, 0)])
    assert gm.contains(ring, gm.Point(0, 0))
    assert gm.contains_xy(ring, 0, 0) is True
    assert gm.relate(ring, gm.Point(0, 0)) == '0F1FFFFF2'
    rays = gm.from_wkt(
        'MULTILINESTRING ((0 0, 1 0), (0 0, -1 0), (0 0, 0 1), (0 0, 0 -1))'
    )
    assert gm.contains(rays, gm.Point(0, 0))


def test_hole_boundary_point_relations_across_spellings() -> None:
    donut = gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
        holes=[[(3, 3), (7, 3), (7, 7), (3, 7), (3, 3)]],
    )
    on_hole_edge = gm.Point(3, 5)
    expectations = {
        'contains': False,
        'contains_properly': False,
        'covers': True,
        'intersects': True,
        'touches': True,
        'disjoint': False,
    }
    prepared = donut.prepare()
    batch = gm.GeometryArray([on_hole_edge] * 20)
    for name, expected in expectations.items():
        assert getattr(gm, name)(donut, on_hole_edge) is expected, name
        assert getattr(prepared, name)(on_hole_edge) is expected, name
        assert (
            getattr(gm, name)(donut, on_hole_edge) == getattr(gm, name)(donut, batch)[0]
        ), name
    assert gm.within(on_hole_edge, donut) is False
    assert gm.covered_by(on_hole_edge, donut) is True
    in_hole = gm.Point(5, 5)
    assert not gm.covers(donut, in_hole) and gm.disjoint(donut, in_hole)


def test_shared_edge_multipolygon_is_invalid_but_strategies_agree() -> None:
    mp = gm.from_wkt(
        'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((1 0, 2 0, 2 1, 1 1, 1 0)))'
    )
    assert not mp.is_valid
    edge_point = gm.Point(1, 0.5)
    scalar = gm.contains(mp, edge_point)
    np.testing.assert_array_equal(
        gm.contains(mp, gm.GeometryArray([edge_point] * 20)), [scalar] * 20
    )
    assert mp.prepare().contains(edge_point) is scalar


def test_equals_exact_tolerance_boundary_includes_z_and_m() -> None:
    a = gm.from_wkt('POINT ZM (0 0 0 0)')
    b = gm.from_wkt('POINT ZM (1e-12 -1e-12 1e-12 -1e-12)')
    assert gm.equals_exact(a, b, 1e-12)
    assert not gm.equals_exact(a, b, 5e-13)
    assert not gm.equals_exact(a, b, 0.0)
    c = gm.from_wkt('POINT ZM (0 0 5 5)')
    assert gm.equals_exact(a, c, 0.0, include_z=False, include_m=False)


def test_xy_predicates_accept_empty_iterables() -> None:
    poly = gm.box(0, 0, 1, 1)
    np.testing.assert_array_equal(gm.contains_xy(poly, [], []), [])
    np.testing.assert_array_equal(poly.prepare().intersects_xy([], []), [])
    with pytest.raises(ValueError, match='same length'):
        gm.contains_xy(poly, 0.0, [])


def test_relate_class_predicates_decide_natively_without_the_matrix() -> None:
    square = gm.box(0, 0, 4, 4)
    assert gm.overlaps(square, gm.box(2, 2, 6, 6))
    assert not gm.touches(square, gm.box(2, 2, 6, 6))
    assert not gm.overlaps(square, gm.box(1, 1, 2, 2))
    assert not gm.touches(square, gm.box(1, 1, 2, 2))
    parts = gm.MultiPolygon([
        [[(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]],
        [[(9, 9), (10, 9), (10, 10), (9, 10), (9, 9)]],
    ])
    assert gm.overlaps(square, parts) and gm.overlaps(parts, square)
    assert not gm.touches(square, parts)
    kiss = gm.GeometryCollection([gm.box(9, 0, 10, 1), gm.Point(0, 2)])
    assert gm.touches(square, kiss) and gm.touches(kiss, square)
    inside = gm.GeometryCollection([gm.box(9, 0, 10, 1), gm.Point(2, 2)])
    assert not gm.touches(square, inside) and (not gm.touches(inside, square))
    assert gm.crosses(gm.LineString([(-1, 1), (5, 1)]), square)
    assert gm.crosses(square, gm.LineString([(-1, 1), (5, 1)]))
    assert not gm.crosses(gm.LineString([(1, 1), (2, 2)]), square)
    assert not gm.crosses(gm.LineString([(3.5, 5), (5, 3.5)]), square)


def test_native_areal_relate_grades_every_tangential_contact() -> None:
    a = gm.box(0, 0, 2, 2)
    assert gm.relate(a, gm.box(2, 0, 4, 2)) == 'FF2F11212'
    assert gm.relate(a, gm.box(2, 2, 4, 4)) == 'FF2F01212'
    assert gm.relate(a, gm.box(1, 1, 3, 3)) == '212101212'
    assert gm.relate(a, gm.box(0, 0, 2, 2)) == '2FFF1FFF2'
    assert gm.relate(a, gm.box(0.5, 0.5, 1.5, 1.5)) == '212FF1FF2'
    assert gm.relate(a, gm.box(0, 0, 1, 1)) == '212F11FF2'
    lateral = gm.Polygon([(0, 0), (2, 0), (2, 1), (3, 1), (3, 3), (0, 3)])
    assert gm.relate(a, lateral) == '2FF11F212'
    ring = gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10)], holes=[[(3, 3), (7, 3), (7, 7), (3, 7)]]
    )
    assert gm.relate(ring, gm.box(3, 3, 5, 5)) == 'FF2F11212'
    assert gm.touches(a, gm.box(2, 0, 4, 2)) and (
        not gm.overlaps(a, gm.box(2, 0, 4, 2))
    )
    assert gm.covers(a, gm.box(0, 0, 1, 1)) and (
        not gm.contains_properly(a, gm.box(0, 0, 1, 1))
    )
    assert gm.relate_pattern(a, lateral, 'T*F**F***')


def test_native_lineal_relate_grades_every_contact_class() -> None:
    line = gm.LineString
    assert gm.relate(line([(0, 0), (1, 0)]), line([(1, 0), (2, 1)])) == 'FF1F00102'
    assert gm.relate(line([(0, 0), (2, 2)]), line([(0, 2), (2, 0)])) == '0F1FF0102'
    assert gm.relate(line([(0, 0), (2, 0)]), line([(1, 0), (3, 0)])) == '1010F0102'
    assert gm.relate(line([(0, 0), (2, 0)]), line([(1, 0), (1, 1)])) == 'F01FF0102'
    tangent = line([(0, 1), (1, 0), (2, 1)])
    assert gm.relate(line([(0, 0), (2, 0)]), tangent) == '0F1FF0102'
    assert gm.relate(line([(0, 0), (3, 0)]), line([(1, 0), (2, 0)])) == '101FF0FF2'
    assert gm.relate(line([(0, 0), (1, 0)]), line([(0, 0), (1, 0)])) == '1FFF0FFF2'
    chain = gm.MultiLineString([[(0, 0), (1, 0)], [(1, 0), (2, 0)]])
    assert gm.relate(chain, line([(1, 0), (1, 1)])) == 'F01FF0102'
    ring = line([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    assert gm.relate(ring, line([(1, 0), (2, 0)])) == 'F01FFF102'
    assert gm.touches(line([(0, 0), (1, 0)]), line([(1, 0), (2, 1)]))
    assert not gm.touches(line([(0, 0), (2, 0)]), tangent)
    assert gm.overlaps(line([(0, 0), (2, 0)]), line([(1, 0), (3, 0)]))
    assert not gm.crosses(line([(0, 0), (2, 0)]), line([(1, 0), (3, 0)]))


def test_native_mixed_relate_grades_every_line_area_contact_class() -> None:
    square = gm.box(0, 0, 4, 4)
    line = gm.LineString
    cases = [
        (line([(0, 2), (4, 2)]), '1F2F01FF2', '1FFF0F212'),
        (line([(1, 0), (3, 0)]), 'FF2101FF2', 'F1FF0F212'),
        (line([(4, 4), (6, 6)]), 'FF2F01102', 'FF1F00212'),
        (line([(1, 1), (2, 2)]), '102FF1FF2', '1FF0FF212'),
        (line([(-1, 1), (5, 1)]), '1F20F1102', '101FF0212'),
        (line([(2, 2), (6, 2)]), '1020F1102', '1010F0212'),
        (line([(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)]), 'FF21FFFF2', 'F1FFFF2F2'),
        (line([(1, 0), (3, 0), (3, -2)]), 'FF2101102', 'F11F00212'),
    ]
    for probe, area_first, line_first in cases:
        assert gm.relate(square, probe) == area_first
        assert gm.relate(probe, square) == line_first
    assert gm.touches(square, line([(4, 4), (6, 6)]))
    assert not gm.touches(square, line([(0, 2), (4, 2)]))
    assert gm.crosses(square, line([(-1, 1), (5, 1)]))
    assert not gm.crosses(square, line([(1, 0), (3, 0)]))
    assert gm.touches(square, gm.Point(4, 4)) and (
        not gm.touches(square, gm.Point(2, 2))
    )


def test_convex_container_halfplane_lane_is_exact() -> None:
    hexagon = gm.Polygon([(2, 0), (4, 1), (4, 3), (2, 4), (0, 3), (0, 1)])
    assert gm.contains(hexagon, gm.box(1, 1, 3, 3))
    assert gm.covers(hexagon, gm.box(1, 1, 3, 3))
    assert not gm.contains(hexagon, gm.box(3, 3, 5, 5))
    assert not gm.covers(hexagon, gm.box(3, 3, 5, 5))
    edge_line = gm.LineString([(0, 1), (0, 3)])
    assert gm.covers(hexagon, edge_line)
    assert not gm.contains(hexagon, edge_line)
    chord = gm.LineString([(0, 2), (4, 2)])
    assert gm.contains(hexagon, chord) and gm.covers(hexagon, chord)
    assert gm.covers(hexagon, gm.Point(2, 0)) and (
        not gm.contains(hexagon, gm.Point(2, 0))
    )
    inscribed = gm.Polygon([(2, 0), (4, 1), (4, 3), (2, 4), (0, 3), (0, 1)])
    assert gm.covers(hexagon, inscribed) and gm.contains(hexagon, inscribed)
    star = gm.Polygon([(0, 0), (4, 0), (2, 1), (4, 4), (0, 4), (2, 1.5)])
    assert not gm.contains(star, gm.box(2.5, 1.0, 3.0, 1.4))


def test_equals_identical_is_the_vectorized_value_identity() -> None:
    """`equals_identical` == the scalar `==`: frame-aware (False, never an
    error), bit-exact ordinates (-0.0 != 0.0), vertex order significant.
    """
    p = gm.Point(1, 2)
    assert gm.equals_identical(p, gm.Point(1, 2)) is True
    # frame is part of the value: differing CRS/epoch is inequality, not an error
    assert gm.equals_identical(p, gm.Point(1, 2, crs=4326)) is False
    tagged = gm.Point(1, 2, crs=4326)
    assert gm.equals_identical(tagged, tagged.set_epoch(2020.0)) is False
    # bit-pattern semantics match scalar ==
    assert gm.equals_identical(gm.Point(-0.0, 0), gm.Point(0.0, 0)) is False
    assert (gm.Point(-0.0, 0) == gm.Point(0.0, 0)) is False
    # vertex order is significant (identity, not topology)
    fwd = gm.LineString([(0, 0), (1, 1)])
    rev = gm.LineString([(1, 1), (0, 0)])
    assert gm.equals_identical(fwd, rev) is False
    assert gm.equals(fwd, rev) is True


def test_equals_identical_array_lanes() -> None:
    arr = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    tagged = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)], crs=4326)
    assert gm.equals_identical(arr, arr).tolist() == [True, True]
    assert gm.equals_identical(arr, tagged).tolist() == [False, False]
    assert gm.equals_identical(arr, gm.Point(0, 0)).tolist() == [True, False]
    assert gm.equals_identical(gm.Point(0, 0), arr).tolist() == [True, False]
    # operand-shape rules still hold on the unequal-frame path
    short = gm.GeometryArray([gm.Point(0, 0)], crs=4326)
    with pytest.raises(gm.GeometryError, match='same length'):
        gm.equals_identical(arr, short)


def test_equals_exact_array_lane_validates_frame_and_length() -> None:
    """Regression: the packed fast path used to skip both preconditions —
    cross-CRS arrays compared as equal and mismatched lengths returned
    garbage-shaped results.
    """
    left = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)], crs=4326)
    right = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)], crs=3857)
    with pytest.raises(gm.CRSMismatchError):
        gm.equals_exact(left, right)
    with pytest.raises(gm.GeometryError, match='same length'):
        gm.equals_exact(left, gm.GeometryArray([gm.Point(0, 0)], crs=4326))


def test_point_in_polygon_is_exact_at_extreme_finite_coordinates():
    """The ray-crossing overflow class: opposite-sign huge coordinates used to
    overflow the materialized intersection X to infinity, flipping ``contains``
    by edge direction (True at 1e308, False at 1e307 for the same exterior
    probe). The sign-form decision must classify scale- and
    direction-independently.
    """
    for scale in (1e307, 1e308):
        s = scale / 1e308
        shell = [(-1e308 * s, -1.0), (1e308 * s, 1.0), (1e308 * s, -1.0)]
        for ring in (shell, list(reversed(shell))):
            triangle = gm.Polygon(ring)
            # Above the long diagonal (its y at x=0 is ~0): exterior.
            assert not gm.contains(triangle, gm.Point(0.0, 0.5)), (scale, ring)
            assert not gm.intersects(triangle, gm.Point(0.0, 0.5)), (scale, ring)
            # Below the diagonal, above the bottom edge: interior.
            assert gm.contains(triangle, gm.Point(5e307 * s, 0.0)), (scale, ring)
            # contains_xy rides the same kernel.
            assert not gm.contains_xy(triangle, 0.0, 0.5)
            assert gm.contains_xy(triangle, 5e307 * s, 0.0)
