"""Constructive and overlay operations — buffer/simplify/offset, cleanup,
orientation, normalize, clip, polygonize, summaries, and Z interpolation.
"""

import itertools
import math
from typing import Any, cast

import gometry as gm
import numpy as np
import pytest


def test_ecosystem_parity_ops() -> None:
    assert gm.Point(1, 2).set_z(5.0).to_wkt() == 'POINT Z (1 2 5)'
    assert gm.Point(1, 2).set_m(3.0).to_wkt() == 'POINT M (1 2 3)'
    assert gm.Point(1, 2).force_3d().to_wkt() == 'POINT Z (1 2 0)'
    assert gm.Point(1, 2).force_3d(7.0).to_wkt() == 'POINT Z (1 2 7)'
    assert gm.from_wkt('POINT Z (1 2 9)').force_3d().to_wkt() == 'POINT Z (1 2 9)'
    assert gm.from_wkt('POINT Z (1 2 9)').set_z(4.0).to_wkt() == 'POINT Z (1 2 4)'
    assert gm.from_wkt('POINT ZM (1 2 9 7)').set_z(None).to_wkt() == 'POINT M (1 2 7)'
    assert gm.from_wkt('POINT ZM (1 2 9 7)').force_2d().to_wkt() == 'POINT (1 2)'
    assert gm.from_wkt('POINT ZM (1 2 9 7)').force_2d().to_wkt() == 'POINT (1 2)'
    assert gm.GeometryArray([gm.Point(1, 2)]).set_z(0.0)[0].has_z
    mixed_z = gm.GeometryArray([gm.from_wkt('POINT Z (1 2 9)'), gm.Point(3, 4)])
    assert mixed_z.force_3d(0.0).to_wkt() == ['POINT Z (1 2 9)', 'POINT Z (3 4 0)']
    assert gm.GeometryArray([
        gm.from_wkt('POINT ZM (1 2 9 7)')
    ]).force_2d().to_wkt() == ['POINT (1 2)']
    ring = gm.LineString([(0, 0), (1, 0), (1, 1), (0, 0)])
    assert ring.is_ccw and (not ring.reverse().is_ccw)
    assert not gm.LineString([(0, 0), (1, 1)]).is_ccw
    assert not gm.Point(0, 0).is_ccw
    np.testing.assert_array_equal(
        gm.GeometryArray([ring, ring.reverse()]).is_ccw, [True, False]
    )
    line = gm.shortest_line(gm.box(0, 0, 1, 1), gm.box(3, 0, 4, 1))
    assert line.geometry_type == 'LineString'
    assert line.length == pytest.approx(2.0)
    assert gm.equals(gm.shortest_line(gm.box(0, 0, 1, 1), gm.box(3, 0, 4, 1)), line)
    mic = gm.box(0, 0, 4, 2).maximum_inscribed_circle(tolerance=0.01)
    assert mic.geometry_type == 'Polygon'
    assert gm.box(0, 0, 4, 2).maximum_inscribed_radius(tolerance=0.01) == pytest.approx(
        1.0, abs=0.02
    )
    assert gm.box(0, 0, 4, 2).maximum_inscribed_radius() == pytest.approx(
        1.0, abs=1e-06
    )


def test_polylabel_family_on_empty_polygonal_input() -> None:
    """Empty polygonal input yields the documented empty results (never a
    fabricated ``POINT (0 0)``), for both the typed POLYGON EMPTY and the
    empty MultiPolygon container.
    """
    import math

    for wkt in ('POLYGON EMPTY', 'MULTIPOLYGON EMPTY', 'MULTIPOLYGON Z EMPTY'):
        empty = gm.from_wkt(wkt)
        assert empty.polylabel().to_wkt() == 'POINT EMPTY', wkt
        assert empty.maximum_inscribed_circle().to_wkt() == 'POLYGON EMPTY', wkt
        assert math.isnan(empty.maximum_inscribed_radius()), wkt


def test_polylabel_handles_extreme_aspect_ratio_with_scale_aware_default() -> None:
    ribbon = gm.box(0.0, 0.0, 1.0e12, 1.0)
    pole = ribbon.polylabel()
    assert (pole.x, pole.y) == pytest.approx((5.0e11, 0.5))
    assert ribbon.maximum_inscribed_radius() == pytest.approx(0.5)


def test_minimum_bounding_circle_polygon_contains_support_points() -> None:
    # Support directions intentionally fall between the 64-gon's vertices.
    points = gm.MultiPoint([
        (math.cos(0.017), math.sin(0.017)),
        (math.cos(2.111), math.sin(2.111)),
        (math.cos(4.205), math.sin(4.205)),
    ])
    circle = points.minimum_bounding_circle()
    assert all(gm.covers(circle, point) for point in gm.parts(points))


def test_cell_parity_accessors() -> None:
    h3_cell = gm.H3Cell(13.4, 52.5, resolution=9)
    assert len(h3_cell.neighbors) == 6
    assert all(neighbor.resolution == 9 for neighbor in h3_cell.neighbors)
    assert gm.H3Cell(h3_cell.token) == h3_cell
    s2_cell = gm.S2Cell(13.4, 52.5, level=12)
    assert 3000000.0 < s2_cell.area < 7000000.0
    assert 50000.0 < h3_cell.area < 150000.0


def test_simplify_vw_area_criterion_and_topology_preservation() -> None:
    line = gm.LineString([(0, 0), (1, 0.1), (2, 0), (3, 0.1), (4, 0)])
    assert list(line.simplify(1.0, method='vw').coords) == [(0.0, 0.0), (4.0, 0.0)]
    assert list(line.simplify(0.01, method='vw').coords) == list(line.coords)
    polygon = gm.Polygon([(0, 0), (10, 0), (10, 10), (5, 10.01), (0, 10)])
    simplified = polygon.simplify(1.0, method='vw')
    assert simplified.geometry_type == 'Polygon'
    assert simplified.is_valid
    assert len(simplified.exterior.coords) < len(polygon.exterior.coords)
    tricky = gm.from_wkt('POLYGON ((5 5.2, 8.2 7.7, 4.2 7, 4 0.7, 6.8 5.9, 5 5.2))')
    raw = tricky.simplify(3.0, method='vw', preserve_topology=False)
    assert raw.to_wkt() == 'POLYGON EMPTY'
    safe = tricky.simplify(3.0, method='vw')
    assert safe.is_valid and (not safe.is_empty)
    assert set(safe.coords) <= set(tricky.coords)
    array = gm.GeometryArray([line], crs=4326)
    by_array = array.simplify(1.0, method='vw')
    by_free = cast('gm.GeometryArray', array.simplify(1.0, method='vw'))
    assert gm.equals(by_array[0], by_free[0])
    assert by_array.crs == 'EPSG:4326'
    lifted = gm.LineString([(0, 0), (1, 0.1), (2, 0)], z=[5, 6, 7])
    assert list(lifted.simplify(5.0, method='vw').coords) == [
        (0.0, 0.0, 5.0),
        (2.0, 0.0, 7.0),
    ]
    with pytest.raises(gm.GeometryError, match='non-negative'):
        line.simplify(-1.0, method='vw')


def test_simplify_vw_packed_lines_matches_per_row() -> None:
    """Packed Lines take the columnar VW lane when topology is off."""
    lines = gm.GeometryArray(
        [
            gm.LineString([(0, 0), (1, 0.1), (2, 0), (3, 0.1), (4, 0)]),
            gm.LineString([(0, 0), (2, 0.2), (4, 0)]),
        ],
        crs=4326,
    )
    packed = lines.simplify(1.0, method='vw', preserve_topology=False)
    scalar = gm.GeometryArray(
        [geom.simplify(1.0, method='vw', preserve_topology=False) for geom in lines],
        crs=4326,
    )
    assert packed.to_wkt() == scalar.to_wkt()
    assert packed.to_arrow().type.extension_name == 'geoarrow.linestring'


def test_buffer_side_builds_one_sided_strips() -> None:
    line = gm.LineString([(0, 0), (2, 0)])
    left = line.buffer(1.0, side='left')
    right = line.buffer(1.0, side='right')
    assert left.bounds == (0.0, 0.0, 2.0, 1.0)
    assert right.bounds == (0.0, -1.0, 2.0, 0.0)
    assert abs(left.area - 2.0) < 1e-12
    assert abs(right.area - 2.0) < 1e-12
    both_bounds = line.buffer(1.0).bounds
    assert both_bounds is not None and both_bounds[1] < -0.99
    bend = gm.LineString([(0, 0), (1, 0), (1, 1), (0, 1)])
    strip = bend.buffer(0.3, side='left')
    assert strip.geometry_type == 'Polygon'
    assert strip.is_valid
    array = gm.GeometryArray([line], crs=32634)
    assert gm.equals(array.buffer(1.0, side='left')[0], left.set_crs(32634))
    assert gm.equals(cast('gm.Geometry', line.buffer(1.0, side='right')), right)
    geodesic = gm.LineString([(13.0, 52.0), (13.01, 52.0)], crs=4326)
    sided = geodesic.buffer(100.0, side='left')
    assert sided.geometry_type == 'Polygon'
    assert 60000 < sided.area < 80000
    zigzag = gm.LineString([(0, 0), (2, 2), (0, 2), (2, 0)])
    swept = zigzag.buffer(0.2, side='left')
    assert swept.is_valid and (not swept.is_empty)
    assert 0.566 < swept.area <= zigzag.length * 0.2
    square = gm.LineString([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    assert abs(square.buffer(0.2, side='left').area - 0.64) < 1e-06
    assert abs(square.buffer(0.2, side='right').area - 0.96) < 1e-06
    spike = gm.LineString([(0, 0), (2, 0), (0.001, 1e-06), (2, 2e-06)])
    for side in ('left', 'right'):
        strip2 = spike.buffer(0.2, side=side)
        spike_bounds = strip2.bounds
        assert spike_bounds is not None
        assert spike_bounds[0] > -1.5 and spike_bounds[2] < 3.5
        assert strip2.area < 2.0
    with pytest.raises(TypeError, match='lineal'):
        gm.box(0, 0, 1, 1).buffer(1.0, side='left')
    with pytest.raises(gm.GeometryError, match='non-negative'):
        line.buffer(-1.0, side='right')


def _circle_center_radius(circle):
    """Recover the (cx, cy, radius) of a bounding-circle Polygon from its
    64-gon vertices (centroid + maximum polygon-vertex distance). The polygon
    is circumscribed, so this is intentionally larger than the exact radius
    returned by ``minimum_bounding_radius``.
    """
    verts = list(circle.coords)[:-1]
    cx = sum((x for x, _ in verts)) / len(verts)
    cy = sum((y for _, y in verts)) / len(verts)
    radius = max((math.hypot(x - cx, y - cy) for x, y in verts))
    return (cx, cy, radius)


def test_minimum_bounding_circle_is_smallest_enclosing() -> None:
    circle = gm.box(0, 0, 2, 2).minimum_bounding_circle()
    assert circle.geometry_type == 'Polygon'
    cx, cy, radius = _circle_center_radius(circle)
    assert (round(cx, 9), round(cy, 9)) == (1.0, 1.0)
    assert gm.box(0, 0, 2, 2).minimum_bounding_radius() == pytest.approx(math.sqrt(2))
    assert radius == pytest.approx(math.sqrt(2) / math.cos(math.pi / 64.0))
    points = gm.MultiPoint([(0, 0), (5, 1), (2, 4), (1, 1), (3, 3)])
    cx, cy, radius = _circle_center_radius(points.minimum_bounding_circle())
    for x, y in [(0, 0), (5, 1), (2, 4), (1, 1), (3, 3)]:
        assert math.hypot(x - cx, y - cy) <= radius * (1 + 1e-09)
    assert gm.Point(3, 4).minimum_bounding_circle().to_wkt() == 'POINT (3 4)'
    assert (
        gm.from_wkt('MULTIPOINT ((1 1), (1 1))').minimum_bounding_circle().to_wkt()
        == 'POINT (1 1)'
    )
    assert gm.LineString([]).minimum_bounding_circle().to_wkt() == 'POLYGON EMPTY'
    _, _, r_col = _circle_center_radius(
        gm.LineString([(0, 0), (1, 0), (4, 0)]).minimum_bounding_circle()
    )
    assert r_col == pytest.approx(2.0 / math.cos(math.pi / 64.0))
    assert gm.LineString([
        (0, 0),
        (1, 0),
        (4, 0),
    ]).minimum_bounding_radius() == pytest.approx(2.0)
    assert math.isnan(gm.LineString([]).minimum_bounding_radius())
    shapely = pytest.importorskip('shapely')
    array = gm.GeometryArray([points], crs=3857)
    assert array.minimum_bounding_circle().crs == 'EPSG:3857'
    assert array.minimum_bounding_circle()[0].geometry_type == 'Polygon'
    np.testing.assert_allclose(
        array.minimum_bounding_radius(), [points.minimum_bounding_radius()]
    )
    free = cast('gm.Geometry', points.minimum_bounding_circle())
    assert free.area == pytest.approx(
        shapely.minimum_bounding_circle(shapely.from_wkt(points.to_wkt())).area,
        rel=0.02,
    )
    lifted = gm.Point(0, 0, z=1)
    assert lifted.minimum_bounding_circle().to_wkt() == 'POINT (0 0)'
    _, _, r_huge = _circle_center_radius(
        gm.MultiPoint([(1e155, 0), (0, 1e155), (-1e155, 0)]).minimum_bounding_circle()
    )
    assert r_huge == pytest.approx(1e155 / math.cos(math.pi / 64.0), rel=1e-10)


def test_minimum_bounding_radius_is_crs_aware_with_planar_escape() -> None:
    # The bounding circle is an approximate bounding primitive, usable at
    # city-to-continental extent: a 1° span builds a valid geodesic circle
    # (it does NOT go through buffer/offset's scale-error fit check).
    points = gm.MultiPoint([(0, 0), (0, 1)], crs=4326)
    radius = points.minimum_bounding_radius()
    assert radius == pytest.approx(
        gm.distance(gm.Point(0, 0, crs=4326), gm.Point(0, 1, crs=4326)) / 2.0
    )
    assert points.minimum_bounding_radius(unit='planar') == pytest.approx(0.5)
    np.testing.assert_allclose(
        gm.GeometryArray([points]).minimum_bounding_radius(),
        [radius],
    )
    circle = points.minimum_bounding_circle()
    assert circle.crs == 'EPSG:4326'
    assert circle.geometry_type == 'Polygon'
    polygon_radius = 0.5 / math.cos(math.pi / 64.0)
    assert points.minimum_bounding_circle(unit='planar').bounds == pytest.approx((
        -polygon_radius,
        0.5 - polygon_radius,
        polygon_radius,
        0.5 + polygon_radius,
    ))


def test_geographic_concave_hull_preserves_selected_input_vertices_exactly() -> None:
    line = gm.LineString(
        [
            (13.123456789012344, 52.123456789012344),
            (13.123456789012344, 52.123456789012344),
            (13.223456789012343, 52.123456789012344),
            (13.123456789012344, 52.22345678901234),
        ],
        crs=4326,
    )
    hull = line.concave_hull(length_threshold=100000.0)
    input_vertices = {
        (13.123456789012344, 52.123456789012344),
        (13.223456789012343, 52.123456789012344),
        (13.123456789012344, 52.22345678901234),
    }
    selected = [(coord[0], coord[1]) for coord in hull.coords]
    assert selected[0] == selected[-1]
    assert set(selected[:-1]) == input_vertices
    assert all(vertex in input_vertices for vertex in selected)


def test_envelope_and_mrr_empty_are_typed() -> None:
    for wkt in (
        'POLYGON EMPTY',
        'LINESTRING EMPTY',
        'POINT EMPTY',
        'GEOMETRYCOLLECTION EMPTY',
    ):
        assert gm.from_wkt(wkt).envelope().to_wkt() == 'POLYGON EMPTY'
        assert gm.from_wkt(wkt).minimum_rotated_rectangle().to_wkt() == 'POLYGON EMPTY'
    arr = gm.GeometryArray([gm.from_wkt('POLYGON EMPTY'), gm.box(0, 0, 2, 2)])
    assert arr.envelope()[0].to_wkt() == 'POLYGON EMPTY'
    assert arr.envelope()[1].geometry_type == 'Polygon'
    assert arr.minimum_rotated_rectangle()[0].to_wkt() == 'POLYGON EMPTY'


def test_simplify_collapse_drops_degenerate_rings_not_invalid() -> None:
    sliver = 'POLYGON ((0 0, 4 0, 4 0.01, 0 0))'
    for method in ('dp', 'vw'):
        out = gm.from_wkt(sliver).simplify(1.0, method=method, preserve_topology=False)
        assert out.to_wkt() == 'POLYGON EMPTY', method
        assert out.is_valid, method
    holed = 'POLYGON ((0 0, 8 0, 8 8, 0 8, 0 0), (1 1, 7 1, 7 1.001, 1 1))'
    cleaned = gm.from_wkt(holed).simplify(1.0, preserve_topology=False)
    assert cleaned.is_valid
    assert cleaned.area == pytest.approx(64)
    assert gm.from_wkt(sliver).simplify(1.0, preserve_topology=True).is_valid
    chunky = 'POLYGON ((0 0, 10 0, 10 10, 5 5, 0 10, 0 0))'
    simple = gm.from_wkt(chunky).simplify(0.5, preserve_topology=False)
    assert simple.geometry_type == 'Polygon' and simple.is_valid
    for tol in (0.001, 0.1, 1.0, 5.0, 100.0):
        result = gm.from_wkt(sliver).simplify(tol, preserve_topology=False)
        assert result.is_valid, tol


def test_subdivide_bounds_complexity_and_covers_input() -> None:
    ring = gm.Point(0, 0, crs=4326).buffer(10, quadrant_segments=64)
    parts = ring.subdivide(max_vertices=64)
    assert len(parts) > 1
    assert all(len(cast('gm.Polygon', part).exterior.coords) <= 64 for part in parts)
    assert parts.crs == 'EPSG:4326'
    union = parts.union_all()
    assert abs(union.area - ring.area) <= 1e-06 * ring.area
    assert len(gm.box(0, 0, 1, 1).subdivide()) == 1
    array = gm.GeometryArray([ring, gm.box(0, 0, 1, 1, crs=4326)])
    # Array subdivide is per-row Groups: one group of parts per input geometry.
    groups = array.subdivide(max_vertices=64)
    assert isinstance(groups, gm.Groups) and len(groups) == 2
    assert len(groups[0]) == len(parts)
    assert len(groups[1]) == 1
    assert len(ring.subdivide(max_vertices=64)) == len(parts)
    dense = gm.Point(0, 0).buffer(10, quadrant_segments=128)
    tight = dense.subdivide(max_vertices=16)
    assert max(len(part.coords) for part in tight) <= 16
    assert abs(sum(part.area for part in tight) - dense.area) <= 1e-06 * dense.area
    skewed = gm.LineString(
        [(1000000000.0, 0.0)] + [(i * 1e-09, float(i % 2)) for i in range(8)]
    )
    parts2 = skewed.subdivide(max_vertices=8)
    assert len(parts2) <= 8
    assert all(len(part.coords) <= 8 for part in parts2)
    with pytest.raises(gm.GeometryError, match='max_vertices must be >= 8'):
        ring.subdivide(max_vertices=4)


def test_subdivide_parts_preserves_coordinate_epoch() -> None:
    polygon = gm.box(0, 0, 10, 10, crs=3857).set_epoch(2020.0)
    parts = polygon.subdivide(max_vertices=8)
    assert parts.epoch == 2020.0
    assert all(part.epoch == 2020.0 for part in parts)


def test_dissolve_unions_per_group_in_first_occurrence_order() -> None:
    array = gm.GeometryArray([
        gm.box(0, 0, 1, 1),
        gm.box(1, 0, 2, 1),
        gm.box(5, 5, 6, 6),
        gm.box(0.5, 0.5, 1.5, 1.5),
    ])
    result = array.dissolve(by=['a', 'a', 'b', 'a'])
    assert isinstance(result, tuple)
    geometries, groups = result
    assert groups == ['a', 'b']
    assert geometries[0].geometry_type == 'Polygon'
    assert abs(geometries[0].area - 2.5) < 1e-09
    assert gm.equals(geometries[1], gm.box(5, 5, 6, 6))
    tagged = gm.GeometryArray([gm.box(0, 0, 1, 1, crs=4326)], crs=4326)
    assert tagged.dissolve(by=[0])[0].crs == 'EPSG:4326'
    _, keyed_groups = array.dissolve(by=[(2, 1), 0, (2, 1), 0])
    assert keyed_groups == [(2, 1), 0]
    with pytest.raises(gm.GeometryError, match='one key per geometry'):
        array.dissolve(by=['a', 'a'])

    dissolved = array.union_all()
    assert isinstance(dissolved, gm.Geometry)
    assert dissolved.area == pytest.approx(3.5)


def test_dissolve_preserves_hashable_and_unhashable_key_grouping_order() -> None:
    class UnhashableAlias:
        __hash__ = None  # type: ignore[assignment]

        def __init__(self, value: str) -> None:
            self.value = value

        def __eq__(self, other: object) -> bool:
            if isinstance(other, UnhashableAlias):
                return self.value == other.value
            return other == self.value

    first_b = UnhashableAlias('b')
    array = gm.GeometryArray([gm.box(i, 0, i + 1, 1) for i in range(6)])
    geometries, groups = array.dissolve(
        by=['a', UnhashableAlias('a'), first_b, 'b', ('tuple'), ('tuple')]
    )

    assert groups[0] == 'a'
    assert groups[1] is first_b
    assert groups[1] == 'b'
    assert groups[2] == ('tuple')
    np.testing.assert_allclose([geom.area for geom in geometries], [2.0, 2.0, 2.0])

    class BrokenEquality:
        __hash__ = None

        def __eq__(self, other: object) -> bool:
            raise RuntimeError('group comparison failed')

    with pytest.raises(RuntimeError, match='group comparison failed'):
        array[:2].dissolve(by=[BrokenEquality(), BrokenEquality()])


def test_swap_xy_exchanges_axes_and_keeps_zm() -> None:
    assert gm.Point(52.0, 21.0).swap_xy().to_wkt() == 'POINT (21 52)'
    swapped = gm.LineString([(1, 2), (3, 4)], z=[9, 8], m=[1, 2]).swap_xy()
    assert swapped.to_wkt() == 'LINESTRING ZM (2 1 9 1, 4 3 8 2)'
    array = gm.points([1.0, 2.0], [3.0, 4.0], crs=3857).swap_xy()
    assert array.crs == 'EPSG:3857'
    assert (array[0].x, array[0].y) == (3.0, 1.0)
    assert gm.equals(array.swap_xy()[1], gm.Point(2.0, 4.0, crs=3857))
    assert gm.box(0, 1, 2, 3).swap_xy().bounds == (1.0, 0.0, 3.0, 2.0)
    assert gm.from_wkt('POINT EMPTY').swap_xy().is_empty


def test_snap_to_grid_snaps_dedups_and_collapses() -> None:
    line = gm.LineString(
        [(0.2, 0.1), (0.4, 0.3), (1.3, 0.9)], z=[1, 2, 3], m=[10, 20, 30]
    )
    assert line.snap_to_grid(1).to_wkt() == 'LINESTRING ZM (0 0 1 10, 1 1 3 30)'
    assert len(line.quantize(0).coords) == 3
    assert gm.equals(gm.Point(0.5, -0.5).snap_to_grid(1), gm.Point(1, -1))
    assert gm.equals(
        gm.Point(0.7, 0.7).snap_to_grid(1, origin=(0.5, 0.5)), gm.Point(0.5, 0.5)
    )
    assert gm.equals(gm.Point(1.4, 1.4).snap_to_grid((1.0, 0.5)), gm.Point(1, 1.5))
    assert gm.equals(
        gm.Point(1.4, 1.4).snap_to_grid(iter([1.0, 0.5]), origin=[0, 0]),
        gm.Point(1, 1.5),
    )
    assert gm.LineString([(0.1, 0.1), (0.2, 0.2)]).snap_to_grid(1).is_empty
    assert gm.box(0.1, 0.1, 0.4, 0.4).snap_to_grid(1).to_wkt() == 'POLYGON EMPTY'
    multi = gm.from_wkt('MULTILINESTRING ((0.1 0.1, 0.2 0.2), (0 0, 3.4 0.1))')
    assert multi.snap_to_grid(1).to_wkt() == 'MULTILINESTRING ((0 0, 3 0))'
    collection = gm.from_wkt(
        'GEOMETRYCOLLECTION (POINT (1.2 1.2), LINESTRING (0.1 0.1, 0.2 0.2))'
    )
    assert collection.snap_to_grid(1).to_wkt() == 'GEOMETRYCOLLECTION (POINT (1 1))'
    snapped_box = gm.box(0.1, 0.1, 0.9, 1.9).snap_to_grid(1)
    assert snapped_box.is_valid
    assert snapped_box.area == 2.0
    assert gm.equals(gm.Point(0.2, 0.1).snap_to_grid(1), gm.Point(0, 0))
    array = gm.GeometryArray([gm.Point(0.2, 0.1, crs=4326)]).snap_to_grid(1)
    assert array.crs == 'EPSG:4326'
    assert gm.equals(array[0], gm.Point(0, 0, crs=4326))
    with pytest.raises(gm.GeometryError, match='positive finite'):
        gm.Point(0, 0).snap_to_grid(0)
    with pytest.raises(gm.GeometryError, match='positive finite'):
        gm.Point(0, 0).snap_to_grid((1.0, -2.0))
    with pytest.raises(gm.GeometryError, match='got 3 values'):
        gm.Point(0, 0).snap_to_grid((1.0, 1.0, 1.0))
    with pytest.raises(TypeError, match='positive number or an'):
        gm.Point(0, 0).snap_to_grid(cast('Any', 'nope'))
    with pytest.raises(gm.GeometryError, match='origin must be finite'):
        gm.Point(0, 0).snap_to_grid(1, origin=(float('nan'), 0.0))
    with pytest.raises(TypeError, match='origin must be a finite'):
        gm.Point(0, 0).snap_to_grid(1, origin=cast('Any', 7))
    with pytest.raises(gm.GeometryError, match='too fine'):
        gm.Point(1e308, 0).snap_to_grid(1e-308)
    with pytest.raises(gm.GeometryError, match='too fine'):
        gm.Point(1e308, 0).snap_to_grid(1, origin=(-1e308, 0))


def test_sample_points_is_deterministic_and_area_weighted() -> None:
    box = gm.box(0, 0, 1, 1, crs=3857)
    first = box.sample_points(100, seed=7)
    again = box.sample_points(100, seed=7)
    assert first.crs == 'EPSG:3857'
    assert len(first) == 100
    assert [(p.x, p.y) for p in first] == [(p.x, p.y) for p in again]
    assert all(gm.within(p, box) for p in first)
    assert [(p.x, p.y) for p in box.sample_points(100, seed=8)] != [
        (p.x, p.y) for p in first
    ]
    parts = gm.from_wkt(
        'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 0, 5 0, 5 3, 2 3, 2 0)))'
    )
    big = gm.box(2, 0, 5, 3)
    inside_big = sum(gm.within(p, big) for p in parts.sample_points(200, seed=1))
    assert 160 <= inside_big <= 199
    donut = gm.from_wkt(
        'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 3 1, 3 3, 1 3, 1 1))'
    )
    hole = gm.box(1, 1, 3, 3)
    samples = donut.sample_points(300, seed=3)
    assert all(gm.within(p, donut) for p in samples)
    assert not any(gm.within(p, hole) for p in samples)
    assert len(box.sample_points(0, seed=1)) == 0
    assert len(gm.from_wkt('POLYGON EMPTY').sample_points(0, seed=1)) == 0
    line = gm.LineString([(0, 0), (10, 0), (10, 5)])
    on_line = line.sample_points(50, seed=4)
    assert len(on_line) == 50
    assert all(gm.distance(line, p) < 1e-09 for p in on_line)
    assert [(p.x, p.y) for p in on_line] == [
        (p.x, p.y) for p in line.sample_points(50, seed=4)
    ]
    first_leg = sum(p.x < 10 or (p.x == 10 and p.y == 0) for p in on_line)
    assert 25 <= first_leg <= 45
    multi = gm.from_wkt('MULTIPOINT ((0 0), (1 1), (2 2))')
    atoms = {(p.x, p.y) for p in multi.sample_points(60, seed=5)}
    assert atoms == {(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)}
    assert [(p.x, p.y) for p in gm.Point(7, 8).sample_points(3, seed=1)] == [
        (7.0, 8.0)
    ] * 3
    # Degenerate area-zero triangle (closed, ≥4 verts) samples its boundary.
    sliver = gm.from_wkt('POLYGON ((0 0, 2 0, 1 0, 0 0))')
    boundary_samples = sliver.sample_points(10, seed=6)
    assert all(0 <= p.x <= 2 and p.y == 0 for p in boundary_samples)
    titan = gm.box(0, 0, 1e155, 1e155)
    assert all(gm.covers(titan, p) for p in titan.sample_points(5, seed=1))
    long_line = gm.LineString([(0, 0), (1e308, 0)])
    assert all(p.y == 0 for p in long_line.sample_points(4, seed=2))
    rows = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(0, 0, 1, 1)])
    per_row = rows.sample_points(5, seed=7)
    assert isinstance(per_row, gm.Groups)
    assert [(p.x, p.y) for p in per_row[0]] != [(p.x, p.y) for p in per_row[1]]
    assert [(p.x, p.y) for p in rows.sample_points(5, seed=7)[0]] == [
        (p.x, p.y) for p in per_row[0]
    ]
    varied = rows.sample_points([2, 3], seed=[11, 12])
    assert [len(group) for group in varied] == [2, 3]
    assert [(p.x, p.y) for p in varied[0]] == [
        (p.x, p.y) for p in rows[0].sample_points(2, seed=11)
    ]
    assert [(p.x, p.y) for p in varied[1]] == [
        (p.x, p.y) for p in rows[1].sample_points(3, seed=12)
    ]
    assert len(gm.box(0, 0, 1, 1).sample_points(9, seed=2)) == 9


def test_sample_points_error_gates() -> None:
    box = gm.box(0, 0, 1, 1)
    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        gm.from_wkt('POLYGON EMPTY').sample_points(3, seed=1)
    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        gm.from_wkt('POINT EMPTY').sample_points(3, seed=1)
    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        gm.from_wkt('GEOMETRYCOLLECTION EMPTY').sample_points(3, seed=1)
    with pytest.raises(gm.GeometryError, match='count must be >= 0'):
        box.sample_points(-1, seed=1)
    with pytest.raises(TypeError, match='seed'):
        box.sample_points(5)
    with pytest.raises(TypeError, match='count must be an integer'):
        box.sample_points(cast('Any', 2.5), seed=1)
    rows = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.from_wkt('POLYGON EMPTY')])
    with pytest.raises(gm.GeometryError, match='length-2'):
        rows.sample_points([1], seed=1)
    # An empty ROW degrades to an empty group — one bad row never fails the
    # batch. The scalar surface (asserted above) still raises.
    assert [len(group) for group in rows.sample_points(3, seed=1)] == [3, 0]


def test_vertex_inventing_ops_have_fixed_2d_results() -> None:
    box_z = gm.from_wkt('POLYGON Z ((0 0 9, 1 0 9, 1 1 9, 0 1 9, 0 0 9))')
    assert all(
        point.coordinate_axes == 'XY' for point in box_z.sample_points(3, seed=1)
    )
    assert len(gm.box(0, 0, 1, 1).sample_points(3, seed=1)) == 3
    z_lines = gm.from_wkt(
        'MULTILINESTRING Z ((0 0 5, 1 0 5), (1 0 5, 1 1 5), (1 1 5, 0 0 5))'
    )
    carried = z_lines.polygonize()
    assert len(carried) == 1
    assert carried[0].coordinate_axes == 'XYZ'
    assert carried[0].force_2d().coordinate_axes == 'XY'
    z_with_dangle = gm.from_wkt(
        'MULTILINESTRING Z ((0 0 5, 1 0 5), (1 0 5, 1 1 5), (1 1 5, 0 0 5), (2 2 5, 3 3 5))'
    )
    polygons, _cuts, dangles, _invalid = gm.polygonize_full([z_with_dangle])
    assert polygons[0].coordinate_axes == 'XYZ'
    assert dangles and all(d.coordinate_axes == 'XYZ' for d in dangles)
    bowtie_z = gm.from_wkt('POLYGON Z ((0 0 1, 2 2 5, 2 0 3, 0 2 7, 0 0 1))')
    assert bowtie_z.repair().coordinate_axes == 'XYZ'
    assert bowtie_z.repair().force_2d().coordinate_axes == 'XY'


def test_normalize_is_the_smallest_presentation() -> None:
    """The canonical form is the lexicographically smallest equivalent
    presentation — one comparator (pointwise, then shorter-first), parts
    ascending, lines by smaller direction, closed lines by smallest
    rotation, polygon rings min-vertex-first with RFC 7946 winding.
    Deliberately NOT GEOS's normalized form (descending order, CW shells).
    """
    cases = [
        ('MULTIPOINT ((2 2), (0 0), (1 1))', 'MULTIPOINT ((0 0), (1 1), (2 2))'),
        (
            'MULTILINESTRING ((3 3, 4 4), (1 1, 0 0))',
            'MULTILINESTRING ((0 0, 1 1), (3 3, 4 4))',
        ),
        ('LINESTRING (9 9, 0 0)', 'LINESTRING (0 0, 9 9)'),
        ('LINESTRING (5 1, 3 7, 0 0, 5 1)', 'LINESTRING (0 0, 3 7, 5 1, 0 0)'),
        (
            'POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0), (1 1, 1.5 1, 1.5 1.5, 1 1))',
            'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0), (1 1, 1.5 1.5, 1.5 1, 1 1))',
        ),
        (
            'GEOMETRYCOLLECTION (LINESTRING (0 0, 1 1), POINT (5 5), POINT (1 2))',
            'GEOMETRYCOLLECTION (POINT (1 2), POINT (5 5), LINESTRING (0 0, 1 1))',
        ),
    ]
    for source, canon in cases:
        assert gm.from_wkt(source).normalize().to_wkt() == canon, source
    cycle = [(0, 0), (5, 1), (3, 7), (1, 4)]
    forms = set()
    for start in range(len(cycle)):
        rotated = cycle[start:] + cycle[:start]
        for pts in (rotated, rotated[::-1]):
            normalized = gm.LineString([*pts, pts[0]]).normalize()
            forms.add(normalized.to_wkt())
            assert gm.equals_exact(normalized.normalize(), normalized)
    assert len(forms) == 1
    permutations = itertools.permutations([(0, 0), (2, 1), (1, 2)])
    assert len({gm.MultiPoint(list(p)).normalize().to_wkt() for p in permutations}) == 1
    tricky = gm.from_wkt(
        'POLYGON ((2 0, 0 0, 0 2, 2 2, 2 0), (1 1, 1.5 1.5, 1 1.5, 1 1))'
    )
    assert gm.equals(tricky.normalize(), tricky)


def test_simplify_delta_guard_catches_hole_escape() -> None:
    shell = [(0, 0), (20, 0), (20, 20), (0, 20)]
    hole = [(2, 2), (18, 2), (18, 4), (3, 4), (3, 16), (18, 16), (18, 18), (2, 18)]
    polygon = gm.Polygon(shell, holes=[hole])
    assert polygon.is_valid
    raw = polygon.simplify(12.0, preserve_topology=False)
    assert raw.is_valid and len(raw.interiors) == 0
    safe = polygon.simplify(12.0)
    assert safe.is_valid and len(safe.interiors) == 1


def test_build_area_and_node_surfaces_agree() -> None:
    square_edges = gm.MultiLineString(
        [[(0, 0), (1, 0)], [(1, 0), (1, 1)], [(1, 1), (0, 1)], [(0, 1), (0, 0)]],
        crs=3857,
    )
    built = square_edges.build_area()
    top_level = square_edges.build_area()
    array_built = gm.GeometryArray([square_edges]).build_area()[0]
    assert built.geometry_type == 'Polygon'
    assert built.area == pytest.approx(1.0)
    assert built.crs == 'EPSG:3857'
    assert top_level.to_wkt() == built.to_wkt()
    assert array_built.to_wkt() == built.to_wkt()
    hole_edges = gm.GeometryCollection(
        [
            gm.LineString([(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)]),
            gm.LineString([(1, 1), (1, 3), (3, 3), (3, 1), (1, 1)]),
        ],
        crs=3857,
    )
    with_hole = cast('gm.Polygon', hole_edges.build_area())
    assert with_hole.geometry_type == 'Polygon'
    assert len(with_hole.interiors) == 1
    assert with_hole.area == pytest.approx(12.0)
    open_line = gm.LineString([(0, 0), (1, 0)])
    assert open_line.build_area().is_empty
    assert open_line.build_area().geometry_type == 'Polygon'
    crossing = gm.MultiLineString([[(0, 0), (2, 0)], [(1, -1), (1, 1)]], crs=3857)
    noded = crossing.node()
    top_noded = crossing.node()
    array_noded = gm.GeometryArray([crossing]).node()[0]
    assert noded.geometry_type == 'MultiLineString'
    assert noded.num_geometries == 4
    assert noded.crs == 'EPSG:3857'
    assert any(coord[:2] == (1.0, 0.0) for line in noded.parts for coord in line.coords)
    assert top_noded.to_wkt() == noded.to_wkt()
    assert array_noded.to_wkt() == noded.to_wkt()
    z_crossing = gm.from_wkt(
        'MULTILINESTRING Z ((0 0 5, 2 0 5), (1 -1 5, 1 1 5))', crs=3857
    )
    assert z_crossing.node().coordinate_axes == 'XYZ'


def test_w4b_smooth_catmull_and_chaikin_bit_stable_and_budget() -> None:
    """W4B-topology: Catmull-Rom knot prep + SoA emission stay bit-stable;
    budget still rejects pathological iterations before allocation.
    """
    line = gm.LineString([(float(i), (i % 7) * 0.1) for i in range(32)])
    for method in ('chaikin', 'catmull_rom'):
        for iterations in (1, 2, 3):
            a = line.smooth(iterations=iterations, method=method)
            b = line.smooth(iterations=iterations, method=method)
            assert a.to_wkt() == b.to_wkt(), (method, iterations)
            assert a.num_coordinates > line.num_coordinates
    # Closed polygon ring path.
    poly = gm.box(0, 0, 10, 10)
    smoothed = poly.smooth(iterations=2, method='chaikin')
    assert smoothed.geometry_type == 'Polygon'
    assert smoothed.is_valid
    # Budget gate still rejects blow-ups.
    with pytest.raises(gm.GeometryError, match=r'iterations|too large|budget|reduce'):
        line.smooth(iterations=40, method='catmull_rom')
