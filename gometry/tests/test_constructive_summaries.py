"""Constructive and overlay operations — buffer/simplify/offset, cleanup,
orientation, normalize, clip, polygonize, summaries, and Z interpolation.
"""

import itertools
from typing import Any, cast

import gometry as gm
import numpy as np
import pytest


def test_constructive_summary_geometries_are_rust_backed() -> None:
    polygon = gm.Polygon([(0, 0), (4, 0), (4, 2), (0, 2), (0, 0)], crs=3857)
    polygon_with_hole = gm.Polygon(
        [(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)],
        holes=[[(0.75, 0.75), (1.25, 0.75), (1.25, 1.25), (0.75, 1.25), (0.75, 0.75)]],
        crs=3857,
    )
    concave_polygon = gm.Polygon(
        [(0, 0), (3, 0), (3, 1), (1, 1), (1, 3), (0, 3), (0, 0)], crs=3857
    )
    line = gm.LineString([(0, 0), (2, 0)], crs=3857)
    triangulation_points = gm.MultiPoint([(0, 0), (1, 0), (0, 1), (1, 1)], crs=3857)
    concave_points = gm.MultiPoint(
        [(0, 0), (4, 0), (4, 4), (2, 1), (0, 4), (1, 2), (3, 2)], crs=3857
    )
    rotated = gm.Polygon([
        (0.0, 0.0),
        (2**0.5, 2**0.5),
        (2**0.5 / 2, 3 * 2**0.5 / 2),
        (-(2**0.5) / 2, 2**0.5 / 2),
        (0.0, 0.0),
    ])
    array = gm.GeometryArray([polygon, line])
    centroids = cast('gm.GeometryArray', array.centroid())
    surfaces = cast('gm.GeometryArray', array.point_on_surface())
    point_envelope = cast('gm.Geometry', gm.Point(1, 2).envelope())
    line_envelope = cast('gm.Geometry', line.envelope())
    rotated_rectangles = cast('gm.GeometryArray', array.minimum_rotated_rectangle())
    boundaries = cast('gm.GeometryArray', array.boundary())
    point_hull = cast(
        'gm.Geometry', gm.MultiPoint([(0, 0), (1, 1), (2, 0), (1, 0)]).convex_hull()
    )
    concave_hull = concave_points.concave_hull(concavity=1.0)
    concave_array = gm.GeometryArray([concave_points]).concave_hull(concavity=1.0)
    label = polygon.polylabel(tolerance=0.01)
    label_array = gm.GeometryArray([polygon]).polylabel(tolerance=0.01)
    triangles = triangulation_points.triangulate(method='delaunay')
    constrained_triangles = polygon_with_hole.triangulate(method='constrained')
    polygon_triangles = concave_polygon.triangulate(method='earcut')
    holed_polygon_triangles = polygon_with_hole.triangulate(method='earcut')
    top_level_triangles = triangulation_points.triangulate(method='delaunay')
    top_level_constrained = polygon_with_hole.triangulate(method='constrained')
    top_level_polygon_triangles = concave_polygon.triangulate(method='earcut')
    array_triangles = gm.GeometryArray([
        triangulation_points,
        gm.MultiPoint([(0, 0), (1, 0)], crs=3857),
    ]).triangulate(method='delaunay')
    array_constrained = gm.GeometryArray([polygon_with_hole]).triangulate(
        method='constrained'
    )
    array_polygon_triangles = gm.GeometryArray([
        concave_polygon,
        polygon_with_hole,
    ]).triangulate(method='earcut')
    voronoi_cells = triangulation_points.voronoi_polygons(clip='envelope')
    voronoi_edges = triangulation_points.voronoi_edges(clip='envelope')
    clipped_voronoi_cells = triangulation_points.voronoi_polygons(
        clip=gm.box(-1, -1, 2, 2, crs=3857)
    )
    assert polygon.centroid().to_wkt() == 'POINT (2 1)'
    assert centroids[0].to_wkt() == 'POINT (2 1)'
    assert gm.covered_by(polygon.point_on_surface(), polygon)
    assert gm.covered_by(surfaces[1], line)
    assert gm.equals_exact(polygon.envelope(), polygon)
    assert point_envelope.to_wkt() == 'POINT (1 2)'
    assert line_envelope.to_wkt() == 'LINESTRING (0 0, 2 0)'
    signed_zero_envelope = cast(
        'gm.Geometry', gm.MultiPoint([(-0.0, 0.0), (0.0, -0.0)]).envelope()
    )
    assert signed_zero_envelope.geometry_type == 'Point'
    assert gm.equals(signed_zero_envelope, gm.Point(0.0, 0.0))
    assert gm.equals(polygon.convex_hull(), polygon)
    assert point_hull.area == 1
    assert concave_hull.geometry_type == 'Polygon'
    assert concave_hull.crs == 'EPSG:3857'
    assert 0 < concave_hull.area < concave_points.convex_hull().area
    assert all(
        (
            gm.covers(concave_hull, gm.Point(x, y, crs=3857))
            for x, y in [(0, 0), (4, 0), (4, 4), (2, 1), (0, 4), (1, 2), (3, 2)]
        )
    )
    assert gm.equals_exact(
        cast('gm.Geometry', concave_points.concave_hull(concavity=1.0)), concave_hull
    )
    assert gm.equals_exact(concave_array[0], concave_hull)
    with pytest.raises(ValueError, match='non-negative finite'):
        concave_points.concave_hull(concavity=-1)
    assert label.geometry_type == 'Point'
    assert label.crs == 'EPSG:3857'
    assert label.x == pytest.approx(2)
    assert label.y == pytest.approx(1)
    assert gm.covered_by(label, polygon)
    assert gm.equals_exact(
        cast('gm.Geometry', polygon.polylabel(tolerance=0.01)), label
    )
    assert gm.equals_exact(label_array[0], label)
    with pytest.raises(ValueError, match='positive finite'):
        polygon.polylabel(tolerance=0)
    with pytest.raises(TypeError, match='Polygon or MultiPolygon'):
        line.polylabel()
    tall = gm.Polygon([(0, 60), (10, 60), (10, 61), (0, 61)], crs=4326)
    pole = tall.polylabel(tolerance=100)
    circle = tall.maximum_inscribed_circle(tolerance=100)
    # The inscribed disk is a filled Polygon centered on the pole of
    # inaccessibility, so it covers that pole; the radius is its metric twin.
    assert circle.geometry_type == 'Polygon'
    assert gm.covers(circle, pole)
    assert tall.maximum_inscribed_radius(tolerance=100) > 0.0
    planar_pole = tall.polylabel(tolerance=0.001, unit='planar')
    assert (planar_pole.x, planar_pole.y) == pytest.approx((5.0, 60.5))
    assert len(triangles) == 2
    assert triangles.crs == 'EPSG:3857'
    assert all(triangle.geometry_type == 'Polygon' for triangle in triangles)
    assert sum(triangle.area for triangle in triangles) == pytest.approx(1)
    assert len(constrained_triangles) > 2
    assert constrained_triangles.crs == 'EPSG:3857'
    assert sum(triangle.area for triangle in constrained_triangles) == pytest.approx(
        polygon_with_hole.area
    )
    assert len(polygon_triangles) == 4
    assert polygon_triangles.crs == 'EPSG:3857'
    assert all(triangle.geometry_type == 'Polygon' for triangle in polygon_triangles)
    assert sum(triangle.area for triangle in polygon_triangles) == pytest.approx(
        concave_polygon.area
    )
    assert len(holed_polygon_triangles) > 0
    assert sum(triangle.area for triangle in holed_polygon_triangles) == pytest.approx(
        polygon_with_hole.area
    )
    assert len(top_level_triangles) == 2
    assert len(top_level_constrained) == len(constrained_triangles)
    assert len(top_level_polygon_triangles) == len(polygon_triangles)
    # Array tessellation returns per-row Groups: one group of triangles per
    # input geometry, each matching the scalar op on that row (parity).
    assert len(array_triangles) == 2
    assert array_triangles[0] == triangulation_points.triangulate(method='delaunay')
    assert len(array_constrained) == 1
    assert array_constrained[0] == constrained_triangles
    assert len(array_polygon_triangles) == 2
    assert len(array_polygon_triangles[0]) == len(polygon_triangles)
    assert len(array_polygon_triangles[1]) == len(holed_polygon_triangles)
    assert len(voronoi_cells) == 4
    assert voronoi_cells.crs == 'EPSG:3857'
    assert all(cell.geometry_type == 'Polygon' for cell in voronoi_cells)
    assert sum(cell.area for cell in voronoi_cells) == pytest.approx(1)
    assert len(voronoi_edges) > 0
    assert all(edge.geometry_type == 'LineString' for edge in voronoi_edges)
    assert len(triangulation_points.voronoi_polygons(clip='envelope')) == len(
        voronoi_cells
    )
    assert len(
        gm.GeometryArray([triangulation_points]).voronoi_edges(clip='envelope')[0]
    ) == len(voronoi_edges)
    assert all(
        gm.covers(gm.box(-1, -1, 2, 2, crs=3857), cell)
        for cell in clipped_voronoi_cells
    )
    assert len(gm.MultiPoint([(0, 0), (1, 0)]).triangulate(method='delaunay')) == 0
    assert len(gm.Point(0, 0).voronoi_polygons()) == 0
    with pytest.raises(ValueError, match='collinear'):
        gm.MultiPoint([(0, 0), (1, 0), (2, 0)]).voronoi_polygons()
    with pytest.raises(ValueError, match='unknown Voronoi clip'):
        triangulation_points.voronoi_polygons(clip='bad')
    with pytest.raises(TypeError, match='clip geometry must be a Polygon'):
        triangulation_points.voronoi_polygons(clip=gm.Point(0, 0))
    clip_box = gm.box(-1, -1, 2, 2)
    with pytest.raises(ValueError, match='matching CRS'):
        triangulation_points.set_crs('EPSG:3857').voronoi_polygons(
            clip=clip_box.set_crs('EPSG:4326')
        )
    with pytest.raises(TypeError, match='Polygon or MultiPolygon'):
        gm.Point(0, 0).triangulate(method='constrained')
    with pytest.raises(TypeError, match='Polygon or MultiPolygon'):
        gm.Point(0, 0).triangulate(method='earcut')
    assert rotated.minimum_rotated_rectangle().area == pytest.approx(rotated.area)
    assert gm.equals(
        rotated.minimum_rotated_rectangle(), rotated.minimum_rotated_rectangle()
    )
    assert rotated_rectangles[0].area == pytest.approx(polygon.area)
    assert rotated_rectangles[1].to_wkt() == 'LINESTRING (0 0, 2 0)'
    assert polygon.boundary().to_wkt() == 'LINESTRING (0 0, 4 0, 4 2, 0 2, 0 0)'
    assert line.boundary().to_wkt() == 'MULTIPOINT ((0 0), (2 0))'
    assert gm.Point(0, 0).boundary().geometry_type == 'GeometryCollection'
    assert boundaries[1].to_wkt() == 'MULTIPOINT ((0 0), (2 0))'
    assert (
        gm.MultiLineString([[(0, 0), (1, 0)], [(1, 0), (2, 0)]]).boundary().to_wkt()
        == 'MULTIPOINT ((0 0), (2 0))'
    )
    closed = gm.LineString([(0, 0), (1, 0), (1, 1), (0, 0)])
    assert closed.boundary().to_wkt() == 'MULTIPOINT EMPTY'
    assert isinstance(closed.boundary(), gm.MultiPoint)
    assert (
        gm.MultiLineString([[(0, 0), (1, 1)], [(1, 1), (0, 0)]]).boundary().to_wkt()
        == 'MULTIPOINT EMPTY'
    )
    assert (
        gm.GeometryCollection([gm.Point(0, 0), closed]).boundary().to_wkt()
        == 'GEOMETRYCOLLECTION EMPTY'
    )
    assert gm.Polygon().boundary().to_wkt() == 'MULTILINESTRING EMPTY'
    assert (
        gm.from_wkt('MULTIPOLYGON EMPTY').boundary().to_wkt() == 'MULTILINESTRING EMPTY'
    )
    assert gm.from_wkt('LINESTRING EMPTY').boundary().to_wkt() == 'MULTIPOINT EMPTY'
    assert gm.Point().boundary().to_wkt() == 'GEOMETRYCOLLECTION EMPTY'


def test_concave_hull_degenerate_outputs_are_dimensionally_honest() -> None:
    assert gm.from_wkt('POINT EMPTY').concave_hull().to_wkt() == 'POLYGON EMPTY'
    assert gm.MultiPoint([(1, 2), (1, 2)]).concave_hull().to_wkt() == 'POINT (1 2)'
    assert (
        gm.MultiPoint([(0, 0), (1, 1), (2, 2), (1, 1)]).concave_hull().to_wkt()
        == 'LINESTRING (0 0, 2 2)'
    )


def test_concave_hull_contract_invariants_and_parameters() -> None:
    coords = [(0, 0), (4, 0), (4, 4), (2, 1), (0, 4), (1, 2), (3, 2), (2, 3)]
    points = gm.MultiPoint(coords)
    convex = points.convex_hull()
    permissive = points.concave_hull(concavity=0.0)
    tight = points.concave_hull(concavity=0.75)
    loose = points.concave_hull(concavity=3.0)
    thresholded = points.concave_hull(concavity=0.75, length_threshold=5.0)
    for hull in (permissive, tight, loose, thresholded):
        assert hull.is_valid
        assert all((gm.covers(hull, gm.Point(x, y)) for x, y in coords))
    assert permissive.area <= tight.area + 1e-09
    assert tight.area <= loose.area + 1e-09
    assert loose.area <= convex.area + 1e-09
    assert thresholded.area >= tight.area - 1e-09
    assert gm.equals_exact(thresholded, convex)


def test_concave_hull_is_permutation_invariant() -> None:
    coords = [(0, 0), (4, 0), (4, 4), (0, 4), (2, 1), (1, 3)]
    expected = gm.MultiPoint(coords).concave_hull(concavity=1.0).to_wkt()
    for permuted in itertools.permutations(coords):
        hull = gm.MultiPoint(list(permuted)).concave_hull(concavity=1.0)
        assert hull.to_wkt() == expected
        assert gm.equals(hull, gm.from_wkt(expected))


def test_concave_hull_preserves_zm_and_crs_on_native_vertices() -> None:
    points = gm.from_wkt(
        'MULTIPOINT ZM ((0 0 1 10), (4 0 2 20), (4 4 3 30), (2 1 4 40), (0 4 5 50), (1 2 6 60), (3 2 7 70))',
        crs=3857,
    )
    hull = points.concave_hull(concavity=1.0)
    assert hull.crs == 'EPSG:3857'
    assert hull.coordinate_axes == 'XYZM'
    assert {
        coord
        for coord in cast('gm.Polygon', hull).exterior.coords
        if coord[:2] == (2.0, 1.0)
    } == {(2.0, 1.0, 4.0, 40.0)}


def test_polygonize_builds_polygons_from_exact_noded_linework() -> None:
    square_edges = gm.MultiLineString(
        [
            [(0, 0), (1, 0)],
            [(1, 0), (1, 1)],
            [(1, 1), (0, 1)],
            [(0, 1), (0, 0)],
            [(2, 2), (3, 3)],
        ],
        crs=3857,
    )
    hole_edges = gm.GeometryCollection(
        [
            gm.LineString([(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)]),
            gm.LineString([(1, 1), (1, 3), (3, 3), (3, 1), (1, 1)]),
        ],
        crs=3857,
    )
    polygons = square_edges.polygonize()
    top_level = square_edges.polygonize()
    full_polygons, full_cuts, full_dangles, full_invalid = gm.polygonize_full([
        square_edges
    ])
    top_level_full = gm.polygonize_full([square_edges])
    invalid_full = gm.polygonize_full([
        gm.MultiLineString([[(0, 0), (1, 0)], [(0, 0), (1, 0)]])
    ])
    with_hole = hole_edges.polygonize()
    array_polygons = gm.GeometryArray([square_edges]).polygonize()
    pooled_full = gm.polygonize_full([square_edges])
    assert len(polygons) == 1
    assert polygons.crs == 'EPSG:3857'
    assert polygons[0].geometry_type == 'Polygon'
    assert polygons[0].area == pytest.approx(1)
    assert [
        len(part) for part in (full_polygons, full_cuts, full_dangles, full_invalid)
    ] == [1, 0, 1, 0]
    assert full_polygons.crs == 'EPSG:3857'
    assert full_dangles[0].to_wkt() == 'LINESTRING (2 2, 3 3)'
    assert top_level_full[0][0].area == pytest.approx(1)
    assert [len(part) for part in invalid_full] == [0, 0, 0, 1]
    assert invalid_full[3][0].to_wkt() == 'LINESTRING (0 0, 1 0, 0 0)'
    assert len(top_level) == 1
    assert len(array_polygons) == 1
    assert len(pooled_full[0]) == 1
    assert len(pooled_full[2]) == 1
    assert pooled_full.polygons.crs == 'EPSG:3857'
    assert pooled_full.dangles.crs == 'EPSG:3857'
    assert len(with_hole) == 2
    np.testing.assert_allclose(
        sorted(round(polygon.area, 4) for polygon in with_hole), [4.0, 12.0]
    )
    assert list(gm.Point(0, 0).polygonize()) == []


def test_buffer_forms_are_consistent_across_geometry_array_and_top_level() -> None:
    line = gm.LineString([(0.0, 0.0), (2.0, 0.0)])
    assert line.buffer(1.0).coordinate_axes == 'XY'
    assert gm.GeometryArray([line]).buffer(1.0)[0].coordinate_axes == 'XY'
    assert cast('gm.Geometry', line.buffer(1.0)).coordinate_axes == 'XY'
    geographic = gm.Point(0.0, 0.0, crs=4326)
    for call in (
        lambda: geographic.buffer(100.0),
        lambda: gm.GeometryArray([geographic]).buffer(100.0)[0],
        lambda: cast('gm.Geometry', geographic.buffer(100.0)),
    ):
        result = call()
        assert result.geometry_type in ('Polygon', 'MultiPolygon')
        assert not result.is_empty


@pytest.mark.parametrize(
    'op', ['intersection', 'union', 'difference', 'symmetric_difference']
)
def test_overlay_interpolates_z(op: str) -> None:
    """Overlay ops carry Z onto the result automatically (no policy arg)."""
    left = gm.from_wkt('POLYGON Z ((0 0 5, 4 0 5, 4 4 5, 0 4 5, 0 0 5))')
    right = gm.from_wkt('POLYGON Z ((2 2 5, 6 2 5, 6 6 5, 2 6 5, 2 2 5))')
    result = getattr(gm, op)(left, right)
    assert 'Z' in result.coordinate_axes


def test_polygonize_full_preserves_epoch() -> None:
    ring = gm.MultiLineString(
        [[(0, 0), (1, 0)], [(1, 0), (1, 1)], [(1, 1), (0, 0)]], crs=4326, epoch=2020.0
    )
    for arr in gm.polygonize_full([ring]):
        assert arr.crs == 'EPSG:4326'
        assert arr.epoch == 2020.0


def test_empty_polygon_rejects_non_iterable_holes() -> None:
    with pytest.raises(TypeError):
        gm.Polygon(None, holes=cast('Any', 42))


def test_polygon_boundary_contact_intersections_dissolve_to_maximal_lines() -> None:
    """Polygon∩polygon boundary contact degenerates to linework/points.

    Only ``intersection`` emits the lower-dimensional contact;
    union/difference/symmetric_difference stay areal. The contact linework is
    dissolved into maximal chains (split only at genuine degree->=3 junctions),
    not the per-edge fragments GEOS leaks — see
    ``test_overlay_lines_dissolve_to_maximal_chains``.
    """
    a = gm.box(0, 0, 1, 1)
    edge = gm.intersection(a, gm.box(1, 0, 2, 1))
    assert edge.geometry_type == 'LineString'
    assert gm.equals(edge, gm.LineString([(1, 0), (1, 1)]))
    assert gm.equals(gm.intersection(a, gm.box(1, 1, 2, 2)), gm.Point(1, 1))
    partial = gm.intersection(a, gm.box(1, 0.25, 2, 0.75))
    assert gm.equals(partial, gm.LineString([(1, 0.25), (1, 0.75)]))
    donut = gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
        holes=[[(3, 3), (7, 3), (7, 7), (3, 7), (3, 3)]],
    )
    hole_touch = gm.intersection(donut, gm.box(3, 3, 5, 5))
    assert hole_touch.geometry_type == 'LineString'
    assert gm.equals(hole_touch, gm.LineString([(5, 3), (3, 3), (3, 5)]))
    assert hole_touch.length == pytest.approx(4.0)
    overlapper = gm.union_all([gm.box(1, 0, 2, 1), gm.box(0.5, 0.4, 1.1, 0.6)])
    mixed = gm.intersection(a, overlapper)
    assert mixed.geometry_type == 'GeometryCollection'
    kinds = {part.geometry_type for part in gm.parts(mixed)}
    assert 'Polygon' in kinds and kinds & {'LineString', 'MultiLineString'}
    assert gm.intersection(a, gm.box(0, 0, 1, 1)).geometry_type == 'Polygon'
    touch = gm.box(1, 0, 2, 1)
    assert gm.union(a, touch).geometry_type == 'Polygon'
    assert gm.equals(gm.difference(a, touch), a)
    assert gm.symmetric_difference(a, touch).geometry_type == 'Polygon'


def test_overlay_lines_dissolve_to_maximal_chains() -> None:
    """Lineal overlay output is dissolved into the fewest maximal chains.

    Regression for fragmenting a result into one ``LineString`` per noded
    segment. Overlays node linework at every mutual intersection; the result is
    then reassembled into maximal lines, split only at genuine degree->=3
    junctions — cleaner than the per-edge arrangement GEOS leaks.
    """
    bent = gm.LineString([(-2, 5), (-1, 8), (0, 11)])
    assert gm.equals(gm.difference(bent, gm.LineString([(0, 5), (-3, 0)])), bent)
    assert (
        gm.difference(bent, gm.LineString([(0, 5), (-3, 0)])).geometry_type
        == 'LineString'
    )
    clipped = gm.intersection(
        gm.LineString([(0, 5), (1, 5), (2, 5), (3, 5), (4, 5)]), gm.box(0, 0, 10, 10)
    )
    assert clipped.geometry_type == 'LineString'
    assert gm.equals(clipped, gm.LineString([(0, 5), (4, 5)]))
    crossed = gm.difference(
        gm.LineString([(0, 0), (4, 4)]), gm.LineString([(0, 4), (4, 0)])
    )
    assert crossed.geometry_type == 'LineString'
    assert gm.equals(crossed, gm.LineString([(0, 0), (4, 4)]))
    el = gm.union(gm.LineString([(0, 0), (5, 0)]), gm.LineString([(5, 0), (5, 5)]))
    assert el.geometry_type == 'LineString'
    assert gm.equals(el, gm.LineString([(0, 0), (5, 0), (5, 5)]))
    cross = gm.union(gm.LineString([(0, 0), (2, 2)]), gm.LineString([(0, 2), (2, 0)]))
    assert cross.geometry_type == 'MultiLineString'
    assert len(gm.parts(cross)) == 4
    sym = gm.symmetric_difference(
        gm.LineString([(0, 0), (3, 0)]), gm.LineString([(1, 0), (5, 0)])
    )
    assert sym.geometry_type == 'MultiLineString'
    assert sym.length == pytest.approx(3.0)


def test_line_merge_dissolves_only_degree_two_nodes() -> None:
    """``line_merge`` joins chains at degree-2 nodes and SPLITS every junction.

    Regression for a greedy endpoint merge that fused two arms straight through
    a degree->=3 junction (and missed ``+0.0``/``-0.0`` joints by comparing raw
    bits). The JTS LineMerger contract keeps all junction arms separate.
    """
    y = gm.from_wkt('MULTILINESTRING ((0 0, 1 1), (1 1, 2 0), (1 1, 1 2))').line_merge()
    assert y.geometry_type == 'MultiLineString'
    assert len(gm.parts(y)) == 3
    t = gm.from_wkt('MULTILINESTRING ((0 0, 2 0), (2 0, 4 0), (2 0, 2 2))').line_merge()
    assert len(gm.parts(t)) == 3
    chain = gm.from_wkt('MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))').line_merge()
    assert gm.equals(chain, gm.LineString([(0, 0), (1, 1), (2, 2)]))
    loop = gm.from_wkt(
        'MULTILINESTRING ((0 0, 1 0), (1 0, 1 1), (1 1, 0 1), (0 1, 0 0))'
    ).line_merge()
    assert loop.geometry_type == 'LineString'
    assert loop.is_ring
    crossing = gm.from_wkt('MULTILINESTRING ((0 0, 2 2), (2 0, 0 2))').line_merge()
    assert len(gm.parts(crossing)) == 2
    signed = gm.union(
        gm.LineString([(-1.0, 0.0), (0.0, 0.0)]),
        gm.LineString([(0.0, -0.0), (1.0, 0.0)]),
    )
    assert gm.equals(signed, gm.LineString([(-1, 0), (1, 0)]))


def test_overlay_detects_collinear_overlap_with_self_intersecting_operand() -> None:
    """Collinear overlap is found even when the carrying operand self-intersects.

    Regression: a third segment crossing a shared collinear run was solved
    against two different host extents, minting ulp-twin cut points so the
    shared-edge match (``al && bl``) failed and the run collapsed to a stray
    point. The pool is now noded once with source tracking, so coincident
    strokes are bit-identical and the shared run is recognised.
    """
    a = gm.LineString([(-3, -1), (5, 3)])
    b = gm.LineString([(5, 0), (-2, -1), (-1, 3), (7, 4), (-7, -3)])
    assert gm.intersection(a, b).length == pytest.approx(a.length)
    assert gm.intersection(b, a).length == pytest.approx(a.length)
    assert gm.difference(a, b).is_empty
    assert gm.union(a, b).length == pytest.approx(b.length)
    assert gm.symmetric_difference(a, b).length == pytest.approx(b.length - a.length)


def test_overlay_drops_degenerate_zero_length_lines() -> None:
    """A zero-length ``LineString`` carries no linework: it vanishes from results.

    Emitting an awkward zero-length ``LineString`` (start == end) is worse than
    an empty/absent line, so overlay drops it and ``line_merge`` ignores it.
    """
    degenerate = gm.LineString([(8, 1), (8, 1)])
    out = gm.difference(degenerate, gm.box(0, 3, 4, 7))
    assert out.is_empty and out.geometry_type == 'LineString'
    assert gm.difference(degenerate, gm.LineString([(0, 0), (1, 1)])).is_empty
    assert gm.equals(gm.union(degenerate, gm.box(0, 3, 4, 7)), gm.box(0, 3, 4, 7))
    assert gm.equals(
        gm.symmetric_difference(degenerate, gm.box(0, 3, 4, 7)), gm.box(0, 3, 4, 7)
    )
    mixed = gm.from_wkt('MULTILINESTRING ((8 1, 8 1), (0 0, 2 0))')
    assert gm.equals(
        gm.difference(mixed, gm.box(20, 20, 30, 30)), gm.LineString([(0, 0), (2, 0)])
    )
    merged = gm.from_wkt(
        'MULTILINESTRING ((5 5, 5 5), (0 0, 1 1), (1 1, 2 2))'
    ).line_merge()
    assert gm.equals(merged, gm.LineString([(0, 0), (2, 2)]))


def test_constructive_outputs_collapse_to_the_narrowest_type() -> None:
    assert gm.Point(1, 2).buffer(1.0).geometry_type == 'Polygon'
    assert cast('gm.Polygon', gm.Point(1, 2).buffer(1.0)).exterior.is_ring
    assert gm.Point(1, 2, crs=4326).buffer(100.0).geometry_type == 'Polygon'
    split = gm.MultiPoint([(0, 0), (10, 10)]).buffer(1.0)
    assert split.geometry_type == 'MultiPolygon'
    eroded = gm.box(0, 0, 2, 2).buffer(-10)
    assert eroded.geometry_type == 'Polygon' and eroded.is_empty
    assert gm.box(0, 0, 4, 4).clip_by_rect(1, 1, 3, 3).geometry_type == 'Polygon'
