from typing import Any, cast

import gometry as gm
import numpy as np
import pytest

shapely = pytest.importorskip('shapely')

def test_predicates_and_overlay_match_shapely_oracle() -> None:
    from shapely import (
        boundary,
        constrained_delaunay_triangles,
        contains,
        convex_hull,
        covered_by,
        delaunay_triangles,
        frechet_distance,
        from_wkt,
        hausdorff_distance,
        intersection,
        is_closed,
        is_empty,
        is_ring,
        is_simple,
        line_interpolate_point,
        line_locate_point,
        maximum_inscribed_circle,
        minimum_clearance,
        minimum_rotated_rectangle,
        normalize,
        orient_polygons,
        point_on_surface,
        relate,
        relate_pattern,
        remove_repeated_points,
        reverse,
        segmentize,
        shared_paths,
        snap,
        union,
        voronoi_polygons,
    )
    from shapely.geometry import Point
    from shapely.ops import (
        clip_by_rect,
        linemerge,
        nearest_points,
        polygonize,
        polygonize_full,
        split,
        substring,
    )

    left = gm.box(0, 0, 2, 2)
    right = gm.box(1, 1, 3, 3)
    lines = gm.MultiLineString([[(0, 0), (1, 0)], [(2, 0), (1, 0)]])
    reference_line = gm.LineString([(0, 0), (3, 4), (6, 4)])
    gometry_points = gm.points([0.5, 1.5, 2.5], [0.5, 1.5, 2.5])
    shapely_left = from_wkt(left.to_wkt())
    shapely_right = from_wkt(right.to_wkt())
    shapely_lines = from_wkt(lines.to_wkt())
    shapely_reference_line = from_wkt(reference_line.to_wkt())
    shapely_contains = [
        bool(contains(shapely_left, Point(x, y)))
        for x, y in ((0.5, 0.5), (1.5, 1.5), (2.5, 2.5))
    ]
    assert np.array_equal(gm.contains(left, gometry_points), shapely_contains)
    assert gm.intersection(left, right).area == pytest.approx(
        intersection(shapely_left, shapely_right).area
    )
    assert gm.union(left, right).area == pytest.approx(
        union(shapely_left, shapely_right).area
    )
    assert lines.line_merge().to_wkt() == linemerge(cast('Any', shapely_lines)).wkt
    assert left.clip_by_rect(0.5, 0.5, 1.5, 1.5).area == pytest.approx(
        clip_by_rect(shapely_left, 0.5, 0.5, 1.5, 1.5).area
    )
    assert (
        reference_line.line_interpolate(6).to_wkt()
        == line_interpolate_point(cast('Any', shapely_reference_line), 6).wkt
    )
    assert (
        reference_line.line_substring(2, 6).to_wkt()
        == substring(cast('Any', shapely_reference_line), 2, 6).wkt
    )
    split_line = gm.LineString([(0, 0), (4, 0)])
    split_points = gm.MultiPoint([(1, 0), (3, 0)])
    gometry_split = gm.split(split_line, split_points)
    shapely_split = split(
        from_wkt(split_line.to_wkt()), from_wkt(split_points.to_wkt())
    )
    assert [piece.to_wkt() for piece in gometry_split] == [
        piece.wkt for piece in shapely_split.geoms
    ]
    assert reference_line.line_locate(gm.Point(4, 5)) == pytest.approx(
        line_locate_point(cast('Any', shapely_reference_line), Point(4, 5))
    )
    assert left.centroid().to_wkt() == shapely_left.centroid.wkt
    assert left.convex_hull().area == pytest.approx(convex_hull(shapely_left).area)
    hull_points = gm.MultiPoint([
        (0, 0),
        (4, 0),
        (4, 4),
        (2, 1),
        (0, 4),
        (1, 2),
        (3, 2),
    ])
    shapely_hull_points = from_wkt(hull_points.to_wkt())
    gometry_concave = hull_points.concave_hull(concavity=1.0)
    assert gometry_concave.is_valid
    assert gometry_concave.area <= convex_hull(shapely_hull_points).area
    assert all(
        (
            gm.covers(gometry_concave, gm.Point(x, y))
            for x, y in [(0, 0), (4, 0), (4, 4), (2, 1), (0, 4), (1, 2), (3, 2)]
        )
    )
    gometry_label = left.polylabel(tolerance=0.01)
    shapely_label = cast('Any', maximum_inscribed_circle)(
        shapely_left, tolerance=0.01
    ).coords[0]
    assert gometry_label.x == pytest.approx(shapely_label[0], abs=0.01)
    assert gometry_label.y == pytest.approx(shapely_label[1], abs=0.01)
    gometry_triangles = gm.MultiPoint([(0, 0), (1, 0), (0, 1), (1, 1)]).triangulate(
        method='delaunay'
    )
    shapely_triangles = delaunay_triangles(
        from_wkt('MULTIPOINT ((0 0), (1 0), (0 1), (1 1))')
    )
    assert len(gometry_triangles) == len(shapely_triangles.geoms)
    assert sum(triangle.area for triangle in gometry_triangles) == pytest.approx(
        shapely_triangles.area
    )
    gometry_voronoi = gm.MultiPoint([(0, 0), (1, 0), (0, 1), (1, 1)]).voronoi_polygons(
        clip='envelope'
    )
    shapely_voronoi = voronoi_polygons(
        from_wkt('MULTIPOINT ((0 0), (1 0), (0 1), (1 1))'),
        extend_to=from_wkt('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'),
    )
    shapely_clipped_voronoi = [
        intersection(cell, from_wkt('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))
        for cell in shapely_voronoi.geoms
    ]
    assert len(gometry_voronoi) == len(shapely_voronoi.geoms)
    assert all(cell.geometry_type == 'Polygon' for cell in gometry_voronoi)
    assert sum(cell.area for cell in gometry_voronoi) == pytest.approx(1)
    assert shapely_voronoi.area == pytest.approx(9)
    assert sum(cell.area for cell in shapely_clipped_voronoi) == pytest.approx(1)
    gometry_constrained = gm.Polygon(
        [(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)],
        holes=[[(0.75, 0.75), (1.25, 0.75), (1.25, 1.25), (0.75, 1.25), (0.75, 0.75)]],
    ).triangulate(method='constrained')
    shapely_constrained = constrained_delaunay_triangles(
        from_wkt(
            'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0), (0.75 0.75, 1.25 0.75, 1.25 1.25, 0.75 1.25, 0.75 0.75))'
        )
    )
    assert len(gometry_constrained) == len(shapely_constrained.geoms)
    assert sum(triangle.area for triangle in gometry_constrained) == pytest.approx(
        shapely_constrained.area
    )
    concave_polygon = gm.Polygon([
        (0, 0),
        (3, 0),
        (3, 1),
        (1, 1),
        (1, 3),
        (0, 3),
        (0, 0),
    ])
    gometry_polygon_triangles = concave_polygon.triangulate(method='earcut')
    shapely_polygon_triangles = constrained_delaunay_triangles(
        from_wkt(concave_polygon.to_wkt())
    )
    assert len(gometry_polygon_triangles) == 4
    assert sum(
        triangle.area for triangle in gometry_polygon_triangles
    ) == pytest.approx(shapely_polygon_triangles.area)
    assert left.minimum_rotated_rectangle().area == pytest.approx(
        minimum_rotated_rectangle(shapely_left).area
    )
    assert left.boundary().to_wkt() == boundary(shapely_left).wkt
    polygonize_input = gm.MultiLineString([
        [(0, 0), (1, 0)],
        [(1, 0), (1, 1)],
        [(1, 1), (0, 1)],
        [(0, 1), (0, 0)],
        [(2, 2), (3, 3)],
    ])
    gometry_polygonized = polygonize_input.polygonize()
    shapely_polygonized = list(polygonize(from_wkt(polygonize_input.to_wkt())))
    gometry_full = gm.polygonize_full([polygonize_input])
    shapely_full = polygonize_full(from_wkt(polygonize_input.to_wkt()))
    assert len(gometry_polygonized) == len(shapely_polygonized)
    assert sum(polygon.area for polygon in gometry_polygonized) == pytest.approx(
        sum(polygon.area for polygon in shapely_polygonized)
    )
    assert [len(part) for part in gometry_full] == [
        len(shapely_full[0].geoms),
        len(shapely_full[1].geoms),
        len(shapely_full[2].geoms),
        len(shapely_full[3].geoms),
    ]
    assert (
        gm.LineString([(0, 0), (0, 0), (2, 0)]).remove_repeated_points().to_wkt()
        == remove_repeated_points(from_wkt('LINESTRING (0 0, 0 0, 2 0)')).wkt
    )
    assert (
        gm.LineString([(0, 0), (2, 0)]).segmentize(0.5).to_wkt()
        == segmentize(from_wkt('LINESTRING (0 0, 2 0)'), 0.5).wkt
    )
    snap_line = gm.LineString([(0, 0), (0.9, 0.1), (2, 0)])
    assert (
        gm.snap(snap_line, gm.Point(1, 0), 0.25).to_wkt()
        == snap(from_wkt(snap_line.to_wkt()), from_wkt('POINT (1 0)'), 0.25).wkt
    )
    shared_line = gm.LineString([(0, 0), (2, 0)])
    shared_reference = gm.LineString([(1, 0), (3, 0)])
    assert (
        gm.shared_paths(shared_line, shared_reference).to_wkt()
        == cast('Any', shared_paths)(
            from_wkt(shared_line.to_wkt()), from_wkt(shared_reference.to_wkt())
        ).wkt
    )
    unoriented = gm.Polygon(
        [(0, 0), (0, 1), (1, 1), (0, 0)],
        holes=[[(0.2, 0.2), (0.5, 0.2), (0.2, 0.5), (0.2, 0.2)]],
    )
    shapely_unoriented = from_wkt(unoriented.to_wkt())
    assert unoriented.reverse().to_wkt() == reverse(shapely_unoriented).wkt
    assert (
        unoriented.orient_polygons().to_wkt() == orient_polygons(shapely_unoriented).wkt
    )
    ours_normalized = from_wkt(unoriented.normalize().to_wkt())
    assert shapely.equals(ours_normalized, normalize(shapely_unoriented))
    structural = gm.LineString([(0, 0), (1, 1), (1, 0), (0, 1)])
    shapely_structural = from_wkt(structural.to_wkt())
    assert structural.is_empty == bool(is_empty(shapely_structural))
    assert structural.is_closed == bool(is_closed(shapely_structural))
    assert structural.is_ring == bool(is_ring(shapely_structural))
    assert structural.is_simple == bool(is_simple(shapely_structural))
    assert structural.minimum_clearance() == pytest.approx(
        minimum_clearance(shapely_structural)
    )
    assert gm.relate(left, right) == relate(shapely_left, shapely_right)
    assert gm.relate_pattern(left, right, 'T*T***T**') == bool(
        relate_pattern(shapely_left, shapely_right, 'T*T***T**')
    )
    gometry_nearest = gm.nearest_points(gm.Point(1, 3), reference_line)
    shapely_nearest = nearest_points(Point(1, 3), cast('Any', shapely_reference_line))
    assert tuple(point.to_wkt() for point in gometry_nearest) == tuple(
        point.wkt for point in shapely_nearest
    )
    assert gm.covered_by(left.point_on_surface(), left)
    assert covered_by(point_on_surface(shapely_left), shapely_left)
    assert gm.hausdorff_distance(
        reference_line, gm.LineString([(0, 1), (3, 5), (6, 5)])
    ) == pytest.approx(
        hausdorff_distance(
            shapely_reference_line, from_wkt('LINESTRING (0 1, 3 5, 6 5)')
        )
    )
    assert gm.frechet_distance(
        reference_line, gm.LineString([(0, 1), (3, 5), (6, 5)])
    ) == pytest.approx(
        frechet_distance(shapely_reference_line, from_wkt('LINESTRING (0 1, 3 5, 6 5)'))
    )


def test_mixed_point_line_polygon_overlay_matches_shapely_oracle() -> None:
    """Point- and line-involving set overlay matches GEOS/Shapely semantics:
    pieces are kept/dropped by whole-operand coverage, lower-dimensional pieces
    inside the polygon output are absorbed, line/polygon clipping keeps the
    interior (intersection) or exterior (other ops) portions plus boundary
    contact points, and results narrow to the tightest representable type.
    """
    from shapely import from_wkt

    poly = gm.Polygon([(0, 0), (4, 0), (4, 4), (0, 4)])
    cases = [
        (gm.Point(0.5, 0.5), poly),
        (gm.Point(5, 5), poly),
        (gm.Point(0, 0), poly),
        (gm.MultiPoint([(0.5, 0.5), (5, 5)]), poly),
        (gm.Point(0, 0), gm.Point(0, 0)),
        (gm.MultiPoint([(0, 0), (1, 1)]), gm.MultiPoint([(1, 1), (2, 2)])),
        (gm.LineString([(-1, 2), (5, 2)]), poly),
        (gm.LineString([(1, 1), (3, 3)]), poly),
        (gm.LineString([(5, 5), (6, 6)]), poly),
        (gm.LineString([(-1, 0), (-1, -1), (0, 0)]), poly),
        (gm.LineString([(2, 2), (6, 2)]), poly),
        (poly, gm.LineString([(-1, 2), (5, 2)])),
        (gm.LineString([(0, 0), (2, 2)]), gm.LineString([(0, 2), (2, 0)])),
        (gm.LineString([(0, 0), (2, 0)]), gm.LineString([(1, 0), (1, 2)])),
        (gm.LineString([(0, 0), (3, 0)]), gm.LineString([(1, 0), (5, 0)])),
        (gm.LineString([(0, 0), (5, 0)]), gm.LineString([(1, 0), (3, 0)])),
        (gm.LineString([(0, 0), (1, 1)]), gm.LineString([(1, 1), (2, 0)])),
        (gm.LineString([(0, 0), (2, 0)]), gm.LineString([(0, 0), (2, 0)])),
        (gm.LineString([(0, 0), (1, 0)]), gm.LineString([(0, 1), (1, 1)])),
        (
            gm.MultiLineString([[(0, 0), (2, 0)], [(0, 1), (2, 1)]]),
            gm.LineString([(1, -1), (1, 2)]),
        ),
        (gm.Point(1, 0), gm.LineString([(0, 0), (2, 0)])),
        (gm.Point(1, 1), gm.LineString([(0, 0), (2, 0)])),
        (gm.LineString([(0, 0), (2, 0)]), gm.Point(1, 0)),
        (
            gm.GeometryCollection([gm.LineString([(1, 1), (3, 1)]), poly]),
            gm.box(2, -1, 6, 5),
        ),
        (
            gm.GeometryCollection([gm.LineString([(-1, 2), (5, 2)]), poly]),
            gm.box(2, -1, 6, 5),
        ),
    ]
    for left, right in cases:
        shapely_left = from_wkt(left.to_wkt())
        shapely_right = from_wkt(right.to_wkt())
        for name in ('intersection', 'union', 'difference', 'symmetric_difference'):
            got = getattr(gm, name)(left, right)
            want = getattr(shapely_left, name)(shapely_right)
            assert got.is_empty == bool(want.is_empty), (name, left.to_wkt())
            if not got.is_empty:
                assert shapely.equals(from_wkt(got.to_wkt()), want), (
                    name,
                    left.to_wkt(),
                    got.to_wkt(),
                    want.wkt,
                )


def test_h3_edges_match_h3py_oracle() -> None:
    h3 = pytest.importorskip('h3')
    origin = h3.latlng_to_cell(52.5, 13.4, 7)
    cell = gm.H3Cell(origin)
    for destination in h3.grid_ring(origin, 1):
        edge = cell.edge(gm.H3Cell(destination))
        want = h3.cells_to_directed_edge(origin, destination)
        assert edge.token == want
        assert edge.origin.token == h3.get_directed_edge_origin(want)
        assert edge.destination.token == h3.get_directed_edge_destination(want)
        assert edge.length == pytest.approx(h3.edge_length(want, unit='m'), rel=1e-06)
    assert {e.token for e in cell.edges} == set(h3.origin_to_directed_edges(origin))


def test_densified_hausdorff_frechet_match_shapely_oracle() -> None:
    import shapely

    a = gm.LineString([(0, 0), (10, 0)])
    b = gm.LineString([(0, 1), (5, 8), (10, 1)])
    sa, sb = (shapely.from_wkt(a.to_wkt()), shapely.from_wkt(b.to_wkt()))
    for densify in (0.5, 0.1, 0.01):
        assert gm.hausdorff_distance(a, b, densify=densify) == pytest.approx(
            shapely.hausdorff_distance(sa, sb, densify=densify)
        )
        assert gm.frechet_distance(a, b, densify=densify) == pytest.approx(
            shapely.frechet_distance(sa, sb, densify=densify)
        )
    coarse = gm.hausdorff_distance(a, b)
    fine = gm.hausdorff_distance(a, b, densify=0.01)
    assert coarse == pytest.approx(8)
    assert fine <= coarse + 1e-09
    with pytest.raises(gm.GeometryError, match='densify must be in'):
        gm.hausdorff_distance(a, b, densify=1.5)


def test_geographic_densified_hausdorff_uses_geodesic_path() -> None:
    geo_peak = gm.LineString([(0, 0), (10, 0)], crs=4326)
    geo_far = gm.LineString([(0, 1), (5, 8), (10, 1)], crs=4326)
    raw = gm.hausdorff_distance(geo_peak, geo_far)
    assert raw > 500000
    geodesic_half = gm.hausdorff_distance(geo_peak, geo_far, densify=0.5)
    assert geodesic_half > 500000
    assert geodesic_half == pytest.approx(raw)
    assert gm.hausdorff_distance(
        geo_peak, geo_far, densify=0.5, unit='planar'
    ) == pytest.approx(8)
    equator_a = gm.LineString([(0, 0), (30, 0)], crs=4326)
    equator_b = gm.LineString([(0, 0.5), (15, 0.5), (30, 0.5)], crs=4326)
    d1 = gm.hausdorff_distance(equator_a, equator_b, densify=1.0)
    d05 = gm.hausdorff_distance(equator_a, equator_b, densify=0.5)
    d01 = gm.hausdorff_distance(equator_a, equator_b, densify=0.1)
    d001 = gm.hausdorff_distance(equator_a, equator_b, densify=0.01)
    assert d1 == pytest.approx(gm.hausdorff_distance(equator_a, equator_b))
    assert d1 >= d05 >= d01 >= d001
    assert gm.hausdorff_distance(
        equator_a, equator_b, densify=0.5, unit='planar'
    ) == pytest.approx(0.5)


def test_repair_matches_geos_make_valid_oracle() -> None:
    """The linework fill rule is even-odd over the DEDUPLICATED noded
    linework — exactly GEOS make_valid: keyholes cancel to donuts, a fully
    retraced ring still encloses its area, and curated self-intersecting
    polygons agree to the last drop of area.
    """
    import shapely

    retraced = gm.from_wkt(
        'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0, 2 0, 2 2, 0 2, 0 0))'
    ).repair()
    assert retraced.to_wkt() == 'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'
    for inner in ('1 1, 1 3, 3 3, 3 1, 1 1', '1 1, 3 1, 3 3, 1 3, 1 1'):
        keyhole = gm.from_wkt(
            f'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0, {inner}, 0 0))'
        ).repair()
        assert keyhole.area == pytest.approx(12.0)
    one_cross_bowtie = [(0, 0), (4, 4), (0, 4), (4, 0)]
    doubled_to_polygon = [
        (4.540126082768255, 0.9570675097437181),
        (2.520989352233717, 2.394208236592851),
        (2.600313849925394, 3.790885304732522),
        (0.7719372019579285, 2.1444254220989505),
        (2.7277371344588563, 0.18440287987167236),
        (1.8125627165166125, 2.287550034465033),
    ]
    cases = {
        'valid_concave': [(0, 0), (4, 0), (4, 4), (2, 2), (0, 4)],
        'one_cross_bowtie': one_cross_bowtie,
        'seven_cross_multi_component': [
            (4.666059121095044, 4.787207808332334),
            (1.167844851350604, 0.7816524743163067),
            (4.872482965592507, 4.677101257538264),
            (0.18378179534758288, 4.336066853993354),
            (4.559825845201101, 2.1296697831851152),
            (0.883004535670659, 1.4950586152812724),
        ],
        'self_cross_single_component': [
            (4.9509744757218055, 1.9250700155316465),
            (2.979473883332302, 4.317555476583561),
            (1.333585858569926, 2.0685966595264804),
            (1.7306094726745203, 2.2187047785519893),
            (0.0701995811589784, 4.631119113382912),
            (0.8955317835175325, 1.0313258865690726),
        ],
        'doubled_bowtie': one_cross_bowtie + one_cross_bowtie,
        'doubled_to_polygon': doubled_to_polygon + doubled_to_polygon,
    }
    for name, coords in cases.items():
        geom = gm.Polygon(coords)
        ours = geom.repair()
        want = shapely.make_valid(shapely.from_wkt(geom.to_wkt()))
        assert ours.is_valid, name
        assert ours.area == pytest.approx(want.area, abs=1e-09), name


def test_minimum_clearance_line_matches_shapely_oracle() -> None:
    import shapely

    wkts = [
        'POLYGON ((0 0, 3 0, 3 2, 0 2, 0 0))',
        'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))',
        'LINESTRING (0 0, 10 0, 10 1, 0 0.5)',
        'MULTIPOINT ((0 0), (5 0), (5 0.25))',
    ]
    for wkt in wkts:
        line = gm.from_wkt(wkt).minimum_clearance_line()
        want = shapely.minimum_clearance_line(shapely.from_wkt(wkt))
        assert line.length == pytest.approx(want.length)
        assert line.length == pytest.approx(gm.from_wkt(wkt).minimum_clearance())


def test_unique_points_matches_shapely_oracle() -> None:
    import shapely

    wkts = [
        'LINESTRING (0 0, 1 1, 0 0, 2 2)',
        'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))',
        'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((1 1, 2 1, 2 2, 1 1)))',
        'GEOMETRYCOLLECTION (POINT (5 5), LINESTRING (0 0, 5 5))',
        'LINESTRING EMPTY',
    ]
    for wkt in wkts:
        got = gm.from_wkt(wkt).unique_points()
        want = shapely.extract_unique_points(shapely.from_wkt(wkt))
        assert shapely.equals(shapely.from_wkt(got.to_wkt()), want) or (
            got.is_empty and want.is_empty
        ), (wkt, got.to_wkt(), want.wkt)


def test_union_all_dissolves_mixed_dimensions_like_shapely_oracle() -> None:
    """``union_all`` dissolves polygons, nodes linework, and dedups points across
    every dimension at once, matching Shapely's ``union_all``.
    """
    import shapely
    from shapely import from_wkt

    groups = [
        [gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)],
        [gm.Point(0, 0), gm.Point(1, 1), gm.Point(0, 0)],
        [gm.LineString([(0, 0), (2, 0)]), gm.LineString([(1, 0), (3, 0)])],
        [gm.box(0, 0, 2, 2), gm.LineString([(-1, 1), (3, 1)]), gm.Point(5, 5)],
        [gm.Point(1, 1), gm.box(0, 0, 2, 2)],
    ]
    for group in groups:
        got = gm.union_all(group)
        want = shapely.union_all([from_wkt(item.to_wkt()) for item in group])
        assert got.is_empty == bool(want.is_empty)
        if not got.is_empty:
            assert shapely.equals(from_wkt(got.to_wkt()), want), (
                got.to_wkt(),
                want.wkt,
            )


def test_geometry_collection_relate_clean_cases_match_shapely_oracle() -> None:
    import shapely
    from shapely import relate
    square = 'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'
    cases = [
        ('point_equal', 'POINT (0 0)', 'POINT (0 0)', False),
        ('point_disjoint', 'POINT (0 0)', 'POINT (1 0)', False),
        ('point_line_on', 'POINT (1 0)', 'LINESTRING (0 0, 2 0)', False),
        ('point_line_off', 'POINT (1 1)', 'LINESTRING (0 0, 2 0)', False),
        ('point_polygon_inside', 'POINT (1 1)', square, False),
        ('point_polygon_boundary', 'POINT (1 0)', square, False),
        ('lines_cross', 'LINESTRING (0 0, 2 2)', 'LINESTRING (0 2, 2 0)', False),
        ('lines_overlap', 'LINESTRING (0 0, 2 0)', 'LINESTRING (1 0, 3 0)', False),
        ('lines_parallel', 'LINESTRING (0 0, 2 0)', 'LINESTRING (0 1, 2 1)', False),
        ('line_crosses_square', 'LINESTRING (-1 1, 3 1)', square, False),
        ('line_on_square_boundary', 'LINESTRING (0 0, 2 0)', square, False),
        ('square_overlap', square, 'POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))', False),
        ('square_shared_edge', square, 'POLYGON ((2 0, 4 0, 4 2, 2 2, 2 0))', False),
        ('square_contains_polygon', 'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))', 'POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))', False),
        ('square_partial_shared_edge', square, 'POLYGON ((3 0, 4 0, 4 1, 3 1, 3 0))', False),
        ('both_wrapped_lines', 'LINESTRING (0 0, 2 0)', 'LINESTRING (1 1, 3 1)', True),
        ('wrapped_polygon_line', 'POLYGON ((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))', 'LINESTRING (2 1, 3 1)', True),
        ('wrapped_line_polygon', 'LINESTRING (1 1, 3 1)', square, True),
    ]

    def make(value: str, wrapped: bool) -> tuple[gm.Geometry, Any]:
        native = gm.from_wkt(value)
        oracle = shapely.from_wkt(value)
        if wrapped:
            return gm.GeometryCollection([native]), shapely.GeometryCollection([oracle])
        return native, oracle

    for name, left_wkt, right_wkt, right_wrapped in cases:
        left, oracle_left = make(left_wkt, True)
        right, oracle_right = make(right_wkt, right_wrapped)
        assert gm.relate(left, right) == relate(oracle_left, oracle_right), name
        assert gm.relate(right, left) == relate(oracle_right, oracle_left), name


def test_geometry_collection_relate_residual_regressions_match_shapely() -> None:
    import shapely
    from shapely import relate

    collapsed = gm.from_wkt('GEOMETRYCOLLECTION(POLYGON((0 0,1 0,0 0,1 0,0 0)))')
    collapsed_oracle = shapely.from_wkt(
        'GEOMETRYCOLLECTION(POLYGON((0 0,1 0,0 0,1 0,0 0)))'
    )
    line = gm.LineString([(0, 0), (1, 0)])
    line_oracle = shapely.from_wkt('LINESTRING(0 0,1 0)')
    assert gm.relate(collapsed, line) == shapely.relate(collapsed_oracle, line_oracle)
    assert gm.relate(line, collapsed) == shapely.relate(line_oracle, collapsed_oracle)
    left = gm.from_wkt(
        'GEOMETRYCOLLECTION(POINT(3 1),POLYGON((-2 -2,-1 -2,-1 -1,-2 -1,-2 -2)),POLYGON((1 1,3 1,3 3,1 3,1 1)),POINT(1 0))'
    )
    oracle_left = shapely.from_wkt(
        'GEOMETRYCOLLECTION(POINT(3 1),POLYGON((-2 -2,-1 -2,-1 -1,-2 -1,-2 -2)),POLYGON((1 1,3 1,3 3,1 3,1 1)),POINT(1 0))'
    )
    right = gm.from_wkt(
        'GEOMETRYCOLLECTION(LINESTRING(0 0,2 0),POINT(1 0),POINT(3 1),LINESTRING(0 0,2 0),LINESTRING(0.5 0,1.5 0))'
    )
    oracle_right = shapely.from_wkt(
        'GEOMETRYCOLLECTION(LINESTRING(0 0,2 0),POINT(1 0),POINT(3 1),LINESTRING(0 0,2 0),LINESTRING(0.5 0,1.5 0))'
    )
    assert gm.relate(left, right) == relate(oracle_left, oracle_right)
    assert gm.relate(right, left) == relate(oracle_right, oracle_left)
    puntal = gm.from_wkt('GEOMETRYCOLLECTION(POINT(1 1),POINT(1 0))')
    oracle_puntal = shapely.from_wkt('GEOMETRYCOLLECTION(POINT(1 1),POINT(1 0))')
    lineal_collection = gm.from_wkt(
        'GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,2 0),LINESTRING(1 1,3 1),LINESTRING(3 0,3 2))'
    )
    oracle_lineal_collection = shapely.from_wkt(
        'GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,2 0),LINESTRING(1 1,3 1),LINESTRING(3 0,3 2))'
    )
    assert gm.relate(puntal, lineal_collection) == relate(
        oracle_puntal, oracle_lineal_collection
    )
    assert gm.relate(lineal_collection, puntal) == relate(
        oracle_lineal_collection, oracle_puntal
    )
    boundary_left = gm.from_wkt(
        'GEOMETRYCOLLECTION(POLYGON((-2 -2,-1 -2,-1 -1,-2 -1,-2 -2)),LINESTRING(1 1,3 1),POINT(1 1),POLYGON((2 0,4 0,4 2,2 2,2 0)),LINESTRING(3 0,3 2))'
    )
    oracle_boundary_left = shapely.from_wkt(
        'GEOMETRYCOLLECTION(POLYGON((-2 -2,-1 -2,-1 -1,-2 -1,-2 -2)),LINESTRING(1 1,3 1),POINT(1 1),POLYGON((2 0,4 0,4 2,2 2,2 0)),LINESTRING(3 0,3 2))'
    )
    boundary_right = gm.from_wkt(
        'GEOMETRYCOLLECTION(POLYGON((0.5 0.5,1.5 0.5,1.5 1.5,0.5 1.5,0.5 0.5)),LINESTRING(0 0,2 0),LINESTRING(0.5 0,1.5 0))'
    )
    oracle_boundary_right = shapely.from_wkt(
        'GEOMETRYCOLLECTION(POLYGON((0.5 0.5,1.5 0.5,1.5 1.5,0.5 1.5,0.5 0.5)),LINESTRING(0 0,2 0),LINESTRING(0.5 0,1.5 0))'
    )
    assert gm.relate(boundary_left, boundary_right) == relate(
        oracle_boundary_left, oracle_boundary_right
    )
    assert gm.relate(boundary_right, boundary_left) == relate(
        oracle_boundary_right, oracle_boundary_left
    )
    line_boundary_left = gm.from_wkt(
        'GEOMETRYCOLLECTION(POLYGON((-2 -2,-1 -2,-1 -1,-2 -1,-2 -2)),LINESTRING(-1 1,3 1))'
    )
    oracle_line_boundary_left = shapely.from_wkt(
        'GEOMETRYCOLLECTION(POLYGON((-2 -2,-1 -2,-1 -1,-2 -1,-2 -2)),LINESTRING(-1 1,3 1))'
    )
    line_boundary_right = gm.from_wkt(
        'GEOMETRYCOLLECTION(LINESTRING(0 0,2 0),POINT(1 0),POINT(3 1),LINESTRING(2 1,3 1))'
    )
    oracle_line_boundary_right = shapely.from_wkt(
        'GEOMETRYCOLLECTION(LINESTRING(0 0,2 0),POINT(1 0),POINT(3 1),LINESTRING(2 1,3 1))'
    )
    assert gm.relate(line_boundary_left, line_boundary_right) == relate(
        oracle_line_boundary_left, oracle_line_boundary_right
    )
    assert gm.relate(line_boundary_right, line_boundary_left) == relate(
        oracle_line_boundary_right, oracle_line_boundary_left
    )
    absorbed_boundary_left = gm.from_wkt(
        'GEOMETRYCOLLECTION(LINESTRING(3 0,3 2),POINT(0 0),LINESTRING(1 1,3 1),POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((2 0,4 0,4 2,2 2,2 0)),LINESTRING(0 0,2 0),LINESTRING(0.5 0,1.5 0))'
    )
    oracle_absorbed_boundary_left = shapely.from_wkt(
        'GEOMETRYCOLLECTION(LINESTRING(3 0,3 2),POINT(0 0),LINESTRING(1 1,3 1),POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((2 0,4 0,4 2,2 2,2 0)),LINESTRING(0 0,2 0),LINESTRING(0.5 0,1.5 0))'
    )
    absorbed_boundary_right = gm.from_wkt('GEOMETRYCOLLECTION(LINESTRING(2 1,3 1))')
    oracle_absorbed_boundary_right = shapely.from_wkt(
        'GEOMETRYCOLLECTION(LINESTRING(2 1,3 1))'
    )
    assert gm.relate(absorbed_boundary_left, absorbed_boundary_right) == relate(
        oracle_absorbed_boundary_left, oracle_absorbed_boundary_right
    )
    assert gm.relate(absorbed_boundary_right, absorbed_boundary_left) == relate(
        oracle_absorbed_boundary_right, oracle_absorbed_boundary_left
    )
