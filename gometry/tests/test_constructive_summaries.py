"""Constructive and overlay operations — buffer/simplify/offset, cleanup,
orientation, normalize, clip, polygonize, summaries, and Z interpolation.
"""

import itertools
import json
from pathlib import Path
from typing import Any, cast

import gometry as gm
import numpy as np
import pytest


@pytest.mark.parametrize('aspect', [100, 100_000, 1_000_000])
def test_inscribed_circle_on_high_aspect_ratio_triangle(aspect: int) -> None:
    polygon = gm.Polygon([(0, 0), (aspect, 1), (2 * aspect, 0), (0, 0)])
    disk = polygon.maximum_inscribed_circle()
    radius = polygon.maximum_inscribed_radius()
    assert disk.geometry_type == 'Polygon'
    assert gm.covers(polygon, disk)
    # The isosceles triangle's inradius is area / semiperimeter. The omitted
    # tolerance is the one-unit short extent divided by 500.
    expected = aspect / (aspect + np.hypot(aspect, 1))
    assert expected - 0.002 <= radius <= expected


def test_inscribed_triangle_radius_matches_shapely() -> None:
    import shapely

    polygon = gm.Polygon([(0, 0), (100, 1), (200, 0), (0, 0)])
    reference = shapely.Polygon([(0, 0), (100, 1), (200, 0), (0, 0)])
    expected = shapely.maximum_inscribed_circle(reference, tolerance=0.002).length
    assert polygon.maximum_inscribed_radius() == pytest.approx(expected, abs=0.002)


@pytest.mark.parametrize(
    'aspect', [10, 30, 100, 300, 1000, 3000, 10_000, 100_000, 1_000_000]
)
@pytest.mark.parametrize('kind', ['triangle', 'trapezoid', 'rectangle'])
def test_pole_searches_meet_their_tolerance_on_thin_nonrectangular_support(
    aspect: int, kind: str
) -> None:
    if kind == 'triangle':
        points = [(0, 0), (aspect, 0), (0, 1), (0, 0)]
        expected = aspect / (aspect + 1 + np.hypot(aspect, 1))
    elif kind == 'trapezoid':
        points = [(0, 0), (aspect, 0), (aspect, 0.9), (0, 1), (0, 0)]
        expected = 1 / (1 + 0.1 / aspect + np.hypot(1, 0.1 / aspect))
    else:
        points = [(0, 0), (aspect, 0), (aspect, 1), (0, 1), (0, 0)]
        expected = 0.5
    polygon = gm.Polygon(points)
    label = polygon.polylabel(tolerance=0.001)
    disk = polygon.maximum_inscribed_circle(tolerance=0.001)
    radius = polygon.maximum_inscribed_radius(tolerance=0.001)
    assert gm.contains(polygon, label)
    assert gm.covers(polygon, disk)
    assert expected - 0.001 <= radius <= expected + 1e-15
    assert (
        expected - 0.001 <= gm.distance(label, polygon.boundary()) <= expected + 1e-15
    )


@pytest.mark.parametrize(
    ('wkt', 'center'),
    [
        ('POLYGON((0 0,2 2,4 4,0 0))', (2, 2)),
        (
            'POLYGON((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))',
            (2, 2),
        ),
        ('POLYGON((8 4,8 8,0 2,8 8,8 4))', (4, 5)),
    ],
)
def test_inscribed_degenerate_polygon_uses_polylabel_representative(
    wkt: str, center: tuple[int, int]
) -> None:
    polygon = gm.from_wkt(wkt)
    circle = polygon.maximum_inscribed_circle()
    label = polygon.polylabel()
    assert circle.geometry_type == 'Point'
    assert (circle.x, circle.y) == center
    assert gm.equals_exact(circle, label)
    assert polygon.maximum_inscribed_radius() == 0
    array = gm.GeometryArray([polygon])
    assert gm.equals_exact(array.maximum_inscribed_circle()[0], circle)
    assert array.maximum_inscribed_radius()[0] == 0


@pytest.mark.parametrize(
    'wkt', ['POLYGON EMPTY', 'MULTIPOLYGON EMPTY', 'MULTIPOLYGON(EMPTY,EMPTY)']
)
def test_empty_polygon_pole_operations_keep_their_typed_empty_contract(
    wkt: str,
) -> None:
    polygon = gm.from_wkt(wkt)
    label = polygon.polylabel()
    circle = polygon.maximum_inscribed_circle()
    assert label.geometry_type == 'Point'
    assert label.is_empty
    assert circle.geometry_type == 'Polygon'
    assert circle.is_empty
    assert np.isnan(polygon.maximum_inscribed_radius())
    array = gm.GeometryArray([polygon])
    assert array.polylabel()[0].is_empty
    assert array.maximum_inscribed_circle()[0].is_empty
    assert np.isnan(array.maximum_inscribed_radius()[0])


@pytest.mark.parametrize(
    ('country', 'attained_radius'),
    [
        ('NZ', 1.5943274923002981),
        ('KI', 0.5287821533585101),
        ('FJ', 1.474348875000004),
        ('TV', 0.3169333745952573),
        ('FR', 3.6830364815762566),
    ],
)
def test_country_pole_clearance_reaches_an_attained_inscribed_radius(
    country: str, attained_radius: float
) -> None:
    fixture = Path(__file__).parents[1] / 'fixtures/osm_countries_0_1.geojson'
    features = json.loads(fixture.read_text())['features']
    geometry = next(
        row['geometry']
        for row in features
        if row['properties']['tags'].get('ISO3166-1:alpha2') == country
    )
    polygon = gm.from_geojson(json.dumps(geometry)).with_crs(None)
    minx, miny, maxx, maxy = polygon.bounds
    tolerance = min(maxx - minx, maxy - miny) / 500
    label = polygon.polylabel()
    # These source clearances were attained by the certified circle search;
    # they are independent witnesses against an inaccurate label search.
    clearance = gm.distance(label, polygon.boundary())
    assert gm.contains(polygon, label)
    assert clearance >= attained_radius - tolerance
    assert polygon.maximum_inscribed_radius() >= attained_radius - tolerance


def test_pole_search_covers_parts_beyond_a_unit_normalized_start_cell() -> None:
    polygon = gm.from_wkt(
        'MULTIPOLYGON('
        '((174 0,178 0,178 4,174 4,174 0)),'
        '((174 6,178 6,178 8,174 8,174 6)),'
        '((-179 12,-178 12,-178 14,-179 14,-179 12)))'
    )
    tolerance = 0.001
    label = polygon.polylabel(tolerance=tolerance)
    clearance = gm.distance(label, polygon.boundary())
    assert gm.contains(polygon, label)
    # The four-unit square has inradius two. The global horizontal chord
    # selects the thinner rectangle, while a unit power-of-two start cell
    # misses all three parts.
    assert 2 - tolerance <= clearance <= 2
    assert polygon.maximum_inscribed_radius(tolerance=tolerance) == 2


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
    voronoi_cells = triangulation_points.voronoi(output='polygons', clip='envelope')
    voronoi_edge_lines = triangulation_points.voronoi(output='edges', clip='envelope')
    clipped_voronoi_cells = triangulation_points.voronoi(
        output='polygons', clip=gm.box(-1, -1, 2, 2, crs=3857)
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
    assert concave_hull.crs == gm.CRS('EPSG:3857')
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
    assert label.crs == gm.CRS('EPSG:3857')
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
    assert triangles.crs == gm.CRS('EPSG:3857')
    assert all(triangle.geometry_type == 'Polygon' for triangle in triangles)
    assert sum(triangle.area for triangle in triangles) == pytest.approx(1)
    assert len(constrained_triangles) > 2
    assert constrained_triangles.crs == gm.CRS('EPSG:3857')
    assert sum(triangle.area for triangle in constrained_triangles) == pytest.approx(
        polygon_with_hole.area
    )
    assert len(polygon_triangles) == 4
    assert polygon_triangles.crs == gm.CRS('EPSG:3857')
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
    assert voronoi_cells.crs == gm.CRS('EPSG:3857')
    assert all(cell.geometry_type == 'Polygon' for cell in voronoi_cells)
    assert sum(cell.area for cell in voronoi_cells) == pytest.approx(1)
    assert len(voronoi_edge_lines) > 0
    assert all(edge.geometry_type == 'LineString' for edge in voronoi_edge_lines)
    assert len(triangulation_points.voronoi(output='polygons', clip='envelope')) == len(
        voronoi_cells
    )
    assert len(
        gm.GeometryArray([triangulation_points]).voronoi(
            output='edges', clip='envelope'
        )[0]
    ) == len(voronoi_edge_lines)
    assert all(
        gm.covers(gm.box(-1, -1, 2, 2, crs=3857), cell)
        for cell in clipped_voronoi_cells
    )
    assert len(gm.MultiPoint([(0, 0), (1, 0)]).triangulate(method='delaunay')) == 0
    assert (
        len(
            gm.Point(0, 0).voronoi(
                output='polygons',
            )
        )
        == 0
    )
    assert (
        len(
            gm.MultiPoint([(0, 0), (1, 0), (2, 0)]).voronoi(
                output='polygons',
            )
        )
        == 3
    )
    with pytest.raises(ValueError, match='unknown Voronoi clip'):
        triangulation_points.voronoi(output='polygons', clip='bad')
    with pytest.raises(TypeError, match='clip geometry must be a Polygon'):
        triangulation_points.voronoi(output='polygons', clip=gm.Point(0, 0))
    clip_box = gm.box(-1, -1, 2, 2)
    with pytest.raises(ValueError, match='matching CRS'):
        triangulation_points.with_crs('EPSG:3857').voronoi(
            output='polygons', clip=clip_box.with_crs('EPSG:4326')
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
    thresholded = points.concave_hull(concavity=0.75, length_threshold=5.0)
    for hull in (permissive, thresholded):
        assert hull.is_valid
        assert all((gm.covers(hull, gm.Point(x, y)) for x, y in coords))
        assert hull.area <= convex.area + 1e-09
    assert gm.equals_exact(thresholded, convex)


def test_concave_hull_larger_length_threshold_contains_previous_hull() -> None:
    sites = [
        (16.34, 28.58),
        (29.7, 85.74),
        (30.12, 52.31),
        (48.05, 46.18),
        (48.46, 36.23),
        (50.07, 79.63),
        (59.0, 65.59),
        (63.85, 46.2),
        (65.39, 94.34),
        (88.05, 51.6),
        (89.45, 78.21),
        (93.66, 10.48),
        (94.44, 21.21),
    ]
    clouds = [('13-site cloud', sites)]
    for seed in (7, 29, 2025):
        cloud = np.random.default_rng(seed).uniform(-50.0, 50.0, size=(40, 2))
        clouds.append((f'fixed-seed cloud {seed}', cloud.tolist()))

    thresholds = (0.0, 2.0, 5.0, 10.0, 25.0, 1_000.0)
    found_distinct_hulls = False
    for name, coords in clouds:
        points = gm.MultiPoint(coords)
        hulls = [
            points.concave_hull(concavity=0.0, length_threshold=threshold)
            for threshold in thresholds
        ]
        for lower_threshold, upper_threshold, smaller, larger in zip(
            thresholds[:-1],
            thresholds[1:],
            hulls[:-1],
            hulls[1:],
            strict=True,
        ):
            assert gm.covers(larger, smaller), (
                f'{name}: threshold {upper_threshold} did not contain '
                f'threshold {lower_threshold}'
            )
            assert larger.area >= smaller.area, (
                f'{name}: area decreased from threshold {lower_threshold} '
                f'to {upper_threshold}'
            )
        found_distinct_hulls |= any(not gm.equals(hulls[0], hull) for hull in hulls[1:])

    assert found_distinct_hulls, 'threshold ladder never changed a hull'


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
    assert hull.crs == gm.CRS('EPSG:3857')
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
    full_polygons, full_cuts, full_dangles, full_invalid = gm.polygonize_all(
        [square_edges], diagnostics=True
    )
    top_level_full = gm.polygonize_all([square_edges], diagnostics=True)
    invalid_full = gm.polygonize_all(
        [gm.MultiLineString([[(0, 0), (1, 0)], [(0, 0), (1, 0)]])], diagnostics=True
    )
    with_hole = hole_edges.polygonize()
    array_polygons = gm.GeometryArray([square_edges]).polygonize()
    pooled_full = gm.polygonize_all([square_edges], diagnostics=True)
    assert len(polygons) == 1
    assert polygons.crs == gm.CRS('EPSG:3857')
    assert polygons[0].geometry_type == 'Polygon'
    assert polygons[0].area == pytest.approx(1)
    assert [
        len(full_polygons),
        *[
            sum(len(row) for row in groups)
            for groups in (full_cuts, full_dangles, full_invalid)
        ],
    ] == [1, 0, 1, 0]
    assert full_polygons.crs == gm.CRS('EPSG:3857')
    assert full_dangles[0][0].to_wkt() == 'LINESTRING (2 2, 3 3)'
    assert top_level_full[0][0].area == pytest.approx(1)
    assert [
        len(invalid_full[0]),
        *[sum(len(row) for row in groups) for groups in invalid_full[1:]],
    ] == [0, 0, 0, 1]
    assert invalid_full[3][0][0].to_wkt() == 'LINESTRING (0 0, 1 0, 0 0)'
    assert len(top_level) == 1
    assert len(array_polygons) == 1
    assert len(pooled_full[0]) == 1
    assert len(pooled_full[2][0]) == 1
    assert pooled_full.polygons.crs == gm.CRS('EPSG:3857')
    assert pooled_full.dangles[0].crs == gm.CRS('EPSG:3857')
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
    result = gm.polygonize_all([ring], diagnostics=True)
    assert result.polygons.crs == gm.CRS('EPSG:4326')
    assert result.polygons.epoch == 2020.0
    for groups in (result.cut_edges, result.dangles, result.invalid_rings):
        assert groups[0].crs == gm.CRS('EPSG:4326')
        assert groups[0].epoch == 2020.0


def test_polygonize_full_diagnostics_keep_source_rows_after_missing_input() -> None:
    joined_cycles = gm.MultiLineString([
        [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)],
        [(1, 0), (2, 0)],
        [(2, 0), (3, 0), (3, 1), (2, 1), (2, 0)],
    ])
    dangle = gm.LineString([(-1, 5), (0, 5)])
    invalid = gm.MultiLineString([[(10, 0), (11, 0)], [(10, 0), (11, 0)]])

    result = gm.polygonize_all(
        gm.GeometryArray([None, joined_cycles, dangle, invalid]), diagnostics=True
    )

    assert len(result.polygons) == 2
    assert [None if row is None else len(row) for row in result.cut_edges] == [
        None,
        1,
        0,
        0,
    ]
    assert [None if row is None else len(row) for row in result.dangles] == [
        None,
        0,
        1,
        0,
    ]
    assert [None if row is None else len(row) for row in result.invalid_rings] == [
        None,
        0,
        0,
        1,
    ]

    cut = list(result.cut_edges[1][0].coords)
    assert tuple(sorted((tuple(cut[0]), tuple(cut[-1])))) == ((1.0, 0.0), (2.0, 0.0))
    dangle_coords = list(result.dangles[2][0].coords)
    assert tuple(sorted((tuple(dangle_coords[0]), tuple(dangle_coords[-1])))) == (
        (-1.0, 5.0),
        (0.0, 5.0),
    )
    invalid_coords = list(result.invalid_rings[3][0].coords)
    assert len(invalid_coords) == 3
    assert invalid_coords[0] == invalid_coords[-1]
    assert {tuple(point) for point in invalid_coords} == {(10.0, 0.0), (11.0, 0.0)}


@pytest.mark.parametrize(
    ('right_crs', 'right_epoch'),
    [(3857, 2020.0), (4326, 2021.0)],
    ids=['crs-mismatch', 'epoch-mismatch'],
)
@pytest.mark.parametrize('diagnostics', [False, True], ids=['polygons', 'diagnostics'])
def test_pooled_polygonize_rejects_crs_and_epoch_mismatches(
    right_crs: int,
    right_epoch: float,
    diagnostics: bool,
) -> None:
    left = gm.LineString([(0, 0), (1, 0)], crs=4326, epoch=2020.0)
    right = gm.LineString([(1, 0), (1, 1)], crs=right_crs, epoch=right_epoch)

    with pytest.raises(gm.CRSMismatchError):
        gm.polygonize_all([left, right], diagnostics=diagnostics)


def test_empty_polygon_rejects_non_iterable_holes() -> None:
    with pytest.raises(TypeError):
        gm.Polygon(None, holes=cast('Any', 42))


def test_polygon_boundary_contact_intersections_dissolve_to_maximal_lines() -> None:
    """Polygon∩polygon boundary contact degenerates to linework/points.

    Only ``intersection`` emits the lower-dimensional contact;
    union/difference/symmetric_difference stay areal. The contact linework is
    dissolved into maximal chains (split only at genuine degree->=3 junctions),
    not one fragment per noded edge — see
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
    kinds = {part.geometry_type for part in mixed.parts}
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
    junctions — rather than one result per noded edge.
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
    assert len(cross.parts) == 4
    sym = gm.symmetric_difference(
        gm.LineString([(0, 0), (3, 0)]), gm.LineString([(1, 0), (5, 0)])
    )
    assert sym.geometry_type == 'MultiLineString'
    assert sym.length == pytest.approx(3.0)


def test_line_merge_dissolves_only_degree_two_nodes() -> None:
    """``line_merge`` joins chains at degree-2 nodes and SPLITS every junction.

    Regression for a greedy endpoint merge that fused two arms straight through
    a degree->=3 junction (and missed ``+0.0``/``-0.0`` joints by comparing raw
    bits). Junction arms remain separate.
    """
    y = gm.from_wkt('MULTILINESTRING ((0 0, 1 1), (1 1, 2 0), (1 1, 1 2))').line_merge()
    assert y.geometry_type == 'MultiLineString'
    assert len(y.parts) == 3
    t = gm.from_wkt('MULTILINESTRING ((0 0, 2 0), (2 0, 4 0), (2 0, 2 2))').line_merge()
    assert len(t.parts) == 3
    chain = gm.from_wkt('MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))').line_merge()
    assert gm.equals(chain, gm.LineString([(0, 0), (1, 1), (2, 2)]))
    loop = gm.from_wkt(
        'MULTILINESTRING ((0 0, 1 0), (1 0, 1 1), (1 1, 0 1), (0 1, 0 0))'
    ).line_merge()
    assert loop.geometry_type == 'LineString'
    assert loop.is_ring
    crossing = gm.from_wkt('MULTILINESTRING ((0 0, 2 2), (2 0, 0 2))').line_merge()
    assert len(crossing.parts) == 2
    signed = gm.union(
        gm.LineString([(-1.0, 0.0), (0.0, 0.0)]),
        gm.LineString([(0.0, -0.0), (1.0, 0.0)]),
    )
    assert gm.equals(signed, gm.LineString([(-1, 0), (1, 0)]))


def test_line_merge_scalar_preserves_reversed_chain_coordinate_order() -> None:
    merged = gm.from_wkt('MULTILINESTRING ((2 0, 1 0), (0 0, 1 0))').line_merge()

    assert merged.coords.to_nested() == [
        (2.0, 0.0),
        (1.0, 0.0),
        (0.0, 0.0),
    ]


def test_line_merge_array_multiline_rows_preserve_nullable_gather_order() -> None:
    reversed_parts = gm.MultiLineString([[(2, 0), (1, 0)], [(0, 0), (1, 0)]])
    forward_parts = gm.MultiLineString([[(10, 0), (11, 0)], [(11, 0), (12, 0)]])
    dense = gm.GeometryArray([reversed_parts, forward_parts])
    dense_merged = dense.line_merge()
    assert dense_merged.coords.to_nested() == [
        [(2.0, 0.0), (1.0, 0.0), (0.0, 0.0)],
        [(10.0, 0.0), (11.0, 0.0), (12.0, 0.0)],
    ]

    nullable = gm.GeometryArray([None, reversed_parts, forward_parts])
    merged = nullable.line_merge()
    assert merged.coords.to_nested() == [
        [(2.0, 0.0), (1.0, 0.0), (0.0, 0.0)],
        [(10.0, 0.0), (11.0, 0.0), (12.0, 0.0)],
    ]
    assert merged.is_missing.tolist() == [True, False, False]

    gathered = nullable[[2, 0, 1, 2]]
    gathered_merged = gathered.line_merge()
    assert gathered_merged.coords.to_nested() == [
        [(10.0, 0.0), (11.0, 0.0), (12.0, 0.0)],
        [(2.0, 0.0), (1.0, 0.0), (0.0, 0.0)],
        [(10.0, 0.0), (11.0, 0.0), (12.0, 0.0)],
    ]
    assert gathered_merged.is_missing.tolist() == [False, True, False, False]


def test_line_merge_all_coincident_multipart_keeps_typed_empty() -> None:
    merged = gm.MultiLineString([[(3, 4), (3, 4)], [(5, 6), (5, 6)]]).line_merge()

    assert merged.geometry_type == 'MultiLineString'
    assert merged.is_empty


def test_line_merge_rejects_geometry_collections() -> None:
    collection = gm.GeometryCollection([gm.LineString([(0, 0), (1, 0)])])

    with pytest.raises(TypeError, match='LineString or MultiLineString'):
        collection.line_merge()


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


def test_overlay_preserves_zero_length_line_point_support() -> None:
    """A zero-length ``LineString`` contributes its point support to results.

    The point remains a valid lower-dimensional result, while ``line_merge``
    ignores the zero-length member when joining genuine linework.
    """
    degenerate = gm.LineString([(8, 1), (8, 1)])
    out = gm.difference(degenerate, gm.box(0, 3, 4, 7))
    assert gm.equals(out, gm.Point(8, 1))
    assert gm.equals(
        gm.difference(degenerate, gm.LineString([(0, 0), (1, 1)])),
        gm.Point(8, 1),
    )
    expected_union = gm.GeometryCollection([gm.Point(8, 1), gm.box(0, 3, 4, 7)])
    assert gm.equals(gm.union(degenerate, gm.box(0, 3, 4, 7)), expected_union)
    assert gm.equals(
        gm.symmetric_difference(degenerate, gm.box(0, 3, 4, 7)), expected_union
    )
    mixed = gm.from_wkt('MULTILINESTRING ((8 1, 8 1), (0 0, 2 0))')
    assert gm.equals(
        gm.difference(mixed, gm.box(20, 20, 30, 30)),
        gm.GeometryCollection([gm.Point(8, 1), gm.LineString([(0, 0), (2, 0)])]),
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
