"""split_antimeridian — geographic seam and pole-closure contract tests."""

import math
from typing import cast

import gometry as gm
import numpy as np
import pytest

from tests._support import (
    EAST_DEGREE_SOUTH_LATITUDE_CRS,
    WEST_RADIAN_SOUTH_GRAD_CRS,
)

pyproj = pytest.importorskip('pyproj', reason='pyproj is an optional oracle dependency')
Geod = pyproj.Geod


def test_split_antimeridian_lines_and_seam_latitudes() -> None:
    line = gm.LineString([(170, -10), (-170, 10)], crs=4326)
    split = line.split_antimeridian()
    assert split.to_wkt() == 'MULTILINESTRING ((170 -10, 180 0), (-180 0, -170 10))'
    assert split.crs == gm.CRS('EPSG:4326')
    multi = gm.from_wkt(
        'MULTILINESTRING ((170 -10, -170 10), (0 0, 1 1))', crs=4326
    ).split_antimeridian()
    assert (
        multi.to_wkt()
        == 'MULTILINESTRING ((170 -10, 180 0), (-180 0, -170 10), (0 0, 1 1))'
    )
    measured = gm.LineString(
        [(170, 0), (-170, 0)], z=[10, 20], m=[1, 3]
    ).split_antimeridian()
    east, west = (list(part.coords) for part in measured.parts)
    assert east[-1] == (180.0, 0.0, 15.0, 2.0)
    assert west[0] == (-180.0, 0.0, 15.0, 2.0)
    for line, expected in (
        (
            gm.LineString(
                [(170.0, 10.0), (-150.0, 30.0)],
                z=[10.0, 50.0],
                m=[1.0, 5.0],
                crs=4326,
            ),
            (15.0, 20.339021393918856, 2.0339021393918855),
        ),
        (
            gm.LineString(
                [(-150.0, 30.0), (170.0, 10.0)],
                z=[50.0, 10.0],
                m=[5.0, 1.0],
                crs=4326,
            ),
            (15.0, 20.339021393918856, 2.0339021393918855),
        ),
    ):
        seam = [
            coordinate[1:]
            for part in line.split_antimeridian().parts
            for coordinate in part.coords
            if abs(coordinate[0]) == 180.0
        ]
        assert len(seam) == 2
        for value in seam:
            assert value == pytest.approx(expected, abs=1.0e-7)


@pytest.mark.parametrize(
    ('z', 'm', 'expected'),
    [
        ([0.0, 100.0], None, (50.43879822149836,)),
        (None, [1000.0, 3000.0], (2008.7759644299672,)),
        ([0.0, 100.0], [1000.0, 3000.0], (50.43879822149836, 2008.7759644299672)),
    ],
    ids=['xyz', 'xym', 'xyzm'],
)
def test_split_antimeridian_tilted_edge_ordinates_follow_ellipsoidal_arclength(
    z: list[float] | None,
    m: list[float] | None,
    expected: tuple[float, ...],
) -> None:
    line = gm.LineString([(170.0, 0.0), (-170.0, 60.0)], z=z, m=m, crs=4326)
    split = line.split_antimeridian()
    seam = [
        coordinate[2:]
        for part in split.parts
        for coordinate in part.coords
        if abs(coordinate[0]) == 180.0
    ]

    assert len(seam) == 2
    for value in seam:
        assert value == pytest.approx(expected, abs=2.0e-9)


def test_overlay_quotients_longitude_aliases_without_rewriting_inputs() -> None:
    left = gm.from_wkt('POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))', crs=4326)
    right = gm.from_wkt(
        'POLYGON ((350 -10, 370 -10, 370 10, 350 10, 350 -10))', crs=4326
    )
    spelling = right.to_wkb()

    outputs = [
        gm.union(left, right),
        gm.union_all([left, right]),
        gm.union_all(gm.GeometryArray([left, right])),
    ]
    for output in outputs:
        assert len(output.parts) == 1
        assert output.parts[0].geometry_type == 'Polygon'
        assert gm.equals(output, left)

    for output in [
        gm.symmetric_difference(left, right),
        gm.symmetric_difference_all([left, right]),
        gm.symmetric_difference_all(gm.GeometryArray([left, right])),
    ]:
        assert output.is_empty
    assert right.to_wkb() == spelling

    seam_union = gm.union(
        gm.box(179.5, -1.0, 180.0, 1.0, crs=4326),
        gm.box(-180.0, -1.0, -179.5, 1.0, crs=4326),
    )
    assert len(seam_union.parts) == 1
    assert not seam_union.is_empty


def test_split_antimeridian_polygons_use_affine_chart_crossings() -> None:
    box = gm.Polygon([(170, 40), (-170, 40), (-170, 50), (170, 50)], crs=4326)
    parts = box.split_antimeridian().parts
    assert [part.geometry_type for part in parts] == ['Polygon', 'Polygon']
    seam_lats = sorted({
        y
        for part in parts
        for x, y in cast('gm.Polygon', part).exterior.coords
        if abs(x) == 180.0
    })
    assert seam_lats == [40.0, 50.0]
    ring = [(-45, 40), (45, 40), (135, 40), (-135, 40)]
    source_cap = gm.Polygon(ring, crs=4326)
    capped = source_cap.split_antimeridian()
    assert capped.geometry_type == 'Polygon'
    coords = list(cast('gm.Polygon', capped).exterior.coords)
    assert (180.0, 90.0) in coords
    assert (-180.0, 90.0) in coords
    assert gm.contains(source_cap, gm.Point(0, 89, crs=4326))
    assert gm.contains(capped, gm.Point(0, 89.9, crs=4326))
    both = gm.from_geojson(
        '{"type": "Polygon", "coordinates": [[[100, 60], [175, 65], [-175, 65], [-120, 0], [-170, -85], [175, -85], [0, -68], [-90, 0], [-90, 80], [0, 85], [100, 60]]]}'
    ).split_antimeridian()
    assert both.geometry_type == 'MultiPolygon'
    assert len(both.parts) == 2
    # A ring touching the seam keeps to its side: its seam vertices take that
    # side's spelling and nothing else changes.
    seam_ring = gm.Polygon([(166, 77), (180, 78), (-180, 81), (162, 81)])
    canonical = seam_ring.split_antimeridian()
    assert canonical.to_wkt() == 'POLYGON ((166 77, 180 78, 180 81, 162 81, 166 77))'
    again = canonical.split_antimeridian()
    assert again.to_wkt() == canonical.to_wkt()
    donut_source = gm.from_geojson(
        '{"type": "Polygon", "coordinates": [[[170, 40], [-170, 40], [-170, 60], [170, 60], [170, 40]],[[175, 45], [-175, 45], [-175, 55], [175, 55], [175, 45]]]}',
        crs=4326,
    )
    donut = donut_source.split_antimeridian()
    assert donut.geometry_type == 'MultiPolygon'
    halves = donut.parts
    assert len(halves) == 2
    for point, expected in (
        (gm.Point(172, 57, crs=4326), True),
        (gm.Point(-172, 57, crs=4326), True),
        (gm.Point(179, 50, crs=4326), False),
    ):
        assert gm.contains(donut_source, point) is expected
        assert gm.contains(donut, point) is expected
    assert any(gm.contains(part, gm.Point(172, 57, crs=4326)) for part in halves)
    assert any(gm.contains(part, gm.Point(-172, 57, crs=4326)) for part in halves)
    assert not any(gm.contains(part, gm.Point(179, 50, crs=4326)) for part in halves)
    plain = gm.box(90, 40, 100, 50, crs=4326)
    assert plain.split_antimeridian().to_wkt() == plain.to_wkt()
    assert gm.equals(
        gm.Point(1, 2, crs=4326).split_antimeridian(), gm.Point(1, 2, crs=4326)
    )
    line = gm.LineString([(170, -10), (-170, 10)], crs=4326)
    rows = gm.GeometryArray([line, gm.LineString([(0, 0), (1, 1)], crs=4326)])
    split_rows = rows.split_antimeridian()
    assert split_rows[0].geometry_type == 'MultiLineString'
    assert split_rows[1].geometry_type == 'LineString'
    assert line.split_antimeridian().to_wkt() == split_rows[0].to_wkt()


def test_multipolygon_perimeter_cancels_partial_periodic_seam_overlap() -> None:
    geometry = gm.from_wkt(
        'MULTIPOLYGON (((170 -10, 180 -10, 180 10, 170 10, 170 -10)),'
        '((-180 0, -170 0, -170 20, -180 20, -180 0)))',
        crs=4326,
    )
    assert geometry.is_valid
    west, east = geometry.parts
    shared = gm.distance(
        gm.Point(180, 0, crs=4326),
        gm.Point(-180, 10, crs=4326),
    )
    expected = west.length + east.length - 2.0 * shared
    assert geometry.length == pytest.approx(expected, abs=1.0e-8)


def test_multipolygon_perimeter_cancels_seam_overlap_with_south_axis() -> None:
    def polygon(points: list[tuple[float, float]]) -> gm.Polygon:
        return gm.Polygon(
            [(x, -y) for x, y in points],
            crs=EAST_DEGREE_SOUTH_LATITUDE_CRS,
        )

    west = polygon([(170, -10), (180, -10), (180, 10), (170, 10), (170, -10)])
    east = polygon([(-180, 0), (-170, 0), (-170, 20), (-180, 20), (-180, 0)])
    geometry = gm.MultiPolygon([west, east])
    shared = gm.distance(
        gm.Point(180, 0, crs=EAST_DEGREE_SOUTH_LATITUDE_CRS),
        gm.Point(180, -10, crs=EAST_DEGREE_SOUTH_LATITUDE_CRS),
    )

    expected = west.length + east.length - 2.0 * shared
    assert geometry.length == pytest.approx(expected, abs=1.0e-8)


@pytest.mark.parametrize('seam', [180.0, -180.0])
@pytest.mark.parametrize(
    'ring',
    [
        # Reaches the seam along an edge and stays east of it.
        [(170.0, 0.0), ('seam', 0.0), ('seam', 10.0), (170.0, 10.0)],
        # Runs along the seam, then leaves to the west: Fiji's outline.
        [
            (177.0, -16.0),
            ('seam', -15.5),
            ('seam', -19.2),
            (-179.0, -21.0),
            ('seam', -19.8),
            (177.0, -19.0),
        ],
    ],
    ids=['touching', 'transiting'],
)
def test_seam_vertex_spelling_does_not_change_validity(
    ring: list[tuple[float | str, float]], seam: float
) -> None:
    spelled = [(seam if x == 'seam' else x, y) for x, y in ring]
    polygon = gm.Polygon(spelled, crs=4326)
    assert polygon.is_valid
    assert gm.union_all([polygon]).is_valid


def test_point_set_centroid_uses_the_common_chart_for_shifted_sites() -> None:
    points = gm.MultiPoint([(370.0, 0.0), (380.0, 10.0)], crs=4326)

    centroid = points.centroid()

    assert (centroid.x, centroid.y) == (15.0, 5.0)


def test_offcenter_ordinary_multipoint_centroid_matches_analytic_mean() -> None:
    points = gm.MultiPoint([(100.0, 0.0), (110.0, 10.0)], crs=4326)

    centroid = points.centroid()

    assert (centroid.x, centroid.y) == (105.0, 5.0)


def test_offcenter_ordinary_multipoint_surface_returns_a_source_point() -> None:
    points = gm.MultiPoint([(100.0, 0.0), (110.0, 10.0)], crs=4326)

    surface_point = points.point_on_surface()

    assert (surface_point.x, surface_point.y) in {(100.0, 0.0), (110.0, 10.0)}


@pytest.mark.parametrize(
    'operation_name',
    ['centroid', 'point_on_surface'],
    ids=['centroid', 'point-on-surface'],
)
def test_late_nan_point_set_error_keeps_source_coordinate_index(
    operation_name: str,
) -> None:
    points = gm.MultiPoint(
        np.array(
            [
                [10.0, 1.0],
                [20.0, 2.0],
                [30.0, 3.0],
                [40.0, 4.0],
                [np.nan, 5.0],
            ],
            dtype=np.float64,
        ),
        crs=4326,
    )

    with pytest.raises(gm.GeometryError, match='member 4, coordinate 0') as error:
        getattr(points, operation_name)()
    assert 'longitude' in str(error.value)


def test_point_set_hull_and_rectangle_use_the_common_chart() -> None:
    points = gm.MultiPoint([(170.0, 0.0), (-170.0, 0.0), (180.0, 10.0)], crs=4326)

    hull = points.convex_hull()
    rectangle = points.minimum_rotated_rectangle()
    rows = gm.GeometryArray([points, points])

    assert list(hull.exterior.coords) == [
        (170.0, 0.0),
        (-170.0, 0.0),
        (-180.0, 10.0),
        (170.0, 0.0),
    ]
    assert list(rectangle.exterior.coords) == [
        (170.0, 0.0),
        (-170.0, 0.0),
        (-170.0, 10.0),
        (170.0, 10.0),
        (170.0, 0.0),
    ]
    assert [list(value.exterior.coords) for value in rows.convex_hull()] == [
        list(hull.exterior.coords),
        list(hull.exterior.coords),
    ]
    assert [
        list(value.exterior.coords) for value in rows.minimum_rotated_rectangle()
    ] == [
        list(rectangle.exterior.coords),
        list(rectangle.exterior.coords),
    ]


def test_point_alias_hull_and_rectangle_keep_the_first_source_record() -> None:
    points = gm.from_wkt('MULTIPOINT Z ((10 0 7), (370 0 11))', crs=4326)

    hull = points.convex_hull()
    rectangle = points.minimum_rotated_rectangle()
    rows = gm.GeometryArray([points])

    for result in (
        hull,
        rectangle,
        rows.convex_hull()[0],
        rows.minimum_rotated_rectangle()[0],
    ):
        assert result.geometry_type == 'Point'
        assert (result.x, result.y, result.z) == (10.0, 0.0, 7.0)


def test_point_hull_keeps_adjacent_native_sites_through_the_published_edge() -> None:
    points = gm.from_wkt(
        'MULTIPOINT Z ((-170 1 1), (-100 1 2), (0.2 1 3), '
        '(0.20000000000000004 1 4), (170 1 5))',
        crs=4326,
    )
    rows = gm.GeometryArray([points])

    for result in (
        points.convex_hull(),
        points.minimum_rotated_rectangle(),
        rows.convex_hull()[0],
        rows.minimum_rotated_rectangle()[0],
    ):
        coordinates = list(result.coords)
        assert result.geometry_type == 'LineString'
        assert coordinates[0] == (170.0, 1.0, 5.0)
        assert coordinates[-1] == (0.20000000000000004, 1.0, 4.0)
        assert len(coordinates) == 3
        assert coordinates[1][2] == 4.5


def test_point_hull_and_rectangle_preserve_scalar_and_packed_point_records() -> None:
    points = [
        gm.Point(370.0, -0.0, z=7.0, m=11.0, crs=4326),
        gm.Point(-170.0, 5.0, z=13.0, m=17.0, crs=4326),
    ]
    rows = gm.GeometryArray(points)

    assert points[0].coordinate_axes == 'XYZM'
    assert points[0].convex_hull().to_wkb() == points[0].to_wkb()
    assert points[0].minimum_rotated_rectangle().to_wkb() == points[0].to_wkb()
    assert [value.to_wkb() for value in rows.convex_hull()] == [
        point.to_wkb() for point in points
    ]
    assert [value.to_wkb() for value in rows.minimum_rotated_rectangle()] == [
        point.to_wkb() for point in points
    ]
    gathered = gm.GeometryArray([points[0], None, points[1], points[0]], crs=4326)[
        [2, 1, 0]
    ]
    for result in (gathered.convex_hull(), gathered.minimum_rotated_rectangle()):
        assert result[0].to_wkb() == points[1].to_wkb()
        assert result[1] is None
        assert result[2].to_wkb() == points[0].to_wkb()


def test_point_set_surface_keeps_first_source_on_a_chart_distance_tie() -> None:
    points = gm.MultiPoint([(370.0, 0.0), (380.0, 10.0)], crs=4326)

    representative = points.point_on_surface()

    assert (representative.x, representative.y) == (370.0, 0.0)
    assert representative.coordinate_axes == 'XY'


def test_late_nan_point_set_surface_error_keeps_source_coordinate_index() -> None:
    points = gm.MultiPoint(
        np.array(
            [
                [10.0, 1.0],
                [20.0, 2.0],
                [30.0, 3.0],
                [40.0, 4.0],
                [50.0, np.nan],
            ],
            dtype=np.float64,
        ),
        crs=4326,
    )

    with pytest.raises(gm.GeometryError, match='member 4, coordinate 0') as error:
        points.point_on_surface()
    assert 'latitude' in str(error.value)


def test_point_only_collection_chart_is_shared_by_scalar_and_array_selection() -> None:
    collection = gm.GeometryCollection(
        [
            gm.MultiPoint([(170.0, 0.0)], crs=4326),
            gm.GeometryCollection([gm.Point(-170.0, 10.0, crs=4326)]),
        ],
        crs=4326,
    )

    centroid = collection.centroid()
    representative = collection.point_on_surface()
    rows = gm.GeometryArray([collection])

    assert (centroid.x, centroid.y) == (-180.0, 5.0)
    assert (representative.x, representative.y) == (170.0, 0.0)
    assert gm.equals(rows.centroid()[0], centroid)
    assert gm.equals(rows.point_on_surface()[0], representative)


def test_gathered_point_sets_keep_chart_results_and_missing_alignment() -> None:
    source = gm.GeometryArray([
        gm.MultiPoint([(170.0, 0.0), (-170.0, 10.0)], crs=4326),
        gm.MultiPoint([(370.0, 0.0), (380.0, 10.0)], crs=4326),
        None,
    ])
    gathered = source[[1, 0, 2]]

    centroids = gathered.centroid()
    representatives = gathered.point_on_surface()

    assert (centroids[0].x, centroids[0].y) == (15.0, 5.0)
    assert (centroids[1].x, centroids[1].y) == (-180.0, 5.0)
    assert centroids[2] is None
    assert (representatives[0].x, representatives[0].y) == (370.0, 0.0)
    assert (representatives[1].x, representatives[1].y) == (170.0, 0.0)
    assert representatives[2] is None


def test_point_only_collection_hulls_and_rectangles_keep_gathered_missing_alignment() -> (
    None
):
    collection = gm.GeometryCollection(
        [
            gm.MultiPoint([(170.0, 0.0)], crs=4326),
            gm.GeometryCollection([gm.Point(-170.0, 10.0, crs=4326)]),
        ],
        crs=4326,
    )
    scalar_hull = collection.convex_hull()
    scalar_rectangle = collection.minimum_rotated_rectangle()
    source = gm.GeometryArray([
        gm.MultiPoint([(170.0, 0.0), (-170.0, 0.0), (180.0, 10.0)], crs=4326),
        gm.MultiPoint([(370.0, 0.0), (380.0, 10.0), (375.0, 20.0)], crs=4326),
        collection,
        None,
    ])
    gathered = source[[1, 0, 2, 3]]

    hulls = gathered.convex_hull()
    rectangles = gathered.minimum_rotated_rectangle()

    assert gm.equals(hulls[0], source[1].convex_hull())
    assert gm.equals(hulls[1], source[0].convex_hull())
    assert list(hulls[2].coords) == [(170.0, 0.0), (-170.0, 10.0)]
    assert gm.equals(hulls[2], scalar_hull)
    assert hulls[3] is None
    assert gm.equals(rectangles[0], source[1].minimum_rotated_rectangle())
    assert gm.equals(rectangles[1], source[0].minimum_rotated_rectangle())
    assert list(rectangles[2].coords) == [(170.0, 0.0), (-170.0, 10.0)]
    assert gm.equals(rectangles[2], scalar_rectangle)
    assert rectangles[3] is None


def test_centroid_and_point_on_surface_array_match_scalar_antimeridian() -> None:
    poly = gm.Polygon(
        [(170, -5), (-170, -5), (-170, 5), (170, 5)],
        crs=4326,
        epoch=2020.0,
    )
    arr = gm.GeometryArray([poly])
    scalar_centroid = poly.centroid()
    array_centroid = arr.centroid()[0]
    assert gm.equals(scalar_centroid, array_centroid)
    assert scalar_centroid.x == pytest.approx(-180.0)
    assert scalar_centroid.y == pytest.approx(0.0)
    scalar_surface = poly.point_on_surface()
    array_surface = arr.point_on_surface()[0]
    assert gm.equals(scalar_surface, array_surface)
    assert gm.covers(poly, scalar_surface)
    assert scalar_surface.coordinate_axes == 'XY'
    assert (scalar_surface.crs, scalar_surface.epoch) == (poly.crs, poly.epoch)
    assert (array_surface.crs, array_surface.epoch) == (poly.crs, poly.epoch)


def test_split_seam_connected_polygon_centroid_matches_unsplit_scalar_and_array() -> (
    None
):
    raw = gm.Polygon(
        [(170, -10), (-170, -10), (-170, 10), (170, 10), (170, -10)],
        crs=4326,
    )
    split = raw.split_antimeridian()
    wrapped = gm.box(170, -10, -170, 10, crs=4326, wrap='split')
    expected = raw.centroid()
    assert (expected.x, expected.y) == (-180.0, 0.0)

    for geometry in (split, wrapped):
        assert gm.equals(raw, geometry)
        centroid = geometry.centroid()
        assert (centroid.x, centroid.y) == (expected.x, expected.y)
        assert gm.contains(geometry, centroid)
        assert gm.contains(geometry, geometry.point_on_surface())

    rows = gm.GeometryArray([split, wrapped])
    centroids = rows.centroid()
    surfaces = rows.point_on_surface()
    for index, geometry in enumerate((split, wrapped)):
        centroid = centroids[index]
        assert (centroid.x, centroid.y) == (expected.x, expected.y)
        assert gm.contains(geometry, centroid)
        assert gm.contains(geometry, surfaces[index])


def test_asymmetric_split_polygon_centroid_matches_unsplit() -> None:
    raw = gm.Polygon(
        [(175, -10), (-165, -10), (-165, 10), (175, 10), (175, -10)],
        crs=4326,
    )
    split = raw.split_antimeridian()
    expected = raw.centroid()
    assert (expected.x, expected.y) == (-175.0, 0.0)

    centroid = split.centroid()
    assert (centroid.x, centroid.y) == (expected.x, expected.y)
    assert gm.contains(split, centroid)


def test_split_line_centroid_matches_unsplit_scalar_and_array() -> None:
    raw = gm.LineString([(170, -10), (-170, 10)], crs=4326)
    split = raw.split_antimeridian()
    expected = raw.centroid()
    assert (expected.x, expected.y) == (-180.0, 0.0)

    centroid = split.centroid()
    assert (centroid.x, centroid.y) == (expected.x, expected.y)
    assert gm.covers(split, centroid)

    array_centroid = gm.GeometryArray([split]).centroid()[0]
    assert (array_centroid.x, array_centroid.y) == (expected.x, expected.y)
    assert gm.covers(split, array_centroid)


def test_disconnected_parts_at_opposite_seam_spellings_keep_planar_centroid() -> None:
    east = gm.box(170, -10, 180, -5, crs=4326)
    west = gm.box(-180, 5, -170, 10, crs=4326)
    geometry = gm.MultiPolygon([east, west], crs=4326)

    # The seam-contact latitude intervals do not overlap, so these unrelated
    # components do not form the paired halves emitted by split_antimeridian.
    centroid = geometry.centroid()
    assert (centroid.x, centroid.y) == (0.0, 0.0)


def test_single_side_seam_touch_and_ordinary_centroids_are_unchanged() -> None:
    one_side = gm.Polygon(
        [(170, -10), (180, -10), (180, 10), (170, 10), (170, -10)],
        crs=4326,
    )
    split = one_side.split_antimeridian()
    assert split.to_wkb() == one_side.to_wkb()
    assert split.centroid().to_wkb() == one_side.centroid().to_wkb()
    assert (split.centroid().x, split.centroid().y) == (175.0, 0.0)

    ordinary = gm.box(1, 2, 3, 4, crs=4326)
    assert (ordinary.centroid().x, ordinary.centroid().y) == (2.0, 3.0)


def test_centroid_uses_native_frame_period_for_grad_seam_scalar_and_array() -> None:
    factor = cast('float', gm.CRS(4807).axes[0]['unit_conversion_factor'])
    scale = math.degrees(factor)
    line = gm.LineString(
        [(170.0 / scale, -10.0 / scale), (-170.0 / scale, 10.0 / scale)],
        crs=4807,
    )
    centroid = line.centroid()
    array_centroid = gm.GeometryArray([line]).centroid()[0]

    # The native seam is ±200 grads, the same location as ±180 degrees. The
    # half-open canonical representative can differ in sign at the seam.
    assert abs(centroid.x * scale) == pytest.approx(180.0)
    assert centroid.y * scale == pytest.approx(0.0)
    assert centroid.crs == gm.CRS('EPSG:4807')
    assert gm.equals(centroid, array_centroid)
    assert abs(array_centroid.x * scale) == pytest.approx(180.0)


def _epsg_4807_degree_scale() -> float:
    factor = cast('float', gm.CRS(4807).axes[0]['unit_conversion_factor'])
    return math.degrees(factor)


def test_grad_centroid_nonsymmetric_crossing_rewraps_away_from_seam() -> None:
    scale = _epsg_4807_degree_scale()
    degree_line = gm.LineString([(170.0, 0.0), (-160.0, 20.0)], crs=4326)
    grad_line = gm.LineString(
        [(170.0 / scale, 0.0), (-160.0 / scale, 20.0 / scale)],
        crs=4807,
    )

    degree_centroid = degree_line.centroid()
    grad_centroid = grad_line.centroid()
    assert degree_centroid.x == pytest.approx(-175.0)
    assert degree_centroid.y == pytest.approx(10.0)
    assert grad_centroid.x * scale == pytest.approx(-175.0)
    assert grad_centroid.y * scale == pytest.approx(10.0)
    assert gm.equals(grad_centroid, gm.GeometryArray([grad_line]).centroid()[0])


def test_grad_centroid_non_crossing_uses_direct_kernel() -> None:
    scale = _epsg_4807_degree_scale()
    ordinary = gm.LineString(
        [(0.0, 0.0), (10.0 / scale, 2.0 / scale)],
        crs=4807,
    )
    ordinary_centroid = ordinary.centroid()
    assert ordinary.crosses_antimeridian is False
    assert ordinary_centroid.x == pytest.approx(5.0 / scale)
    assert ordinary_centroid.y == pytest.approx(1.0 / scale)


def test_grad_centroid_pole_and_interior_use_split_normalization() -> None:
    scale = _epsg_4807_degree_scale()
    # Interior and pole-enclosing routes still use split normalization rather
    # than the unwrapped centroid chart.
    degree_cap = gm.Polygon(
        [(-45.0, 40.0), (45.0, 40.0), (135.0, 40.0), (-135.0, 40.0)],
        crs=4326,
    )
    grad_cap = gm.Polygon(
        [
            (-45.0 / scale, 40.0 / scale),
            (45.0 / scale, 40.0 / scale),
            (135.0 / scale, 40.0 / scale),
            (-135.0 / scale, 40.0 / scale),
        ],
        crs=4807,
    )
    degree_surface = degree_cap.point_on_surface()
    grad_surface = grad_cap.point_on_surface()
    assert grad_surface.x * scale == pytest.approx(degree_surface.x)
    assert grad_surface.y * scale == pytest.approx(degree_surface.y)
    degree_cap_centroid = degree_cap.centroid()
    grad_cap_centroid = grad_cap.centroid()
    assert grad_cap_centroid.x * scale == pytest.approx(degree_cap_centroid.x)
    assert grad_cap_centroid.y * scale == pytest.approx(degree_cap_centroid.y)


def test_packed_bounds_match_scalar_antimeridian_crossing() -> None:
    poly = gm.from_wkt('POLYGON((170 40,-170 40,-170 50,170 50,170 40))', crs=4326)
    assert poly.bounds == (170.0, 40.0, -170.0, 50.0)
    arr = gm.GeometryArray([poly])
    assert np.array_equal(arr.bounds, np.array([[170.0, 40.0, -170.0, 50.0]]))
    assert arr.total_bounds == poly.bounds
    plain = gm.box(150.0, 41.0, 160.0, 45.0, crs=4326)
    mixed = gm.GeometryArray([poly, plain])
    assert np.array_equal(
        mixed.bounds,
        np.array([
            [170.0, 40.0, -170.0, 50.0],
            [150.0, 41.0, 160.0, 45.0],
        ]),
    )
    assert mixed.total_bounds == (150.0, 40.0, -170.0, 50.0)


def test_seam_only_bounds_use_the_canonical_seam() -> None:
    polygon = gm.Polygon(
        [(-180, -30), (180, -30), (180, 30), (-180, 30), (-180, -30)],
        crs='OGC:CRS84',
    )
    expected = (-180.0, -30.0, -180.0, 30.0)
    assert polygon.bounds == expected
    array = gm.GeometryArray([polygon])
    np.testing.assert_array_equal(array.bounds, [expected])
    assert array.total_bounds == expected

    # Both seam spellings denote the same periodic longitude in public bounds.
    assert gm.LineString([(180, 0), (180, 10)], crs=4326).bounds == (
        -180.0,
        0.0,
        -180.0,
        10.0,
    )
    assert gm.LineString([(-180, 0), (180, 10)], crs=4326).bounds == (
        -180.0,
        0.0,
        -180.0,
        10.0,
    )
    assert gm.LineString([(180, 0), (-170, 10)], crs=4326).bounds == (
        -180.0,
        0.0,
        -170.0,
        10.0,
    )

    crossing = gm.from_wkt(
        'POLYGON((170 40,-170 40,-170 50,170 50,170 40))',
        crs=4326,
    )
    assert crossing.bounds == (170.0, 40.0, -170.0, 50.0)
    assert _pole_ring().bounds == (-180.0, 80.0, 180.0, 90.0)


def test_ordinary_geographic_bounds_preserve_endpoint_bits_through_retagging() -> None:
    endpoint = math.nextafter(1.0, 2.0)
    line = gm.LineString([(-0.0, -0.0), (endpoint, endpoint)], crs=4326)
    expected = line.bounds
    assert [value.hex() for value in expected] == [
        (-0.0).hex(),
        (-0.0).hex(),
        endpoint.hex(),
        endpoint.hex(),
    ]

    array = gm.GeometryArray([line], crs=4326)
    assert [float(value).hex() for value in array.bounds[0]] == [
        value.hex() for value in expected
    ]

    untagged = gm.GeometryArray([gm.LineString([(170.0, 0.0), (-170.0, 10.0)])])
    assert untagged.total_bounds == (-170.0, 0.0, 170.0, 10.0)
    retagged = untagged.with_crs(4326)
    assert tuple(retagged.bounds[0]) == (170.0, 0.0, -170.0, 10.0)
    assert retagged.total_bounds == (170.0, 0.0, -170.0, 10.0)


def test_geographic_bounds_keep_invalid_getter_and_empty_full_results() -> None:
    raw = gm.LineString([(0.0, 100.0), (1.0, 100.0)], crs=4326)
    crossing = gm.LineString([(170.0, 100.0), (-170.0, 100.0)], crs=4326)
    assert raw.bounds == (0.0, 100.0, 1.0, 100.0)
    assert crossing.bounds is None
    rows = gm.GeometryArray([raw, crossing], crs=4326)
    assert tuple(rows.bounds[0]) == raw.bounds
    assert np.isnan(rows.bounds[1]).all()

    assert gm.GeometryArray([None], crs=4326).total_bounds is None
    assert gm.box(-180.0, -10.0, 180.0, 10.0, crs=4326).bounds == (
        -180.0,
        -10.0,
        180.0,
        10.0,
    )


def test_crosses_antimeridian_matches_split_frame() -> None:
    for crs in (None, 4269, 4326):
        line = gm.LineString([(170, -10), (-170, 10)], crs=crs)
        assert line.crosses_antimeridian is True
    with pytest.raises(gm.CRSError, match='geographic CRS'):
        _ = gm.LineString([(0, 0), (1, 1)], crs=3857).crosses_antimeridian


@pytest.mark.parametrize('crs', [None, 4326])
@pytest.mark.parametrize(
    'geometry',
    [
        gm.LineString([(-180, 0), (180, 0)]),
        gm.LineString([(0, 0), (360, 0)]),
        gm.Polygon([(-180, -30), (180, -30), (180, 30), (-180, 30)]),
        gm.Polygon([(170, 0), (-180, 0), (-180, 10), (170, 10)]),
    ],
    ids=[
        'seam-to-seam-line',
        'explicit-period-circle',
        'full-longitude-band',
        'touching-spelled-west',
    ],
)
def test_crosses_antimeridian_agrees_across_crs_and_array_lanes(
    geometry: gm.Geometry, crs: int | None
) -> None:
    geometry = geometry.with_crs(crs)
    expected = geometry.crosses_antimeridian
    assert gm.GeometryArray([geometry]).crosses_antimeridian.tolist() == [expected]
    assert geometry.with_crs(4326).crosses_antimeridian is expected


def test_split_antimeridian_gates() -> None:
    with pytest.raises(gm.CRSError, match='requires a geographic CRS'):
        gm.LineString([(170, 0), (-170, 0)], crs=3857).split_antimeridian()
    measured_pole = gm.from_wkt(
        'POLYGON ZM ((-45 40 1 2, 45 40 1 2, 135 40 1 2, -135 40 1 2, -45 40 1 2))',
        crs=3857,
    )
    with pytest.raises(gm.CRSError, match='requires a geographic CRS'):
        measured_pole.split_antimeridian()
    periodic = gm.LineString([(190, 0), (200, 0)]).split_antimeridian()
    assert periodic.to_wkt() == 'LINESTRING (-170 0, -160 0)'
    zm_ring = gm.from_wkt(
        'POLYGON Z ((-45 40 1, 45 40 1, 135 40 1, -135 40 1, -45 40 1))'
    )
    with pytest.raises(gm.InvalidGeometryError, match='pole-closure'):
        zm_ring.split_antimeridian()
    nad = gm.LineString([(170, -10), (-170, 10)], crs=4269).split_antimeridian()
    assert nad.geometry_type == 'MultiLineString'
    rows = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 1)]),
        gm.LineString([(181, 0), (1, 1)]),
        None,
    ]).split_antimeridian()
    assert rows[1].to_wkt() == 'LINESTRING (-179 0, 1 1)'
    assert rows[2] is None


@pytest.mark.parametrize(
    ('wkt', 'axes', 'seam'),
    [
        ('LINESTRING Z (170 0 10, -170 0 20)', 'XYZ', (15.0,)),
        ('LINESTRING M (170 0 1, -170 0 3)', 'XYM', (2.0,)),
        ('LINESTRING ZM (170 0 10 1, -170 0 20 3)', 'XYZM', (15.0, 2.0)),
    ],
)
def test_split_antimeridian_ordinate_policy(
    wkt: str, axes: str, seam: tuple[float, ...]
) -> None:
    split = gm.from_wkt(wkt, crs=4326).split_antimeridian()
    assert split.coordinate_axes == axes
    east = list(split.parts[0].coords)[-1]
    assert east[2:] == seam
    assert split.split_antimeridian().to_wkt() == split.to_wkt()

    # Explicit polygon fixtures keep the policy assertion readable below.
    pole_wkt = {
        'XYZ': 'POLYGON Z ((-45 40 1,45 40 1,135 40 1,-135 40 1,-45 40 1))',
        'XYM': 'POLYGON M ((-45 40 1,45 40 1,135 40 1,-135 40 1,-45 40 1))',
        'XYZM': 'POLYGON ZM ((-45 40 1 2,45 40 1 2,135 40 1 2,-135 40 1 2,-45 40 1 2))',
    }[axes]
    with pytest.raises(gm.InvalidGeometryError, match='cannot preserve Z/M'):
        gm.from_wkt(pole_wkt, crs=4326).split_antimeridian()


def test_crs_free_split_antimeridian_uses_crs84_arclength_for_zm() -> None:
    line = gm.LineString(
        [(170.0, 0.0), (-170.0, 60.0)],
        z=[0.0, 100.0],
        m=[1000.0, 3000.0],
    )

    split = line.split_antimeridian()

    assert split.crs is None
    seam = [
        coordinate
        for part in split.parts
        for coordinate in part.coords
        if abs(coordinate[0]) == 180.0
    ]
    assert len(seam) == 2
    expected = (50.43879822149836, 2008.7759644299672)
    for coordinate in seam:
        assert coordinate[2:] == pytest.approx(expected, abs=2.0e-9)


def test_split_antimeridian_exact_world_band_and_xy_pole() -> None:
    band = gm.Polygon([(0, -30), (360, -30), (360, 30), (0, 30)], crs=4326)
    split = band.split_antimeridian()
    assert split.to_wkt() == (
        'POLYGON ((-180 -30, -90 -30, 0 -30, 90 -30, 180 -30, '
        '180 30, 90 30, 0 30, -90 30, -180 30, -180 -30))'
    )
    for longitude in (-179, 0, 179):
        point = gm.Point(longitude, 0, crs=4326)
        assert gm.contains(band, point)
        assert gm.contains(split, point)
    assert split.split_antimeridian().to_wkt() == split.to_wkt()

    cap = gm.Polygon([(-45, 40), (45, 40), (135, 40), (-135, 40)], crs=4326)
    fixed = cap.split_antimeridian()
    assert gm.contains(cap, gm.Point(0, 89, crs=4326))
    assert gm.contains(fixed.with_crs(None), gm.Point(0, 89))


_RADIAN_CRS = (
    'GEOGCS["WGS 84 rad",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],UNIT["radian",1],'
    'AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]'
)


@pytest.mark.parametrize(
    ('crs', 'period'),
    [(4326, 360.0), (_RADIAN_CRS, math.tau), (4807, 400.0)],
    ids=['degree', 'radian', 'grad'],
)
def test_split_antimeridian_uses_selected_native_period_for_seam_coordinates(
    crs: int | str, period: float
) -> None:
    native_per_degree = period / 360.0
    # Independent WGS84 chart-arclength fractions at t=1/4, integrated from
    # the ellipsoid metric along the retained +40° source edge.
    fraction = 0.25847437372344956 if crs == 4807 else 0.25847553484797166
    for coordinates, z, m in (
        ([(170.0, 10.0), (-150.0, 30.0)], [10.0, 50.0], [1.0, 5.0]),
        ([(-150.0, 30.0), (170.0, 10.0)], [50.0, 10.0], [5.0, 1.0]),
    ):
        line = gm.LineString(
            [
                (longitude * native_per_degree, latitude * native_per_degree)
                for longitude, latitude in coordinates
            ],
            z=z,
            m=m,
            crs=crs,
        )
        seam = [
            coordinate
            for part in line.split_antimeridian().parts
            for coordinate in part.coords
            if abs(coordinate[0]) == period / 2.0
        ]
        assert sorted(coordinate[0] for coordinate in seam) == [
            -period / 2.0,
            period / 2.0,
        ]
        expected = (10.0 + 40.0 * fraction, 1.0 + 4.0 * fraction)
        for coordinate in seam:
            assert coordinate[1] == pytest.approx(15.0 * native_per_degree, abs=1.0e-12)
            assert coordinate[2:] == pytest.approx(expected, abs=1.0e-7)
    rotation = period
    polygon = gm.Polygon(
        [
            (
                longitude * native_per_degree + rotation,
                latitude * native_per_degree,
            )
            for longitude, latitude in (
                (170.0, 0.0),
                (-170.0, 0.0),
                (-170.0, 12.0),
                (170.0, 12.0),
            )
        ],
        crs=crs,
    )
    point = gm.Point(
        175.0 * native_per_degree + rotation,
        6.0 * native_per_degree,
        crs=crs,
    )
    assert gm.contains(polygon, point)
    assert gm.contains(polygon.split_antimeridian(), point)


def test_split_antimeridian_reexpresses_west_south_mixed_axes() -> None:
    # The radian endpoints and grad unit factor are binary64 approximations;
    # these direction-specific latitudes follow those exact stored values.
    for physical, z, m, expected_latitude in (
        (
            [(179.0, 0.0), (-175.0, 6.0)],
            [10.0, 50.0],
            [2.0, 10.0],
            0.9999999999999915,
        ),
        (
            [(-175.0, 6.0), (179.0, 0.0)],
            [50.0, 10.0],
            [10.0, 2.0],
            0.9999999999999919,
        ),
    ):
        native = [
            (-math.radians(longitude), -latitude * 10.0 / 9.0)
            for longitude, latitude in physical
        ]
        split = gm.LineString(
            native, z=z, m=m, crs=WEST_RADIAN_SOUTH_GRAD_CRS
        ).split_antimeridian()
        coordinates = np.asarray(split.to_crs(4326).coords, dtype=np.float64).tolist()
        seam = [point for point in coordinates if abs(point[0]) == 180.0]

        assert len(seam) == 2
        latitude_tolerance = 2.0 * math.ulp(expected_latitude)
        assert all(
            point[1] == pytest.approx(expected_latitude, abs=latitude_tolerance)
            for point in seam
        )
        expected = (16.67246866128446, 3.3344937322568917)
        for point in seam:
            assert point[2:] == pytest.approx(expected, abs=1.0e-7)


@pytest.mark.parametrize(
    ('crs', 'period', 'pole'),
    [(4326, 360.0, 90.0), (_RADIAN_CRS, math.tau, math.pi / 2), (4807, 400.0, 100.0)],
    ids=['degree', 'radian', 'grad'],
)
@pytest.mark.parametrize('north', [False, True], ids=['south', 'north'])
def test_split_antimeridian_normalizes_only_admitted_pole_ulp_in_chart(
    crs: int | str, period: float, pole: float, *, north: bool
) -> None:
    half_turn = period / 2.0
    sign = 1.0 if north else -1.0
    exterior = math.nextafter(sign * pole, math.copysign(math.inf, sign))
    interior = sign * (pole - half_turn / 18.0)
    line = gm.LineString(
        [
            (half_turn - half_turn / 18.0, exterior),
            (-half_turn + half_turn / 18.0, interior),
        ],
        crs=crs,
    )
    split = line.split_antimeridian()
    assert any(y == sign * pole for part in split.parts for _, y in part.coords)
    assert split.split_antimeridian().to_wkt() == split.to_wkt()

    two_outside = math.nextafter(exterior, math.copysign(math.inf, sign))
    invalid = gm.LineString(
        [
            (half_turn - half_turn / 18.0, two_outside),
            (-half_turn + half_turn / 18.0, interior),
        ],
        crs=crs,
    )
    with pytest.raises(
        gm.InvalidGeometryError, match='outside the physical axis limits'
    ):
        invalid.split_antimeridian()


def test_split_antimeridian_review_regressions() -> None:
    doubled = gm.LineString(
        [(170, 0), (170, 0), (-170, 0)], z=[0, 100, 200], m=[0, 10, 20]
    )
    east = list(doubled.split_antimeridian().parts[0].coords)
    assert east[-1] == (180.0, 0.0, 150.0, 15.0)
    two_holes = gm.from_wkt(
        'POLYGON ((170 0, -170 0, -170 30, 170 30, 170 0), (172 5, 174 5, 174 10, 172 10, 172 5), (172 15, 174 15, 174 20, 172 20, 172 15))'
    )
    assert sorted(
        len(cast('gm.Polygon', p).interiors)
        for p in two_holes.split_antimeridian().parts
    ) == [0, 2]
    swath = gm.Polygon([
        (98.0, 60.0),
        (98.6, 73.1),
        (98.8, 80.3),
        (94.1, 87.4),
        (37.99, 89.6),
        (-122.0, 88.9),
        (-134.4, 80.6),
        (-160.0, 70.0),
        (-179.0, 65.0),
        (170.0, 64.0),
        (120.0, 62.0),
    ])
    assert swath.split_antimeridian().geometry_type in ('Polygon', 'MultiPolygon')


def test_split_antimeridian_preserves_south_pole_ownership() -> None:
    """A south-owned ring keeps its cap when seam edges have equal latitudes."""
    ring = [(-45, -40), (45, -40), (135, -40), (-135, -40)]
    source = gm.Polygon(ring, crs=4326)
    split = source.split_antimeridian()
    coords = list(cast('gm.Polygon', split).exterior.coords)
    seam_lats = {y for x, y in coords if abs(x) == 180.0}

    assert -90.0 in seam_lats
    assert 90.0 not in seam_lats
    for longitude in (0.0, 90.0, -90.0, 179.0, -179.0):
        for latitude in (-89.0, -60.0):
            point = gm.Point(longitude, latitude, crs=4326)
            assert gm.contains(source, point)
            assert gm.contains(split, point)
            assert gm.intersects(source, point)
            assert gm.intersects(split, point)
        for latitude in (-39.0, 0.0, 60.0, 89.0):
            point = gm.Point(longitude, latitude, crs=4326)
            assert not gm.contains(source, point)
            assert not gm.contains(split, point)
            assert not gm.intersects(source, point)
            assert not gm.intersects(split, point)
    assert split.split_antimeridian().to_wkt() == split.to_wkt()


def test_split_antimeridian_preserves_north_pole_ownership() -> None:
    """A north-owned ring keeps its cap when seam edges have equal latitudes."""
    ring = [(-45, 40), (45, 40), (135, 40), (-135, 40)]
    source = gm.Polygon(ring, crs=4326)
    split = source.split_antimeridian()
    coords = list(cast('gm.Polygon', split).exterior.coords)
    seam_lats = {y for x, y in coords if abs(x) == 180.0}

    assert 90.0 in seam_lats
    assert -90.0 not in seam_lats
    for longitude in (0.0, 90.0, -90.0, 179.0, -179.0):
        for latitude in (89.0, 60.0):
            point = gm.Point(longitude, latitude, crs=4326)
            assert gm.contains(source, point)
            assert gm.contains(split, point)
            assert gm.intersects(source, point)
            assert gm.intersects(split, point)
        for latitude in (39.0, 0.0, -60.0, -89.0):
            point = gm.Point(longitude, latitude, crs=4326)
            assert not gm.contains(source, point)
            assert not gm.contains(split, point)
            assert not gm.intersects(source, point)
            assert not gm.intersects(split, point)
    assert split.split_antimeridian().to_wkt() == split.to_wkt()


def test_t2_split_reused_notched_overlay_keeps_pole_free_strip_support() -> None:
    source = gm.Polygon(
        [
            (-180, 5),
            (-90, 5),
            (-10, 5),
            (-10, -10),
            (0, -10),
            (10, -10),
            (10, 5),
            (90, 5),
            (180, 5),
            (180, 90),
            (90, 90),
            (0, 90),
            (-90, 90),
            (-180, 90),
            (-180, 5),
        ],
        crs=4326,
    )
    band = gm.box(-180, -30, 180, 30, crs=4326)
    strip = gm.intersection(source, band)
    split = strip.split_antimeridian()

    assert split.is_valid
    assert split.validate().valid
    for point in ((0, -5), (0, 20), (180, 20), (-180, 20)):
        assert gm.covers(split, gm.Point(*point, crs=4326)), point
    for point in ((50, 0), (0, 60), (0, 90), (0, -90)):
        assert not gm.contains(split, gm.Point(*point, crs=4326)), point
    assert split.area == pytest.approx(strip.area)
    assert split.length == pytest.approx(strip.length)
    assert split.split_antimeridian().to_wkt() == split.to_wkt()


def test_split_antimeridian_both_poles_reverse_winding() -> None:
    """Inverted winding that implies both poles reverses segments instead of capping."""
    source = gm.from_geojson(
        '{"type": "Polygon", "coordinates": [[[100, -60], [175, -65], [-175, -65], [-120, 0], [-170, 85], [175, 85], [0, 68], [-90, 0], [-90, -80], [0, -85], [100, -60]]]}'
    )
    split = source.split_antimeridian()
    assert split.geometry_type in ('Polygon', 'MultiPolygon')
    assert not split.is_empty
    for longitude, latitude, contains, intersects in (
        (0.0, 0.0, True, True),
        (90.0, 0.0, True, True),
        (-90.0, 0.0, False, True),
        (-90.0, 30.0, False, False),
        (0.0, 89.0, False, False),
        (0.0, -89.0, False, False),
    ):
        point = gm.Point(longitude, latitude, crs=4326)
        assert gm.contains(source, point) is contains
        assert gm.contains(split, point) is contains
        assert gm.intersects(source, point) is intersects
        assert gm.intersects(split, point) is intersects
    again = split.split_antimeridian()
    assert again.to_wkt() == split.to_wkt()


def test_split_antimeridian_tilted_full_turn_non_dyadic_ordinates() -> None:
    line = gm.LineString(
        [(170.0, 0.0), (530.0, 60.0)],
        z=[0.0, 100.0],
        m=[100.0, 1100.0],
        crs=4326,
    )
    split = line.split_antimeridian()
    coordinates = [coordinate for part in split.parts for coordinate in part.coords]
    seam = [coordinate for coordinate in coordinates if abs(coordinate[0]) == 180.0]
    assert len(seam) >= 2
    assert {-180.0, 180.0} <= {coordinate[0] for coordinate in seam}
    for coordinate in seam:
        expected_y = 60.0 / 36.0
        expected_z = 3.332353583809422
        expected_m = 133.32353583809422
        assert abs(coordinate[1] - expected_y) <= 4.0 * math.ulp(expected_y)
        assert abs(coordinate[2] - expected_z) <= 4.0 * math.ulp(expected_z)
        assert abs(coordinate[3] - expected_m) <= 4.0 * math.ulp(expected_m)
    assert len({(coordinate[2], coordinate[3]) for coordinate in seam}) == 1
    quarter = [
        coordinate for coordinate in coordinates if coordinate[:2] == (-100.0, 15.0)
    ]
    assert quarter
    expected_z = 29.665344452905156
    expected_m = 396.6534445290516
    assert abs(quarter[0][2] - expected_z) <= 4.0 * math.ulp(expected_z)
    assert abs(quarter[0][3] - expected_m) <= 4.0 * math.ulp(expected_m)


def test_split_antimeridian_reversed_grad_hole_overlay_ordinates() -> None:
    crs = (
        'GEOGCS["West south grad",DATUM["Custom datum",'
        'SPHEROID["Custom",7000000,150]],PRIMEM["Greenwich",0],'
        'UNIT["grad",0.015707963267948967],'
        'AXIS["Longitude",WEST],AXIS["Latitude",SOUTH]]'
    )
    lower_owner_z = 38.36571009570337
    lower_owner_m = 383.6571009570337
    upper_owner_z = 76.731420191406755
    upper_owner_m = 767.31420191406755
    polygon = gm.from_wkt(
        f'POLYGON ZM ('
        '(-150 50 0 0, 150 50 0 0, 150 -80 100 1000, '
        '-150 -80 100 1000, -150 50 0 0),'
        f'(-160 0 {lower_owner_z!r} {lower_owner_m!r}, '
        f'-160 -50 {upper_owner_z!r} {upper_owner_m!r}, '
        f'-200 -50 {upper_owner_z!r} {upper_owner_m!r}, '
        f'-200 0 {lower_owner_z!r} {lower_owner_m!r}, '
        f'-160 0 {lower_owner_z!r} {lower_owner_m!r}))',
        crs=crs,
    )

    split = polygon.split_antimeridian()
    assert split.coordinate_axes == 'XYZM'
    # The lower seam owner rounds one ULP below the compensated Simpson reference.
    for native_latitude, owner_z, owner_m, expected_z, expected_m in (
        (0.0, lower_owner_z, lower_owner_m, 38.365710095703378, 383.65710095703378),
        (-50.0, upper_owner_z, upper_owner_m, 76.731420191406755, 767.31420191406755),
    ):
        seam = [
            coordinate
            for part in split.parts
            for coordinate in part.coords
            if abs(coordinate[0]) == 200.0 and coordinate[1] == native_latitude
        ]
        assert seam, f'missing final seam node at latitude {native_latitude}'
        for coordinate in seam:
            assert (coordinate[2].hex(), coordinate[3].hex()) == (
                owner_z.hex(),
                owner_m.hex(),
            )
            assert abs(coordinate[2] - expected_z) <= 4.0 * math.ulp(expected_z)
            assert abs(coordinate[3] - expected_m) <= 4.0 * math.ulp(expected_m)
        assert len({(coordinate[2], coordinate[3]) for coordinate in seam}) == 1


def test_split_antimeridian_crs84_canonical_identity_and_full_turn_rewrite() -> None:
    half_period = gm.LineString([(180, 0), (0, 0)], crs=4326)
    assert half_period.split_antimeridian().to_wkt() == half_period.to_wkt()
    full_period = gm.LineString([(-180, 0), (180, 0)], crs=4326)
    assert full_period.split_antimeridian().to_wkt() == full_period.to_wkt()
    two_periods = gm.LineString([(0, 0), (720, 0)], crs=4326).split_antimeridian()
    assert two_periods.geometry_type == 'LineString'
    assert len(two_periods.coords) == 2

    circle = gm.LineString(
        [(0, 0), (360, 0)], z=[10, 110], m=[2, 202], crs=4326
    ).split_antimeridian()
    assert circle.geometry_type == 'MultiLineString'
    circle_coords = [coordinate for part in circle.parts for coordinate in part.coords]
    assert any(coordinate[:2] == (90.0, 0.0) for coordinate in circle_coords)
    assert any(coordinate[:2] == (-90.0, 0.0) for coordinate in circle_coords)
    circle_seam = [
        coordinate for coordinate in circle_coords if abs(coordinate[0]) == 180.0
    ]
    assert {-180.0, 180.0} <= {coordinate[0] for coordinate in circle_seam}
    assert all(coordinate[2:] == (60.0, 102.0) for coordinate in circle_seam)

    reverse_circle = gm.LineString(
        [(0, 0), (-360, 0)], z=[10, 110], m=[2, 202], crs=4326
    ).split_antimeridian()
    reverse_coords = [
        coordinate for part in reverse_circle.parts for coordinate in part.coords
    ]
    assert any(coordinate == (-90.0, 0.0, 35.0, 52.0) for coordinate in reverse_coords)
    assert any(coordinate == (90.0, 0.0, 85.0, 152.0) for coordinate in reverse_coords)

    band_zm = gm.Polygon(
        [(0, -30), (360, -30), (360, 30), (0, 30)],
        z=[0, 100, 200, 300],
        m=[100, 500, 2000, 3000],
        crs=4326,
    ).split_antimeridian()
    assert band_zm.coordinate_axes == 'XYZM'
    band_coords = np.asarray(band_zm.coords, dtype=np.float64)
    assert np.any(
        np.all(
            np.isclose(
                band_coords,
                [90.0, 30.0, 225.0, 2250.0],
                rtol=0.0,
                atol=2.0e-9,
            ),
            axis=1,
        )
    )
    collection = gm.GeometryCollection(
        [gm.LineString([(0, 0), (360, 0)], z=[10, 110], m=[2, 202])],
        crs=4326,
    ).split_antimeridian()
    assert collection.geometry_type == 'GeometryCollection'
    assert 'MULTILINESTRING' in collection.to_wkt()

    # A pole-to-pole world ring needs ordinary longitude witnesses; seam
    # aliases alone have no full-turn edge.
    world = gm.Polygon(
        [
            (-180, -90),
            (-90, -90),
            (0, -90),
            (90, -90),
            (180, -90),
            (180, 90),
            (90, 90),
            (0, 90),
            (-90, 90),
            (-180, 90),
            (-180, -90),
        ],
        crs=4326,
    )
    split_world = world.split_antimeridian()
    assert split_world.to_wkt() == world.to_wkt()
    for point in (
        gm.Point(0.0, 0.0, crs=4326),
        gm.Point(179.0, 0.0, crs=4326),
        gm.Point(-179.0, 0.0, crs=4326),
    ):
        assert gm.contains(world, point)
        assert gm.contains(split_world, point)
        assert gm.intersects(world, point)
        assert gm.intersects(split_world, point)
    with_holes = gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
        holes=[[(2, 2), (4, 2), (4, 4), (2, 4), (2, 2)]],
        crs=4326,
    )
    assert with_holes.split_antimeridian().to_wkt() == with_holes.to_wkt()
    nested = gm.GeometryCollection(
        [
            gm.LineString([(1, 3), (2, 4)], z=[10, 20], m=[7, 8]),
            gm.GeometryCollection([gm.Point(5, 6, z=1, m=2)]),
        ],
        crs=4326,
    )
    assert nested.split_antimeridian().to_wkt() == nested.to_wkt()
    band = gm.Polygon([(0, -30), (360, -30), (360, 30), (0, 30)], crs=4326)
    assert band.split_antimeridian().to_wkt() == (
        'POLYGON ((-180 -30, -90 -30, 0 -30, 90 -30, 180 -30, '
        '180 30, 90 30, 0 30, -90 30, -180 30, -180 -30))'
    )


def test_split_antimeridian_full_longitude_band() -> None:
    """A ring spanning the full longitude band splits and stays non-empty."""
    band = gm.Polygon([(170, -30), (-170, -30), (-170, 30), (170, 30)], crs=4326)
    split = band.split_antimeridian()
    assert split.geometry_type == 'MultiPolygon'
    assert not split.is_empty
    assert len(split.parts) == 2
    for longitude in (175.0, -175.0):
        point = gm.Point(longitude, 0.0, crs=4326)
        assert gm.contains(band, point)
        assert gm.contains(split, point)
    for latitude in (40.0, -40.0):
        point = gm.Point(175.0, latitude, crs=4326)
        assert not gm.contains(band, point)
        assert not gm.contains(split, point)
    assert any(gm.contains(part, gm.Point(175, 0, crs=4326)) for part in split.parts)
    assert any(gm.contains(part, gm.Point(-175, 0, crs=4326)) for part in split.parts)


def test_split_antimeridian_line_crossing_north_pole() -> None:
    """A meridional line through the pole splits into seam-bounded pieces."""
    line = gm.LineString([(0, 80), (0, -80)], crs=4326)
    split = line.split_antimeridian()
    assert split.geometry_type == 'LineString'
    assert not split.is_empty
    coords = list(split.coords)
    assert any((abs(x) == 180.0 for x, _ in coords)) or len(coords) >= 2


_CROSSING_BAND = [(170, 40), (-170, 40), (-170, 50), (170, 50)]
_TOPOLOGY_PREDICATES = (
    'contains',
    'within',
    'covers',
    'covered_by',
    'contains_properly',
    'intersects',
    'disjoint',
    'touches',
    'overlaps',
    'crosses',
    'equals',
)
_OVERLAY_OPS = ('intersection', 'union', 'difference', 'symmetric_difference')


def _crossing_band_polygon(*, crs: int | str = 4326) -> gm.Polygon:
    return gm.Polygon(_CROSSING_BAND, crs=crs)


def _crossing_band_probes() -> list[gm.Geometry]:
    return [
        gm.Point(175, 45, crs=4326),
        gm.box(-10, 40, 10, 50, crs=4326),
        gm.box(172, 42, 178, 48, crs=4326),
    ]


def test_crossing_polygon_predicates_match_split_form() -> None:
    """Unsplit antimeridian-crossing polygons answer predicates like the split."""
    poly = _crossing_band_polygon()
    split = poly.split_antimeridian()
    for predicate in _TOPOLOGY_PREDICATES:
        for probe in _crossing_band_probes():
            raw = getattr(gm, predicate)(poly, probe)
            fixed = getattr(gm, predicate)(split, probe)
            assert raw == fixed, (predicate, probe.to_wkt(), raw, fixed)


def test_crossing_polygon_concrete_contains_and_intersects() -> None:
    poly = _crossing_band_polygon()
    assert gm.contains(poly, gm.Point(175, 45, crs=4326))
    assert gm.contains(poly, gm.Point(-175, 45, crs=4326))
    assert not gm.contains(poly, gm.Point(0, 45, crs=4326))
    assert not gm.intersects(poly, gm.box(-10, 40, 10, 50, crs=4326))


@pytest.mark.parametrize(
    ('height', 'longitude'),
    [
        pytest.param(12.0, 175.0, id='north'),
        pytest.param(12.0, -185.0, id='north-negative-alias'),
        pytest.param(12.0, 535.0, id='north-positive-alias'),
        pytest.param(12.0, 180.0, id='north-positive-seam'),
        pytest.param(12.0, -180.0, id='north-negative-seam'),
        pytest.param(-12.0, 175.0, id='south'),
        pytest.param(-12.0, -185.0, id='south-negative-alias'),
        pytest.param(-12.0, 535.0, id='south-positive-alias'),
        pytest.param(-12.0, 180.0, id='south-positive-seam'),
        pytest.param(-12.0, -180.0, id='south-negative-seam'),
    ],
)
def test_crossing_polygon_chart_predicates_match_split(
    height: float, longitude: float
) -> None:
    polygon = gm.Polygon(
        [
            (170.0, 0.0),
            (-170.0, 0.0),
            (-170.0, height),
            (170.0, height),
            (170.0, 0.0),
        ],
        crs=4326,
    )
    split = polygon.split_antimeridian()
    for latitude, contains, intersects in (
        (math.copysign(11.95, height), True, True),
        (height, False, True),
        (math.copysign(12.05, height), False, False),
    ):
        point = gm.Point(longitude, latitude, crs=4326)
        assert gm.contains(polygon, point) is contains
        assert gm.intersects(polygon, point) is intersects
        assert gm.contains(split, point) is contains
        assert gm.intersects(split, point) is intersects


def test_split_antimeridian_is_invariant_under_longitude_rotation() -> None:
    coordinates = [(170.0, 0.0), (-170.0, 0.0), (-170.0, 12.0), (170.0, 12.0)]
    expected = gm.Polygon(coordinates, crs=4326).split_antimeridian()
    for rotation in (-360.0, 360.0, 720.0):
        rotated = gm.Polygon(
            [(longitude + rotation, latitude) for longitude, latitude in coordinates],
            crs=4326,
        )
        point = gm.Point(175.0 + rotation, 6.0, crs=4326)
        assert gm.contains(rotated, point)
        assert gm.intersects(rotated, point)
        split = rotated.split_antimeridian()
        assert gm.contains(split, point)
        assert gm.intersects(split, point)
        assert gm.equals(split, expected)


def test_split_antimeridian_preserves_straight_chart_under_longitude_rotation() -> None:
    crossing = gm.Polygon(
        [(170.0, 0.0), (-170.0, 0.0), (-170.0, 12.0), (170.0, 12.0)],
        crs=4326,
    )
    ordinary = gm.Polygon(
        [(100.0, 0.0), (120.0, 0.0), (120.0, 12.0), (100.0, 12.0)],
        crs=4326,
    )
    crossing_split = crossing.split_antimeridian()
    for latitude, contains, intersects in (
        (11.95, True, True),
        (12.0, False, True),
        (12.05, False, False),
    ):
        seam_point = gm.Point(175.0, latitude, crs=4326)
        rotated_point = gm.Point(105.0, latitude, crs=4326)
        assert gm.contains(crossing, seam_point) is contains
        assert gm.intersects(crossing, seam_point) is intersects
        assert gm.contains(crossing_split, seam_point) is contains
        assert gm.intersects(crossing_split, seam_point) is intersects
        assert gm.contains(ordinary, rotated_point) is contains
        assert gm.intersects(ordinary, rotated_point) is intersects


def test_crossing_polygon_contains_broadcast_agrees() -> None:
    poly = _crossing_band_polygon()
    pt = gm.Point(175, 45, crs=4326)
    arr_poly = gm.GeometryArray([poly])
    arr_pt = gm.GeometryArray([pt])
    scalar = gm.contains(poly, pt)
    assert scalar is True
    assert gm.contains(poly, arr_pt)[0] == scalar
    assert gm.contains(arr_poly, pt)[0] == scalar
    assert gm.contains(arr_poly, arr_pt)[0] == scalar


def test_crossing_polygon_overlay_and_operator_match_split() -> None:
    poly = _crossing_band_polygon()
    split = poly.split_antimeridian()
    box = gm.box(172, 42, 178, 48, crs=4326)
    for op_name in _OVERLAY_OPS:
        op = getattr(gm, op_name)
        raw_area = op(poly, box).area
        fixed_area = op(split, box).area
        assert abs(raw_area - fixed_area) < 1.0, op_name
    assert (poly | box).area == pytest.approx((split | box).area, abs=1.0)


def test_crossing_polygon_relate_centroid_bounds_point_on_surface() -> None:
    poly = _crossing_band_polygon()
    split = poly.split_antimeridian()
    box = gm.box(172, 42, 178, 48, crs=4326)
    assert gm.relate(poly, box) == gm.relate(split, box)
    centroid_lon = poly.centroid().x
    assert abs(centroid_lon) >= 170.0
    assert abs(centroid_lon) <= 180.0
    pos_lon = poly.point_on_surface().x
    assert pos_lon >= 170.0 or pos_lon <= -170.0
    assert poly.bounds == (170.0, 40.0, -170.0, 50.0)


def test_crossing_polygon_clip_by_rect_scalar_and_array() -> None:
    poly = _crossing_band_polygon()
    split = poly.split_antimeridian()
    east_rect = (172, 42, 178, 48)
    assert poly.clip_by_rect(*east_rect).area == pytest.approx(
        split.clip_by_rect(*east_rect).area
    )
    rows = gm.GeometryArray([poly, split])
    east_clips = rows.clip_by_rect(*east_rect)
    assert east_clips[0].area == pytest.approx(east_clips[1].area)
    cross_rect = (175, 42, -175, 48)
    cross_clip = poly.clip_by_rect(*cross_rect)
    half_union = gm.union(
        poly.clip_by_rect(175, 42, 180, 48), poly.clip_by_rect(-180, 42, -175, 48)
    )
    assert cross_clip.area == pytest.approx(half_union.area)
    cross_rows = gm.GeometryArray([poly]).clip_by_rect(*cross_rect)
    half_rows = gm.GeometryArray([poly]).clip_by_rect(175, 42, 180, 48)
    west_rows = gm.GeometryArray([poly]).clip_by_rect(-180, 42, -175, 48)
    assert cross_rows[0].area == pytest.approx(
        gm.union(half_rows[0], west_rows[0]).area
    )


def test_geographic_clip_by_rect_canonical_boundaries_match_scalar_and_array() -> None:
    cases = [
        (
            gm.LineString([(-2, 0), (2, 0)], crs=4326),
            (-1, -1, 1, 1),
            gm.LineString([(-1, 0), (1, 0)], crs=4326),
        ),
        (
            gm.LineString([(175, 0), (-175, 0)], crs=4326),
            (170, -1, -170, 1),
            gm.LineString([(175, 0), (-175, 0)], crs=4326),
        ),
        (
            gm.LineString([(-10, 0), (10, 0)], crs=4326),
            (-180, -1, 180, 1),
            gm.LineString([(-10, 0), (10, 0)], crs=4326),
        ),
        (
            gm.LineString([(180, -2), (180, 2)], crs=4326),
            (180, -1, 180, 1),
            gm.LineString([(180, -1), (180, 1)], crs=4326),
        ),
    ]
    for shape, rect, expected in cases:
        assert gm.equals(shape.clip_by_rect(*rect), expected)
        assert gm.equals(gm.GeometryArray([shape]).clip_by_rect(*rect)[0], expected)


def test_geographic_clip_by_rect_resolves_longitude_and_pole_aliases() -> None:
    noncanonical = gm.Point(360, 0, crs=4326)
    pole = gm.Point(0, 90, crs=4326)
    cases = [
        (noncanonical, (-1, -1, 1, 1), gm.Point(0, 0, crs=4326)),
        (pole, (10, 80, 20, 90), gm.Point(10, 90, crs=4326)),
        (
            gm.LineString([(170, 0), (-170, 0)], crs=4326),
            (175, -1, 180, 1),
            gm.LineString([(175, 0), (180, 0)], crs=4326),
        ),
        (gm.Point(0, 0, crs=4326), (-1, -1, 1, 1), gm.Point(0, 0, crs=4326)),
    ]
    for shape, rect, expected in cases:
        clipped = shape.clip_by_rect(*rect)
        assert gm.equals(clipped, expected)
        assert gm.equals(gm.GeometryArray([shape]).clip_by_rect(*rect)[0], expected)


@pytest.mark.parametrize(
    ('shape', 'rect'),
    [
        (
            gm.LineString([(-175, 0), (0, 0), (175, 0)], crs=4326),
            (170, -1, 190, 1),
        ),
        (
            gm.LineString([(-175, 0), (0, 0), (175, 0)], crs=4326),
            (190, -1, -170, 1),
        ),
        (
            gm.LineString([(-175, 0), (0, 0), (175, 0)], crs=4326),
            (-190, -1, 170, 1),
        ),
    ],
    ids=['ordered-alias', 'crossing-alias', 'west-out-of-domain'],
)
def test_geographic_clip_by_rect_rejects_endpoints_outside_the_frame(
    shape: gm.Geometry, rect: tuple[float, float, float, float]
) -> None:
    for clipped in (
        lambda: shape.clip_by_rect(*rect),
        lambda: gm.GeometryArray([shape]).clip_by_rect(*rect),
    ):
        with pytest.raises(ValueError, match='geographic rectangle bounds'):
            clipped()


def test_geographic_clip_by_rect_rejects_unordered_latitudes_scalar_and_array() -> None:
    shape = gm.LineString([(-2, 0), (2, 0)], crs=4326)
    rect = (1, 1, -1, -1)
    for clipped in (
        lambda: shape.clip_by_rect(*rect),
        lambda: gm.GeometryArray([shape]).clip_by_rect(*rect),
    ):
        with pytest.raises(ValueError, match='geographic rectangle bounds'):
            clipped()


def test_grad_clip_by_rect_matches_wrapped_box_in_scalar_mixed_and_packed_paths() -> (
    None
):
    point = gm.Point(195, 0, crs=4807)
    rect = (190, -1, -190, 1)
    wrapped = gm.box(*rect, crs=4807, wrap='split')
    expected = gm.intersection(point, wrapped)

    scalar = point.clip_by_rect(*rect)
    assert scalar.to_wkb() == expected.to_wkb()
    assert gm.equals(scalar, point)

    mixed = gm.GeometryArray([
        point,
        gm.LineString([(-199, 0), (199, 0)], crs=4807),
    ])[0:1]
    mixed_result = mixed.clip_by_rect(*rect)[0]
    assert mixed_result.to_wkb() == expected.to_wkb()

    packed = gm.GeometryArray([point]).clip_by_rect(*rect)[0]
    assert packed.to_wkb() == expected.to_wkb()

    wrapped_rows = gm.boxes([190], [-1], [-190], [1], crs=4807, wrap='split')
    assert gm.equals(wrapped_rows[0], wrapped)

    with pytest.raises(ValueError, match='geographic rectangle bounds'):
        point.clip_by_rect(201, -1, 190, 1)
    with pytest.raises(ValueError, match='geographic rectangle bounds'):
        gm.box(201, -1, 190, 1, crs=4807, wrap='split')
    with pytest.raises(ValueError, match='geographic rectangle bounds'):
        point.clip_by_rect(190, -1, -190, 101)


def test_packed_geographic_clip_preserves_canonical_rows_and_missingness() -> None:
    canonical_line = gm.LineString([(-2, 0), (0, 0), (2, 0)], crs=4326)
    canonical_expected = gm.LineString([(-1, 0), (0, 0), (1, 0)], crs=4326)
    assert gm.equals(
        gm.GeometryArray([canonical_line]).clip_by_rect(-1, -1, 1, 1)[0],
        canonical_expected,
    )

    rows = gm.GeometryArray([
        canonical_line,
        gm.LineString([], crs=4326),
        canonical_line,
    ])
    rows = rows._with_missing([False, False, True]).clip_by_rect(-1, -1, 1, 1)
    assert gm.equals(rows[0], canonical_expected)
    assert rows[1].to_wkt() == 'LINESTRING EMPTY'
    assert rows[2] is None


def test_crossing_line_and_multiline_predicates_and_planar_ops() -> None:
    """Crossing linework uses geographic predicates; simplify/hull stay planar."""
    line = gm.LineString([(170, 10), (-170, 10)], crs=4326)
    meridian = gm.LineString([(180, 0), (180, 20)], crs=4326)
    assert gm.intersects(line, meridian)
    assert gm.distance(line, meridian) == 0.0
    multi = gm.MultiLineString([[(170, 10), (-170, 10)], [(0, 0), (1, 1)]], crs=4326)
    assert gm.intersects(multi, meridian)
    assert multi.length > 0.0
    assert line.simplify(1.0).to_wkt() == 'LINESTRING (170 10, -170 10)'
    hull = line.convex_hull()
    assert list(hull.coords) == [(-170.0, 10.0), (0.0, 10.0), (170.0, 10.0)]
    assert gm.covers(hull, gm.Point(0, 10, crs=4326))
    assert not gm.covers(hull, gm.Point(180, 10, crs=4326))


def test_crossing_line_geographic_distance_to_seam_is_zero() -> None:
    crossing = gm.LineString([(170, 10), (-170, 10)], crs=4326)
    seam = gm.LineString([(180, 0), (180, 20)], crs=4326)
    assert gm.distance(crossing, seam) == 0.0


def test_geodesic_distance_measures_split_crossing_polygons() -> None:
    split = _crossing_band_polygon().split_antimeridian()
    east_lobe = gm.box(172, 42, 178, 48, crs=4326)
    inside = gm.Point(175, 45, crs=4326)
    assert gm.distance(split, east_lobe) == 0.0
    assert gm.dwithin(split, east_lobe, 0.0)
    assert gm.distance(split, inside) == 0.0
    assert gm.dwithin(split, inside, 0.0)
    assert gm.distance(split, east_lobe, unit='planar') == 0.0
    assert gm.distance(split, inside, unit='planar') == 0.0


def test_geodesic_distance_measures_unsplit_crossing_polygon() -> None:
    poly = _crossing_band_polygon()
    inside = gm.Point(175, 45, crs=4326)
    east_lobe = gm.box(172, 42, 178, 48, crs=4326)
    assert gm.distance(poly, inside) == 0.0
    assert gm.distance(poly, east_lobe) == 0.0
    assert gm.dwithin(poly, inside, 0.0)
    assert gm.dwithin(poly, east_lobe, 0.0)


def test_nad83_crossing_polygon_predicates_match_split_form() -> None:
    """EPSG:4269 shares the antimeridian predicate frame with WGS84."""
    poly = _crossing_band_polygon(crs=4269)
    split = poly.split_antimeridian()
    pt = gm.Point(175, 45, crs=4269)
    assert gm.contains(poly, pt) == gm.contains(split, pt)
    assert gm.intersects(poly, gm.box(172, 42, 178, 48, crs=4269)) == gm.intersects(
        split, gm.box(172, 42, 178, 48, crs=4269)
    )


@pytest.mark.parametrize(
    'base_latitude',
    [
        pytest.param(0.0, id='equator'),
        pytest.param(40.0, id='mid-latitude'),
    ],
)
def test_thin_crossing_band_split_preserves_chart_area_and_support(
    base_latitude: float,
) -> None:
    height = 1e-8
    polygon = gm.Polygon(
        [
            (170.0, base_latitude),
            (-170.0, base_latitude),
            (-170.0, base_latitude + height),
            (170.0, base_latitude + height),
            (170.0, base_latitude),
        ],
        crs=4326,
    )
    split = polygon.split_antimeridian()
    parts = list(split.parts)
    unwrapped_width = 20.0
    # The analytic rectangle uses the stored binary64 endpoint height, not
    # the decimal request that may round at a nonzero base latitude.
    represented_height = (base_latitude + height) - base_latitude
    expected_piece_area = unwrapped_width * represented_height / 2.0

    assert len(parts) == 2
    assert gm.area(parts[0], unit='planar') == expected_piece_area
    assert gm.area(parts[1], unit='planar') == expected_piece_area
    assert gm.area(split, unit='planar') == unwrapped_width * represented_height
    assert polygon.bounds == (170.0, base_latitude, -170.0, base_latitude + height)
    assert split.bounds == (
        170.0,
        base_latitude,
        -170.0,
        base_latitude + height,
    )
    for longitude in (175.0, -175.0, 180.0, -180.0):
        point = gm.Point(longitude, base_latitude + height / 2.0, crs=4326)
        assert gm.contains(polygon, point)
        assert gm.contains(split, point)


def test_spatial_index_handles_crossing_geographic() -> None:
    poly = _crossing_band_polygon()
    idx = gm.SpatialIndex(
        gm.GeometryArray([poly, gm.box(0, 0, 10, 10, crs=4326)], crs=4326)
    )
    assert sorted(idx.query(gm.Point(175, 45, crs=4326), predicate='intersects')) == [0]
    assert sorted(idx.query(gm.Point(0, 45, crs=4326), predicate='intersects')) == []
    assert sorted(
        idx.query(gm.box(172, 42, 178, 48, crs=4326), predicate='intersects')
    ) == [0]
    assert (
        sorted(idx.query(gm.box(-10, 40, 10, 50, crs=4326), predicate='intersects'))
        == []
    )
    east = gm.SpatialIndex(
        gm.GeometryArray([gm.box(174, 42, 178, 48, crs=4326)], crs=4326)
    )
    assert sorted(east.query(poly, predicate='intersects')) == [0]


def test_prepared_geometry_handles_crossing_geographic() -> None:
    poly = _crossing_band_polygon()
    assert gm.contains(poly, gm.Point(175, 45, crs=4326)) is True
    assert gm.contains(poly, gm.Point(0, 45, crs=4326)) is False
    probes = gm.GeometryArray(
        [gm.Point(175, 45, crs=4326), gm.Point(0, 45, crs=4326)], crs=4326
    )
    assert [bool(x) for x in gm.contains(poly, probes)] == [True, False]


def test_spatial_index_handles_pole_enclosing_geographic() -> None:
    hex_north = gm.Polygon(
        [(0, 80), (60, 80), (120, 80), (180, 80), (-120, 80), (-60, 80)], crs=4326
    )
    idx = gm.SpatialIndex(
        gm.GeometryArray([hex_north, gm.Point(1, 1, crs=4326)], crs=4326)
    )
    assert sorted(idx.query(gm.Point(0, 90, crs=4326), predicate='intersects')) == [0]
    assert sorted(idx.query(gm.Point(0, -90, crs=4326), predicate='intersects')) == []


def _crossing_band() -> 'gm.Polygon':
    return gm.Polygon([(170, -10), (-170, -10), (-170, 10), (170, 10)], crs=4326)


def _pole_ring() -> 'gm.Polygon':
    return gm.Polygon(
        [(-179, 80), (-120, 80), (-60, 80), (0, 80), (60, 80), (120, 80), (179, 80)],
        crs=4326,
    )


def _latitude_ring(latitude: float) -> list[tuple[float, float]]:
    return [
        (0, latitude),
        (60, latitude),
        (120, latitude),
        (180, latitude),
        (-120, latitude),
        (-60, latitude),
    ]


def test_xy_predicates_auto_split_antimeridian() -> None:
    poly = _crossing_band()
    for x, y in [(178, 0), (-178, 0), (175, 5)]:
        assert gm.contains(poly, x=x, y=y) == gm.contains(
            poly, gm.Point(x, y, crs=4326)
        )
        assert gm.intersects(poly, x=x, y=y) == gm.intersects(
            poly, gm.Point(x, y, crs=4326)
        )
    assert not gm.contains(poly, x=0, y=0)
    assert list(gm.contains(poly, x=[178, 0, -178], y=[0, 0, 0])) == [True, False, True]
    assert gm.contains(poly, x=178, y=0)


def test_vertex_at_pole_predicates_are_consistent() -> None:
    tri = gm.Polygon([(-10, 80), (10, 80), (0, 90)], crs=4326)
    pole = gm.Point(0, 90, crs=4326)
    assert gm.intersects(tri, pole) and gm.covers(tri, pole) and gm.touches(tri, pole)
    assert not gm.contains(tri, pole) and (not gm.disjoint(tri, pole))
    assert gm.intersects(tri, gm.Point(180, 90, crs=4326))
    ring = _pole_ring()
    assert gm.contains(ring, pole) and (not gm.touches(ring, pole))
    collection = gm.GeometryCollection([ring, tri], crs=4326)
    assert gm.contains(collection, pole)
    assert not gm.touches(collection, pole)

    # The polar enclosure shortcut is areal-only: it must not intercept the
    # ordinary point kernel for a non-areal container.
    assert gm.intersects(pole, pole)
    assert not gm.disjoint(pole, pole)
    assert gm.contains(pole, pole)
    assert gm.covers(pole, pole)
    assert not gm.touches(pole, pole)


def test_xy_predicates_match_full_topology_at_poles() -> None:
    cap = gm.Polygon(_latitude_ring(80), crs=4326)
    for longitude in (0.0, 180.0, -180.0):
        pole = gm.Point(longitude, 90, crs=4326)
        assert gm.contains(cap, x=longitude, y=90) == gm.contains(cap, pole) is True
        assert gm.intersects(cap, x=longitude, y=90) == gm.intersects(cap, pole) is True
        assert gm.contains(cap, x=longitude, y=90) is True
        assert gm.intersects(cap, x=longitude, y=90) is True

    boundary = gm.Polygon([(-10, 80), (10, 80), (0, 90)], crs=4326)
    for longitude in (0.0, 180.0, -180.0):
        assert gm.contains(boundary, x=longitude, y=90) is False
        assert gm.intersects(boundary, x=longitude, y=90) is True
        assert gm.contains(boundary, x=longitude, y=90) is False
        assert gm.intersects(boundary, x=longitude, y=90) is True


def test_polar_prepared_and_free_point_batches_keep_original_topology() -> None:
    cap = gm.Polygon(_latitude_ring(80), crs=4326)
    probes = gm.GeometryArray(
        [gm.Point(0, 90, crs=4326), gm.Point(0, 85, crs=4326), None],
        crs=4326,
    )
    for evaluate in (gm.contains, gm.intersects, gm.covers):
        np.testing.assert_array_equal(evaluate(cap, probes), [True, True, False])
    for name in ('contains', 'intersects', 'covers'):
        np.testing.assert_array_equal(
            getattr(gm, name)(cap, probes), [True, True, False]
        )


def test_xy_pole_broadcast_array_and_missing_rows() -> None:
    north = gm.Polygon(_latitude_ring(80), crs=4326)
    south = gm.Polygon([(x, -y) for x, y in _latitude_ring(80)], crs=4326)
    annulus = gm.Polygon(_latitude_ring(70), holes=[_latitude_ring(80)], crs=4326)
    rows = gm.GeometryArray([north, south, None, annulus], crs=4326)

    np.testing.assert_array_equal(
        gm.contains(rows, x=[0, 0, 0, 180], y=[90, -90, 90, 75]),
        [True, True, False, True],
    )
    np.testing.assert_array_equal(
        gm.intersects(rows, x=[0, 0, 0, 180], y=[90, -90, 90, 75]),
        [True, True, False, True],
    )
    np.testing.assert_array_equal(
        gm.contains(rows, x=0, y=90), [True, False, False, False]
    )
    np.testing.assert_array_equal(
        gm.contains(rows, x=[0, 180, 0, 180], y=90),
        [True, False, False, False],
    )
    probes = gm.GeometryArray(
        [
            gm.Point(0, 90, crs=4326),
            gm.Point(0, -90, crs=4326),
            gm.Point(0, 90, crs=4326),
            gm.Point(180, 75, crs=4326),
        ],
        crs=4326,
    )
    np.testing.assert_array_equal(gm.contains(rows, probes), [True, True, False, True])
    np.testing.assert_array_equal(
        gm.intersects(rows, probes), [True, True, False, True]
    )


def test_polar_annulus_split_membership_bounds_and_index() -> None:
    cap = gm.Polygon(_latitude_ring(80), crs=4326)
    annulus = gm.Polygon(_latitude_ring(70), holes=[_latitude_ring(80)], crs=4326)
    split = annulus.split_antimeridian()
    assert split.is_valid
    assert annulus.bounds == (-180.0, 70.0, 180.0, 80.0)

    for longitude in (0.0, 180.0, -180.0):
        for latitude, expected in ((89.0, False), (75.0, True), (65.0, False)):
            point = gm.Point(longitude, latitude, crs=4326)
            assert gm.contains(annulus, point) is expected
            assert gm.contains(split, point) is expected
            assert gm.contains(annulus, x=longitude, y=latitude) is expected
            assert gm.contains(split, x=longitude, y=latitude) is expected
        for latitude in (70.0, 80.0):
            point = gm.Point(longitude, latitude, crs=4326)
            assert not gm.contains(annulus, point)
            assert not gm.contains(split, point)
            assert gm.intersects(annulus, point)
            assert gm.intersects(split, point)
            assert not gm.contains(annulus, x=longitude, y=latitude)
            assert not gm.contains(split, x=longitude, y=latitude)
            assert gm.intersects(annulus, x=longitude, y=latitude)
            assert gm.intersects(split, x=longitude, y=latitude)

    index = gm.SpatialIndex([cap, annulus])
    assert list(index.query(gm.Point(180, 89, crs=4326), predicate='intersects')) == [0]
    assert list(index.query(gm.Point(180, 75, crs=4326), predicate='intersects')) == [1]
    assert list(index.query(gm.Point(0, 75, crs=4326), predicate='intersects')) == [1]


@pytest.mark.parametrize(
    (
        'shell_latitude',
        'hole_latitude',
        'inside_latitude',
        'hole_probe',
        'outside_latitude',
    ),
    [
        (70.0, 80.0, 75.0, 85.0, 65.0),
        (60.0, 75.0, 67.5, 80.0, 50.0),
    ],
    ids=['70-80', '60-75'],
)
@pytest.mark.parametrize('south', [False, True], ids=['north', 'south'])
def test_polar_annulus_split_preserves_chart_area_length_and_membership(
    shell_latitude: float,
    hole_latitude: float,
    inside_latitude: float,
    hole_probe: float,
    outside_latitude: float,
    *,
    south: bool,
) -> None:
    direction = -1.0 if south else 1.0
    annulus = gm.Polygon(
        _latitude_ring(direction * shell_latitude),
        holes=[_latitude_ring(direction * hole_latitude)],
        crs=4326,
    )
    split = annulus.split_antimeridian()

    assert annulus.is_valid
    assert split.is_valid
    assert split.area == pytest.approx(annulus.area, rel=1e-15)
    assert split.length == pytest.approx(annulus.length, rel=1e-13)
    for longitude in (0.0, 180.0, -180.0):
        for latitude, expected in (
            (direction * inside_latitude, True),
            (direction * hole_probe, False),
            (direction * outside_latitude, False),
        ):
            point = gm.Point(longitude, latitude, crs=4326)
            assert gm.contains(annulus, point) is expected
            assert gm.contains(split, point) is expected
            assert gm.contains(annulus, x=longitude, y=latitude) is expected
            assert gm.contains(split, x=longitude, y=latitude) is expected


def test_split_seam_donut_preserves_area_and_membership() -> None:
    # Existing seam vertices keep the split on the source chart edges.
    shell = [(160, -20), (180, -20), (-160, -20), (-160, 20), (180, 20), (160, 20)]
    hole = [(170, -5), (180, -5), (-170, -5), (-170, 5), (180, 5), (170, 5)]
    donut = gm.Polygon(shell, holes=[hole], crs=4326)
    split = donut.split_antimeridian()

    assert donut.is_valid
    assert split.is_valid
    assert split.area == pytest.approx(donut.area, rel=1e-15)
    assert split.length == pytest.approx(donut.length, rel=1e-13)
    for longitude, latitude, expected in (
        (180.0, 0.0, False),
        (-180.0, 0.0, False),
        (165.0, 0.0, True),
        (-165.0, 0.0, True),
        (180.0, 10.0, True),
        (-180.0, 10.0, True),
        (155.0, 0.0, False),
        (-155.0, 0.0, False),
    ):
        point = gm.Point(longitude, latitude, crs=4326)
        assert gm.contains(donut, point) is expected
        assert gm.contains(split, point) is expected
        assert gm.contains(donut, x=longitude, y=latitude) is expected
        assert gm.contains(split, x=longitude, y=latitude) is expected


def test_split_polar_annulus_carries_resolvable_zm() -> None:
    longitudes = (0, 60, 120, 180, -120, -60)
    shell = [(longitude, 70, 5, 50) for longitude in longitudes]
    hole = [(longitude, 80, 5, 50) for longitude in longitudes]
    annulus = gm.Polygon(shell, holes=[hole], crs=4326)
    split = annulus.split_antimeridian()
    coordinates = np.asarray(split.coords, dtype=np.float64)

    assert annulus.is_valid
    assert split.is_valid
    assert split.coordinate_axes == 'XYZM'
    assert split.area == annulus.area
    np.testing.assert_array_equal(
        coordinates[:, 2:], np.tile([5.0, 50.0], (len(coordinates), 1))
    )


def test_geographic_validation_and_repair_use_normalized_topology() -> None:
    shell = _latitude_ring(70)
    hole = _latitude_ring(80)
    planar = gm.Polygon(shell, holes=[hole])
    annulus = gm.Polygon(shell, holes=[hole], crs=4326)

    # Frame semantics are deliberate and cache-safe: the same stored
    # coordinates are invalid in a planar frame but form a valid geographic
    # annulus once the antimeridian and polar hole are normalized.
    assert not planar.is_valid
    assert not planar.is_simple
    assert not planar.validate().valid
    assert annulus.is_valid
    assert annulus.is_simple
    assert annulus.validate().valid
    assert gm.require(annulus, crs=4326).to_wkt() == annulus.to_wkt()
    with pytest.raises(gm.InvalidGeometryError, match='self-intersection'):
        gm.require(planar)

    # CRS re-tagging shares the frozen coordinates. A cached planar verdict
    # must never leak into the geographic frame, or vice versa.
    assert planar.with_crs(4326).is_valid
    assert not annulus.with_crs(None).is_valid
    planar_rows = gm.GeometryArray([planar])
    np.testing.assert_array_equal(planar_rows.is_valid, [False])
    np.testing.assert_array_equal(planar_rows.with_crs(4326).is_valid, [True])

    repaired = annulus.repair()
    report_repaired = annulus.repair()
    snapped = annulus.snap_to_grid(1, repair=True)
    for unchanged in (repaired, report_repaired, snapped):
        assert unchanged.to_wkt() == annulus.to_wkt()
        assert unchanged.crs == annulus.crs
        assert unchanged.is_valid

    assert len(annulus.self_intersections()) == 0
    assert len(planar.self_intersections()) > 0

    rows = gm.GeometryArray([annulus, annulus], crs=4326)
    np.testing.assert_array_equal(rows.is_valid, [True, True])
    np.testing.assert_array_equal(rows.is_simple, [True, True])
    assert all(report is not None and report.valid for report in rows.validate())
    assert rows.repair().to_wkt() == rows.to_wkt()
    assert [len(group) for group in rows.self_intersections()] == [0, 0]
    missing = gm.GeometryArray([annulus, None], crs=4326)
    np.testing.assert_array_equal(missing.is_valid, [True, False])
    assert missing.validate()[0] is not None and missing.validate()[0].valid
    assert missing.validate()[1] is None
    assert missing.repair()[1] is None

    ring = gm.LineString([*shell, shell[0]], crs=4326)
    assert ring.is_closed and ring.is_simple and ring.is_ring
    assert len(ring.self_intersections()) == 0


def test_geographic_validation_does_not_hide_an_outside_crossing_hole() -> None:
    shell = [(170, -10), (-170, -10), (-170, 10), (170, 10), (170, -10)]
    outside_hole = [(170, 20), (-170, 20), (-170, 30), (170, 30), (170, 20)]
    invalid = gm.Polygon(shell, holes=[outside_hole], crs=4326)

    assert not invalid.is_valid
    report = invalid.validate()
    assert not report.valid
    assert report.reason is not None and 'not contained' in report.reason
    with pytest.raises(gm.InvalidGeometryError, match='not contained'):
        gm.require(invalid, crs=4326)

    # Repair may deliberately use the same set-difference assembly to discard
    # the outside hole; validation itself must never do that implicitly.
    repaired = invalid.repair()
    assert repaired.is_valid
    assert gm.equals(repaired, gm.Polygon(shell, crs=4326))

    nested_holes = gm.Polygon(
        _latitude_ring(60),
        holes=[_latitude_ring(70), _latitude_ring(80)],
        crs=4326,
    )
    assert not nested_holes.is_valid
    nested_report = nested_holes.validate()
    assert (
        nested_report.reason is not None
        and 'intersect on an area' in nested_report.reason
    )

    shared_edge_hole = [(170, -10), (-170, -10), (-170, 0), (170, 0), (170, -10)]
    shared_edge = gm.Polygon(shell, holes=[shared_edge_hole], crs=4326)
    assert not shared_edge.is_valid
    shared_report = shared_edge.validate()
    assert shared_report.reason is not None and 'on a line' in shared_report.reason

    invalid = gm.Polygon(
        [(160, 0), (170, 10), (160, 10), (170, 0), (-170, 5)], crs=4326
    )
    assert not invalid.is_valid
    assert not invalid.is_simple
    assert not invalid.validate().valid
    assert len(invalid.self_intersections()) > 0
    assert invalid.repair().is_valid

    annulus = gm.Polygon(_latitude_ring(70), holes=[_latitude_ring(80)], crs=4326)
    mixed = gm.GeometryArray([annulus, invalid], crs=4326)
    np.testing.assert_array_equal(mixed.is_valid, [True, False])
    repaired_mixed = mixed.repair()
    np.testing.assert_array_equal(repaired_mixed.is_valid, [True, True])
    assert repaired_mixed[0].to_wkt() == annulus.to_wkt()

    unrepairable = gm.Polygon(
        [(170, 0), (-170, 0), (170, 10), (-170, 10), (175, 5)], crs=4326
    )
    with pytest.raises(
        gm.InvalidGeometryError, match='repair did not produce valid geometry'
    ):
        unrepairable.repair()


def test_artificial_seam_is_not_a_topological_boundary() -> None:
    crossing = gm.LineString([(170, 0), (-170, 0)], crs=4326)
    endpoint = gm.LineString([(170, 0), (180, 0)], crs=4326)
    for longitude in (180.0, -180.0):
        point = gm.Point(longitude, 0, crs=4326)
        assert gm.contains(crossing, point)
        assert not gm.touches(crossing, point)
        assert gm.contains(crossing, x=longitude, y=0)
    assert gm.covers(endpoint, gm.Point(180, 0, crs=4326))
    assert gm.touches(endpoint, gm.Point(180, 0, crs=4326))
    assert not gm.contains(endpoint, x=180, y=0)


def test_derived_points_and_bounds_handle_pole_enclosure() -> None:
    ring = _pole_ring()
    assert gm.contains(ring, ring.point_on_surface())
    assert gm.contains(ring, ring.centroid())
    bounds = ring.bounds
    assert bounds is not None and bounds[3] == 90.0
    band = _crossing_band()
    assert gm.contains(band, band.point_on_surface())


def test_geodesic_shortest_line_uses_crossing_polygon_boundary() -> None:
    poly = _crossing_band()
    probe = gm.Point(179, 12, crs=4326)
    left, right = gm.nearest_points(poly, probe)
    assert left.y == 10.0
    assert 170.0 <= left.x <= 180.0
    assert (right.x, right.y) == (probe.x, probe.y)

    geod = Geod(ellps='WGS84')
    sampled_minimum = min(
        geod.inv(179.0, 12.0, 170.0 + 20.0 * step / 4096.0, 10.0)[2]
        for step in range(4097)
    )
    witnesses = (
        gm.shortest_line(poly, probe),
        gm.shortest_line(poly.split_antimeridian(), probe),
        gm.shortest_line(gm.GeometryArray([poly]), probe)[0],
    )
    for shortest in witnesses:
        (lon, lat), (query_lon, query_lat) = list(shortest.coords)
        assert lat == 10.0
        assert 170.0 <= lon <= 180.0
        assert (query_lon, query_lat) == (probe.x, probe.y)
        assert geod.inv(probe.x, probe.y, lon, lat)[2] <= sampled_minimum


def test_zm_polygon_crossing_antimeridian_overlay() -> None:
    zp = gm.from_wkt(
        'POLYGON Z ((179 -1 5, -179 -1 5, -179 1 5, 179 1 5, 179 -1 5))', crs=4326
    )
    assert not gm.intersection(zp, gm.box(178, -2, 180, 2, crs=4326)).is_empty
    assert not gm.union(zp, gm.box(178, -2, 180, 2, crs=4326)).is_empty
    pole_z = gm.from_wkt(
        'POLYGON Z ((-179 80 5, -60 80 5, 60 80 5, 179 80 5, -179 80 5))', crs=4326
    )
    with pytest.raises(gm.InvalidGeometryError, match='cannot preserve Z/M'):
        pole_z.split_antimeridian()


def test_relate_and_relate_pattern_split_on_array_and_freefn() -> None:
    poly = _crossing_band()
    probe = gm.Point(175, 0, crs=4326)
    scalar = gm.relate(poly, probe)
    assert scalar == gm.relate(poly.split_antimeridian(), probe)
    assert list(gm.relate(gm.GeometryArray([poly]), probe)) == [scalar]
    assert gm.relate_pattern(poly, probe, 'T*****FF*')
    assert list(gm.relate_pattern(gm.GeometryArray([poly]), probe, 'T*****FF*')) == [
        True
    ]
