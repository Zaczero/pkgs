"""split_antimeridian — geographic seam and pole-closure contract tests."""

import math
from typing import cast

import gometry as gm
import numpy as np
import pytest


def _great_circle_crossing_latitude(
    start: tuple[float, float], end: tuple[float, float]
) -> float:
    """Return the antimeridian crossing latitude from the plane equation."""
    if start[0] > end[0]:
        start, end = end, start

    longitude_1, latitude_1 = map(math.radians, start)
    longitude_2, latitude_2 = map(math.radians, end)
    normal_x = math.cos(latitude_1) * math.sin(longitude_1) * math.sin(
        latitude_2
    ) - math.sin(latitude_1) * math.cos(latitude_2) * math.sin(longitude_2)
    normal_z = (
        math.cos(latitude_1)
        * math.cos(latitude_2)
        * math.sin(longitude_2 - longitude_1)
    )
    vertical = normal_x if normal_z >= 0.0 else -normal_x
    return math.degrees(math.atan2(vertical, abs(normal_z)))


def test_split_antimeridian_lines_and_seam_latitudes() -> None:
    line = gm.LineString([(170, -10), (-170, 10)], crs=4326)
    split = line.split_antimeridian()
    assert split.to_wkt() == 'MULTILINESTRING ((170 -10, 180 0), (-180 0, -170 10))'
    assert split.crs == 'EPSG:4326'
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
    east, west = (list(part.coords) for part in gm.parts(measured))
    assert east[-1] == (180.0, 0.0, 15.0, 2.0)
    assert west[0] == (-180.0, 0.0, 15.0, 2.0)


def test_overlay_quotients_longitude_aliases_without_rewriting_inputs() -> None:
    left = gm.from_wkt(
        'POLYGON ((-10 -10, 10 -10, 10 10, -10 10, -10 -10))', crs=4326
    )
    right = gm.from_wkt(
        'POLYGON ((350 -10, 370 -10, 370 10, 350 10, 350 -10))', crs=4326
    )
    spelling = right.to_wkb()

    outputs = [
        gm.union(left, right),
        gm.union_all([left, right]),
        gm.GeometryArray([left, right]).union_all(),
    ]
    for output in outputs:
        assert len(gm.parts(output)) == 1
        assert gm.parts(output)[0].geometry_type == 'Polygon'
        assert gm.equals(output, left)

    for output in [
        gm.symmetric_difference(left, right),
        gm.symmetric_difference_all([left, right]),
        gm.GeometryArray([left, right]).symmetric_difference_all(),
    ]:
        assert output.is_empty
    assert right.to_wkb() == spelling

    seam_union = gm.union(
        gm.box(179.5, -1.0, 180.0, 1.0, crs=4326),
        gm.box(-180.0, -1.0, -179.5, 1.0, crs=4326),
    )
    assert len(gm.parts(seam_union)) == 1
    assert not seam_union.is_empty


def test_split_antimeridian_polygons_use_great_circle_crossings() -> None:
    box = gm.Polygon([(170, 40), (-170, 40), (-170, 50), (170, 50)], crs=4326)
    parts = gm.parts(box.split_antimeridian())
    assert [part.geometry_type for part in parts] == ['Polygon', 'Polygon']
    seam_lats = sorted({
        round(y, 7)
        for part in parts
        for x, y in cast('gm.Polygon', part).exterior.coords
        if abs(x) == 180.0
    })
    expected_latitudes = sorted(
        round(_great_circle_crossing_latitude((170.0, latitude), (-170.0, latitude)), 7)
        for latitude in (40.0, 50.0)
    )
    assert seam_lats == expected_latitudes
    ring = [(-45, 40), (45, 40), (135, 40), (-135, 40)]
    capped = gm.Polygon(ring).split_antimeridian()
    assert capped.geometry_type == 'Polygon'
    coords = list(cast('gm.Polygon', capped).exterior.coords)
    assert (180.0, 90.0) in coords
    assert (-180.0, 90.0) in coords
    assert gm.contains(capped, gm.Point(0, 89.9))
    both = gm.from_geojson(
        '{"type": "Polygon", "coordinates": [[[100, 60], [175, 65], [-175, 65], [-120, 0], [-170, -85], [175, -85], [0, -68], [-90, 0], [-90, 80], [0, 85], [100, 60]]]}'
    ).split_antimeridian()
    assert both.geometry_type == 'MultiPolygon'
    assert len(gm.parts(both)) == 2
    seam_ring = gm.Polygon([(166, 77), (180, 78), (-180, 81), (162, 81)])
    canonical = seam_ring.split_antimeridian()
    assert canonical.to_wkt() == seam_ring.to_wkt()
    again = canonical.split_antimeridian()
    assert again.to_wkt() == canonical.to_wkt()
    donut = gm.from_geojson(
        '{"type": "Polygon", "coordinates": [[[170, 40], [-170, 40], [-170, 60], [170, 60], [170, 40]],[[175, 45], [-175, 45], [-175, 55], [175, 55], [175, 45]]]}',
        crs=4326,
    ).split_antimeridian()
    assert donut.geometry_type == 'MultiPolygon'
    halves = gm.parts(donut)
    assert len(halves) == 2
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


def test_centroid_and_point_on_surface_array_match_scalar_antimeridian() -> None:
    poly = gm.Polygon([(170, -5), (-170, -5), (-170, 5), (170, 5)], crs=4326)
    arr = gm.GeometryArray([poly])
    scalar_centroid = poly.centroid()
    array_centroid = arr.centroid()[0]
    assert gm.equals(scalar_centroid, array_centroid)
    assert scalar_centroid.x == pytest.approx(-180.0)
    assert scalar_centroid.y == pytest.approx(0.0)
    scalar_surface = poly.point_on_surface()
    array_surface = arr.point_on_surface()[0]
    assert gm.equals(scalar_surface, array_surface)
    assert scalar_surface.x == pytest.approx(175.0)
    assert scalar_surface.y == pytest.approx(0.0)
    assert gm.equals(arr.centroid()[0], scalar_centroid)
    assert gm.equals(arr.point_on_surface()[0], scalar_surface)


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
    assert centroid.crs == 'EPSG:4807'
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


def test_crosses_antimeridian_matches_split_frame() -> None:
    for crs in (None, 4269, 4326):
        line = gm.LineString([(170, -10), (-170, 10)], crs=crs)
        assert line.crosses_antimeridian is True
    with pytest.raises(gm.CRSError, match='geographic CRS'):
        _ = gm.LineString([(0, 0), (1, 1)], crs=3857).crosses_antimeridian


def test_split_antimeridian_gates() -> None:
    with pytest.raises(gm.CRSError, match='requires a geographic CRS'):
        gm.LineString([(170, 0), (-170, 0)], crs=3857).split_antimeridian()
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
    east = list(gm.parts(split)[0].coords)[-1]
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


def test_split_antimeridian_exact_world_band_and_xy_pole() -> None:
    band = gm.Polygon([(0, -30), (360, -30), (360, 30), (0, 30)], crs=4326)
    split = band.split_antimeridian()
    assert split.to_wkt() == 'POLYGON ((-180 -30, 180 -30, 180 30, -180 30, -180 -30))'
    for longitude in (-179, 0, 179):
        assert gm.contains(split.set_crs(None), gm.Point(longitude, 0))
    assert split.split_antimeridian().to_wkt() == split.to_wkt()

    cap = gm.Polygon([(-45, 40), (45, 40), (135, 40), (-135, 40)], crs=4326)
    fixed = cap.split_antimeridian()
    assert gm.contains(fixed.set_crs(None), gm.Point(0, 89))


_RADIAN_CRS = (
    'GEOGCS["WGS 84 rad",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],UNIT["radian",1],'
    'AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]'
)


@pytest.mark.parametrize(
    'crs',
    [4326, _RADIAN_CRS, 4807],
    ids=['degree', 'radian', 'grad'],
)
@pytest.mark.parametrize('north', [False, True], ids=['south', 'north'])
def test_split_antimeridian_normalizes_only_admitted_pole_ulp_in_chart(
    crs: int | str, *, north: bool
) -> None:
    factor = cast('float', gm.CRS(crs).axes[0]['unit_conversion_factor'])
    half_turn = math.pi / factor
    pole = math.pi / 2.0 / factor
    sign = 1.0 if north else -1.0
    exterior = math.nextafter(sign * pole, math.copysign(math.inf, sign))
    interior = sign * (pole - half_turn / 18.0)
    line = gm.LineString(
        [(half_turn - half_turn / 18.0, exterior), (-half_turn + half_turn / 18.0, interior)],
        crs=crs,
    )
    split = line.split_antimeridian()
    assert any(
        y == sign * pole
        for part in gm.parts(split)
        for _, y in part.coords
    )
    assert split.split_antimeridian().to_wkt() == split.to_wkt()

    two_outside = math.nextafter(exterior, math.copysign(math.inf, sign))
    invalid = gm.LineString(
        [(half_turn - half_turn / 18.0, two_outside), (-half_turn + half_turn / 18.0, interior)],
        crs=crs,
    )
    with pytest.raises(gm.InvalidGeometryError, match='outside the physical axis limits'):
        invalid.split_antimeridian()


def test_split_antimeridian_review_regressions() -> None:
    doubled = gm.LineString(
        [(170, 0), (170, 0), (-170, 0)], z=[0, 100, 200], m=[0, 10, 20]
    )
    east = list(gm.parts(doubled.split_antimeridian())[0].coords)
    assert east[-1] == (180.0, 0.0, 150.0, 15.0)
    two_holes = gm.from_wkt(
        'POLYGON ((170 0, -170 0, -170 30, 170 30, 170 0), (172 5, 174 5, 174 10, 172 10, 172 5), (172 15, 174 15, 174 20, 172 20, 172 15))'
    )
    assert sorted(
        len(cast('gm.Polygon', p).interiors)
        for p in gm.parts(two_holes.split_antimeridian())
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


def test_split_antimeridian_force_south_pole_closure() -> None:
    """Southern seam-crossing ring extends past the input via pole closure."""
    ring = [(-45, -40), (45, -40), (135, -40), (-135, -40)]
    capped = gm.Polygon(ring, crs=4326).split_antimeridian()
    assert capped.geometry_type == 'Polygon'
    assert not capped.is_empty
    coords = list(cast('gm.Polygon', capped).exterior.coords)
    seam_lats = {round(y, 7) for x, y in coords if abs(x) == 180.0}
    assert seam_lats
    assert min(seam_lats) < -40.0
    again = capped.split_antimeridian()
    assert again.to_wkt() == capped.to_wkt()


def test_split_antimeridian_both_poles_reverse_winding() -> None:
    """Inverted winding that implies both poles reverses segments instead of capping."""
    inverted = gm.from_geojson(
        '{"type": "Polygon", "coordinates": [[[100, -60], [175, -65], [-175, -65], [-120, 0], [-170, 85], [175, 85], [0, 68], [-90, 0], [-90, -80], [0, -85], [100, -60]]]}'
    )
    split = inverted.split_antimeridian()
    assert split.geometry_type in ('Polygon', 'MultiPolygon')
    assert not split.is_empty
    again = split.split_antimeridian()
    assert again.to_wkt() == split.to_wkt()


def test_split_antimeridian_crs84_canonical_identity_and_full_turn_rewrite() -> None:
    half_period = gm.LineString([(180, 0), (0, 0)], crs=4326)
    assert half_period.split_antimeridian().to_wkt() == half_period.to_wkt()
    full_period = gm.LineString([(-180, 0), (180, 0)], crs=4326)
    assert full_period.split_antimeridian().to_wkt() == full_period.to_wkt()
    world = gm.Polygon(
        [(-180, -90), (180, -90), (180, 90), (-180, 90), (-180, -90)],
        crs=4326,
    )
    assert world.split_antimeridian().to_wkt() == world.to_wkt()
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
    assert (
        band.split_antimeridian().to_wkt()
        == 'POLYGON ((-180 -30, 180 -30, 180 30, -180 30, -180 -30))'
    )


def test_split_antimeridian_full_longitude_band() -> None:
    """A ring spanning the full longitude band splits and stays non-empty."""
    band = gm.Polygon([(170, -30), (-170, -30), (-170, 30), (170, 30)], crs=4326)
    split = band.split_antimeridian()
    assert split.geometry_type == 'MultiPolygon'
    assert not split.is_empty
    assert len(gm.parts(split)) == 2
    assert any(
        gm.contains(part, gm.Point(175, 0, crs=4326)) for part in gm.parts(split)
    )
    assert any(
        gm.contains(part, gm.Point(-175, 0, crs=4326)) for part in gm.parts(split)
    )


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
        assert gm.equals(
            gm.GeometryArray([shape]).clip_by_rect(*rect)[0], expected
        )


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
        (
            gm.LineString([(-199, 0), (199, 0)], crs=4807),
            (198, -1, -198, 1),
        ),
    ],
    ids=['ordered-alias', 'crossing-alias', 'west-out-of-domain', 'grad-native'],
)
def test_geographic_clip_by_rect_rejects_noncanonical_longitudes(
    shape: gm.Geometry, rect: tuple[float, float, float, float]
) -> None:
    for clipped in (
        lambda: shape.clip_by_rect(*rect),
        lambda: gm.GeometryArray([shape]).clip_by_rect(*rect),
    ):
        with pytest.raises(ValueError, match='clip rectangle bounds'):
            clipped()


def test_geographic_clip_by_rect_rejects_unordered_latitudes_scalar_and_array() -> None:
    shape = gm.LineString([(-2, 0), (2, 0)], crs=4326)
    rect = (1, 1, -1, -1)
    for clipped in (
        lambda: shape.clip_by_rect(*rect),
        lambda: gm.GeometryArray([shape]).clip_by_rect(*rect),
    ):
        with pytest.raises(ValueError, match='clip rectangle bounds'):
            clipped()


def test_packed_geographic_clip_declines_crossing_grad_rectangle() -> None:
    grad_line = gm.LineString([(-2, 0), (2, 0)], crs=4807)
    grad_rect = (1, -1, -1, 1)
    grad_expected = gm.from_wkt(
        'MULTILINESTRING ((-2 0, -1 0), (1 0, 2 0))', crs=4807
    )
    assert gm.equals(grad_line.clip_by_rect(*grad_rect), grad_expected)
    assert gm.equals(
        gm.GeometryArray([grad_line]).clip_by_rect(*grad_rect)[0], grad_expected
    )


def test_packed_geographic_clip_preserves_canonical_rows_and_missingness() -> None:
    canonical_line = gm.LineString([(-2, 0), (0, 0), (2, 0)], crs=4326)
    canonical_expected = gm.LineString([(-1, 0), (0, 0), (1, 0)], crs=4326)
    assert gm.equals(
        gm.GeometryArray([canonical_line]).clip_by_rect(-1, -1, 1, 1)[0],
        canonical_expected,
    )

    rows = gm.GeometryArray(
        [canonical_line, gm.LineString([], crs=4326), canonical_line]
    )
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
    assert line.convex_hull().to_wkt() == 'LINESTRING (-170 10, 170 10)'


def test_crossing_line_geographic_distance_to_seam_is_zero() -> None:
    crossing = gm.LineString([(170, 10), (-170, 10)], crs=4326)
    seam = gm.LineString([(180, 0), (180, 20)], crs=4326)
    assert gm.distance(crossing, seam) == 0.0


def test_geodesic_distance_rejects_split_crossing_polygon() -> None:
    split = _crossing_band_polygon().split_antimeridian()
    east_lobe = gm.box(172, 42, 178, 48, crs=4326)
    with pytest.raises(gm.CRSError, match='polygon interiors is unsupported'):
        gm.distance(split, east_lobe)
    with pytest.raises(gm.CRSError, match='polygon interiors is unsupported'):
        gm.distance(split, gm.Point(175, 45, crs=4326))
    assert gm.distance(split, east_lobe, unit='planar') == 0.0
    assert gm.distance(split, gm.Point(175, 45, crs=4326), unit='planar') == 0.0


def test_geodesic_distance_rejects_unsplit_crossing_polygon() -> None:
    poly = _crossing_band_polygon()
    with pytest.raises(gm.CRSError, match='polygon interiors is unsupported'):
        gm.distance(poly, gm.Point(175, 45, crs=4326))
    with pytest.raises(gm.CRSError, match='polygon interiors is unsupported'):
        gm.distance(poly, gm.box(172, 42, 178, 48, crs=4326))
    with pytest.raises(gm.CRSError, match='polygon interiors is unsupported'):
        gm.dwithin(poly, gm.Point(175, 45, crs=4326), 1.0)


def test_nad83_crossing_polygon_predicates_match_split_form() -> None:
    """EPSG:4269 shares the antimeridian predicate frame with WGS84."""
    poly = _crossing_band_polygon(crs=4269)
    split = poly.split_antimeridian()
    pt = gm.Point(175, 45, crs=4269)
    assert gm.contains(poly, pt) == gm.contains(split, pt)
    assert gm.intersects(poly, gm.box(172, 42, 178, 48, crs=4269)) == gm.intersects(
        split, gm.box(172, 42, 178, 48, crs=4269)
    )


def test_crossing_polygon_geodesic_area_close_to_split_parts_sum() -> None:
    poly = _crossing_band_polygon()
    split = poly.split_antimeridian()
    part_total = sum(part.area for part in gm.parts(split))
    assert poly.area == pytest.approx(part_total, rel=0.001)


@pytest.mark.parametrize(
    "base_latitude",
    [
        pytest.param(0.0, id="equator"),
        pytest.param(40.0, id="mid-latitude"),
    ],
)
def test_thin_crossing_band_area_survives_public_split(base_latitude: float) -> None:
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

    # The geodesic ring-area kernel has a pre-existing residual below 5.3e-5
    # relative on this band; 1e-4 leaves a tight margin while rejecting the
    # former half-area result by more than three orders of magnitude.
    assert split.area == pytest.approx(polygon.area, rel=1e-4)


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
    pg = poly.prepare()
    assert gm.contains(pg, gm.Point(175, 45, crs=4326)) is True
    assert gm.contains(pg, gm.Point(0, 45, crs=4326)) is False
    probes = gm.GeometryArray(
        [gm.Point(175, 45, crs=4326), gm.Point(0, 45, crs=4326)], crs=4326
    )
    assert [bool(x) for x in gm.contains(pg, probes)] == [True, False]


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
        assert gm.contains_xy(poly, x, y) == gm.contains(poly, gm.Point(x, y, crs=4326))
        assert gm.intersects_xy(poly, x, y) == gm.intersects(
            poly, gm.Point(x, y, crs=4326)
        )
    assert not gm.contains_xy(poly, 0, 0)
    assert list(gm.contains_xy(poly, [178, 0, -178], [0, 0, 0])) == [True, False, True]
    assert gm.contains_xy(poly, 178, 0)


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
    prepared = cap.prepare()
    for longitude in (0.0, 180.0, -180.0):
        pole = gm.Point(longitude, 90, crs=4326)
        assert gm.contains_xy(cap, longitude, 90) == gm.contains(cap, pole) is True
        assert gm.intersects_xy(cap, longitude, 90) == gm.intersects(cap, pole) is True
        assert gm.contains_xy(prepared, longitude, 90) is True
        assert gm.intersects_xy(prepared, longitude, 90) is True

    boundary = gm.Polygon([(-10, 80), (10, 80), (0, 90)], crs=4326)
    boundary_prepared = boundary.prepare()
    for longitude in (0.0, 180.0, -180.0):
        assert gm.contains_xy(boundary, longitude, 90) is False
        assert gm.intersects_xy(boundary, longitude, 90) is True
        assert gm.contains_xy(boundary_prepared, longitude, 90) is False
        assert gm.intersects_xy(boundary_prepared, longitude, 90) is True


def test_polar_prepared_and_free_point_batches_keep_original_topology() -> None:
    cap = gm.Polygon(_latitude_ring(80), crs=4326)
    probes = gm.GeometryArray(
        [gm.Point(0, 90, crs=4326), gm.Point(0, 85, crs=4326), None],
        crs=4326,
    )
    for evaluate in (gm.contains, gm.intersects, gm.covers):
        np.testing.assert_array_equal(evaluate(cap, probes), [True, True, False])
    prepared = cap.prepare()
    for name in ('contains', 'intersects', 'covers'):
        np.testing.assert_array_equal(
            getattr(gm, name)(prepared, probes), [True, True, False]
        )


def test_xy_pole_broadcast_array_and_missing_rows() -> None:
    north = gm.Polygon(_latitude_ring(80), crs=4326)
    south = gm.Polygon([(x, -y) for x, y in _latitude_ring(80)], crs=4326)
    annulus = gm.Polygon(_latitude_ring(70), holes=[_latitude_ring(80)], crs=4326)
    rows = gm.GeometryArray([north, south, None, annulus], crs=4326)

    np.testing.assert_array_equal(
        gm.contains_xy(rows, [0, 0, 0, 180], [90, -90, 90, 75]),
        [True, True, False, True],
    )
    np.testing.assert_array_equal(
        gm.intersects_xy(rows, [0, 0, 0, 180], [90, -90, 90, 75]),
        [True, True, False, True],
    )
    np.testing.assert_array_equal(
        gm.contains_xy(rows, 0, 90), [True, False, False, False]
    )
    np.testing.assert_array_equal(
        gm.contains_xy(rows, [0, 180, 0, 180], 90),
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
            assert gm.contains_xy(annulus, longitude, latitude) is expected
            assert gm.contains_xy(annulus, longitude, latitude) is expected
        for latitude in (70.0, 80.0):
            point = gm.Point(longitude, latitude, crs=4326)
            assert not gm.contains(annulus, point)
            assert gm.intersects(annulus, point)
            assert not gm.contains_xy(annulus, longitude, latitude)
            assert gm.intersects_xy(annulus, longitude, latitude)

    index = gm.SpatialIndex([cap, annulus])
    assert list(index.query(gm.Point(180, 89, crs=4326), predicate='intersects')) == [0]
    assert list(index.query(gm.Point(180, 75, crs=4326), predicate='intersects')) == [1]
    assert list(index.query(gm.Point(0, 75, crs=4326), predicate='intersects')) == [1]


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
    assert planar.set_crs(4326).is_valid
    assert not annulus.set_crs(None).is_valid
    planar_rows = gm.GeometryArray([planar])
    np.testing.assert_array_equal(planar_rows.is_valid, [False])
    np.testing.assert_array_equal(planar_rows.set_crs(4326).is_valid, [True])

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
        assert gm.contains_xy(crossing, longitude, 0)
        assert gm.contains_xy(crossing, longitude, 0)
    assert gm.covers(endpoint, gm.Point(180, 0, crs=4326))
    assert gm.touches(endpoint, gm.Point(180, 0, crs=4326))
    assert not gm.contains_xy(endpoint, 180, 0)


def test_derived_points_and_bounds_handle_pole_enclosure() -> None:
    ring = _pole_ring()
    assert gm.contains(ring, ring.point_on_surface())
    assert gm.contains(ring, ring.centroid())
    bounds = ring.bounds
    assert bounds is not None and bounds[3] == 90.0
    band = _crossing_band()
    assert gm.contains(band, band.point_on_surface())


def test_geodesic_shortest_line_rejects_filled_polygon_operands() -> None:
    poly = _crossing_band()
    probe = gm.Point(179, 12, crs=4326)
    with pytest.raises(gm.CRSError, match='polygon interiors is unsupported'):
        gm.shortest_line(poly, probe)
    with pytest.raises(gm.CRSError, match='polygon interiors is unsupported'):
        gm.shortest_line(poly.split_antimeridian(), probe)
    with pytest.raises(gm.CRSError, match='polygon interiors is unsupported'):
        gm.shortest_line(gm.GeometryArray([poly]), probe)


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
