"""split_antimeridian — port of the JOSS ``antimeridian`` algorithm.

Oracle values are hardcoded from the upstream test fixtures
(gadomski/antimeridian, ``tests/data/output/spherical``); the kernel was
differential-tested against all of them at port time.
"""

from typing import cast

import gometry as gm
import numpy as np
import pytest


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


def test_split_antimeridian_polygons_match_upstream_oracles() -> None:
    box = gm.Polygon([(170, 40), (-170, 40), (-170, 50), (170, 50)], crs=4326)
    parts = gm.parts(box.split_antimeridian())
    assert [part.geometry_type for part in parts] == ['Polygon', 'Polygon']
    seam_lats = sorted({
        round(y, 7)
        for part in parts
        for x, y in cast('gm.Polygon', part).exterior.coords
        if abs(x) == 180.0
    })
    assert seam_lats == [40.4324611, 50.431313]
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
        '{"type": "Polygon", "coordinates": [[[170, 40], [-170, 40], [-170, 60], [170, 60], [170, 40]],[[175, 45], [-175, 45], [-175, 55], [175, 55], [175, 45]]]}'
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
    with pytest.raises(gm.InvalidGeometryError, match=r'invalid longitude/latitude \(190, 0\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data'):
        gm.LineString([(190, 0), (200, 0)]).split_antimeridian()
    zm_ring = gm.from_wkt(
        'POLYGON Z ((-45 40 1, 45 40 1, 135 40 1, -135 40 1, -45 40 1))'
    )
    with pytest.raises(gm.InvalidGeometryError, match='pole-closure'):
        zm_ring.split_antimeridian()
    nad = gm.LineString([(170, -10), (-170, 10)], crs=4269).split_antimeridian()
    assert nad.geometry_type == 'MultiLineString'
    with pytest.raises(
        gm.InvalidGeometryError, match=r'invalid longitude/latitude \(181, 0\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data'
    ) as info:
        gm.GeometryArray([
            gm.LineString([(0, 0), (1, 1)]),
            gm.LineString([(181, 0), (1, 1)]),
        ]).split_antimeridian()
    assert 'array element 1' in ''.join(info.value.__notes__)


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


def test_split_crossing_polygon_distance_to_east_lobe_is_zero() -> None:
    split = _crossing_band_polygon().split_antimeridian()
    east_lobe = gm.box(172, 42, 178, 48, crs=4326)
    assert gm.distance(split, east_lobe) == 0.0
    assert gm.distance(split, gm.Point(175, 45, crs=4326)) == 0.0


def test_unsplit_crossing_polygon_distance_to_interior_is_zero() -> None:
    poly = _crossing_band_polygon()
    assert gm.distance(poly, gm.Point(175, 45, crs=4326)) == 0.0
    assert gm.distance(poly, gm.box(172, 42, 178, 48, crs=4326)) == 0.0
    assert gm.dwithin(poly, gm.Point(175, 45, crs=4326), 1.0) is True


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
    assert pg.contains(gm.Point(175, 45, crs=4326)) is True
    assert pg.contains(gm.Point(0, 45, crs=4326)) is False
    probes = gm.GeometryArray(
        [gm.Point(175, 45, crs=4326), gm.Point(0, 45, crs=4326)], crs=4326
    )
    assert [bool(x) for x in pg.contains(probes)] == [True, False]


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
    assert poly.prepare().contains_xy(178, 0)


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
        assert prepared.contains_xy(longitude, 90) is True
        assert prepared.intersects_xy(longitude, 90) is True

    boundary = gm.Polygon([(-10, 80), (10, 80), (0, 90)], crs=4326)
    boundary_prepared = boundary.prepare()
    for longitude in (0.0, 180.0, -180.0):
        assert gm.contains_xy(boundary, longitude, 90) is False
        assert gm.intersects_xy(boundary, longitude, 90) is True
        assert boundary_prepared.contains_xy(longitude, 90) is False
        assert boundary_prepared.intersects_xy(longitude, 90) is True


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
            getattr(prepared, name)(probes), [True, True, False]
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
            assert annulus.prepare().contains_xy(longitude, latitude) is expected
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
    report_repaired = annulus.validate().repair()
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
    with pytest.raises(gm.InvalidGeometryError, match='repair did not produce valid geometry'):
        unrepairable.repair()


def test_artificial_seam_is_not_a_topological_boundary() -> None:
    crossing = gm.LineString([(170, 0), (-170, 0)], crs=4326)
    endpoint = gm.LineString([(170, 0), (180, 0)], crs=4326)
    for longitude in (180.0, -180.0):
        point = gm.Point(longitude, 0, crs=4326)
        assert gm.contains(crossing, point)
        assert not gm.touches(crossing, point)
        assert gm.contains_xy(crossing, longitude, 0)
        assert crossing.prepare().contains_xy(longitude, 0)
    assert not gm.contains(endpoint, gm.Point(180, 0, crs=4326))
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


def test_nearest_and_shortest_line_parity_across_paths() -> None:
    poly = _crossing_band()
    probe = gm.Point(179, 12, crs=4326)
    oracle = gm.shortest_line(poly.split_antimeridian(), probe).to_wkt()
    assert gm.shortest_line(poly, probe).to_wkt() == oracle
    assert gm.shortest_line(poly, probe).to_wkt() == oracle
    assert gm.shortest_line(gm.GeometryArray([poly]), probe)[0].to_wkt() == oracle
    assert gm.shortest_line(poly, gm.GeometryArray([probe]))[0].to_wkt() == oracle
    assert (
        gm.shortest_line(gm.GeometryArray([poly]), gm.GeometryArray([probe]))[
            0
        ].to_wkt()
        == oracle
    )


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
