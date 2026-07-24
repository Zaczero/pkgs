"""Regression tests pinning the CRS/metric correctness-completeness pass.

Each test documents one fix we must never reintroduce. Geodesic expectations are
derived by running gometry and cross-checked against ``pyproj.Geod`` where natural.
"""

from __future__ import annotations

import json
import math

import gometry as gm
import pytest
from conftest import bools, floats, ids
from pyproj import Geod

FOOT_CRS = 2263
FOOT_UNIT_M = 0.3048006096012192
GEOD = Geod(ellps='WGS84')


def test_huge_coordinate_distance_is_panic_free() -> None:
    """Huge-but-finite coordinates return a finite distance, never a PanicException."""
    huge = gm.LineString([(-1e308, 0), (1e308, 0)])
    result = gm.distance(huge, gm.Point(0, 1))
    assert math.isfinite(result)
    assert result == pytest.approx(1.0)


def test_distance_is_commutative() -> None:
    """distance(a, b) == distance(b, a); the geodesic kernel is order-independent."""
    a = gm.Point(0, 0, crs=4326)
    b = gm.Point(3, 4, crs=4326)
    assert gm.distance(a, b) == gm.distance(b, a)


def test_epsg4326_scalar_vs_many_geodesic_broadcast_reuses_fixed_state() -> None:
    """Scalar-vs-array geodesic broadcasts match scalar row kernels exactly."""
    scalar = gm.MultiLineString(
        [
            [(-1.0, 0.0), (1.0, 0.0)],
            [(179.0, 0.0), (-179.0, 0.0)],
            [(-45.0, 88.5), (45.0, 88.5)],
        ],
        crs=4326,
    )
    rows = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        gm.LineString([(180.0, -1.0), (180.0, 1.0)], crs=4326),
        gm.MultiPoint([(12.0, -4.0), (12.5, -3.5)], crs=4326),
        gm.Point(0.0, 87.0, crs=4326),
        gm.Polygon([(2.0, 0.2), (3.0, 0.2), (2.5, 1.0), (2.0, 0.2)], crs=4326),
    ])
    row_list = list(rows)
    expected_distances = [gm.distance(row, scalar) for row in row_list]
    assert floats(gm.distance(rows, scalar)) == pytest.approx(expected_distances)
    assert floats(gm.distance(scalar, rows)) == pytest.approx([
        gm.distance(scalar, row) for row in row_list
    ])
    assert bools(gm.dwithin(rows, scalar, 0.0)) == [
        gm.dwithin(row, scalar, 0.0) for row in row_list
    ]
    assert bools(gm.dwithin(rows, scalar, 250000.0)) == [
        gm.dwithin(row, scalar, 250000.0) for row in row_list
    ]
    assert list(zip(*gm.nearest_points(rows, scalar), strict=True)) == [
        gm.nearest_points(row, scalar) for row in row_list
    ]
    assert list(zip(*gm.nearest_points(scalar, rows), strict=True)) == [
        gm.nearest_points(scalar, row) for row in row_list
    ]
    assert [line.to_wkt() for line in list(gm.shortest_line(rows, scalar))] == [
        gm.shortest_line(row, scalar).to_wkt() for row in row_list
    ]
    assert [line.to_wkt() for line in list(gm.shortest_line(scalar, rows))] == [
        gm.shortest_line(scalar, row).to_wkt() for row in row_list
    ]


def test_distance_rejects_mixed_crs_presence() -> None:
    """A CRS-tagged operand against a CRS-free one is rejected, not silently mixed."""
    tagged = gm.Point(0, 0, crs=4326)
    free = gm.Point(3, 4)
    with pytest.raises(gm.CRSMismatchError, match='requires matching CRS metadata'):
        gm.distance(tagged, free)
    with pytest.raises(gm.CRSMismatchError, match='requires matching CRS metadata'):
        gm.distance(free, tagged)


def test_distance_rejects_mismatched_epoch() -> None:
    """Matching CRS but differing/mixed coordinate epoch is rejected; equal epoch works."""
    a = gm.Point(0, 0, crs=4326, epoch=2020.0)
    same = gm.Point(3, 4, crs=4326, epoch=2020.0)
    assert gm.distance(a, same) == pytest.approx(554058.9237526914)
    with pytest.raises(gm.CRSMismatchError, match='requires matching coordinate epoch'):
        gm.distance(a, gm.Point(3, 4, crs=4326, epoch=2021.0))
    with pytest.raises(gm.CRSMismatchError, match='requires matching coordinate epoch'):
        gm.distance(a, gm.Point(3, 4, crs=4326))


def test_crs_mismatch_attributes_are_raw_values_with_a_field_discriminator() -> None:
    """Frame conflicts expose values callers can branch on without parsing prose."""
    with pytest.raises(gm.CRSMismatchError) as crs_error:
        gm.distance(gm.Point(0, 0, crs=4326), gm.Point(3, 4, crs=3857))
    assert crs_error.value.field == 'crs'
    assert crs_error.value.left == 'EPSG:4326'
    assert crs_error.value.right == 'EPSG:3857'
    assert str(gm.CRS(crs_error.value.right)) == 'EPSG:3857'

    with pytest.raises(gm.CRSMismatchError) as epoch_error:
        gm.distance(
            gm.Point(0, 0, crs=4326, epoch=2020.0),
            gm.Point(3, 4, crs=4326, epoch=2021.0),
        )
    assert epoch_error.value.field == 'epoch'
    assert epoch_error.value.left == 2020.0
    assert epoch_error.value.right == 2021.0


def test_foot_crs_default_metrics_are_native_units() -> None:
    """A US-foot projected CRS defaults to native feet; explicit meters still converts."""
    a = gm.Point(0, 0, crs=FOOT_CRS)
    b = gm.Point(10, 0, crs=FOOT_CRS)
    unit_square = gm.box(0, 0, 1, 1, crs=FOOT_CRS)
    line = gm.LineString([(0, 0), (10, 0)], crs=FOOT_CRS)

    assert gm.distance(a, b) == pytest.approx(10.0)
    assert gm.distance(a, b, unit='planar') == pytest.approx(10.0)
    assert gm.distance(a, b, unit='meters') == pytest.approx(10.0 * FOOT_UNIT_M)
    assert unit_square.area == pytest.approx(1.0)
    assert gm.area(unit_square, unit='planar') == pytest.approx(1.0)
    assert gm.area(unit_square, unit='meters') == pytest.approx(
        FOOT_UNIT_M * FOOT_UNIT_M
    )
    assert line.length == pytest.approx(10.0)
    assert gm.length(line, unit='planar') == pytest.approx(10.0)
    assert gm.length(line, unit='meters') == pytest.approx(10.0 * FOOT_UNIT_M)
    assert gm.area(unit_square) == unit_square.area
    assert gm.length(line) == line.length
    assert gm.dwithin(a, b, 10.0)
    assert not gm.dwithin(a, b, 9.99)
    distance_m = gm.distance(a, b, unit='meters')
    assert distance_m == pytest.approx(10.0 * FOOT_UNIT_M)
    assert gm.dwithin(a, b, distance_m, unit='meters')
    assert not gm.dwithin(a, b, distance_m * 0.999, unit='meters')


def test_foot_crs_default_distance_parameters_are_native_units() -> None:
    """Constructive and LRS distance parameters default to native feet on EPSG:2263."""
    point = gm.Point(0, 0, crs=FOOT_CRS)
    buffered = point.buffer(1.0)
    assert buffered.area == pytest.approx(math.pi, rel=0.02)
    assert point.buffer(FOOT_UNIT_M, unit='meters').area == pytest.approx(
        math.pi, rel=0.02
    )

    line = gm.LineString([(0, 0), (10, 0)], crs=FOOT_CRS)
    interpolated = line.line_interpolate(1.0)
    assert (interpolated.x, interpolated.y) == pytest.approx((1.0, 0.0))
    assert line.line_interpolate(FOOT_UNIT_M, unit='meters').x == pytest.approx(1.0)
    assert line.line_locate(gm.Point(1, 0, crs=FOOT_CRS)) == pytest.approx(1.0)
    assert line.line_locate(
        gm.Point(1, 0, crs=FOOT_CRS), unit='meters'
    ) == pytest.approx(FOOT_UNIT_M)
    assert line.line_substring(0.0, 1.0).length == pytest.approx(1.0)
    destination = gm.destination(point, 90.0, 1.0)
    assert (destination.x, destination.y) == pytest.approx((1.0, 0.0))
    between = gm.point_between(point, gm.Point(10, 0, crs=FOOT_CRS), 1.0)
    assert (between.x, between.y) == pytest.approx((1.0, 0.0))


def test_meter_projected_crs_defaults_stay_meter_equivalent() -> None:
    """A meter projected CRS is unchanged because native units and meters coincide."""
    box = gm.box(0, 0, 1, 1, crs=3857)
    line = gm.LineString([(0, 0), (1, 0)], crs=3857)
    point = gm.Point(0, 0, crs=3857)
    other = gm.Point(1, 0, crs=3857)

    assert box.area == pytest.approx(1.0)
    assert gm.area(box, unit='planar') == pytest.approx(1.0)
    assert gm.area(box, unit='meters') == pytest.approx(1.0)
    assert line.length == pytest.approx(1.0)
    assert gm.length(line, unit='planar') == pytest.approx(1.0)
    assert gm.length(line, unit='meters') == pytest.approx(1.0)
    assert gm.distance(point, other) == pytest.approx(1.0)
    assert gm.distance(point, other, unit='planar') == pytest.approx(1.0)
    assert gm.distance(point, other, unit='meters') == pytest.approx(1.0)
    assert point.buffer(1.0).area == pytest.approx(math.pi, rel=0.02)


def test_geographic_and_crs_free_unit_modes_are_unchanged() -> None:
    """The projected native-unit change does not alter geographic or CRS-free semantics."""
    geo_line = gm.LineString([(0, 0), (1, 0)], crs=4326)
    assert geo_line.length == pytest.approx(111319.49079327357, rel=1e-09)
    assert gm.length(geo_line, unit='meters') == pytest.approx(
        111319.49079327357, rel=1e-09
    )
    assert gm.length(geo_line, unit='planar') == pytest.approx(1.0)
    assert geo_line.line_interpolate(1.0, unit='planar').x == pytest.approx(1.0)

    raw_line = gm.LineString([(0, 0), (1, 0)])
    assert raw_line.length == pytest.approx(1.0)
    assert gm.length(raw_line, unit='planar') == pytest.approx(1.0)
    with pytest.raises(gm.GeometryError, match="unit='meters' requires a CRS"):
        gm.length(raw_line, unit='meters')


def test_antimeridian_crossing_lines_have_zero_distance() -> None:
    """Lines crossing the antimeridian register as intersecting (distance 0)."""
    crossing = gm.LineString([(170, 10), (-170, 10)], crs=4326)
    meridian = gm.LineString([(180, 0), (180, 20)], crs=4326)
    assert gm.distance(crossing, meridian) == 0.0
    assert gm.dwithin(crossing, meridian, 1.0)


def test_non_crossing_geodesic_distance_is_positive() -> None:
    """A control line far from the antimeridian span retains a large finite distance."""
    crossing = gm.LineString([(170, 10), (-170, 10)], crs=4326)
    far = gm.LineString([(0, 40), (10, 40)], crs=4326)
    assert gm.distance(crossing, far) == pytest.approx(14099840.273582024, rel=1e-09)


def test_point_to_segment_geodesic_matches_pyproj() -> None:
    """Point-to-segment distance is the perpendicular geodesic distance (pyproj check)."""
    segment = gm.LineString([(0, 0), (10, 0)], crs=4326)
    point = gm.Point(5, 1, crs=4326)
    result = gm.distance(segment, point)
    _, _, expected = GEOD.inv(5, 1, 5, 0)
    assert result == pytest.approx(expected)


def test_geodesic_lrs_is_metre_based_and_self_consistent() -> None:
    """Geographic LRS absolute distances are ellipsoidal meters, consistent with length()."""
    line = gm.LineString([(0, 0), (0, 10)], crs=4326)
    length = line.length
    assert length == pytest.approx(1105854.8332343723)
    interpolated = line.line_interpolate(1000.0)
    assert interpolated.y == pytest.approx(0.009043694769749644)
    loc = line.line_locate(gm.Point(0, 5, crs=4326))
    assert loc == pytest.approx(length / 2, rel=0.001)
    assert line.line_substring(0, loc).length == pytest.approx(loc)


def test_normalized_lrs_fraction_is_geodesic() -> None:
    """normalized=True LRS fractions are consistent with geodesic length()."""
    line = gm.LineString([(0, 0), (0, 10)], crs=4326)
    half = line.line_interpolate(0.5, normalized=True)
    loc = line.line_locate(half, normalized=True)
    assert loc == pytest.approx(0.5)


def test_geodesic_lrs_zm_interpolation_never_corrupts_xy() -> None:
    """A huge-but-finite Z must not overflow into a non-finite ordinate that
    snaps the whole geodesic point back to an endpoint (robust convex form).
    """
    line = gm.LineString([(0, 0, -1e308), (10, 0, 1e308)], crs=4326)
    midpoint = line.line_interpolate(0.5, normalized=True)
    assert midpoint.x == pytest.approx(5.0, abs=0.2)
    assert 0.0 < midpoint.x < 10.0
    assert midpoint.z == pytest.approx(0.0, abs=1e292)


def test_distance_3d_requires_z_on_every_vertex() -> None:
    """distance_3d/length_3d demand a Z ordinate everywhere; no silent Z=0."""
    with pytest.raises(ValueError, match='requires a Z ordinate'):
        gm.distance_3d(gm.Point(0, 0), gm.Point(1, 0))
    with pytest.raises(ValueError, match='requires a Z ordinate'):
        gm.distance_3d(gm.Point(0, 0, z=3), gm.Point(1, 0))
    with pytest.raises(ValueError, match='requires a Z ordinate'):
        _ = gm.LineString([(0, 0), (1, 0)]).length_3d


def test_distance_3d_and_length_3d_valid_values() -> None:
    """Fully-3D inputs produce the expected Euclidean 3D metrics."""
    assert gm.distance_3d(gm.Point(0, 0, z=3), gm.Point(3, 4, z=15)) == 13.0
    line = gm.LineString([(0, 0), (1, 0)], z=[0, 3])
    assert line.length_3d == pytest.approx(math.sqrt(1 + 9))


def test_geographic_out_of_domain_raises() -> None:
    """Out-of-domain latitude raises for geographic metrics and point methods."""
    with pytest.raises(ValueError):
        _ = gm.LineString([(0, 95), (1, 95)], crs=4326).length
    with pytest.raises(ValueError):
        _ = gm.box(0, 95, 1, 96, crs=4326).area
    with pytest.raises(ValueError):
        gm.distance(gm.Point(0, 95, crs=4326), gm.Point(1, 95, crs=4326))
    with pytest.raises(ValueError):
        gm.bearing(gm.Point(0, 95, crs=4326), gm.Point(1, 95, crs=4326))
    with pytest.raises(ValueError):
        gm.destination(gm.Point(0, 95, crs=4326), 45, 1000)
    with pytest.raises(ValueError):
        gm.CRS(4326).geodesic(0, 95, 1, 95)


def test_negative_buffer_erodes_polygon() -> None:
    """box.buffer(-1) erodes inward to a 8x8 polygon of area 64."""
    eroded = gm.box(0, 0, 10, 10).buffer(-1)
    assert eroded.geometry_type == 'Polygon'
    assert eroded.area == pytest.approx(64.0)


def test_negative_buffer_beyond_extent_is_empty() -> None:
    """Eroding past the geometry, or eroding a line/point, yields empty."""
    assert gm.box(0, 0, 10, 10).buffer(-100).area == pytest.approx(0.0)
    assert gm.LineString([(0, 0), (10, 0)]).buffer(-1).is_empty
    assert gm.Point(0, 0).buffer(-1).is_empty


def test_negative_buffer_foot_crs_is_native_unit_scaled() -> None:
    """Foot-CRS erosion defaults to native feet; eroding 1 ft shrinks 100ft->98ft."""
    eroded = gm.box(0, 0, 100, 100, crs=FOOT_CRS).buffer(-1.0)
    inner = gm.box(1, 1, 99, 99, crs=FOOT_CRS).area
    assert eroded.area == pytest.approx(inner, rel=0.0001)
    eroded_meters = gm.box(0, 0, 100, 100, crs=FOOT_CRS).buffer(
        -FOOT_UNIT_M, unit='meters'
    )
    assert eroded_meters.area == pytest.approx(inner, rel=0.0001)


def test_negative_buffer_non_finite_still_raises() -> None:
    """Non-finite buffer distance remains an error even with erosion supported."""
    with pytest.raises(ValueError, match='distance must be finite'):
        gm.box(0, 0, 10, 10).buffer(math.inf)


def test_spatial_index_rejects_mixed_crs_build() -> None:
    """Building an index over mixed-CRS geometries raises."""
    with pytest.raises(ValueError, match='spatial index'):
        gm.SpatialIndex([gm.Point(0, 0, crs=4326), gm.Point(1, 1)])


def test_spatial_index_rejects_mismatched_query() -> None:
    """query/nearest with a CRS-mismatched query raises; matching CRS works."""
    index = gm.SpatialIndex(gm.points([0, 1], [0, 1], crs=4326))
    with pytest.raises(ValueError, match='spatial index query'):
        index.query(gm.box(-1, -1, 2, 2))
    with pytest.raises(ValueError, match='spatial index nearest'):
        index.nearest(gm.Point(0.5, 0.5))
    assert sorted(ids(index.query(gm.box(-1, -1, 2, 2, crs=4326)))) == [0, 1]


def test_spatial_index_insert_rejects_mismatched_crs() -> None:
    """index.insert with a CRS-mismatched geometry raises; matching CRS works."""
    index = gm.SpatialIndex(gm.points([0, 1], [0, 1], crs=4326))
    with pytest.raises(ValueError, match='spatial index'):
        index.insert(gm.Point(2, 2))
    index.insert(gm.Point(2, 2, crs=4326))
    assert sorted(ids(index.query(gm.box(-1, -1, 3, 3, crs=4326)))) == [0, 1, 2]


@pytest.mark.parametrize(
    ('wkt', 'geojson_type'),
    [
        ('LINESTRING EMPTY', 'LineString'),
        ('MULTIPOINT EMPTY', 'MultiPoint'),
        ('MULTILINESTRING EMPTY', 'MultiLineString'),
        ('MULTIPOLYGON EMPTY', 'MultiPolygon'),
    ],
)
def test_empty_lineal_and_multi_geometries_roundtrip_io(
    wkt: str, geojson_type: str
) -> None:
    """Empty line/multi geometries use valid WKT, WKB, and GeoJSON encodings."""
    geometry = gm.from_wkt(wkt)
    assert geometry.to_wkt() == wkt
    assert gm.from_wkt(geometry.to_wkt()).to_wkt() == wkt
    assert gm.from_wkb(geometry.to_wkb()).to_wkt() == wkt
    geojson = json.loads(geometry.to_geojson())
    assert geojson == {'type': geojson_type, 'coordinates': []}
    assert gm.from_geojson(geojson).to_wkt() == wkt


def test_spatial_index_dwithin_matches_crs_aware_geometry_metrics() -> None:
    """Index dwithin uses the same CRS-aware metric as gometry.dwithin."""
    geographic_items = [gm.Point(179.0, 0.0, crs=4326), gm.Point(-170.0, 0.0, crs=4326)]
    geographic_query = gm.Point(-180.0, 0.0, crs=4326)
    geographic_distance = 200000.0
    geographic_index = gm.SpatialIndex(geographic_items)
    expected_geographic = [
        idx
        for idx, item in enumerate(geographic_items)
        if gm.dwithin(item, geographic_query, geographic_distance)
    ]
    assert expected_geographic == [0]
    assert (
        ids(
            geographic_index.query(
                geographic_query, predicate='dwithin', distance=geographic_distance
            )
        )
        == expected_geographic
    )
    assert ids(geographic_index.nearest(geographic_query)) == [0]
    assert ids(geographic_index.nearest(geographic_query, unit='planar')) == [1]
    projected_items = [
        gm.Point(0.0, 0.0, crs=FOOT_CRS),
        gm.Point(10.0, 0.0, crs=FOOT_CRS),
    ]
    projected_query = gm.Point(4.0, 0.0, crs=FOOT_CRS)
    projected_distance = 5.0
    projected_index = gm.SpatialIndex(projected_items)
    expected_projected = [
        idx
        for idx, item in enumerate(projected_items)
        if gm.dwithin(item, projected_query, projected_distance)
    ]
    assert expected_projected == [0]
    assert (
        ids(
            projected_index.query(
                projected_query, predicate='dwithin', distance=projected_distance
            )
        )
        == expected_projected
    )
    assert ids(
        projected_index.query(
            projected_query,
            predicate='dwithin',
            distance=projected_distance,
            unit='planar',
        )
    ) == [0]
    assert ids(
        projected_index.query(
            projected_query,
            predicate='dwithin',
            distance=projected_distance * FOOT_UNIT_M,
            unit='meters',
        )
    ) == [0]
    assert (
        ids(
            projected_index.query(
                projected_query,
                predicate='dwithin',
                distance=3.99 * FOOT_UNIT_M,
                unit='meters',
            )
        )
        == []
    )


def test_geometry_collection_infers_and_validates_member_metadata() -> None:
    """GeometryCollection inherits one shared member CRS/epoch and rejects conflicts."""
    first = gm.Point(0, 0, crs=4326, epoch=2020.0)
    second = gm.Point(1, 1, crs=4326, epoch=2020.0)
    collection = gm.GeometryCollection([first, second])
    assert collection.crs == 'EPSG:4326'
    assert collection.epoch == 2020.0
    assert [part.crs for part in gm.parts(collection)] == ['EPSG:4326', 'EPSG:4326']
    assert (
        gm.GeometryCollection([first, second], crs=4326, epoch=2020.0).crs
        == 'EPSG:4326'
    )
    with pytest.raises(ValueError, match='one shared CRS'):
        gm.GeometryCollection([first, second], crs=3857)
    with pytest.raises(ValueError, match='one shared CRS'):
        gm.GeometryCollection([first, gm.Point(1, 1, crs=3857)])
    with pytest.raises(ValueError, match='one shared coordinate epoch'):
        gm.GeometryCollection([first, gm.Point(1, 1, crs=4326, epoch=2021.0)])


def test_explicit_frame_metadata_fills_missing_array_and_collection_items() -> None:
    bare = gm.Point(0, 0)
    crs_only = gm.Point(1, 1, crs=4326)
    epoch_tagged = gm.Point(2, 2, crs=4326, epoch=2020.0)
    array = gm.GeometryArray([bare, crs_only], crs=4326, epoch=2020.0)
    assert array.crs == 'EPSG:4326'
    assert array.epoch == 2020.0
    assert [item.crs for item in array] == ['EPSG:4326', 'EPSG:4326']
    assert [item.epoch for item in array] == [2020.0, 2020.0]
    epoch_array = gm.GeometryArray([crs_only, epoch_tagged], epoch=2020.0)
    assert [item.epoch for item in epoch_array] == [2020.0, 2020.0]
    collection = gm.GeometryCollection([bare, crs_only], crs=4326, epoch=2020.0)
    assert collection.crs == 'EPSG:4326'
    assert collection.epoch == 2020.0
    assert [item.epoch for item in gm.parts(collection)] == [2020.0, 2020.0]
    epoch_collection = gm.GeometryCollection([crs_only, epoch_tagged], epoch=2020.0)
    assert [item.epoch for item in gm.parts(epoch_collection)] == [2020.0, 2020.0]
    with pytest.raises(ValueError, match='one shared CRS'):
        gm.GeometryArray([bare, crs_only])
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or set_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.GeometryArray([bare], epoch=2020.0)
    with pytest.raises(ValueError, match='one shared coordinate epoch'):
        gm.GeometryArray([epoch_tagged], epoch=2021.0)
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or set_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.GeometryCollection([bare], epoch=2020.0)


def test_non_degree_geographic_crs_rejected_for_geodesic_metrics() -> None:
    """Grad-unit geographic CRS are rejected instead of misread as degrees."""
    grad_crs = gm.CRS(4807)
    assert grad_crs.is_geographic
    assert [axis['unit_name'] for axis in grad_crs.axes[:2]] == ['grad', 'grad']
    with pytest.raises(ValueError, match='degree angular axis units'):
        gm.distance(gm.Point(0, 0, crs=4807), gm.Point(1, 0, crs=4807))
    with pytest.raises(ValueError, match='degree angular axis units'):
        gm.SpatialIndex([gm.Point(0, 0, crs=4807)]).query(
            gm.Point(0, 0, crs=4807), predicate='dwithin', distance=1.0
        )


def test_to_wkb_rejects_srid_for_non_epsg_crs() -> None:
    """EWKB SRID output fails loudly when the CRS has no EPSG integer SRID."""
    geometry = gm.Point(0, 0, crs='OGC:CRS84')
    with pytest.raises(ValueError, match='EWKB SRID requires an EPSG-authority CRS'):
        geometry.to_wkb(include_srid=True)


def test_geo_rs_non_finite_results_are_rejected() -> None:
    """geo-rs conversions cannot reintroduce NaN/inf-bearing geometry."""
    line = gm.LineString([(1e308, 1e308), (-1e308, -1e308)])
    with pytest.raises(ValueError, match='coordinates must be finite'):
        line.centroid()


def test_proj_out_of_domain_errors_stay_projection_flavored() -> None:
    """The trusted column rebuild never revalidates PROJ output: a PROJ
    failure (out-of-domain input for the pipeline) must surface as the
    projection error, not as 'coordinates must be finite'.
    """
    bad = gm.LineString([(95.0, 95.0), (96.0, 96.0)], crs=4326)
    with pytest.raises(ValueError, match=r'projection|outside|domain|finite'):
        bad.to_crs(2154)


def test_empty_geographic_distance_metrics_match_planar() -> None:
    """EMPTY geographic operands follow the same metric rules as planar empties."""
    empty = gm.from_wkt('LINESTRING EMPTY', crs=4326)
    line = gm.LineString([(0, 0), (1, 1)], crs=4326)
    assert gm.distance(empty, line) == math.inf
    assert gm.hausdorff_distance(empty, line) == math.inf
    with pytest.raises(gm.InvalidGeometryError, match='non-empty linework'):
        gm.frechet_distance(empty, line)


def test_empty_geographic_buffer_and_zero_measures() -> None:
    empty = gm.from_wkt('LINESTRING EMPTY', crs=4326)
    assert empty.buffer(1).to_wkt() == 'POLYGON EMPTY'
    for wkt in ('MULTILINESTRING EMPTY', 'MULTIPOLYGON EMPTY'):
        geometry = gm.from_wkt(wkt, crs=4326)
        measure = geometry.length if 'LINE' in wkt else geometry.area
        assert measure == 0.0
        assert math.copysign(1.0, measure) == 1.0
    collection = gm.GeometryCollection([], crs=4326)
    assert collection.length == 0.0
    assert math.copysign(1.0, collection.length) == 1.0
    assert collection.area == 0.0


def test_nad83_geodesic_length_area_and_distance() -> None:
    """EPSG:4269 metrics are ellipsoidal metres on the NAD83 ellipsoid."""
    line = gm.LineString([(0, 0), (1, 0)], crs=4269)
    assert line.length == pytest.approx(111319.49079327357, rel=1e-09)
    box = gm.box(0, 0, 1, 1, crs=4269)
    assert box.area > 10000000000.0
    off_line = gm.Point(0.5, 1, crs=4269)
    assert gm.distance(line, off_line) > 100000.0


def test_huge_planar_coordinates_are_panic_free_and_finite() -> None:
    """Huge-but-finite planar coordinates never panic and stay finite where defined."""
    huge = gm.box(0, 0, 1e150, 1e150)
    assert math.isfinite(huge.area)
    assert math.isfinite(huge.exterior.length)
    assert huge.bounds == (0.0, 0.0, 1e150, 1e150)
    with pytest.raises(ValueError, match='finite'):
        huge.centroid()


def test_transform_batches_with_empty_members_and_extreme_longitudes() -> None:
    """Two trusted-rebuild edges: an empty XY member inside an otherwise
    all-XYZ batch must transform cleanly (not panic on the missing Z column),
    and an extreme finite longitude that overflows the Web Mercator formula
    must raise rather than store a non-finite ordinate.
    """
    mixed = gm.GeometryCollection(
        [gm.LineString([(10.0, 50.0, 100.0), (10.5, 50.5, 200.0)]), gm.LineString([])],
        crs=4326,
    )
    out = mixed.to_crs(3857)
    assert out.crs == 'EPSG:3857'
    with pytest.raises(ValueError):
        gm.Point(1e308, 0.0, crs=4326).to_crs(3857)
    with pytest.raises(ValueError):
        gm.crs_transform(4326, 3857, [1e308], [0.0])


def test_mixed_2d_3d_collection_proj_batch_preserves_per_member_axes() -> None:
    """EPSG:4979→4978 mixed batches must not reuse stale Z from a whole-batch
    XY-only PROJ call — each 3D member must match a standalone transform.
    """
    pt_3d = gm.Point(21.0, 52.0, z=100.0, m=7.0, crs=4979)
    pt_2d = gm.Point(22.0, 53.0, crs=4979)
    standalone_3d = pt_3d.to_crs(4978)
    standalone_2d = pt_2d.to_crs(4978)
    mixed = gm.GeometryCollection([pt_3d, pt_2d], crs=4979).to_crs(4978)
    out_3d, out_2d = gm.parts(mixed)
    assert out_3d.coordinate_axes == 'XYZM'
    assert out_3d.coords.to_nested() == standalone_3d.coords.to_nested()
    assert out_3d.z == pytest.approx(standalone_3d.z)
    assert out_3d.z != pytest.approx(100.0)
    assert out_3d.m == pytest.approx(7.0)
    assert out_2d.coordinate_axes == 'XY'
    assert out_2d.coords.to_nested() == standalone_2d.coords.to_nested()


def test_mixed_2d_3d_array_proj_batch_preserves_per_member_axes() -> None:
    """GeometryArray batching shares the mixed-Z gather/scatter path."""
    pt_3d = gm.Point(21.0, 52.0, z=100.0, m=7.0, crs=4979)
    pt_2d = gm.Point(22.0, 53.0, crs=4979)
    standalone_3d = pt_3d.to_crs(4978)
    standalone_2d = pt_2d.to_crs(4978)
    mixed = gm.GeometryArray([pt_3d, pt_2d]).to_crs(4978)
    out_3d, out_2d = mixed
    assert out_3d.coordinate_axes == 'XYZM'
    assert out_3d.coords.to_nested() == standalone_3d.coords.to_nested()
    assert out_3d.z == pytest.approx(standalone_3d.z)
    assert out_3d.z != pytest.approx(100.0)
    assert out_3d.m == pytest.approx(7.0)
    assert out_2d.coordinate_axes == 'XY'
    assert out_2d.coords.to_nested() == standalone_2d.coords.to_nested()
