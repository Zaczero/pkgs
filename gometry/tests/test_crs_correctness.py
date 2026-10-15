"""Regression tests for CRS-aware measurement and coordinate-frame behavior."""

from __future__ import annotations

import json
import math

import gometry as gm
import numpy as np
import pytest

from tests._support import (
    EAST_DEGREE_SOUTH_LATITUDE_CRS,
    WEST_RADIAN_SOUTH_GRAD_CRS,
    bools,
    chart_parallel_length,
    chart_rectangle_perimeter,
    chart_zone_area,
    floats,
    ids,
)

pyproj = pytest.importorskip('pyproj', reason='pyproj is an optional oracle dependency')
Geod = pyproj.Geod

FOOT_CRS = 2263
FOOT_UNIT_M = 0.3048006096012192
GEOD = Geod(ellps='WGS84')
RADIAN_CRS = (
    'GEOGCS["WGS 84 rad",'
    'DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],UNIT["radian",1],'
    'AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]'
)
NEAR_UNIFORM_ENGCRS = """ENGCRS["Near-uniform custom",
 EDATUM["Custom datum"],
 CS[Cartesian,2],
 AXIS["easting (X)",east,ORDER[1],LENGTHUNIT["metre",1]],
 AXIS["northing (Y)",north,ORDER[2],LENGTHUNIT["custom",1.0000000005]]]"""
UNIFORM_CUSTOM_ENGCRS = """ENGCRS["Uniform custom",
 EDATUM["Custom datum"],
 CS[Cartesian,2],
 AXIS["easting (X)",east,ORDER[1],LENGTHUNIT["custom",1.0000000005]],
 AXIS["northing (Y)",north,ORDER[2],LENGTHUNIT["custom",1.0000000005]]]"""


def _angular_frame_parameters(crs: object) -> tuple[float, float, float]:
    info = gm.CRS(crs)
    longitude_factor = float(info.axes[0]['unit_conversion_factor'])
    latitude_factor = float(info.axes[1]['unit_conversion_factor'])
    prime_meridian = info.prime_meridian
    prime_meridian_radians = (
        0.0
        if prime_meridian is None
        else float(prime_meridian['longitude'])
        * float(prime_meridian['unit_conversion_factor'])
    )
    return longitude_factor, latitude_factor, prime_meridian_radians


def _native_box_bounds(
    crs: object, physical_bounds: tuple[float, float, float, float]
) -> tuple[float, float, float, float]:
    longitude_factor, latitude_factor, prime_meridian = _angular_frame_parameters(crs)
    minx, miny, maxx, maxy = physical_bounds
    return (
        (math.radians(minx) - prime_meridian) / longitude_factor,
        math.radians(miny) / latitude_factor,
        (math.radians(maxx) - prime_meridian) / longitude_factor,
        math.radians(maxy) / latitude_factor,
    )


def _physical_coordinates(box: gm.Geometry, crs: object) -> list[tuple[float, float]]:
    longitude_factor, latitude_factor, prime_meridian = _angular_frame_parameters(crs)
    return [
        (float(x) * longitude_factor + prime_meridian, float(y) * latitude_factor)
        for x, y in np.asarray(box.coords, dtype=np.float64).tolist()
    ]


def _wgs84_surface_area() -> float:
    eccentricity_squared = GEOD.es
    eccentricity = math.sqrt(eccentricity_squared)
    return (
        2.0
        * math.pi
        * GEOD.a**2
        * (1.0 + (1.0 - eccentricity_squared) * math.atanh(eccentricity) / eccentricity)
    )


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
            [(0.0, 0.0), (0.1, 0.0)],
            [(0.2, 0.0), (0.2, 0.1)],
        ],
        crs=4326,
    )
    rows = gm.GeometryArray([
        gm.Point(0.05, 0.0, crs=4326),
        gm.LineString([(0.0, 0.01), (0.1, 0.01)], crs=4326),
        gm.MultiPoint([(0.3, 0.0), (0.31, 0.01)], crs=4326),
    ])
    row_list = list(rows)
    expected_distances = [gm.distance(row, scalar) for row in row_list]
    assert floats(gm.distance(rows, scalar)) == pytest.approx(expected_distances)
    assert floats(gm.distance(scalar, rows)) == pytest.approx(expected_distances)
    expected_dwithin = [gm.dwithin(row, scalar, 0.0) for row in row_list]
    assert bools(gm.dwithin(rows, scalar, 0.0)) == expected_dwithin
    assert bools(gm.dwithin(scalar, rows, 0.0)) == expected_dwithin
    expected_dwithin = [gm.dwithin(row, scalar, 2000.0) for row in row_list]
    assert bools(gm.dwithin(rows, scalar, 2000.0)) == expected_dwithin
    assert bools(gm.dwithin(scalar, rows, 2000.0)) == expected_dwithin
    expected_nearest = [gm.nearest_points(row, scalar) for row in row_list]
    assert list(zip(*gm.nearest_points(rows, scalar), strict=True)) == expected_nearest
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


def test_custom_near_uniform_axes_are_not_substituted_for_metric_units() -> None:
    """A near-equal but distinct axis factor must not be replaced by X's."""
    uniform_a = gm.Point(1_000_000_000.0, 0.0, crs=UNIFORM_CUSTOM_ENGCRS)
    uniform_b = gm.Point(2_000_000_000.0, 0.0, crs=UNIFORM_CUSTOM_ENGCRS)
    assert gm.distance(uniform_a, uniform_b, unit='meters') == 1_000_000_000.5

    northing_a = gm.Point(0.0, 1_000_000_000.0, crs=NEAR_UNIFORM_ENGCRS)
    northing_b = gm.Point(0.0, 2_000_000_000.0, crs=NEAR_UNIFORM_ENGCRS)

    with pytest.raises(gm.CRSError, match='different linear units'):
        gm.distance(northing_a, northing_b, unit='meters')
    with pytest.raises(gm.CRSError, match='axis units differ'):
        gm.distance_3d(
            gm.Point(0.0, 1_000_000_000.0, z=0.0, crs=NEAR_UNIFORM_ENGCRS),
            gm.Point(0.0, 2_000_000_000.0, z=0.0, crs=NEAR_UNIFORM_ENGCRS),
        )


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
    destination = point.destination(90.0, 1.0)
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


def test_chart_exterior_length_is_ellipsoidal_and_excludes_holes() -> None:
    """An exterior line is not a polygon perimeter or a planar/spherical proxy."""
    exterior = [
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0),
    ]
    hole = [
        (0.25, 0.25),
        (0.75, 0.25),
        (0.75, 0.75),
        (0.25, 0.75),
        (0.25, 0.25),
    ]
    exterior_line = gm.MultiLineString([exterior], crs=4326)
    polygon = gm.Polygon(exterior, holes=[hole], crs=4326)
    exterior_length = chart_rectangle_perimeter(GEOD, 0.0, 1.0, 0.0, 1.0)
    hole_length = chart_rectangle_perimeter(GEOD, 0.25, 0.75, 0.25, 0.75)

    assert exterior_line.length == pytest.approx(exterior_length, rel=1e-12)
    assert polygon.length == pytest.approx(exterior_length + hole_length, rel=1e-12)
    assert exterior_line.length != pytest.approx(polygon.length, rel=1e-12)
    assert exterior_line.length != pytest.approx(4.0)
    z_line = gm.LineString([(0.0, 0.0, 10.0), (1.0, 0.0, 20.0)], crs=4326)
    assert z_line.length == pytest.approx(
        chart_parallel_length(0.0, 1.0, semi_major=GEOD.a, flattening=GEOD.f)
    )


def test_chart_area_uses_the_chart_interior_for_large_regions() -> None:
    ellipsoid_area = _wgs84_surface_area()
    origin = gm.Point(0.0, 0.0, crs=4326)
    for bounds in (
        (-170.0, -60.0, 170.0, 60.0),
        (-180.0, -85.0, 180.0, 85.0),
    ):
        geometry = gm.box(*bounds, crs=4326)
        west, south, east, north = bounds
        expected = chart_zone_area(
            west, east, south, north, semi_major=GEOD.a, flattening=GEOD.f
        )
        assert expected > ellipsoid_area / 2.0
        coordinates = np.asarray(geometry.coords, dtype=np.float64).tolist()
        for ring in (coordinates[:-1], list(reversed(coordinates[:-1]))):
            polygon = gm.Polygon(ring, crs=4326)
            assert gm.contains(polygon, origin)
            assert polygon.area == pytest.approx(expected, rel=2e-15)


def test_chart_polar_cap_area_is_orientation_independent() -> None:
    ring = [
        (0.0, 80.0),
        (60.0, 80.0),
        (120.0, 80.0),
        (180.0, 80.0),
        (-120.0, 80.0),
        (-60.0, 80.0),
    ]
    north_pole = gm.Point(0.0, 90.0, crs=4326)
    expected = chart_zone_area(
        -180.0, 180.0, 80.0, 90.0, semi_major=GEOD.a, flattening=GEOD.f
    )
    for vertices in (ring, list(reversed(ring))):
        polygon = gm.Polygon(vertices, crs=4326)
        assert gm.contains(polygon, north_pole)
        # Both the closed-form oracle and edge series use binary64 near the pole.
        assert polygon.area == pytest.approx(expected, rel=3e-15)


@pytest.mark.parametrize(
    ('physical_latitude', 'native_latitude', 'native_pole', 'south', 'north'),
    [
        pytest.param(80.0, -80.0, -90.0, 80.0, 90.0, id='north-pole'),
        pytest.param(-80.0, 80.0, 90.0, -90.0, -80.0, id='south-pole'),
    ],
)
@pytest.mark.parametrize(
    'crs',
    [EAST_DEGREE_SOUTH_LATITUDE_CRS, WEST_RADIAN_SOUTH_GRAD_CRS],
    ids=['east-degree-south-degree', 'west-radian-south-grad'],
)
def test_chart_polar_cap_area_respects_south_directed_latitude_axis(
    physical_latitude: float,
    native_latitude: float,
    native_pole: float,
    south: float,
    north: float,
    crs: str,
) -> None:
    longitudes = (0.0, 60.0, 120.0, 180.0, -120.0, -60.0)
    standard = gm.Polygon(
        [(longitude, physical_latitude) for longitude in longitudes], crs=4326
    )
    if crs == WEST_RADIAN_SOUTH_GRAD_CRS:
        native_latitude *= 10.0 / 9.0
        native_pole = math.copysign(100.0, native_pole)
        native_longitudes = [-math.radians(longitude) for longitude in longitudes]
    else:
        native_longitudes = longitudes
    south_axis = gm.Polygon(
        [(longitude, native_latitude) for longitude in native_longitudes],
        crs=crs,
    )
    expected = chart_zone_area(
        -180.0, 180.0, south, north, semi_major=GEOD.a, flattening=GEOD.f
    )

    assert standard.area == pytest.approx(expected, rel=3e-15)
    assert south_axis.area == pytest.approx(expected, rel=3e-15)
    assert gm.contains(south_axis, gm.Point(0.0, native_pole, crs=crs))
    assert not gm.contains(south_axis, gm.Point(0.0, -native_pole, crs=crs))


def test_chart_area_subtracts_holes_from_a_large_chart_interior() -> None:
    shell = gm.box(-170.0, -60.0, 170.0, 60.0, crs=4326)
    shell_coordinates = np.asarray(shell.coords, dtype=np.float64).tolist()
    hole = [
        (-10.0, -10.0),
        (10.0, -10.0),
        (10.0, 10.0),
        (-10.0, 10.0),
        (-10.0, -10.0),
    ]
    polygon = gm.Polygon(shell_coordinates[:-1], holes=[hole], crs=4326)
    ellipsoid_area = _wgs84_surface_area()
    expected = chart_zone_area(
        -170.0, 170.0, -60.0, 60.0, semi_major=GEOD.a, flattening=GEOD.f
    ) - chart_zone_area(-10.0, 10.0, -10.0, 10.0, semi_major=GEOD.a, flattening=GEOD.f)

    assert expected > ellipsoid_area / 2.0
    assert gm.contains(polygon, gm.Point(100.0, 0.0, crs=4326))
    assert not gm.contains(polygon, gm.Point(0.0, 0.0, crs=4326))
    assert polygon.area == pytest.approx(expected, rel=2e-15)


def test_chart_area_array_matches_scalar_in_packed_and_mixed_storage() -> None:
    wide = gm.box(-170.0, -60.0, 170.0, 60.0, crs=4326)
    coordinates = np.asarray(wide.coords, dtype=np.float64).tolist()
    reversed_wide = gm.Polygon(list(reversed(coordinates[:-1])), crs=4326)
    expected = [wide.area, reversed_wide.area]

    packed = gm.GeometryArray([wide, reversed_wide], crs=4326)
    mixed = gm.GeometryArray(
        [wide, gm.Point(0.0, 0.0, crs=4326), reversed_wide],
        crs=4326,
    )

    assert packed.area.tolist() == pytest.approx(expected, rel=0.0, abs=0.0)
    assert mixed.area.tolist() == pytest.approx(
        [expected[0], 0.0, expected[1]],
        rel=0.0,
        abs=0.0,
    )


def test_chart_area_matches_the_authalic_zone_formula_for_a_small_polygon() -> None:
    polygon = gm.box(2.0, 48.0, 3.0, 49.0, crs=4326)
    formula = chart_zone_area(
        2.0, 3.0, 48.0, 49.0, semi_major=GEOD.a, flattening=GEOD.f
    )
    # 100-digit Decimal integral from exact binary64 ellipsoid values and true π.
    expected = 8_217_732_076.120_943
    assert formula == pytest.approx(expected, rel=1e-14)
    assert polygon.area == pytest.approx(expected, rel=1e-14)


def test_degenerate_chart_rings_have_zero_area() -> None:
    seam_aliases = gm.Polygon(
        [(180.0, -10.0), (-180.0, -10.0), (-180.0, 10.0), (180.0, 10.0)],
        crs=4326,
    )
    equatorial = gm.Polygon(
        [(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)],
        crs=4326,
    )
    chart_collinear = gm.Polygon(
        [(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)],
        crs=4326,
    )

    assert seam_aliases.area == 0.0
    assert equatorial.area == 0.0
    assert chart_collinear.area == 0.0


def test_packed_chart_polygon_length_uses_selected_ring_windows() -> None:
    shell = [
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0),
    ]
    hole = [
        (0.25, 0.25),
        (0.75, 0.25),
        (0.75, 0.75),
        (0.25, 0.75),
        (0.25, 0.25),
    ]
    polygon = gm.Polygon(shell, holes=[hole], crs=4326)
    expected = chart_rectangle_perimeter(GEOD, 0.0, 1.0, 0.0, 1.0) + (
        chart_rectangle_perimeter(GEOD, 0.25, 0.75, 0.25, 0.75)
    )
    source = gm.GeometryArray(
        [polygon, gm.from_wkt('POLYGON EMPTY', crs=4326), None, polygon],
        crs=4326,
    )

    measured = gm.length(source[[3, 1, 2, 0, 3]], unit='meters')

    assert measured[0] == pytest.approx(expected, rel=1e-12)
    assert measured[1] == 0.0
    assert math.isnan(measured[2])
    assert measured[3] == pytest.approx(expected, rel=1e-12)
    assert measured[4] == pytest.approx(expected, rel=1e-12)


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


def test_geodesic_point_to_segment_array_matches_scalar_across_cache_clear() -> None:
    """Packed queries preserve interior and endpoint minima on cold and warm paths."""
    segment = gm.LineString([(0, 0), (5, 0)], crs=4326)
    query_points = [gm.Point(2, 1, crs=4326), gm.Point(6, 3, crs=4326)]
    queries = gm.points([2, 6], [1, 3], crs=4326)
    expected = [
        GEOD.inv(2, 1, 2, 0)[2],
        GEOD.inv(6, 3, 5, 0)[2],
    ]

    gm.crs_clear_cache()
    cold = gm.distance(queries, segment)
    assert cold == pytest.approx(expected)
    assert cold == pytest.approx([
        gm.distance(point, segment) for point in query_points
    ])

    warm = gm.distance(queries, segment)
    assert warm == pytest.approx(expected)

    gm.crs_clear_cache()
    cleared = gm.distance(queries, segment)
    assert cleared == pytest.approx(expected)


def _geodesic_point_collection_membership_cases() -> list[object]:
    query = gm.Point(0.0, 0.0, crs=4326)
    far = gm.Point(20.0, 0.0, crs=4326)
    return [
        pytest.param(
            query,
            gm.GeometryCollection([query, far]),
            id='direct-point-siblings',
        ),
        pytest.param(
            query,
            gm.GeometryCollection([gm.GeometryCollection([query]), far]),
            id='nested-point-collection',
        ),
        pytest.param(
            query,
            gm.GeometryCollection([query, gm.GeometryCollection([], crs=4326)]),
            id='trailing-empty-collection',
        ),
        pytest.param(
            query,
            gm.GeometryCollection([
                query,
                gm.LineString([(20.0, 0.0), (21.0, 0.0)], crs=4326),
            ]),
            id='trailing-nonpoint-member',
        ),
    ]


@pytest.mark.parametrize(
    ('query', 'collection'), _geodesic_point_collection_membership_cases()
)
def test_geodesic_point_collections_keep_members_for_distance_and_dwithin(
    query: gm.Point, collection: gm.GeometryCollection
) -> None:
    """Collection point sweeps retain nested and preceding members."""
    assert gm.distance(query, collection) == 0.0
    assert gm.distance(collection, query) == 0.0
    assert gm.dwithin(query, collection, 0.0) is True
    assert gm.dwithin(collection, query, 0.0) is True


def test_geodesic_nearest_points_collection_tie_keeps_source_order() -> None:
    """Equal-distance collection points use their original member order."""
    query = gm.Point(0.0, 0.0, crs=4326)
    ordered = gm.GeometryCollection([
        gm.Point(-1.0, 0.0, crs=4326),
        gm.Point(1.0, 0.0, crs=4326),
    ])
    _, right_nearest = gm.nearest_points(query, ordered)
    left_nearest, _ = gm.nearest_points(ordered, query)
    assert (right_nearest.x, right_nearest.y) == (-1.0, 0.0)
    assert (left_nearest.x, left_nearest.y) == (-1.0, 0.0)


def test_geographic_lrs_is_metre_based_and_self_consistent() -> None:
    """Meridian chart edges are geodesics; geographic LRS reports ellipsoidal metres."""
    line = gm.LineString([(0, 0), (0, 10)], crs=4326)
    length = line.length
    assert length == pytest.approx(1105854.8332343723)
    interpolated = line.line_interpolate(1000.0)
    assert interpolated.y == pytest.approx(0.009043694769749644)
    loc = line.line_locate(gm.Point(0, 5, crs=4326))
    assert loc == pytest.approx(length / 2, rel=0.001)
    assert line.line_substring(0, loc).length == pytest.approx(loc)


def test_normalized_lrs_fraction_uses_chart_edge_length() -> None:
    """normalized=True uses the chart-edge total; a meridian is also geodesic."""
    line = gm.LineString([(0, 0), (0, 10)], crs=4326)
    half = line.line_interpolate(0.5, normalized=True)
    loc = line.line_locate(half, normalized=True)
    assert loc == pytest.approx(0.5)


def test_geographic_chart_lrs_zm_interpolation_never_corrupts_xy() -> None:
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
    with pytest.raises(gm.InvalidGeometryError):
        _ = gm.LineString([(0, 95), (1, 95)], crs=4326).length
    with pytest.raises(gm.InvalidGeometryError):
        _ = gm.box(0, 95, 1, 96, crs=4326).area
    with pytest.raises(gm.InvalidGeometryError):
        _ = gm.GeometryCollection([gm.Point(0, 95, crs=4326)]).area
    with pytest.raises(gm.InvalidGeometryError):
        gm.distance(gm.Point(0, 95, crs=4326), gm.Point(1, 95, crs=4326))
    with pytest.raises(gm.InvalidGeometryError):
        gm.bearing(gm.Point(0, 95, crs=4326), gm.Point(1, 95, crs=4326))
    with pytest.raises(gm.InvalidGeometryError):
        gm.Point(0, 95, crs=4326).destination(45, 1000)


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
    assert collection.crs == gm.CRS('EPSG:4326')
    assert collection.epoch == 2020.0
    assert [part.crs for part in collection.parts] == [gm.CRS('EPSG:4326')] * 2
    assert gm.GeometryCollection([first, second], crs=4326, epoch=2020.0).crs == gm.CRS(
        'EPSG:4326'
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
    assert array.crs == gm.CRS('EPSG:4326')
    assert array.epoch == 2020.0
    assert [item.crs for item in array] == [gm.CRS('EPSG:4326')] * 2
    assert [item.epoch for item in array] == [2020.0, 2020.0]
    epoch_array = gm.GeometryArray([crs_only, epoch_tagged], epoch=2020.0)
    assert [item.epoch for item in epoch_array] == [2020.0, 2020.0]
    collection = gm.GeometryCollection([bare, crs_only], crs=4326, epoch=2020.0)
    assert collection.crs == gm.CRS('EPSG:4326')
    assert collection.epoch == 2020.0
    assert [item.epoch for item in collection.parts] == [2020.0, 2020.0]
    epoch_collection = gm.GeometryCollection([crs_only, epoch_tagged], epoch=2020.0)
    assert [item.epoch for item in epoch_collection.parts] == [2020.0, 2020.0]
    with pytest.raises(ValueError, match='one shared CRS'):
        gm.GeometryArray([bare, crs_only])
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or with_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.GeometryArray([bare], epoch=2020.0)
    with pytest.raises(ValueError, match='one shared coordinate epoch'):
        gm.GeometryArray([epoch_tagged], epoch=2021.0)
    with pytest.raises(
        ValueError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or with_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.GeometryCollection([bare], epoch=2020.0)


def test_non_degree_geographic_crs_uses_native_units_for_geodesic_metrics() -> None:
    """Grad-unit geographic coordinates are admitted and normalized at kernels."""
    grad_crs = gm.CRS(4807)
    assert grad_crs.is_geographic
    assert [axis['unit_name'] for axis in grad_crs.axes[:2]] == ['grad', 'grad']
    assert gm.distance(
        gm.Point(0, 0, crs=4807), gm.Point(1, 0, crs=4807)
    ) == pytest.approx(100189.3, rel=1e-6)


def test_oriented_mixed_unit_chart_consumers_use_the_selected_frame() -> None:
    longitudes = [170.0 + 2.5 * index for index in range(9)]
    latitude = 60.0
    native_line = [
        (-math.radians(longitude), -latitude * 10.0 / 9.0) for longitude in longitudes
    ]
    line = gm.LineString(native_line, crs=WEST_RADIAN_SOUTH_GRAD_CRS)
    expected_length = chart_parallel_length(
        latitude, 20.0, semi_major=GEOD.a, flattening=GEOD.f
    )
    assert line.length == pytest.approx(expected_length, rel=1e-14)

    midpoint = line.line_interpolate(expected_length / 2.0)
    assert line.line_locate(midpoint) == pytest.approx(expected_length / 2.0, rel=1e-12)
    physical_midpoint = midpoint.to_crs(4326)
    assert abs(abs(physical_midpoint.x) - 180.0) <= 1.0e-12
    assert physical_midpoint.y == pytest.approx(latitude, abs=2.0e-13)

    query = gm.Point(-math.pi, -60.1 * 10.0 / 9.0, crs=WEST_RADIAN_SOUTH_GRAD_CRS)
    expected_clearance = GEOD.inv(180.0, latitude, 180.0, 60.1)[2]
    assert gm.distance(query, line) == pytest.approx(expected_clearance, rel=1e-12)
    assert gm.dwithin(query, line, expected_clearance + 0.01)
    assert not gm.dwithin(query, line, expected_clearance - 0.01)
    nearest = gm.nearest_points(query, line)
    assert gm.distance(*nearest) == pytest.approx(expected_clearance, rel=1e-12)

    alias = gm.LineString(
        [(longitude + math.tau, y) for longitude, y in native_line],
        crs=WEST_RADIAN_SOUTH_GRAD_CRS,
    )
    assert gm.hausdorff_distance(line, alias) == 0.0
    offset_parallel = gm.LineString(
        [(-math.radians(longitude), -60.01 * 10.0 / 9.0) for longitude in longitudes],
        crs=WEST_RADIAN_SOUTH_GRAD_CRS,
    )
    expected_hausdorff = GEOD.inv(180.0, latitude, 180.0, 60.01)[2]
    assert gm.hausdorff_distance(line, offset_parallel) == pytest.approx(
        expected_hausdorff, rel=1e-12
    )


def test_oriented_mixed_unit_polygon_perimeter_matches_chart_rectangle() -> None:
    physical_ring = [
        (10.0, 30.0),
        (20.0, 30.0),
        (20.0, 40.0),
        (10.0, 40.0),
        (10.0, 30.0),
    ]
    native_ring = [
        (-math.radians(longitude), -latitude * 10.0 / 9.0)
        for longitude, latitude in physical_ring
    ]
    polygon = gm.Polygon(native_ring, crs=WEST_RADIAN_SOUTH_GRAD_CRS)
    expected = chart_rectangle_perimeter(GEOD, 10.0, 20.0, 30.0, 40.0)

    assert polygon.length == pytest.approx(expected, rel=1e-12)


def test_local_offset_certificate_uses_oriented_mixed_unit_frame() -> None:
    crs = WEST_RADIAN_SOUTH_GRAD_CRS
    line = gm.LineString(
        [
            (-math.radians(10.0), -50.0 * 10.0 / 9.0),
            (-math.radians(10.01), -50.0 * 10.0 / 9.0),
        ],
        crs=crs,
    )
    assert line.estimate_local_crs().is_projected

    midpoint = line.line_interpolate(0.5, normalized=True)
    offset = line.offset_curve(50.0, unit='meters')

    assert offset.crs == line.crs
    assert gm.distance(midpoint, offset) == pytest.approx(50.0, rel=1.0e-3)


@pytest.mark.parametrize(
    ('physical_bounds', 'grad_latitude_above_90', 'grad_longitude_above_180'),
    [
        pytest.param(
            (0.0, 84.0, 90.0, 85.0),
            True,
            False,
            id='grad-latitude-above-90',
        ),
        pytest.param(
            (2.33722917, 40.0, 173.33722917, 45.0),
            False,
            True,
            id='grad-longitude-above-180',
        ),
    ],
)
def test_geographic_box_uses_frame_native_half_period(
    physical_bounds: tuple[float, float, float, float],
    grad_latitude_above_90: bool,
    grad_longitude_above_180: bool,
) -> None:
    """Equivalent angular frames retain the same stored physical box geometry."""
    crss = (4326, RADIAN_CRS, 4807)
    native_bounds = [_native_box_bounds(crs, physical_bounds) for crs in crss]
    grad_bounds = native_bounds[-1]
    if grad_latitude_above_90:
        assert grad_bounds[1] > 90.0
    if grad_longitude_above_180:
        assert grad_bounds[2] > 180.0

    boxes = [
        gm.box(*bounds, crs=crs)
        for crs, bounds in zip(crss, native_bounds, strict=True)
    ]
    for box, bounds in zip(boxes, native_bounds, strict=True):
        assert box.bounds == bounds

    reference = _physical_coordinates(boxes[0], crss[0])
    assert len(reference) == 5
    for box, crs in zip(boxes[1:], crss[1:], strict=True):
        actual = _physical_coordinates(box, crs)
        assert len(actual) == len(reference)
        for actual_point, reference_point in zip(actual, reference, strict=True):
            # Conversion round-trips are compared within a few binary64 ulps
            # at unit scale; this is not a geometry-fidelity tolerance.
            assert actual_point == pytest.approx(
                reference_point,
                rel=0.0,
                abs=8.0 * math.ulp(1.0),
            )


def test_bound_geographic_crs_uses_its_typed_frame_for_metrics() -> None:
    """A bound geographic CRS keeps its source angular frame and ellipsoid."""
    bound_crs = '+proj=longlat +ellps=clrk80ign +pm=paris +towgs84=0,0,0 +type=crs'
    origin = gm.Point(0.0, 0.0, crs=bound_crs)
    east = gm.Point(1.0, 0.0, crs=bound_crs)

    assert gm.CRS(bound_crs).kind == 'bound'
    assert gm.distance(origin, east) == pytest.approx(111321.44905269431, rel=1e-12)
    index = gm.SpatialIndex([origin])
    aliases = gm.GeometryArray([gm.Point(360.0, 0.0, crs=bound_crs)])
    assert ids(index.candidates(origin)) == [0]
    assert [row.tolist() for row in index.candidates(aliases)] == [[0]]
    with pytest.raises(ValueError, match='invalid longitude/latitude'):
        gm.crs_transform(bound_crs, bound_crs, [0.0], [101.0])


@pytest.mark.parametrize(
    ('crs', 'period'),
    [(4326, 360.0), (4807, 400.0), (RADIAN_CRS, math.tau)],
    ids=['degree', 'grad', 'radian'],
)
def test_geographic_native_aliases_reach_scalar_array_and_index_paths(
    crs: object, period: float
) -> None:
    """One native periodic turn is one location through every metric path."""
    origin = gm.Point(0.0, 0.0, crs=crs)
    aliases = gm.GeometryArray([
        gm.Point(period, 0.0, crs=crs),
        gm.Point(-period, 0.0, crs=crs),
    ])

    assert gm.distance(origin, aliases).tolist() == pytest.approx([0.0, 0.0])
    assert bools(gm.dwithin(origin, aliases, 1.0)) == [True, True]

    index = gm.SpatialIndex([origin, gm.Point(1.0, 0.0, crs=crs)])
    alias = aliases[0]
    assert ids(index.candidates(alias)) == [0]
    assert ids(index.query(alias, predicate='intersects')) == [0]
    assert ids(index.query(alias, predicate='dwithin', distance=1.0)) == [0]
    assert ids(index.nearest(alias)) == [0]


def test_grad_alias_transform_uses_the_same_prime_meridian_location() -> None:
    """EPSG:4807 aliases transform to the same Greenwich location."""
    origin = gm.Point(0.0, 0.0, crs=4807)
    alias = gm.Point(400.0, 0.0, crs=4807)
    greenwich = origin.to_crs(4326)
    alias_greenwich = alias.to_crs(4326)

    assert (alias_greenwich.x, alias_greenwich.y) == pytest.approx((
        greenwich.x,
        greenwich.y,
    ))


def test_geographic_native_aliases_preserve_empty_missing_and_epoch_contracts() -> None:
    """Frame admission does not turn empty or missing rows into coordinates."""
    origin = gm.Point(0.0, 0.0, crs=4807)
    empty = gm.Point(crs=4807)
    rows = gm.GeometryArray([gm.Point(400.0, 0.0, crs=4807), None, empty])

    distances = gm.distance(rows, origin).tolist()
    assert distances[0] == pytest.approx(0.0)
    assert math.isnan(distances[1])
    assert math.isinf(distances[2])
    assert bools(gm.dwithin(rows, origin, 1.0)) == [True, False, False]

    first = gm.Point(0.0, 0.0, crs=9000, epoch=2020.0)
    same_epoch = gm.Point(360.0, 0.0, crs=9000, epoch=2020.0)
    assert gm.distance(first, same_epoch) == pytest.approx(0.0)
    with pytest.raises(gm.CRSMismatchError, match='requires matching coordinate epoch'):
        gm.distance(first, gm.Point(0.0, 0.0, crs=9000, epoch=2021.0))


def test_to_wkb_wire_aliases_crs84_and_rejects_unknown() -> None:
    """CRS84/CRS84h are exact PostGIS wire aliases; other non-EPSG CRS still raise."""
    crs84 = gm.Point(10, 50, crs='OGC:CRS84')
    ewkb = crs84.to_wkb(include_srid=True)
    lost = gm.from_wkb(ewkb)
    assert lost.crs == gm.CRS('EPSG:4326')
    assert gm.equals(lost, gm.Point(10, 50, crs=4326))
    assert gm.from_wkb(ewkb, crs='OGC:CRS84').crs == gm.CRS('OGC:CRS84')

    crs84h = gm.Point(10, 50, z=0, crs='OGC:CRS84h')
    ewkb_h = crs84h.to_wkb(include_srid=True)
    assert gm.from_wkb(ewkb_h).crs == gm.CRS('EPSG:4979')
    assert gm.from_wkb(ewkb_h, crs='OGC:CRS84h').crs == gm.CRS('OGC:CRS84h')

    # A genuine conflict still raises (not every CRS is a wire alias of 4326).
    with pytest.raises(gm.CRSMismatchError):
        gm.from_wkb(ewkb, crs=4258)

    # Non-alias non-EPSG CRS still cannot embed an SRID.
    geometry = gm.Point(0, 0, crs='ESRI:102001')
    with pytest.raises(ValueError, match='EWKB SRID requires an EPSG-authority CRS'):
        geometry.to_wkb(include_srid=True)


def test_extreme_finite_line_centroid_stays_finite() -> None:
    """A mathematically finite centroid may not inherit an overflowed weight."""
    line = gm.LineString([(1e308, 1e308), (-1e308, -1e308)])
    centroid = line.centroid()
    assert (centroid.x, centroid.y) == (0.0, 0.0)


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
    assert gm.frechet_distance(empty, line) == math.inf


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
    # Shared-scale centroid rescue keeps the geometric centre representable.
    c = huge.centroid()
    assert math.isfinite(c.x) and math.isfinite(c.y)
    assert c.x == pytest.approx(0.5e150, rel=1e-9)
    assert c.y == pytest.approx(0.5e150, rel=1e-9)


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
    assert out.crs == gm.CRS('EPSG:3857')
    with pytest.raises(gm.InvalidGeometryError):
        gm.Point(1e308, 0.0, crs=4326).to_crs(3857)
    with pytest.raises(gm.InvalidGeometryError):
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
    out_3d, out_2d = mixed.parts
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
