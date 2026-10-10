"""CRS behavior — geodesic distances, chart-edge measures, transforms,
best-UTM selection, runtime config, cache info, and PROJ authority metadata.
"""

import json
import math
from itertools import pairwise
from typing import cast

import gometry as gm
import pytest

from tests._support import (
    chart_parallel_length,
    chart_rectangle_perimeter,
    chart_zone_area,
)

shapely = pytest.importorskip(
    'shapely', reason='shapely is an optional oracle dependency'
)
pyproj = pytest.importorskip('pyproj', reason='pyproj is an optional oracle dependency')
Geod = pyproj.Geod

GEOD = Geod(ellps='WGS84')
LONG_PARALLEL_WEST = -0.463_147_336_714_631_5
LONG_PARALLEL_EAST = 6.463_147_336_714_631_5
LONG_PARALLEL_SOUTH = 39.0
LONG_PARALLEL_NORTH = 39.2


def _six_hundred_kilometer_parallel() -> gm.Polygon:
    return gm.Polygon(
        [
            (LONG_PARALLEL_WEST, LONG_PARALLEL_SOUTH),
            (LONG_PARALLEL_EAST, LONG_PARALLEL_SOUTH),
            (LONG_PARALLEL_EAST, LONG_PARALLEL_NORTH),
            (LONG_PARALLEL_WEST, LONG_PARALLEL_NORTH),
            (LONG_PARALLEL_WEST, LONG_PARALLEL_SOUTH),
        ],
        crs=4326,
    )


def _projected_chart_outline(
    geometry: gm.Geometry,
    local: gm.CRS,
) -> shapely.Geometry:
    chart_densified = geometry.segmentize(250.0)
    return shapely.from_wkt(chart_densified.to_crs(local).to_wkt())


def _distance_to_long_parallel_boundary(longitude: float, latitude: float) -> float:
    clamped_longitude = min(
        LONG_PARALLEL_EAST,
        max(LONG_PARALLEL_WEST, longitude),
    )
    closest = min(
        GEOD.inv(
            longitude,
            latitude,
            clamped_longitude,
            LONG_PARALLEL_SOUTH,
        )[2],
        GEOD.inv(
            longitude,
            latitude,
            clamped_longitude,
            LONG_PARALLEL_NORTH,
        )[2],
    )
    for meridian in (LONG_PARALLEL_WEST, LONG_PARALLEL_EAST):
        low, high = LONG_PARALLEL_SOUTH, LONG_PARALLEL_NORTH
        for _ in range(44):
            left = low + (high - low) / 3.0
            right = high - (high - low) / 3.0
            if (
                GEOD.inv(longitude, latitude, meridian, left)[2]
                < GEOD.inv(
                    longitude,
                    latitude,
                    meridian,
                    right,
                )[2]
            ):
                high = right
            else:
                low = left
        closest = min(
            closest,
            GEOD.inv(longitude, latitude, meridian, low)[2],
            GEOD.inv(longitude, latitude, meridian, LONG_PARALLEL_SOUTH)[2],
            GEOD.inv(longitude, latitude, meridian, LONG_PARALLEL_NORTH)[2],
        )
    return closest


def _custom_geographic_crs(flattening: float) -> str:
    return f'+proj=longlat +a=6378137 +f={flattening:.17g} +type=crs'


@pytest.mark.parametrize(
    'flattening',
    [0.0, 0.02],
    ids=['sphere', 'oblate-boundary'],
)
def test_geodesic_series_domain_boundaries_reach_each_metric_path(
    flattening: float,
) -> None:
    crs = _custom_geographic_crs(flattening)
    geodesic = gm.CRS(crs)
    assert geodesic.ellipsoid is not None
    assert float(geodesic.ellipsoid['inverse_flattening']) == pytest.approx(
        0.0 if flattening == 0.0 else 1.0 / flattening
    )
    start = gm.Point(0.0, 0.0, crs=crs)
    end = gm.Point(1.0, 0.0, crs=crs)
    line = gm.LineString([(0.0, 0.0), (1.0, 0.0)], crs=crs)
    crossing = gm.LineString([(0.5, -1.0), (0.5, 1.0)], crs=crs)

    distance = gm.distance(start, end)
    assert distance > 0.0
    direct = start.destination(90.0, distance)
    assert direct.x == pytest.approx(1.0)
    assert direct.y == pytest.approx(0.0)
    assert gm.distance(start, line) == 0.0
    assert gm.dwithin(start, line, 0.0)
    assert gm.intersects(line, crossing)
    assert gm.nearest_points(start, line)[0].to_wkt() == start.to_wkt()
    assert gm.hausdorff_distance(start, end) == pytest.approx(distance)
    parallel = gm.LineString([(0.0, 60.0), (1.0, 60.0)], crs=crs)
    assert parallel.length == pytest.approx(
        chart_parallel_length(60.0, 1.0, semi_major=6_378_137.0, flattening=flattening),
        rel=1e-15,
    )
    box = gm.box(0.0, 0.0, 1.0, 1.0, crs=crs)
    assert box.area == pytest.approx(
        chart_zone_area(
            0.0,
            1.0,
            0.0,
            1.0,
            semi_major=6_378_137.0,
            flattening=flattening,
        ),
        rel=1e-13,
    )


def test_geodesic_parallel_proximity_is_not_segment_contact() -> None:
    left = gm.LineString([(-1.0, 0.0), (1.0, 0.0)], crs=4326)
    right = gm.LineString([(-1.0, 1e-14), (1.0, 1e-14)], crs=4326)

    distance = gm.distance(left, right)

    assert distance > 0.0
    assert not gm.dwithin(left, right, 0.0)
    first, second = gm.nearest_points(left, right)
    assert first.to_wkt() != second.to_wkt()


def test_huge_longitude_chart_position_matches_distance_and_dwithin() -> None:
    line = gm.LineString([(1e18, 0.0), (0.0, 0.0)], crs=4326)
    point = gm.Point(48.0, 0.0, crs=4326)
    expected_endpoint_distance = 6_378_137.0 * math.radians(48.0)

    assert gm.distance(line, point) == pytest.approx(
        expected_endpoint_distance, abs=1e-8
    )
    assert not gm.dwithin(line, point, 1.0)
    assert not gm.dwithin(line, point, expected_endpoint_distance - 1.0)
    assert gm.dwithin(line, point, expected_endpoint_distance + 1.0)


def test_huge_longitude_conditioned_curve_keeps_distance_and_nearest_on_section() -> (
    None
):
    curve = gm.LineString([(1e18, -80.0), (0.0, 0.0)], crs=4326)
    query = gm.Point(48.0, 0.0, crs=4326)
    expected_endpoint_distance = 6_378_137.0 * math.radians(48.0)

    assert gm.distance(curve, query) == pytest.approx(
        expected_endpoint_distance, abs=1e-8
    )
    assert not gm.dwithin(curve, query, 1.0)
    line_witness, query_witness = gm.nearest_points(curve, query)
    assert line_witness.to_wkt() == 'POINT (0 0)'
    assert query_witness.to_wkt() == 'POINT (48 0)'
    assert gm.distance(line_witness, query_witness) == pytest.approx(
        expected_endpoint_distance, abs=1e-8
    )


def test_geodesic_zero_length_line_retains_point_support() -> None:
    line = gm.LineString([(0.0, 0.0), (0.0, 0.0)], crs=4326)
    point = gm.Point(0.0, 0.0, crs=4326)

    assert gm.distance(line, point) == 0.0
    assert gm.dwithin(line, point, 0.0)
    first, second = gm.nearest_points(line, point)
    assert first.to_wkt() == second.to_wkt()


def test_geodesic_periodic_zero_edge_uses_point_support_without_resplitting() -> None:
    seam = gm.LineString([(-180.0, 0.0), (180.0, 0.0)], crs=4326)
    meridian = gm.LineString([(-180.0, -1.0), (-180.0, 1.0)], crs=4326)

    assert gm.distance(seam, meridian) == 0.0
    assert gm.dwithin(seam, meridian, 0.0)


def test_geodesic_positive_infinite_dwithin_does_not_force_distance_resolution() -> (
    None
):
    left = gm.Point(0.0, 0.0, crs=4326)
    right = gm.Point(180.0, 0.0, crs=4326)

    assert gm.dwithin(left, right, math.inf)


def test_geodesic_positive_infinite_dwithin_broadcasts_over_array_operands() -> None:
    left = gm.points([0.0, 1.0], [0.0, 0.0], crs=4326)
    right = gm.Point(180.0, 0.0, crs=4326)
    empty = gm.LineString([], crs=4326)

    assert gm.dwithin(left, right, math.inf).tolist() == [True, True]
    assert gm.dwithin(left, right, [math.inf, math.inf]).tolist() == [True, True]
    assert gm.dwithin(left, empty, math.inf).tolist() == [False, False]


def test_positive_infinite_dwithin_preserves_many_to_many_missing_and_empty_rows() -> (
    None
):
    left = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        None,
        gm.from_wkt('POINT EMPTY', crs=4326),
    ])
    right = gm.GeometryArray([
        gm.Point(180.0, 0.0, crs=4326),
        gm.Point(1.0, 0.0, crs=4326),
        gm.Point(2.0, 0.0, crs=4326),
    ])

    assert gm.dwithin(left, right, math.inf).tolist() == [True, False, False]
    assert gm.dwithin(left, right, [math.inf, math.inf, math.inf]).tolist() == [
        True,
        False,
        False,
    ]


@pytest.mark.parametrize('bad', [math.nan, -math.inf, -1.0])
def test_dwithin_rejects_nan_negative_infinity_and_negative_thresholds(
    bad: float,
) -> None:
    left = gm.Point(0.0, 0.0, crs=4326)
    right = gm.Point(1.0, 0.0, crs=4326)
    rows = gm.points([0.0, 1.0], [0.0, 0.0], crs=4326)

    with pytest.raises(gm.GeometryError, match='non-negative finite'):
        gm.dwithin(left, right, bad)
    with pytest.raises(gm.GeometryError, match='non-negative finite'):
        gm.dwithin(rows, right, [0.0, bad])


@pytest.mark.parametrize('radius', [1.0, 1e8, 1e9, 1e12])
def test_geodesic_hausdorff_gap_scales_with_radius(radius: float) -> None:
    crs = f'+proj=longlat +a={radius!r} +b={radius!r} +type=crs'
    source = gm.LineString([(-1.0, 0.0), (1.0, 0.0)], crs=crs)
    target = gm.Point(0.0, 90.0, crs=crs)

    assert gm.hausdorff_distance(source, target) == pytest.approx(
        math.pi * radius / 2.0,
        rel=2e-15,
    )


def test_geographic_hausdorff_uses_chart_edges_across_scalar_and_gathered_rows() -> (
    None
):
    source = gm.LineString([(0.0, 85.0), (179.0, 85.0)], crs=4326)
    target = gm.Point(10.0, 89.9, crs=4326)
    # The independent PyProj geodesic inverse samples the stored parallel edge;
    # its farthest point is the 179-degree endpoint for this fixed fixture.
    expected = max(
        GEOD.inv(10.0, 89.9, 179.0 * index / 4_000.0, 85.0)[2] for index in range(4_001)
    )
    assert expected == pytest.approx(569_423.751_409_724, abs=1e-6)

    scalar = gm.hausdorff_distance(source, target)
    assert abs(scalar - expected) <= 0.010000_001

    sources = gm.GeometryArray([source, None, source])
    targets = gm.GeometryArray([target, target, target])
    values = gm.hausdorff_distance(sources, targets)
    assert abs(values[0] - expected) <= 0.010000_001
    assert math.isnan(values[1])
    assert abs(values[2] - expected) <= 0.010000_001

    gathered = gm.hausdorff_distance(sources[[2, 1, 0]], targets[[2, 1, 0]])
    assert abs(gathered[0] - expected) <= 0.010000_001
    assert math.isnan(gathered[1])
    assert abs(gathered[2] - expected) <= 0.010000_001


def test_geographic_hausdorff_rejects_max_error() -> None:
    source = gm.LineString([(0.0, 85.0), (179.0, 85.0)], crs=4326)
    target = gm.Point(10.0, 89.9, crs=4326)

    with pytest.raises(
        gm.GeometryError,
        match='max_error is only supported for planar Hausdorff distance',
    ):
        gm.hausdorff_distance(source, target, max_error=0.01)


@pytest.mark.parametrize(
    'flattening',
    [
        math.nextafter(0.02, math.inf),
        0.1,
        0.9,
    ],
    ids=['oblate-adjacent', 'wide-oblate', 'wide-oblate-extreme'],
)
def test_geodesic_series_domain_rejection_is_shared_across_entry_points(
    flattening: float,
) -> None:
    crs = _custom_geographic_crs(flattening)
    expected = r'order-six binary64 series domain \|f\| <= 0\.02'
    start = gm.Point(0.0, 0.0, crs=crs)
    end = gm.Point(1.0, 0.0, crs=crs)
    line = gm.LineString([(0.0, 0.0), (1.0, 0.0)], crs=crs)
    crossing = gm.LineString([(0.5, -1.0), (0.5, 1.0)], crs=crs)
    far_line = gm.LineString([(0.0, 2.0), (1.0, 2.0)], crs=crs)
    operations = [
        ('bearing', lambda: gm.bearing(start, end)),
        ('destination', lambda: start.destination(90.0, 1000.0)),
        (
            'point between',
            lambda: gm.point_between(start, end, 0.5, normalized=True),
        ),
        ('line length', lambda: line.length),
        ('distance', lambda: gm.distance(start, end)),
        ('dwithin', lambda: gm.dwithin(line, far_line, 1.0)),
        # The public overlay `intersection` is topology-only. A crossing
        # distance forces the geodesic segment-intersection owner.
        ('segment intersection', lambda: gm.distance(line, crossing)),
        ('nearest', lambda: gm.nearest_points(line, far_line)),
        ('hausdorff', lambda: gm.hausdorff_distance(line, far_line)),
    ]

    messages = []
    for name, operation in operations:
        with pytest.raises(gm.CRSError, match=expected) as error:
            operation()
        messages.append((name, str(error.value)))

    message = messages[0][1]
    assert all(operation_message == message for _, operation_message in messages)


def test_geodesic_distance_and_chart_area_use_wgs84_ellipsoid() -> None:
    start = gm.Point(0, 0, crs=4326)
    end = gm.Point(1, 0, crs=4326)
    north = gm.Point(0, 1, crs=4326)
    square = gm.box(0, 0, 1, 1, crs=4326)
    meridian = gm.LineString([(0, 0), (0, 1), (0, 2)], crs=4326)
    multilines = gm.MultiLineString([[(0, 0), (1, 0)], [(1, 0), (1, 1)]], crs=4326)
    collection = gm.GeometryCollection([square, meridian], crs=4326)
    assert gm.distance(start, end) == pytest.approx(111319.49079327357)
    assert gm.distance(start, north) == pytest.approx(110574.38855779878)
    assert square.area == pytest.approx(
        chart_zone_area(
            0.0, 1.0, 0.0, 1.0, semi_major=6_378_137.0, flattening=1.0 / 298.257223563
        ),
        # The public closed-form helper and native series each round in binary64.
        rel=3e-14,
    )
    assert gm.Polygon(
        [(-179, -80), (179, -80), (179, 80), (-179, 80), (-179, -80)], crs=4326
    ).area == pytest.approx(
        chart_zone_area(
            179.0,
            181.0,
            -80.0,
            80.0,
            semi_major=6_378_137.0,
            flattening=1.0 / 298.257223563,
        ),
        rel=1e-14,
    )
    assert meridian.length == pytest.approx(221149.45337213494)
    assert multilines.length == pytest.approx(221893.8793510729)
    assert square.length == pytest.approx(
        chart_rectangle_perimeter(GEOD, 0.0, 1.0, 0.0, 1.0), rel=1e-14
    )
    assert collection.length == pytest.approx(square.length + meridian.length, rel=0.01)
    assert gm.distance(gm.points([0, 0], [0, 1], crs=4326), end) == pytest.approx([
        111319.49079327357,
        156899.56829134029,
    ])
    assert gm.distance(start, gm.points([1, 0], [0, 1], crs=4326)) == pytest.approx([
        111319.49079327357,
        110574.38855779878,
    ])
    assert gm.distance(
        gm.points([0, 0], [0, 0], crs=4326), gm.points([1, 0], [0, 1], crs=4326)
    ) == pytest.approx([111319.49079327357, 110574.38855779878])
    assert gm.bearing(start, end) == pytest.approx(90.0)
    destination = start.destination(90.0, gm.distance(start, end))
    assert destination.x == pytest.approx(end.x)
    assert destination.y == pytest.approx(end.y)
    batch_destinations = start.destination(
        [90.0, 0.0], [gm.distance(start, end), gm.distance(start, north)]
    )
    assert batch_destinations[0].x == pytest.approx(end.x)
    assert batch_destinations[1].y == pytest.approx(north.y)
    point_destination = start.destination(90.0, 1000.0)
    assert point_destination.crs == 'EPSG:4326'
    midpoint = gm.point_between(start, end, 0.5, normalized=True)
    assert midpoint.x == pytest.approx(0.5)
    assert midpoint.y == pytest.approx(0.0)
    batch_bearings = gm.bearing(
        gm.points([0.0, 0.0], [0.0, 1.0], crs=4326),
        gm.points([1.0, 1.0], [0.0, 0.0], crs=4326),
    )
    assert batch_bearings[0] == pytest.approx(90.0)
    batch_midpoints = gm.point_between(
        gm.points([0.0, 0.0], [0.0, 0.0], crs=4326),
        gm.points([1.0, 0.0], [0.0, 1.0], crs=4326),
        0.5,
        normalized=True,
    )
    assert [point.x for point in batch_midpoints] == pytest.approx([0.5, 0.0])
    assert [point.y for point in batch_midpoints] == pytest.approx([0.0, 0.5])
    with pytest.raises(ValueError, match='same length'):
        gm.distance(gm.points([0, 0], [0, 0], crs=4326), gm.points([1], [0], crs=4326))
    measured = gm.point_between(
        gm.Point(0.0, 0.0, z=10.0, m=100.0, crs=4326),
        gm.Point(1.0, 0.0, z=20.0, m=200.0, crs=4326),
        0.25,
        normalized=True,
    )
    assert measured.z == pytest.approx(12.5)
    assert measured.m == pytest.approx(125.0)
    assert gm.GeometryArray([square]).area == pytest.approx([square.area])
    assert square.length == pytest.approx(
        chart_rectangle_perimeter(GEOD, 0.0, 1.0, 0.0, 1.0), rel=1e-14
    )
    projected_start = start.to_crs(32631)
    projected_end = end.to_crs(32631)
    assert gm.distance(
        projected_start.to_crs(4326), projected_end.to_crs(4326)
    ) == pytest.approx(gm.distance(start, end), rel=1e-06)
    assert gm.bearing(
        projected_start.to_crs(4326), projected_end.to_crs(4326)
    ) == pytest.approx(gm.bearing(start, end), abs=1e-06)
    assert square.to_crs(32631).to_crs(4326).area == pytest.approx(
        square.area, rel=1e-06
    )


def test_geodesic_buffer_uses_local_projection_and_preserves_crs() -> None:
    point = gm.Point(21.0, 52.0, crs=4326)
    assert point.estimate_local_crs() == 'EPSG:32634'
    buffered = point.buffer(100)
    array = cast('gm.GeometryArray', gm.GeometryArray([point]).buffer(100))
    polar = gm.Point(0.0, 89.0, crs=4326)
    polar_buffered = polar.buffer(100)
    assert buffered.geometry_type == 'Polygon'
    assert buffered.crs == 'EPSG:4326'
    assert buffered.area == pytest.approx(math.pi * 100 * 100, rel=0.01)
    assert array[0].area == pytest.approx(buffered.area)
    assert polar.estimate_local_crs().is_projected
    with pytest.raises(gm.CRSError, match=r'0\.1%'):
        gm.Point(3, 0, crs=4326).buffer(1_000_000)
    with pytest.raises(gm.CRSError, match=r'0\.1%'):
        gm.LineString([(3, 0), (3, 1)], crs=4326).offset_curve(1_000_000)
    assert polar_buffered.crs == 'EPSG:4326'
    assert polar_buffered.area == pytest.approx(math.pi * 100 * 100, rel=0.02)
    eroded_point = point.buffer(-1)
    assert eroded_point.is_empty
    assert eroded_point.area == pytest.approx(0.0)
    with pytest.raises(ValueError, match='distance must be finite'):
        point.buffer(math.inf)


def test_geographic_local_certificate_supports_offset_and_minimum_clearance() -> None:
    line = gm.LineString([(21.0, 52.0), (21.01, 52.0)], crs=4326)
    assert line.estimate_local_crs() == 'EPSG:32634'

    offset = line.offset_curve(50.0, unit='meters')
    assert offset.geometry_type == 'LineString'
    assert offset.crs == 'EPSG:4326'

    polygon = gm.box(21.0, 52.0, 21.001, 52.001, crs=4326)
    clearance = polygon.minimum_clearance(unit='meters')
    witness = polygon.minimum_clearance_line(unit='meters')
    assert math.isfinite(clearance)
    assert clearance > 0.0
    assert witness.geometry_type == 'LineString'
    assert witness.crs == 'EPSG:4326'
    assert witness.length == pytest.approx(clearance, rel=1.0e-6)


@pytest.mark.parametrize('longitude', [3.0, 0.1, 5.9, 179.99, 180.0, -179.99])
def test_geodesic_buffer_does_not_depend_on_the_utm_zone_position(
    longitude: float,
) -> None:
    # A point beside a zone edge fits that zone while its buffer reaches past
    # the zone's scale-error band; the frame follows the result instead. The
    # 0.1% linear scale-error promise bounds the area to 0.2%.
    buffered = gm.Point(longitude, 0, crs=4326).buffer(200_000)
    central = gm.Point(3.0, 0, crs=4326).buffer(200_000)
    assert buffered.geometry_type == 'Polygon'
    assert buffered.area == pytest.approx(central.area, rel=2e-3)


def test_geographic_buffer_refines_a_long_chart_parallel_before_construction() -> None:
    source = _six_hundred_kilometer_parallel()
    local = source.estimate_local_crs()
    assert local.to_epsg() == 32631
    fine_input = source.segmentize(250.0)

    line = gm.LineString(
        [
            (LONG_PARALLEL_WEST, LONG_PARALLEL_SOUTH),
            (LONG_PARALLEL_EAST, LONG_PARALLEL_SOUTH),
        ],
        crs=4326,
    )
    fine_line = line.segmentize(250.0)

    for distance, budget in ((1_000.0, 0.1), (10_000.0, 1.0)):
        # The selected TM certificate bounds projected curvature K between
        # 1e-7 and 2e-7 /m on this parallel and image scale by 1.02 (native
        # certificate coverage checks these envelopes). Its K*l²/8 refinement
        # gives l <= sqrt(8*budget/K_lower). Tangent rotation is at most K*l;
        # comparing two endpoint normals adds the fine reference's piece.
        curvature_upper = 2.0e-7
        maximum_piece_length = math.sqrt(8.0 * budget / 1.0e-7)
        fine_piece_length = 1.02 * 250.0
        fine_budget = curvature_upper * fine_piece_length**2 / 8.0
        angle_bound = curvature_upper * (maximum_piece_length + fine_piece_length)
        endpoint_bound = 2.0 * distance * math.sin(angle_bound / 2.0)
        arc_error = distance * (1.0 - math.cos(math.pi / (4.0 * 64)))
        # Distance functions and round Minkowski dilation are 1-Lipschitz in
        # input Hausdorff distance. The inscribed round-arc polygon contributes
        # d*(1-cos(pi/(4*q))); endpoint tangent displacement is separate.
        interior_bound = budget + fine_budget + arc_error
        result = source.buffer(distance, quadrant_segments=64)
        reference = fine_input.buffer(distance, quadrant_segments=64)
        actual_projected = shapely.from_wkt(result.to_crs(local).to_wkt())
        reference_projected = shapely.from_wkt(reference.to_crs(local).to_wkt())
        hausdorff = shapely.hausdorff_distance(actual_projected, reference_projected)
        assert hausdorff <= interior_bound

        actual_offset = line.offset_curve(distance, quadrant_segments=64)
        reference_offset = fine_line.offset_curve(distance, quadrant_segments=64)
        actual_offset_local = shapely.from_wkt(actual_offset.to_crs(local).to_wkt())
        reference_offset_local = shapely.from_wkt(
            reference_offset.to_crs(local).to_wkt()
        )
        for actual_end, reference_end in (
            (actual_offset_local.coords[0], reference_offset_local.coords[0]),
            (actual_offset_local.coords[-1], reference_offset_local.coords[-1]),
        ):
            assert math.dist(actual_end, reference_end) <= endpoint_bound
        fine_line_local = shapely.from_wkt(fine_line.to_crs(local).to_wkt())
        interior_samples = 0
        for start, end in pairwise(actual_offset_local.coords):
            for fraction in (0.0, 0.25, 0.5, 0.75):
                point = shapely.Point(
                    start[0] + fraction * (end[0] - start[0]),
                    start[1] + fraction * (end[1] - start[1]),
                )
                along = reference_offset_local.project(point)
                if (
                    maximum_piece_length
                    < along
                    < (reference_offset_local.length - maximum_piece_length)
                ):
                    interior_samples += 1
                    assert (
                        abs(fine_line_local.distance(point) - distance)
                        <= interior_bound
                    )
        assert interior_samples > 0

        if distance == 1_000.0:
            batch = gm.GeometryArray([source, None, source], crs=4326).buffer(
                distance,
                quadrant_segments=64,
            )
            assert batch[0].to_wkb() == result.to_wkb()
            assert batch[1] is None
            assert batch[2].to_wkb() == result.to_wkb()

        if distance == 1_000.0:

            def maximum_relative_distance_error(
                geometry: gm.Geometry,
                operation_distance: float,
            ) -> float:
                samples = list(geometry.exterior.coords)
                errors = []
                for start, end in pairwise(samples):
                    for fraction in (0.0, 0.25, 0.5, 0.75):
                        longitude = start[0] + fraction * (end[0] - start[0])
                        latitude = start[1] + fraction * (end[1] - start[1])
                        errors.append(
                            abs(
                                _distance_to_long_parallel_boundary(longitude, latitude)
                                / operation_distance
                                - 1.0
                            )
                        )
                return max(errors)

            coarse_projected = source.to_crs(local).buffer(
                distance,
                quadrant_segments=64,
            )
            coarse_result = coarse_projected.to_crs(4326)
            assert maximum_relative_distance_error(result, distance) <= 0.001
            assert maximum_relative_distance_error(coarse_result, distance) > 0.001


def test_geographic_interior_searches_use_refined_boundary_support() -> None:
    source = _six_hundred_kilometer_parallel()
    fine_input = source.segmentize(250.0)
    local = source.estimate_local_crs()
    assert local.to_epsg() == 32631
    projected_bounds = source.to_crs(local).bounds
    assert projected_bounds is not None
    extent = math.hypot(
        projected_bounds[2] - projected_bounds[0],
        projected_bounds[3] - projected_bounds[1],
    )
    input_budget = 1.0e-4 * extent

    actual_label = source.polylabel(tolerance=1.0)
    actual_label_local = actual_label.to_crs(local)
    fine_polygon_local = shapely.from_wkt(fine_input.to_crs(local).to_wkt())
    label_local = shapely.Point(actual_label_local.x, actual_label_local.y)
    assert fine_polygon_local.covers(label_local)
    actual_clearance = fine_polygon_local.boundary.distance(label_local)
    # Each input chord approximation perturbs both candidate clearance and
    # optimum radius; the candidate search can lose another tolerance.
    fine_input_budget = 2.0e-7 * (1.02 * 250.0) ** 2 / 8.0
    clearance_bound = 2.0 * (input_budget + fine_input_budget) + 1.0
    independent_radius_line = shapely.maximum_inscribed_circle(
        fine_polygon_local,
        tolerance=1.0,
    )
    reference_clearance = independent_radius_line.length
    assert abs(actual_clearance - reference_clearance) <= clearance_bound

    actual_radius = source.maximum_inscribed_radius(tolerance=1.0)
    reference_radius = fine_input.maximum_inscribed_radius(tolerance=1.0)
    assert abs(actual_radius - reference_clearance) <= clearance_bound
    assert abs(actual_radius - reference_radius) <= clearance_bound

    actual_circle = source.maximum_inscribed_circle(tolerance=1.0)
    actual_circle_local = _projected_chart_outline(actual_circle, local)
    assert shapely.covers(
        shapely.buffer(fine_polygon_local, input_budget),
        actual_circle_local,
    )

    actual_enclosing_radius = source.minimum_bounding_radius()
    reference_enclosing_radius = fine_input.to_crs(local).minimum_bounding_radius(
        unit='planar'
    )
    assert (
        abs(actual_enclosing_radius - reference_enclosing_radius) <= 4.0 * input_budget
    )
    enclosing_circle_local = source.minimum_bounding_circle().to_crs(local)
    circle_vertices = list(enclosing_circle_local.exterior.coords)[:-1]
    center_x = sum(point[0] for point in circle_vertices) / len(circle_vertices)
    center_y = sum(point[1] for point in circle_vertices) / len(circle_vertices)
    realized_radius = max(
        math.hypot(point[0] - center_x, point[1] - center_y)
        for point in circle_vertices
    ) * math.cos(math.pi / 64.0)
    assert abs(realized_radius - actual_enclosing_radius) <= 4.0 * input_budget


def test_large_polar_buffer_uses_concise_local_crs_diagnostic() -> None:
    with pytest.raises(gm.TransformError) as excinfo:
        gm.Point(0.0, 89.9999, crs=4326).buffer(50_000_000)

    error = excinfo.value
    assert error.target == 'EPSG:4326'
    assert error.source is not None
    definition = json.loads(error.source)
    assert definition['name'] == 'Local conformal CRS'
    assert definition['type'] == 'ProjectedCRS'
    message = str(error)
    assert 'Local conformal CRS' in message
    assert 'projjson.schema.json' not in message
    assert 'Point outside of projection domain' in message


LATITUDE_DOMAIN_MSG = r'invalid longitude/latitude \(0, 95\)'


def test_geodesic_distance_scalar_batch_latitude_domain_parity() -> None:
    outside = gm.Point(0.0, 95.0, crs=4326)
    end = gm.Point(1.0, 0.0, crs=4326)
    with pytest.raises(gm.InvalidGeometryError, match=LATITUDE_DOMAIN_MSG):
        gm.distance(outside, end)
    with pytest.raises(gm.InvalidGeometryError, match=LATITUDE_DOMAIN_MSG):
        gm.distance(gm.GeometryArray([outside]), end)


def test_line_interpolate_count_admits_source_before_ellipsoid_resolution() -> None:
    unsupported = '+proj=longlat +a=6378137 +f=0.1 +type=crs'

    invalid_source = gm.LineString([(0.0, 0.0), (0.0, 95.0)], crs=unsupported)
    two_faults = gm.GeometryArray([invalid_source], crs=unsupported)
    with pytest.raises(gm.InvalidGeometryError, match=LATITUDE_DOMAIN_MSG) as excinfo:
        two_faults.line_interpolate(count=1)
    assert excinfo.value.__notes__ == ['while processing array element 0']

    valid_source = gm.LineString([(0.0, 0.0), (0.0, 1.0)], crs=unsupported)
    with pytest.raises(
        gm.CRSError, match='order-six binary64 series domain'
    ) as excinfo:
        gm.GeometryArray([valid_source], crs=unsupported).line_interpolate(count=1)
    assert excinfo.value.__notes__ == ['while processing array element 0']

    wgs84_invalid_source = gm.LineString([(0.0, 0.0), (0.0, 95.0)], crs=4326)
    with pytest.raises(gm.InvalidGeometryError, match=LATITUDE_DOMAIN_MSG) as excinfo:
        gm.GeometryArray([wgs84_invalid_source], crs=4326).line_interpolate(count=1)
    assert excinfo.value.__notes__ == ['while processing array element 0']

    nullable = gm.GeometryArray([None, invalid_source], crs=unsupported)
    with pytest.raises(gm.InvalidGeometryError, match=LATITUDE_DOMAIN_MSG) as excinfo:
        nullable.line_interpolate(count=1)
    assert excinfo.value.__notes__ == ['while processing array element 1']


def test_packed_geodesic_measures_validate_present_line_and_polygon_rows() -> None:
    valid_line = gm.LineString([(0, 0), (1, 0)], crs=4326)
    invalid_line = gm.LineString([(0, 0), (0, 95)], crs=4326)
    lines = gm.GeometryArray([valid_line, None, invalid_line], crs=4326)
    with pytest.raises(gm.InvalidGeometryError, match=LATITUDE_DOMAIN_MSG):
        _ = lines.length

    valid_polygon = gm.from_wkt(
        'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))',
        crs=4326,
    )
    invalid_polygon = gm.from_wkt(
        'POLYGON ((0 0, 2 0, 0 95, 0 0))',
        crs=4326,
    )
    polygons = gm.GeometryArray([valid_polygon, None, invalid_polygon], crs=4326)
    with pytest.raises(gm.InvalidGeometryError, match=LATITUDE_DOMAIN_MSG):
        _ = polygons.length


def test_transform_broadcasts_source_epoch_to_every_vertex() -> None:
    # A time-dependent (plate-motion) transform: ITRF2014 -> GDA2020, where the
    # coordinate epoch materially changes each vertex. The batch transform packs
    # every vertex into one PROJ call with a length-1 broadcast epoch lane, so it
    # must reproduce the per-vertex scalar transform exactly and stay sensitive
    # to the epoch value.
    verts = [(133.0 + i * 0.5, -25.0 - i * 0.3) for i in range(8)]
    line = gm.LineString(verts, crs=9000, epoch=2010.0).to_crs(7844)
    xs = line.coords.x
    ys = line.coords.y
    for i, (lon, lat) in enumerate(verts):
        scalar = gm.Point(lon, lat, crs=9000, epoch=2010.0).to_crs(7844)
        assert scalar.x == pytest.approx(float(xs[i]), abs=1e-12)
        assert scalar.y == pytest.approx(float(ys[i]), abs=1e-12)
    # A different epoch shifts the result — proof the epoch is truly applied, not
    # silently dropped to the first vertex or zero.
    other = gm.LineString(verts, crs=9000, epoch=2000.0).to_crs(7844)
    assert float(other.coords.x[0]) != pytest.approx(float(xs[0]), abs=1e-9)
