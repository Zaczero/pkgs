"""CRS behavior — geodesic measurement, transforms, best-UTM selection,
runtime config, cache info, and the PROJ authority metadata surface.
"""

import json
import math
from typing import cast

import gometry as gm
import pytest


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
        0.0 if flattening == 0.0 else 50.0
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


def test_geodesic_parallel_proximity_is_not_segment_contact() -> None:
    left = gm.LineString([(-1.0, 0.0), (1.0, 0.0)], crs=4326)
    right = gm.LineString([(-1.0, 1e-14), (1.0, 1e-14)], crs=4326)

    distance = gm.distance(left, right)

    assert distance > 0.0
    assert not gm.dwithin(left, right, 0.0)
    first, second = gm.nearest_points(left, right)
    assert first.to_wkt() != second.to_wkt()


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


def test_geodesic_distance_and_area_use_wgs84_ellipsoid() -> None:
    start = gm.Point(0, 0, crs=4326)
    end = gm.Point(1, 0, crs=4326)
    north = gm.Point(0, 1, crs=4326)
    square = gm.box(0, 0, 1, 1, crs=4326)
    meridian = gm.LineString([(0, 0), (0, 1), (0, 2)], crs=4326)
    multilines = gm.MultiLineString([[(0, 0), (1, 0)], [(1, 0), (1, 1)]], crs=4326)
    collection = gm.GeometryCollection([square, meridian], crs=4326)
    assert gm.distance(start, end) == pytest.approx(111319.49079327357)
    assert gm.distance(start, north) == pytest.approx(110574.38855779878)
    assert square.area == pytest.approx(12308778361.469452)
    assert gm.Polygon(
        [(-179, -80), (179, -80), (179, 80), (-179, 80), (-179, -80)], crs=4326
    ).area == pytest.approx(2790277931191.946)
    assert meridian.length == pytest.approx(221149.45337213494)
    assert multilines.length == pytest.approx(221893.8793510729)
    assert square.length == pytest.approx(443770.917248302)
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
    assert gm.GeometryArray([square]).area == pytest.approx([12308778361.469452])
    assert square.length == pytest.approx(443770.917248302)
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
