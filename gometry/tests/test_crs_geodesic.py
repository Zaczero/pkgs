"""CRS behavior — geodesic measurement, transforms, best-UTM selection,
runtime config, cache info, and the PROJ authority metadata surface.
"""

import math
from typing import cast

import gometry as gm
import pytest


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
    wgs84 = gm.CRS(4326)
    batch_destinations = wgs84.geodesic_direct(
        [0.0, 0.0],
        [0.0, 0.0],
        [90.0, 0.0],
        [gm.distance(start, end), gm.distance(start, north)],
    )
    assert batch_destinations['longitude'][0] == pytest.approx(end.x)
    assert batch_destinations['latitude'][1] == pytest.approx(north.y)
    point_destination = start.destination(90.0, 1000.0)
    assert point_destination.crs == 'EPSG:4326'
    midpoint = gm.point_between(start, end, 0.5, normalized=True)
    assert midpoint.x == pytest.approx(0.5)
    assert midpoint.y == pytest.approx(0.0)
    batch_bearings = wgs84.geodesic_inverse(
        [0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [0.0, 0.0]
    )
    assert batch_bearings['forward_azimuth'][0] == pytest.approx(90.0)
    batch_midpoints = wgs84.geodesic_interpolate(
        [0.0, 0.0], [0.0, 0.0], [1.0, 0.0], [0.0, 1.0], 0.5, normalized=True
    )
    assert batch_midpoints['longitude'] == pytest.approx([0.5, 0.0])
    assert batch_midpoints['latitude'] == pytest.approx([0.0, 0.5])
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


LATITUDE_DOMAIN_MSG = (
    'geographic latitude is outside the valid \\[-90, 90\\] degree domain'
)


def test_geodesic_inverse_scalar_batch_latitude_domain_parity() -> None:
    wgs84 = gm.CRS(4326)
    with pytest.raises(gm.CRSError, match=LATITUDE_DOMAIN_MSG):
        wgs84.geodesic_inverse(0.0, 95.0, 1.0, 0.0)
    with pytest.raises(gm.CRSError, match=LATITUDE_DOMAIN_MSG):
        wgs84.geodesic_inverse([0.0], [95.0], [1.0], [0.0])


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
