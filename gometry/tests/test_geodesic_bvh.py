"""Geodesic distance BVH path (≥64 segments) — internal + pyproj cross-check."""

from __future__ import annotations

import itertools
import math

import gometry as gm
import pytest
from pyproj import Geod

GEOD = Geod(ellps='WGS84')
MIN_BVH_SEGMENTS = 64
# Q8's documented chart-edge search gap, 4e-11 times the WGS 84 semi-major axis.
WITNESS_GAP_METERS = 4e-11 * 6_378_137.0


def _assert_point_witness(
    actual: gm.Point, expected: tuple[float, float]
) -> None:
    error = GEOD.inv(expected[0], expected[1], actual.x, actual.y)[2]
    assert error <= WITNESS_GAP_METERS, (
        f'witness error {error} m exceeds the Q8 gap {WITNESS_GAP_METERS} m'
    )


def _geodesic_dense_line(
    start: tuple[float, float], end: tuple[float, float], count: int
) -> gm.LineString:
    base = gm.LineString([start, end], crs=4326)
    coords = [
        next(
            iter((base).line_interpolate(index / (count - 1), normalized=True).coords)
        )[:2]
        for index in range(count)
    ]
    return gm.LineString(coords, crs=4326)


def _wavy_dense_line(
    start_lon: float,
    start_lat: float,
    count: int,
    *,
    width: float = 4.0,
    amplitude: float = 0.25,
) -> gm.LineString:
    coords = []
    for index in range(count):
        t = index / (count - 1)
        lon = start_lon + t * width
        lat = start_lat + math.sin(t * math.tau) * amplitude
        coords.append((lon, lat))
    return gm.LineString(coords, crs=4326)


def _dense_ring_line(
    cx: float, cy: float, radius: float, vertices: int
) -> gm.LineString:
    ring = []
    for index in range(vertices):
        angle = index / vertices * math.tau
        ring.append((cx + radius * math.cos(angle), cy + radius * math.sin(angle)))
    ring.append(ring[0])
    return gm.LineString(ring, crs=4326)


def _subsample_line(line: gm.LineString, stride: int) -> gm.LineString:
    coords = list(line.coords)
    sampled = coords[::stride]
    if sampled[-1] != coords[-1]:
        sampled.append(coords[-1])
    return gm.LineString(sampled, crs=line.crs)


def test_geodesic_distance_dense_line_matches_coarse_same_shape() -> None:
    """≥64-segment geodesic line matches a subsampled twin on the same path."""
    dense = _geodesic_dense_line((0.0, 0.0), (10.0, 0.0), 96)
    coarse = _subsample_line(dense, stride=12)
    probe = gm.Point(5.0, 0.5, crs=4326)
    assert len(list(dense.coords)) >= MIN_BVH_SEGMENTS
    assert gm.distance(probe, dense) == pytest.approx(
        gm.distance(probe, coarse), rel=1e-08
    )


def test_geodesic_distance_two_dense_lines_is_commutative() -> None:
    left = _wavy_dense_line(-2.0, 1.0, 65, width=1.0, amplitude=0.1)
    right = _wavy_dense_line(-2.0, 0.0, 65, width=1.0, amplitude=0.1)
    assert len(list(left.coords)) - 1 >= MIN_BVH_SEGMENTS
    forward = gm.distance(left, right)
    reverse = gm.distance(right, left)
    assert forward == reverse
    assert math.isfinite(forward)


def test_geodesic_distance_dense_ring_line_matches_coarser_circle() -> None:
    probe = gm.Point(5.0, 0.0, crs=4326)
    dense = _dense_ring_line(0.0, 0.0, 2.0, 96)
    coarse = _dense_ring_line(0.0, 0.0, 2.0, 24)
    assert gm.distance(probe, dense) == pytest.approx(
        gm.distance(probe, coarse), rel=0.01
    )


def test_geodesic_distance_dense_line_matches_pyproj_segment_oracle() -> None:
    """Point-to-wavy-line distance matches a pyproj Geod segment minimum."""
    line = _wavy_dense_line(-2.0, 0.0, 72)
    probe = gm.Point(0.0, 0.5, crs=4326)
    gometry_dist = gm.distance(probe, line)
    coords = list(line.coords)
    best = math.inf
    for start, end in itertools.pairwise(coords):
        for steps in range(17):
            frac = steps / 16
            lon = start[0] + frac * (end[0] - start[0])
            lat = start[1] + frac * (end[1] - start[1])
            _, _, d = GEOD.inv(probe.x, probe.y, lon, lat)
            best = min(best, d)
    # No sampled chart-edge point may beat the returned minimum.
    assert gometry_dist <= best + 1e-9
    assert gometry_dist == pytest.approx(best, rel=1e-05)
    left, right = gm.nearest_points(probe, line)
    assert left.to_wkt() == probe.to_wkt()
    assert abs(gm.distance(left, right) - gometry_dist) <= 1e-9
    assert list(gm.shortest_line(probe, line).coords) == [
        next(iter(left.coords)),
        next(iter(right.coords)),
    ]


@pytest.mark.parametrize(
    ('start', 'end', 'probe'),
    [
        ((179.0, 0.0), (-179.0, 0.0), (180.0, 1.0)),
        ((179.0, -1.0), (-179.0, -1.0), (0.0, 0.0)),
    ],
    ids=['antimeridian-seam', 'near-antipodal-probe'],
)
def test_dense_seam_and_near_antipodal_lines_match_coarse_twins(
    start: tuple[float, float],
    end: tuple[float, float],
    probe: tuple[float, float],
) -> None:
    dense = _geodesic_dense_line(start, end, 65)
    coarse = _subsample_line(dense, stride=8)
    point = gm.Point(*probe, crs=4326)
    assert len(list(dense.coords)) - 1 >= MIN_BVH_SEGMENTS
    expected = gm.distance(point, coarse)
    actual = gm.distance(point, dense)
    assert actual == pytest.approx(expected, rel=1e-8)
    assert gm.dwithin(point, dense, math.nextafter(max(expected, actual), math.inf))


def test_geodesic_dwithin_dense_ring_line_near_and_far() -> None:
    target = _dense_ring_line(0.0, 0.0, 0.5, 72)
    near = gm.Point(0.0, 0.0, crs=4326)
    far = gm.Point(10.0, 10.0, crs=4326)
    assert gm.dwithin(near, target, 100000.0)
    assert not gm.dwithin(far, target, 100000.0)


def test_geodesic_polygon_distance_and_dwithin_measure_filled_support() -> None:
    target = gm.box(-1.0, -1.0, 1.0, 1.0, crs=4326)
    inside = gm.Point(0.0, 0.0, crs=4326)
    outside = gm.Point(3.0, 0.0, crs=4326)
    expected = 222_638.98158654713
    assert gm.intersects(inside, target)
    assert gm.distance(inside, target) == 0.0
    assert gm.dwithin(inside, target, 0.0)
    assert not gm.intersects(outside, target)
    assert not gm.dwithin(outside, target, 0.0)
    assert gm.distance(outside, target) == pytest.approx(expected, abs=1e-3)
    assert not gm.dwithin(outside, target, expected - 0.01)
    assert gm.dwithin(outside, target, expected + 0.01)


def test_geodesic_polygon_nearest_points_and_shortest_line_return_chart_witnesses() -> None:
    target = gm.box(-1.0, -1.0, 1.0, 1.0, crs=4326)
    inside = gm.Point(0.0, 0.0, crs=4326)
    outside = gm.Point(3.0, 0.0, crs=4326)

    left, right = gm.nearest_points(outside, target)
    assert left.to_wkt() == outside.to_wkt()
    assert right.x == 1.0
    _assert_point_witness(right, (1.0, 0.0))
    shortest = gm.shortest_line(outside, target)
    assert list(shortest.coords) == [
        next(iter(left.coords)),
        next(iter(right.coords)),
    ]
    inside_left, inside_right = gm.nearest_points(inside, target)
    assert inside_left.to_wkt() == inside.to_wkt()
    assert inside_right.to_wkt() == inside.to_wkt()


def test_shortest_line_length_tracks_geodesic_distance_for_short_edges() -> None:
    first = gm.LineString([(10.0, 20.0), (10.004, 20.003)], crs=4326)
    second = gm.LineString(
        [(10.0008, 19.9992), (10.0048, 20.0022)], crs=4326
    )
    assert first.length < 1000.0
    assert second.length < 1000.0

    left, right = gm.nearest_points(first, second)
    exact = GEOD.inv(left.x, left.y, right.x, right.y)[2]
    distance = gm.distance(first, second)
    shortest = gm.shortest_line(first, second)

    assert distance == pytest.approx(exact, abs=1e-9)
    assert shortest.length >= distance - 1e-9
    assert shortest.length == pytest.approx(exact, abs=1e-9)
    assert list(shortest.coords) == [
        next(iter(left.coords)),
        next(iter(right.coords)),
    ]


def test_geodesic_point_distance_to_polygon_hole_uses_hole_boundary() -> None:
    polygon_with_hole = gm.Polygon(
        [(-2, -2), (2, -2), (2, 2), (-2, 2), (-2, -2)],
        holes=[[(-1, -1), (1, -1), (1, 1), (-1, 1), (-1, -1)]],
        crs=4326,
    )
    hole_probe = gm.Point(0.0, 0.0, crs=4326)
    hole_distance = gm.distance(hole_probe, polygon_with_hole)
    assert hole_distance == pytest.approx(110574.38855779878, abs=1e-3)
    assert not gm.dwithin(hole_probe, polygon_with_hole, 0.0)
    assert gm.dwithin(hole_probe, polygon_with_hole, hole_distance + 0.01)


def test_packed_geodesic_point_columns_measure_polygons() -> None:
    target = gm.box(-1.0, -1.0, 1.0, 1.0, crs=4326)
    expected = 222_638.98158654713
    points = gm.points([0.0, 3.0], [0.0, 0.0], crs=4326)
    assert gm.distance(points, target) == pytest.approx([0.0, expected], abs=1e-3)
    assert gm.intersects(points, target).tolist() == [True, False]
    assert gm.dwithin(points, target, [0.0, 0.0]).tolist() == [True, False]
    assert gm.dwithin(points, target, [0.0, expected - 0.01]).tolist() == [True, False]


def test_geodesic_polygon_arrays_broadcast_against_a_point() -> None:
    inside = gm.Point(0.0, 0.0, crs=4326)
    polygons = gm.GeometryArray([
        gm.box(-1.0, -1.0, 1.0, 1.0, crs=4326),
        gm.box(4.0, -1.0, 6.0, 1.0, crs=4326),
    ])
    expected = GEOD.inv(0.0, 0.0, 4.0, 0.0)[2]
    assert gm.distance(polygons, inside) == pytest.approx([0.0, expected], abs=1e-3)
    assert gm.intersects(polygons, inside).tolist() == [True, False]
    assert gm.dwithin(polygons, inside, [0.0, 0.0]).tolist() == [True, False]
    assert gm.dwithin(polygons, inside, [0.0, expected - 0.01]).tolist() == [True, False]

    nearest_left, nearest_right = gm.nearest_points(polygons, inside)
    assert nearest_left[0].to_wkt() == inside.to_wkt()
    assert nearest_right[0].to_wkt() == inside.to_wkt()
    assert nearest_left[1].x == 4.0
    _assert_point_witness(nearest_left[1], (4.0, 0.0))
    assert nearest_right[1].to_wkt() == inside.to_wkt()
    lines = gm.shortest_line(polygons, inside)
    assert lines[0].to_wkt() == 'LINESTRING (0 0, 0 0)'
    assert list(lines[1].coords) == [
        next(iter(nearest_left[1].coords)),
        next(iter(nearest_right[1].coords)),
    ]


def test_fixed_polygon_nearest_and_shortest_line_broadcast_over_packed_points() -> None:
    target = gm.box(-1.0, -1.0, 1.0, 1.0, crs=4326)
    points = gm.points([0.0, 3.0], [0.0, 0.0], crs=4326)
    inside = gm.Point(0.0, 0.0, crs=4326)
    outside = gm.Point(3.0, 0.0, crs=4326)
    assert gm.intersects(points, target).tolist() == [True, False]
    assert gm.dwithin(points, target, [0.0, 0.0]).tolist() == [True, False]
    nearest_left, nearest_right = gm.nearest_points(points, target)
    assert nearest_left[0].to_wkt() == inside.to_wkt()
    assert nearest_right[0].to_wkt() == inside.to_wkt()
    assert nearest_left[1].to_wkt() == outside.to_wkt()
    assert nearest_right[1].x == 1.0
    _assert_point_witness(nearest_right[1], (1.0, 0.0))
    lines = gm.shortest_line(points, target)
    assert lines[0].to_wkt() == 'LINESTRING (0 0, 0 0)'
    assert list(lines[1].coords) == [
        next(iter(nearest_left[1].coords)),
        next(iter(nearest_right[1].coords)),
    ]


def test_geodesic_hausdorff_remains_linework_only() -> None:
    target = gm.box(-1.0, -1.0, 1.0, 1.0, crs=4326)
    inside = gm.Point(0.0, 0.0, crs=4326)
    with pytest.raises(gm.CRSError, match='polygon interiors is unsupported'):
        gm.hausdorff_distance(inside, target)
    with pytest.raises(gm.CRSError, match='polygon interiors is unsupported'):
        gm.hausdorff_distance(target, target)
    target_array = gm.GeometryArray([target])
    with pytest.raises(gm.CRSError, match='polygon interiors is unsupported'):
        gm.hausdorff_distance(target_array, target_array)
    empty_target = gm.from_wkt('POLYGON EMPTY', crs=4326)
    assert gm.hausdorff_distance(empty_target, empty_target) == math.inf


def test_packed_geodesic_polygon_dwithin_still_validates_latitudes() -> None:
    inside = gm.Point(0.0, 0.0, crs=4326)
    with pytest.raises(gm.GeometryError, match='invalid longitude/latitude'):
        gm.dwithin(gm.points([0.0], [95.0], crs=4326), inside, [math.inf])

def test_planar_polygon_distance_contract_is_unchanged() -> None:
    target = gm.box(-1.0, -1.0, 1.0, 1.0, crs=4326)
    inside = gm.Point(0.0, 0.0, crs=4326)
    assert gm.distance(inside, target, unit='planar') == 0.0
    assert gm.dwithin(inside, target, 0.0, unit='planar')


def test_dwithin_infinity_keeps_missing_and_empty_rows() -> None:
    """An infinite threshold does not drop non-live or empty array rows."""
    rows = gm.GeometryArray([
        gm.Point(0.0, 0.0),
        None,
        gm.from_wkt('POINT EMPTY'),
    ])
    result = gm.dwithin(rows, gm.Point(1.0, 1.0), math.inf)
    assert result.tolist() == [True, False, False]
    assert gm.dwithin(gm.from_wkt('POINT EMPTY'), gm.Point(1.0, 1.0), math.inf) is False
