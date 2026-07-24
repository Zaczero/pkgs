"""Geodesic distance BVH path (≥64 segments) — internal + pyproj cross-check."""

from __future__ import annotations

import itertools
import math

import gometry as gm
import pytest
from pyproj import Geod

GEOD = Geod(ellps='WGS84')
MIN_BVH_SEGMENTS = 64


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


def _wavy_dense_line(start_lon: float, start_lat: float, count: int) -> gm.LineString:
    coords = []
    for index in range(count):
        t = index / (count - 1)
        lon = start_lon + t * 4.0
        lat = start_lat + math.sin(t * math.tau) * 0.25
        coords.append((lon, lat))
    return gm.LineString(coords, crs=4326)


def _dense_polygon(cx: float, cy: float, radius: float, vertices: int) -> gm.Polygon:
    ring = []
    for index in range(vertices):
        angle = index / vertices * math.tau
        ring.append((cx + radius * math.cos(angle), cy + radius * math.sin(angle)))
    ring.append(ring[0])
    return gm.Polygon(ring, crs=4326)


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
        gm.distance(probe, coarse), rel=1e-09
    )


def test_geodesic_distance_two_dense_lines_is_commutative() -> None:
    left = _wavy_dense_line(-2.0, 1.0, 96)
    right = _wavy_dense_line(-2.0, 0.0, 96)
    assert gm.distance(left, right) == gm.distance(right, left)
    assert math.isfinite(gm.distance(left, right))


def test_geodesic_distance_dense_polygon_matches_coarser_circle() -> None:
    probe = gm.Point(5.0, 0.0, crs=4326)
    dense = _dense_polygon(0.0, 0.0, 2.0, 96)
    coarse = _dense_polygon(0.0, 0.0, 2.0, 24)
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
    assert gometry_dist == pytest.approx(best, rel=1e-05)


def test_geodesic_dwithin_dense_polygon_near_and_far() -> None:
    target = _dense_polygon(0.0, 0.0, 0.5, 72)
    near = gm.Point(0.0, 0.0, crs=4326)
    far = gm.Point(10.0, 10.0, crs=4326)
    assert gm.dwithin(near, target, 100000.0)
    assert not gm.dwithin(far, target, 100000.0)
