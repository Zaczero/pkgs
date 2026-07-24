"""Packed geodesic array-to-array point-pair metric parity."""

from __future__ import annotations

import gometry as gm
import pytest
from conftest import floats

WGS84 = 4326


def _scalar_row_oracle_distance(
    left: gm.GeometryArray, right: gm.GeometryArray
) -> list[float]:
    return [gm.distance(a, b) for a, b in zip(list(left), list(right), strict=True)]


def _scalar_row_oracle_dwithin(
    left: gm.GeometryArray, right: gm.GeometryArray, distance: float
) -> list[bool]:
    return [
        gm.dwithin(a, b, distance) for a, b in zip(list(left), list(right), strict=True)
    ]


def test_packed_geodesic_point_pair_distance_matches_scalar_row_oracle() -> None:
    xs = [0.0, 1.0, 10.0, -73.0, 170.0]
    ys = [0.0, 0.5, 45.0, 41.0, -10.0]
    left = gm.points(xs, ys, crs=WGS84)
    right = gm.points([x + 0.1 for x in xs], [y + 0.1 for y in ys], crs=WGS84)
    expected = _scalar_row_oracle_distance(left, right)
    assert floats(gm.distance(left, right)) == expected
    assert floats(gm.distance(left, right)) == expected


def test_packed_geodesic_point_pair_dwithin_matches_scalar_row_oracle() -> None:
    xs = [0.0, 1.0, 10.0, -73.0, 170.0]
    ys = [0.0, 0.5, 45.0, 41.0, -10.0]
    left = gm.points(xs, ys, crs=WGS84)
    right = gm.points([x + 0.1 for x in xs], [y + 0.1 for y in ys], crs=WGS84)
    distance = 50000.0
    expected = _scalar_row_oracle_dwithin(left, right, distance)
    assert list(gm.dwithin(left, right, distance)) == expected
    assert list(gm.dwithin(left, right, distance)) == expected


def test_packed_geodesic_point_pair_out_of_domain_latitude_raises_same_error() -> None:
    good = gm.points([0.0], [0.0], crs=WGS84)
    bad = gm.points([0.0], [95.0], crs=WGS84)
    with pytest.raises(
        gm.InvalidGeometryError, match=r'invalid longitude/latitude \(0, 95\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data'
    ) as packed:
        gm.distance(good, bad)
    with pytest.raises(
        gm.InvalidGeometryError, match=r'invalid longitude/latitude \(0, 95\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data'
    ) as scalar:
        gm.distance(next(iter(good)), next(iter(bad)))
    assert str(packed.value) == str(scalar.value)
    with pytest.raises(
        gm.InvalidGeometryError, match=r'invalid longitude/latitude \(0, 95\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data'
    ) as packed_dw:
        gm.dwithin(good, bad, 1000.0)
    with pytest.raises(
        gm.InvalidGeometryError, match=r'invalid longitude/latitude \(0, 95\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data'
    ) as scalar_dw:
        gm.dwithin(next(iter(good)), next(iter(bad)), 1000.0)
    assert str(packed_dw.value) == str(scalar_dw.value)
