"""Pole-encircling geographic polygons — membership and sensible geodesic area."""

import math

import gometry as gm
from conftest import bools


def _pole_hexagons() -> tuple[gm.Polygon, gm.Polygon]:
    ring = [(0, 80), (60, 80), (120, 80), (180, 80), (-120, 80), (-60, 80)]
    hex_north = gm.Polygon(ring, crs=4326)
    hex_south = gm.Polygon([(x, -abs(y)) for x, y in ring], crs=4326)
    return (hex_north, hex_south)


def test_pole_encircling_hexagon_contains_and_covers() -> None:
    hex_north, hex_south = _pole_hexagons()
    north_pole = gm.Point(0, 90, crs=4326)
    south_pole = gm.Point(0, -90, crs=4326)
    assert gm.contains(hex_north, north_pole)
    assert not gm.contains(hex_north, south_pole)
    assert gm.covers(hex_north, north_pole)
    assert gm.intersects(hex_north, north_pole)
    assert gm.within(north_pole, hex_north)
    assert gm.contains(hex_south, south_pole)
    assert not gm.contains(hex_south, north_pole)
    assert gm.covers(hex_south, south_pole)
    assert gm.intersects(hex_south, south_pole)
    assert gm.within(south_pole, hex_south)


def test_pole_encircling_hexagon_array_predicates() -> None:
    hex_north, hex_south = _pole_hexagons()
    rows = gm.GeometryArray([hex_north, hex_south])
    north_pole = gm.Point(0, 90, crs=4326)
    south_pole = gm.Point(0, -90, crs=4326)
    assert bools(gm.contains(rows, gm.GeometryArray([north_pole, north_pole]))) == [
        True,
        False,
    ]
    assert bools(gm.contains(rows, south_pole)) == [False, True]
    assert bools(gm.covers(rows, gm.GeometryArray([north_pole, south_pole]))) == [
        True,
        True,
    ]
    assert bools(gm.intersects(rows, gm.GeometryArray([north_pole, south_pole]))) == [
        True,
        True,
    ]


def test_pole_encircling_hexagon_area_is_nontrivial() -> None:
    hex_north, _ = _pole_hexagons()
    area = hex_north.area
    assert area > 0.0
    assert math.isfinite(area)
    assert area < 0.5 * 510000000000000.0
