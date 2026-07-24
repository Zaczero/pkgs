"""Geodesic linear referencing on arrays and MultiLineString."""

from __future__ import annotations

import gometry as gm
import pytest
from conftest import canon


def test_geodesic_line_cache_isolated_across_frame_retag() -> None:
    coordinates = [(-100.0, 30.0), (-80.0, 45.0)]
    original = gm.LineString(coordinates, crs=4326)
    # Warm WGS84 before the metadata-only retag shares the immutable shape.
    original.line_interpolate(500_000.0)
    retagged = original.set_crs(4267, overwrite=True)
    fresh = gm.LineString(coordinates, crs=4267)
    assert (
        retagged.line_interpolate(500_000.0).to_wkb()
        == fresh.line_interpolate(500_000.0).to_wkb()
    )


def _geo_meridian() -> gm.LineString:
    return gm.LineString([(0, 0), (0, 10)], crs=4326)


def _geo_parallel() -> gm.LineString:
    return gm.LineString([(0, 0), (10, 0)], crs=4326)


def _planar_line() -> gm.LineString:
    return gm.LineString([(0, 0), (0, 10)], crs=3857)


def test_multi_line_string_geodesic_interpolate_locate_round_trip() -> None:
    multi = gm.MultiLineString([[(0, 0), (0, 5)], [(0, 5), (0, 10)]], crs=4326)
    _ = multi.length
    half = (multi).line_interpolate(0.5, normalized=True)
    loc = (multi).line_locate(half, normalized=True)
    assert loc == pytest.approx(0.5, rel=1e-06)
    assert half.y == pytest.approx(5.0, abs=0.1)


def test_array_geodesic_lrs_fraction_round_trip() -> None:
    rows = gm.GeometryArray([_geo_meridian(), _geo_parallel()])
    fractions = [0.25, 0.75]
    points = (rows).line_interpolate(fractions, normalized=True)
    located = (rows).line_locate(points, normalized=True)
    assert list(located) == pytest.approx(fractions, rel=1e-05)


def test_array_geodesic_lrs_absolute_round_trip() -> None:
    rows = gm.GeometryArray([_geo_meridian(), _geo_meridian()])
    lengths = rows.length
    distances = [length * 0.4 for length in lengths]
    points = (rows).line_interpolate(distances)
    relocated = (rows).line_locate(points)
    assert list(relocated) == pytest.approx(distances, rel=1e-05)


def test_planar_array_lrs_uses_coordinate_units() -> None:
    rows = gm.GeometryArray([_planar_line(), _planar_line()])
    point = (rows).line_interpolate(5.0)[0]
    assert point.y == pytest.approx(5.0)
    assert (rows).line_locate(point)[0] == pytest.approx(5.0)


def test_geodesic_vs_planar_lrs_diverge_by_crs() -> None:
    geo = _geo_meridian()
    planar = _planar_line()
    assert geo.length > 1000000.0
    assert planar.length == pytest.approx(10.0)
    geo_mid = (geo).line_interpolate(500000.0)
    planar_mid = (planar).line_interpolate(5.0)
    assert geo_mid.y == pytest.approx(4.5, abs=0.2)
    assert planar_mid.y == pytest.approx(5.0)


def test_multi_line_string_substring_preserves_geodesic_length() -> None:
    multi = gm.MultiLineString([[(0, 0), (0, 5)], [(0, 5), (0, 10)]], crs=4326)
    part = (multi).line_substring(0.2, 0.8, normalized=True)
    assert part.length == pytest.approx(multi.length * 0.6, rel=1e-05)


def test_array_multi_line_string_lrs_matches_scalar() -> None:
    multi = gm.MultiLineString([[(0, 0), (0, 5)], [(0, 5), (0, 10)]], crs=4326)
    arr = gm.GeometryArray([multi, multi])
    scalar_point = (multi).line_interpolate(100000.0)
    array_points = (arr).line_interpolate([100000.0, 100000.0])
    assert canon(array_points) == canon(gm.GeometryArray([scalar_point, scalar_point]))
