"""W5 linear & shape extras — line_interpolate, interpolate_m,
is_convex, extremes.
"""

import math

import gometry as gm
import numpy as np
import pytest


def test_line_interpolate_counts_distances_and_index_reuse() -> None:
    line = gm.LineString([(0, 0), (10, 0)], z=[0, 10], m=[10, 20])
    sampled = line.line_interpolate(count=3)
    assert [(p.x, p.z, p.m) for p in sampled] == [
        (0.0, 0.0, 10.0),
        (5.0, 5.0, 15.0),
        (10.0, 10.0, 20.0),
    ]
    assert [p.x for p in line.line_interpolate(count=1)] == [0.0]
    assert [p.x for p in line.line_interpolate([2.5, 99.0])] == [2.5, 10.0]
    points = line.line_interpolate([0.25, 0.75], normalized=True)
    assert [p.x for p in points] == [2.5, 7.5]
    geodesic = gm.LineString([(0, 0), (1, 0)], crs=4326)
    metre_points = geodesic.line_interpolate([55000.0])
    assert metre_points.crs == 'EPSG:4326'
    assert metre_points[0].x == pytest.approx(0.494, abs=0.01)
    rows = gm.GeometryArray([line, gm.LineString([(0, 0), (4, 0)])])
    per_row = rows.line_interpolate(count=2)
    assert isinstance(per_row, gm.Groups)
    assert [[p.x for p in part] for part in per_row] == [[0.0, 10.0], [0.0, 4.0]]
    varied = rows.line_interpolate(count=[2, 3])
    assert [[p.x for p in part] for part in varied] == [
        [0.0, 10.0],
        [0.0, 2.0, 4.0],
    ]
    with pytest.raises(gm.GeometryError, match='exactly one'):
        line.line_interpolate()
    with pytest.raises(gm.GeometryError, match='exactly one'):
        line.line_interpolate([1.0], count=2)
    with pytest.raises(gm.GeometryError, match='count must be >= 1, got 0'):
        line.line_interpolate(count=0)
    with pytest.raises(gm.GeometryError, match='normalized applies to distances'):
        line.line_interpolate(count=2, normalized=True)
    with pytest.raises(TypeError, match='LineString'):
        gm.box(0, 0, 1, 1).line_interpolate(count=2)


def test_interpolate_m_runs_continuously_across_parts() -> None:
    line = gm.LineString([(0, 0), (10, 0)], z=[7, 8])
    measured = line.interpolate_m(0, 100)
    assert measured.to_wkt() == 'LINESTRING ZM (0 0 7 0, 10 0 8 100)'
    multi = gm.from_wkt('MULTILINESTRING ((0 0, 10 0), (20 0, 30 0))')
    measured_multi = multi.interpolate_m(10, 70)
    assert (
        measured_multi.to_wkt()
        == 'MULTILINESTRING M ((0 0 10, 10 0 40), (20 0 40, 30 0 70))'
    )
    assert measured.line_substring(25, 75, basis='m').to_wkt().startswith('LINESTRING')
    with pytest.raises(gm.InvalidGeometryError, match='overwrite=True'):
        measured.interpolate_m(0, 1)
    assert (
        measured.interpolate_m(0, 1, overwrite=True).to_wkt()
        == 'LINESTRING ZM (0 0 7 0, 10 0 8 1)'
    )
    with pytest.raises(gm.GeometryError, match='end_m >= start_m'):
        line.interpolate_m(5, 1)
    with pytest.raises(TypeError, match='LineString'):
        gm.box(0, 0, 1, 1).interpolate_m(0, 1)
    with pytest.raises(gm.InvalidGeometryError, match='linework'):
        gm.from_wkt('LINESTRING EMPTY').interpolate_m(0, 1)
    array = gm.GeometryArray([line]).interpolate_m(0, 100)
    assert gm.equals(array[0], measured)
    assert gm.equals(line.interpolate_m(0, 100), measured)


def test_is_convex_polygon_matrix() -> None:
    assert gm.box(0, 0, 1, 1).is_convex
    assert gm.Polygon([(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]).is_convex
    assert gm.Polygon([(0, 0), (1, 0), (2, 0), (2, 2), (0, 2), (0, 0)]).is_convex
    assert not gm.Polygon([(0, 0), (2, 0), (1, 0.5), (2, 2), (0, 2), (0, 0)]).is_convex
    holed = gm.Polygon(
        [(0, 0), (3, 0), (3, 3), (0, 3), (0, 0)],
        holes=[[(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]],
    )
    assert not holed.is_convex
    assert gm.from_wkt('POLYGON EMPTY').is_convex
    assert gm.Polygon([
        (0, 0),
        (1, 0),
        (1, 0),
        (1, 1),
        (1, 1),
        (0, 1),
        (0, 1),
        (0, 0),
    ]).is_convex
    assert gm.from_wkt('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 1, 0 0))').is_convex
    pts = [
        (math.cos(2 * math.pi * i / 5), math.sin(2 * math.pi * i / 5)) for i in range(5)
    ]
    star = gm.Polygon([pts[i] for i in (0, 2, 4, 1, 3, 0)])
    assert not star.is_convex
    assert not gm.from_wkt('MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))').is_convex
    assert not gm.Point(5, 5).is_convex
    assert gm.box(0, 0, 1, 1).is_convex
    np.testing.assert_array_equal(
        gm.GeometryArray([gm.box(0, 0, 1, 1), holed, gm.Point(5, 5)]).is_convex,
        [True, False, False],
    )


def test_extremes_picks_first_tied_vertex_and_carries_zm() -> None:
    geom = gm.MultiPoint([(0, 1), (2, 3), (0, 3), (2, 1)])
    extremes = geom.extremes()
    assert isinstance(extremes, gm.Extremes)
    west, south, east, north = extremes
    assert (west.x, west.y) == (0.0, 1.0)
    assert (south.x, south.y) == (0.0, 1.0)
    assert (east.x, east.y) == (2.0, 3.0)
    assert (north.x, north.y) == (2.0, 3.0)
    lifted = gm.LineString([(0, 0), (1, 1)], z=[5, 6], crs=3857).extremes()
    assert lifted.west.z == 5.0 and lifted.west.crs == 'EPSG:3857'
    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        gm.from_wkt('POINT EMPTY').extremes()
    rows = gm.GeometryArray([gm.Point(1, 2), gm.box(0, 0, 5, 5)]).extremes()
    assert isinstance(rows, gm.Extremes)
    west, south, east, north = rows
    assert all(isinstance(column, gm.GeometryArray) for column in rows)
    assert north[1].y == 5.0 and west[0].x == 1.0
    assert gm.Point(1, 2).extremes().west.x == 1.0
    degraded = gm.GeometryArray([
        gm.Point(1, 2),
        gm.from_wkt('POINT EMPTY'),
        None,
    ]).extremes()
    assert degraded.east[1].is_empty
    assert all(column[2] is None for column in degraded)
    assert degraded.south[0].y == 2.0


def test_cross_track_distance_signs_and_gates() -> None:
    start, end = (gm.Point(0, 0, crs=4326), gm.Point(1, 0, crs=4326))
    left = gm.cross_track_distance(gm.Point(0.5, 0.1, crs=4326), start, end)
    assert left == pytest.approx(11120, rel=0.01)
    assert gm.cross_track_distance(
        gm.Point(0.5, -0.1, crs=4326), start, end
    ) == pytest.approx(-left, rel=1e-09)
    assert gm.cross_track_distance(
        gm.Point(0.5, 0.1, crs=4326), end, start
    ) == pytest.approx(-left, rel=1e-09)
    assert gm.cross_track_distance(gm.Point(0.5, 0, crs=4326), start, end) == 0.0
    with pytest.raises(gm.CRSError, match='geographic'):
        gm.cross_track_distance(gm.Point(0.5, 0.1), gm.Point(0, 0), gm.Point(1, 0))
    with pytest.raises(gm.InvalidGeometryError, match='distinct'):
        gm.cross_track_distance(gm.Point(0.5, 0.1, crs=4326), start, start)
    with pytest.raises(gm.CRSMismatchError):
        gm.cross_track_distance(gm.Point(0.5, 0.1, crs=4326), gm.Point(0, 0), end)


def test_rhumb_trio_matches_rhumbsolve_oracles() -> None:
    jfk = gm.Point(-73.8, 40.6, crs=4326)
    lhr = gm.Point(-0.5, 51.6, crs=4326)
    assert gm.rhumb_distance(jfk, lhr) == pytest.approx(5771083.383328027, abs=0.0001)
    assert gm.bearing(jfk, lhr, path='rhumb') == pytest.approx(
        77.768389710256, abs=1e-09
    )
    jfk_exact = gm.Point(-73.778888888889, 40.639722222222, crs=4326)
    sin = gm.Point(103.989444444444, 1.359166666667, crs=4326)
    assert gm.rhumb_distance(jfk_exact, sin) == pytest.approx(
        18523563.04237743, abs=0.001
    )
    assert gm.bearing(jfk_exact, sin, path='rhumb') == pytest.approx(
        103.582833003411, abs=1e-09
    )
    east = gm.Point(170.0, 15.0, crs=4326)
    west = gm.Point(-170.0, 15.0, crs=4326)
    assert gm.rhumb_distance(east, west) == pytest.approx(2151009.774334714, abs=0.0001)
    assert gm.bearing(east, west, path='rhumb') == pytest.approx(90.0, abs=1e-09)
    assert gm.bearing(west, east, path='rhumb') == pytest.approx(270.0, abs=1e-09)
    end = gm.destination(jfk, 51.0, 5500000.0, path='rhumb')
    assert (end.y, end.x) == (
        pytest.approx(71.688899882813, abs=1e-08),
        pytest.approx(0.255519824423, abs=1e-08),
    )
    end = gm.destination(gm.Point(-10, 45, crs=4326), 90.0, 1000000.0, path='rhumb')
    assert (end.y, end.x) == (
        pytest.approx(45.0, abs=1e-12),
        pytest.approx(2.682817246984, abs=1e-08),
    )
    end = gm.destination(gm.Point(170, -45, crs=4326), 0.0, 1000000.0, path='rhumb')
    assert (end.y, end.x) == (
        pytest.approx(-35.994607984102956, abs=1e-08),
        pytest.approx(170.0, abs=0),
    )
    end = gm.destination(gm.Point(40, 88, crs=4326), 80.0, 500000.0, path='rhumb')
    assert (end.y, end.x) == (
        pytest.approx(88.77734535586033, abs=1e-08),
        pytest.approx(-160.06955185679547, abs=1e-07),
    )
    end = gm.destination(gm.Point(179, 10, crs=4326), 90.0, 500000.0, path='rhumb')
    assert (end.y, end.x) == (
        pytest.approx(10.0, abs=1e-12),
        pytest.approx(-176.43959412525237, abs=1e-08),
    )
    tagged = gm.destination(
        gm.Point(0, 0, z=5.0, m=7.0, crs=4326), 90.0, 1000.0, path='rhumb'
    )
    assert tagged.crs == 'EPSG:4326'
    assert (tagged.z, tagged.m) == (5.0, 7.0)


def test_rhumb_trio_error_gates() -> None:
    with pytest.raises(gm.InvalidGeometryError, match='crosses a pole'):
        gm.destination(gm.Point(0, 89, crs=4326), 0.0, 200000.0, path='rhumb')
    with pytest.raises(gm.CRSError, match='requires a geographic CRS'):
        gm.rhumb_distance(gm.Point(0, 0), gm.Point(1, 1))
    with pytest.raises(gm.CRSError, match='requires a geographic CRS'):
        gm.bearing(gm.Point(0, 0, crs=3857), gm.Point(1, 1, crs=3857), path='rhumb')
    with pytest.raises(gm.CRSMismatchError, match='rhumb_distance'):
        gm.rhumb_distance(gm.Point(0, 0, crs=4326), gm.Point(1, 1, crs=3857))
    with pytest.raises(gm.GeometryError, match='non-negative'):
        gm.destination(gm.Point(0, 0, crs=4326), 90.0, -1.0, path='rhumb')
    with pytest.raises(gm.GeometryError, match='bearing'):
        gm.destination(gm.Point(0, 0, crs=4326), float('nan'), 1.0, path='rhumb')
    with pytest.raises(gm.InvalidGeometryError, match='crosses a pole'):
        gm.destination(gm.Point(0, 0, crs=4326), 80.0, 57599025.01538026, path='rhumb')
    arrival = gm.destination(
        gm.Point(7, 0, crs=4326), 0.0, 10001965.729311671, path='rhumb'
    )
    assert arrival.x == 7.0
    assert arrival.y == pytest.approx(90.0, abs=1e-09)


def test_rhumb_pole_endpoints_and_slope_continuity() -> None:
    pole = gm.Point(0, 90, crs=4326)
    assert gm.rhumb_distance(pole, gm.Point(10, 90, crs=4326)) == 0.0
    expected = gm.rhumb_distance(pole, gm.Point(0, 80, crs=4326))
    assert gm.rhumb_distance(pole, gm.Point(180, 80, crs=4326)) == pytest.approx(
        expected, abs=1e-06
    )
    assert gm.bearing(pole, gm.Point(180, 80, crs=4326), path='rhumb') == 180.0
    assert (
        gm.bearing(gm.Point(10, 89, crs=4326), gm.Point(20, 90, crs=4326), path='rhumb')
        == 0.0
    )
    p = gm.Point(0, 89.999, crs=4326)
    q1 = gm.Point(0.01, 89.99957238483734, crs=4326)
    q2 = gm.Point(0.01, 89.99957353075293, crs=4326)
    assert gm.rhumb_distance(p, q1) == pytest.approx(63.931941671, abs=0.001)
    assert gm.rhumb_distance(p, q2) == pytest.approx(64.059933538, abs=0.001)


def test_interpolate_m_stations_geodesically_on_a_geographic_crs() -> None:
    line = gm.LineString([(0, 0), (0, 60), (10, 60)], crs=4326)
    geodesic = [point[2] for point in line.interpolate_m(0, 100).coords]
    planar = [point[2] for point in line.interpolate_m(0, 100, unit='planar').coords]
    assert planar[1] == pytest.approx(85.714, abs=0.01)
    assert geodesic[1] == pytest.approx(92.27, abs=0.1)
    assert geodesic[1] > planar[1]
    xy = [(p[0], p[1]) for p in line.interpolate_m(0, 100).coords]
    assert xy == [(0.0, 0.0), (0.0, 60.0), (10.0, 60.0)]
    free = list(gm.LineString([(0, 0), (10, 0)]).interpolate_m(0, 100).coords)
    assert (free[0][2], free[-1][2]) == (0.0, 100.0)


def test_w4b_interpolate_m_endpoint_exact_and_multipart_prefix() -> None:
    """W4B-topology: one-pass segment lengths keep final vertex at end_m
    exactly and multipart gaps consume no measure (PostGIS ST_AddMeasure).
    """
    line = gm.LineString([(0.0, 0.0), (5.0, 0.0), (10.0, 0.0), (10.0, 10.0)])
    measured = line.interpolate_m(0.0, 100.0)
    # XYM coords are (x, y, m).
    coords = list(measured.coords)
    assert coords[0][2] == 0.0
    assert coords[-1][2] == 100.0
    # Vertex at (10,0) is 10 units of 20 total length → measure 50.
    assert coords[2][2] == pytest.approx(50.0)
    multi = gm.from_wkt('MULTILINESTRING ((0 0, 10 0), (100 0, 110 0))')
    mm = multi.interpolate_m(0.0, 200.0)
    # Two equal-length parts of 10: measures continuous across the gap.
    parts = list(mm.parts)
    p0 = list(parts[0].coords)
    p1 = list(parts[1].coords)
    assert p0[0][2] == 0.0
    assert p0[-1][2] == pytest.approx(100.0)
    assert p1[0][2] == pytest.approx(100.0)
    assert p1[-1][2] == 200.0
