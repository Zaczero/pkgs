"""Geographic linear referencing and segmentize routes on arrays."""

from __future__ import annotations

import math
import struct
from itertools import pairwise
from typing import Literal

import gometry as gm
import numpy as np
import pytest

from tests._support import canon

pyproj = pytest.importorskip('pyproj', reason='pyproj is an optional oracle dependency')

RADIAN_CRS = (
    'GEOGCS["WGS 84 rad",'
    'DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],UNIT["radian",1],'
    'AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]'
)
GRAD_CRS = (
    'GEOGCS["WGS 84 grad",'
    'DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],UNIT["grad",0.015707963267948967],'
    'AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]'
)

# Fixed per-fixture allowances. Each includes two 4-epsilon arclength
# inversions, four-ULP physical coordinate quantization, sampled-speed
# parameter rounding and station-result ULPs. These are fixture budgets, not a
# public accuracy promise.
_PARALLEL_60_STATION_BUDGETS_M = (
    4.379270181329882e-9,
    4.572268687309775e-9,
    4.572268687309775e-9,
)
_EQUATOR_STATION_BUDGETS_M = (
    2.5531051500495636e-9,
    3.12877891180728e-9,
    3.12877891180728e-9,
)
_MERIDIAN_STATION_BUDGETS_M = (
    2.537073977135474e-9,
    3.10977784340784e-9,
    3.10981546551732e-9,
)
_MERIDIAN_0_4_STATION_BUDGET_M = 2.7354731392650395e-9
_SLOPED_STATION_BUDGETS_M = (
    2.61104038767807e-8,
    3.07035733457654e-8,
    3.54020085158243e-8,
)
_SEAM_STATION_BUDGETS_M = (
    1.30693911723018e-8,
    1.31350672290156e-8,
    1.31641710594723e-8,
)
_PARALLEL_60_XY_BUDGETS_NATIVE = (
    (1.06581410364015e-14, 2.84217094304040e-14),
    (1.24344978758018e-14, 2.84217094304040e-14),
    (1.24344978758018e-14, 2.84217094304040e-14),
)
_SLOPED_XY_BUDGETS_NATIVE = (
    (2.45731802264673e-13, 1.10677916194148e-13),
    (2.59942656979875e-13, 1.24888770909350e-13),
    (2.88364366410279e-13, 1.24888770909350e-13),
)
_SEAM_XY_BUDGETS_NATIVE = (
    (1.15463194561016e-13, 7.10542735760100e-15),
    (1.15463194561016e-13, 7.10542735760100e-15),
    (1.15463194561016e-13, 7.10542735760100e-15),
)
_PARALLEL_60_SUBSTRING_LENGTH_BUDGETS_M = {
    (0.25, 0.75): 7.934261019153545e-9,
    (0.5, 0.9): 7.908106812869739e-9,
}
_SLOPED_SUBSTRING_LENGTH_BUDGET_M = 4.091897092081309e-8
_SEAM_SUBSTRING_LENGTH_BUDGET_M = 2.5817155686429858e-8
_MERIDIAN_MULTIPART_SUBSTRING_LENGTH_BUDGET_M = 4.069654198825482e-9
_WGS84_MERIDIAN_0_TO_10_M = 1_105_854.8332343723
_WGS84_EQUATOR_0_TO_10_M = 1_113_194.9079327357
# Independent WGS84 meridian-arc series, evaluated outside gometry. Solve
# M(phi)=M(10°)/2 for M(phi)=a(1-e²)∫₀^phi(1-e²sin²θ)^(-3/2)dθ, with
# a=6,378,137 m and f=1/298.257223563. Expand (1-q)^(-3/2) as
# Σ c_n q^n, c_n=(2n+1)!!/(2^n n!), and integrate each sin^(2n) term:
# I_0=phi, I_n=-sin^(2n-1)(phi)cos(phi)/(2n)+(2n-1)I_(n-1)/(2n).
# Summing n=0..11 and bisecting at 90 decimal digits gives
# 5.000379494754140659332234714015479775913... degrees. The omitted
# series tail is below 2.05e-38 m. By symmetry, this is also the magnitude
# of the quarter/three-quarter arclength latitudes on -10°..10°.
_WGS84_MERIDIAN_HALF_0_TO_10_LATITUDE_DEG = 5.0003794947541405
# The 4-epsilon inverse-length residual converted by the meridian radius, plus
# source/prefix summation and four output-coordinate ULPs.
_WGS84_MERIDIAN_HALF_LATITUDE_BUDGET_DEG = 2.0e-14


def _length_oracle_budget(length: float) -> float:
    # Closed-form/Karney oracle allowance: the four-epsilon numerical quality
    # target plus two result ULPs.
    return 4.0 * math.ulp(1.0) * length + 2.0 * math.ulp(length)


def _assert_station_within_budget(
    actual: float,
    expected_m: float,
    budget_m: float,
    *,
    total_length: float | None = None,
) -> None:
    actual_m = actual * total_length if total_length is not None else actual
    assert abs(actual_m - expected_m) <= budget_m


def _assert_xy_within_budget(
    actual: tuple[float, float],
    expected: tuple[float, float],
    budget: tuple[float, float],
) -> None:
    assert abs(actual[0] - expected[0]) <= budget[0]
    assert abs(actual[1] - expected[1]) <= budget[1]


def _ordinate_rounding_budget(expected: float, source_delta: float) -> float:
    return 4.0 * math.ulp(expected) + 8.0 * math.ulp(1.0) * abs(source_delta)


def _parallel_alias_coordinate_budget(
    start_longitude: float,
    expected_longitude: float,
    latitude: float,
    longitude_span: float,
) -> tuple[float, float]:
    # On a parallel, ds/dt is constant, so t=distance/length has one division
    # rounding. Four source/output ULPs enclose native alias and edge-position
    # arithmetic; the parameter term is propagated by the source span.
    fraction_rounding = 4.0 * math.ulp(1.0)
    return (
        4.0 * max(math.ulp(start_longitude), math.ulp(expected_longitude))
        + abs(longitude_span) * fraction_rounding,
        4.0 * math.ulp(latitude),
    )


def _wgs84_parallel_length(latitude_degrees: float, longitude_degrees: float) -> float:
    """Closed-form ellipsoidal arclength of a straight chart parallel."""
    semi_major = 6_378_137.0
    flattening = 1.0 / 298.257_223_563
    eccentricity_squared = flattening * (2.0 - flattening)
    latitude = math.radians(latitude_degrees)
    prime_vertical = semi_major / math.sqrt(
        1.0 - eccentricity_squared * math.sin(latitude) ** 2
    )
    return prime_vertical * math.cos(latitude) * math.radians(longitude_degrees)


# Canonical k=0 results pin the established route coordinates and WKB bytes.
_CANONICAL_MERIDIAN_WKB = {
    (None, 'fraction'): bytes.fromhex(
        '010200000003000000000000000000244000000000000000000000000000002440'
        'caa0da340300f03f00000000000024400000000000000040'
    ),
    (None, 'max_length'): bytes.fromhex(
        '010200000004000000000000000000244000000000000000000000000000002440'
        '64c687665a55e53f0000000000002440339303805855f53f0000000000002440'
        '0000000000000040'
    ),
    ('geodesic', 'fraction'): bytes.fromhex(
        '010200000003000000000000000000244000000000000000000000000000002440'
        'caa0da340300f03f00000000000024400000000000000040'
    ),
    ('geodesic', 'max_length'): bytes.fromhex(
        '010200000004000000000000000000244000000000000000000000000000002440'
        '64c687665a55e53f0000000000002440339303805855f53f0000000000002440'
        '0000000000000040'
    ),
    ('rhumb', 'fraction'): bytes.fromhex(
        '010200000003000000000000000000244000000000000000000000000000002440'
        'cba0da340300f03f00000000000024400000000000000040'
    ),
    ('rhumb', 'max_length'): bytes.fromhex(
        '010200000004000000000000000000244000000000000000000000000000002440'
        '67c687665a55e53f0000000000002440359303805855f53f0000000000002440'
        '0000000000000040'
    ),
}


def _xy_bits(coordinate: tuple[float, ...]) -> tuple[bytes, bytes]:
    return (struct.pack('>d', coordinate[0]), struct.pack('>d', coordinate[1]))


def _segmentize_target(
    line: gm.LineString,
    target: Literal['fraction', 'max_length'],
    path: Literal['geodesic', 'rhumb'] | None,
) -> gm.LineString:
    if target == 'fraction':
        return line.segmentize(fraction=0.5, path=path)
    return line.segmentize(100_000.0, path=path)


def test_chart_line_cache_isolated_across_frame_retag() -> None:
    coordinates = [(-100.0, 30.0), (-80.0, 45.0)]
    original = gm.LineString(coordinates, crs=4326)
    # Warm WGS84 before the metadata-only retag shares the immutable shape.
    original.line_interpolate(500_000.0)
    retagged = original.with_crs(4267, overwrite=True)
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


def test_multi_line_string_chart_interpolate_locate_round_trip() -> None:
    multi = gm.MultiLineString([[(0, 0), (0, 5)], [(0, 5), (0, 10)]], crs=4326)
    half = (multi).line_interpolate(0.5, normalized=True)
    loc = (multi).line_locate(half, normalized=True)
    assert loc == 0.5
    assert (
        abs(half.y - _WGS84_MERIDIAN_HALF_0_TO_10_LATITUDE_DEG)
        <= _WGS84_MERIDIAN_HALF_LATITUDE_BUDGET_DEG
    )


@pytest.mark.parametrize(
    'normalized,expected', [(False, 268178.80229270144), (True, 0.706698050287263)]
)
def test_geographic_witness_role_locate_uses_selected_interior_station(
    normalized, expected
) -> None:
    route = gm.from_wkt(
        'MULTILINESTRING ZM ((0 1 10 100,1 2 20 200),(-1 1 30 300,1 1 50 500))',
        crs=4326,
    )
    point = gm.from_wkt('POINT ZM (0 0 7 70)', crs=4326)
    assert route.line_locate(point, normalized=normalized) == expected
    assert route.line_locate(point, basis='m') == 400.0
    rows = gm.GeometryArray([route, None, route])[[2, 0, 1, 2]]
    values = list(rows.line_locate(point, normalized=normalized))
    assert values[:2] == [expected, expected]
    assert math.isnan(values[2])
    assert values[3] == expected
    measured = list(rows.line_locate(gm.GeometryArray([point] * 4), basis='m'))
    assert measured[:2] == [400.0, 400.0]
    assert math.isnan(measured[2])
    assert measured[3] == 400.0


@pytest.mark.parametrize(
    'wkt,station,measure',
    [
        (
            'MULTILINESTRING ZM ((0 1 10 100,0 1 10 100),(-1 1 30 300,1 1 50 500))',
            111302.6497697324,
            400.0,
        ),
        (
            'MULTILINESTRING ZM ((0 1 10 100,1 2 20 200),(-1 1.0000000000000002 30 300,1 1.0000000000000002 50 500))',
            0.0,
            100.0,
        ),
        (
            'MULTILINESTRING ZM ((0 1 10 100,2 1 20 200),(-2 1 30 300,0 1 50 500))',
            0.0,
            250.0,
        ),
        (
            'MULTILINESTRING ZM ((-1 1 10 100,1 1 20 200),(-1 1 30 300,1 1 50 500))',
            111302.6497697324,
            150.0,
        ),
    ],
    ids=['degenerate', 'two-ulp-farther', 'same-endpoint-role', 'same-interior-role'],
)
def test_geographic_witness_role_locate_preserves_eligibility_and_source_order(
    wkt, station, measure
) -> None:
    route = gm.from_wkt(wkt, crs=4326)
    point = gm.Point(0.0, 0.0, crs=4326)
    assert route.line_locate(point) == station
    # M's flattened route also admits the interpart connector; its interior
    # can win where distance-basis support contains only tied endpoints.
    assert route.line_locate(point, basis='m') == measure


def test_geographic_witness_role_locate_packed_connected_line() -> None:
    route = gm.from_wkt(
        'LINESTRING ZM (0 1 10 100,1 2 20 200,-1 1 30 300,1 1 50 500)', crs=4326
    )
    point = gm.from_wkt('POINT ZM (0 0 7 70)', crs=4326)
    rows = gm.GeometryArray([route, route, None])[[1, 0, 2]]
    assert route.line_locate(point, basis='m') == 400.0
    measured = list(rows.line_locate(point, basis='m'))
    assert measured[:2] == [400.0, 400.0]
    assert math.isnan(measured[2])
    station = route.line_locate(point)
    assert station > 111302.6497697324
    located = list(rows.line_locate(gm.GeometryArray([point] * 3)))
    assert located[:2] == [station, station]
    assert math.isnan(located[2])


def test_crs84_full_turn_parallel_locate_interpolate_round_trip() -> None:
    crs = 'OGC:CRS84'
    route = gm.LineString([(10.0, 60.0), (370.0, 60.0)], crs=crs)
    longitudes = [28.0, 100.0, -170.0, -80.0, -8.0]
    stations = []
    for longitude in longitudes:
        query = gm.Point(longitude, 60.0, crs=crs)
        station = route.line_locate(query, normalized=True)
        stations.append(station)
        interpolated = route.line_interpolate(station, normalized=True)
        _assert_xy_within_budget(
            (interpolated.x, interpolated.y),
            (longitude, 60.0),
            _parallel_alias_coordinate_budget(10.0, longitude, 60.0, 360.0),
        )

    assert stations[0] < 0.1
    assert stations[-1] > 0.9
    assert all(left < right for left, right in pairwise(stations))


def test_line_substring_keeps_retained_circle_intervals_representable() -> None:
    route = gm.LineString(
        [(10.0, 60.0, 7.0, 101.0), (370.0, 60.0, 47.0, 701.0)],
        crs='OGC:CRS84',
    )
    source = route.to_wkb()

    east = route.line_substring(0.25, 1.0, normalized=True)
    west = route.line_substring(0.0, 0.75, normalized=True)
    whole = route.line_substring(0.0, 1.0, normalized=True)

    assert list(east.coords) == [
        (100.0, 60.0, 17.0, 251.0),
        (235.0, 60.0, 32.0, 476.0),
        (370.0, 60.0, 47.0, 701.0),
    ]
    assert list(west.coords) == [
        (10.0, 60.0, 7.0, 101.0),
        (145.0, 60.0, 22.0, 326.0),
        (280.0, 60.0, 37.0, 551.0),
    ]
    assert whole.to_wkb() == source
    assert east.length == pytest.approx(route.length * 0.75, rel=0.0, abs=2.0e-8)
    assert west.length == pytest.approx(route.length * 0.75, rel=0.0, abs=2.0e-8)
    assert route.to_wkb() == source


def test_generated_geographic_half_turns_are_refined_for_public_consumers() -> None:
    route = gm.LineString([(10.0, 0.0), (370.0, 0.0)], crs=4326)

    half = route.line_substring(0.0, 0.5, normalized=True)
    assert list(half.coords) == [
        (10.0, 0.0),
        (100.0, 0.0),
        (190.0, 0.0),
    ]
    assert half.length > 0.0

    segmented = route.segmentize(30_000_000.0)
    assert list(segmented.coords) == [
        (10.0, 0.0),
        (100.0, 0.0),
        (190.0, 0.0),
        (280.0, 0.0),
        (370.0, 0.0),
    ]
    assert segmented.length == pytest.approx(route.length, rel=0.0, abs=2.0e-8)

    literal_half_turn = gm.LineString([(10.0, 0.0), (190.0, 0.0)], crs=4326)
    with pytest.raises(gm.GeometryError, match='exactly half the longitude period'):
        _ = literal_half_turn.length


@pytest.mark.parametrize(
    ('z', 'm', 'expected_ordinates'),
    [
        pytest.param([-0.0, 40.0], None, [(10.0,), (20.0,), (30.0,)], id='xyz'),
        pytest.param(None, [100.0, 500.0], [(200.0,), (300.0,), (400.0,)], id='xym'),
        pytest.param(
            [-0.0, 40.0],
            [100.0, 500.0],
            [(10.0, 200.0), (20.0, 300.0), (30.0, 400.0)],
            id='xyzm',
        ),
    ],
)
@pytest.mark.parametrize('array', [False, True], ids=['scalar', 'array'])
@pytest.mark.parametrize('fractional', [False, True], ids=['max-length', 'fraction'])
def test_chart_segmentize_half_turn_refinement_carries_emitted_source_ordinates(
    z: list[float] | None,
    m: list[float] | None,
    expected_ordinates: list[tuple[float, ...]],
    array: bool,
    fractional: bool,
) -> None:
    line = gm.LineString([(10.0, 0.0), (370.0, 0.0)], z=z, m=m, crs=4326, epoch=2020.0)
    target = gm.GeometryArray([line, None], crs=4326, epoch=2020.0) if array else line
    result = (
        target.segmentize(fraction=0.5)
        if fractional
        else target.segmentize(30_000_000.0)
    )
    assert result.crs == line.crs
    assert result.epoch == line.epoch
    if isinstance(result, gm.GeometryArray):
        assert result.is_missing.tolist() == [False, True]
        result = result[0]
    assert isinstance(result, gm.LineString)
    assert result.coordinate_axes == line.coordinate_axes
    coordinates = list(result.coords)
    assert [point[:2] for point in coordinates] == [
        (10.0, 0.0),
        (100.0, 0.0),
        (190.0, 0.0),
        (280.0, 0.0),
        (370.0, 0.0),
    ]
    # The quarter/three-quarter vertices refine half-turn representation, not
    # the requested half-arclength station; carry must follow their own roots.
    for point, expected in zip(coordinates[1:-1], expected_ordinates, strict=True):
        for actual, ordinate, start, end in zip(
            point[2:], expected, coordinates[0][2:], coordinates[-1][2:], strict=True
        ):
            assert actual == pytest.approx(
                ordinate, rel=0.0, abs=_ordinate_rounding_budget(ordinate, end - start)
            )
    for actual, source in zip(
        (coordinates[0], coordinates[-1]),
        (line.coords[0], line.coords[-1]),
        strict=True,
    ):
        assert struct.pack(f'{len(actual)}d', *actual) == struct.pack(
            f'{len(source)}d', *source
        )


@pytest.mark.parametrize(
    'end_latitude',
    [
        pytest.param(math.nextafter(10.0, math.inf), id='increasing-latitude'),
        pytest.param(math.nextafter(10.0, -math.inf), id='decreasing-latitude'),
    ],
)
def test_geographic_substring_accepts_quantized_latitude_when_longitude_advances(
    end_latitude: float,
) -> None:
    route = gm.LineString([(0.0, 10.0), (10.0, end_latitude)], crs=4326)

    assert route.length > 1_000_000.0
    substring = route.line_substring(0.125, 0.25, normalized=True)

    assert list(substring.coords) == [(1.25, 10.0), (2.5, 10.0)]
    assert substring.length > 0.0


def test_array_chart_lrs_fraction_round_trip() -> None:
    rows = gm.GeometryArray([_geo_meridian(), _geo_parallel()])
    fractions = [0.25, 0.75]
    points = (rows).line_interpolate(fractions, normalized=True)
    located = (rows).line_locate(points, normalized=True)
    lengths = [_WGS84_MERIDIAN_0_TO_10_M, _WGS84_EQUATOR_0_TO_10_M]
    budgets = [_MERIDIAN_STATION_BUDGETS_M[0], _EQUATOR_STATION_BUDGETS_M[2]]
    for row in range(2):
        _assert_station_within_budget(
            located[row],
            fractions[row] * lengths[row],
            budgets[row],
            total_length=lengths[row],
        )


def test_array_chart_lrs_absolute_round_trip() -> None:
    rows = gm.GeometryArray([_geo_meridian(), _geo_meridian()])
    lengths = [_WGS84_MERIDIAN_0_TO_10_M] * 2
    distances = [length * 0.4 for length in lengths]
    points = (rows).line_interpolate(distances)
    relocated = (rows).line_locate(points)
    for actual, expected in zip(relocated, distances, strict=True):
        _assert_station_within_budget(actual, expected, _MERIDIAN_0_4_STATION_BUDGET_M)


def test_chart_locate_keeps_an_earlier_equal_endpoint_found_after_a_later_edge() -> (
    None
):
    route = gm.MultiLineString(
        [[(0.0, 0.0), (-1.0, -1.0)], [(0.0, 0.0), (10.0, -11.0)]],
        crs=4326,
    )
    query = gm.Point(1.0, 1.0, crs=4326)

    # Both edges attain the same endpoint distance. The second edge has the
    # tighter chart-box bound and is refined first; source order still owns the
    # exact-distance tie.
    assert route.line_locate(query) == 0.0
    assert list(gm.GeometryArray([route, route]).line_locate(query)) == [0.0, 0.0]

    repeated = gm.LineString([(0.0, 0.0), (1.0, 0.0), (0.0, 0.0), (1.0, 0.0)], crs=4326)
    first_route_station = _wgs84_parallel_length(0.0, 1.0) / 2.0
    assert repeated.line_locate(gm.Point(0.5, 0.0, crs=4326)) == first_route_station

    crossing = gm.LineString(
        [(-1.0, -1.0), (1.0, 1.0), (-1.0, 1.0), (1.0, -1.0)], crs=4326
    )
    first_crossing_station = gm.LineString([(-1.0, -1.0), (0.0, 0.0)], crs=4326).length
    assert crossing.line_locate(gm.Point(0.0, 0.0, crs=4326)) == first_crossing_station


def test_planar_array_lrs_uses_coordinate_units() -> None:
    rows = gm.GeometryArray([_planar_line(), _planar_line()])
    point = (rows).line_interpolate(5.0)[0]
    assert point.y == pytest.approx(5.0)
    assert (rows).line_locate(point)[0] == pytest.approx(5.0)


def test_geographic_chart_lrs_uses_ellipsoidal_length() -> None:
    geo = _geo_meridian()
    planar = _planar_line()
    assert geo.length == pytest.approx(
        _WGS84_MERIDIAN_0_TO_10_M,
        abs=_length_oracle_budget(_WGS84_MERIDIAN_0_TO_10_M),
    )
    assert planar.length == pytest.approx(10.0)
    geo_mid = (geo).line_interpolate(500000.0)
    planar_mid = (planar).line_interpolate(5.0)
    # Independent GeographicLib WGS84 meridian direct result at 500,000 m.
    assert geo_mid.y == pytest.approx(4.52175323353737, rel=0.0, abs=2e-14)
    assert planar_mid.y == pytest.approx(5.0)


def test_multi_line_string_substring_preserves_chart_length() -> None:
    multi = gm.MultiLineString([[(0, 0), (0, 5)], [(0, 5), (0, 10)]], crs=4326)
    part = (multi).line_substring(0.2, 0.8, normalized=True)
    expected = 663_512.899_940_623_3
    assert part.length == pytest.approx(
        expected, rel=0.0, abs=_MERIDIAN_MULTIPART_SUBSTRING_LENGTH_BUDGET_M
    )


def test_array_multi_line_string_chart_lrs_matches_scalar() -> None:
    multi = gm.MultiLineString([[(0, 0), (0, 5)], [(0, 5), (0, 10)]], crs=4326)
    arr = gm.GeometryArray([multi, multi])
    scalar_point = (multi).line_interpolate(100000.0)
    array_points = (arr).line_interpolate([100000.0, 100000.0])
    assert canon(array_points) == canon(gm.GeometryArray([scalar_point, scalar_point]))


def test_geographic_fraction_densify_keeps_chart_edges_scalar_and_bulk() -> None:
    line = gm.LineString([(0.0, 0.0), (10.0, 10.0)], crs=4326)
    scalar = line.segmentize(fraction=0.5)
    explicit_default = line.segmentize(fraction=0.5, path=None)
    bulk = gm.GeometryArray([line]).segmentize(fraction=0.5)[0]
    length_scalar = line.segmentize(1_000_000.0)
    length_bulk = gm.GeometryArray([line]).segmentize(1_000_000.0)[0]

    assert scalar.coords[1][0] == scalar.coords[1][1]
    assert list(explicit_default.coords) == list(scalar.coords)
    assert tuple(bulk.coords[1]) == tuple(scalar.coords[1])
    assert tuple(length_scalar.coords[1]) == tuple(scalar.coords[1])
    assert tuple(length_bulk.coords[1]) == tuple(scalar.coords[1])

    lrs_midpoint = line.line_interpolate(0.5, normalized=True)
    assert tuple(scalar.coords[1][:2]) == (lrs_midpoint.x, lrs_midpoint.y)

    piece_lengths = [
        gm.LineString([scalar.coords[index], scalar.coords[index + 1]], crs=4326).length
        for index in range(2)
    ]
    assert piece_lengths[0] == pytest.approx(piece_lengths[1], rel=2e-15)


def test_geographic_refinement_validates_coordinates_and_skips_missing_rows() -> None:
    invalid = gm.LineString([(0.0, 95.0), (1.0, 95.0)], crs=4326)
    for refine in (
        lambda: invalid.segmentize(1_000.0),
        lambda: invalid.segmentize(fraction=0.5),
        lambda: gm.GeometryArray([invalid]).segmentize(1_000.0),
        lambda: gm.GeometryArray([invalid]).segmentize(fraction=0.5),
    ):
        with pytest.raises(ValueError, match='invalid longitude/latitude'):
            refine()

    valid_row = gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326)
    with pytest.raises(ValueError, match='invalid longitude/latitude') as error:
        gm.GeometryArray([valid_row, invalid], crs=4326).segmentize(100_000.0)
    assert error.value.__notes__ == ['while processing array element 1']

    valid = gm.LineString([(0.0, 0.0), (10.0, 10.0)], crs=4326)
    refined = gm.GeometryArray([valid, None], crs=4326).segmentize(fraction=0.5)
    assert refined.is_missing.tolist() == [False, True]


def test_geographic_segmentize_preserves_xyzm_and_array_metadata_with_missing_row() -> (
    None
):
    valid = gm.LineString(
        [(0.0, 0.0), (10.0, 10.0)],
        z=[10.0, 30.0],
        m=[100.0, 300.0],
        crs=4326,
        epoch=2020.0,
    )
    refined = gm.GeometryArray([valid, None], crs=4326, epoch=2020.0).segmentize(
        1_000_000.0
    )

    assert refined.is_missing.tolist() == [False, True]
    assert refined.crs is not None and refined.crs.to_epsg() == 4326
    assert refined.epoch == 2020.0
    coordinates = refined[0].coords.to_nested()
    assert coordinates[0] == (0.0, 0.0, 10.0, 100.0)
    assert coordinates[1][0] == coordinates[1][1]
    # Interior source carry measures the emitted chart parameter; inverse
    # arclength placement and remeasurement need not recover exact midpoint bits.
    assert abs(coordinates[1][2] - 20.0) <= _ordinate_rounding_budget(20.0, 20.0)
    assert abs(coordinates[1][3] - 200.0) <= _ordinate_rounding_budget(200.0, 200.0)
    assert coordinates[2] == (10.0, 10.0, 30.0, 300.0)


def test_geographic_segmentize_conserves_chart_shape_and_piece_lengths() -> None:
    polygon = gm.Polygon(
        [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)],
        crs=4326,
    )
    refined = polygon.segmentize(500_000.0)

    assert gm.equals(polygon, refined)
    assert refined.area == pytest.approx(polygon.area, rel=3e-15)
    assert refined.length == pytest.approx(polygon.length, rel=3e-15)
    ring = refined.exterior.coords
    lengths = [
        gm.LineString([ring[index], ring[index + 1]], crs=4326).length
        for index in range(len(ring) - 1)
    ]
    assert max(lengths) <= 500_000.0
    for start in range(0, len(lengths), 3):
        edge_lengths = lengths[start : start + 3]
        assert max(edge_lengths) == pytest.approx(min(edge_lengths), rel=3e-15)


def test_geographic_segmentize_insertions_stay_on_sloped_chart_edge() -> None:
    line = gm.LineString([(0.0, 10.0), (120.0, 60.0)], crs=4326)
    refined = line.segmentize(1_500_000.0)
    coordinates = refined.coords

    assert coordinates[0] == (0.0, 10.0)
    assert coordinates[-1] == (120.0, 60.0)
    assert len(coordinates) > 2
    for index in range(1, len(coordinates) - 1):
        longitude, latitude = coordinates[index]
        expected_latitude = 10.0 + 50.0 * (longitude / 120.0)
        assert abs(latitude - expected_latitude) <= 4.0 * math.ulp(expected_latitude)
    assert (
        max(
            gm.LineString([coordinates[index], coordinates[index + 1]], crs=4326).length
            for index in range(len(coordinates) - 1)
        )
        <= 1_500_000.0
    )


def test_geodesic_segmentize_matches_pyproj_npts_for_scalar_array_and_fraction() -> (
    None
):
    start = (-73.0, 41.0)
    end = (-74.0, 42.0)
    line = gm.LineString([start, end], crs=4326)
    geod = pyproj.Geod(ellps='WGS84')
    _, _, route_length = geod.inv(*start, *end)
    expected = geod.npts(*start, *end, 3)
    max_length = route_length / 4.0 * (1.0 + 1e-12)

    scalar = line.segmentize(max_length, path='geodesic')
    bulk = gm.GeometryArray([line]).segmentize(max_length, path='geodesic')[0]
    fractional = line.segmentize(fraction=0.25, path='geodesic')

    for result in (scalar, bulk, fractional):
        coordinates = list(result.coords)
        assert len(coordinates) == 5
        assert coordinates[0] == start
        assert coordinates[-1] == end
        # Separate Karney builds should agree within about 22 μm in degrees.
        for point, expected_point in zip(coordinates[1:-1], expected, strict=True):
            assert tuple(point[:2]) == pytest.approx(expected_point, abs=2e-10)
        pieces = [
            geod.inv(*left[:2], *right[:2])[2] for left, right in pairwise(coordinates)
        ]
        # Inverse distances independently confirm four equal route intervals.
        assert pieces == pytest.approx([route_length / 4.0] * 4, abs=1e-7, rel=0.0)

    assert list(scalar.coords) == list(bulk.coords)
    assert list(scalar.coords) == list(fractional.coords)


def test_geodesic_segmentize_rounds_a_period_shift_once() -> None:
    base = gm.LineString([(170, 0), (190, 50)], crs=4326).segmentize(
        fraction=0.5, path='geodesic'
    )
    shifted = gm.LineString([(-190, 0), (-170, 50)], crs=4326).segmentize(
        fraction=0.5, path='geodesic'
    )
    base_midpoint = tuple(base.coords[1])
    shifted_midpoint = tuple(shifted.coords[1])
    assert base_midpoint[0] == 177.81301506408582
    # The represented base longitude minus exactly 360 is representable:
    # -0x1.6c5fbc7d51313p+7, not its adjacent lower binary64 value.
    assert shifted_midpoint[0] == -182.18698493591418
    assert shifted_midpoint[1] == base_midpoint[1]


def test_rhumb_segmentize_matches_rhumbsolve_parallel_literal_and_array() -> None:
    # GeographicLib RhumbSolve gives the endpoint for a 1,000 km due-east
    # route from (-10, 45); equal rhumb arclength on this parallel is linear
    # in longitude.
    line = gm.LineString([(-10.0, 45.0), (2.682817246984, 45.0)], crs=4326)
    rhumb_length = gm.distance(
        gm.Point(-10.0, 45.0, crs=4326),
        gm.Point(2.682817246984, 45.0, crs=4326),
        path='rhumb',
    )
    max_length = rhumb_length / 2.0 * (1.0 + 1e-12)

    by_length = line.segmentize(max_length, path='rhumb')
    by_fraction = line.segmentize(fraction=0.5, path='rhumb')
    bulk = gm.GeometryArray([line]).segmentize(fraction=0.5, path='rhumb')[0]

    expected_midpoint = (-3.658591376508, 45.0)
    for result in (by_length, by_fraction, bulk):
        assert len(result.coords) == 3
        # The RhumbSolve endpoint is rounded to 1e-12 degrees.
        assert tuple(result.coords[1]) == pytest.approx(
            expected_midpoint, abs=1e-10, rel=0.0
        )
    assert list(by_length.coords) == list(by_fraction.coords)
    assert list(by_fraction.coords) == list(bulk.coords)


@pytest.mark.parametrize(
    'path',
    [None, 'geodesic', 'rhumb'],
    ids=['chart', 'geodesic', 'rhumb'],
)
@pytest.mark.parametrize('target', ['fraction', 'max_length'])
@pytest.mark.parametrize('turns', [-2, -1, 0, 1, 2])
def test_segmentize_periodic_longitude_aliases_preserve_line_topology(
    path: Literal['geodesic', 'rhumb'] | None,
    target: Literal['fraction', 'max_length'],
    turns: int,
) -> None:
    longitude = 10.0 + 360.0 * turns
    line = gm.LineString([(longitude, 0.0), (longitude, 2.0)], crs=4326)
    source_wkb = line.to_wkb()

    refined = _segmentize_target(line, target, path)
    coordinates = list(refined.coords)

    assert gm.relate(line, refined) == '1FFF0FFF2'
    assert _xy_bits(coordinates[0]) == _xy_bits(tuple(line.coords[0]))
    assert _xy_bits(coordinates[-1]) == _xy_bits(tuple(line.coords[-1]))
    assert line.to_wkb() == source_wkb
    assert refined.length == pytest.approx(line.length, rel=1e-12, abs=1e-8)

    if turns == 0:
        assert refined.to_wkb() == _CANONICAL_MERIDIAN_WKB[(path, target)]
    else:
        assert all(
            _xy_bits(coordinate)[0] == _xy_bits((longitude, 0.0))[0]
            for coordinate in coordinates[1:-1]
        )

    if turns == 1:
        # The former canonical emission inserts longitude 10 into a line
        # whose source vertices are spelled at 370 degrees.
        canonical = gm.LineString([(10.0, 0.0), (10.0, 2.0)], crs=4326)
        canonical_refined = _segmentize_target(canonical, target, path)
        old_coordinates = [
            tuple(line.coords[0]),
            *((10.0, coordinate[1]) for coordinate in canonical_refined.coords[1:-1]),
            tuple(line.coords[-1]),
        ]
        old_canonical_emission = gm.LineString(old_coordinates, crs=4326)
        assert gm.relate(line, old_canonical_emission) != '1FFF0FFF2'


@pytest.mark.parametrize(
    ('crs', 'base_longitude', 'period', 'end_latitude'),
    [
        pytest.param(
            RADIAN_CRS,
            math.radians(10.0),
            math.tau,
            math.radians(2.0),
            id='radian',
        ),
        pytest.param(4807, 0.0, 400.0, 2.0, id='epsg-4807-grads'),
    ],
)
@pytest.mark.parametrize(
    'path', [None, 'geodesic', 'rhumb'], ids=['chart', 'geodesic', 'rhumb']
)
@pytest.mark.parametrize('target', ['fraction', 'max_length'])
@pytest.mark.parametrize('turns', [-1, 1])
def test_segmentize_periodic_aliases_in_radian_and_grad_frames(
    crs: int | str,
    base_longitude: float,
    period: float,
    end_latitude: float,
    path: Literal['geodesic', 'rhumb'] | None,
    target: Literal['fraction', 'max_length'],
    turns: int,
) -> None:
    longitude = base_longitude + period * turns
    line = gm.LineString([(longitude, 0.0), (longitude, end_latitude)], crs=crs)
    source_wkb = line.to_wkb()

    refined = _segmentize_target(line, target, path)
    coordinates = list(refined.coords)

    assert gm.relate(line, refined) == '1FFF0FFF2'
    assert _xy_bits(coordinates[0]) == _xy_bits(tuple(line.coords[0]))
    assert _xy_bits(coordinates[-1]) == _xy_bits(tuple(line.coords[-1]))
    assert line.to_wkb() == source_wkb
    assert refined.length == pytest.approx(line.length, rel=1e-12, abs=1e-8)


@pytest.mark.parametrize('turns', [-1, 0, 1])
def test_chart_segmentize_periodic_aliases_conserve_polygon_area_and_length(
    turns: int,
) -> None:
    longitude = 10.0 + 360.0 * turns
    polygon = gm.Polygon(
        [
            (longitude, 0.0),
            (longitude + 1.0, 0.0),
            (longitude + 1.0, 2.0),
            (longitude, 2.0),
            (longitude, 0.0),
        ],
        crs=4326,
    )
    source_wkb = polygon.to_wkb()
    refined = polygon.segmentize(fraction=0.5)

    assert gm.relate(polygon, refined) == '2FFF1FFF2'
    assert refined.area == pytest.approx(polygon.area, rel=1e-12, abs=1e-6)
    assert refined.length == pytest.approx(polygon.length, rel=1e-12, abs=1e-8)
    assert polygon.to_wkb() == source_wkb


@pytest.mark.parametrize('turns', [-2, -1, 0, 1, 2])
def test_planar_unit_segmentize_keeps_periodic_source_aliases(turns: int) -> None:
    longitude = 10.0 + 360.0 * turns
    line = gm.LineString([(longitude, 0.0), (longitude, 2.0)], crs=4326)
    refined = line.segmentize(0.5, unit='planar')
    coordinates = list(refined.coords)

    assert gm.relate(line, refined) == '1FFF0FFF2'
    assert _xy_bits(coordinates[0]) == _xy_bits(tuple(line.coords[0]))
    assert _xy_bits(coordinates[-1]) == _xy_bits(tuple(line.coords[-1]))


@pytest.mark.parametrize(
    ('path', 'unit'),
    [
        pytest.param('geodesic', 'meters', id='geodesic-meters'),
        pytest.param('rhumb', 'meters', id='rhumb-meters'),
        pytest.param('rhumb', 'planar', id='rhumb-planar'),
    ],
)
@pytest.mark.parametrize('array', [False, True], ids=['scalar', 'array'])
def test_segmentize_explicit_path_rejects_unit(
    path: str,
    unit: str,
    array: bool,
) -> None:
    geographic = gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326)
    target = gm.GeometryArray([geographic]) if array else geographic

    with pytest.raises(
        gm.GeometryError,
        match='segmentize does not accept unit when path is set; route distances are meters',
    ):
        target.segmentize(100_000.0, path=path, unit=unit)


def test_segmentize_path_unit_error_precedes_fraction_unit_error() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326)
    with pytest.raises(
        gm.GeometryError,
        match='segmentize does not accept unit when path is set; route distances are meters',
    ):
        line.segmentize(fraction=0.5, path='rhumb', unit='meters')


@pytest.mark.parametrize(
    'crs',
    [None, 3857],
    ids=['crs-free', 'projected'],
)
def test_geodesic_segmentize_is_planar_without_geographic_crs(
    crs: int | None,
) -> None:
    line = gm.LineString([(0.0, 0.0), (10.0, 10.0)], crs=crs)
    planar = line.segmentize(2.0)
    geodesic = line.segmentize(2.0, path='geodesic')
    array = gm.GeometryArray([line])

    assert list(geodesic.coords) == list(planar.coords)
    assert list(array.segmentize(2.0, path='geodesic')[0].coords) == list(planar.coords)


@pytest.mark.parametrize(
    'crs',
    [None, 3857],
    ids=['crs-free', 'projected'],
)
@pytest.mark.parametrize('array', [False, True], ids=['scalar', 'array'])
def test_rhumb_segmentize_requires_geographic_crs(
    crs: int | None,
    array: bool,
) -> None:
    line = gm.LineString([(0.0, 0.0), (10.0, 10.0)], crs=crs)
    target = gm.GeometryArray([line]) if array else line

    with pytest.raises(gm.CRSError, match=r"path='rhumb'.*requires a geographic CRS"):
        target.segmentize(2.0, path='rhumb')


def test_geodesic_segmentize_matches_near_antipodal_unique_route() -> None:
    start = (0.0, 10.0)
    end = (179.999, -9.999)
    line = gm.LineString([start, end], crs=4326)
    expected = pyproj.Geod(ellps='WGS84').npts(*start, *end, 1)[0]

    midpoint = line.segmentize(fraction=0.5, path='geodesic').coords[1]
    # Near-antipodal inverse conditioning permits a millimetre-scale coordinate
    # delta while still pinning the selected shortest route.
    assert tuple(midpoint) == pytest.approx(expected, abs=2e-8, rel=0.0)


def test_geodesic_segmentize_rejects_non_unique_antipodal_route() -> None:
    line = gm.LineString([(0.0, 0.0), (180.0, 0.0)], crs=4326)

    with pytest.raises(gm.CRSError, match='multiple shortest geodesics'):
        line.segmentize(11_000_000.0, path='geodesic')


def test_array_geodesic_segmentize_keeps_ambiguous_route_row_note() -> None:
    valid = gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326)
    ambiguous = gm.LineString([(0.0, 0.0), (180.0, 0.0)], crs=4326)
    geometries = gm.GeometryArray([valid, ambiguous], crs=4326)

    with pytest.raises(gm.CRSError, match='multiple shortest geodesics') as error:
        geometries.segmentize(11_000_000.0, path='geodesic')
    assert error.value.__notes__ == ['while processing array element 1']


def test_geodesic_segmentize_length_excess_scales_with_piece_size() -> None:
    start = gm.Point(0.0, 60.0, crs=4326)
    end = start.destination(90.0, 1_000_000.0)
    line = gm.LineString([(start.x, start.y), (end.x, end.y)], crs=4326)
    route_length = gm.distance(start, end)

    coarse = line.segmentize(2_000.0, path='geodesic')
    fine = line.segmentize(1_000.0, path='geodesic')
    coarse_excess = coarse.length - route_length
    fine_excess = fine.length - route_length

    assert len(fine.coords) >= 1_001
    assert coarse_excess >= -1e-5
    assert fine_excess >= -1e-5
    # The chart-chord excess is second-order in piece size; allow rounding slack.
    assert fine_excess <= max(coarse_excess, 0.0) * 0.26 + 1e-5


def test_geographic_frechet_densify_uses_segmentize_chart_placement() -> None:
    left = gm.LineString([(0.0, 0.0), (10.0, 10.0)], crs=4326)
    chart_midpoint = left.line_interpolate(0.5, normalized=True)
    right = gm.LineString(
        [(0.0, 0.0), (chart_midpoint.x, chart_midpoint.y), (10.0, 10.0)],
        crs=4326,
    )
    left_points = list(left.segmentize(fraction=0.5).coords)
    right_points = list(right.segmentize(fraction=0.5).coords)

    previous = [math.inf] * len(right_points)
    for left_index, left_xy in enumerate(left_points):
        current = [math.inf] * len(right_points)
        for right_index, right_xy in enumerate(right_points):
            distance = gm.distance(
                gm.Point(*left_xy, crs=4326),
                gm.Point(*right_xy, crs=4326),
            )
            if left_index == 0 and right_index == 0:
                current[right_index] = distance
            else:
                current[right_index] = max(
                    distance,
                    min(
                        previous[right_index],
                        current[right_index - 1] if right_index else math.inf,
                        previous[right_index - 1] if right_index else math.inf,
                    ),
                )
        previous = current

    assert gm.frechet_distance(left, right, densify=0.5) == pytest.approx(
        previous[-1], abs=1e-8, rel=0.0
    )


def test_fraction_densify_keeps_step_count_independent_of_shared_budget() -> None:
    line = gm.LineString([(0.0, 0.0), (10.0, 10.0)], crs=4326)
    collection = gm.GeometryCollection([line, line])

    refined = collection.segmentize(fraction=0.5)
    assert np.asarray(refined.coords, dtype=np.float64).shape == (6, 2)


def test_geographic_chart_lrs_matches_independent_parallel_oracle() -> None:
    line = gm.LineString(
        [(0.0, 60.0), (10.0, 60.0)],
        z=[10.0, 50.0],
        m=[100.0, 300.0],
        crs=4326,
    )
    length = _wgs84_parallel_length(60.0, 10.0)
    assert line.length == pytest.approx(
        length, rel=0.0, abs=_length_oracle_budget(length)
    )

    midpoint = line.line_interpolate(length / 2.0)
    _assert_xy_within_budget(
        (midpoint.x, midpoint.y), (5.0, 60.0), _PARALLEL_60_XY_BUDGETS_NATIVE[1]
    )
    assert abs(midpoint.z - 30.0) <= _ordinate_rounding_budget(30.0, 40.0)
    assert abs(midpoint.m - 200.0) <= _ordinate_rounding_budget(200.0, 200.0)
    located = line.line_locate(gm.Point(5.0, 60.0, crs=4326))
    _assert_station_within_budget(
        located, length / 2.0, _PARALLEL_60_STATION_BUDGETS_M[1]
    )
    _assert_station_within_budget(
        line.line_locate(midpoint),
        length / 2.0,
        _PARALLEL_60_STATION_BUDGETS_M[1],
    )

    by_measure = line.line_interpolate(200.0, basis='m')
    _assert_xy_within_budget(
        (by_measure.x, by_measure.y), (5.0, 60.0), _PARALLEL_60_XY_BUDGETS_NATIVE[1]
    )
    measured_substring = line.line_substring(150.0, 250.0, basis='m')
    for actual, expected, xy_budget in zip(
        measured_substring.coords,
        [(2.5, 60.0), (7.5, 60.0)],
        [_PARALLEL_60_XY_BUDGETS_NATIVE[0], _PARALLEL_60_XY_BUDGETS_NATIVE[2]],
        strict=True,
    ):
        _assert_xy_within_budget(tuple(actual[:2]), expected, xy_budget)

    substring = line.line_substring(length / 4.0, 3.0 * length / 4.0)
    for actual, expected, xy_budget, z_budget, m_budget in zip(
        substring.coords.to_nested(),
        [(2.5, 60.0, 20.0, 150.0), (7.5, 60.0, 40.0, 250.0)],
        [_PARALLEL_60_XY_BUDGETS_NATIVE[0], _PARALLEL_60_XY_BUDGETS_NATIVE[2]],
        [
            _ordinate_rounding_budget(20.0, 40.0),
            _ordinate_rounding_budget(40.0, 40.0),
        ],
        [
            _ordinate_rounding_budget(150.0, 200.0),
            _ordinate_rounding_budget(250.0, 200.0),
        ],
        strict=True,
    ):
        _assert_xy_within_budget(tuple(actual[:2]), tuple(expected[:2]), xy_budget)
        assert abs(actual[2] - expected[2]) <= z_budget
        assert abs(actual[3] - expected[3]) <= m_budget
    assert substring.length == pytest.approx(
        length / 2.0,
        rel=0.0,
        abs=_PARALLEL_60_SUBSTRING_LENGTH_BUDGETS_M[(0.25, 0.75)],
    )

    rows = gm.GeometryArray([line, line])
    points = rows.line_interpolate([length / 4.0, 3.0 * length / 4.0])
    for point, expected, xy_budget in zip(
        points,
        [(2.5, 60.0), (7.5, 60.0)],
        [_PARALLEL_60_XY_BUDGETS_NATIVE[0], _PARALLEL_60_XY_BUDGETS_NATIVE[2]],
        strict=True,
    ):
        _assert_xy_within_budget((point.x, point.y), expected, xy_budget)
    for actual, fraction, budget in zip(
        rows.line_locate(points),
        (0.25, 0.75),
        (_PARALLEL_60_STATION_BUDGETS_M[0], _PARALLEL_60_STATION_BUDGETS_M[2]),
        strict=True,
    ):
        _assert_station_within_budget(actual, fraction * length, budget)
    substrings = rows.line_substring(
        [length / 4.0, length / 2.0],
        [3.0 * length / 4.0, 9.0 * length / 10.0],
    )
    for substring, fractions in zip(
        substrings, ((0.25, 0.75), (0.5, 0.9)), strict=True
    ):
        assert substring.length == pytest.approx(
            (fractions[1] - fractions[0]) * length,
            rel=0.0,
            abs=_PARALLEL_60_SUBSTRING_LENGTH_BUDGETS_M[fractions],
        )


def test_geographic_chart_lrs_matches_independent_sloped_edge_values() -> None:
    # Literals are from 60-digit mpmath quadrature/inversion of ellipsoidal
    # chart-edge speed for WGS 84, not from the implementation under test.
    line = gm.LineString([(0.0, 10.0), (120.0, 60.0)], crs=4326)
    expected_length = 12_009_721.500859138
    middle_distance = 6_004_860.750429569
    expected_midpoint = (53.07463035002673, 32.11442931251114)
    assert line.length == pytest.approx(
        expected_length, rel=0.0, abs=_length_oracle_budget(expected_length)
    )

    midpoint = line.line_interpolate(middle_distance)
    _assert_xy_within_budget(
        (midpoint.x, midpoint.y),
        expected_midpoint,
        _SLOPED_XY_BUDGETS_NATIVE[1],
    )
    normalized_midpoint = line.line_interpolate(0.5, normalized=True, unit='meters')
    default_normalized_midpoint = line.line_interpolate(0.5, normalized=True)
    counted_midpoint = line.line_interpolate(count=3, unit='meters')[1]
    default_counted_midpoint = line.line_interpolate(count=3)[1]
    for actual in (
        normalized_midpoint,
        default_normalized_midpoint,
        counted_midpoint,
        default_counted_midpoint,
    ):
        _assert_xy_within_budget(
            (actual.x, actual.y),
            expected_midpoint,
            _SLOPED_XY_BUDGETS_NATIVE[1],
        )
    _assert_station_within_budget(
        line.line_locate(midpoint),
        middle_distance,
        _SLOPED_STATION_BUDGETS_M[1],
    )

    start_distance = 3_002_430.3752147846
    end_distance = 9_007_291.125644354
    substring = line.line_substring(start_distance, end_distance)
    expected_coordinates = [
        (25.725961510550192, 20.719150629395913),
        (83.51027086740391, 44.79594619475163),
    ]
    for actual, expected, xy_budget in zip(
        substring.coords,
        expected_coordinates,
        (_SLOPED_XY_BUDGETS_NATIVE[0], _SLOPED_XY_BUDGETS_NATIVE[2]),
        strict=True,
    ):
        _assert_xy_within_budget(tuple(actual[:2]), expected, xy_budget)
    normalized_substring = line.line_substring(
        0.25, 0.75, normalized=True, unit='meters'
    )
    default_normalized_substring = line.line_substring(0.25, 0.75, normalized=True)
    for route in (normalized_substring, default_normalized_substring):
        for actual, expected, xy_budget in zip(
            route.coords,
            expected_coordinates,
            (_SLOPED_XY_BUDGETS_NATIVE[0], _SLOPED_XY_BUDGETS_NATIVE[2]),
            strict=True,
        ):
            _assert_xy_within_budget(tuple(actual[:2]), expected, xy_budget)
    assert substring.length == pytest.approx(
        end_distance - start_distance,
        rel=0.0,
        abs=_SLOPED_SUBSTRING_LENGTH_BUDGET_M,
    )
    oracle_point = gm.Point(*expected_midpoint, crs=4326)
    for normalized_station in (
        line.line_locate(oracle_point, normalized=True),
        line.line_locate(oracle_point, normalized=True, unit='meters'),
    ):
        _assert_station_within_budget(
            normalized_station * expected_length,
            middle_distance,
            _SLOPED_STATION_BUDGETS_M[1],
        )


def test_geographic_chart_lrs_preserves_lifted_370_degree_meridian() -> None:
    line = gm.LineString(
        [(370.0, -10.0), (370.0, 10.0)],
        z=[0.0, 40.0],
        m=[100.0, 300.0],
        crs=4326,
    )
    source = line.to_wkb()
    midpoint = line.line_interpolate(0.5, normalized=True)
    assert (midpoint.x, midpoint.y, midpoint.z, midpoint.m) == (10.0, 0.0, 20.0, 200.0)

    # M and Z are linear in ellipsoidal arclength, not latitude. The independent
    # meridian-arc oracle above supplies these X/Y expectations; the four-ULP
    # latitude allowance remains the same as the lifted M-substring assertion.
    expected_latitudes = (
        -_WGS84_MERIDIAN_HALF_0_TO_10_LATITUDE_DEG,
        _WGS84_MERIDIAN_HALF_0_TO_10_LATITUDE_DEG,
    )
    for fraction, expected_y, expected_z, expected_m in zip(
        (0.25, 0.75), expected_latitudes, (10.0, 30.0), (150.0, 250.0), strict=True
    ):
        for point in (
            line.line_interpolate(fraction, normalized=True),
            line.line_interpolate(expected_m, basis='m'),
        ):
            assert point.x == 10.0
            assert abs(point.y - expected_y) <= 4.0 * math.ulp(expected_y)
            assert abs(point.z - expected_z) <= _ordinate_rounding_budget(
                expected_z, 40.0
            )
            assert abs(point.m - expected_m) <= _ordinate_rounding_budget(
                expected_m, 200.0
            )
        located_m = line.line_locate(gm.Point(10.0, expected_y, crs=4326), basis='m')
        assert abs(located_m - expected_m) <= _ordinate_rounding_budget(
            expected_m, 200.0
        )

    substring = line.line_substring(0.25, 0.75, normalized=True)
    coordinates = list(substring.coords)
    assert [point[0] for point in coordinates] == [370.0, 370.0]
    for point, expected_y, expected in zip(
        coordinates, expected_latitudes, [(10.0, 150.0), (30.0, 250.0)], strict=True
    ):
        assert abs(point[1] - expected_y) <= 4.0 * math.ulp(expected_y)
        assert abs(point[2] - expected[0]) <= _ordinate_rounding_budget(
            expected[0], 40.0
        )
        assert abs(point[3] - expected[1]) <= _ordinate_rounding_budget(
            expected[1], 200.0
        )

    whole = line.line_substring(0.0, 1.0, normalized=True)
    assert whole.to_wkb() == source
    measured_midpoint = line.line_interpolate(200.0, basis='m')
    assert (measured_midpoint.x, measured_midpoint.y) == (10.0, 0.0)
    measured_substring = line.line_substring(150.0, 250.0, basis='m')
    for point, expected_y, expected_z, expected_m in zip(
        measured_substring.coords,
        expected_latitudes,
        (10.0, 30.0),
        (150.0, 250.0),
        strict=True,
    ):
        assert abs(point[0] - 370.0) <= 4.0 * math.ulp(370.0)
        assert abs(point[1] - expected_y) <= 4.0 * math.ulp(expected_y)
        assert abs(point[2] - expected_z) <= _ordinate_rounding_budget(expected_z, 40.0)
        assert abs(point[3] - expected_m) <= _ordinate_rounding_budget(
            expected_m, 200.0
        )

    unmeasured = gm.LineString(
        [
            (370.0, -10.0),
            (370.0, expected_latitudes[0]),
            (370.0, expected_latitudes[1]),
            (370.0, 10.0),
        ],
        z=[0.0, 10.0, 30.0, 40.0],
        crs=4326,
    )
    stationed = unmeasured.interpolate_m(100.0, 300.0)
    for point, original, expected_m in zip(
        stationed.coords, unmeasured.coords, (100.0, 150.0, 250.0, 300.0), strict=True
    ):
        assert tuple(point[:3]) == tuple(original)
        assert abs(point[3] - expected_m) <= _ordinate_rounding_budget(
            expected_m, 200.0
        )


def test_geographic_chart_lrs_substring_keeps_the_seam_lift() -> None:
    line = gm.LineString(
        [(179.0, 10.0), (-179.0, 10.0)],
        z=[0.0, 10.0],
        m=[0.0, 100.0],
        crs=4326,
    )
    source = line.to_wkb()
    midpoint = line.line_interpolate(0.5, normalized=True)
    assert (midpoint.x, midpoint.y, midpoint.z, midpoint.m) == (
        -180.0,
        10.0,
        5.0,
        50.0,
    )

    substring = line.line_substring(0.25, 0.75, normalized=True)
    for point, expected, xy_budget, z_budget, m_budget in zip(
        substring.coords,
        [(179.5, 10.0, 2.5, 25.0), (180.5, 10.0, 7.5, 75.0)],
        [_SEAM_XY_BUDGETS_NATIVE[0], _SEAM_XY_BUDGETS_NATIVE[2]],
        [
            _ordinate_rounding_budget(2.5, 10.0),
            _ordinate_rounding_budget(7.5, 10.0),
        ],
        [
            _ordinate_rounding_budget(25.0, 100.0),
            _ordinate_rounding_budget(75.0, 100.0),
        ],
        strict=True,
    ):
        _assert_xy_within_budget(tuple(point[:2]), tuple(expected[:2]), xy_budget)
        assert abs(point[2] - expected[2]) <= z_budget
        assert abs(point[3] - expected[3]) <= m_budget
    assert substring.length == pytest.approx(
        _wgs84_parallel_length(10.0, 1.0),
        rel=0.0,
        abs=_SEAM_SUBSTRING_LENGTH_BUDGET_M,
    )
    _assert_station_within_budget(
        line.line_locate(midpoint, normalized=True) * _wgs84_parallel_length(10.0, 2.0),
        _wgs84_parallel_length(10.0, 2.0) / 2.0,
        _SEAM_STATION_BUDGETS_M[1],
    )
    assert line.line_substring(0.0, 1.0, normalized=True).to_wkb() == source


@pytest.mark.parametrize(
    ('crs', 'period', 'units_per_degree', 'station_budgets_m'),
    [
        pytest.param(
            4326,
            360.0,
            1.0,
            (
                2.609226359849549e-8,
                1.34722307030883e-8,
                3.48799730850791e-9,
                1.34722307030883e-8,
                2.609226359849549e-8,
            ),
            id='degrees',
        ),
        pytest.param(
            GRAD_CRS,
            400.0,
            200.0 / 180.0,
            (
                2.40381498902241e-8,
                1.31546637086853e-8,
                6.0328495415763e-9,
                1.31546637086853e-8,
                2.40381498902241e-8,
            ),
            id='grads',
        ),
        pytest.param(
            RADIAN_CRS,
            math.tau,
            math.pi / 180.0,
            (
                2.39125885980427e-8,
                1.30865846988912e-8,
                6.00238514590819e-9,
                1.30865846988912e-8,
                2.39125885980427e-8,
            ),
            id='radians',
        ),
    ],
)
@pytest.mark.parametrize('turns', [-2, -1, 0, 1, 2])
def test_geographic_chart_lrs_preserves_rigid_native_longitude_aliases(
    crs: int | str,
    period: float,
    units_per_degree: float,
    turns: int,
    station_budgets_m: tuple[float, float, float, float, float],
) -> None:
    latitude = 60.0 * units_per_degree
    start_longitude = period * turns + 10.0 * units_per_degree
    end_longitude = start_longitude + 2.0 * units_per_degree
    longitude_span = end_longitude - start_longitude
    line = gm.LineString(
        [(start_longitude, latitude), (end_longitude, latitude)],
        z=[10.0, 30.0],
        m=[100.0, 200.0],
        crs=crs,
    )
    source = line.to_wkb()
    # Use the exact represented source delta (after forming the rigid alias),
    # with the independent ellipsoidal parallel formula.
    latitude_degrees = latitude / units_per_degree
    longitude_degrees = longitude_span / units_per_degree
    expected_length = _wgs84_parallel_length(latitude_degrees, longitude_degrees)
    assert line.length == pytest.approx(
        expected_length, rel=0.0, abs=_length_oracle_budget(expected_length)
    )

    midpoint = line.line_interpolate(0.5, normalized=True)
    lifted_midpoint = start_longitude + 0.5 * longitude_span
    expected_midpoint_x = (lifted_midpoint + period / 2.0) % period - period / 2.0
    _assert_xy_within_budget(
        (midpoint.x, midpoint.y),
        (expected_midpoint_x, latitude),
        _parallel_alias_coordinate_budget(
            start_longitude, expected_midpoint_x, latitude, longitude_span
        ),
    )
    assert abs(midpoint.z - 20.0) <= _ordinate_rounding_budget(20.0, 20.0)
    assert abs(midpoint.m - 150.0) <= _ordinate_rounding_budget(150.0, 100.0)
    _assert_station_within_budget(
        line.line_locate(midpoint, normalized=True),
        expected_length / 2.0,
        station_budgets_m[turns + 2],
        total_length=expected_length,
    )

    substring = line.line_substring(0.25, 0.75, normalized=True)
    inserted = [tuple(point) for point in substring.coords]
    for actual, fraction, expected_z, expected_m in zip(
        inserted,
        (0.25, 0.75),
        (15.0, 25.0),
        (125.0, 175.0),
        strict=True,
    ):
        expected_longitude = start_longitude + fraction * longitude_span
        _assert_xy_within_budget(
            tuple(actual[:2]),
            (expected_longitude, latitude),
            _parallel_alias_coordinate_budget(
                start_longitude, expected_longitude, latitude, longitude_span
            ),
        )
        assert abs(actual[2] - expected_z) <= _ordinate_rounding_budget(
            expected_z, 20.0
        )
        assert abs(actual[3] - expected_m) <= _ordinate_rounding_budget(
            expected_m, 100.0
        )
    assert line.line_substring(0.0, 1.0, normalized=True).to_wkb() == source
