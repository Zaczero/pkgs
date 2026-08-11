from __future__ import annotations

import math
import random
import struct
from fractions import Fraction

import gometry as gm
import numpy as np
import pytest

MU = 2.0**-1074
BASE_LENGTH = 2.0**-20
TRANSITION_LEFT = 2**19 - 1
TRANSITION_RIGHT = 2**19


def _fraction(value: float) -> Fraction:
    """Exact value of the stored binary64, never of its source spelling."""
    return Fraction(value)


def _rounded_sqrt(value: Fraction) -> float:
    """Correctly rounded binary64 square root of a non-negative fraction."""
    if value == 0:
        return 0.0
    low_bits = 0
    upper_bits = 0x7FEF_FFFF_FFFF_FFFF
    while low_bits < upper_bits:
        middle = low_bits + (upper_bits - low_bits + 1) // 2
        candidate = struct.unpack('>d', struct.pack('>Q', middle))[0]
        if _fraction(candidate) ** 2 <= value:
            low_bits = middle
        else:
            upper_bits = middle - 1
    lower = struct.unpack('>d', struct.pack('>Q', low_bits))[0]
    if _fraction(lower) ** 2 == value or low_bits == 0x7FEF_FFFF_FFFF_FFFF:
        return lower
    upper = struct.unpack('>d', struct.pack('>Q', low_bits + 1))[0]
    midpoint = (_fraction(lower) + _fraction(upper)) / 2
    midpoint_square = midpoint * midpoint
    if value < midpoint_square or (value == midpoint_square and low_bits & 1 == 0):
        return lower
    return upper


def _exact_projection(
    start: tuple[float, float],
    end: tuple[float, float],
    point: tuple[float, float],
) -> tuple[Fraction, tuple[float, float], float]:
    sx, sy = map(_fraction, start)
    ex, ey = map(_fraction, end)
    px, py = map(_fraction, point)
    dx, dy = ex - sx, ey - sy
    qx, qy = px - sx, py - sy
    denominator = dx * dx + dy * dy
    numerator = qx * dx + qy * dy
    fraction = min(max(numerator / denominator, Fraction(0)), Fraction(1))
    foot_x = sx + fraction * dx
    foot_y = sy + fraction * dy
    distance_squared = (px - foot_x) ** 2 + (py - foot_y) ** 2
    return fraction, (float(foot_x), float(foot_y)), _rounded_sqrt(distance_squared)


def _coords(point: gm.Point) -> tuple[float, float]:
    return tuple(point.coords[0])


def _exact_distance_squared(
    point: tuple[float, float], witness: tuple[float, float]
) -> Fraction:
    px, py = map(_fraction, point)
    wx, wy = map(_fraction, witness)
    return (px - wx) ** 2 + (py - wy) ** 2


@pytest.mark.parametrize('base', [0.0, 1.0e3, 5.0e5, 5.2e6, 1.0e9, 1.0e12])
@pytest.mark.parametrize('distance', [1.0e-3, 1.0, 1.0e3])
def test_projection_metric_retains_small_distance_across_coordinate_scales(
    base: float, distance: float
) -> None:
    start = (float(base), float(base))
    end = (float(base + 4.0 * distance), float(base + 3.0 * distance))
    point_xy = (
        float(base + 0.88 * distance),
        float(base + 1.91 * distance),
    )
    _, _, expected = _exact_projection(start, end, point_xy)
    point = gm.Point(*point_xy)

    actual = gm.distance(point, gm.LineString([start, end]))

    assert abs(actual - expected) / expected <= 3.0e-16


def test_utm_polygon_distance_uses_segment_local_residual() -> None:
    point_xy = (500000.5, 5200000.5)
    exterior = [
        (500008.64, 5200000.0),
        (500000.0, 5200006.8),
        (499992.64, 5200000.0),
        (500000.0, 5199990.8),
    ]
    hole = [
        (500000.6703304128, 5199997.833004009),
        (499998.36726275895, 5199999.494935186),
        (499999.48824958614, 5200001.654349965),
        (500002.1886087155, 5200000.677016012),
    ]
    _, _, expected = _exact_projection(hole[2], hole[3], point_xy)

    actual = gm.distance(gm.Point(*point_xy), gm.Polygon(exterior, [hole]))

    assert actual == expected == 0.7411226985789381


def test_indexed_facet_metric_retains_small_distance_at_large_base() -> None:
    base = 1.0e12
    distance = 1.0e-3
    points = [
        (float(base + index * 4.0 * distance), float(base + index * 3.0 * distance))
        for index in range(66)
    ]
    point_xy = (
        float(base + 0.88 * distance),
        float(base + 1.91 * distance),
    )
    _, _, expected = _exact_projection(points[0], points[1], point_xy)

    actual = gm.distance(gm.Point(*point_xy), gm.LineString(points))

    assert abs(actual - expected) / expected <= 3.0e-16


def _nonmonotone_witness_cases(
    count: int = 32,
) -> list[tuple[tuple[float, float], tuple[float, float]]]:
    """Generate ordinary near-endpoint projections whose rounded keys invert."""
    rng = random.Random(7)
    cases: list[tuple[tuple[float, float], tuple[float, float]]] = []
    while len(cases) < count:
        dx = rng.uniform(-4.0, 4.0)
        dy = rng.uniform(-4.0, 4.0)
        if dx == 0.0 and dy == 0.0:
            continue
        perpendicular_scale = rng.uniform(0.2, 2.0)
        along = math.ldexp(rng.random(), -rng.randint(30, 44))
        px = -dy * perpendicular_scale + dx * along
        py = dx * perpendicular_scale + dy * along
        fdx, fdy, fpx, fpy = map(_fraction, (dx, dy, px, py))
        denominator = fdx * fdx + fdy * fdy
        fraction = (fpx * fdx + fpy * fdy) / denominator
        if not 0 < fraction < 1:
            continue
        foot = (float(fraction * fdx), float(fraction * fdy))
        point = (px, py)
        if not _exact_distance_squared(point, foot) < _exact_distance_squared(
            point, (0.0, 0.0)
        ):
            continue
        foot_key = math.sqrt((px - foot[0]) ** 2 + (py - foot[1]) ** 2)
        endpoint_key = math.sqrt(px * px + py * py)
        if foot_key >= endpoint_key:
            cases.append(((dx, dy), point))
    return cases


@pytest.mark.parametrize('reverse', [False, True])
def test_projection_underflow_metric_and_witness_family(reverse: bool) -> None:
    n = TRANSITION_LEFT
    endpoints = [(0.0, 0.0), (BASE_LENGTH, BASE_LENGTH)]
    if reverse:
        endpoints.reverse()
    point_xy = (n * MU, (n + 1) * MU)
    line = gm.LineString(endpoints)
    point = gm.Point(*point_xy)
    fraction, foot, expected_distance = _exact_projection(
        endpoints[0], endpoints[1], point_xy
    )

    assert 0 < fraction < 1
    assert expected_distance == MU
    for left, right, line_is_left in ((line, point, True), (point, line, False)):
        assert gm.distance(left, right) == expected_distance
        assert gm.dwithin(left, right, MU) is True
        nearest = gm.nearest_points(left, right)
        assert _coords(nearest[0 if line_is_left else 1]) == foot
        assert _coords(nearest[1 if line_is_left else 0]) == point_xy
        shortest = gm.shortest_line(left, right)
        expected = (foot, point_xy) if line_is_left else (point_xy, foot)
        assert tuple(map(tuple, shortest.coords)) == expected

    located = line.line_locate(point)
    expected_located = float(_fraction(line.length) * fraction)
    assert located == expected_located
    if reverse:
        # The exact residual from the end is far below one ulp of total length;
        # the correctly rounded absolute offset is therefore the total.
        assert expected_located == line.length


def test_projection_underflow_array_family_and_direction() -> None:
    n = TRANSITION_LEFT
    point_xy = (n * MU, (n + 1) * MU)
    forward = gm.LineString([(0.0, 0.0), (BASE_LENGTH, BASE_LENGTH)])
    reverse = gm.LineString([(BASE_LENGTH, BASE_LENGTH), (0.0, 0.0)])
    lines = gm.GeometryArray([forward, reverse])
    points = gm.GeometryArray([gm.Point(*point_xy), gm.Point(*point_xy)])
    forward_fraction, foot, _ = _exact_projection(
        (0.0, 0.0), (BASE_LENGTH, BASE_LENGTH), point_xy
    )
    reverse_fraction, _, _ = _exact_projection(
        (BASE_LENGTH, BASE_LENGTH), (0.0, 0.0), point_xy
    )

    np.testing.assert_array_equal(gm.distance(lines, points), [MU, MU])
    np.testing.assert_array_equal(gm.distance(points, lines), [MU, MU])
    np.testing.assert_array_equal(gm.dwithin(lines, points, MU), [True, True])
    np.testing.assert_array_equal(gm.dwithin(points, lines, MU), [True, True])
    for left, right, line_is_left in ((lines, points, True), (points, lines, False)):
        nearest = gm.nearest_points(left, right)
        for index in range(2):
            assert _coords(nearest[0 if line_is_left else 1][index]) == foot
            assert _coords(nearest[1 if line_is_left else 0][index]) == point_xy
        shortest = gm.shortest_line(left, right)
        expected = (foot, point_xy) if line_is_left else (point_xy, foot)
        assert [tuple(map(tuple, value.coords)) for value in shortest] == [
            expected,
            expected,
        ]

    located = lines.line_locate(points)
    expected_located = np.array([
        float(_fraction(forward.length) * forward_fraction),
        float(_fraction(reverse.length) * reverse_fraction),
    ])
    np.testing.assert_array_equal(located, expected_located)
    assert expected_located[1] == reverse.length


def test_exact_ratio_survives_when_binary64_parameter_rounds_to_zero() -> None:
    length = 2.0**1023
    foot = (2.0**-52, 0.0)
    point_xy = (foot[0], 2.0**-53)
    line = gm.LineString([(0.0, 0.0), (length, 0.0)])
    point = gm.Point(*point_xy)

    # The exact parameter is 2**-1075, the tie between +0 and the minimum
    # subnormal. Ties-to-even rounds it to zero, so only an endpoint-local
    # exact ratio can still reconstruct this representable interior foot.
    fraction, expected_foot, expected_distance = _exact_projection(
        (0.0, 0.0), (length, 0.0), point_xy
    )
    assert fraction == Fraction(1, 2**1075)
    assert float(fraction) == 0.0
    assert expected_foot == foot
    assert gm.distance(line, point) == expected_distance == point_xy[1]
    assert gm.dwithin(line, point, expected_distance) is True
    assert _coords(gm.nearest_points(line, point)[0]) == foot
    assert tuple(map(tuple, gm.shortest_line(line, point).coords)) == (foot, point_xy)
    assert line.line_locate(point) == foot[0]


def _assert_bvh_witness(line: gm.LineString) -> None:
    x = 2.0**-578
    point_xy = (x, 1.0)
    point = gm.Point(*point_xy)
    expected_foot = (x, 0.0)
    assert gm.distance(line, point) == 1.0
    for left, right, line_is_left in ((line, point, True), (point, line, False)):
        nearest = gm.nearest_points(left, right)
        assert _coords(nearest[0 if line_is_left else 1]) == expected_foot
        shortest = gm.shortest_line(left, right)
        expected = (
            (expected_foot, point_xy) if line_is_left else (point_xy, expected_foot)
        )
        assert tuple(map(tuple, shortest.coords)) == expected


def test_exact_ratio_survives_bvh_equal_squared_key() -> None:
    _assert_bvh_witness(gm.LineString([(0.0, 0.0), (1.0e150, 0.0)]))


def test_exact_ratio_survives_bvh_nonzero_damaged_quotient() -> None:
    line = gm.LineString([(index * 1.0e150 / 200, 0.0) for index in range(201)])
    point = gm.Point(2.0**-578, 1.0)
    expected_foot = (2.0**-578, 0.0)
    witnesses = gm.nearest_points(point, line)
    assert _coords(witnesses[1]) == expected_foot
    assert tuple(map(tuple, gm.shortest_line(point, line).coords)) == (
        tuple(point.coords[0]),
        expected_foot,
    )
    array_witnesses = gm.nearest_points(point, gm.GeometryArray([line]))
    assert _coords(array_witnesses[1][0]) == expected_foot


def test_cancellation_damaged_projection_declines_to_exact_ratio() -> None:
    dx = float.fromhex('0x1.e72e44270581dp-1')
    dy = float.fromhex('0x1.f061e34d6ace7p+0')
    point_xy = (
        float.fromhex('0x1.a8bb03fe8173fp+0'),
        float.fromhex('-0x1.a0db6c63eb3d7p-1'),
    )
    line = gm.LineString([(0.0, 0.0), (dx, dy)])
    point = gm.Point(*point_xy)
    fraction, foot, expected_distance = _exact_projection(
        (0.0, 0.0), (dx, dy), point_xy
    )
    assert float(fraction).hex() == '0x1.006b17248e1e8p-49'
    assert tuple(value.hex() for value in foot) == (
        '0x1.e7fa10883ae7ep-50',
        '0x1.f131891718eb6p-49',
    )
    for left, right, line_is_left in ((line, point, True), (point, line, False)):
        assert gm.distance(left, right) == expected_distance
        nearest = gm.nearest_points(left, right)
        assert _coords(nearest[0 if line_is_left else 1]) == foot
        shortest = gm.shortest_line(left, right)
        expected = (foot, point_xy) if line_is_left else (point_xy, foot)
        assert tuple(map(tuple, shortest.coords)) == expected


def test_certified_ratio_retains_exact_scale_and_ordinate_provenance() -> None:
    dx = float.fromhex('0x1.38743a0f38a7ep+0')
    dy = float.fromhex('0x1.6b5af8d532efdp-1')
    point_xy = (
        float.fromhex('0x1.7ba998e1992dep+0'),
        float.fromhex('-0x1.467a0a9ab5799p+1'),
    )
    line = gm.LineString([(0.0, 0.0), (dx, dy)])
    point = gm.Point(*point_xy)
    fraction, foot, _ = _exact_projection((0.0, 0.0), (dx, dy), point_xy)
    expected_located = float(_fraction(line.length) * fraction)
    assert float(fraction).hex() == '0x1.3bdf76d46f424p-47'
    assert expected_located.hex() == '0x1.bdf7198e689a2p-47'
    assert line.line_locate(point) == expected_located
    np.testing.assert_array_equal(
        gm.GeometryArray([line]).line_locate(point), [expected_located]
    )

    line_z = gm.LineString([(0.0, 0.0, 0.0), (dx, dy, line.length)])
    point_z = gm.from_wkt(f'POINT Z ({point_xy[0]} {point_xy[1]} 0)')
    expected_z = float(_fraction(line.length) * fraction)
    expected_foot = (*foot, expected_z)
    for left, right, line_is_left in (
        (line_z, point_z, True),
        (point_z, line_z, False),
    ):
        nearest = gm.nearest_points(left, right)
        assert tuple(nearest[0 if line_is_left else 1].coords[0]) == expected_foot
        shortest = gm.shortest_line(left, right)
        witness = tuple(shortest.coords[0 if line_is_left else 1])
        assert witness == expected_foot


def test_generated_nonmonotone_keys_keep_the_exactly_closer_interior_witness() -> None:
    cases = _nonmonotone_witness_cases()
    lines = [gm.LineString([(0.0, 0.0), segment_end]) for segment_end, _ in cases]
    points = [gm.Point(*point_xy) for _, point_xy in cases]
    expected: list[tuple[float, float]] = []

    for line, point, (_, point_xy) in zip(lines, points, cases, strict=True):
        located = line.line_locate(point)
        assert 0.0 < located < line.length
        for left, right, line_is_left in (
            (line, point, True),
            (point, line, False),
        ):
            nearest = gm.nearest_points(left, right)
            witness = _coords(nearest[0 if line_is_left else 1])
            assert witness != (0.0, 0.0)
            assert _exact_distance_squared(
                point_xy, witness
            ) <= _exact_distance_squared(point_xy, (0.0, 0.0))
            shortest = tuple(map(tuple, gm.shortest_line(left, right).coords))
            assert shortest[0 if line_is_left else 1] == witness
        expected.append(witness)

    line_rows = gm.GeometryArray(lines)
    point_rows = gm.GeometryArray(points)
    for left, right, lines_are_left in (
        (line_rows, point_rows, True),
        (point_rows, line_rows, False),
    ):
        nearest = gm.nearest_points(left, right)
        shortest = gm.shortest_line(left, right)
        for row, witness in enumerate(expected):
            assert _coords(nearest[0 if lines_are_left else 1][row]) == witness
            assert tuple(shortest[row].coords[0 if lines_are_left else 1]) == witness

    far_segments = [
        [(100.0 + index, 100.0), (100.0 + index, 101.0)] for index in range(64)
    ]
    for line, point, witness in zip(lines, points, expected, strict=True):
        indexed = gm.MultiLineString([list(line.coords), *far_segments])
        for left, right, indexed_is_left in (
            (indexed, point, True),
            (point, indexed, False),
        ):
            nearest = gm.nearest_points(left, right)
            assert _coords(nearest[0 if indexed_is_left else 1]) == witness
            shortest = gm.shortest_line(left, right)
            assert tuple(shortest.coords[0 if indexed_is_left else 1]) == witness


def test_faithful_witness_contract_keeps_case_a_closer_point() -> None:
    dx = float.fromhex('0x1.3632375862748p-510')
    dy = float.fromhex('0x1.0fbf19953ae0ap-511')
    point_xy = (
        float.fromhex('-0x1.8fbe4c3d79f2ep-510'),
        float.fromhex('0x1.964233d01e367p-508'),
    )
    line = gm.LineString([(0.0, 0.0), (dx, dy)])
    reverse = gm.LineString([(dx, dy), (0.0, 0.0)])
    point = gm.Point(*point_xy)
    shipped = (
        float.fromhex('0x1.05d33a16aa5d1p-510'),
        float.fromhex('0x1.cabe086eeb4f9p-512'),
    )
    correctly_rounded_foot = (
        float.fromhex('0x1.05d33a16aa5d2p-510'),
        float.fromhex('0x1.cabe086eeb4f9p-512'),
    )
    assert _exact_distance_squared(point_xy, shipped) < _exact_distance_squared(
        point_xy, correctly_rounded_foot
    )
    for left, right, line_is_left in ((line, point, True), (point, line, False)):
        nearest = gm.nearest_points(left, right)
        assert _coords(nearest[0 if line_is_left else 1]) == shipped
        shortest = gm.shortest_line(left, right)
        assert tuple(shortest.coords[0 if line_is_left else 1]) == shipped
    assert _coords(gm.nearest_points(reverse, point)[0]) == correctly_rounded_foot

    # Both direction-dependent faithful spellings are candidates. Exact
    # selection must choose the forward lerp, which Fraction proves closer;
    # treating reversed ideal supports as identical keeps the farther first.
    far_segments = [
        [(100.0 + index, 100.0), (100.0 + index, 101.0)] for index in range(64)
    ]
    for carrier in (
        gm.MultiLineString([list(reverse.coords), list(line.coords)]),
        gm.MultiLineString([list(reverse.coords), list(line.coords), *far_segments]),
    ):
        for left, right, carrier_is_left in (
            (carrier, point, True),
            (point, carrier, False),
        ):
            nearest = gm.nearest_points(left, right)
            assert _coords(nearest[0 if carrier_is_left else 1]) == shipped
            shortest = gm.shortest_line(left, right)
            assert tuple(shortest.coords[0 if carrier_is_left else 1]) == shipped
    assert gm.distance(line, point).hex() == '0x1.9c38bb13b0b5fp-508'


def test_exact_selection_compares_the_emitted_witness_in_mixed_carriers() -> None:
    start = (-2.5049672666597784, -0.22855985577211868)
    end = (-2.640447616310041, 2.361006325545838)
    point_xy = (-2.506180295312065, -0.18635112572983203)
    closer_point = (-2.5065331214448894, -0.1854219813304938)
    farther_foot = (-2.507172816964488, -0.18640305225770984)
    point = gm.Point(*point_xy)
    line = gm.LineString([start, end])
    target = gm.GeometryCollection([line, gm.Point(*closer_point)])

    assert _exact_distance_squared(point_xy, closer_point) < _exact_distance_squared(
        point_xy, farther_foot
    )
    for left, right, target_is_left in (
        (target, point, True),
        (point, target, False),
    ):
        nearest = gm.nearest_points(left, right)
        assert _coords(nearest[0 if target_is_left else 1]) == closer_point
        shortest = gm.shortest_line(left, right)
        assert tuple(shortest.coords[0 if target_is_left else 1]) == closer_point

    targets = gm.GeometryArray([target, target])
    points = gm.GeometryArray([point, point])
    for left, right, targets_are_left in (
        (targets, points, True),
        (points, targets, False),
    ):
        nearest = gm.nearest_points(left, right)
        shortest = gm.shortest_line(left, right)
        for row in range(2):
            assert _coords(nearest[0 if targets_are_left else 1][row]) == closer_point
            assert (
                tuple(shortest[row].coords[0 if targets_are_left else 1])
                == closer_point
            )


def test_exact_xy_tie_retains_projected_ordinate_provenance() -> None:
    unit = 2.0**-52
    base = 1.5
    line = gm.from_wkt(
        f'LINESTRING Z ({base} {base} 0, {base + 3 * unit} {base + 4 * unit} 25)'
    )
    point = gm.from_wkt(f'POINT Z ({base - unit} {base + unit} 99)')
    projected = (base, base, 1.0)
    probe = tuple(point.coords[0])

    # The exact projection fraction is 1/25, but its emitted XY rounds to the
    # start vertex. Selection must retain the interior candidate's interpolated
    # Z rather than silently switching to the endpoint's Z=0 provenance.
    for left, right, line_is_left in ((line, point, True), (point, line, False)):
        nearest = gm.nearest_points(left, right)
        assert tuple(nearest[0 if line_is_left else 1].coords[0]) == projected
        assert tuple(nearest[1 if line_is_left else 0].coords[0]) == probe
        shortest = gm.shortest_line(left, right)
        expected = (projected, probe) if line_is_left else (probe, projected)
        assert tuple(map(tuple, shortest.coords)) == expected

    # Fixed scalar-to-array dispatch prepares and reuses the carrier. Exercise
    # it in both operand orders so the exact-tie rule is shared with that path.
    points = gm.GeometryArray([point, point])
    for left, right, line_is_left in ((line, points, True), (points, line, False)):
        nearest = gm.nearest_points(left, right)
        shortest = gm.shortest_line(left, right)
        for row in range(2):
            assert tuple(nearest[0 if line_is_left else 1][row].coords[0]) == projected
            assert tuple(nearest[1 if line_is_left else 0][row].coords[0]) == probe
            expected = (projected, probe) if line_is_left else (probe, projected)
            assert tuple(map(tuple, shortest[row].coords)) == expected


def test_segment_pair_witness_retains_exact_projection_ordinates() -> None:
    mu = 2.0**-1074
    huge = 2.0**1023
    dy = -1.0 + 2.0**-52
    left = gm.from_wkt(f'LINESTRING Z ({mu} {mu} 0, {mu} 1 0)')
    right = gm.from_wkt(f'LINESTRING Z (0 0 0, 1 {dy} {huge})')
    expected_left = (mu, mu, 0.0)
    exact_t = (_fraction(mu) + _fraction(mu) * _fraction(dy)) / (
        Fraction(1) + _fraction(dy) ** 2
    )
    expected_right = (0.0, -0.0, float(_fraction(huge) * exact_t))
    assert expected_right[2].hex() == '0x1.0000000000001p-104'

    for first, second, left_is_first in ((left, right, True), (right, left, False)):
        nearest = gm.nearest_points(first, second)
        assert tuple(nearest[0 if left_is_first else 1].coords[0]) == expected_left
        assert tuple(nearest[1 if left_is_first else 0].coords[0]) == expected_right
        shortest = gm.shortest_line(first, second)
        expected = (
            (expected_left, expected_right)
            if left_is_first
            else (expected_right, expected_left)
        )
        assert tuple(map(tuple, shortest.coords)) == expected

    left_array = gm.GeometryArray([left, left])
    right_array = gm.GeometryArray([right, right])
    for first, second, left_is_first in (
        (left_array, right_array, True),
        (right_array, left_array, False),
    ):
        nearest = gm.nearest_points(first, second)
        for index in range(2):
            assert (
                tuple(nearest[0 if left_is_first else 1][index].coords[0])
                == expected_left
            )
            assert (
                tuple(nearest[1 if left_is_first else 0][index].coords[0])
                == expected_right
            )


def test_overlay_ordinates_use_the_retained_exact_projection() -> None:
    huge = 2.0**1023
    x = 2.0**-52
    source = gm.from_wkt(f'LINESTRING Z (0 0 0, {huge} 0 {huge})')
    cutter = gm.LineString([(x, -1.0), (x, 1.0)])
    expected = (x, 0.0, x)

    scalar = gm.intersection(source, cutter)
    assert tuple(scalar.coords[0]) == expected
    array = gm.intersection(gm.GeometryArray([source]), cutter)
    assert tuple(array[0].coords[0]) == expected


def test_exact_ratio_survives_measured_locate() -> None:
    length = 2.0**1023
    x = 2.0**-52
    point = gm.Point(x, 2.0**-53)
    measured = gm.from_wkt(f'LINESTRING M (0 0 0, {length!r} 0 {length!r})')
    assert measured.line_locate(point, basis='m') == x
    np.testing.assert_array_equal(
        gm.GeometryArray([measured]).line_locate(point, basis='m'), [x]
    )

    reverse = gm.from_wkt(f'LINESTRING M ({length!r} 0 0, 0 0 {length!r})')
    expected_reverse = float(_fraction(length) - _fraction(x))
    assert reverse.line_locate(point, basis='m') == expected_reverse == length


def test_exact_ratio_survives_snap_ordering() -> None:
    length = 2.0**1023
    line = gm.LineString([(0.0, 0.0), (length, 0.0)])
    first = 2.0**-53
    second = 2.0**-52
    references = gm.MultiPoint([(second, 0.0), (first, 0.0)])
    expected = ((0.0, 0.0), (first, 0.0), (second, 0.0), (length, 0.0))
    assert tuple(map(tuple, gm.snap(line, references, 0.0).coords)) == expected
    snapped = gm.snap(gm.GeometryArray([line]), references, 0.0)
    assert tuple(map(tuple, snapped[0].coords)) == expected


@pytest.mark.parametrize('n', range(TRANSITION_LEFT - 8, TRANSITION_RIGHT + 9))
@pytest.mark.parametrize('length_exponent', range(-24, -15))
def test_projection_underflow_neighbourhood_matches_stored_double_reference(
    n: int,
    length_exponent: int,
) -> None:
    length = 2.0**length_exponent
    point_xy = (n * MU, (n + 1) * MU)
    point = gm.Point(*point_xy)
    endpoints = ((0.0, 0.0), (length, length))
    fraction, foot, expected_distance = _exact_projection(*endpoints, point_xy)
    assert 0 < fraction < 1

    for vertices in (endpoints, endpoints[::-1]):
        line = gm.LineString(vertices)
        assert gm.distance(line, point) == expected_distance
        assert gm.distance(point, line) == expected_distance
        assert gm.dwithin(line, point, expected_distance) is True
        nearest = gm.nearest_points(point, line)
        assert _coords(nearest[1]) == foot


@pytest.mark.parametrize('n', [TRANSITION_LEFT, TRANSITION_RIGHT])
def test_projection_underflow_transition_sides(n: int) -> None:
    point = gm.Point(n * MU, (n + 1) * MU)
    line = gm.LineString([(0.0, 0.0), (BASE_LENGTH, BASE_LENGTH)])
    _, foot, expected_distance = _exact_projection(
        (0.0, 0.0),
        (BASE_LENGTH, BASE_LENGTH),
        (n * MU, (n + 1) * MU),
    )
    assert gm.distance(line, point) == expected_distance
    assert _coords(gm.nearest_points(point, line)[1]) == foot


@pytest.mark.parametrize('length_exponent', [-20, -19])
def test_projection_underflow_length_transition_sides(length_exponent: int) -> None:
    length = 2.0**length_exponent
    n = TRANSITION_LEFT
    point_xy = (n * MU, (n + 1) * MU)
    point = gm.Point(*point_xy)
    line = gm.LineString([(0.0, 0.0), (length, length)])
    _, foot, expected_distance = _exact_projection(
        (0.0, 0.0), (length, length), point_xy
    )
    assert gm.distance(line, point) == expected_distance
    assert _coords(gm.nearest_points(point, line)[1]) == foot
