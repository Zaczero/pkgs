"""SIMD chunk-scale overflow/underflow rescue battery.

Pins that length/distance over ~1k-vertex lines with 1e200 / 1e-200 coordinates
and a single poisoned segment stay finite and match a pure-Python chained-hypot
oracle — for both packed point-line arrays and mixed storage.
"""

from __future__ import annotations

import math

import gometry as gm
import numpy as np
import pytest

N = 1024
HUGE = 1e200
TINY = 1e-200
POISON_AT = 512


def _chained_hypot_length(coords: list[tuple[float, float]]) -> float:
    """Reference length: sum of per-segment ``math.hypot`` (libm, no SIMD)."""
    total = 0.0
    for i in range(1, len(coords)):
        dx = coords[i][0] - coords[i - 1][0]
        dy = coords[i][1] - coords[i - 1][1]
        total += math.hypot(dx, dy)
    return total


def _axis_line(n: int = N) -> list[tuple[float, float]]:
    return [(float(i), 0.0) for i in range(n)]


def _huge_poisoned_line() -> list[tuple[float, float]]:
    coords = _axis_line()
    # One vertex shoots to ±1e200 so two adjacent segments are huge; the rest
    # stay O(1). The SIMD length kernel must rescue via scalar hypot.
    coords[POISON_AT] = (float(POISON_AT), HUGE)
    return coords


def _tiny_line() -> list[tuple[float, float]]:
    # Consecutive vertices 1e-200 apart — underflows a naive sum-of-squares.
    return [(float(i) * TINY, 0.0) for i in range(N)]


def _assert_length_matches_oracle(coords: list[tuple[float, float]]) -> float:
    geom = gm.LineString(coords)
    oracle = _chained_hypot_length(coords)
    got = geom.length
    assert math.isfinite(got), got
    assert math.isfinite(oracle), oracle
    # Relative for large magnitudes; absolute floor for tiny ones.
    assert got == pytest.approx(oracle, rel=1e-12, abs=1e-300)
    return got


def test_scalar_length_huge_poisoned_segment_matches_hypot_oracle() -> None:
    got = _assert_length_matches_oracle(_huge_poisoned_line())
    # Sanity: the two huge legs dominate — magnitude is ~2e200.
    assert got == pytest.approx(2.0 * HUGE, rel=1e-9)


def test_scalar_length_tiny_coordinates_matches_hypot_oracle() -> None:
    got = _assert_length_matches_oracle(_tiny_line())
    expected = (N - 1) * TINY
    assert got == pytest.approx(expected, rel=1e-12, abs=1e-300)


def test_scalar_distance_huge_points_is_finite() -> None:
    a = gm.Point(0.0, HUGE)
    b = gm.Point(0.0, 0.0)
    d = gm.distance(a, b)
    assert math.isfinite(d)
    assert d == pytest.approx(HUGE)
    # Segment distance through a poisoned line to an offset point.
    line = gm.LineString(_huge_poisoned_line())
    d2 = gm.distance(line, gm.Point(0.0, 1.0))
    assert math.isfinite(d2)
    assert d2 == pytest.approx(1.0)


def test_scalar_distance_tiny_points_matches_hypot() -> None:
    a = gm.Point(0.0, 0.0)
    b = gm.Point(TINY, TINY)
    d = gm.distance(a, b)
    assert math.isfinite(d)
    assert d == pytest.approx(math.hypot(TINY, TINY), rel=1e-12, abs=1e-300)


def test_packed_array_length_huge_and_tiny() -> None:
    huge = gm.LineString(_huge_poisoned_line())
    tiny = gm.LineString(_tiny_line())
    ordinary = gm.LineString([(0.0, 0.0), (3.0, 4.0)])
    # Homogeneous LineString array → packed storage.
    arr = gm.GeometryArray([huge, tiny, ordinary])
    lengths = np.asarray(arr.length, dtype=np.float64)
    assert lengths.shape == (3,)
    assert np.all(np.isfinite(lengths))
    assert lengths[0] == pytest.approx(huge.length, rel=1e-12)
    assert lengths[1] == pytest.approx(tiny.length, rel=1e-12, abs=1e-300)
    assert lengths[2] == pytest.approx(5.0)


def test_mixed_array_length_with_poisoned_row() -> None:
    huge = gm.LineString(_huge_poisoned_line())
    # Mixed kinds force boxed/mixed storage rather than packed lines.
    arr = gm.GeometryArray([
        huge,
        gm.Point(0.0, 0.0),
        gm.LineString([(0.0, 0.0), (3.0, 4.0)]),
        gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
    ])
    lengths = np.asarray(arr.length, dtype=np.float64)
    assert lengths.shape == (4,)
    assert np.all(np.isfinite(lengths))
    assert lengths[0] == pytest.approx(huge.length, rel=1e-12)
    assert lengths[1] == pytest.approx(0.0)
    assert lengths[2] == pytest.approx(5.0)
    assert lengths[3] == pytest.approx(
        gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]).length
    )


def test_packed_array_distance_huge_pairs() -> None:
    pts = gm.GeometryArray([
        gm.Point(0.0, 0.0),
        gm.Point(0.0, HUGE),
        gm.Point(HUGE, 0.0),
        gm.Point(TINY, 0.0),
    ])
    origin = gm.Point(0.0, 0.0)
    distances = np.asarray(gm.distance(pts, origin), dtype=np.float64)
    assert distances.shape == (4,)
    assert np.all(np.isfinite(distances))
    assert distances[0] == pytest.approx(0.0)
    assert distances[1] == pytest.approx(HUGE)
    assert distances[2] == pytest.approx(HUGE)
    assert distances[3] == pytest.approx(TINY, rel=1e-12, abs=1e-300)


def test_mixed_array_distance_poisoned_line_vs_point() -> None:
    line = gm.LineString(_huge_poisoned_line())
    probe = gm.Point(0.0, 1.0)
    mixed = gm.GeometryArray([
        line,
        gm.Point(0.0, HUGE),
        gm.LineString([(0.0, 0.0), (1.0, 0.0)]),
    ])
    distances = np.asarray(gm.distance(mixed, probe), dtype=np.float64)
    assert np.all(np.isfinite(distances))
    assert distances[0] == pytest.approx(gm.distance(line, probe))
    assert distances[1] == pytest.approx(HUGE - 1.0, rel=1e-12)
    assert distances[2] == pytest.approx(1.0)


def test_length_one_poisoned_segment_in_otherwise_unit_line() -> None:
    """A single non-finite-risk segment must not poison the whole accumulator."""
    coords = _axis_line()
    # Replace one mid segment with a 1e200 vertical spike and return.
    coords[POISON_AT] = (float(POISON_AT), HUGE)
    coords[POISON_AT + 1] = (float(POISON_AT + 1), 0.0)
    got = _assert_length_matches_oracle(coords)
    # Two huge legs of length ~HUGE plus ~1021 unit steps.
    assert got > HUGE
    assert math.isfinite(got)
