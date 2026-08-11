"""Packed dual-strategy length: missing/empty rows, short/long mix, rescue parity.

Drives the public ``GeometryArray.length`` / ``length_3d`` surface after the
columnar-reduce dual path (topology ``SegmentedRuns`` + PerRun/ColumnStream).
"""

from __future__ import annotations

import math

import gometry as gm
import numpy as np
import pytest

HUGE = 1e200
N = 1024
POISON_AT = 512


def _chained_hypot_length(coords: list[tuple[float, float]]) -> float:
    total = 0.0
    for i in range(1, len(coords)):
        total += math.hypot(
            coords[i][0] - coords[i - 1][0], coords[i][1] - coords[i - 1][1]
        )
    return total


def _huge_poisoned_line() -> list[tuple[float, float]]:
    coords = [(float(i), 0.0) for i in range(N)]
    coords[POISON_AT] = (float(POISON_AT), HUGE)
    return coords


def test_packed_length_missing_empty_degenerate_rows() -> None:
    arr = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (3.0, 4.0)]),
        None,
        gm.LineString([]),
        gm.LineString([(0.0, 0.0), (0.0, 0.0)]),  # zero-length segment
        gm.LineString([(0.0, 0.0), (0.0, 1.0), (1.0, 1.0)]),
    ])
    got = np.asarray(arr.length, dtype=np.float64)
    assert got.shape == (5,)
    assert got[0] == pytest.approx(5.0)
    assert math.isnan(got[1])
    assert got[2] == 0.0
    assert got[3] == 0.0
    assert got[4] == pytest.approx(2.0)


def test_packed_length_mixed_short_and_long_rows_match_scalar() -> None:
    short = [
        gm.LineString([(float(j + k), float(k % 2)) for k in range(10)])
        for j in range(200)
    ]
    long = [gm.LineString([(float(i), 0.0) for i in range(2_000)]) for _ in range(3)]
    arr = gm.GeometryArray(short + long)
    packed = np.asarray(arr.length, dtype=np.float64)
    scalar = np.array([g.length for g in arr], dtype=np.float64)
    np.testing.assert_allclose(packed, scalar, rtol=1e-12, atol=0.0)


def test_packed_length_huge_poisoned_hex_matches_scalar() -> None:
    """Compact-guard rescue: packed and scalar stay in lockstep (incl. huge)."""
    coords = _huge_poisoned_line()
    geom = gm.LineString(coords)
    tiny = gm.LineString([(float(i) * 1e-200, 0.0) for i in range(N)])
    arr = gm.GeometryArray([geom, tiny])
    packed = np.asarray(arr.length, dtype=np.float64)
    assert packed[0] == geom.length
    assert packed[1] == arr[1].length
    assert packed[0] == pytest.approx(_chained_hypot_length(coords), rel=1e-12)
    assert math.isfinite(packed[0])
    assert packed[0] == pytest.approx(2.0 * HUGE, rel=1e-9)


def test_packed_length_3d_matches_scalar() -> None:
    lines = [
        gm.LineString([(float(i), 0.0, float(i % 3)) for i in range(12)])
        for _ in range(50)
    ]
    arr = gm.GeometryArray(lines)
    packed = np.asarray(arr.length_3d, dtype=np.float64)
    scalar = np.array([g.length_3d for g in arr], dtype=np.float64)
    np.testing.assert_allclose(packed, scalar, rtol=1e-12, atol=0.0)


def test_packed_polygon_perimeter_matches_scalar() -> None:
    polys = [
        gm.Polygon(
            [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)],
            holes=[[(0.2, 0.2), (0.8, 0.2), (0.8, 0.8), (0.2, 0.8), (0.2, 0.2)]],
        )
        for _ in range(100)
    ]
    arr = gm.GeometryArray(polys)
    packed = np.asarray(arr.length, dtype=np.float64)
    scalar = np.array([g.length for g in arr], dtype=np.float64)
    np.testing.assert_allclose(packed, scalar, rtol=1e-12, atol=0.0)
