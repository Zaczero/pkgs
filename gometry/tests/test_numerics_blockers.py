"""R15-A numerics blockers: subnormal squared distances + tiny-ring topology.

Deterministic Fraction-referenced fixtures. No wall-clock assertions.
"""

from __future__ import annotations

from fractions import Fraction

import gometry as gm
import numpy as np
import pytest

# Subnormal-square separations (exact f64 values used as Fraction numerators).
_SEP_EXACT_ZERO = 1.5e-162  # square fully underflows → pre-fix rescue path
_SEP_SUBNORMAL_SQ = 1.6e-162  # positive subnormal square (was +38.9% wrong)
_SEP_BOUNDARY_DW = 2.2e-162  # dwithin(1.6e-162) must be False
_DW_LIMIT = 1.6e-162


def _exact_sep(value: float) -> Fraction:
    return Fraction(value).limit_denominator()


def test_subnormal_distance_matches_exact_separation() -> None:
    """Squared norm is trustworthy only when normal (or exact-zero deltas)."""
    origin = gm.Point(0.0, 0.0)
    for sep in (_SEP_EXACT_ZERO, _SEP_SUBNORMAL_SQ, _SEP_BOUNDARY_DW, 1e-160, 3e-162):
        got = gm.distance(origin, gm.Point(sep, 0.0))
        exact = float(sep)
        assert got == exact, f'sep={sep}: got {got!r} != exact {exact!r}'
        # Relative error vs Fraction truth must be 0 at these dyadic inputs.
        rel = abs(Fraction(got) - Fraction(exact)) / Fraction(exact)
        assert rel == 0


def test_subnormal_dwithin_boolean_is_exact() -> None:
    """Dwithin must not accept a positive-subnormal square as a faithful norm."""
    origin = gm.Point(0.0, 0.0)
    cases = [
        (_SEP_EXACT_ZERO, True),
        (_SEP_SUBNORMAL_SQ, True),
        (_SEP_BOUNDARY_DW, False),
    ]
    for sep, expected in cases:
        got = gm.dwithin(origin, gm.Point(sep, 0.0), _DW_LIMIT)
        assert got is expected, f'sep={sep}: dwithin={got} expected={expected}'


def test_subnormal_distance_is_monotone_in_separation() -> None:
    """Answer must not get worse as the true separation grows (pre-fix did)."""
    origin = gm.Point(0.0, 0.0)
    seps = [1.5e-162, 1.6e-162, 1.8e-162, 2.0e-162, 2.2e-162]
    dists = [gm.distance(origin, gm.Point(s, 0.0)) for s in seps]
    for i in range(len(dists) - 1):
        assert dists[i] <= dists[i + 1], f'non-monotone: {dists}'
        assert dists[i] == seps[i]
    assert dists[-1] == seps[-1]


def test_subnormal_distance_and_dwithin_array_packed() -> None:
    """Array/packed lanes share the same squared-norm trust rule."""
    origins = gm.GeometryArray([
        gm.Point(0.0, 0.0),
        gm.Point(0.0, 0.0),
        gm.Point(0.0, 0.0),
    ])
    seps = [_SEP_EXACT_ZERO, _SEP_SUBNORMAL_SQ, _SEP_BOUNDARY_DW]
    others = gm.GeometryArray([gm.Point(s, 0.0) for s in seps])
    dists = gm.distance(origins, others)
    np.testing.assert_array_equal(dists, np.asarray(seps, dtype=np.float64))
    dw = gm.dwithin(origins, others, _DW_LIMIT)
    np.testing.assert_array_equal(dw, np.asarray([True, True, False]))
    # Packed homogeneous point columns (same path as GeometryArray of points).
    packed_a = gm.points([0.0, 0.0, 0.0], [0.0, 0.0, 0.0])
    packed_b = gm.points(seps, [0.0, 0.0, 0.0])
    np.testing.assert_array_equal(gm.distance(packed_a, packed_b), seps)
    np.testing.assert_array_equal(
        gm.dwithin(packed_a, packed_b, _DW_LIMIT), [True, True, False]
    )


def test_ordinary_magnitude_distance_bit_identical_smoke() -> None:
    """Ordinary mid-range results stay on the normal-square fast path."""
    cases = [
        ((0.0, 0.0), (3.0, 4.0), 5.0),
        ((1.0, 2.0), (1.0, 2.0), 0.0),
        ((-1e6, 0.0), (1e6, 0.0), 2e6),
        ((0.0, 0.0), (1e-8, 0.0), 1e-8),
    ]
    for (ax, ay), (bx, by), expected in cases:
        got = gm.distance(gm.Point(ax, ay), gm.Point(bx, by))
        assert got == expected


def test_subnormal_hausdorff_matches_exact_separation() -> None:
    """Hausdorff finishers inherit the shared squared-norm trust rule."""
    origin = gm.Point(0.0, 0.0)
    for sep in (_SEP_EXACT_ZERO, _SEP_SUBNORMAL_SQ, _SEP_BOUNDARY_DW):
        got = gm.hausdorff_distance(origin, gm.Point(sep, 0.0))
        assert got == float(sep), f'scalar sep={sep}: {got!r}'
        # Parallel unit lines: continuous HD equals the offset.
        left = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
        right = gm.LineString([(0.0, sep), (1.0, sep)])
        got_line = gm.hausdorff_distance(left, right)
        assert got_line == float(sep), f'parallel sep={sep}: {got_line!r}'


def test_subnormal_frechet_matches_exact_separation() -> None:
    """Frechet finishers inherit the shared squared-norm trust rule."""
    for sep in (_SEP_EXACT_ZERO, _SEP_SUBNORMAL_SQ, _SEP_BOUNDARY_DW):
        left = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
        right = gm.LineString([(0.0, sep), (1.0, sep)])
        got = gm.frechet_distance(left, right)
        assert got == float(sep), f'sep={sep}: {got!r}'


def test_subnormal_minimum_clearance_matches_exact_separation() -> None:
    """Minimum clearance finisher inherits the shared squared-norm trust rule."""
    for sep in (_SEP_EXACT_ZERO, _SEP_SUBNORMAL_SQ, _SEP_BOUNDARY_DW):
        mp = gm.MultiPoint([(0.0, 0.0), (sep, 0.0)])
        got = mp.minimum_clearance()
        assert got == float(sep), f'sep={sep}: {got!r}'


def test_subnormal_metric_family_is_monotone() -> None:
    """Hausdorff / Frechet / clearance must not get worse as separation grows."""
    seps = [1.5e-162, 1.6e-162, 1.8e-162, 2.0e-162, 2.2e-162]
    haus = []
    frech = []
    clear = []
    for sep in seps:
        a = gm.Point(0.0, 0.0)
        b = gm.Point(sep, 0.0)
        haus.append(gm.hausdorff_distance(a, b))
        left = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
        right = gm.LineString([(0.0, sep), (1.0, sep)])
        frech.append(gm.frechet_distance(left, right))
        clear.append(gm.MultiPoint([(0.0, 0.0), (sep, 0.0)]).minimum_clearance())
    for series, name in ((haus, 'hausdorff'), (frech, 'frechet'), (clear, 'clearance')):
        for i in range(len(series) - 1):
            assert series[i] <= series[i + 1], f'{name} non-monotone: {series}'
            assert series[i] == seps[i], f'{name} sep={seps[i]}: {series[i]!r}'
        assert series[-1] == seps[-1]


def test_tiny_ring_area_decision_keeps_nonzero_orientation() -> None:
    """1e-162 rings must not be classified as zero (topology deleted)."""
    s = 1e-162
    ring = gm.Polygon([(0.0, 0.0), (s, 0.0), (s, s), (0.0, s), (0.0, 0.0)])
    # Measurement may underflow to 0; the ring must still be valid topology
    # and produce non-empty constructive results.
    assert ring.is_valid is True
    triangles = list(ring.triangulate(method='earcut'))
    assert len(triangles) >= 1
    assert all(not t.is_empty for t in triangles)
    edges = gm.MultiLineString([
        [(0.0, 0.0), (s, 0.0)],
        [(s, 0.0), (s, s)],
        [(s, s), (0.0, s)],
        [(0.0, s), (0.0, 0.0)],
    ])
    polys = gm.polygonize(edges)
    assert len(polys) >= 1
    assert not polys[0].is_empty


def test_tiny_overlapping_rectangles_union_contains_intersection() -> None:
    """Union is a superset of intersection — empty-union + nonempty-inter is impossible."""
    s = 1e-162
    r1 = gm.Polygon([(0.0, 0.0), (s, 0.0), (s, s), (0.0, s), (0.0, 0.0)])
    r2 = gm.Polygon([
        (s / 2.0, s / 2.0),
        (s * 1.5, s / 2.0),
        (s * 1.5, s * 1.5),
        (s / 2.0, s * 1.5),
        (s / 2.0, s / 2.0),
    ])
    inter = gm.intersection(r1, r2)
    union = gm.union(r1, r2)
    assert not inter.is_empty
    assert not union.is_empty
    assert gm.covers(union, inter) is True


def test_mixed_axis_ring_orientation_is_nonzero() -> None:
    """Per-axis power-of-two scaling keeps 1e300 x 1e-300 rings decisive."""
    a, b = 1e300, 1e-300
    ring = gm.Polygon([(0.0, 0.0), (a, 0.0), (a, b), (0.0, b), (0.0, 0.0)])
    assert ring.is_valid is True
    # Exact area is a*b = 1.0 in f64; measurement must recover it.
    assert ring.area == pytest.approx(1.0, rel=0, abs=0.0)
    assert float(Fraction(a) * Fraction(b)) == pytest.approx(
        ring.area, rel=0, abs=1e-15
    )
    assert ring.area > 0.0
    # A zero-area classification would make both paths empty/invalid under overlay.
    assert not gm.intersection(ring, ring).is_empty
