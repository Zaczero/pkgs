"""R15-N: deterministic scale and realized-work regressions.

Each numeric oracle starts from the stored IEEE-754 operands, never the
source literals.  These fixtures deliberately name the exceptional paths.
"""

from __future__ import annotations

import math
from fractions import Fraction
from itertools import pairwise

import gometry as gm
import pytest


def _stored(value: float) -> Fraction:
    return Fraction(value)


def _diagonal_ordinate(
    start: float, end: float, point: float, lo: float, hi: float
) -> float:
    """Z/M reference for the stored source diagonal, not its typed literal."""
    fraction = (_stored(point) - _stored(start)) / (_stored(end) - _stored(start))
    return float(_stored(lo) * (1 - fraction) + _stored(hi) * fraction)


@pytest.mark.parametrize('reverse', [False, True])
def test_extreme_clip_carries_zm_in_source_order(reverse: bool) -> None:
    """A no-vertex-in-window diagonal reaches ordinate segment interpolation."""
    forward = [(-1e308, -1e308, 100.0, 1000.0), (1e308, 1e308, 300.0, 3000.0)]
    source = list(reversed(forward)) if reverse else forward
    # The XY clip has no source vertex to reuse, so `carry_ordinates` misses
    # the vertex table and must enter `segment_ordinate_at` for both corners.
    clipped = gm.LineString(source).clip_by_rect(0.0, 0.0, 1.0, 1.0)
    expected_xy = [(1.0, 1.0), (0.0, 0.0)] if reverse else [(0.0, 0.0), (1.0, 1.0)]
    got = list(clipped.coords)
    assert [(x, y) for x, y, _, _ in got] == expected_xy
    start, end = source
    for (x, y, z, m), (expected_x, expected_y) in zip(got, expected_xy, strict=True):
        assert (x, y) == (expected_x, expected_y)
        assert z == _diagonal_ordinate(start[0], end[0], x, start[2], end[2])
        assert m == _diagonal_ordinate(start[0], end[0], x, start[3], end[3])


def test_vw_opposite_sign_extremes_uses_scaled_original_operands() -> None:
    """The overflowing `x - origin` frame still removes the sub-threshold vertex."""
    coords = [(-1e308, 0.0), (0.0, 1e307), (1e308, 0.0)]
    tolerance = 5e307
    # This is the exact stored-double triangle area, and establishes the VW
    # decision independent of the arithmetic frame used by the implementation.
    (ax, ay), (bx, by), (cx, cy) = [(_stored(x), _stored(y)) for x, y in coords]
    area = abs((bx - ax) * (cy - ay) - (by - ay) * (cx - ax)) / 2
    assert area < _stored(tolerance) ** 2 / 2
    assert not math.isfinite(coords[-1][0] - coords[0][0])
    simplified = gm.LineString(coords).simplify(
        tolerance, method='vw', preserve_topology=False
    )
    assert list(simplified.coords) == [coords[0], coords[-1]]


def test_noncollinear_tiny_catmull_rom_keeps_its_nonzero_knots() -> None:
    """The half-segment sample enters `lerp_knot` with a nonzero 1e-20 span."""
    scale = 1e-40
    coords = [
        (0.0, 0.0),
        (scale, 2.0 * scale),
        (2.0 * scale, 0.0),
        (3.0 * scale, scale),
    ]
    assert all(
        (_stored(right[0]) - _stored(left[0])) != 0 for left, right in pairwise(coords)
    )
    # Non-collinearity makes this a true Catmull-Rom corner, not its linear
    # fallback.  The stored-double reference is the first half sample's known
    # 0.5 / 1.25 scale; a relative comparison accounts for final f64 rounding.
    out = list(gm.LineString(coords).smooth(iterations=1, method='catmull_rom').coords)
    assert len(out) == 7
    expected = (float(_stored(scale) / 2), float(_stored(scale) * Fraction(5, 4)))
    assert out[1][0] == pytest.approx(expected[0], rel=1e-14)
    assert out[1][1] == pytest.approx(expected[1], rel=1e-14)
    assert out[1] != coords[0]


@pytest.mark.parametrize(
    ('suffix', 'tail'),
    [
        ('packed', []),
        ('missing', [None]),
        ('heterogeneous', [gm.Point(0.0, 0.0)]),
    ],
)
def test_segmentize_shares_one_budget_across_packed_missing_and_mixed_rows(
    suffix: str, tail: list[gm.Geometry | None]
) -> None:
    """Every storage lane reaches the one operation-wide subdivision budget."""
    values = gm.GeometryArray(
        [gm.LineString([(0.0, 0.0), (1.0, 0.0)]) for _ in range(2)] + tail
    )
    # Both lines independently create 8,000,001 vertices (under the cap); the
    # second pushes the one shared operation budget to 16,000,002. `missing`
    # forces the mask-aware fallback and `heterogeneous` forces mixed storage,
    # the two lanes that previously constructed a fresh scalar budget per row.
    with pytest.raises(ValueError, match=r'16,000,000|16000000|generated'):
        values.segmentize(1.0 / 8_000_002.0)


def _arc_heavy_line() -> gm.Geometry:
    return gm.LineString([(float(index), float(index & 1)) for index in range(1000)])


def test_round_walk_counts_match_the_public_arc_heavy_buffer_and_offset() -> None:
    """Alternating turns enter the WalkPlan arc/join emitter, not the cap-only case."""
    line = _arc_heavy_line()
    # Every alternating interior turn reaches a round join.  These counts are
    # the deterministic public outputs of the same 1000 x q=512 workload used
    # for the mechanism measurement: an open offset, and the closed stroke.
    assert (
        len(line.offset_curve(1.0, join_style='round', quadrant_segments=512).coords)
        == 256_488
    )
    buffered = line.buffer(1.0, join_style='round', quadrant_segments=512)
    assert len(buffered.exterior.coords) == 515_023


def test_rounded_stroke_cap_rejects_before_materializing_its_arc_walk() -> None:
    """Two round caps enter the same raw-stroke count/output traversal."""
    line = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
    # q=8M has no joins; the two pi caps alone already exceed the limit in the
    # open shared walk, so it rejects before any 32M-coordinate allocation.
    with pytest.raises(ValueError, match=r'16000000|generated'):
        line.buffer(1.0, join_style='round', quadrant_segments=8_000_000)


def test_point_buffer_counts_its_closing_coordinate_before_circle_allocation() -> None:
    """The exact 16M open circle still exceeds the realized closed polygon limit."""
    with pytest.raises(ValueError, match=r'16000000|generated'):
        gm.Point(0.0, 0.0).buffer(1.0, quadrant_segments=4_000_000)


def test_deep_single_member_collection_buffer_reaches_the_real_member_once() -> None:
    """One-child collection wrappers flatten iteratively before the point path."""
    nested: gm.Geometry = gm.Point(0.0, 0.0)
    for _ in range(1024):
        nested = gm.GeometryCollection([nested])
    assert nested.buffer(1.0).to_wkb() == gm.Point(0.0, 0.0).buffer(1.0).to_wkb()
