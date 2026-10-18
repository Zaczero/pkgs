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
def test_extreme_clip_carries_zm_from_source_segments(reverse: bool) -> None:
    """A no-vertex-in-window diagonal reaches ordinate segment interpolation."""
    forward = [(-1e308, -1e308, 100.0, 1000.0), (1e308, 1e308, 300.0, 3000.0)]
    source = list(reversed(forward)) if reverse else forward
    # The XY clip has no source vertex to reuse, so `carry_ordinates` misses
    # the vertex table and must enter `segment_ordinate_at` for both corners.
    line = gm.LineString(source)
    rectangle = gm.box(0.0, 0.0, 1.0, 1.0)
    clipped = line.clip_by_rect(0.0, 0.0, 1.0, 1.0)
    assert clipped.to_wkb() == gm.intersection(line, rectangle).to_wkb()
    expected_xy = [(0.0, 0.0), (1.0, 1.0)]
    got = list(clipped.coords)
    assert [(x, y) for x, y, _, _ in got] == expected_xy
    start, end = source
    for (x, y, z, m), (expected_x, expected_y) in zip(got, expected_xy, strict=True):
        assert (x, y) == (expected_x, expected_y)
        assert z == _diagonal_ordinate(start[0], end[0], x, start[2], end[2])
        assert m == _diagonal_ordinate(start[0], end[0], x, start[3], end[3])


@pytest.mark.parametrize('reverse', [False, True])
@pytest.mark.parametrize(
    ('y_end', 'expected_y'),
    [
        (1.0e-20, (5.0e-21, 5.000000000000005e-21)),
        (1.0e-19, (5.0e-20, 5.0000000000000047e-20)),
    ],
)
def test_exact_clip_rounds_thin_crossings(
    reverse: bool, y_end: float, expected_y: tuple[float, float]
) -> None:
    forward = [(-1.0e15, 0.0), (1.0e15, y_end)]
    source = list(reversed(forward)) if reverse else forward
    line = gm.LineString(source)
    rectangle = gm.box(0.0, 0.0, 1.0, 1.0)
    clipped = line.clip_by_rect(0.0, 0.0, 1.0, 1.0)
    assert clipped.to_wkb() == gm.intersection(line, rectangle).to_wkb()
    expected = [(0.0, expected_y[0]), (1.0, expected_y[1])]
    assert list(clipped.coords) == expected


def test_exact_clip_rounds_mixed_exponent_crossings() -> None:
    tiny = 2.0**-601
    line = gm.LineString([(2.0**-600, -tiny), (-(2.0**500), 2.0**500)])
    clipped = line.clip_by_rect(0.0, 0.0, 1.0, 2.0**600)
    assert (
        clipped.to_wkb()
        == gm.intersection(line, gm.box(0.0, 0.0, 1.0, 2.0**600)).to_wkb()
    )
    assert list(clipped.coords) == [(0.0, tiny), (tiny, 0.0)]

    line = gm.LineString([(2.0**-1074, 2.0**-500), (-(2.0**500), -(2.0**1023))])
    clipped = line.clip_by_rect(0.0, 0.0, 1.0, 1.0)
    assert (
        clipped.to_wkb() == gm.intersection(line, gm.box(0.0, 0.0, 1.0, 1.0)).to_wkb()
    )
    assert list(clipped.coords) == [
        (0.0, float.fromhex('0x1.ffffffffffffcp-501')),
        (2.0**-1074, 2.0**-500),
    ]


def test_exact_clip_rounds_thin_crossing_zm_and_packed_missing_rows() -> None:
    start = (-1.0e15, 0.0, 100.0, 1000.0)
    end = (1.0e15, 1.0e-20, 300.0, 3000.0)
    line = gm.LineString([start, end])
    # Exact Fraction interpolation of the stored binary64 endpoints, followed
    # by float conversion, is the independent correctly rounded oracle.
    expected = [
        (
            0.0,
            _diagonal_ordinate(start[0], end[0], 0.0, start[1], end[1]),
            _diagonal_ordinate(start[0], end[0], 0.0, start[2], end[2]),
            _diagonal_ordinate(start[0], end[0], 0.0, start[3], end[3]),
        ),
        (
            1.0,
            _diagonal_ordinate(start[0], end[0], 1.0, start[1], end[1]),
            _diagonal_ordinate(start[0], end[0], 1.0, start[2], end[2]),
            _diagonal_ordinate(start[0], end[0], 1.0, start[3], end[3]),
        ),
    ]
    assert list(line.clip_by_rect(0.0, 0.0, 1.0, 1.0).coords) == expected

    rows = gm.GeometryArray([gm.LineString([(-1.0e15, 0.0), (1.0e15, 1.0e-20)]), None])
    result = rows.clip_by_rect(0.0, 0.0, 1.0, 1.0)
    assert list(result[0].coords) == [(0.0, 5.0e-21), (1.0, 5.000000000000005e-21)]
    assert result.is_missing.tolist() == [False, True]


def test_packed_line_clip_matches_each_geometry_including_emptied_rows() -> None:
    lines = [
        [(0.2, 0.2), (0.8, 0.8)],
        [(2.0, 2.0), (3.0, 3.0)],
        [(-1.0, 0.5), (2.0, 0.5)],
        [(0.5, 1.6), (1.6, 0.5)],
        [(-1.0, 0.5), (0.5, 0.5), (0.5, 2.0), (0.7, 2.0), (0.7, 0.5), (2.0, 0.5)],
    ]
    expected = [
        repr(gm.LineString(line).clip_by_rect(0.0, 0.0, 1.0, 1.0)) for line in lines
    ]
    assert expected[1] == expected[3] == '<LINESTRING EMPTY>'
    assert expected[4].startswith('<MULTILINESTRING')
    for rows in (lines[:4], lines):
        clipped = gm.line_strings(rows).clip_by_rect(0.0, 0.0, 1.0, 1.0)
        assert [repr(row) for row in clipped] == expected[: len(rows)]


def test_exact_clip_preserves_closed_boundaries_signed_zero_and_degenerate_rectangles() -> (
    None
):
    signed_line = gm.LineString([(-2.0, -0.0), (2.0, -0.0)])
    signed = signed_line.clip_by_rect(0.0, 0.0, 1.0, 1.0)
    assert (
        signed.to_wkb()
        == gm.intersection(signed_line, gm.box(0.0, 0.0, 1.0, 1.0)).to_wkb()
    )
    assert [point[1].hex() for point in signed.coords] == ['0x0.0p+0', '0x0.0p+0']

    collinear = gm.LineString([(-2.0, 0.0), (2.0, 0.0)]).clip_by_rect(
        0.0, 0.0, 1.0, 1.0
    )
    assert list(collinear.coords) == [(0.0, 0.0), (1.0, 0.0)]
    corner_line = gm.LineString([(-1.0, 1.0), (0.0, 0.0)])
    corner_touch = corner_line.clip_by_rect(0.0, 0.0, 1.0, 1.0)
    assert corner_touch.to_wkt() == 'POINT (0 0)'
    assert (
        corner_touch.to_wkb()
        == gm.intersection(corner_line, gm.box(0.0, 0.0, 1.0, 1.0)).to_wkb()
    )

    vertical = gm.LineString([(0.0, -1.0), (0.0, 2.0)]).clip_by_rect(0.0, 0.0, 0.0, 1.0)
    horizontal = gm.LineString([(-1.0, 0.0), (2.0, 0.0)]).clip_by_rect(
        0.0, 0.0, 1.0, 0.0
    )
    assert list(vertical.coords) == [(0.0, 0.0), (0.0, 1.0)]
    assert list(horizontal.coords) == [(0.0, 0.0), (1.0, 0.0)]


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
def test_segmentize_bounds_each_row_on_every_storage_lane(
    suffix: str, tail: list[gm.Geometry | None]
) -> None:
    """Every storage lane refuses a row its parameter amplifies past the cap.

    The budget belongs to one geometry: the array's length is the caller's
    business, so rows do not share it. `missing` forces the mask-aware lane and
    `heterogeneous` forces mixed storage.
    """
    values = gm.GeometryArray([gm.LineString([(0.0, 0.0), (1.0, 0.0)]), *tail])
    with pytest.raises(gm.ResourceLimitError) as error:
        values.segmentize(1.0 / 16_000_002.0)
    assert (error.value.code, error.value.operation, error.value.parameter) == (
        'output_limit',
        'segmentize',
        'max_length',
    )
    assert error.value.produced > error.value.limit
    assert error.value.__notes__ == ['while processing array element 0']


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
    with pytest.raises(gm.ResourceLimitError) as error:
        line.buffer(1.0, join_style='round', quadrant_segments=8_000_000)
    assert (error.value.code, error.value.operation, error.value.parameter) == (
        'output_limit',
        'buffer',
        'quadrant_segments',
    )
    assert error.value.produced > error.value.limit


def test_point_buffer_counts_its_closing_coordinate_before_circle_allocation() -> None:
    """The exact 16M open circle still exceeds the realized closed polygon limit."""
    with pytest.raises(gm.ResourceLimitError) as error:
        gm.Point(0.0, 0.0).buffer(1.0, quadrant_segments=4_000_000)
    assert (error.value.code, error.value.operation, error.value.parameter) == (
        'output_limit',
        'buffer',
        'quadrant_segments',
    )
    assert error.value.produced > error.value.limit


def test_deep_single_member_collection_buffer_reaches_the_real_member_once() -> None:
    """One-child collection wrappers reach the member at the nesting boundary."""
    nested: gm.Geometry = gm.Point(0.0, 0.0)
    for _ in range(128):
        nested = gm.GeometryCollection([nested])
    assert nested.buffer(1.0).to_wkb() == gm.Point(0.0, 0.0).buffer(1.0).to_wkb()
