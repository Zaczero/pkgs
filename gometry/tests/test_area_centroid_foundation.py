"""R15-J: area/centroid foundation (C2 + N6).

Acceptance (exact Fraction references from stored doubles only):
1. +/-1e308 x +/-2 rectangle: area +inf, centroid (0, 0), contains interior box,
   nonempty union/difference/symmetric_difference.
2. Same geometry meaning across the ≥128-vertex implementation crossover.
3. Equal boxes at offsets +/-1e9/1e12/1e15 -> centroid x = 0.5 under every
   component-order permutation.
4. Equal points / equal-length lines at [1e16, 1, -1e16] -> centroid x = 1/3
   under every permutation.
5. Already-landed tiny-ring and mixed-axis ring decisions stay green.
"""

from __future__ import annotations

import math
from fractions import Fraction
from itertools import pairwise, permutations

import gometry as gm
import pytest


def _stored(x: float) -> Fraction:
    """Exact rational of a value as stored in f64 (never the typed literal)."""
    return Fraction(float(x))


def _extreme_rect() -> gm.Polygon:
    return gm.box(-1e308, -2.0, 1e308, 2.0)


def _interior_box() -> gm.Polygon:
    return gm.box(-0.5, -0.5, 0.5, 0.5)


def _densify_extreme_rect(n_per_edge: int) -> gm.Polygon:
    """Same +/-1e308 x +/-2 rectangle with collinear edge samples (scale-safe lerp)."""
    corners = [
        (-1e308, -2.0),
        (1e308, -2.0),
        (1e308, 2.0),
        (-1e308, 2.0),
    ]
    pts: list[tuple[float, float]] = []
    for i in range(4):
        x0, y0 = corners[i]
        x1, y1 = corners[(i + 1) % 4]
        pts.append((x0, y0))
        for k in range(1, n_per_edge + 1):
            t = k / (n_per_edge + 1)
            sc = math.ldexp(1.0, -1022)
            x = ((1.0 - t) * (x0 * sc) + t * (x1 * sc)) / sc
            y = y0 + t * (y1 - y0)
            assert math.isfinite(x) and math.isfinite(y)
            pts.append((x, y))
    pts.append(pts[0])
    return gm.Polygon(pts)


def test_c2_extreme_rectangle_area_centroid_contains_overlays() -> None:
    """C2: +/-1e308 x +/-2 rect has +inf area, centroid (0,0), contains, nonempty overlays."""
    rect = _extreme_rect()
    interior = _interior_box()

    area = rect.area
    assert math.isinf(area) and area > 0.0 and not math.isnan(area)

    c = rect.centroid()
    assert c.x == 0.0 and c.y == 0.0

    assert gm.contains(rect, interior) is True

    u = gm.union(rect, interior)
    d = gm.difference(rect, interior)
    x = gm.symmetric_difference(rect, interior)
    assert u.is_empty is False
    assert d.is_empty is False
    assert x.is_empty is False


@pytest.mark.parametrize('n_per_edge', [0, 30, 31, 32, 33])
def test_c2_extreme_rect_invariant_across_vertex_crossover(n_per_edge: int) -> None:
    """Geometry meaning must not depend on redundant vertex count (≥128 path)."""
    poly = _densify_extreme_rect(n_per_edge)
    nverts = len(list(poly.exterior.coords))
    # n_per_edge=0 -> 5 verts; 30 -> 125; 31 -> 129 (straddles the 128 gate).
    if n_per_edge == 0:
        assert nverts == 5
    elif n_per_edge == 30:
        assert nverts == 125
    elif n_per_edge == 31:
        assert nverts == 129

    interior = _interior_box()
    area = poly.area
    assert math.isinf(area) and area > 0.0

    c = poly.centroid()
    assert c.x == 0.0 and c.y == 0.0

    assert gm.contains(poly, interior) is True
    assert gm.union(poly, interior).is_empty is False
    assert gm.difference(poly, interior).is_empty is False
    assert gm.symmetric_difference(poly, interior).is_empty is False


@pytest.mark.parametrize('offset', [1e9, 1e12, 1e15])
def test_n6_equal_boxes_centroid_order_invariant(offset: float) -> None:
    """N6: three equal unit boxes -> exact centroid x=0.5 under every order."""
    boxes = [
        gm.box(offset, 0.0, offset + 1.0, 1.0),
        gm.box(0.0, 0.0, 1.0, 1.0),
        gm.box(-offset, 0.0, -offset + 1.0, 1.0),
    ]
    # Exact mean of the three component centroids as stored doubles.
    component_xs = [b.centroid().x for b in boxes]
    exact = sum(_stored(x) for x in component_xs) / 3
    assert float(exact) == 0.5

    for perm in permutations(range(3)):
        mp = gm.MultiPolygon([boxes[i] for i in perm])
        c = mp.centroid()
        assert c.x == float(exact), f'perm={perm} got {c.x!r} expected {float(exact)!r}'
        assert c.y == 0.5


def test_n6_equal_points_centroid_order_invariant() -> None:
    """N6: MultiPoint at [1e16, 1, -1e16] -> x=1/3 under every permutation."""
    coords = [1e16, 1.0, -1e16]
    for perm in permutations(range(3)):
        xs = [coords[i] for i in perm]
        exact = sum(_stored(x) for x in xs) / 3
        c = gm.MultiPoint([(x, 0.0) for x in xs]).centroid()
        assert c.x == float(exact)
        assert abs(c.x - 1.0 / 3.0) < 1e-15


def test_n6_equal_length_lines_centroid_order_invariant() -> None:
    """N6: equal-length vertical lines at [1e16, 1, -1e16] -> x=1/3 every order."""
    coords = [1e16, 1.0, -1e16]
    for perm in permutations(range(3)):
        xs = [coords[i] for i in perm]
        exact = sum(_stored(x) for x in xs) / 3
        lines = [gm.LineString([(x, 0.0), (x, 1.0)]) for x in xs]
        c = gm.MultiLineString(lines).centroid()
        assert c.x == float(exact)
        assert abs(c.x - 1.0 / 3.0) < 1e-15


def test_tiny_ring_and_mixed_axis_remain_green() -> None:
    """Already-landed tiny-ring / mixed-axis decisions must stay green."""
    s = 1e-162
    tiny = gm.Polygon([(0.0, 0.0), (s, 0.0), (s, s), (0.0, s), (0.0, 0.0)])
    assert tiny.is_valid is True

    a, b = 1e300, 1e-300
    mixed = gm.Polygon([(0.0, 0.0), (a, 0.0), (a, b), (0.0, b), (0.0, 0.0)])
    assert mixed.is_valid is True
    assert mixed.area == pytest.approx(1.0, rel=0, abs=0.0)
    assert float(_stored(a) * _stored(b)) == pytest.approx(mixed.area, rel=0, abs=1e-15)


def test_exact_product_tails_keep_near_collinear_polygon_topology() -> None:
    """A closed stored-double ring must not lose its nonzero exact tails.

    The rounded shoelace terms are all zero, so this enters the native exact
    tail fallback rather than the ordinary decision filter. The reference is
    built only from the doubles passed to the constructor.
    """
    epsilon = 2.0**-27
    vertices = [
        (0.0, 0.0),
        (1.0, 1.0 + epsilon),
        (1.0 - epsilon, 1.0),
        (0.0, 0.0),
    ]
    rounded_terms = [x0 * y1 - x1 * y0 for (x0, y0), (x1, y1) in pairwise(vertices)]
    assert rounded_terms == [0.0, 0.0, 0.0]
    twice_area = sum(
        _stored(x0) * _stored(y1) - _stored(x1) * _stored(y0)
        for (x0, y0), (x1, y1) in pairwise(vertices)
    )
    assert twice_area == Fraction(1, 1 << 54)

    polygon = gm.Polygon(vertices)
    assert polygon.area == float(twice_area / 2)
    assert polygon.exterior.is_ccw is True
    assert (polygon | polygon).is_empty is False
    assert gm.covers(polygon, polygon.point_on_surface()) is True


def test_ordinary_area_centroid_bit_identity() -> None:
    """Ordinary-magnitude results stay on the established bit path."""
    assert gm.box(0.0, 0.0, 2.0, 3.0).area == 6.0
    c = gm.box(0.0, 0.0, 2.0, 4.0).centroid()
    assert c.x == 1.0 and c.y == 2.0


def test_shared_scale_off_center_triangle_not_snapped_to_bbox_mid() -> None:
    """Off-center shapes at large absolute coords must not snap to bbox midpoint.

    Shared-scale routing engages for max_abs > 1e12. A right triangle with legs
    of length 3 at offset 1e15 has true areal centroid offset from the bbox mid
    by 0.5 in each axis — a real residual that a world multi-ULP snap would
    wrongly discard.
    """
    o = 1e15
    # Store doubles first; build Fraction refs only from stored values.
    x0 = float(o)
    x1 = float(o + 3.0)
    y0 = 0.0
    y1 = 3.0
    tri = gm.Polygon([(x0, y0), (x1, y0), (x0, y1), (x0, y0)])
    c = tri.centroid()
    # Right triangle centroid = vertex mean = (2*x0 + x1)/3, (2*y0 + y1)/3.
    exact_x = (2 * _stored(x0) + _stored(x1)) / 3
    exact_y = (2 * _stored(y0) + _stored(y1)) / 3
    assert c.x == float(exact_x)
    assert c.y == float(exact_y)
    # Must NOT equal the bbox midpoint (the false snap target).
    mid_x = float((_stored(x0) + _stored(x1)) / 2)
    mid_y = float((_stored(y0) + _stored(y1)) / 2)
    assert c.x != mid_x
    assert c.y != mid_y
