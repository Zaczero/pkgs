from __future__ import annotations

import math
from fractions import Fraction

import gometry as gm


def _fraction(value: float) -> Fraction:
    return Fraction.from_float(value)


def test_mixed_scale_diagonal_predicates_and_overlay_use_exact_turn() -> None:
    mu = math.ulp(0.0)
    length = math.ldexp(1.0, -20)
    a = (0.0, 0.0)
    b = (length, length)
    c = (mu, 2.0 * mu)
    determinant = (_fraction(b[0]) - _fraction(a[0])) * (
        _fraction(c[1]) - _fraction(a[1])
    ) - (_fraction(b[1]) - _fraction(a[1])) * (_fraction(c[0]) - _fraction(a[0]))
    assert determinant == Fraction(1, 1 << 1094) > 0

    line = gm.LineString([a, b])
    point = gm.Point(*c)
    assert not gm.intersects(line, point)
    assert not gm.covers(line, point)
    assert not gm.contains(line, point)

    right = gm.LineString([(0.0, mu), (0.0, 2.0 * mu)])
    assert not gm.intersects(line, right)
    assert not gm.touches(line, right)
    assert gm.disjoint(line, right)
    assert gm.distance(line, right) == mu
    assert gm.intersection(line, right).is_empty


def test_wrong_nonzero_ray_filter_declines_to_exact_orientation() -> None:
    mu = math.ulp(0.0)
    a = (1.0, -4096 * mu)
    b = (-(2.0**-12), 6144 * mu)
    point = (float.fromhex('0x1.9973333333333p-2'), 2048 * mu)
    d = (-1.0, a[1])
    determinant = (_fraction(b[0]) - _fraction(a[0])) * (
        _fraction(point[1]) - _fraction(a[1])
    ) - (_fraction(b[1]) - _fraction(a[1])) * (_fraction(point[0]) - _fraction(a[0]))
    assert determinant == Fraction(1, 1 << 1117) > 0

    polygon = gm.Polygon([a, b, d, a])
    probe = gm.Point(*point)
    assert gm.contains(polygon, probe)
    assert gm.covers(polygon, probe)
    assert gm.intersects(polygon, probe)
    assert not gm.disjoint(polygon, probe)
    assert gm.distance(polygon, probe) == 0.0
    assert polygon.prepare().contains(probe)


def test_simd_ray_filter_accepts_exactly_valid_hole() -> None:
    mu = math.ulp(0.0)
    a = (1.0, -4096 * mu)
    b = (-(2.0**-12), 6144 * mu)
    point = (float.fromhex('0x1.9973333333333p-2'), 2048 * mu)
    d = (-1.0, a[1])
    shell = [a, b, d]
    shell.extend(
        (x := -1.0 + 2.0 * index / 254.0, a[1] - (1.0 - x * x))
        for index in range(1, 254)
    )
    shell.append(a)
    assert len(shell) - 1 == 256
    hole = [point, (0.2, 1024 * mu), (0.2, 3072 * mu), point]
    polygon = gm.Polygon(shell, holes=[hole])
    assert polygon.is_valid
    assert len(polygon.triangulate(method='earcut')) == 259


def test_noncollinear_subnormal_triangle_has_polygonal_hulls() -> None:
    mu = math.ulp(0.0)
    length = math.ldexp(1.0, -20)
    points = gm.MultiPoint([(0.0, 0.0), (length, length), (mu, 2.0 * mu)])
    assert points.convex_hull().geometry_type == 'Polygon'
    assert points.concave_hull().geometry_type == 'Polygon'
    assert points.minimum_rotated_rectangle().geometry_type == 'Polygon'


def test_hausdorff_preserves_underflowed_local_determinant() -> None:
    length = 2.0**996
    height = 2.0**396
    offset = 2.0**344
    baseline = gm.LineString([(0.0, 0.0), (length, height)])
    graph = gm.LineString([(0.0, 0.0), (height, 0.0), (length, height + offset)])
    assert gm.hausdorff_distance(baseline, graph) == offset
    assert gm.hausdorff_distance(graph, baseline) == offset


def test_spade_rejection_returns_to_certified_source_delaunay() -> None:
    mu = math.ulp(0.0)
    points = [
        (0.00, 257 * mu),
        (0.04, 1.0),
        (0.16, 154 * mu),
        (0.40, 109 * mu),
        (0.72, -209 * mu),
        (0.92, 1.375),
        (1.00, -19 * mu),
    ]
    hull = gm.MultiPoint(points).concave_hull(concavity=0.0, length_threshold=0.0)
    point_index = {point: index for index, point in enumerate(points)}
    cycle = tuple(point_index[tuple(point)] for point in list(hull.coords)[:-1])

    def canonical(values: tuple[int, ...]) -> tuple[int, ...]:
        variants = []
        for direction in (values, tuple(reversed(values))):
            variants.extend(
                direction[offset:] + direction[:offset] for offset in range(len(values))
            )
        return min(variants)

    expected_cycle = (0, 2, 3, 1, 5, 6, 4)
    assert canonical(cycle) == expected_cycle
    twice_area = Fraction(0)
    for left, right in zip(
        expected_cycle,
        expected_cycle[1:] + expected_cycle[:1],
        strict=True,
    ):
        left_x, left_y = map(_fraction, points[left])
        right_x, right_y = map(_fraction, points[right])
        twice_area += left_x * right_y - right_x * left_y
    exact_area = abs(twice_area) / 2
    assert float(exact_area).hex() == '0x1.d70a3d70a3d71p-1'
    assert hull.area == float(exact_area) == 0.92
