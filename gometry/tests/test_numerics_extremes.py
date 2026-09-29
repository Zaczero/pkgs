"""R15-T deterministic numerical-correctness regressions.

Every oracle is derived from the stored binary64 values, not the source
literals.  These fixtures name their intended execution path and avoid timing
assertions.
"""

from __future__ import annotations

import math
from fractions import Fraction
from itertools import pairwise

import gometry as gm
import numpy as np
import pytest


def _stored(value: float) -> Fraction:
    return Fraction(value)


def _assert_close_to_stored_reference(actual: float, expected: Fraction) -> None:
    reference = float(expected)
    assert math.isfinite(actual)
    assert actual == pytest.approx(reference, rel=2e-14, abs=0.0)


def _lineal_centroid_reference(
    runs: list[list[tuple[float, float]]],
) -> tuple[float, float]:
    total = Fraction()
    moment_x = Fraction()
    moment_y = Fraction()
    for run in runs:
        for (x0, y0), (x1, y1) in pairwise(run):
            length = Fraction(math.hypot(x1 - x0, y1 - y0))
            midpoint_x = Fraction(float((_stored(x0) + _stored(x1)) / 2))
            midpoint_y = Fraction(float((_stored(y0) + _stored(y1)) / 2))
            total += length
            moment_x += length * midpoint_x
            moment_y += length * midpoint_y
    return float(moment_x / total), float(moment_y / total)


def test_multiline_centroid_normalizes_finite_component_weights_before_sum() -> None:
    """Two finite segment lengths overflow only when their raw weights sum."""
    first = (0.0, 0.0), (1e308, 0.0)
    second = (0.0, 0.0), (0.0, 9e307)
    # This fixture must reach the lineal multi-component merge, not the areal
    # path or an infinite-length special case.
    assert math.isfinite(math.hypot(first[1][0], first[1][1]))
    assert math.isfinite(math.hypot(second[1][0], second[1][1]))
    assert math.isinf(
        math.hypot(first[1][0], first[1][1]) + math.hypot(second[1][0], second[1][1])
    )

    centroid = gm.MultiLineString([first, second]).centroid()
    first_weight = _stored(first[1][0])
    second_weight = _stored(second[1][1])
    total = first_weight + second_weight
    expected_x = first_weight * (_stored(first[1][0]) / 2) / total
    expected_y = second_weight * (_stored(second[1][1]) / 2) / total
    _assert_close_to_stored_reference(float(centroid.x), expected_x)
    _assert_close_to_stored_reference(float(centroid.y), expected_y)


def test_lineal_component_weight_underflow_preserves_min_subnormal_scalar() -> None:
    tiny = math.ulp(0.0)
    large = [(-5.0e307, 0.0), (5.0e307, 0.0)]
    small = [(1.0e308, 0.0), (1.0e308, tiny)]
    large_weight = Fraction(math.hypot(large[1][0] - large[0][0], 0.0))
    small_weight = Fraction(math.hypot(0.0, tiny))
    assert small_weight > 0
    assert float(small_weight / large_weight) == 0.0

    expected = _lineal_centroid_reference([large, small])
    assert expected == (tiny, 0.0)
    large_line = gm.LineString(large)
    small_line = gm.LineString(small)
    aggregates = [
        gm.MultiLineString([large, small]),
        gm.MultiLineString([small, large]),
        gm.GeometryCollection([large_line, small_line]),
        gm.GeometryCollection([small_line, large_line]),
        gm.GeometryCollection([
            gm.GeometryCollection([large_line]),
            gm.GeometryCollection([gm.GeometryCollection([small_line])]),
        ]),
        gm.GeometryCollection([
            gm.GeometryCollection([small_line]),
            gm.GeometryCollection([gm.GeometryCollection([large_line])]),
        ]),
    ]
    for aggregate in aggregates:
        centroid = aggregate.centroid()
        assert (float(centroid.x), float(centroid.y)) == expected


def test_lineal_merge_normalization_underflow_is_detected_in_fast_path() -> None:
    tiny = math.ulp(0.0)
    ordinary = [(0.0, 0.0), (0.0, 4.0)]
    small = [(4.0, 0.0), (4.0, tiny)]
    zero_length = [(100.0, 100.0), (100.0, 100.0)]
    assert Fraction(tiny) > 0
    assert float(Fraction(tiny) / 4) == 0.0

    expected = _lineal_centroid_reference([ordinary, small, zero_length])
    assert expected == (tiny, 2.0)

    ordinary_line = gm.LineString(ordinary)
    ordinary_reversed_line = gm.LineString(ordinary[::-1])
    small_line = gm.LineString(small)
    small_reversed_line = gm.LineString(small[::-1])
    zero_line = gm.LineString(zero_length)
    geometries = [
        gm.MultiLineString([ordinary, small, zero_length]),
        gm.MultiLineString([small, ordinary, zero_length]),
        gm.MultiLineString([ordinary[::-1], small[::-1], zero_length]),
        gm.MultiLineString([small[::-1], ordinary[::-1], zero_length]),
        gm.GeometryCollection([ordinary_line, small_line, zero_line]),
        gm.GeometryCollection([small_line, ordinary_line, zero_line]),
        gm.GeometryCollection([
            ordinary_reversed_line,
            small_reversed_line,
            zero_line,
        ]),
        gm.GeometryCollection([
            small_reversed_line,
            ordinary_reversed_line,
            zero_line,
        ]),
    ]
    for geometry in geometries:
        centroid = geometry.centroid()
        assert (float(centroid.x), float(centroid.y)) == expected

    dense = gm.GeometryArray(geometries)
    assert [(float(point.x), float(point.y)) for point in dense.centroid()] == [
        expected
    ] * len(geometries)
    gathered = dense[np.array([3, 1, 2], dtype=np.intp)]
    assert [(float(point.x), float(point.y)) for point in gathered.centroid()] == [
        expected,
        expected,
        expected,
    ]


def test_lineal_component_weight_underflow_preserves_dense_and_gathered_arrays() -> (
    None
):
    tiny = math.ulp(0.0)
    large = [(-5.0e307, 0.0), (5.0e307, 0.0)]
    small = [(1.0e308, 0.0), (1.0e308, tiny)]
    expected = _lineal_centroid_reference([large, small])
    ordinary = gm.MultiLineString([
        [(0.0, 0.0), (4.0, 0.0)],
        [(0.0, 2.0), (0.0, 6.0)],
    ])
    ordinary_expected = (1.0, 2.0)
    multi = gm.MultiLineString([large, small])
    multi_reversed = gm.MultiLineString([small, large])

    dense = gm.GeometryArray([multi, multi_reversed, ordinary, multi])
    dense_centroids = dense.centroid()
    expected_rows = [expected, expected, ordinary_expected, expected]
    assert [
        (float(point.x), float(point.y)) for point in dense_centroids
    ] == expected_rows

    gathered = dense[np.array([3, 0, 2], dtype=np.intp)]
    gathered_centroids = gathered.centroid()
    assert [(float(point.x), float(point.y)) for point in gathered_centroids] == [
        expected,
        expected,
        ordinary_expected,
    ]

    large_line = gm.LineString(large)
    small_line = gm.LineString(small)
    collections = gm.GeometryArray([
        gm.GeometryCollection([large_line, small_line]),
        gm.GeometryCollection([small_line, large_line]),
        gm.GeometryCollection([
            gm.GeometryCollection([large_line]),
            gm.GeometryCollection([gm.GeometryCollection([small_line])]),
        ]),
    ])
    assert [(float(point.x), float(point.y)) for point in collections.centroid()] == [
        expected
    ] * 3
    gathered_collections = collections[np.array([2, 0], dtype=np.intp)]
    assert [
        (float(point.x), float(point.y)) for point in gathered_collections.centroid()
    ] == [expected] * 2


def test_lineal_component_weight_underflow_keeps_zero_x_and_finite_order_controls() -> (
    None
):
    vertical = gm.MultiLineString([
        [(0.0, 0.0), (0.0, 1.0)],
        [(0.0, 1.0), (0.0, 2.0)],
    ])
    assert (float(vertical.centroid().x), float(vertical.centroid().y)) == (0.0, 1.0)

    parts = [
        [(0.0, 0.0), (4.0, 0.0)],
        [(0.0, 2.0), (0.0, 6.0)],
    ]
    for ordered in (parts, list(reversed(parts))):
        aggregate = gm.MultiLineString(ordered)
        assert (float(aggregate.centroid().x), float(aggregate.centroid().y)) == (
            1.0,
            2.0,
        )


def test_multiline_component_exact_rescue_keeps_finite_maximum_coordinate() -> None:
    maximum = float.fromhex('0x1.fffffffffffffp+1023')
    large_line = [(maximum, 0.0), (maximum, 4.0)]
    centered_line = [(0.0, 0.0), (0.0, 4.0)]
    expected = (maximum / 2, 2.0)

    aggregates = [
        gm.MultiLineString([large_line, centered_line]),
        gm.MultiLineString([centered_line, large_line]),
        gm.GeometryCollection([
            gm.LineString(large_line),
            gm.LineString(centered_line),
        ]),
        gm.GeometryCollection([
            gm.LineString(centered_line),
            gm.LineString(large_line),
        ]),
    ]
    for aggregate in aggregates:
        centroid = aggregate.centroid()
        assert (float(centroid.x), float(centroid.y)) == expected


def test_zero_length_collection_line_does_not_move_positive_length_frame() -> None:
    """Zero-measure coordinates cannot move a finite centroid into an error."""
    maximum = float.fromhex('0x1.fffffffffffffp+1023')
    anchor = math.ldexp(3.0, 970)
    minimum = float(2 * Fraction(anchor) - Fraction(maximum))
    zero_line = gm.LineString([(minimum, 0.0), (minimum, 0.0)])
    positive_line = gm.LineString([(maximum, -maximum), (maximum, maximum)])
    assert math.isinf(float(positive_line.length))
    expected = (float(Fraction(maximum)), 0.0)
    assert (float(positive_line.centroid().x), float(positive_line.centroid().y)) == expected

    geometries = [
        gm.GeometryCollection([zero_line, positive_line]),
        gm.GeometryCollection([positive_line, zero_line]),
    ]
    for geometry in geometries:
        centroid = geometry.centroid()
        assert (float(centroid.x), float(centroid.y)) == expected

    dense = gm.GeometryArray(geometries)
    assert [
        (float(point.x), float(point.y)) for point in dense.centroid()
    ] == [expected, expected]
    gathered = dense[np.array([1, 0], dtype=np.intp)]
    assert [
        (float(point.x), float(point.y)) for point in gathered.centroid()
    ] == [expected, expected]


def test_repeated_shared_collection_centroid_preserves_occurrence_weights() -> None:
    first = gm.box(0.0, 0.0, 2.0, 2.0)
    shared = gm.GeometryCollection([first])
    area = gm.GeometryCollection([
        gm.GeometryCollection([shared, gm.box(20.0, 0.0, 22.0, 2.0)]),
        gm.GeometryCollection([shared, gm.box(40.0, 0.0, 42.0, 2.0)]),
    ])
    line = gm.LineString([(0.0, 0.0), (2.0, 0.0)])
    lineal = gm.GeometryCollection([
        line,
        line,
        gm.LineString([(10.0, 0.0), (10.0, 4.0)]),
    ])
    points = gm.MultiPoint([(0.0, 0.0), (2.0, 0.0)])
    puntal = gm.GeometryCollection([points, points, gm.Point(10.0, 0.0)])
    expected = [(16.0, 1.0), (5.5, 1.0), (2.8, 0.0)]

    for geometry, answer in zip([area, lineal, puntal], expected, strict=True):
        centroid = geometry.centroid()
        assert (float(centroid.x), float(centroid.y)) == answer

    rows = gm.GeometryArray([area, lineal, puntal, None])
    assert [
        None if point is None else (float(point.x), float(point.y))
        for point in rows.centroid()
    ] == [*expected, None]
    gathered = rows[np.array([2, 0, 3, 1], dtype=np.intp)]
    assert [
        None if point is None else (float(point.x), float(point.y))
        for point in gathered.centroid()
    ] == [expected[2], expected[0], None, expected[1]]


def test_repeated_zero_length_lines_keep_puntal_anchors() -> None:
    zero_line = gm.LineString([(2.0, 4.0), (2.0, 4.0)])
    aggregate = gm.GeometryCollection([
        zero_line,
        zero_line,
        gm.Point(10.0, 0.0),
    ])

    centroid = aggregate.centroid()

    assert (float(centroid.x), float(centroid.y)) == (14.0 / 3.0, 8.0 / 3.0)


def _assert_line_centroid_exact(
    points: list[tuple[float, float]], expected: tuple[float, float]
) -> None:
    assert _lineal_centroid_reference([points]) == expected
    line = gm.LineString(points)
    centroids = (line.centroid(), gm.GeometryArray([line]).centroid()[0])
    assert [(float(centroid.x), float(centroid.y)) for centroid in centroids] == [
        expected,
        expected,
    ]


def test_translated_short_line_centroid_is_exact_when_reversed() -> None:
    magnitude = 2.0**500
    height = 2.0**445
    offset = 2.0**476
    cycle = [
        (offset, 0.0),
        (offset + magnitude, 0.0),
        (offset + magnitude, height),
        (offset - magnitude, height),
        (offset, 0.0),
    ]
    expected = (1.9510928441746322e143, 5.678427533559429e133)

    _assert_line_centroid_exact(list(reversed(cycle)), expected)


def test_translated_alternating_line_centroid_is_exact_through_simd_tail() -> None:
    magnitude = 2.0**500
    height = 2.0**445
    offset = 2.0**476
    cycle = [
        (offset, 0.0),
        (offset + magnitude, 0.0),
        (offset + magnitude, height),
        (offset - magnitude, height),
        (offset, 0.0),
    ]
    vertices = cycle[:-1]
    points = [vertices[0]]
    for repetition in range(1025):
        ordered = (
            [vertices[0], *reversed(vertices[1:]), vertices[0]]
            if repetition % 2
            else cycle
        )
        points.extend(ordered[1:])
    expected = (1.9510928441746322e143, 5.678427533559429e133)

    assert len(points) - 1 == 4100
    _assert_line_centroid_exact(points, expected)


@pytest.mark.parametrize('height', [1e-308, 1e-300, 1e-200])
def test_mixed_axis_rectangle_area_uses_independent_rescue_scales(
    height: float,
) -> None:
    """Huge-X/tiny-Y rectangle enters the overflowed closed-ring area rescue."""
    widths = (-1e308, 1e308)
    reference = (_stored(widths[1]) - _stored(widths[0])) * _stored(height)
    assert reference > 0
    polygon = gm.Polygon([
        (widths[0], 0.0),
        (widths[1], 0.0),
        (widths[1], height),
        (widths[0], height),
    ])
    _assert_close_to_stored_reference(float(polygon.area), reference)


@pytest.mark.parametrize('height', [1e-308, 1e-300, 1e-200])
def test_mixed_axis_rectangle_area_one_ulp_neighbours(height: float) -> None:
    """Both stored-double neighbours around every extreme-scale boundary work."""
    for neighbour in (math.nextafter(height, 0.0), math.nextafter(height, math.inf)):
        for left in (math.nextafter(-1e308, -math.inf), math.nextafter(-1e308, 0.0)):
            for right in (math.nextafter(1e308, 0.0), math.nextafter(1e308, math.inf)):
                reference = (_stored(right) - _stored(left)) * _stored(neighbour)
                polygon = gm.Polygon([
                    (left, 0.0),
                    (right, 0.0),
                    (right, neighbour),
                    (left, neighbour),
                ])
                _assert_close_to_stored_reference(float(polygon.area), reference)


def test_akl_toussaint_keeps_every_extreme_hull_vertex_at_its_256_point_gate() -> None:
    """The prefilter may discard only points analytically inside its extreme quad."""
    hull_vertices = [
        (-1e308, 0.0),
        (0.0, -1e308),
        (1e308, 0.0),
        (9e307, 9e307),
        (0.0, 1e308),
        (-9e307, 9e307),
    ]
    points = hull_vertices + [(0.0, 0.0)] * 250
    assert len(points) == 256
    hull = gm.MultiPoint(points).convex_hull()
    coordinates = {(float(x), float(y)) for x, y in hull.exterior.coords[:-1]}
    assert set(hull_vertices) <= coordinates


def test_akl_toussaint_keeps_maximum_finite_x_across_its_gate_neighbours() -> None:
    """The 256-point prefilter threshold cannot erase a near-max X vertex."""
    maximum_x = 1.7e308
    extremes = [(0.0, 0.0), (maximum_x, 0.0), (0.0, maximum_x)]
    for count in (255, 256, 257):
        hull = gm.MultiPoint(
            extremes + [(1.0, 1.0)] * (count - len(extremes))
        ).convex_hull()
        assert max(float(x) for x, _ in hull.exterior.coords) == maximum_x


def test_multi_component_centroid_keeps_unequal_unrepresentable_weights() -> None:
    """Infinite f64 weights are scaled mathematical ratios, never equal buckets."""
    low_line = [(-1e308, 0.0), (1e308, 0.0)]
    high_line = [(-9e307, 10.0), (9e307, 10.0)]
    lines = gm.MultiLineString([low_line, high_line])
    reversed_lines = gm.MultiLineString([high_line, low_line])
    collection = gm.GeometryCollection([
        gm.LineString(low_line),
        gm.LineString(high_line),
    ])
    reversed_collection = gm.GeometryCollection([
        gm.LineString(high_line),
        gm.LineString(low_line),
    ])
    assert math.isinf(gm.LineString(low_line).length)
    assert math.isinf(gm.LineString(high_line).length)
    low_weight = _stored(low_line[1][0]) - _stored(low_line[0][0])
    high_weight = _stored(high_line[1][0]) - _stored(high_line[0][0])
    expected_line_y = 10 * high_weight / (low_weight + high_weight)
    for aggregate in (lines, reversed_lines, collection, reversed_collection):
        packed = gm.GeometryArray([aggregate])
        for centroid in (aggregate.centroid(), packed.centroid()[0]):
            _assert_close_to_stored_reference(float(centroid.x), Fraction(0))
            _assert_close_to_stored_reference(float(centroid.y), expected_line_y)
            assert float(centroid.y) == pytest.approx(4.736842105263158, rel=2e-14)

    low = gm.box(-1e308, 0.0, 1e308, 2.0)
    high = gm.box(-1e308, 7.5, 1e308, 8.5)
    assert math.isinf(high.area)
    assert gm.MultiPolygon([low, high]).centroid().y == pytest.approx(
        10.0 / 3.0, rel=2e-14
    )


def test_polygon_area_uses_one_frame_for_shell_and_holes_scalar_and_packed() -> None:
    shell = [(-1e308, -1e308), (1e308, -1e308), (1e308, 1e308), (-1e308, 1e308)]
    hole = [(-1e307, -1e307), (1e307, -1e307), (1e307, 1e307), (-1e307, 1e307)]
    polygon = gm.Polygon(shell, [hole])
    packed = gm.GeometryArray([polygon])
    assert math.isinf(polygon.area) and polygon.area > 0.0
    assert math.isinf(float(packed.area[0])) and packed.area[0] > 0.0


def test_shared_polygon_frame_preserves_one_ulp_area_neighbours_and_centroid() -> None:
    """Exact shared-frame tails keep area and centroid coupled on both sides."""
    for exponent in (29, 28, 27):
        epsilon = 2.0**-exponent
        polygon = gm.Polygon([(0.0, 0.0), (1.0, 1.0 + epsilon), (1.0 - epsilon, 1.0)])
        packed = gm.GeometryArray([polygon])
        # All arithmetic is over the stored binary64 input, not source decimal
        # spelling. The triangle's signed shoelace is epsilon² / 2.
        expected_area = _stored(epsilon) * _stored(epsilon) / 2
        assert polygon.area == float(expected_area)
        assert float(packed.area[0]) == float(expected_area)
        expected_x = (2 - _stored(epsilon)) / 3
        expected_y = (2 + _stored(epsilon)) / 3
        for centroid in (polygon.centroid(), packed.centroid()[0]):
            _assert_close_to_stored_reference(float(centroid.x), expected_x)
            _assert_close_to_stored_reference(float(centroid.y), expected_y)


def _stored_ring_area(vertices: list[tuple[float, float]]) -> Fraction:
    """Analytic shoelace area from the actual binary64 coordinates."""
    total = Fraction()
    for left, right in zip(vertices, vertices[1:] + vertices[:1], strict=True):
        total += _stored(left[0]) * _stored(right[1]) - _stored(right[0]) * _stored(
            left[1]
        )
    return abs(total) / 2


def _assert_exact_centroid(
    geometry, expected_x: Fraction, expected_y: Fraction
) -> None:
    values = [geometry.centroid(), gm.GeometryArray([geometry]).centroid()[0]]
    for centroid in values:
        assert float(centroid.x) == float(expected_x)
        assert float(centroid.y) == float(expected_y)


def test_shared_centroid_retains_primary_residual_when_reversed() -> None:
    """The primary residual survives both scalar/packed paths and ring reversal."""
    k = 2**50
    vertices = [(-k, 0), (k, 0), (1, k)]
    expected = Fraction(1, 3), Fraction(k, 3)
    polygon = gm.Polygon(vertices)
    _assert_exact_centroid(polygon, *expected)
    _assert_exact_centroid(gm.Polygon(list(reversed(vertices))), *expected)


def test_shared_centroid_retains_translated_primary_residual() -> None:
    k = 2**50
    vertices = [(-k, 0), (k, 0), (1, k)]
    translated = gm.Polygon([(x + k, y - k) for x, y in vertices])
    _assert_exact_centroid(
        translated, Fraction(k) + Fraction(1, 3), Fraction(-2 * k, 3)
    )


def test_shared_centroid_uses_exact_hole_moments_and_canonical_zero() -> None:
    k = 2**50
    vertices = [(-k, 0), (k, 0), (1, k)]
    shell = [(-2 * k, -k), (2 * k, -k), (2 * k, 2 * k), (-2 * k, 2 * k)]
    holed = gm.Polygon(shell, [vertices])
    _assert_exact_centroid(holed, Fraction(-1, 33), Fraction(17 * k, 33))
    centered_hole = gm.Polygon(shell, [[(-k, 0), (k, 0), (0, k)]])
    _assert_exact_centroid(centered_hole, Fraction(0), Fraction(17 * k, 33))
    assert float(centered_hole.centroid().x).hex() == '0x0.0p+0'


def test_shared_centroid_recovers_residual_lost_before_final_snap() -> None:
    cancellation_k = 2**54
    cancellation = gm.Polygon([
        (-cancellation_k, 0),
        (cancellation_k, 0),
        (1, cancellation_k),
    ])
    _assert_exact_centroid(cancellation, Fraction(1, 3), Fraction(cancellation_k, 3))


def test_shared_centroid_multipart_reuses_exact_component_centroids() -> None:
    k = 2**50
    polygon = gm.Polygon([(-k, 0), (k, 0), (1, k)])
    expected = Fraction(1, 3), Fraction(k, 3)
    for aggregate in (
        gm.MultiPolygon([polygon, polygon]),
        gm.GeometryCollection([polygon, polygon]),
    ):
        _assert_exact_centroid(aggregate, *expected)


def test_finite_cancellation_area_rescue_is_rotation_and_multipart_invariant() -> None:
    """A finite but cancellation-damaged shoelace must take the exact frame."""
    epsilon = 2.0**-27
    vertices = [(0.0, 0.0), (1.0, 1.0 + epsilon), (1.0 - epsilon, 1.0)]
    expected_area = _stored_ring_area(vertices)
    assert expected_area == Fraction(1, 2**55)

    polygons = []
    for start in range(len(vertices)):
        rotated = vertices[start:] + vertices[:start]
        polygon = gm.Polygon(rotated)
        polygons.append(polygon)
        for actual in (polygon.area, float(gm.GeometryArray([polygon]).area[0])):
            assert actual == float(expected_area)

    translated = [(x + 4.0, y) for x, y in vertices]
    translated_polygon = gm.Polygon(translated)
    expected_total_area = expected_area * 2
    expected_x = (
        sum(_stored(x) for x, _ in vertices) / 3
        + sum(_stored(x) for x, _ in translated) / 3
    ) / 2
    expected_y = (
        sum(_stored(y) for _, y in vertices) / 3
        + sum(_stored(y) for _, y in translated) / 3
    ) / 2
    # Both collection orders must preserve the analytic aggregate.  This is
    # deliberately separate from cyclic rotation: the merge path has its own
    # opportunity to lose a finite answer.
    for aggregate in (
        gm.MultiPolygon([polygons[2], translated_polygon]),
        gm.MultiPolygon([translated_polygon, polygons[1]]),
        gm.GeometryCollection([polygons[0], translated_polygon]),
        gm.GeometryCollection([translated_polygon, polygons[2]]),
    ):
        packed = gm.GeometryArray([aggregate])
        assert aggregate.area == float(expected_total_area)
        assert float(packed.area[0]) == float(expected_total_area)
        for centroid in (aggregate.centroid(), packed.centroid()[0]):
            _assert_close_to_stored_reference(float(centroid.x), expected_x)
            _assert_close_to_stored_reference(float(centroid.y), expected_y)


def test_finite_area_rescue_is_rotation_invariant_across_coordinate_magnitudes() -> (
    None
):
    """The lost-answer guard follows arithmetic, from tiny to near-max axes."""
    epsilon = 2.0**-27
    for exponent in (-1022, -900, -500, 0, 500, 900, 1022):
        x = math.ldexp(1.0, exponent)
        y = math.ldexp(1.0, -exponent)
        vertices = [(0.0, 0.0), (x, y * (1.0 + epsilon)), (x * (1.0 - epsilon), y)]
        expected_area = _stored_ring_area(vertices)
        assert expected_area == Fraction(1, 2**55)
        expected_x = sum(_stored(value) for value, _ in vertices) / 3
        expected_y = sum(_stored(value) for _, value in vertices) / 3
        polygons = []
        for start in range(len(vertices)):
            polygon = gm.Polygon(vertices[start:] + vertices[:start])
            polygons.append(polygon)
            packed = gm.GeometryArray([polygon])
            assert polygon.area == float(expected_area)
            assert float(packed.area[0]) == float(expected_area)
            for centroid in (polygon.centroid(), packed.centroid()[0]):
                _assert_close_to_stored_reference(float(centroid.x), expected_x)
                _assert_close_to_stored_reference(float(centroid.y), expected_y)
        # A multipolygon's parts are measured as stored; a collection's
        # members may validly overlap, so a duplicate counts once.
        for aggregate, area in (
            (gm.MultiPolygon([polygons[0], polygons[2]]), 2 * expected_area),
            (gm.GeometryCollection([polygons[1], polygons[0]]), expected_area),
        ):
            packed = gm.GeometryArray([aggregate])
            assert aggregate.area == float(area)
            assert float(packed.area[0]) == float(area)
            for centroid in (aggregate.centroid(), packed.centroid()[0]):
                _assert_close_to_stored_reference(float(centroid.x), expected_x)
                _assert_close_to_stored_reference(float(centroid.y), expected_y)


def test_line_centroid_rescue_covers_subnormal_to_near_max_magnitudes() -> None:
    """Finite nonzero quotient underflow is loss, not a successful centroid."""
    for exponent in (-1074, -900, -600, -300, 0, 500, 900, 1022):
        midpoint = math.ldexp(1.0, exponent)
        endpoint = midpoint * 2.0
        line = gm.LineString([(0.0, 0.0), (endpoint, endpoint)])
        values = (
            line.centroid(),
            gm.GeometryArray([line]).centroid()[0],
            gm.MultiLineString([line]).centroid(),
            gm.GeometryCollection([line]).centroid(),
        )
        for centroid in values:
            assert float(centroid.x) == midpoint
            assert float(centroid.y) == midpoint


def test_huge_by_min_subnormal_area_unscales_once_through_neighbours() -> None:
    """A normal final area may never lose bits in a subnormal intermediate."""
    min_subnormal = math.ulp(0.0)
    for height in (min_subnormal, math.nextafter(min_subnormal, math.inf)):
        for left in (math.nextafter(-1e308, -math.inf), math.nextafter(-1e308, 0.0)):
            for right in (math.nextafter(1e308, 0.0), math.nextafter(1e308, math.inf)):
                polygon = gm.Polygon([
                    (left, 0.0),
                    (right, 0.0),
                    (right, height),
                    (left, height),
                ])
                expected = (_stored(right) - _stored(left)) * _stored(height)
                _assert_close_to_stored_reference(float(polygon.area), expected)


def test_opposite_extreme_line_components_have_finite_zero_centroid_scalar_and_packed() -> (
    None
):
    """Centered online weights must not form an opposite-extremes infinity."""
    for extent in (1e308, math.nextafter(1e308, 0.0)):
        lines = gm.MultiLineString([
            [(extent, -extent), (extent, extent)],
            [(-extent, -extent), (-extent, extent)],
        ])
        packed = gm.GeometryArray([lines])
        for centroid in (lines.centroid(), packed.centroid()[0]):
            assert float(centroid.x) == 0.0
            assert float(centroid.y) == 0.0
