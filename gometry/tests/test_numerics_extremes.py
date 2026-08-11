"""R15-T deterministic numerical-correctness regressions.

Every oracle is derived from the stored binary64 values, not the source
literals.  These fixtures name their intended execution path and avoid timing
assertions.
"""

from __future__ import annotations

import math
from fractions import Fraction

import gometry as gm
import pytest


def _stored(value: float) -> Fraction:
    return Fraction.from_float(value)


def _assert_close_to_stored_reference(actual: float, expected: Fraction) -> None:
    reference = float(expected)
    assert math.isfinite(actual)
    assert actual == pytest.approx(reference, rel=2e-14, abs=0.0)


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
    assert math.isinf(gm.LineString(low_line).length)
    assert math.isinf(gm.LineString(high_line).length)
    low_weight = _stored(low_line[1][0]) - _stored(low_line[0][0])
    high_weight = _stored(high_line[1][0]) - _stored(high_line[0][0])
    expected_line_y = 10 * high_weight / (low_weight + high_weight)
    _assert_close_to_stored_reference(float(lines.centroid().y), expected_line_y)
    assert float(lines.centroid().y) == pytest.approx(4.736842105263158, rel=2e-14)

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
        for aggregate in (
            gm.MultiPolygon([polygons[0], polygons[2]]),
            gm.GeometryCollection([polygons[1], polygons[0]]),
        ):
            packed = gm.GeometryArray([aggregate])
            assert aggregate.area == float(expected_area * 2)
            assert float(packed.area[0]) == float(expected_area * 2)
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
