"""R21-A generic shape certificates require complete, finite premises."""

from __future__ import annotations

import math
from fractions import Fraction

import gometry as gm
import pytest


def _carrier_results(left: gm.Geometry, right: gm.Geometry) -> list[float]:
    left_array = gm.GeometryArray([left])
    right_array = gm.GeometryArray([right])
    return [
        gm.hausdorff_distance(left, right),
        gm.hausdorff_distance(left, right_array).tolist()[0],
        gm.hausdorff_distance(left_array, right).tolist()[0],
        gm.hausdorff_distance(left_array, right_array).tolist()[0],
        gm.hausdorff_distance(
            gm.GeometryCollection([left]), gm.GeometryCollection([right])
        ),
    ]


def test_translation_certificate_preserves_multiline_structure() -> None:
    a, b, c, d, e = (0, 0), (1, 0), (1, 10), (10, 10), (10, 11)
    left = gm.MultiLineString([[a, b], [c, d, e]])
    right = gm.MultiLineString([[a, b, c], [d, e]])
    forward = _carrier_results(left, right)
    reverse = _carrier_results(right, left)
    assert forward == [5.0] * len(forward)
    assert reverse == [5.0] * len(reverse)


def test_translation_certificate_ignores_duplicate_vertex_distribution() -> None:
    points = [(float(index), float((index * 17) % 31)) for index in range(251)]
    join = len(points) // 2
    flat = points[:join] + [points[join]] * 4 + points[join + 1 :]
    delta = 0.125
    left_split = join + 2
    right_split = join + 3
    left = gm.MultiLineString([flat[:left_split], flat[left_split:]])
    shifted = [(x, y + delta) for x, y in flat]
    right = gm.MultiLineString([shifted[:right_split], shifted[right_split:]])
    assert _carrier_results(left, right) == [delta] * 5
    assert _carrier_results(right, left) == [delta] * 5


@pytest.mark.parametrize('count', [4, 7])
@pytest.mark.parametrize('separation', [1e-150, 1e-200, 5e-324])
def test_manufactured_normal_residual_falls_back_to_source_distance(
    count: int, separation: float
) -> None:
    xs = [index / (count - 1) for index in range(count)]
    ys = [separation] * count
    ys[count // 2] = 2.0 * separation
    left = gm.LineString([(x, 0.0) for x in xs])
    right = gm.LineString(list(zip(xs, ys, strict=True)))
    expected = float(Fraction.from_float(separation) * 2)
    assert _carrier_results(left, right) == [expected] * 5
    assert _carrier_results(right, left) == [expected] * 5


def test_small_target_root_capacity_is_proved_at_ordinary_scale() -> None:
    source = gm.LineString([(-1, 0), (3, -3), (-5, -1), (2, -2), (0, -2)])
    target = gm.LineString([(2, -2), (-5, -4)])
    expected = 2.910427500435995
    assert _carrier_results(source, target) == [expected] * 5
    assert _carrier_results(target, source) == [expected] * 5


@pytest.mark.parametrize('exponent', range(150, 206))
def test_normal_squared_residual_is_not_a_completeness_certificate(
    exponent: int,
) -> None:
    length = 10.0**exponent
    source = gm.LineString([(-length, 0.0), (length, 0.0)])
    target = gm.MultiPoint([(-length, 1.0), (length / 7.0, 1.0), (length, 1.0)])
    expected = float(
        (Fraction.from_float(target[1].x) - Fraction.from_float(target[0].x)) / 2
    )
    for first, second in ((source, target), (target, source)):
        results = _carrier_results(first, second)
        assert results == [results[0]] * len(results)
        assert all(math.isfinite(result) for result in results)
        assert results == [expected] * len(results)


@pytest.mark.parametrize(
    'exponent', [-550, -540, -500, -450, -300, -1, 0, 1, 300, 450, 500, 520, 550]
)
def test_hausdorff_exact_power_of_two_similarity_homogeneity(exponent: int) -> None:
    left_points = [(0.0, 0.0), (1.0, 0.5), (2.0, -0.25)]
    right_points = [(0.0, 1.0), (0.75, -0.5), (2.0, 0.75)]
    baseline = gm.hausdorff_distance(
        gm.LineString(left_points), gm.LineString(right_points)
    )
    scale = 2.0**exponent
    left = gm.LineString([(x * scale, y * scale) for x, y in left_points])
    right = gm.LineString([(x * scale, y * scale) for x, y in right_points])
    expected = float(Fraction.from_float(baseline) * Fraction.from_float(scale))
    for first, second in ((left, right), (right, left)):
        assert _carrier_results(first, second) == [expected] * 5
