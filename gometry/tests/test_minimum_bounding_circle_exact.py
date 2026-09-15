"""Independent exact oracle tests for planar minimum enclosing circles."""

import math
import struct
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal, localcontext
from fractions import Fraction
from itertools import combinations, permutations

import gometry as gm
import numpy as np
import pytest

Point = tuple[Fraction, Fraction]


def q(point: tuple[float, float] | tuple[int, int]) -> Point:
    return (Fraction(float(point[0])), Fraction(float(point[1])))


def distance_squared(left: Point, right: Point) -> Fraction:
    return sum((a - b) ** 2 for a, b in zip(left, right, strict=True))


def orientation(a: Point, b: Point, c: Point) -> Fraction:
    return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])


@dataclass(frozen=True)
class OracleCircle:
    center: Point
    radius_squared: Fraction
    support: tuple[Point, ...]


def through(support: tuple[Point, ...]) -> OracleCircle | None:
    if len(support) == 1:
        return OracleCircle(support[0], Fraction(0), support)
    a, b = support[:2]
    if len(support) == 2:
        center = ((a[0] + b[0]) / 2, (a[1] + b[1]) / 2)
        return OracleCircle(center, distance_squared(center, a), support)
    c = support[2]
    bx, by = 2 * (b[0] - a[0]), 2 * (b[1] - a[1])
    cx, cy = 2 * (c[0] - a[0]), 2 * (c[1] - a[1])
    determinant = bx * cy - by * cx
    if not determinant:
        return None
    a_squared = distance_squared(a, (Fraction(0), Fraction(0)))
    b_squared = distance_squared(b, (Fraction(0), Fraction(0)))
    c_squared = distance_squared(c, (Fraction(0), Fraction(0)))
    center = (
        (b_squared - a_squared) * cy - (c_squared - a_squared) * by,
        bx * (c_squared - a_squared) - cx * (b_squared - a_squared),
    )
    center = (center[0] / determinant, center[1] / determinant)
    return OracleCircle(center, distance_squared(center, a), support)


def is_optimal_support(circle: OracleCircle) -> bool:
    if any(
        distance_squared(circle.center, point) != circle.radius_squared
        for point in circle.support
    ):
        return False
    if len(circle.support) != 3:
        return True
    a, b, c = circle.support
    area = orientation(a, b, c)
    return all(
        orientation(left, right, circle.center) * area >= 0
        for left, right in ((a, b), (b, c), (c, a))
    )


def is_certificate(circle: OracleCircle, points: tuple[Point, ...]) -> bool:
    return is_optimal_support(circle) and all(
        distance_squared(circle.center, point) <= circle.radius_squared
        for point in points
    )


def exhaustive(
    points: Iterable[tuple[float, float] | tuple[int, int]],
) -> OracleCircle | None:
    unique = tuple(dict.fromkeys(q(point) for point in points))
    best: OracleCircle | None = None
    for support_size in (1, 2, 3):
        for support in combinations(unique, support_size):
            candidate = through(support)
            if candidate is None or (
                best is not None and candidate.radius_squared >= best.radius_squared
            ):
                continue
            if all(
                distance_squared(candidate.center, point) <= candidate.radius_squared
                for point in unique
            ):
                best = candidate
    if best is not None:
        assert is_certificate(best, unique)
    return best


def round_sqrt(value: Fraction) -> float:
    """Round an exact non-negative rational square root to binary64."""
    with localcontext() as context:
        context.prec = 160
        proposal = float((Decimal(value.numerator) / Decimal(value.denominator)).sqrt())
    if math.isinf(proposal):
        overflow_midpoint = Fraction(sys_float_max()) + Fraction(2) ** 970
        return math.inf if value >= overflow_midpoint**2 else sys_float_max()
    while Fraction(proposal) ** 2 > value:
        proposal = math.nextafter(proposal, 0.0)
    while (
        math.isfinite(successor := math.nextafter(proposal, math.inf))
        and Fraction(successor) ** 2 <= value
    ):
        proposal = successor
    if not math.isfinite(successor):
        return proposal
    midpoint = (Fraction(proposal) + Fraction(successor)) / 2
    if value < midpoint**2:
        return proposal
    if value > midpoint**2:
        return successor
    return (
        proposal
        if (struct.unpack('>Q', struct.pack('>d', proposal))[0] & 1) == 0
        else successor
    )


def sys_float_max() -> float:
    return float.fromhex('0x1.fffffffffffffp+1023')


CASES: dict[str, list[tuple[float, float] | tuple[int, int]]] = {
    'one': [(3, 4)],
    'duplicates-signedzero': [(0.0, -0.0), (-0.0, 0.0), (0.0, 0.0)],
    'diameter': [(0, 0), (4, 0)],
    'adjacent-at-one': [(1.0, 0.0), (math.nextafter(1.0, math.inf), 0.0)],
    'adjacent-at-2^60': [(2.0**60, 0.0), (2.0**60 + 256.0, 0.0)],
    'collinear': [(-3, -3), (0, 0), (4, 4), (1, 1)],
    'nearly-collinear': [(0.0, 0.0), (1.0, 1.0), (2.0, math.nextafter(2.0, math.inf))],
    'obtuse': [(0, 0), (4, 0), (1, 0.5)],
    'acute': [(0, 0), (4, 0), (1, 3)],
    'just-outside-diameter': [(-1.0, 0.0), (1.0, 0.0), (0.0, 1.0 + 2.5e-13)],
    'minsub': [(-math.ulp(0.0), 0.0), (0.0, 0.0), (math.ulp(0.0), 0.0)],
    'minsub-adjacent': [(0.0, 0.0), (math.ulp(0.0), 0.0)],
    'max-diameter': [(-sys_float_max(), 0.0), (sys_float_max(), 0.0)],
    'max-diagonal-overflow': [
        (-sys_float_max(), -sys_float_max()),
        (sys_float_max(), sys_float_max()),
    ],
    'mixed-minsub-max': [
        (-sys_float_max(), 0.0),
        (sys_float_max(), 0.0),
        (0.0, math.ulp(0.0)),
    ],
    'erased-small-coordinate': [(-1.0, 0.0), (1.0, 0.0), (1.0, 2.0**-600)],
    'translated-acute': [
        (2.0**60, 2.0**60),
        (2.0**60 + 1024.0, 2.0**60),
        (2.0**60 + 256.0, 2.0**60 + 768.0),
    ],
    'five-cloud': [(0, 0), (5, 1), (2, 4), (1, 1), (3, 3)],
}

for exponent in (-600, 600):
    CASES[f'acute-scale-{exponent}'] = [
        (0.0, 0.0),
        (4 * 2.0**exponent, 0.0),
        (2.0**exponent, 3 * 2.0**exponent),
    ]


def permutations_for(
    points: list[tuple[float, float] | tuple[int, int]],
) -> list[tuple]:
    unique = list(dict.fromkeys(points))
    if len(unique) <= 4:
        return list(permutations(unique))
    return [tuple(unique), tuple(reversed(unique))]


@pytest.mark.parametrize('name', list(CASES), ids=list(CASES))
def test_planar_radius_matches_independent_exact_support_oracle(name: str) -> None:
    points = CASES[name]
    expected = exhaustive(points)
    assert expected is not None
    expected_radius = round_sqrt(expected.radius_squared)
    for order in permutations_for(points):
        actual = gm.MultiPoint(order).minimum_bounding_radius(unit='planar')
        assert actual.hex() == expected_radius.hex(), (
            name,
            order,
            actual,
            expected_radius,
        )
        assert is_certificate(expected, tuple(q(point) for point in points))


@pytest.mark.parametrize(
    'name',
    [
        'diameter',
        'collinear',
        'obtuse',
        'acute',
        'just-outside-diameter',
        'five-cloud',
    ],
)
def test_planar_circle_is_valid_and_exactly_source_enclosing(name: str) -> None:
    points = CASES[name]
    expected = exhaustive(points)
    assert expected is not None and expected.radius_squared > 0
    circle = gm.MultiPoint(points).minimum_bounding_circle(unit='planar')
    assert circle.geometry_type == 'Polygon'
    assert circle.is_valid
    vertices = [q((x, y)) for x, y in list(circle.coords)[:-1]]
    turns = [
        orientation(
            vertices[index - 1], vertices[index], vertices[(index + 1) % len(vertices)]
        )
        for index in range(len(vertices))
    ]
    direction = next((1 if turn > 0 else -1 for turn in turns if turn), 0)
    assert direction and all(turn * direction > 0 for turn in turns)
    assert all(
        orientation(left, right, point) * direction >= 0
        for left, right in zip(vertices, vertices[1:] + vertices[:1], strict=True)
        for point in map(q, points)
    )


@pytest.mark.parametrize('name', ['one', 'duplicates-signedzero'])
def test_zero_exact_radius_returns_the_source_point(name: str) -> None:
    source = CASES[name][0]
    result = gm.MultiPoint(CASES[name]).minimum_bounding_circle(unit='planar')
    assert result.geometry_type == 'Point'
    assert (result.x, result.y) == (float(source[0]), float(source[1]))


@pytest.mark.parametrize(
    'name', ['adjacent-at-one', 'adjacent-at-2^60', 'translated-acute']
)
def test_planar_circle_reports_the_declared_realization_budget(name: str) -> None:
    with pytest.raises(gm.GeometryError, match='reduce realization') as caught:
        gm.MultiPoint(CASES[name]).minimum_bounding_circle(unit='planar')
    assert caught.value.operation == 'minimum_bounding_circle'
    assert caught.value.parameter == 'realization'
    assert caught.value.produced == 16_000_010
    assert caught.value.limit == 16_000_000


def test_planar_circle_rejects_a_positive_radius_below_the_minimum_subnormal() -> None:
    with pytest.raises(gm.GeometryError, match='contained geometry') as caught:
        gm.MultiPoint(CASES['minsub-adjacent']).minimum_bounding_circle(unit='planar')
    assert caught.value.operation == 'minimum_bounding_circle'


def test_planar_circle_emits_a_minimum_subnormal_diameter() -> None:
    points = CASES['minsub']
    expected = exhaustive(points)
    assert expected is not None
    circle = gm.MultiPoint(points).minimum_bounding_circle(unit='planar')
    assert circle.geometry_type == 'Polygon'
    vertices = [q((x, y)) for x, y in list(circle.coords)[:-1]]
    assert len(vertices) == 64
    assert all(
        distance_squared(expected.center, point) > expected.radius_squared
        for point in vertices
    )
    assert all(
        orientation(left, right, expected.center) >= 0
        and orientation(left, right, expected.center) ** 2
        >= expected.radius_squared * distance_squared(left, right)
        for left, right in zip(vertices, vertices[1:] + vertices[:1], strict=True)
    )


def test_planar_circle_rejects_overflowed_output_without_changing_radius() -> None:
    points = gm.MultiPoint(CASES['max-diagonal-overflow'])
    assert math.isinf(points.minimum_bounding_radius(unit='planar'))
    with pytest.raises(gm.GeometryError, match='contained geometry'):
        points.minimum_bounding_circle(unit='planar')


def test_planar_circle_emits_a_full_range_finite_diameter() -> None:
    points = CASES['max-diameter']
    expected = exhaustive(points)
    assert expected is not None
    circle = gm.MultiPoint(points).minimum_bounding_circle(unit='planar')
    assert circle.geometry_type == 'Polygon'
    vertices = [q((x, y)) for x, y in list(circle.coords)[:-1]]
    assert len(vertices) == 64
    assert all(math.isfinite(float(value)) for point in vertices for value in point)
    assert all(
        distance_squared(expected.center, point) > expected.radius_squared
        for point in vertices
    )
    assert all(
        (orientation(left, right, expected.center) >= 0)
        and orientation(left, right, expected.center) ** 2
        >= expected.radius_squared * distance_squared(left, right)
        for left, right in zip(vertices, vertices[1:] + vertices[:1], strict=True)
    )


def test_planar_radius_array_matches_scalar_and_preserves_missing_rows() -> None:
    points = gm.MultiPoint(CASES['acute'])
    array = gm.GeometryArray([points, None, gm.LineString([])])
    values = array.minimum_bounding_radius(unit='planar')
    np.testing.assert_array_equal(
        values.view(np.uint64),
        np.array(
            [
                np.float64(points.minimum_bounding_radius(unit='planar')).view(
                    np.uint64
                ),
                np.float64(np.nan).view(np.uint64),
                np.float64(np.nan).view(np.uint64),
            ],
            dtype=np.uint64,
        ),
    )


def test_planar_radius_matches_every_permutation_of_small_grid_subsets() -> None:
    grid = [(0, 0), (2, 0), (0, 2), (2, 2), (1, 1), (1, 3)]
    for size in range(1, 6):
        for subset in combinations(grid, size):
            expected = exhaustive(subset)
            assert expected is not None and is_certificate(
                expected, tuple(map(q, subset))
            )
            expected_radius = round_sqrt(expected.radius_squared)
            for order in permutations(subset):
                actual = gm.MultiPoint(order).minimum_bounding_radius(unit='planar')
                assert actual.hex() == expected_radius.hex(), (subset, order)
