"""Print-only O(1) MRR workload with a geometric expected-area oracle."""

import argparse
from fractions import Fraction
from itertools import pairwise
import math
import time

import gometry as gm


parser = argparse.ArgumentParser()
parser.add_argument('--vertices', type=int, choices=(12, 256), required=True)
args = parser.parse_args()

radius = 10.0
points = [
    (
        radius * math.cos(2.0 * math.pi * index / args.vertices),
        radius * math.sin(2.0 * math.pi * index / args.vertices),
    )
    for index in range(args.vertices)
]
source = gm.MultiPoint(points)
iterations = 50_000 if args.vertices == 12 else 10_000


def _stored_fraction(value: float) -> Fraction:
    return Fraction(value)


def _exact_mrr_area(vertices: list[tuple[float, float]]) -> Fraction:
    """Exact support-width oracle over the generated binary64 vertices."""
    stored = [(_stored_fraction(x), _stored_fraction(y)) for x, y in vertices]
    best: Fraction | None = None
    # These are ordered convex-hull vertices, so a minimum-area rectangle
    # has an edge parallel to one of these adjacent hull edges. This exact
    # independent setup oracle is O(n²), not an all-pairs O(n³) benchmark tax.
    for (x1, y1), (x2, y2) in pairwise(stored + stored[:1]):
        dx, dy = x2 - x1, y2 - y1
        norm_squared = dx * dx + dy * dy
        along = [dx * x + dy * y for x, y in stored]
        outward = [-dy * x + dx * y for x, y in stored]
        candidate = (
            (max(along) - min(along)) * (max(outward) - min(outward)) / norm_squared
        )
        if best is None or candidate < best:
            best = candidate
    assert best is not None
    return best


def _assert_exact_enclosure(result: gm.Geometry) -> None:
    corners = list(result.exterior.coords)
    assert len(corners) == 5 and corners[0] == corners[-1]
    ring = [(_stored_fraction(x), _stored_fraction(y)) for x, y in corners]
    for x, y in points:
        point = (_stored_fraction(x), _stored_fraction(y))
        for left, right in pairwise(ring):
            assert (right[0] - left[0]) * (point[1] - left[1]) - (
                right[1] - left[1]
            ) * (point[0] - left[0]) >= 0


expected_area = _exact_mrr_area(points)


def run() -> gm.Geometry:
    return source.minimum_rotated_rectangle()


# Validate once outside the timed loop: a full-hull predicate is an oracle for
# the benchmark, never part of the O(1) construction being measured.
expected_result = run()
assert math.isfinite(expected_result.area)
assert math.isclose(
    expected_result.area, float(expected_area), rel_tol=2e-14, abs_tol=0.0
)
_assert_exact_enclosure(expected_result)
t0 = time.perf_counter()
for _ in range(iterations):
    run()
print(time.perf_counter() - t0)
