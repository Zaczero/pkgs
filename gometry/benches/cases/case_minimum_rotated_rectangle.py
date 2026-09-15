"""Print-only O(1) MRR workload with a geometric expected-area oracle."""

from __future__ import annotations

import argparse
from fractions import Fraction
from itertools import pairwise
import math
import sys
import time

import gometry as gm
from _observation import emit
from _validation import emit_validation


def _stored_fraction(value: float) -> Fraction:
    return Fraction(value)


def _exact_mrr_area(vertices: list[tuple[float, float]]) -> Fraction:
    stored = [(_stored_fraction(x), _stored_fraction(y)) for x, y in vertices]
    best: Fraction | None = None
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


def _assert_exact_enclosure(
    result: gm.Geometry, points: list[tuple[float, float]]
) -> None:
    corners = list(result.exterior.coords)
    assert len(corners) == 5 and corners[0] == corners[-1]
    ring = [(_stored_fraction(x), _stored_fraction(y)) for x, y in corners]
    for x, y in points:
        point = (_stored_fraction(x), _stored_fraction(y))
        for left, right in pairwise(ring):
            assert (right[0] - left[0]) * (point[1] - left[1]) - (
                right[1] - left[1]
            ) * (point[0] - left[0]) >= 0


def _run(vertices: int) -> tuple[gm.Geometry, list[tuple[float, float]], int]:
    radius = 10.0
    points = [
        (
            radius * math.cos(2.0 * math.pi * index / vertices),
            radius * math.sin(2.0 * math.pi * index / vertices),
        )
        for index in range(vertices)
    ]
    source = gm.MultiPoint(points)
    result = source.minimum_rotated_rectangle()
    expected_area = _exact_mrr_area(points)
    assert math.isclose(result.area, float(expected_area), rel_tol=2e-14, abs_tol=0.0)
    _assert_exact_enclosure(result, points)
    return result, points, 50_000 if vertices == 12 else 10_000


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--vertices', type=int, choices=(12, 256), required=True)
    args = parser.parse_args()
    _result, points, iterations = _run(args.vertices)
    source = gm.MultiPoint(points)
    started = time.perf_counter()
    for _ in range(iterations):
        source.minimum_rotated_rectangle()
    emit(
        metric='duration',
        raw_values=[time.perf_counter() - started],
        useful_work={'count': iterations, 'unit': 'rectangles'},
        oracle_identity='case_minimum_rotated_rectangle:exact-enclosure',
        phase='steady',
        calibration_iterations=iterations,
    )


def validate_case() -> None:
    result = gm.MultiPoint([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)])
    rectangle = result.minimum_rotated_rectangle()
    assert math.isclose(rectangle.area, 1.0)
    emit_validation(
        'case_minimum_rotated_rectangle',
        useful_output={'count': 1, 'unit': 'rectangles'},
    )


if __name__ == '__main__':
    validate_case() if '--validate' in sys.argv else main()
