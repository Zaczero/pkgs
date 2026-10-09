"""Print-only bounded Hausdorff point-target scaling workload.

This case keeps the real Python entry point measurable without making machine
speed a test assertion: it prints the warm median 800-point / 100-point ratio.
"""

from __future__ import annotations

import math
import statistics
import sys
import time

from _observation import bootstrap, emit, prepare_measurement

if __name__ == '__main__':
    bootstrap()

import gometry as gm
from _validation import emit_validation


def _case(count: int) -> tuple[gm.LineString, gm.MultiPoint]:
    source = gm.LineString([(0.0, 0.0), (float(count - 1), 0.0)])
    target = gm.MultiPoint([(float(index), 1.0) for index in range(count)])
    return source, target


def _median_hausdorff(count: int, *, prepare: bool = True) -> float:
    source, target = _case(count)
    expected = math.sqrt(1.25)
    max_error = 1e-12
    for _ in range(3):
        actual = gm.hausdorff_distance(source, target, max_error=max_error)
        assert abs(actual - expected) <= max_error
    if prepare:
        prepare_measurement()
    samples = []
    for _ in range(9):
        start = time.perf_counter()
        result = gm.hausdorff_distance(source, target, max_error=max_error)
        samples.append(time.perf_counter() - start)
        assert abs(result - expected) <= max_error
    return statistics.median(samples)


def main() -> None:
    emit(
        metric='scaling_ratio',
        raw_values=[_median_hausdorff(800) / max(_median_hausdorff(100), 1e-12)],
        useful_work={
            'numerator': {'count': 800, 'unit': 'target-points'},
            'denominator': {'count': 100, 'unit': 'target-points'},
        },
        oracle_identity='case_hausdorff_bounded_point_target_scale:analytic-bound',
        warmup_iterations=3,
        calibration_iterations=9,
        calibration_strategy='median-of-repetitions-per-scaling-point',
    )


def validate_case() -> None:
    assert _median_hausdorff(2, prepare=False) > 0.0
    emit_validation(
        'case_hausdorff_bounded_point_target_scale',
        useful_output={'count': 2, 'unit': 'target-points'},
    )


if __name__ == '__main__':
    validate_case() if '--validate' in sys.argv else main()
