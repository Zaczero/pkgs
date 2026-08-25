"""Print-only C7 point-target lower-envelope scaling workload.

The paired Rust operation-count test is the correctness gate. This case keeps
the real Python entry point measurable without making machine speed a test
assertion: it prints the warm median 800-point / 100-point ratio.
"""

from __future__ import annotations

import math
import statistics
import time

import gometry as gm
from _observation import emit


def _case(count: int) -> tuple[gm.LineString, gm.MultiPoint]:
    source = gm.LineString([(0.0, 0.0), (float(count - 1), 0.0)])
    target = gm.MultiPoint([(float(index), 1.0) for index in range(count)])
    return source, target


def _median_hausdorff(count: int) -> float:
    source, target = _case(count)
    expected = math.sqrt(1.25)
    for _ in range(3):
        assert gm.hausdorff_distance(source, target) == expected
    samples = []
    for _ in range(9):
        start = time.perf_counter()
        result = gm.hausdorff_distance(source, target)
        samples.append(time.perf_counter() - start)
        assert result == expected
    return statistics.median(samples)


def main() -> None:
    emit(
        metric='scaling_ratio',
        raw_values=[_median_hausdorff(800) / max(_median_hausdorff(100), 1e-12)],
        useful_work={
            'numerator': {'count': 800, 'unit': 'target-points'},
            'denominator': {'count': 100, 'unit': 'target-points'},
        },
        oracle_identity='case_hausdorff_point_envelope_scale:exact-distance-v1',
        warmup_iterations=3,
        calibration_iterations=9,
        calibration_strategy='median-of-repetitions-per-scaling-point',
    )


if __name__ == '__main__':
    main()
