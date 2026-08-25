"""bench_ab case: Coordinates random-access flat vs run count.

Prints the 100k-run / 1k-run median wall-clock ratio for ``coords[0]`` over
1,000 repeats. O(1) storage-shaped indexing is ~1x; a linear CSR walk grows
with run count. Timing claims belong here, not in pytest.
"""

from __future__ import annotations

import statistics
import sys
import time

import gometry as gm
from _observation import emit
from _validation import emit_validation


def _median_index0(n_runs: int, *, warm: int = 2, reps: int = 7) -> float:
    arr = gm.line_strings([[(float(i), 0.0), (float(i), 1.0)] for i in range(n_runs)])
    coords = arr.coords
    assert len(coords) == n_runs * 2
    first = coords[0]
    assert float(first[0]) == 0.0 and float(first[1]) == 0.0
    for _ in range(warm):
        for _ in range(1_000):
            _ = coords[0]
    samples: list[float] = []
    last = None
    for _ in range(reps):
        t0 = time.perf_counter()
        for _ in range(1_000):
            last = coords[0]
        samples.append(time.perf_counter() - t0)
    # Untimed postcondition: last read matches the first vertex.
    assert last is not None
    assert float(last[0]) == 0.0 and float(last[1]) == 0.0
    return statistics.median(samples)


def main() -> None:
    t_small = _median_index0(1_000)
    t_large = _median_index0(100_000)
    ratio = t_large / max(t_small, 1e-12)
    emit(
        metric='scaling_ratio',
        raw_values=[ratio],
        useful_work={
            'numerator': {'count': 100_000, 'unit': 'geometry-runs'},
            'denominator': {'count': 1_000, 'unit': 'geometry-runs'},
        },
        oracle_identity='case_coordinates_access_scale:first-coordinate',
        warmup_iterations=2,
        calibration_iterations=7,
        calibration_strategy='median-of-repetitions-per-scaling-point',
    )


def validate_case() -> None:
    arr = gm.line_strings([[(0.0, 0.0), (1.0, 1.0)]])
    assert arr.coords[0] == (0.0, 0.0)
    emit_validation(
        'case_coordinates_access_scale',
        useful_output={'count': 1, 'unit': 'geometry-runs'},
    )


if __name__ == '__main__':
    validate_case() if '--validate' in sys.argv else main()
