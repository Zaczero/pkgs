"""bench_ab case: prepared coordinate-predicate probe-count scaling shape.

Prints the median wall-clock of contains(x=..., y=...) at the measured plan-aware
crossover on a large polygon, after warm-up calls have left the polygon's
hierarchical tester built; timing claims belong here, not in pytest.
"""

from __future__ import annotations

import statistics
import sys
import time

from _observation import bootstrap, emit, prepare_measurement

if __name__ == '__main__':
    bootstrap()

import gometry as gm
import numpy as np
from _validation import emit_validation


def _big_polygon(n_edge: int = 10_000) -> gm.Polygon:
    theta = np.linspace(0, 2 * np.pi, n_edge, endpoint=False)
    coords = list(zip(np.cos(theta), np.sin(theta), strict=True))
    coords.append(coords[0])
    return gm.Polygon(coords)


def _median_prepared(n: int, *, warm: int = 3, reps: int = 11) -> float:
    big = _big_polygon()
    prep = big
    rng = np.random.default_rng(0)
    xs = rng.uniform(-1.5, 1.5, n)
    ys = rng.uniform(-1.5, 1.5, n)
    assert gm.contains(prep, x=0.0, y=0.0) is True
    for _ in range(warm):
        gm.contains(prep, x=xs, y=ys)
    prepare_measurement()
    samples: list[float] = []
    last = None
    for _ in range(reps):
        t0 = time.perf_counter()
        last = gm.contains(prep, x=xs, y=ys)
        samples.append(time.perf_counter() - t0)
    # Untimed postconditions: vector length + origin containment.
    assert last is not None
    arr = np.asarray(last)
    assert arr.shape == (n,) and arr.dtype == np.bool_
    assert bool(gm.contains(prep, x=0.0, y=0.0)) is True
    assert bool(gm.contains(prep, x=10.0, y=10.0)) is False
    return statistics.median(samples)


def main() -> None:
    emit(
        metric='duration',
        raw_values=[_median_prepared(63)],
        useful_work={'count': 63, 'unit': 'prepared-probes'},
        oracle_identity='case_prepared_coordinate_predicate_scale:probe-mask',
        warmup_iterations=3,
        calibration_iterations=11,
    )


def validate_case() -> None:
    polygon = gm.box(-1.0, -1.0, 1.0, 1.0)
    result = gm.contains(polygon, x=np.asarray([0.0]), y=np.asarray([0.0]))
    assert result.shape == (1,) and bool(result[0])
    emit_validation(
        'case_prepared_coordinate_predicate_scale',
        useful_output={'count': 1, 'unit': 'prepared-probes'},
    )


if __name__ == '__main__':
    validate_case() if '--validate' in sys.argv else main()
