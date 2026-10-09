"""bench_ab case: validity on many-disjoint holes / multipolygon parts.

Prints the median wall-clock of ``is_valid`` on a 2000-hole polygon. Pre-fix
was O(N²) (~100+ ms); post-fix reuses the sweep candidate visitor (few ms).
Timing claims belong here, not in pytest.
"""

from __future__ import annotations

import statistics
import sys
import time

from _observation import bootstrap, emit, prepare_measurement

if __name__ == '__main__':
    bootstrap()

import gometry as gm
from _validation import emit_validation


def _poly_with_n_holes(n: int, spacing: float = 3.0) -> gm.Polygon:
    shell = [
        (-1.0, -1.0),
        (n * spacing + 2.0, -1.0),
        (n * spacing + 2.0, spacing + 1.0),
        (-1.0, spacing + 1.0),
        (-1.0, -1.0),
    ]
    holes = [
        [
            (i * spacing, 0.0),
            (i * spacing + 1.0, 0.0),
            (i * spacing + 1.0, 1.0),
            (i * spacing, 1.0),
            (i * spacing, 0.0),
        ]
        for i in range(n)
    ]
    return gm.Polygon(shell, holes)


def _median_valid(n: int = 2000, *, warm: int = 2, reps: int = 7) -> float:
    p = _poly_with_n_holes(n)
    for _ in range(warm):
        assert p.is_valid is True
    prepare_measurement()
    samples: list[float] = []
    for _ in range(reps):
        t0 = time.perf_counter()
        assert p.is_valid is True
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples)


def main() -> None:
    emit(
        metric='duration',
        raw_values=[_median_valid()],
        useful_work={'count': 2_000, 'unit': 'polygon-holes'},
        oracle_identity='case_validity_disjoint_scale:validity',
        warmup_iterations=2,
        calibration_iterations=7,
    )


def validate_case() -> None:
    polygon = _poly_with_n_holes(3)
    assert polygon.is_valid is True
    emit_validation(
        'case_validity_disjoint_scale',
        useful_output={'count': 3, 'unit': 'polygon-holes'},
    )


if __name__ == '__main__':
    validate_case() if '--validate' in sys.argv else main()
