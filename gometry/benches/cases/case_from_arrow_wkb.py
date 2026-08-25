"""bench_ab case: from_arrow over a Binary WKB column (50k points)."""

from __future__ import annotations

import sys
import time

import gometry as gm
import pyarrow as pa
from _observation import emit
from _validation import emit_validation


def main() -> None:
    n = 50_000
    points = gm.points(
        [float(i % 360 - 180) for i in range(n)],
        [float(i % 170 - 85) for i in range(n)],
    )
    wkbs = points.to_wkb()
    arrow = pa.array(wkbs, type=pa.binary())
    warm = gm.from_arrow(arrow)
    assert len(warm) == n
    assert gm.equals_identical(warm[0], points[0])
    assert warm[0].geometry_type == 'Point'
    samples: list[float] = []
    last = None
    for _ in range(3):
        start = time.perf_counter()
        last = gm.from_arrow(arrow)
        samples.append(time.perf_counter() - start)
    assert last is not None and len(last) == n
    assert gm.equals_identical(last[0], points[0])
    emit(
        metric='duration',
        raw_values=[min(samples)],
        useful_work={'count': n, 'unit': 'WKB-rows'},
        oracle_identity='case_from_arrow_wkb:roundtrip-first-row',
        warmup_iterations=1,
        calibration_iterations=3,
        calibration_strategy='minimum-of-repetitions',
    )


def validate_case() -> None:
    points = gm.points([0.0, 1.0], [0.0, 1.0])
    restored = gm.from_arrow(pa.array(points.to_wkb(), type=pa.binary()))
    assert len(restored) == 2 and gm.equals_identical(restored[0], points[0])
    emit_validation(
        'case_from_arrow_wkb',
        useful_output={'count': 2, 'unit': 'WKB-rows'},
    )


if __name__ == '__main__':
    validate_case() if '--validate' in sys.argv else main()
