"""Print one timed Web Mercator batch for the exact-zero fast-path A/B run."""

from __future__ import annotations

import sys
import time

from _observation import bootstrap, emit, prepare_measurement

if __name__ == '__main__':
    bootstrap()

import gometry as gm
import numpy as np
from _validation import emit_validation

ROWS = 1_000_000
REPEATS = 12


def main() -> None:
    mode = sys.argv[1]
    if mode not in {'ordinary', 'equator'}:
        raise SystemExit('mode must be ordinary or equator')
    latitudes = np.full(ROWS, 51.5 if mode == 'ordinary' else 0.0)
    longitudes = np.linspace(-170.0, 170.0, ROWS)
    for _ in range(2):
        gm.crs_transform(4326, 3857, longitudes, latitudes)
    prepare_measurement()
    start = time.perf_counter()
    for _ in range(REPEATS):
        result = gm.crs_transform(4326, 3857, longitudes, latitudes)
    elapsed = time.perf_counter() - start
    if not np.isfinite(result).all():
        raise RuntimeError('Web Mercator result must remain finite')
    emit(
        metric='duration',
        raw_values=[elapsed],
        useful_work={'count': ROWS * REPEATS, 'unit': 'coordinates-transformed'},
        oracle_identity='case_webmerc_exact_zero:finite-output',
        warmup_iterations=2,
        calibration_iterations=REPEATS,
    )


def validate_case() -> None:
    longitudes = np.asarray([-1.0, 1.0])
    latitudes = np.asarray([0.0, 0.0])
    result = gm.crs_transform(4326, 3857, longitudes, latitudes)
    assert np.isfinite(result).all() and len(result) == 2
    emit_validation(
        'case_webmerc_exact_zero',
        useful_output={'count': 2, 'unit': 'coordinates-transformed'},
    )


if __name__ == '__main__':
    validate_case() if '--validate' in sys.argv else main()
