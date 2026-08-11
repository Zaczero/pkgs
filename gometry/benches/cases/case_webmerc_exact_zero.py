"""Print one timed Web Mercator batch for the exact-zero fast-path A/B run."""

from __future__ import annotations

import sys
import time

import gometry as gm
import numpy as np

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
    start = time.perf_counter()
    for _ in range(REPEATS):
        result = gm.crs_transform(4326, 3857, longitudes, latitudes)
    elapsed = time.perf_counter() - start
    if not np.isfinite(result).all():
        raise RuntimeError('Web Mercator result must remain finite')
    print(elapsed)


if __name__ == '__main__':
    main()
