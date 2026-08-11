"""bench_ab case: duplicate-rich multipolygon union scaling shape.

Prints the 320-dup / 80-dup median wall-clock ratio. A linear/n-log-n path
is ~3-5x; a quadratic path is tens-to-hundredsx. Timing claims belong here,
not in pytest. Output is validated outside the timed region.
"""

from __future__ import annotations

import statistics
import time

import gometry as gm
import numpy as np


def _ring32(ox: float = 0.0, oy: float = 0.0) -> gm.Polygon:
    n = 32
    coords = [
        (ox + np.cos(2 * np.pi * i / n), oy + np.sin(2 * np.pi * i / n))
        for i in range(n)
    ]
    coords.append(coords[0])
    return gm.Polygon(coords)


def _median_union(n: int, *, warm: int = 3, reps: int = 7) -> float:
    poly = _ring32()
    multi = gm.MultiPolygon([poly] * n)
    other = gm.MultiPolygon([_ring32(0.5, 0.5)] * max(1, n // 4))
    last = None
    for _ in range(warm):
        last = gm.union(multi, other)
    samples: list[float] = []
    for _ in range(reps):
        t0 = time.perf_counter()
        last = gm.union(multi, other)
        samples.append(time.perf_counter() - t0)
    # Untimed semantic postconditions: non-empty polygonal union covering inputs.
    assert last is not None
    assert last.geometry_type in {'Polygon', 'MultiPolygon'}
    assert not last.is_empty
    assert last.area > 0.0
    assert gm.intersects(last, poly)
    return statistics.median(samples)


def main() -> None:
    t80 = _median_union(80)
    t320 = _median_union(320)
    ratio = t320 / max(t80, 1e-12)
    # Single float for bench_ab consumers; ratio is the scaling property.
    print(ratio)


if __name__ == '__main__':
    main()
