"""bench_ab case: gathered Coordinates equality scaling shape.

Prints the 1600-row / 800-row median wall-clock ratio for gather-vs-rebuilt
identity equality. Linear is ~2x; a quadratic CSR re-walk is far higher.
Timing claims belong here, not in pytest.
"""

from __future__ import annotations

import statistics
import time

import gometry as gm


def _line_array(n: int) -> gm.GeometryArray:
    line = gm.LineString([(float(i), float(i % 5)) for i in range(16)])
    return gm.GeometryArray([line] * n)


def _median_eq(n: int, *, warm: int = 3, reps: int = 11) -> float:
    arr = _line_array(n)
    left = arr[::2].coords
    right = gm.GeometryArray(list(arr[::2])).coords
    assert left == right
    for _ in range(warm):
        assert left == right
    samples: list[float] = []
    for _ in range(reps):
        t0 = time.perf_counter()
        assert left == right
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples)


def main() -> None:
    t_small = _median_eq(800)
    t_large = _median_eq(1600)
    ratio = t_large / max(t_small, 1e-12)
    print(ratio)


if __name__ == '__main__':
    main()
