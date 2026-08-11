"""bench_ab case: validity on many-disjoint holes / multipolygon parts.

Prints the median wall-clock of ``is_valid`` on a 2000-hole polygon. Pre-fix
was O(N²) (~100+ ms); post-fix reuses the sweep candidate visitor (few ms).
Timing claims belong here, not in pytest.
"""

from __future__ import annotations

import statistics
import time

import gometry as gm


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
    samples: list[float] = []
    for _ in range(reps):
        t0 = time.perf_counter()
        assert p.is_valid is True
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples)


def main() -> None:
    print(_median_valid())


if __name__ == '__main__':
    main()
