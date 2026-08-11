"""bench_ab case: Coordinates random-access flat vs run count.

Prints the 100k-run / 1k-run median wall-clock ratio for ``coords[0]`` over
1,000 repeats. O(1) storage-shaped indexing is ~1x; a linear CSR walk grows
with run count. Timing claims belong here, not in pytest.
"""

from __future__ import annotations

import statistics
import time

import gometry as gm


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
    print(ratio)


if __name__ == '__main__':
    main()
