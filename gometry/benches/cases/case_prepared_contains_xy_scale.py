"""bench_ab case: prepared contains_xy probe-count scaling shape.

Prints the median wall-clock of prepared 63-probe contains_xy on a large
polygon (the pre-fix cliff site). A prepared hierarchical tester is µs-class
at every count; the free-function MIN_PROBES=64 edge-walk at 63 is ms-class.
Timing claims belong here, not in pytest.
"""

from __future__ import annotations

import statistics
import time

import gometry as gm
import numpy as np


def _big_polygon(n_edge: int = 10_000) -> gm.Polygon:
    theta = np.linspace(0, 2 * np.pi, n_edge, endpoint=False)
    coords = list(zip(np.cos(theta), np.sin(theta), strict=True))
    coords.append(coords[0])
    return gm.Polygon(coords)


def _median_prepared(n: int, *, warm: int = 3, reps: int = 11) -> float:
    big = _big_polygon()
    prep = big.prepare()
    rng = np.random.default_rng(0)
    xs = rng.uniform(-1.5, 1.5, n)
    ys = rng.uniform(-1.5, 1.5, n)
    assert prep.contains_xy(0.0, 0.0) is True
    for _ in range(warm):
        prep.contains_xy(xs, ys)
    samples: list[float] = []
    last = None
    for _ in range(reps):
        t0 = time.perf_counter()
        last = prep.contains_xy(xs, ys)
        samples.append(time.perf_counter() - t0)
    # Untimed postconditions: vector length + origin containment.
    assert last is not None
    arr = np.asarray(last)
    assert arr.shape == (n,) and arr.dtype == np.bool_
    assert bool(prep.contains_xy(0.0, 0.0)) is True
    assert bool(prep.contains_xy(10.0, 10.0)) is False
    return statistics.median(samples)


def main() -> None:
    # Single float: prepared 63-probe seconds (cliff was ~0.4-1 ms; post-fix us).
    print(_median_prepared(63))


if __name__ == '__main__':
    main()
