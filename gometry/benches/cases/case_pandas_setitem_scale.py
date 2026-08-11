"""bench_ab case: pandas GeometryExtensionArray setitem selection scaling.

Prints the 10k-selection / 1-selection median wall-clock ratio on a 100k-row
column. Native batch scatter scales with selection size; a full-column rebuild
is flat ~30 ms. Timing claims belong here, not in pytest.
"""

from __future__ import annotations

import statistics
import time

import gometry as gm
import numpy as np


def _median_setitem(ext: object, n: int, *, warm: int = 2, reps: int = 5) -> float:
    positions = np.arange(n, dtype=np.intp)
    values = [gm.Point(float(i), float(i)) for i in range(n)]
    for _ in range(warm):
        ext[positions] = values  # type: ignore[index]
    samples: list[float] = []
    for _ in range(reps):
        t0 = time.perf_counter()
        ext[positions] = values  # type: ignore[index]
        samples.append(time.perf_counter() - t0)
    # Untimed postcondition: last write landed at the expected coordinate.
    got = ext[n - 1]  # type: ignore[index]
    assert float(got.x) == float(n - 1) and float(got.y) == float(n - 1)
    return statistics.median(samples)


def main() -> None:
    from gometry._pandas import GeometryExtensionArray

    rng = np.random.default_rng(0)
    xy = rng.random((100_000, 2))
    arr = gm.points(xy[:, 0], xy[:, 1])
    ext = GeometryExtensionArray(arr)
    ext[0] = gm.Point(0.0, 0.0)
    t1 = _median_setitem(ext, 1)
    t10k = _median_setitem(ext, 10_000)
    ratio = t10k / max(t1, 1e-12)
    print(ratio)


if __name__ == '__main__':
    main()
