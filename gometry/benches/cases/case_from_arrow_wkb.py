"""bench_ab case: from_arrow over a Binary WKB column (50k points).

Validates type/length/identity outside the timed region (bench_ab contract).
"""

from __future__ import annotations

import time

import gometry as gm
import pyarrow as pa


def main() -> None:
    n = 50_000
    points = gm.points(
        [float(i % 360 - 180) for i in range(n)],
        [float(i % 170 - 85) for i in range(n)],
    )
    wkbs = points.to_wkb()
    arrow = pa.array(wkbs, type=pa.binary())
    # warm + untimed semantic postcondition
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
    print(min(samples))


if __name__ == '__main__':
    main()
