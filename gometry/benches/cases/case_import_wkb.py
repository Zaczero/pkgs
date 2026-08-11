"""bench_ab case: from_wkb over 200k point + 20k linestring WKBs.

Validates type/length/identity outside the timed region so a "win" cannot come
from doing less work (bench_ab contract).
"""

import sys
import statistics
import time

import gometry as gm


def main() -> None:
    points = gm.points(
        [float(i % 360 - 180) for i in range(200_000)],
        [float(i % 170 - 85) for i in range(200_000)],
    )
    lines = gm.GeometryArray([
        gm.LineString([(float(i), 0.0), (float(i) + 1.0, 1.0), (float(i) + 2.0, 0.5)])
        for i in range(20_000)
    ])
    point_wkb = points.to_wkb()
    line_wkb = lines.to_wkb()
    # warm parse + untimed semantic postcondition
    warm_pts = gm.from_wkb(point_wkb)
    warm_lines = gm.from_wkb(line_wkb)
    assert len(warm_pts) == 200_000
    assert len(warm_lines) == 20_000
    assert warm_pts.geometry_type == 'Point' or all(
        g.geometry_type == 'Point' for g in warm_pts[:3]
    )
    assert warm_lines[0].geometry_type == 'LineString'
    assert gm.equals_identical(warm_pts[0], points[0])
    assert gm.equals_identical(warm_lines[0], lines[0])
    samples: list[float] = []
    last_pts = None
    last_lines = None
    for _ in range(3):
        start = time.perf_counter()
        last_pts = gm.from_wkb(point_wkb)
        last_lines = gm.from_wkb(line_wkb)
        samples.append(time.perf_counter() - start)
    # Untimed postconditions after the measured loop.
    assert last_pts is not None and last_lines is not None
    assert len(last_pts) == 200_000 and len(last_lines) == 20_000
    assert gm.equals_identical(last_pts[123], points[123])
    assert gm.equals_identical(last_lines[7], lines[7])
    print(statistics.median(samples))


if __name__ == '__main__':
    sys.exit(main())
