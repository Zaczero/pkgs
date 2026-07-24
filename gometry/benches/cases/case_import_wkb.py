"""bench_ab case: from_wkb over 200k point + 20k linestring WKBs."""

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
    # warm parse
    gm.from_wkb(point_wkb)
    samples: list[float] = []
    for _ in range(3):
        start = time.perf_counter()
        gm.from_wkb(point_wkb)
        gm.from_wkb(line_wkb)
        samples.append(time.perf_counter() - start)
    print(statistics.median(samples))


if __name__ == '__main__':
    sys.exit(main())
