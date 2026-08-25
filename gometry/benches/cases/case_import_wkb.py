"""bench_ab case: from_wkb over 200k points and 20k linestrings."""

from __future__ import annotations

import statistics
import sys
import time

import gometry as gm
from _observation import emit
from _validation import emit_validation


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
    assert last_pts is not None and last_lines is not None
    assert len(last_pts) == 200_000 and len(last_lines) == 20_000
    assert gm.equals_identical(last_pts[123], points[123])
    assert gm.equals_identical(last_lines[7], lines[7])
    emit(
        metric='duration',
        raw_values=[statistics.median(samples)],
        useful_work={'count': 220_000, 'unit': 'WKB-rows'},
        oracle_identity='case_import_wkb:roundtrip-sample-rows',
        warmup_iterations=1,
        calibration_iterations=3,
    )


def validate_case() -> None:
    points = gm.points([0.0, 1.0], [0.0, 1.0])
    restored = gm.from_wkb(points.to_wkb())
    assert len(restored) == 2 and gm.equals_identical(restored[1], points[1])
    emit_validation(
        'case_import_wkb',
        useful_output={'count': 2, 'unit': 'WKB-rows'},
    )


if __name__ == '__main__':
    validate_case() if '--validate' in sys.argv else main()
