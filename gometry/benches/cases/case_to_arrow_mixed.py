"""Print-only A/B case: mixed GeoArrow exports over a large row batch."""

from __future__ import annotations

import sys
import time

from _observation import bootstrap, emit, prepare_measurement

if __name__ == '__main__':
    bootstrap()

import gometry as gm
from _validation import emit_validation

ROWS = 30_000
EXPORTS = 20


def mixed_rows(rows: int = ROWS) -> gm.GeometryArray:
    return gm.GeometryArray([
        gm.Point(float(index), float(index % 97), crs=4326)
        if index % 3 == 0
        else gm.LineString([(float(index), 0.0), (float(index) + 0.5, 1.0)], crs=4326)
        if index % 3 == 1
        else gm.box(float(index), 0.0, float(index) + 0.25, 0.25, crs=4326)
        for index in range(rows)
    ])


def main() -> None:
    geometries = mixed_rows()
    warm = geometries.to_arrow()
    assert len(warm) == ROWS
    prepare_measurement()
    started = time.perf_counter()
    for _ in range(EXPORTS):
        exported = geometries.to_arrow()
    elapsed = time.perf_counter() - started
    assert len(exported) == ROWS
    restored = gm.from_arrow(exported)
    assert restored.to_wkt()[0] == 'POINT (0 0)'
    emit(
        metric='duration',
        raw_values=[elapsed],
        useful_work={'count': EXPORTS * ROWS, 'unit': 'rows-exported'},
        oracle_identity='case_to_arrow_mixed:roundtrip-shape',
        phase='steady',
        calibration_iterations=EXPORTS,
    )


def validate_case() -> None:
    geometries = mixed_rows(3)
    exported = geometries.to_arrow()
    restored = gm.from_arrow(exported)
    assert len(restored) == 3 and restored.to_wkt()[0] == 'POINT (0 0)'
    emit_validation(
        'case_to_arrow_mixed',
        useful_output={'count': 3, 'unit': 'rows-exported'},
    )


if __name__ == '__main__':
    validate_case() if '--validate' in sys.argv else main()
