"""bench_ab case for the public linework-repair fixture.

The returned value is one timed array repair.  Fixture construction and the
semantic checks stay outside that timed call so both sides measure the same
native boundary.
"""

from __future__ import annotations

import sys
import time
import argparse
from pathlib import Path

import gometry as gm

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'python'))
from _bench_public_fixtures import repair_1k_gometry
from _observation import emit
from _validation import emit_validation


def build_input(mix: str) -> gm.GeometryArray:
    invalid = list(repair_1k_gometry())
    if mix == 'all_invalid':
        return repair_1k_gometry()
    invalid_count = {'mixed_20': 200, 'mixed_50': 500, 'all_valid': 0}[mix]
    rows = invalid[:invalid_count]
    rows.extend(
        gm.box(20_000.0 + index * 2.0, 0.0, 20_001.0 + index * 2.0, 1.0)
        for index in range(1000 - invalid_count)
    )
    return gm.GeometryArray(rows, crs='EPSG:32634')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--mix',
        choices=('all_invalid', 'mixed_20', 'mixed_50', 'all_valid'),
        default='all_invalid',
    )
    args = parser.parse_args()
    input_array = build_input(args.mix)
    for _ in range(2):
        input_array.repair(method='linework')
    started = time.perf_counter()
    result = input_array.repair(method='linework')
    elapsed = time.perf_counter() - started
    assert len(result) == 1000
    assert all(result.is_valid)
    assert str(result.crs) == 'EPSG:32634'
    emit(
        metric='duration',
        raw_values=[elapsed],
        useful_work={'count': len(input_array), 'unit': 'geometry-rows'},
        oracle_identity='case_repair_linework:valid-repair-crs',
        warmup_iterations=2,
        calibration_iterations=1,
    )


def validate_case() -> None:
    input_array = gm.GeometryArray([gm.box(0.0, 0.0, 1.0, 1.0)])
    result = input_array.repair(method='linework')
    assert len(result) == 1 and all(result.is_valid)
    emit_validation(
        'case_repair_linework',
        useful_output={'count': 1, 'unit': 'geometry-rows'},
    )


if __name__ == '__main__':
    validate_case() if '--validate' in sys.argv else main()
