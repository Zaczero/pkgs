"""Train shortlink creation and resolution through the Python API."""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pgo_process

GENERATE_FLAGS = ()
PACKAGE = Path(__file__).resolve().parents[1]
OPERATIONS = 89_520
stage_generator = pgo_process.stage_generator
verify_runtime = pgo_process.verify_runtime
known_profile_diagnostic = pgo_process.known_profile_diagnostic


def collect(output: Path) -> Path:
    from osm_shortlink import _lib

    return pgo_process.collect(
        output,
        Path(__file__),
        Path(_lib.__file__),
        units=OPERATIONS,
        functions=['___pyfunction_shortlink_encode', '___pyfunction_shortlink_decode'],
    )


def test(output: Path) -> None:
    pgo_process.run_tests(output, PACKAGE)


def train() -> None:
    from osm_shortlink import _lib, shortlink_decode, shortlink_encode

    rng = random.Random(715893)
    points = [
        (rng.uniform(-180, 180), rng.uniform(-90, 90), zoom)
        for zoom in range(23)
        for _ in range(8)
    ]
    # Wrapped coordinates and poles share the normalization path, but are a
    # minority of calls compared with map positions already in range.
    points.extend(
        (lon, lat, zoom)
        for lon, lat in ((180, 90), (-180, -90), (540, 100), (-720, -280))
        for zoom in (0, 5, 12, 22)
    )
    codes = [shortlink_encode(*point) for point in points]
    units = len(points)
    for _ in range(220):
        for point, code in zip(points, codes, strict=True):
            if shortlink_encode(*point) != code:
                raise RuntimeError('shortlink training encoding changed')
            if shortlink_decode(code)[2] != point[2]:
                raise RuntimeError('shortlink training zoom changed')
            units += 2
        for code in ('0EEQjE--', '0OP4tXGMB', 'ecetE--', '0EEQjE..'):
            if len(shortlink_decode(code)) != 3:
                raise RuntimeError('shortlink training resolution failed')
            units += 1
        for code in ('X', 'A' * 11):
            try:
                shortlink_decode(code)
            except ValueError:
                units += 1
            else:
                raise RuntimeError('shortlink training accepted an invalid code')
    pgo_process.completed(units, Path(_lib.__file__))


if __name__ == '__main__':
    if sys.argv[1:] != ['train']:
        raise SystemExit('expected train')
    train()
