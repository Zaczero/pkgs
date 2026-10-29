"""Train polyline encoding and decoding through the Python API."""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pgo_process

GENERATE_FLAGS = ()
PACKAGE = Path(__file__).resolve().parents[1]
OPERATIONS = 24_000
stage_generator = pgo_process.stage_generator
verify_runtime = pgo_process.verify_runtime
known_profile_diagnostic = pgo_process.known_profile_diagnostic


def collect(output: Path) -> Path:
    from polyline_rs import _lib

    return pgo_process.collect(
        output,
        Path(__file__),
        Path(_lib.__file__),
        units=OPERATIONS,
        functions=['encode17encode_coordinate', 'decode17decode_next_value'],
    )


def test(output: Path) -> None:
    pgo_process.run_tests(output, PACKAGE)


def train() -> None:
    from polyline_rs import (
        _lib,
        decode_latlon,
        decode_lonlat,
        encode_latlon,
        encode_lonlat,
    )

    rng = random.Random(5482)
    units = 0
    for size in (3, 24, 180):
        points = []
        latitude, longitude = 43.0, -72.0
        for _ in range(size):
            latitude += rng.uniform(-0.004, 0.004)
            longitude += rng.uniform(-0.004, 0.004)
            points.append((latitude, longitude))
        encoded = encode_latlon(points)
        if len(decode_latlon(encoded)) != size:
            raise RuntimeError('polyline training setup failed')
        for _ in range(2000):
            if not (
                encode_latlon(points)
                and encode_lonlat(points)
                and decode_latlon(encoded)
                and decode_lonlat(encoded)
            ):
                raise RuntimeError('polyline training returned an empty result')
            units += 4
    pgo_process.completed(units, Path(_lib.__file__))


if __name__ == '__main__':
    if sys.argv[1:] != ['train']:
        raise SystemExit('expected train')
    train()
