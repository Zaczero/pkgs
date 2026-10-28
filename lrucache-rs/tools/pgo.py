"""Collect a mixed cache workload from a normally exiting Python process."""

from __future__ import annotations

import sys
from pathlib import Path

import pgo_process

GENERATE_FLAGS = ()
PACKAGE = Path(__file__).resolve().parents[1]
OPERATIONS = 71_876
EXPECTED_FUNCTIONS = ['8LRUCache6access', '5Store6insert']
stage_generator = pgo_process.stage_generator
verify_runtime = pgo_process.verify_runtime
known_profile_diagnostic = pgo_process.known_profile_diagnostic


def collect(output: Path) -> Path:
    from lrucache_rs import _lib

    return pgo_process.collect(
        output,
        Path(__file__),
        Path(_lib.__file__),
        units=OPERATIONS,
        functions=EXPECTED_FUNCTIONS,
    )


def test(output: Path) -> None:
    pgo_process.run_tests(output, PACKAGE, ignore=('test_stubs.py',))


def train() -> None:
    from lrucache_rs import LRUCache, _lib

    cache = LRUCache(512)
    for i in range(512):
        cache[i] = i
    for i in range(256):
        if cache.get(i) != i:
            raise RuntimeError('cache training setup lost a hot key')
    units = 0
    for i in range(50_000):
        key = (i * 37) % 256
        if cache.get(key) is None:
            raise RuntimeError('cache training lost a hot key')
        units += 1
        if i % 4 == 0:
            if cache.get(key + 100_000) is not None:
                raise RuntimeError('cache training miss unexpectedly hit')
            units += 1
        if i % 8 == 0:
            cache[key] = i
            units += 1
        if i % 32 == 0:
            cache[512 + i] = i
            cache[key] = i
            units += 2
    pgo_process.completed(units, Path(_lib.__file__))


if __name__ == '__main__':
    if sys.argv[1:] != ['train']:
        raise SystemExit('expected train')
    train()
