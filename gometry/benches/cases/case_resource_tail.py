"""Bounded public-API latency/RSS/Python-allocation probe.

Unlike ``bench_ab.py`` block medians, this case retains every kernel-call
sample so p99/p99.9 describe actual calls. RSS includes native allocations;
``tracemalloc`` and allocated-block deltas intentionally describe Python only.
Run one mode per fresh process so ``ru_maxrss`` remains interpretable.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import resource
import statistics
import sys
import time
import tracemalloc
from array import array
from pathlib import Path
from typing import TYPE_CHECKING

import gometry as gm
import gometry._lib as gm_lib
import numpy as np

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any


def percentile(values: list[float] | array[float], quantile: float) -> float:
    ordered = sorted(values)
    position = quantile * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def intersects_case() -> tuple[Callable[[], Any], Callable[[Any], None], int, int]:
    xs = np.linspace(0.0, 100.0, 10_000, endpoint=False)
    ys = np.mod(np.arange(10_000) * 7919, 10_000) / 100.0
    points = gm.points(xs, ys, crs=3857)
    polygon = gm.box(0.0, 0.0, 100.0, 100.0, crs=3857)

    def run() -> Any:
        return gm.intersects(polygon, points)

    def validate(result: Any) -> None:
        assert len(result) == 10_000 and result.all()

    return run, validate, 10_000, 1_000


def bearing_case() -> tuple[Callable[[], Any], Callable[[Any], None], int, int]:
    count = 10_000
    lon = np.linspace(-120.0, -119.0, count, endpoint=False)
    lat = np.linspace(35.0, 36.0, count, endpoint=False)
    left = gm.points(lon, lat, crs=4326)
    right = gm.points(lon + 0.01, lat + 0.015, crs=4326)

    def run() -> Any:
        return gm.bearing(left, right)

    def validate(result: Any) -> None:
        assert len(result) == count and np.isfinite(result).all()

    return run, validate, 2_000, 200


def features_case() -> tuple[Callable[[], Any], Callable[[Any], None], int, int]:
    count = 1_000
    features = {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [i / 10.0, i / 20.0]},
                'properties': {'row': i},
                'id': i,
            }
            for i in range(count)
        ],
    }

    def run() -> Any:
        return gm.from_features(features)

    def validate(result: Any) -> None:
        assert len(result.geometries) == count
        assert result.ids == list(range(count))
        assert result.properties[-1] == {'row': count - 1}

    return run, validate, 10_000, 200


def mixed_case() -> tuple[Callable[[], Any], Callable[[Any], None], int, int]:
    values = gm.GeometryArray(
        [
            gm.Point(179.9, 10.0, z=1.0, m=2.0, crs=4326),
            None,
            gm.LineString([(-179.9, 10.0), (179.8, 10.1)], z=[3.0, 4.0], crs=4326),
            gm.Polygon(
                [(179.7, 9.9), (-179.7, 9.9), (-179.7, 10.2), (179.7, 9.9)], crs=4326
            ),
        ]
        * 2_500
    )

    def run() -> Any:
        return values.to_crs(3857)

    def validate(result: Any) -> None:
        assert len(result) == 10_000 and result[1] is None
        assert result[0].has_z and result[0].has_m

    return run, validate, 2_000, 100


CASES = {
    'bearing': bearing_case,
    'features': features_case,
    'intersects': intersects_case,
    'mixed': mixed_case,
}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def cpu_state() -> dict[str, object]:
    affinity = sorted(os.sched_getaffinity(0))
    states: dict[str, dict[str, str]] = {}
    for cpu in affinity:
        root = Path(f'/sys/devices/system/cpu/cpu{cpu}/cpufreq')
        state: dict[str, str] = {}
        for name in ('scaling_driver', 'scaling_governor', 'scaling_cur_freq'):
            path = root / name
            if path.is_file():
                state[name] = path.read_text().strip()
        if state:
            states[str(cpu)] = state
    return {
        'affinity': affinity,
        'frequency': states,
        'load_average': os.getloadavg(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=CASES, required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()

    environment_before = cpu_state()
    rss_before_setup = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    setup_started = time.perf_counter()
    run, validate, latency_iterations, allocation_iterations = CASES[args.mode]()
    setup_seconds = time.perf_counter() - setup_started
    rss_after_setup = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    warmup_iterations = 64
    warmup_started = time.perf_counter()
    for _ in range(warmup_iterations):
        result = run()
    validate(result)
    warmup_seconds = time.perf_counter() - warmup_started

    samples = array('d')
    for _ in range(latency_iterations):
        started = time.perf_counter_ns()
        result = run()
        samples.append((time.perf_counter_ns() - started) / 1e9)
    validate(result)
    rss_after_latency = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    gc.collect()
    blocks_before = sys.getallocatedblocks()
    tracemalloc.start()
    for _ in range(allocation_iterations):
        result = run()
    _, python_peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    blocks_after = sys.getallocatedblocks()
    validate(result)
    rss_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    extension_path = Path(gm_lib.__file__).resolve()
    case_path = Path(__file__).resolve()
    payload = {
        'schema_version': 2,
        'mode': args.mode,
        'latency_iterations': latency_iterations,
        'allocation_iterations': allocation_iterations,
        'setup_seconds': setup_seconds,
        'warmup_iterations': warmup_iterations,
        'warmup_seconds': warmup_seconds,
        'median_seconds': statistics.median(samples),
        'p99_seconds': percentile(samples, 0.99),
        'p999_seconds': percentile(samples, 0.999),
        'latency_samples_seconds': list(samples),
        'rss_before_setup_kib': rss_before_setup,
        'rss_after_setup_kib': rss_after_setup,
        'rss_after_latency_kib': rss_after_latency,
        'rss_latency_growth_kib': rss_after_latency - rss_after_setup,
        'rss_peak_kib': rss_peak,
        'rss_peak_growth_kib': rss_peak - rss_after_setup,
        'python_peak_traced_bytes': python_peak_bytes,
        'python_allocated_blocks_delta': blocks_after - blocks_before,
        'tracemalloc_active_during_latency': False,
        'extension_path': str(extension_path),
        'extension_sha256': file_sha256(extension_path),
        'case_sha256': file_sha256(case_path),
        'python': sys.version,
        'environment_before': environment_before,
        'environment_after': cpu_state(),
    }
    encoded = json.dumps(payload, sort_keys=True)
    if args.output is None:
        print(encoded)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
