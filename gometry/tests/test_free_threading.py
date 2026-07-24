"""Concurrent stress test for free-threaded CPython support.

On a free-threaded build (``sys._is_gil_enabled()`` is ``False``), threads
exercise gometry kernels in parallel on shared inputs. On a GIL build the same
work still runs as a serialized smoke test (no deadlock or corruption).
"""

from __future__ import annotations

import sys
import threading
from typing import Any

import gometry as gm
import pytest

_THREAD_COUNT = 8
_BARRIER_TIMEOUT = 120.0


def _gil_enabled() -> bool | None:
    probe = getattr(sys, '_is_gil_enabled', None)
    if probe is None:
        return None
    return bool(probe())


def _shared_fixtures() -> dict[str, Any]:
    poly = gm.box(-2, -2, 2, 2)
    hole = gm.Point(0, 0)
    line = gm.LineString([(-3, 0), (3, 0)])
    probe = gm.Point(1, 1)
    outside = gm.Point(5, 5)
    values = [poly, line, gm.Point(-1, -1), outside]
    idx = gm.SpatialIndex(values)
    prepared = poly.prepare()
    coverage = gm.h3_cover(poly, resolution=5)
    arr = gm.GeometryArray(values)
    wgs = gm.box(-1, -1, 1, 1, crs=4326)
    wgs_point = gm.Point(0.1, 0.1, crs=4326)
    merc = gm.CRS(3857)
    return {
        'poly': poly,
        'hole': hole,
        'line': line,
        'probe': probe,
        'outside': outside,
        'values': values,
        'idx': idx,
        'prepared': prepared,
        'coverage': coverage,
        'arr': arr,
        'wgs': wgs,
        'wgs_point': wgs_point,
        'merc': merc,
        'union': gm.union(poly, line),
        'intersection': gm.intersection(poly, line),
        'hausdorff': gm.hausdorff_distance(poly, line),
        'distance': gm.distance(poly, probe),
        'idx_hits': idx.query(probe).tolist(),
        'prepared_contains': prepared.contains(hole),
        'prepared_intersects': prepared.intersects(line),
        'coverage_contains': coverage.contains(wgs_point),
        'to_crs': wgs.to_crs(merc),
        'arr_areas': arr.area.tolist(),
        'arr_intersects': gm.intersects(arr, probe).tolist(),
    }


def _worker(
    fixtures: dict[str, Any],
    barrier: threading.Barrier,
    errors: list[str],
    thread_id: int,
) -> None:
    try:
        barrier.wait(timeout=_BARRIER_TIMEOUT)
        poly = fixtures['poly']
        hole = fixtures['hole']
        line = fixtures['line']
        probe = fixtures['probe']
        outside = fixtures['outside']
        idx = fixtures['idx']
        prepared = fixtures['prepared']
        coverage = fixtures['coverage']
        arr = fixtures['arr']
        wgs = fixtures['wgs']
        wgs_point = fixtures['wgs_point']
        merc = fixtures['merc']

        assert prepared.contains(hole) == fixtures['prepared_contains']
        assert prepared.intersects(line) == fixtures['prepared_intersects']
        assert prepared.disjoint(outside)
        assert idx.query(probe).tolist() == fixtures['idx_hits']
        assert idx.candidates(probe).tolist() == fixtures['idx_hits']
        assert gm.distance(poly, probe) == fixtures['distance']
        assert gm.hausdorff_distance(poly, line) == fixtures['hausdorff']
        assert gm.union(poly, line) == fixtures['union']
        assert gm.intersection(poly, line) == fixtures['intersection']
        assert coverage.contains(wgs_point) == fixtures['coverage_contains']
        assert wgs.to_crs(merc) == fixtures['to_crs']
        assert arr.area.tolist() == fixtures['arr_areas']
        assert gm.intersects(arr, probe).tolist() == fixtures['arr_intersects']

        # Per-thread mutation lane: each thread owns its index handle.
        local_idx = gm.SpatialIndex(fixtures['values'])
        handle = local_idx.insert(gm.Point(float(thread_id), float(thread_id)))
        assert local_idx.remove(handle)
    except Exception as exc:  # pragma: no cover - surfaced via pytest
        errors.append(f'thread {thread_id}: {exc!r}')


def test_free_threading_stress() -> None:
    fixtures = _shared_fixtures()
    barrier = threading.Barrier(_THREAD_COUNT + 1)
    errors: list[str] = []
    threads = [
        threading.Thread(
            target=_worker,
            args=(fixtures, barrier, errors, thread_id),
            name=f'gometry-free-thread-{thread_id}',
        )
        for thread_id in range(_THREAD_COUNT)
    ]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=_BARRIER_TIMEOUT)
    for thread in threads:
        thread.join(timeout=_BARRIER_TIMEOUT)
        assert not thread.is_alive(), f'{thread.name} did not finish'
    if errors:
        pytest.fail('\n'.join(errors))

    gil = _gil_enabled()
    if gil is False:
        # Real parallelism path exercised above.
        assert True
    elif gil is True:
        # Serialized smoke on a GIL build — still must be correct.
        assert True
    else:
        pytest.skip('sys._is_gil_enabled unavailable on this interpreter')


def test_prepared_geometry_is_sendable() -> None:
    prepared = gm.box(0, 0, 1, 1).prepare()
    holder: list[Any] = []

    def capture() -> None:
        holder.append(prepared)

    thread = threading.Thread(target=capture)
    thread.start()
    thread.join(timeout=10.0)
    assert not thread.is_alive()
    assert holder[0].contains(gm.Point(0.5, 0.5))


def test_shared_coverage_iterator_drains_once_across_threads() -> None:
    coverage = gm.h3_cover(gm.box(-3, -3, 3, 3, crs=4326), resolution=5)
    expected = [int(cell) for cell in coverage]
    shared = iter(coverage)
    barrier = threading.Barrier(_THREAD_COUNT + 1)
    errors: list[str] = []
    results: list[int] = []
    results_lock = threading.Lock()

    def drain(thread_id: int) -> None:
        try:
            local: list[int] = []
            barrier.wait(timeout=_BARRIER_TIMEOUT)
            while True:
                try:
                    local.append(int(next(shared)))
                except StopIteration:
                    break
            with results_lock:
                results.extend(local)
        except Exception as exc:  # pragma: no cover - surfaced via pytest
            errors.append(f'thread {thread_id}: {exc!r}')

    threads = [
        threading.Thread(
            target=drain,
            args=(thread_id,),
            name=f'gometry-coverage-iter-{thread_id}',
        )
        for thread_id in range(_THREAD_COUNT)
    ]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=_BARRIER_TIMEOUT)
    for thread in threads:
        thread.join(timeout=_BARRIER_TIMEOUT)
        assert not thread.is_alive(), f'{thread.name} did not finish'
    if errors:
        pytest.fail('\n'.join(errors))

    assert len(results) == len(expected)
    assert len(set(results)) == len(expected)
    assert sorted(results) == sorted(expected)


def test_shared_spatial_index_concurrent_queries_and_serialized_mutation() -> None:
    planar = gm.SpatialIndex([
        gm.box(0, 0, 2, 2),
        gm.Point(5, 5),
        gm.LineString([(10, 0), (10, 4)]),
    ])
    planar_probe = gm.box(-1, -1, 3, 3)
    planar_expected = planar.query(planar_probe).tolist()

    geographic_values = gm.points(
        [-73.9857, -73.9, -74.2, -0.1],
        [40.7484, 40.7, 40.9, 51.5],
        crs=4326,
    )
    geographic = gm.SpatialIndex(geographic_values)
    geographic_probe = gm.Point(-73.98, 40.75, crs=4326)
    geographic_expected = geographic.query(
        geographic_probe,
        predicate='dwithin',
        distance=50_000.0,
    ).tolist()

    mutable = gm.SpatialIndex([gm.Point(-120.0, 35.0, crs=4326)])
    mutation_lock = threading.Lock()
    barrier = threading.Barrier(_THREAD_COUNT + 1)
    errors: list[str] = []

    def query_and_mutate(thread_id: int) -> None:
        try:
            barrier.wait(timeout=_BARRIER_TIMEOUT)
            for _ in range(100):
                assert planar.query(planar_probe).tolist() == planar_expected
                assert (
                    geographic.query(
                        geographic_probe,
                        predicate='dwithin',
                        distance=50_000.0,
                    ).tolist()
                    == geographic_expected
                )

            for step in range(10):
                point = gm.Point(
                    -120.0 + thread_id * 0.1 + step * 0.001,
                    35.0 + thread_id * 0.1,
                    crs=4326,
                )
                with mutation_lock:
                    handle = mutable.insert(point)
                    assert handle in mutable.query(point).tolist()
                    assert mutable.remove(handle)
                    assert handle not in mutable.query(point).tolist()
        except Exception as exc:  # pragma: no cover - surfaced via pytest
            errors.append(f'thread {thread_id}: {exc!r}')

    threads = [
        threading.Thread(
            target=query_and_mutate,
            args=(thread_id,),
            name=f'gometry-index-query-{thread_id}',
        )
        for thread_id in range(_THREAD_COUNT)
    ]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=_BARRIER_TIMEOUT)
    for thread in threads:
        thread.join(timeout=_BARRIER_TIMEOUT)
        assert not thread.is_alive(), f'{thread.name} did not finish'
    if errors:
        pytest.fail('\n'.join(errors))
