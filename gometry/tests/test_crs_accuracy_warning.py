from __future__ import annotations

import math
import queue
import shutil
import threading
import time
import traceback
import warnings
from pathlib import Path
from typing import TYPE_CHECKING

import gometry as gm
import numpy as np
import pytest

from tests._support import gil_is_disabled

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType


@pytest.fixture(autouse=True)
def _isolated_crs_runtime() -> None:
    gm.crs_reset()
    yield
    gm.crs_reset()


def _warning_for(x: float, y: float) -> str:
    with pytest.warns(gm.AccuracyWarning) as seen:
        gm.crs_transform(4267, 4326, x, y)
    assert len(seen) == 1
    return str(seen[0].message)


def test_missing_best_grid_warning_is_coordinate_specific() -> None:
    gm.crs_clear_cache()
    conus = _warning_for(-104.0, 40.0)
    canada = _warning_for(-100.0, 55.0)

    assert 'us_noaa_conus.tif' in conus
    assert 'ca_nrc_ntv2_0.tif' in canada
    assert conus != canada


def test_transform_engine_reports_degraded_proj_directly() -> None:
    gm.crs_clear_cache()
    assert gm.crs_cache_info()['last_transform_engine'] is None

    for source, target in ((4267, 4326), (4326, 3857)):
        gm.crs_clear_cache()
        empty = gm.crs_transform(source, target, [], [])
        assert empty.shape == (0, 2)
        no_execution = gm.crs_cache_info()
        assert no_execution['last_transform_engine'] is None
        assert no_execution['transform_invocations'] == 0

    gm.crs_clear_cache()
    with pytest.raises(gm.InvalidGeometryError):
        gm.crs_transform(4326, 4326, 0.0, 91.0)
    rejected = gm.crs_cache_info()
    assert rejected['last_transform_engine'] is None
    assert rejected['transform_invocations'] == 0

    gm.crs_clear_cache()
    with pytest.warns(gm.AccuracyWarning):
        result = gm.crs_transform(4267, 4326, -122.0, 45.0)
    assert result == pytest.approx((-122.00115466261455, 44.99980363943926))
    degraded = gm.crs_cache_info()
    assert degraded['last_transform_engine'] == 'proj'
    assert degraded['transform_invocations'] == 1
    assert (
        next(
            bucket['entries']
            for bucket in degraded['buckets']
            if bucket['name'] == 'proj_pipeline'
        )
        == 0
    )

    gm.crs_clear_cache()
    gm.crs_transform(4326, 3857, -122.0, 45.0)
    admitted = gm.crs_cache_info()
    assert admitted['last_transform_engine'] == 'in_core'
    assert admitted['transform_invocations'] == 1


def test_internal_transform_diagnostic_does_not_leak_to_next_call() -> None:
    values = gm.GeometryArray([gm.Point(-104.0, 40.0, crs=4267)])
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        gm.h3_cells(values, resolution=5)
        gm.Point(0.0, 0.0, crs=4326).to_crs(3857)
    assert not [item for item in seen if item.category is gm.AccuracyWarning]


@pytest.mark.parametrize(
    'operation',
    [
        pytest.param(
            lambda _point, values: gm.h3_cells(values, resolution=5),
            id='h3-cells-array',
        ),
        pytest.param(
            lambda _point, values: gm.s2_cells(values, level=5),
            id='s2-cells-array',
        ),
        pytest.param(
            lambda _point, values: gm.geohash_cells(values, precision=5),
            id='geohash-cells-array',
        ),
        pytest.param(
            lambda _point, values: gm.tile_cells(values, zoom=5),
            id='tile-cells-array',
        ),
        pytest.param(
            lambda point, _values: gm.H3Cell(point, resolution=5),
            id='h3-constructor',
        ),
        pytest.param(
            lambda point, _values: gm.S2Cell(point, level=5),
            id='s2-constructor',
        ),
        pytest.param(
            lambda point, _values: gm.GeohashCell(point, precision=5),
            id='geohash-constructor',
        ),
        pytest.param(
            lambda point, _values: gm.Tile(point, zoom=5),
            id='tile-constructor',
        ),
        pytest.param(
            lambda point, _values: gm.h3_cover(point, resolution=1),
            id='h3-cover',
        ),
        pytest.param(
            lambda point, _values: gm.s2_cover(point, level=1),
            id='s2-cover',
        ),
        pytest.param(
            lambda point, _values: gm.geohash_cover(point, precision=1),
            id='geohash-cover',
        ),
        pytest.param(
            lambda point, _values: gm.tile_cover(point, zoom=1),
            id='tile-cover',
        ),
        pytest.param(
            lambda _point, values: gm.h3_cover(values, resolution=1),
            id='h3-cover-array',
        ),
        pytest.param(
            lambda _point, values: gm.s2_cover(values, level=1),
            id='s2-cover-array',
        ),
        pytest.param(
            lambda _point, values: gm.geohash_cover(values, precision=1),
            id='geohash-cover-array',
        ),
        pytest.param(
            lambda _point, values: gm.tile_cover(values, zoom=1),
            id='tile-cover-array',
        ),
        pytest.param(
            lambda point, _values: gm.h3_bounding_cell(point),
            id='h3-bounding-cell',
        ),
        pytest.param(
            lambda point, _values: gm.s2_bounding_cell(point),
            id='s2-bounding-cell',
        ),
        pytest.param(
            lambda point, _values: gm.geohash_bounding_cell(point),
            id='geohash-bounding-cell',
        ),
        pytest.param(
            lambda point, _values: gm.tile_bounding_cell(point),
            id='tile-bounding-cell',
        ),
        pytest.param(
            lambda _point, values: gm.h3_bounding_cell(values),
            id='h3-bounding-cell-array',
        ),
        pytest.param(
            lambda _point, values: gm.s2_bounding_cell(values),
            id='s2-bounding-cell-array',
        ),
        pytest.param(
            lambda _point, values: gm.geohash_bounding_cell(values),
            id='geohash-bounding-cell-array',
        ),
        pytest.param(
            lambda _point, values: gm.tile_bounding_cell(values),
            id='tile-bounding-cell-array',
        ),
    ],
)
def test_internal_grid_operations_consume_reprojection_diagnostics(operation) -> None:
    point = gm.Point(-104.0, 40.0, crs=4267)
    values = gm.GeometryArray([point])
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        operation(point, values)
        gm.Point(0.0, 0.0, crs=4326).to_crs(3857)
    assert not [item for item in seen if item.category is gm.AccuracyWarning]


def test_internal_grid_error_consumes_reprojection_diagnostic() -> None:
    values = gm.GeometryArray([
        gm.Point(-104.0, 40.0, crs=4267),
        gm.Point(-104.0, 89.0, crs=4267),
    ])
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        with pytest.raises(
            gm.InvalidGeometryError, match='square Web Mercator tile grid'
        ):
            gm.tile_cells(values, zoom=5)
        gm.Point(0.0, 0.0, crs=4326).to_crs(3857)
    assert not [item for item in seen if item.category is gm.AccuracyWarning]


def test_internal_transform_error_preserves_error_and_consumes_diagnostic() -> None:
    values = gm.GeometryArray([
        gm.Point(-104.0, 40.0, crs=4267),
        gm.Point(-104.0, 91.0, crs=4267),
    ])
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        with pytest.raises(gm.TransformError) as error:
            gm.tile_cells(values, zoom=5)
        gm.Point(0.0, 0.0, crs=4326).to_crs(3857)
    assert error.value.source == 'EPSG:4267'
    assert error.value.target == 'EPSG:4326'
    assert 'while processing array element 1' in error.value.__notes__
    assert 'latitude 9.1e1 is outside the geographic coordinate domain' in str(
        error.value
    )
    assert not [item for item in seen if item.category is gm.AccuracyWarning]


def test_internal_grid_diagnostic_does_not_survive_cache_lifecycle() -> None:
    point = gm.Point(-104.0, 40.0, crs=4267)
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        gm.H3Cell(point, resolution=5)
        gm.crs_clear_cache()
        gm.crs_reset()
        gm.Point(0.0, 0.0, crs=4326).to_crs(3857)
    assert not [item for item in seen if item.category is gm.AccuracyWarning]


@pytest.mark.parametrize(
    'transform',
    [
        lambda: gm.crs_transform(4267, 4326, -104.0, 40.0),
        lambda: gm.Point(-104.0, 40.0, crs=4267).to_crs(4326),
        lambda: gm.GeometryArray([gm.Point(-104.0, 40.0, crs=4267)]).to_crs(4326),
        lambda: gm.crs_transform_bounds(4267, 4326, (-104.0, 40.0, -104.0, 40.0)),
    ],
    ids=['coordinates', 'geometry', 'array', 'bounds'],
)
def test_all_primary_transform_surfaces_warn(transform) -> None:
    with pytest.warns(gm.AccuracyWarning) as seen:
        transform()
    assert len(seen) == 1
    assert 'us_noaa_conus.tif' in str(seen[0].message)


def test_roundtrip_transform_surface_warns_once() -> None:
    with pytest.warns(gm.AccuracyWarning) as seen:
        result = gm.crs_roundtrip(4267, 4326, -104.0, 40.0)
    assert math.isfinite(result)
    assert len(seen) == 1
    assert 'us_noaa_conus.tif' in str(seen[0].message)


def test_available_best_operation_grids_execute_without_warning() -> None:
    grid_dir = Path(__file__).with_name('fixtures')
    names = ('us_noaa_conus.tif', 'us_noaa_nbhpgn.tif')
    gm.crs_configure(search_paths=[grid_dir])
    assert all(gm.crs_grid(name)['available'] is True for name in names)

    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        result = gm.crs_transform(4267, 4326, -104.0, 40.0)
    assert result == pytest.approx((-104.0005055600116, 39.99998770647132), abs=1e-12)
    assert not [item for item in seen if item.category is gm.AccuracyWarning]


def test_replaced_grid_is_reprobed_before_public_availability_and_execution(
    tmp_path: Path,
) -> None:
    fixture_dir = Path(__file__).with_name('fixtures')
    names = ('us_noaa_conus.tif', 'us_noaa_nbhpgn.tif')
    for name in names:
        shutil.copyfile(fixture_dir / name, tmp_path / name)
    gm.crs_configure(search_paths=[tmp_path])

    assert all(gm.crs_grid(name)['available'] is True for name in names)
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        valid = gm.crs_transform(4267, 4326, -104.0, 40.0)
    assert valid == pytest.approx((-104.0005055600116, 39.99998770647132), abs=1e-12)
    assert not [item for item in seen if item.category is gm.AccuracyWarning]

    (tmp_path / 'us_noaa_conus.tif').write_bytes(b'II*\x00' + b'\x00' * 64)
    assert gm.crs_grid('us_noaa_conus.tif')['available'] is False
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        replaced = gm.crs_transform(4267, 4326, -104.0, 40.0)
    assert replaced == pytest.approx((-104.00054135243299, 39.999980362118514))
    assert not [item for item in seen if item.category is gm.AccuracyWarning]
    with pytest.raises(gm.TransformError, match='No operation matching criteria found'):
        gm.crs_transform(4267, 4326, -104.0, 40.0, only_best=True)


def test_malformed_grid_is_not_reported_as_available(tmp_path: Path) -> None:
    (tmp_path / 'us_noaa_conus.tif').write_bytes(b'II*\x00' + b'\x00' * 64)
    gm.crs_configure(search_paths=[tmp_path])

    assert gm.crs_grid('us_noaa_conus.tif')['available'] is False


def test_only_best_raises_transform_error_naming_missing_grid() -> None:
    with pytest.raises(gm.TransformError, match=r'us_noaa_conus[.]tif'):
        gm.crs_transform(4267, 4326, -104.0, 40.0, only_best=True)


def test_grid_free_transform_does_not_warn() -> None:
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        gm.crs_transform(4326, 3857, 0.0, 0.0)
    assert not [item for item in seen if item.category is gm.AccuracyWarning]


def test_large_batch_warns_once_and_uses_one_cached_pipeline() -> None:
    gm.crs_clear_cache()
    values = np.full(100_000, -104.0)
    latitudes = np.full(100_000, 40.0)
    with pytest.warns(gm.AccuracyWarning) as seen:
        gm.crs_transform(4267, 4326, values, latitudes)
    assert len(seen) == 1
    engine = gm.crs_cache_info()
    assert engine['last_transform_engine'] == 'proj'
    assert engine['transform_invocations'] == 1
    pipeline = next(
        bucket
        for bucket in gm.crs_cache_info()['buckets']
        if bucket['name'] == 'proj_pipeline'
    )
    # A degraded PJ is evicted because PROJ latches its diagnostic off after
    # the first coordinate; retaining it would leak that coordinate's grid
    # name into the next call. The entire batch still uses one PJ/FFI call.
    assert pipeline['entries'] == 0


def test_empty_transform_after_degradation_does_not_warn() -> None:
    _warning_for(-104.0, 40.0)
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        result = gm.crs_transform(4267, 4326, [], [])
    assert result.shape == (0, 2)
    assert not [item for item in seen if item.category is gm.AccuracyWarning]


def test_grid_database_exposes_download_metadata() -> None:
    grid = gm.crs_grid('us_noaa_conus.tif')
    assert grid['url'] is not None and grid['url'].startswith('https://')
    assert isinstance(grid['direct_download'], bool)


def test_operation_enumeration_keeps_unavailable_best_candidate() -> None:
    operations = gm.CRS(4267).operations(4326)
    assert operations[0]['accuracy'] == 2.0
    assert operations[0]['grids']
    assert any(grid['available'] is False for grid in operations[0]['grids'])


_FREE_THREADED_WORKER_COUNT = 8
_FREE_THREADED_BARRIER_TIMEOUT_SECONDS = 30.0
_FREE_THREADED_JOIN_TIMEOUT_SECONDS = 60.0


def _run_free_threaded_workers(
    scenario: str,
    worker: Callable[[int, threading.Barrier], None],
) -> None:
    barrier = threading.Barrier(_FREE_THREADED_WORKER_COUNT)
    failures: queue.Queue[tuple[int, BaseException, TracebackType | None]] = (
        queue.Queue()
    )

    def record_failure(
        index: int,
        error: BaseException,
        error_traceback: TracebackType | None,
    ) -> None:
        # Queue before abort wakes peers, and preserve this error ahead of the
        # BrokenBarrierError failures the abort causes.
        failures.put((index, error, error_traceback))
        barrier.abort()

    def run_worker(index: int) -> None:
        try:
            worker(index, barrier)
        except BaseException as error:
            record_failure(index, error, error.__traceback__)

    # Python cannot cancel a worker wedged in native code; the focused gate
    # owns process-level termination after this helper reports survivors.
    threads = [
        threading.Thread(
            target=run_worker,
            args=(index,),
            name=f'{scenario} worker {index}',
            daemon=True,
        )
        for index in range(_FREE_THREADED_WORKER_COUNT)
    ]
    deadline = time.monotonic() + _FREE_THREADED_JOIN_TIMEOUT_SECONDS
    started: list[threading.Thread] = []
    for index, thread in enumerate(threads):
        try:
            thread.start()
        except BaseException as error:
            record_failure(index, error, error.__traceback__)
            break
        started.append(thread)

    for thread in started:
        thread.join(timeout=max(0.0, deadline - time.monotonic()))

    worker_failures: list[tuple[int, BaseException, TracebackType | None]] = []
    while True:
        try:
            worker_failures.append(failures.get_nowait())
        except queue.Empty:
            break

    alive = [index for index, thread in enumerate(threads) if thread.is_alive()]
    failure_details = '\n'.join(
        f'{scenario}: worker {index} raised {type(error).__name__}\n'
        + ''.join(traceback.format_exception(type(error), error, error_traceback))
        for index, error, error_traceback in worker_failures
    )
    if alive:
        failure_details += (
            f'\n{scenario}: worker indexes {alive} remained alive after the shared '
            f'{_FREE_THREADED_JOIN_TIMEOUT_SECONDS:.0f}-second join deadline'
        )

    if worker_failures:
        primary_failure = next(
            (
                failure
                for failure in worker_failures
                if not isinstance(failure[1], threading.BrokenBarrierError)
            ),
            worker_failures[0],
        )
        _, primary_error, primary_traceback = primary_failure
        primary_error.add_note(failure_details)
        raise primary_error.with_traceback(primary_traceback)
    if alive:
        raise AssertionError(failure_details)


@pytest.mark.skipif(
    not gil_is_disabled(),
    reason='requires free-threaded CPython',
)
def test_free_threaded_grid_diagnostics_do_not_cross_threads() -> None:
    assert gil_is_disabled()
    messages: list[str | None] = [None] * _FREE_THREADED_WORKER_COUNT

    def worker(index: int, barrier: threading.Barrier) -> None:
        point = (-104.0, 40.0) if index % 2 == 0 else (-100.0, 55.0)
        barrier.wait(timeout=_FREE_THREADED_BARRIER_TIMEOUT_SECONDS)
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter('always')
            gm.crs_transform(4267, 4326, *point)
        diagnostic = [
            str(item.message) for item in seen if item.category is gm.AccuracyWarning
        ]
        assert len(diagnostic) == 1
        messages[index] = diagnostic[0]

    _run_free_threaded_workers('grid diagnostics', worker)

    for index, message in enumerate(messages):
        assert message is not None
        expected = 'us_noaa_conus.tif' if index % 2 == 0 else 'ca_nrc_ntv2_0.tif'
        assert expected in message


@pytest.mark.skipif(
    not gil_is_disabled(),
    reason='requires free-threaded CPython',
)
def test_free_threaded_internal_grid_diagnostics_stay_owned() -> None:
    assert gil_is_disabled()
    warning_counts: list[int | None] = [None] * _FREE_THREADED_WORKER_COUNT

    def worker(index: int, barrier: threading.Barrier) -> None:
        point = gm.Point(
            -104.0 if index % 2 == 0 else -100.0,
            40.0 if index % 2 == 0 else 55.0,
            crs=4267,
        )
        barrier.wait(timeout=_FREE_THREADED_BARRIER_TIMEOUT_SECONDS)
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter('always')
            if index % 2 == 0:
                gm.H3Cell(point, resolution=5)
            else:
                gm.s2_cells(gm.GeometryArray([point]), level=5)
            gm.Point(0.0, 0.0, crs=4326).to_crs(3857)
        warning_counts[index] = sum(
            item.category is gm.AccuracyWarning for item in seen
        )

    _run_free_threaded_workers('internal grid diagnostics', worker)

    assert warning_counts == [0] * _FREE_THREADED_WORKER_COUNT
