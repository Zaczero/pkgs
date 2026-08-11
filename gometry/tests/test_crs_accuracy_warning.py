from __future__ import annotations

import sys
import threading
import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import gometry as gm
import numpy as np
import pytest


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


def test_available_best_operation_grids_do_not_warn(tmp_path: Path) -> None:
    # PROJ's availability contract is name visibility. The transform may reject
    # these deliberately minimal files, but it must not claim they are missing.
    names = ('us_noaa_conus.tif', 'us_noaa_nbhpgn.tif')
    for name in names:
        (tmp_path / name).write_bytes(b'II*\0' + bytes(64))
    gm.crs_configure(search_paths=[tmp_path])
    assert all(gm.crs_grid(name)['available'] is True for name in names)

    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        gm.crs_transform(4267, 4326, -104.0, 40.0)
    assert not [item for item in seen if item.category is gm.AccuracyWarning]


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


@pytest.mark.skipif(
    not hasattr(sys, '_is_gil_enabled') or sys._is_gil_enabled(),
    reason='requires free-threaded CPython',
)
def test_free_threaded_grid_diagnostics_do_not_cross_threads() -> None:
    assert sys._is_gil_enabled() is False
    barrier = threading.Barrier(8)
    messages: list[str | None] = [None] * 8

    def worker(index: int) -> None:
        point = (-104.0, 40.0) if index % 2 == 0 else (-100.0, 55.0)
        barrier.wait()
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter('always')
            gm.crs_transform(4267, 4326, *point)
        diagnostic = [
            str(item.message) for item in seen if item.category is gm.AccuracyWarning
        ]
        assert len(diagnostic) == 1
        messages[index] = diagnostic[0]

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    for index, message in enumerate(messages):
        assert message is not None
        expected = 'us_noaa_conus.tif' if index % 2 == 0 else 'ca_nrc_ntv2_0.tif'
        assert expected in message
