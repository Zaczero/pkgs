"""Space-filling curves — Hilbert/Morton keys, locality sorts, frames,
and the GeoPandas-interoperable key values.
"""

import gometry as gm
import numpy as np
import pytest

from tests._support import ndarray_lane, uints


def test_hilbert_keys_match_geopandas_and_preserve_locality() -> None:
    points = gm.points([0.0, 1.0, 0.1], [0.0, 1.0, 0.1])
    assert uints((points).spatial_key(curve='hilbert', bounds=(0, 0, 1, 1))) == [
        0,
        2863311530,
        42107522,
    ]
    assert uints((points).spatial_key(curve='hilbert')) == [0, 2863311530, 42107522]
    a, b, c = map(
        int,
        (gm.points([0.1, 0.1001, 0.9], [0.1, 0.1, 0.9])).spatial_key(curve='hilbert'),
    )
    assert abs(a - b) < abs(a - c)
    assert uints((points).spatial_key(curve='hilbert')) == [0, 2863311530, 42107522]


def test_morton_keys_interleave_and_sort_orders_storage() -> None:
    points = gm.points([0.0, 1.0, 0.1], [0.0, 1.0, 0.1])
    assert uints((points).spatial_key(curve='morton')) == [0, 4294967295, 63161283]
    quad = gm.points([0.0, 1.0, 0.0, 1.0], [0.0, 0.0, 1.0, 1.0])
    assert uints((quad).spatial_key(curve='morton', level=1, bounds=(0, 0, 1, 1))) == [
        0,
        1,
        2,
        3,
    ]
    scattered = gm.points([5.0, 0.0, 5.1, 0.1], [5.0, 0.0, 5.1, 0.1], crs=3857)
    ordered = scattered.sort_by_spatial_key(curve='hilbert')
    assert ordered.crs == gm.CRS('EPSG:3857')
    assert [round(p.x, 1) for p in ordered] == [0.0, 0.1, 5.0, 5.1]
    assert [round(p.x, 1) for p in scattered.sort_by_spatial_key(curve='morton')] == [
        0.0,
        0.1,
        5.0,
        5.1,
    ]


def test_curve_frames_and_errors() -> None:
    outside_and_corner = gm.points([10.0, 1.0], [10.0, 1.0])
    clamped, inside = uints(
        outside_and_corner.spatial_key(curve='morton', bounds=(0, 0, 1, 1))
    )
    assert clamped == inside
    assert uints(
        outside_and_corner.spatial_key(curve='morton', bounds=(0, 0, 1, 1), level=16)
    ) == [clamped, inside]
    # A single point's own total bounds are a degenerate frame.
    assert uints(gm.points([3.0], [4.0]).spatial_key(curve='hilbert')) == [0]
    keys = (gm.GeometryArray([])).spatial_key(curve='hilbert')
    ndarray_lane(keys, np.uint64, shape=(0,))
    rows = gm.GeometryArray([gm.Point(0, 0), gm.from_wkt('POINT EMPTY'), None])
    sentinel = np.iinfo(np.uint64).max
    assert rows.spatial_key(curve='hilbert').tolist() == [0, sentinel, sentinel]
    assert rows.spatial_key(curve='morton').tolist() == [0, sentinel, sentinel]
    assert rows.sort_by_spatial_key(curve='hilbert').to_wkt() == [
        'POINT (0 0)',
        'POINT EMPTY',
        None,
    ]
    assert rows.sort_by_spatial_key(curve='morton').to_wkt() == [
        'POINT (0 0)',
        'POINT EMPTY',
        None,
    ]
    origin = gm.points([0.0], [0.0])
    with pytest.raises(gm.GeometryError, match='between 1 and 32, got 0'):
        origin.spatial_key(curve='hilbert', level=0)
    with pytest.raises(gm.GeometryError, match='got 3 values'):
        origin.spatial_key(curve='hilbert', bounds=(0, 0, 1))
    with pytest.raises(gm.GeometryError, match='min <= max'):
        origin.spatial_key(curve='hilbert', bounds=(1, 0, 0, 1))
