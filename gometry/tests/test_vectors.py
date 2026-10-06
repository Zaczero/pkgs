from __future__ import annotations

import gometry as gm
import numpy as np
import pytest

from tests._support import ndarray_lane


def test_bounds_array_semantics() -> None:
    values = gm.GeometryArray([
        gm.Point(0.0, 0.0),
        gm.GeometryCollection([]),
        gm.box(2.0, 3.0, 4.0, 5.0),
    ]).bounds
    assert isinstance(values, np.ndarray)
    assert values.shape == (3, 4)
    assert values.dtype == np.float64
    assert values.flags.writeable is False
    np.testing.assert_allclose(
        values,
        [[0.0, 0.0, 0.0, 0.0], [np.nan, np.nan, np.nan, np.nan], [2.0, 3.0, 4.0, 5.0]],
        equal_nan=True,
    )
    np.testing.assert_allclose(
        values[::-1],
        [[2.0, 3.0, 4.0, 5.0], [np.nan, np.nan, np.nan, np.nan], [0.0, 0.0, 0.0, 0.0]],
        equal_nan=True,
    )


def test_geographic_point_bounds_follow_aliases_masks_and_retagged_caches() -> None:
    first = gm.Point(450.0, 3.0, crs=4326)
    second = gm.Point(-270.0, 3.0, crs=4326)
    assert first.bounds == (90.0, 3.0, 90.0, 3.0)
    assert second.bounds == (90.0, 3.0, 90.0, 3.0)
    rows = gm.GeometryArray([first, None, second], crs=4326)
    expected = [
        [90.0, 3.0, 90.0, 3.0],
        [np.nan, np.nan, np.nan, np.nan],
        [90.0, 3.0, 90.0, 3.0],
    ]
    for selected in (rows, rows[[2, 1, 0]], rows[:2]):
        for _ in range(2):
            np.testing.assert_allclose(
                selected.bounds, expected[: len(selected)], equal_nan=True
            )
            assert selected.total_bounds == (90.0, 3.0, 90.0, 3.0)
    retagged = rows.set_crs(3857, overwrite=True)
    np.testing.assert_allclose(
        retagged.bounds,
        [[450.0, 3.0, 450.0, 3.0], [np.nan] * 4, [-270.0, 3.0, -270.0, 3.0]],
        equal_nan=True,
    )
    assert retagged.total_bounds == (-270.0, 3.0, 450.0, 3.0)


def test_geographic_total_bounds_aggregate_points_before_choosing_a_hull() -> None:
    points = gm.MultiPoint([(0.0, 0.0), (120.0, 0.0), (-120.0, 0.0)], crs=4326)
    extra = gm.Point(180.0, 0.0, crs=4326)
    assert points.bounds == (-120.0, 0.0, 120.0, 0.0)
    rows = gm.GeometryArray([points, extra], crs=4326)
    # The largest gaps are (-120, 0) and (0, 120). Their complements
    # start at 0 and 120, so the canonical-start tie chooses the 240° arc from 0.
    assert rows.total_bounds == (0.0, 0.0, -120.0, 0.0)
    np.testing.assert_array_equal(
        rows.bounds, [[-120.0, 0.0, 120.0, 0.0], [-180.0, 0.0, -180.0, 0.0]]
    )
    assert rows.total_bounds == (0.0, 0.0, -120.0, 0.0)
    assert rows[[1, 0, 1]].total_bounds == (0.0, 0.0, -120.0, 0.0)


def test_buffer_vectors_array_protocol_dtype_and_copy() -> None:
    points = gm.points([0.0, 1.0, 2.0], [0.0, 0.0, 0.0])
    floats = gm.distance(points, gm.Point(0, 0))
    bools = points.is_empty
    ids = gm.SpatialIndex(points).query(gm.box(-1, -1, 1.5, 1.5))
    keys = (points).spatial_key(curve='hilbert')
    assert ndarray_lane(np.asarray(floats, copy=False), np.float64) is floats
    assert ndarray_lane(np.asarray(bools, copy=False), np.bool_) is bools
    assert ndarray_lane(np.asarray(ids, copy=False), np.int64) is ids
    assert ndarray_lane(np.asarray(keys, copy=False), np.uint64) is keys
    copied = np.asarray(floats, dtype=np.float32, copy=True)
    assert copied.dtype == np.float32
    assert copied.flags.owndata
    with pytest.raises(ValueError, match='copy'):
        np.asarray(floats, dtype=np.float32, copy=False)
