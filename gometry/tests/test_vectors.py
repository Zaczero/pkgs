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
