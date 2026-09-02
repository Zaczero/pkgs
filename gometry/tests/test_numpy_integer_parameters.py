"""NumPy integer admission for broadcastable integer parameters."""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest


@pytest.mark.parametrize(
    'value',
    [
        pytest.param(np.int64(1), id='numpy-signed-scalar'),
        pytest.param(np.uint64(1), id='numpy-unsigned-scalar'),
        pytest.param(np.array(1, dtype=np.int64), id='signed-zero-d-array'),
        pytest.param(np.array(1, dtype=np.uint64), id='unsigned-zero-d-array'),
    ],
)
def test_integer_parameter_numpy_scalars_and_zero_d_arrays_remain_scalar(value) -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0), (2, 0)]),
        gm.LineString([(0, 0), (1, 0), (2, 0)]),
    ])
    assert [len(line.coords) for line in lines.smooth(iterations=value)] == [4, 4]
    assert [len(group) for group in lines.line_interpolate(count=value)] == [1, 1]

    boxes = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(0, 0, 1, 1)])
    assert [len(group) for group in boxes.sample_points(value, seed=value)] == [1, 1]


def test_integer_parameter_rank_one_numpy_arrays_are_rowwise() -> None:
    """Rank-one signed and unsigned arrays reach the array consumers.

    Mutation: restoring the scalar ``__index__`` probe before rank detection
    makes every rank-one NumPy case fail with NumPy's scalar-index error.
    """
    for dtype in (np.int64, np.uint64):
        lines = gm.GeometryArray([
            gm.LineString([(0, 0), (1, 0), (2, 0)]),
            gm.LineString([(0, 0), (1, 0), (2, 0)]),
        ])
        iterations = np.array([0, 1], dtype=dtype)
        smoothed = lines.smooth(iterations=iterations)
        assert [len(line.coords) for line in smoothed] == [3, 4]

        counts = np.array([1, 2], dtype=dtype)
        interpolated = lines.line_interpolate(count=counts)
        assert [len(group) for group in interpolated] == [1, 2]

        boxes = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(0, 0, 1, 1)])
        sampled = boxes.sample_points(counts, seed=np.array([11, 12], dtype=dtype))
        assert [len(group) for group in sampled] == [1, 2]


def test_integer_parameter_rejects_invalid_dtype_and_higher_rank_arrays() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)]),
        gm.LineString([(0, 0), (1, 0)]),
    ])
    invalid = np.array([1.0, 1.0], dtype=np.float64)
    rank_two = np.array([[1], [1]], dtype=np.int64)
    with pytest.raises(TypeError, match='integer'):
        lines.smooth(iterations=invalid)
    with pytest.raises(TypeError, match='one-dimensional'):
        lines.smooth(iterations=rank_two)
    with pytest.raises(TypeError, match='one-dimensional'):
        lines.line_interpolate(count=rank_two)


def test_integer_parameter_bool_remains_rejected() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)]),
        gm.LineString([(0, 0), (1, 0)]),
    ])
    with pytest.raises(TypeError):
        lines.smooth(iterations=True)
    with pytest.raises(TypeError):
        lines.line_interpolate(count=True)

    boxes = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(0, 0, 1, 1)])
    with pytest.raises(TypeError):
        boxes.sample_points(count=1, seed=True)


def test_integer_parameter_generators_and_length_mismatches_use_exact_collection() -> (
    None
):
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0), (2, 0)]),
        gm.LineString([(0, 0), (1, 0), (2, 0)]),
    ])
    assert [
        len(line.coords) for line in lines.smooth(iterations=(v for v in [0, 1]))
    ] == [3, 4]
    assert [
        len(group) for group in lines.line_interpolate(count=(v for v in [1, 2]))
    ] == [1, 2]

    with pytest.raises(gm.GeometryError, match='length-2'):
        lines.smooth(iterations=np.array([1], dtype=np.int64))
    with pytest.raises(gm.GeometryError, match='length-2'):
        lines.line_interpolate(count=np.array([1], dtype=np.int64))

    boxes = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(0, 0, 1, 1)])
    assert [
        len(group)
        for group in boxes.sample_points(
            (v for v in [1, 2]), seed=(v for v in [11, 12])
        )
    ] == [1, 2]
    with pytest.raises(gm.GeometryError, match='length-2'):
        boxes.sample_points(np.array([1], dtype=np.int64), seed=1)
