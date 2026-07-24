"""Packed Points slice/take — zero-copy contiguous windows vs scatter copy."""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest
from conftest import canon


def _packed_points() -> gm.GeometryArray:
    return gm.points(
        [0.0, 1.0, 2.0, 3.0, 4.0], [10.0, 11.0, 12.0, 13.0, 14.0], crs=4326
    )


def test_packed_points_contiguous_slice_shares_x_buffer() -> None:
    packed = _packed_points()
    parent_x = np.asarray(packed.coords.x)
    sliced = packed[1:4]
    child_x = np.asarray(sliced.coords.x)
    assert np.shares_memory(parent_x, child_x)
    assert child_x.tolist() == [1.0, 2.0, 3.0]
    assert canon(list(sliced)) == canon(list(packed)[1:4])


def test_packed_points_contiguous_take_shares_x_buffer() -> None:
    packed = _packed_points()
    parent_x = np.asarray(packed.coords.x)
    taken = packed[[1, 2, 3]]
    child_x = np.asarray(taken.coords.x)
    assert np.shares_memory(parent_x, child_x)
    assert child_x.tolist() == [1.0, 2.0, 3.0]


@pytest.mark.parametrize(
    'selector',
    [
        pytest.param(slice(None, None, -1), id='reversed'),
        pytest.param(slice(None, None, 2), id='stride-2'),
    ],
)
def test_packed_points_non_contiguous_slice_copies(selector: slice) -> None:
    packed = _packed_points()
    parent_x = np.asarray(packed.coords.x)
    sliced = packed[selector]
    child_x = np.asarray(sliced.coords.x)
    assert not np.shares_memory(parent_x, child_x)
    assert canon(list(sliced)) == canon(list(packed)[selector])


def test_packed_points_scatter_take_exports_logical_x_buffer() -> None:
    packed = _packed_points()
    parent_x = np.asarray(packed.coords.x)
    taken = packed[[4, 0, 2]]
    child_x = np.asarray(taken.coords.x)
    # row_map stays internal; the public coordinate buffer is logical order.
    assert not np.shares_memory(parent_x, child_x)
    assert list(taken.coords.x) == [4.0, 0.0, 2.0]
    assert taken.coords.x.tolist() == [4.0, 0.0, 2.0]
    assert child_x.tolist() == [4.0, 0.0, 2.0]
    assert taken.coords.x.shape == (3,)
