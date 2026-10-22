"""CSR construction preserves the flat payload's type and value validity."""

from __future__ import annotations

import pickle

import gometry as gm
import numpy as np
import pytest


def test_integer_groups_preserve_large_ids_and_snapshot_input() -> None:
    source = np.array([2**53 + 1, 2**63 - 1, -7], dtype=np.int64)
    groups = gm.Groups(source, [0, 2, 3])
    source[:] = 0
    assert groups.values.dtype == np.int64
    assert groups.to_list() == [[2**53 + 1, 2**63 - 1], [-7]]
    assert groups.to_pairs()[1].tolist() == [2**53 + 1, 2**63 - 1, -7]
    assert pickle.loads(pickle.dumps(groups[::-1])) == groups[::-1]


@pytest.mark.parametrize('dtype', [np.int64, np.float64])
def test_empty_groups_keep_explicit_numeric_kind(dtype) -> None:
    groups = gm.Groups(np.array([], dtype=dtype), [0, 0])
    assert groups.values.dtype == dtype
    assert len(groups[0]) == 0


def test_numeric_groups_copy_non_native_strided_values() -> None:
    source = np.array([2**53 + 1, 2**53 + 3, 2**53 + 5], dtype='>i8')[::-1]
    groups = gm.Groups(source, [0, 1, 3])
    assert groups.to_list() == [[2**53 + 5], [2**53 + 3, 2**53 + 1]]


@pytest.mark.parametrize('dtype', [np.int64, np.float64], ids=['int64', 'float64'])
@pytest.mark.parametrize(
    'layout',
    ['misaligned_contiguous', 'odd_stride', 'reversed_odd_stride'],
    ids=['offset_one', 'stride_nine', 'reversed_stride_nine'],
)
def test_numeric_groups_admit_unaligned_numpy_storage(dtype, layout: str) -> None:
    expected = (
        np.array([2**53 + 1, -(2**53 + 3), 2**63 - 1], dtype=np.int64)
        if dtype is np.int64
        else np.array([1.25, -3.5, 9.75], dtype=np.float64)
    )
    stride = 9 if layout != 'misaligned_contiguous' else 8
    offset = 1 + (2 * stride if layout == 'reversed_odd_stride' else 0)
    backing = bytearray(1 + 2 * 9 + 8)
    source = np.ndarray(
        shape=(3,),
        dtype=dtype,
        buffer=backing,
        offset=offset,
        strides=(-stride if layout == 'reversed_odd_stride' else stride,),
    )
    source[:] = expected

    groups = gm.Groups(source, [0, 3])
    source[:] = 0
    assert groups.to_list() == [expected.tolist()]
    assert groups.values.dtype == dtype


def test_geometry_groups_keep_inner_missing_values_after_gather_and_pickle() -> None:
    values = gm.GeometryArray([gm.Point(3, 4), None, gm.LineString([(5, 6), (7, 8)])])
    groups = gm.Groups(values, [0, 2, 2, 3], missing=[False, True, False])[::-1]
    assert groups[0].to_wkt() == ['LINESTRING (5 6, 7 8)']
    assert groups[1] is None
    assert groups[2].to_wkt() == ['POINT (3 4)', None]
    assert pickle.loads(pickle.dumps(groups)) == groups


def test_cell_groups_keep_inner_missing_values_after_gather_and_pickle() -> None:
    cells = gm.h3_cells(
        gm.GeometryArray([gm.Point(1, 2, crs=4326), None, gm.Point(3, 4, crs=4326)]),
        resolution=3,
    )
    groups = gm.Groups(cells, [0, 2, 3])[::-1]
    assert groups[0].to_list() == cells[2:].to_list()
    assert groups[1].to_list() == cells[:2].to_list()
    assert pickle.loads(pickle.dumps(groups)) == groups


def test_empty_geometry_and_cell_values_keep_their_array_kinds() -> None:
    geometry = gm.GeometryArray([])
    cells = gm.h3_cells(gm.GeometryArray([], crs=4326), resolution=3)
    assert isinstance(gm.Groups(geometry, [0]).values, gm.GeometryArray)
    assert isinstance(gm.Groups(cells, [0]).values, gm.CellArray)


@pytest.mark.parametrize('offsets', [[1, 2], [0, 3], [0, 2, 1, 2]])
def test_groups_reject_invalid_csr_boundaries(offsets: list[int]) -> None:
    with pytest.raises(gm.GeometryError, match='CSR offsets'):
        gm.Groups(np.array([7, 8], dtype=np.int64), offsets)


def test_missing_groups_must_have_empty_windows() -> None:
    with pytest.raises(ValueError, match='empty values windows'):
        gm.Groups(np.array([7], dtype=np.int64), [0, 1], missing=[True])


@pytest.mark.parametrize(
    'values', [np.array([1], dtype=np.float32), np.array([[1, 2]])]
)
def test_groups_require_the_declared_one_dimensional_numeric_schema(values) -> None:
    with pytest.raises(TypeError, match='one-dimensional'):
        gm.Groups(values, [0, 1])
