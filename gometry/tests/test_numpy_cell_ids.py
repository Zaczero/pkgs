"""Numeric NumPy ID arrays preserve sequence behavior across array layouts."""

from __future__ import annotations

import gc

import gometry as gm
import numpy as np
import pytest

ID_LAYOUTS = (
    pytest.param('contiguous', id='c-contiguous'),
    pytest.param('strided', id='strided'),
    pytest.param('negative-stride', id='negative-stride'),
    pytest.param('scalar', id='zero-dimensional'),
    pytest.param('empty', id='empty'),
)


def _ids_with_layout(values: list[int], layout: str) -> np.ndarray:
    contiguous = np.asarray(values, dtype=np.uint64)
    if layout == 'contiguous':
        return contiguous
    if layout == 'strided':
        result = np.repeat(contiguous, 2)[::2]
        assert result.strides[0] > result.itemsize
        return result
    if layout == 'negative-stride':
        result = contiguous[::-1]
        assert result.strides[0] < 0
        return result
    if layout == 'scalar':
        return np.asarray(values[0], dtype=np.uint64)
    if layout == 'empty':
        return np.empty(0, dtype=np.uint64)
    raise AssertionError(f'unrecognized ID layout: {layout}')


def _list_ids(array: np.ndarray) -> list[int]:
    if array.ndim == 0:
        return [int(array.item())]
    return array.tolist()


def _unaligned_ids(values: list[int], layout: str) -> np.ndarray:
    byte_stride = 8 if layout == 'misaligned_contiguous' else 9
    offset = (
        1 + (len(values) - 1) * byte_stride if layout == 'reversed_odd_stride' else 1
    )
    stride = -byte_stride if layout == 'reversed_odd_stride' else byte_stride
    backing = bytearray(1 + max(len(values) - 1, 0) * byte_stride + 8)
    result = np.ndarray(
        (len(values),),
        dtype=np.uint64,
        buffer=backing,
        offset=offset,
        strides=(stride,),
    )
    result[:] = values
    return result


@pytest.mark.parametrize('layout', ID_LAYOUTS)
def test_cell_array_numeric_numpy_ids_match_list_input(layout: str) -> None:
    cells = [
        gm.H3Cell(13.4, 52.5, resolution=7),
        gm.H3Cell(14.4, 52.5, resolution=7),
        gm.H3Cell(15.4, 52.5, resolution=7),
    ]
    source_ids = [cell.id for cell in cells]
    assert len(set(source_ids)) == len(source_ids)
    ids = _ids_with_layout(source_ids, layout)

    expected = gm.CellArray(_list_ids(ids), type=gm.H3Cell)
    actual = gm.CellArray(ids, type=gm.H3Cell)

    assert list(actual) == list(expected)


@pytest.mark.parametrize(
    ('array_type', 'index_kind'),
    [
        pytest.param(gm.H3VertexArray, 'vertices', id='vertices'),
        pytest.param(gm.H3EdgeArray, 'edges', id='edges'),
    ],
)
@pytest.mark.parametrize('layout', ID_LAYOUTS)
def test_h3_index_numeric_numpy_ids_match_list_input(
    array_type: type, index_kind: str, layout: str
) -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    indexes = list(cell.vertices if index_kind == 'vertices' else cell.edges)
    source_ids = [index.id for index in indexes]
    ids = _ids_with_layout(source_ids, layout)

    expected = array_type(_list_ids(ids))
    actual = array_type(ids)

    assert list(actual) == list(expected)


@pytest.mark.parametrize(
    'layout',
    ['misaligned_contiguous', 'odd_stride', 'reversed_odd_stride'],
    ids=['offset_one', 'stride_nine', 'reversed_stride_nine'],
)
def test_cell_array_admits_unaligned_numeric_ids_and_snapshots(layout: str) -> None:
    cells = [
        gm.H3Cell(13.4, 52.5, resolution=7),
        gm.H3Cell(14.4, 52.5, resolution=7),
        gm.H3Cell(15.4, 52.5, resolution=7),
    ]
    ids = [cell.id for cell in cells]
    source = _unaligned_ids(ids, layout)
    actual = gm.CellArray(source, type=gm.H3Cell)
    source[:] = 0
    assert [cell.id for cell in actual] == ids


@pytest.mark.parametrize(
    ('array_type', 'index_kind'),
    [
        pytest.param(gm.H3VertexArray, 'vertices', id='vertices'),
        pytest.param(gm.H3EdgeArray, 'edges', id='edges'),
    ],
)
@pytest.mark.parametrize(
    'layout',
    ['misaligned_contiguous', 'odd_stride', 'reversed_odd_stride'],
    ids=['offset_one', 'stride_nine', 'reversed_stride_nine'],
)
def test_h3_index_arrays_admit_unaligned_numeric_ids_and_snapshot(
    array_type: type, index_kind: str, layout: str
) -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    indexes = list(cell.vertices if index_kind == 'vertices' else cell.edges)
    ids = [index.id for index in indexes]
    source = _unaligned_ids(ids, layout)
    actual = array_type(source)
    source[:] = 0
    assert [index.id for index in actual] == ids


@pytest.mark.parametrize(
    'constructor',
    [
        pytest.param(
            lambda values: gm.CellArray(values, type=gm.H3Cell), id='cell-array'
        ),
        pytest.param(gm.H3VertexArray, id='h3-vertices'),
        pytest.param(gm.H3EdgeArray, id='h3-edges'),
    ],
)
def test_empty_unaligned_cell_id_arrays_do_not_create_typed_views(constructor) -> None:
    source = np.ndarray((0,), dtype=np.uint64, buffer=bytearray(9), offset=1)
    assert len(constructor(source)) == 0


def test_cell_array_keeps_non_native_integer_ids_exact() -> None:
    ids = [
        gm.H3Cell(13.4, 52.5, resolution=7).id,
        gm.H3Cell(14.4, 52.5, resolution=7).id,
    ]
    source = np.asarray(ids, dtype='>u8')
    actual = gm.CellArray(source, type=gm.H3Cell)
    source[:] = 0
    assert [cell.id for cell in actual] == ids


@pytest.mark.parametrize(
    ('array_type', 'index_kind'),
    [
        pytest.param(gm.H3VertexArray, 'vertices', id='vertices'),
        pytest.param(gm.H3EdgeArray, 'edges', id='edges'),
    ],
)
def test_h3_index_arrays_keep_non_native_integer_ids_exact(
    array_type: type, index_kind: str
) -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    indexes = list(cell.vertices if index_kind == 'vertices' else cell.edges)
    ids = [index.id for index in indexes]
    source = np.asarray(ids, dtype='>u8')
    actual = array_type(source)
    source[:] = 0
    assert [index.id for index in actual] == ids


@pytest.mark.parametrize(
    ('array_type', 'array_entity'),
    [
        pytest.param(gm.H3VertexArray, 'H3 vertex', id='vertices'),
        pytest.param(gm.H3EdgeArray, 'H3 edge', id='edges'),
    ],
)
def test_h3_index_numeric_numpy_ids_reject_two_dimensions(
    array_type: type, array_entity: str
) -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    indexes = list(cell.vertices if array_entity == 'H3 vertex' else cell.edges)
    ids = np.asarray([index.id for index in indexes], dtype=np.uint64).reshape(2, 3)

    with pytest.raises(TypeError) as error:
        array_type(ids)

    assert str(error.value) == f'{array_entity} ids must be zero- or one-dimensional'


def test_cell_array_numpy_negative_id_keeps_unsigned_conversion_error() -> None:
    with pytest.raises(TypeError) as error:
        gm.CellArray(np.array([-1], dtype=np.int64), type=gm.H3Cell)

    assert str(error.value) == (
        'cell array ids must be integers representable as uint64'
    )


@pytest.mark.parametrize(
    ('array_type', 'array_entity'),
    [
        pytest.param(gm.H3VertexArray, 'H3 vertex', id='vertices'),
        pytest.param(gm.H3EdgeArray, 'H3 edge', id='edges'),
    ],
)
def test_h3_index_numpy_negative_id_keeps_unsigned_conversion_error(
    array_type: type, array_entity: str
) -> None:
    with pytest.raises(TypeError) as error:
        array_type(np.array([-1], dtype=np.int64))

    assert str(error.value) == f'{array_entity} ids must be unsigned 64-bit integers'


def test_cell_array_arrow_exports_uint64_ids_and_geohash_tokens() -> None:
    pa = pytest.importorskip('pyarrow')
    h3 = gm.H3Cell(13.4, 52.5, resolution=7)
    neighbor = gm.H3Cell(14.4, 52.5, resolution=7)

    ids = gm.CellArray([h3, None, neighbor])
    arrow_ids = pa.array(ids)
    assert arrow_ids.type == pa.uint64()
    assert arrow_ids.to_pylist() == [h3.id, None, neighbor.id]
    assert pa.array(ids, type=pa.uint64()).to_pylist() == [h3.id, None, neighbor.id]

    hashes = gm.CellArray([gm.GeohashCell('u33d'), None, gm.GeohashCell('u33dc')])
    arrow_hashes = pa.array(hashes)
    assert arrow_hashes.type == pa.string()
    assert arrow_hashes.to_pylist() == ['u33d', None, 'u33dc']
    assert pa.array(hashes, type=pa.string()).to_pylist() == ['u33d', None, 'u33dc']


def test_cell_id_arrow_views_share_contiguous_storage_and_retain_their_owner() -> None:
    pa = pytest.importorskip('pyarrow')
    cells = [
        gm.H3Cell(13.4, 52.5, resolution=7),
        gm.H3Cell(14.4, 52.5, resolution=7),
        gm.H3Cell(15.4, 52.5, resolution=7),
    ]
    source = gm.CellArray(cells)
    source_arrow = pa.array(source)
    source_ids = source.to_numpy()
    assert source_arrow.buffers()[1].address == source_ids.ctypes.data

    window = source[1:]
    window_arrow = pa.array(window)
    assert window_arrow.offset == 1
    assert window_arrow.buffers()[1].address == source_arrow.buffers()[1].address
    assert window_arrow.to_pylist() == [cells[1].id, cells[2].id]

    gathered = source[np.array([2, 0], dtype=np.intp)]
    gathered_arrow = pa.array(gathered)
    assert gathered_arrow.offset == 0
    assert gathered_arrow.buffers()[1].address != source_arrow.buffers()[1].address
    assert gathered_arrow.to_pylist() == [cells[2].id, cells[0].id]

    del source_ids
    del source, window, gathered
    gc.collect()
    assert source_arrow.to_pylist() == [cell.id for cell in cells]
    assert window_arrow.to_pylist() == [cells[1].id, cells[2].id]
    assert gathered_arrow.to_pylist() == [cells[2].id, cells[0].id]


@pytest.mark.parametrize(
    ('array_type', 'index_kind'),
    [
        pytest.param(gm.H3VertexArray, 'vertices', id='vertices'),
        pytest.param(gm.H3EdgeArray, 'edges', id='edges'),
    ],
)
def test_h3_index_arrays_export_uint64_ids_through_arrow(
    array_type: type, index_kind: str
) -> None:
    pa = pytest.importorskip('pyarrow')
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    indexes = list(cell.vertices if index_kind == 'vertices' else cell.edges)
    values = array_type(indexes)

    arrow = pa.array(values)

    assert arrow.type == pa.uint64()
    assert arrow.to_pylist() == [index.id for index in indexes]
    assert pa.array(values, type=pa.uint64()).to_pylist() == [
        index.id for index in indexes
    ]


@pytest.mark.parametrize(
    ('array_type', 'index_kind'),
    [
        pytest.param(gm.H3VertexArray, 'vertices', id='vertices'),
        pytest.param(gm.H3EdgeArray, 'edges', id='edges'),
    ],
)
def test_h3_index_arrow_windows_share_and_gathers_own_their_ids(
    array_type: type, index_kind: str
) -> None:
    pa = pytest.importorskip('pyarrow')
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    indexes = list(cell.vertices if index_kind == 'vertices' else cell.edges)
    source = array_type(indexes)
    source_arrow = pa.array(source)

    window = source[1:]
    window_arrow = pa.array(window)
    assert window_arrow.offset == 1
    assert window_arrow.buffers()[1].address == source_arrow.buffers()[1].address

    gathered = source[np.asarray([2, 0], dtype=np.intp)]
    gathered_arrow = pa.array(gathered)
    assert gathered_arrow.offset == 0
    assert gathered_arrow.buffers()[1].address != source_arrow.buffers()[1].address
    assert gathered_arrow.to_pylist() == [indexes[2].id, indexes[0].id]

    del source, window, gathered
    gc.collect()
    assert source_arrow.to_pylist() == [index.id for index in indexes]
    assert window_arrow.to_pylist() == [index.id for index in indexes[1:]]
    assert gathered_arrow.to_pylist() == [indexes[2].id, indexes[0].id]


def test_cell_array_arrow_requested_schema_accepts_only_its_uint64_layout() -> None:
    pa = pytest.importorskip('pyarrow')
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    values = gm.CellArray([cell])
    same_schema, _ = values.__arrow_c_array__()

    assert pa.array(values, type=pa.uint64()).to_pylist() == [cell.id]
    assert len(values.__arrow_c_array__(same_schema)) == 2

    different_schema = pa.array([cell.id], type=pa.int64()).__arrow_c_array__()[0]
    with pytest.raises(TypeError, match='requested_schema does not match'):
        values.__arrow_c_array__(different_schema)
