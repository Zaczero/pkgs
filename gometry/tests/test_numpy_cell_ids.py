"""Numeric NumPy ID arrays preserve sequence behavior across array layouts."""

from __future__ import annotations

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
