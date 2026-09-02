"""Masked NumPy inputs are rejected instead of exposing hidden data values."""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest

MASKED_ERROR = 'masked NumPy arrays.*fill or disable the mask'


def test_masked_coordinate_parameter_and_matrix_inputs_are_rejected() -> None:
    masked_x = np.ma.array([1.0, 999.0], mask=[False, True])
    with pytest.raises(TypeError, match=MASKED_ERROR):
        gm.points(masked_x, [2.0, 888.0])

    masked_distance = np.ma.array([1.0, 999.0], mask=[False, True])
    values = gm.GeometryArray([gm.Point(0.0, 0.0), gm.Point(1.0, 1.0)])
    with pytest.raises(TypeError, match=MASKED_ERROR):
        values.buffer(masked_distance)

    masked_matrix = np.ma.array(
        [1.0, 0.0, 0.0, 1.0, 999.0, 888.0],
        mask=[False, False, False, False, True, True],
    )
    with pytest.raises(TypeError, match=MASKED_ERROR):
        gm.Point(0.0, 0.0).affine_transform(masked_matrix)


def test_masked_boolean_array_index_is_rejected() -> None:
    values = gm.GeometryArray([gm.Point(0.0, 0.0), gm.Point(1.0, 1.0)])
    masked_index = np.ma.array([True, False], mask=[False, True])
    with pytest.raises(TypeError, match=MASKED_ERROR):
        values[masked_index]


def test_masked_numeric_cell_and_h3_index_arrays_are_rejected() -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    other_cell = gm.H3Cell(14.4, 52.5, resolution=7)
    masked_cells = np.ma.array(
        [cell.id, other_cell.id], dtype=np.uint64, mask=[False, True]
    )
    with pytest.raises(TypeError, match=MASKED_ERROR):
        gm.CellArray(masked_cells, type=gm.H3Cell)

    vertex = cell.vertices[0]
    other_vertex = cell.vertices[1]
    masked_vertices = np.ma.array(
        [vertex.id, other_vertex.id], dtype=np.uint64, mask=[False, True]
    )
    with pytest.raises(TypeError, match=MASKED_ERROR):
        gm.H3VertexArray(masked_vertices)

    edge = cell.edges[0]
    other_edge = cell.edges[1]
    masked_edges = np.ma.array(
        [edge.id, other_edge.id], dtype=np.uint64, mask=[False, True]
    )
    with pytest.raises(TypeError, match=MASKED_ERROR):
        gm.H3EdgeArray(masked_edges)


def test_ordinary_ndarrays_and_subclasses_remain_accepted() -> None:
    class OrdinaryArray(np.ndarray):
        @property
        def mask(self) -> object:
            raise AssertionError('ordinary ndarray mask attribute was inspected')

    x = np.array([1.0, 2.0]).view(OrdinaryArray)
    y = np.array([3.0, 4.0]).view(OrdinaryArray)
    assert gm.points(x, y).to_wkt() == ['POINT (1 3)', 'POINT (2 4)']

    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    ids = np.array([cell.id], dtype=np.uint64).view(OrdinaryArray)
    assert list(gm.CellArray(ids, type=gm.H3Cell)) == [cell]


def test_masked_array_subclasses_are_rejected_and_geometry_missing_rows_remain_native() -> None:
    class MaskedSubclass(np.ma.MaskedArray):
        pass

    masked = np.ma.array([1.0, 999.0], mask=[False, True]).view(MaskedSubclass)
    with pytest.raises(TypeError, match=MASKED_ERROR):
        gm.points(masked, [2.0, 888.0])

    values = gm.GeometryArray([gm.Point(0.0, 0.0), None])
    cells = gm.h3_cells(values, resolution=7)
    assert cells.is_missing.tolist() == [False, True]
    assert cells[1] is None
