"""Deterministic resource-bound regressions for amplifying operations."""

from __future__ import annotations

import gometry as gm
import pytest


def test_geometry_amplification_returns_errors_before_allocation() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
    with pytest.raises(gm.GeometryError, match='max_length'):
        line.segmentize(float.fromhex('0x0.0000000000001p-1022'))
    with pytest.raises(gm.GeometryError, match='count'):
        line.sample_points(16_000_001, seed=0)
    with pytest.raises(gm.GeometryError, match='count'):
        line.line_interpolate(count=16_000_001)


def test_h3_fanout_and_traversal_have_explicit_output_budgets() -> None:
    cell = gm.H3Cell(0.0, 0.0, resolution=0)
    assert len(cell.children(2)) == cell.children_count(2)
    with pytest.raises(gm.GeometryError, match='max_cells'):
        cell.grid_disk(1_000, max_cells=100)


def test_cell_array_neighbors_cross_the_old_ragged_collector_limit() -> None:
    """Ragged cell output is governed by allocation, not a hidden cell cap."""
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    rows = 167_000
    cells = gm.CellArray([cell] * rows)

    neighbors = cells.neighbors

    assert len(neighbors) == rows
    assert len(neighbors.values) > 1_000_000


def test_constrained_refinement_rejects_unbounded_or_nonterminating_requests() -> None:
    polygon = gm.box(0.0, 0.0, 10.0, 10.0)
    with pytest.raises(gm.GeometryError, match='30 degrees'):
        polygon.triangulate(method='constrained', min_angle=31.0)
    with pytest.raises(gm.GeometryError, match='max_area'):
        polygon.triangulate(method='constrained', max_area=1.0e-20)
