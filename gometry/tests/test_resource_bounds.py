"""Deterministic resource-bound regressions for amplifying operations."""

import gometry as gm
import pytest


def test_geometry_amplification_returns_errors_before_allocation() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
    with pytest.raises(gm.GeometryError, match='max_segment_length'):
        line.segmentize(float.fromhex('0x0.0000000000001p-1022'))
    with pytest.raises(gm.GeometryError, match='count'):
        line.sample_points(16_000_001, seed=0)
    with pytest.raises(gm.GeometryError, match='count'):
        line.line_interpolate(count=16_000_001)


def test_h3_fanout_and_traversal_share_the_cell_limit() -> None:
    cell = gm.H3Cell(0.0, 0.0, resolution=0)
    with pytest.raises(gm.GeometryError, match='limit'):
        cell.children(15)
    with pytest.raises(gm.GeometryError, match='limit'):
        cell.grid_disk(100_000)


def test_constrained_refinement_rejects_unbounded_or_nonterminating_requests() -> None:
    polygon = gm.box(0.0, 0.0, 10.0, 10.0)
    with pytest.raises(gm.GeometryError, match='30 degrees'):
        polygon.triangulate(method='constrained', min_angle=31.0)
    with pytest.raises(gm.GeometryError, match='max_area'):
        polygon.triangulate(method='constrained', max_area=1.0e-20)
