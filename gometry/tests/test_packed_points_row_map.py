"""Packed Points row_map correctness — permuted take/filter oracle parity."""

from __future__ import annotations

import gometry as gm
import numpy as np
from conftest import canon


def _permuted_take() -> tuple[gm.GeometryArray, gm.GeometryArray]:
    """Packed points with row_map from take([2, 0]) and materialized oracle."""
    points = gm.points(
        [0.0, 1.0, 2.0, 3.0],
        [10.0, 11.0, 12.0, 13.0],
        crs='EPSG:4326',
        epoch=1.0,
    )
    permuted = points[[2, 0]]
    oracle = gm.GeometryArray([points[2], points[0]])
    return permuted, oracle


def test_packed_points_non_contiguous_take_matches_materialized() -> None:
    permuted, oracle = _permuted_take()
    assert canon(permuted) == canon(oracle)
    assert permuted.coords.x.tolist() == oracle.coords.x.tolist()
    assert permuted.coords.y.tolist() == oracle.coords.y.tolist()
    assert permuted.crs == oracle.crs
    assert permuted.epoch == oracle.epoch


def test_packed_points_non_contiguous_filter_matches_materialized() -> None:
    points = gm.points(
        [0.0, 1.0, 2.0, 3.0],
        [10.0, 11.0, 12.0, 13.0],
        crs='EPSG:4326',
        epoch=1.0,
    )
    mask = np.array([False, True, True, False])
    filtered = points[mask]
    oracle = gm.GeometryArray([points[1], points[2]])
    assert canon(filtered) == canon(oracle)
    assert filtered.coords.x.tolist() == oracle.coords.x.tolist()
    assert filtered.coords.y.tolist() == oracle.coords.y.tolist()
    assert filtered.crs == oracle.crs
    assert filtered.epoch == oracle.epoch
