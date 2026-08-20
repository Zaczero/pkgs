"""Prepared XY predicate oracles across the cell-preclassification gate."""

from __future__ import annotations

import math

import gometry as gm
import numpy as np
import pytest


def _regular_polygon(cx: float = 0.0, cy: float = 0.0, radius: float = 10.0):
    vertices = [
        (cx + radius * math.cos(2.0 * math.pi * i / 64.0),
         cy + radius * math.sin(2.0 * math.pi * i / 64.0))
        for i in range(64)
    ]
    return gm.Polygon([*vertices, vertices[0]])


def _shapes_and_probes():
    plain = _regular_polygon()
    holed = gm.Polygon(
        list(plain.exterior.coords),
        holes=[list(_regular_polygon(radius=2.0).exterior.coords)],
    )
    first = _regular_polygon(cx=-20.0)
    multi = gm.MultiPolygon([first, _regular_polygon(cx=20.0)])
    return (
        ("plain", plain, (0.0, 0.0)),
        ("holed", holed, (5.0, 0.0)),
        ("multipolygon", multi, (-20.0, 0.0)),
    )


@pytest.mark.parametrize("predicate", ["contains_xy", "intersects_xy"])
@pytest.mark.parametrize("name, geometry, probe", _shapes_and_probes())
def test_prepared_xy_interior_matches_scalar_oracle(predicate, name, geometry, probe):
    """A certified interior cell must preserve the exact scalar verdict."""
    del name
    prepared = geometry.prepare()
    x, y = probe
    scalar = bool(getattr(gm, predicate[:-3])(geometry, gm.Point(x, y)))

    for count in (9_999, 10_000):
        xs = np.full(count, x)
        ys = np.full(count, y)
        result = getattr(gm, predicate)(prepared, xs, ys)
        np.testing.assert_array_equal(result, np.full(count, scalar))


@pytest.mark.parametrize("predicate", ["contains_xy", "intersects_xy"])
@pytest.mark.parametrize("name, geometry, probe", _shapes_and_probes())
def test_prepared_xy_gate_is_batching_invariant(predicate, name, geometry, probe):
    """Crossing the gate cannot change results relative to below-gate batches."""
    del name
    prepared = geometry.prepare()
    x, y = probe

    for count, half in ((9_998, 4_999), (10_000, 5_000)):
        xs = np.full(count, x)
        ys = np.full(count, y)
        whole = getattr(gm, predicate)(prepared, xs, ys)
        split = np.concatenate(
            [
                getattr(gm, predicate)(prepared, xs[:half], ys[:half]),
                getattr(gm, predicate)(prepared, xs[half:], ys[half:]),
            ]
        )
        np.testing.assert_array_equal(whole, split)
