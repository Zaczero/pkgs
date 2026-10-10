"""Coordinate-predicate oracles across the cell-preclassification gate."""

from __future__ import annotations

import math

import gometry as gm
import numpy as np
import pytest


def _regular_polygon(cx: float = 0.0, cy: float = 0.0, radius: float = 10.0):
    vertices = [
        (
            cx + radius * math.cos(2.0 * math.pi * i / 64.0),
            cy + radius * math.sin(2.0 * math.pi * i / 64.0),
        )
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
        ('plain', plain, (0.0, 0.0)),
        ('holed', holed, (5.0, 0.0)),
        ('multipolygon', multi, (-20.0, 0.0)),
    )


@pytest.mark.parametrize('predicate', ['contains', 'intersects'])
@pytest.mark.parametrize('name, geometry, probe', _shapes_and_probes())
def test_prepared_coordinate_interior_matches_scalar_oracle(
    predicate, name, geometry, probe
):
    """A certified interior cell must preserve the exact scalar verdict."""
    del name
    x, y = probe
    operation = getattr(gm, predicate)
    scalar = bool(operation(geometry, gm.Point(x, y)))
    assert scalar is True

    for count in (9_999, 10_000):
        xs = np.full(count, x)
        ys = np.full(count, y)
        result = operation(geometry, x=xs, y=ys)
        np.testing.assert_array_equal(result, np.full(count, True))


def test_tall_edge_batch_masks_match_scalar_probes() -> None:
    # 34 edges enter the indexed Y-stabbing plan's interval-tree fallback.
    vertices = [(i * 0.1, 0.0 if i % 2 == 0 else 1000.0) for i in range(34)]
    polygon = gm.Polygon([*vertices, vertices[0]])
    xs, ys = np.meshgrid(np.linspace(0, 3.3, 4), np.linspace(0, 1000, 4))
    batch = gm.contains(polygon, x=xs.ravel(), y=ys.ravel())
    scalar = [
        gm.contains(polygon, gm.Point(float(x), float(y)))
        for x, y in zip(xs.ravel(), ys.ravel(), strict=True)
    ]
    np.testing.assert_array_equal(batch, scalar)
