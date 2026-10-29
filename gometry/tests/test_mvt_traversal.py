"""Simplification retains directed line traversal and retraces."""

import gometry as gm
import pytest


@pytest.mark.parametrize('method', ['vw', 'dp'])
def test_simplification_keeps_nonconsecutive_retraces(method: str) -> None:
    line = gm.LineString([(2, 14), (6, 14), (2, 14), (6, 14)])
    layer = gm.mvt_prepare(
        gm.Features([line]),
        frame=gm.TileFrame((0, 0, 16, 16), crs=None),
        extent=16,
        buffer=0,
        tolerance=100,
        method=method,
    )
    assert layer.features.geometries.to_wkt() == ['LINESTRING (2 2, 6 2, 2 2, 6 2)']
