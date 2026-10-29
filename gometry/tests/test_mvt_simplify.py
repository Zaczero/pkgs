"""Tile-local simplification keeps protected boundaries and removes interior detail."""

import gometry as gm
import pytest


@pytest.mark.parametrize('method', ['vw', 'dp'])
def test_tile_local_simplification_removes_interior_vertices(method: str) -> None:
    frame = gm.TileFrame((0, 0, 16, 16), crs=None)
    features = gm.Features([gm.LineString([(2, 14), (4, 13.9), (6, 14)])])
    untouched = gm.mvt_prepare(features, frame=frame, extent=16, buffer=0)
    simplified = gm.mvt_prepare(
        features, frame=frame, extent=16, buffer=0, tolerance=1, method=method
    )
    assert untouched.features.geometries[0].num_coordinates == 3
    assert simplified.features.geometries[0].num_coordinates == 2
