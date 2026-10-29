"""Exact XYZ world-image and grid precision witnesses."""

import gometry as gm


def test_very_large_projected_x_uses_exact_periodic_image() -> None:
    features = gm.Features([gm.Point(1e300, 0, crs=3857)], ids=[7])
    tile = gm.Tile(x=0, y=0, zoom=0)
    layer = gm.mvt_prepare(features, tile=tile, extent=4096, buffer=0)
    assert layer.features.ids == (7,)
    point = layer.features.geometries[0]
    assert point.x == 2688
    assert point.y == 2048
