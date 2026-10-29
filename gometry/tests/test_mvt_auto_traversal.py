"""Automatic traversal prunes by source bounds, not coarse-tile output."""

import gometry as gm


def test_fine_tile_survives_when_ancestor_collapses() -> None:
    line = gm.LineString([(1_000, 1_000), (1_100, 1_000)], crs=3857)
    source = gm.MVTSource(gm.Features([line], ids=[5]), extent=16, buffer=0)
    assert not gm.mvt_prepare(source, tile=gm.Tile(x=0, y=0, zoom=0)).features
    tiles = list(gm.mvt_tiles({'roads': source}, min_zoom=20, max_zoom=20))
    assert tiles
    assert all(
        gm.from_mvt(payload, layer='roads').features.ids == (5,) for _, payload in tiles
    )
    assert all(
        0 <= tile.x < 1 << tile.zoom and 0 <= tile.y < 1 << tile.zoom
        for tile, _ in tiles
    )
