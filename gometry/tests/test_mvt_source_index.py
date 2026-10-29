"""Reusable source selection accounts for wrapped and buffered features."""

import gometry as gm
import pytest


def test_source_index_finds_far_periodic_image() -> None:
    features = gm.Features([gm.Point(1e300, 0, crs=3857)], ids=[17])
    tile = gm.Tile(x=0, y=0, zoom=0)
    source = gm.MVTSource(features, extent=4096, buffer=0)
    indexed = gm.mvt_prepare(source, tile=tile)
    direct = gm.mvt_prepare(features, tile=tile, extent=4096, buffer=0)
    assert indexed.features.ids == direct.features.ids == (17,)
    assert indexed.features.geometries.to_wkt() == direct.features.geometries.to_wkt()


def test_source_index_includes_work_halo_before_grid_rounding() -> None:
    features = gm.Features([gm.Point(16.4, 8)], ids=[3])
    frame = gm.TileFrame((0, 0, 16, 16), crs=None)
    source = gm.MVTSource(features, crs=None, extent=16, buffer=0)
    indexed = gm.mvt_prepare(source, frame=frame)
    direct = gm.mvt_prepare(features, frame=frame, extent=16, buffer=0)
    assert indexed.features.ids == direct.features.ids == (3,)
    assert indexed.features.geometries.to_wkt() == direct.features.geometries.to_wkt()


def test_source_rejects_collection_even_when_outside_tile() -> None:
    geometry = gm.from_wkt('GEOMETRYCOLLECTION (POINT (100 100))')
    with pytest.raises(gm.GeometryError):
        gm.MVTSource(gm.Features([geometry]), crs=None)
