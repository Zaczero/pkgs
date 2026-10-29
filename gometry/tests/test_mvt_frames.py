"""Custom frames preserve literal projected edges while XYZ splits world seams."""

import math

import gometry as gm


def test_geographic_source_distinguishes_custom_frame_from_xyz() -> None:
    world = math.pi * 6_378_137
    line = gm.LineString([(170, 10), (-170, 10)], crs=4326)
    features = gm.Features([line], ids=[7])
    frame = gm.TileFrame((-world, -world, world, world), crs=3857)
    tile = gm.Tile(x=0, y=0, zoom=0)
    projected = gm.Features([line.to_crs(3857)], ids=[7])
    expected_custom = gm.mvt_prepare(projected, frame=frame, extent=4096, buffer=0)
    expected_xyz = gm.mvt_prepare(features, tile=tile, extent=4096, buffer=0)
    direct_custom = gm.mvt_prepare(features, frame=frame, extent=4096, buffer=0)
    source = gm.MVTSource(features, extent=4096, buffer=0)
    source_custom = gm.mvt_prepare(source, frame=frame)
    source_xyz = gm.mvt_prepare(source, tile=tile)
    assert expected_custom.features.geometries.to_wkt() == [
        'LINESTRING (3982 1934, 114 1934)'
    ]
    assert (
        direct_custom.features.geometries.to_wkt()
        == expected_custom.features.geometries.to_wkt()
    )
    assert (
        source_custom.features.geometries.to_wkt()
        == expected_custom.features.geometries.to_wkt()
    )
    assert (
        source_xyz.features.geometries.to_wkt()
        == expected_xyz.features.geometries.to_wkt()
    )
    assert isinstance(expected_xyz.features.geometries[0], gm.MultiLineString)


def test_geographic_batch_source_shares_both_frame_modes() -> None:
    world = math.pi * 6_378_137
    batches = [
        gm.Features([gm.Point(0, 0, crs=4326)], ids=[0]),
        gm.Features([gm.LineString([(170, 10), (-170, 10)], crs=4326)], ids=[7]),
    ]
    source = gm.MVTSource(iter(batches), extent=4096, buffer=0)
    frame = gm.TileFrame((-world, -world, world, world), crs=3857)
    tile = gm.Tile(x=0, y=0, zoom=0)
    custom = gm.mvt_prepare(source.with_options(tolerance=0), frame=frame)
    xyz = gm.mvt_prepare(source, tile=tile)
    assert custom.features.ids == xyz.features.ids == (0, 7)
    assert isinstance(custom.features.geometries[1], gm.LineString)
    assert isinstance(xyz.features.geometries[1], gm.MultiLineString)
