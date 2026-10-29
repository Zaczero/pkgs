"""Static return types for local MVT decoding."""

from collections.abc import Generator, Sequence
from typing import assert_type

import gometry as gm


def local_results(payload: bytes) -> None:
    assert_type(gm.from_mvt(payload, layer='roads'), gm.MVTLayer)
    assert_type(gm.from_mvt(payload), dict[str, gm.MVTLayer])
    assert_type(gm.from_mvt(payload, layer='roads').features.ids, Sequence[int | None])
    tile = gm.Tile(x=0, y=0, zoom=0)
    assert_type(
        gm.from_mvt(payload, layer='roads', tile=tile),
        gm.Features[gm.Geometry, str | bool | int | float | None, int],
    )


def prepared_source(features: gm.Features) -> None:
    source = gm.MVTSource(features, extent=4096, tolerance=0)
    tile = gm.Tile(x=0, y=0, zoom=0)
    frame = gm.TileFrame((0, 0, 1, 1), crs=None)
    assert_type(gm.mvt_prepare(source, tile=tile), gm.MVTLayer)
    assert_type(gm.mvt_prepare(features, frame=frame, method='dp'), gm.MVTLayer)
    assert_type(gm.to_mvt(source, layer='places', tile=tile), bytes)
    assert_type(gm.mvt_query_regions(frame=frame), tuple[gm.TileFrame, ...])
    assert_type(
        gm.mvt_tiles({'places': source}, tiles=[tile]),
        Generator[tuple[gm.Tile, bytes], None, None],
    )
