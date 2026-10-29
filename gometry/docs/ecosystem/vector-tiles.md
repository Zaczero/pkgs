---
description: Prepare, encode and decode Mapbox Vector Tiles with immutable features.
---

# Mapbox Vector Tiles

Mapbox Vector Tiles (MVT) contain tile-local geometry and scalar attributes for
rendering. Gometry prepares feature snapshots for a tile, encodes MVT bytes,
and decodes selected layers and properties. The caller owns tile storage and
transport.

## Prepare and encode

```python exec="on" source="block" result="text"
import gometry as gm

features = gm.Features(
    [gm.Point(-73.9857, 40.7484, crs=4326)],
    properties=[{'name': 'Empire State Building'}],
    ids=[1],
)
tile = gm.Tile(x=1206, y=1539, zoom=12)
payload = gm.to_mvt(features, layer='places', tile=tile)
```

XYZ tiles use Web Mercator. Gometry transforms CRS-aware input, clips it to a
buffered work region, and constructs an integer grid. `extent` controls grid
resolution and `buffer` is measured in grid units; their defaults are 4096 and
64. The default `tolerance=0` preserves grid detail. Set `tolerance` and
`method='vw'` or `'dp'` to simplify in tile-grid units.

MVT stores XY only. Set `drop_zm=True` to permit discarding Z or M. Components
that collapse on the grid are omitted by default; set `on_collapse='raise'`
to detect them.

For a different projection or tile scheme, supply a `TileFrame` with bounds
and an explicit CRS. Its upper-left corner maps to `(0, 0)`.

```python exec="on" source="block" result="text"
import gometry as gm

frame = gm.TileFrame((0, 0, 100, 100), crs=None)
places = gm.Features([gm.Point(25, 75)], ids=[0])
layer = gm.mvt_prepare(places, frame=frame, extent=4096)
payload = gm.to_mvt({'places': layer})
```

`MVTLayer` holds already prepared tile-local geometry. Its `features` and
`extent` can be inspected or encoded again without another projection.

## Reuse a source or consume batches

For repeated tiles, construct an `MVTSource` once. It owns its projection,
spatial index, and tile preparation settings; `with_options` shares the source
preparation with a different tile policy.

```python exec="on" source="block" result="text"
import gometry as gm

features = gm.Features(
    [gm.Point(-73.9857, 40.7484, crs=4326)],
    properties=[{'name': 'Empire State Building'}],
    ids=[1],
)
tile = gm.Tile(x=1206, y=1539, zoom=12)
source = gm.MVTSource(features, extent=4096, buffer=64)
another_policy = source.with_options(tolerance=1, method='vw')

tiles = gm.mvt_tiles({'places': another_policy}, tiles=[tile])
address, payload = next(tiles)
tiles.close()
```

The explicit-address iterator preserves order and repeated addresses, including
empty tiles. Use `min_zoom` and `max_zoom` instead of `tiles` to traverse
nonempty tiles in `(zoom, x, y)` order. Close an iterator when ending early.
For a valid polygonal coverage, `coverage=True` makes shared boundaries part
of source preparation.

For externally selected data, `mvt_prepare` and `to_mvt` also accept an
iterable of `Features` batches. The iterable is consumed once; the caller
continues to own its reader. `mvt_query_regions` returns conservative source
rectangles, including the work halo, for external spatial selection.

## Decode

```python exec="on" source="block" result="text"
import gometry as gm

tile = gm.Tile(x=1, y=1, zoom=1)
features = gm.Features([gm.Point(0, 0, crs=4326)], ids=[1])
payload = gm.to_mvt(features, layer='places', tile=tile)
local = gm.from_mvt(payload, layer='places')
print(local.extent)
print(local.features.geometries.to_wkt())

located = gm.from_mvt(payload, layer='places', tile=tile, crs=4326)
```

Without a tile or frame, decoding returns `MVTLayer` objects with local
integer coordinates. With one, it returns located `Features`. Use
`layers=[...]` to select several layer names, `layer='name'` for one layer,
and `properties=[...]` to select fields. MVT bytes carry neither their tile
address nor their CRS, so retain that context separately.

Feature IDs are unsigned integers; a missing ID remains distinct from ID zero.
MVT properties are strings, booleans, integers, or floating-point values.
Null properties are omitted when encoded. These functions exchange
uncompressed tile bytes.

## Write a PMTiles archive

Install the external archive package with `uv add pmtiles`. Using the `source`
prepared above, stream addressed tiles into its writer:

```python title="partial: archive an existing MVTSource with pmtiles"
import gzip

from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import write

with write("places.pmtiles") as archive:
    for tile, payload in gm.mvt_tiles(
        {"places": source}, min_zoom=2, max_zoom=12
    ):
        archive.write_tile(
            zxy_to_tileid(tile.zoom, tile.x, tile.y),
            gzip.compress(payload, mtime=0),
        )
    archive.finalize(
        {"tile_type": TileType.MVT, "tile_compression": Compression.GZIP},
        {"vector_layers": [{"id": "places", "fields": {"name": "String"}}]},
    )
```

Convert XYZ addresses with `zxy_to_tileid`; gometry's packed `Tile.id` uses a
different encoding. The archive writer owns file storage and directory memory.
