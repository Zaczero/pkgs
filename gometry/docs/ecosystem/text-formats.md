---
description: gometry text and binary IO — WKB/EWKB for databases, WKT/EWKT, GeoJSON Z/M rules, __geo_interface__, pickle, and a round-trip cheat sheet covering CRS, epoch, Z, and M.
---

# Text & binary formats

gometry moves geometry in and out as WKB, EWKB, WKT, EWKT, GeoJSON, the
`__geo_interface__` protocol, and pickle. Each format carries CRS, Z, and M
differently; this page covers what survives each round-trip. Z is preserved unless
you ask to drop it; M is preserved in formats that can hold it and **rejected** in
formats that can't (rather than silently discarded); SRID is embedded only when you
opt in.

## WKB and EWKB

[`to_wkb`][gometry.Geometry.to_wkb] writes **ISO** [WKB](https://www.ogc.org/standard/sfa/)
by default — the portable, standards-based binary boundary. It does **not** embed
an SRID. Pass `include_srid=True` to write **PostGIS-style EWKB** with the SRID
encoded. EWKB is a PostGIS extension (not an OGC standard); use it with systems
that explicitly speak PostGIS EWKB (PostGIS itself, and some drivers that
document EWKB support). For a portable boundary, prefer ISO WKB plus CRS in side
metadata (or GeoArrow).

```python exec="on" source="block" result="text"
import gometry as gm

p = gm.Point(21.0, 52.0, crs=4326)
iso = p.to_wkb()
ewkb = p.to_wkb(include_srid=True)
print('ISO WKB bytes:', len(iso))
print('EWKB bytes:', len(ewkb))
print('from ISO WKB crs:', gm.from_wkb(iso).crs)
print('from EWKB crs:', gm.from_wkb(ewkb).crs)
```

!!! warning "ISO WKB drops the CRS; EWKB carries an integer SRID"
    `from_wkb(to_wkb(...))` returns geometry with **no CRS** — ISO WKB has nowhere
    to store one. EWKB can embed an integer EPSG SRID when the geometry's CRS has
    an EPSG authority code, or when it is one of the exact **PostGIS wire aliases**
    `OGC:CRS84` → SRID 4326 and `OGC:CRS84h` → SRID 4979
    (`to_wkb(include_srid=True)` rejects other non-EPSG CRS values). These aliases
    are a deliberate PostGIS serialization convention (lon/lat storage matches
    EPSG:4326 on the wire), **not** universal CRS identity: object equality
    `CRS(4326) == CRS("OGC:CRS84")` stays `False`, and a bare EWKB round-trip
    therefore loses the OGC spelling:

    ```python
    from_wkb(Point(10, 50, crs="OGC:CRS84").to_wkb(include_srid=True)).crs
    # EPSG:4326
    ```

    Pass `crs="OGC:CRS84"` at decode to restore the alias when the embedded SRID
    is exactly 4326 (a genuine SRID/CRS conflict still raises). Epoch never rides
    either WKB flavour.

### The database boundary

The **portable** binary boundary is ISO WKB with CRS/epoch in side metadata, or
GeoArrow with CRS plus gometry's epoch extension when the peer is gometry or
extension-aware. **EWKB** is the PostGIS-compatible option when the peer is PostGIS or
another system that documents EWKB SRID support — not a universal "what every
database expects." gometry core ships **no database adapters**. Keep the boundary
binary and columnar — write bytes out, read bytes back, and decode the whole
column in one [`gm.from_wkb`][gometry.from_wkb] call rather than a per-row geometry
loop:

```python exec="on" source="block" result="text"
import sqlite3

import gometry as gm

geoms = gm.points([21.0, 22.0], [52.0, 53.0], crs=4326)
ewkb_rows = geoms.to_wkb(include_srid=True)

db = sqlite3.connect(":memory:")
db.execute("create table places (id integer primary key, geom blob not null)")
db.executemany(
    "insert into places (id, geom) values (?, ?)",
    enumerate(ewkb_rows, start=1),
)

payloads = [row[0] for row in db.execute("select geom from places order by id")]
restored = gm.from_wkb(payloads)
db.close()

print(restored)
print("crs:", restored.crs.to_authority())
print(restored.to_wkt())
```

Use EWKB (`include_srid=True`) when talking to PostGIS-compatible storage that
should embed an EPSG SRID. Use plain ISO WKB when the CRS lives in a side column
or table contract, then attach it at ingress with
`gm.from_wkb(payloads, crs=...)`. For columnar handoff (DuckDB, Arrow IPC),
prefer GeoArrow via [`to_arrow`][gometry.GeometryArray.to_arrow] — including
`encoding='wkb'` when a WKB Arrow column is the peer contract rather than a
packed GeoArrow layout.

```python
import gometry as gm

geoms = gm.points([21.0, 22.0], [52.0, 53.0], crs=4326)
# GeoArrow-native by default; WKB encoding for peers that want binary geometry:
wkb_arrow = geoms.to_arrow(encoding="wkb")  # requires gometry[arrow] / pyarrow
```


## WKT and EWKT

WKT is the human-readable / debug format. gometry reads and writes modern
dimensional tags (`Z`, `M`, `ZM`) and lets you control the output dimension
explicitly. Pass `include_srid=True` to emit **EWKT** with an `SRID=` prefix so the
CRS survives the text round-trip.

```python exec="on" source="block" result="text"
import gometry as gm

pz = gm.Point(21.0, 52.0, z=100.0, crs=4979)
print("default:", pz.to_wkt())                       # POINT Z (21 52 100)
print("force 2D:", pz.to_wkt(output_dimension=2))  # POINT (21 52) — Z dropped explicitly
print("EWKT:", pz.to_wkt(include_srid=True))       # SRID=4979;POINT Z (...)

print("parse:", gm.from_wkt("POINT ZM (1 2 3 4)").coordinate_axes)
```

!!! note "WKT is for humans, not throughput"
    Use WKT for debugging, logs, and copy-paste. For high-volume storage prefer
    WKB/EWKB or GeoArrow — parsing decimal text is far slower than reading packed
    binary.

## GeoJSON

[GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946) (RFC 7946) defines
positions as `[lon, lat]` with an optional third altitude element. It has **no place
for M or coordinate epoch metadata**, and [`to_geojson`][gometry.Geometry.to_geojson]
**raises** on an M ordinate by default rather than producing a file that silently lost
your measure. `__geo_interface__` also rejects M and epoch (see below).

```python exec="on" source="block" result="text"
import gometry as gm

p = gm.Point(21.0, 52.0, crs=4326)
print("2D:", p.to_geojson())

pz = gm.Point(21.0, 52.0, z=100.0, crs=4979)
print("with Z:", pz.to_geojson(include_z=True))   # third element = elevation
```

Constructing a geometry with M is always fine; the M is only rejected at
**serialization** time:

```python exec="on" source="block" result="text"
import gometry as gm

pm = gm.Point(21.0, 52.0, m=5.0, crs=4326)   # construction is fine
try:
    pm.to_geojson()                          # serialization rejects M
except gm.InvalidGeometryError as e:
    print("M ordinate raises on to_geojson:", type(e).__name__)
```

!!! warning "GeoJSON cannot carry M or epoch"
    `to_geojson` always raises on M, and epoch metadata must be explicitly dropped with
    `drop_epoch=True`. Clear M with `set_m(None)` and epoch with `set_epoch(None)` only
    when that data loss is intended. WKB/EWKB preserves M, but neither format can
     carry coordinate epoch. For lossless M/epoch interchange (timestamps, route
     measures), use gometry's GeoArrow extension metadata with gometry or an
     extension-aware consumer; standard GeoArrow metadata alone does not carry
     epoch.

## `__geo_interface__`

gometry geometries implement the `__geo_interface__` protocol, so they interoperate
with any library that speaks it. [`gm.from_geojson`][gometry.from_geojson] is the
inverse — it ingests anything exposing that protocol (or a raw GeoJSON-like
mapping), including a GeoJSON `Feature`.

```python exec="on" source="block" result="text"
import gometry as gm

p = gm.Point(21.0, 52.0, crs=4326)
mapping = p.__geo_interface__
print("mapping:", mapping)

# Rebuild from the mapping, or from a GeoJSON Feature dict:
back = gm.from_geojson(mapping, crs=4326)
feat = gm.from_geojson({"type": "Feature", "properties": {}, "geometry": mapping}, crs=4326)
print("round-trip:", back.to_wkt(), "| from Feature:", feat.to_wkt())
```

!!! note "`__geo_interface__` rejects unsupported metadata"
    The `__geo_interface__` mapping follows GeoJSON and therefore **raises** when M or
    coordinate epoch is present rather than silently discarding either. Clear them
    explicitly with `set_m(None)` / `set_epoch(None)` only when loss is intended.
     WKB/EWKB preserves M, but not coordinate epoch. For lossless M/epoch
     interchange, use gometry's GeoArrow extension metadata with gometry or an
     extension-aware consumer; standard GeoArrow metadata alone does not carry
     epoch.

[`gm.from_geojson`][gometry.from_geojson] is the one entry point for everything
GeoJSON-shaped: JSON strings, mappings, Features, FeatureCollections, and any object
exposing `__geo_interface__`. GeoJSON is WGS 84 lon/lat by specification
([RFC 7946](https://www.rfc-editor.org/info/rfc7946) / OGC:CRS84), so decoded
geometry declares `OGC:CRS84` by default (matching GeoParquet); pass
`crs=4326` for EPSG:4326, `crs=None` to opt out, or `crs=4979` for a
WGS84-family 3D frame. A `FeatureCollection` is a feature *set* and decodes to a
[`GeometryArray`][gometry.GeometryArray], one geometry per feature (use
[`gm.from_features`][gometry.from_features] to keep properties and ids).

!!! note "Uniform axes per coordinate sequence"
    RFC 7946 makes the third ordinate optional per *position*, but a gometry
    coordinate sequence is one set of columns: every vertex in a LineString,
    MultiPoint, or ring must share the same axes (all XY or all XYZ). Mixed
    positions such as `[[0, 0], [1, 1, 5]]` raise `ParseError` rather than
    inventing elevation (`0`) or using non-finite sentinels (NaN is rejected
    by the finite-coordinate contract). Distinct members of a
    `GeometryCollection` may still differ in axes.

```python exec="on" source="block" result="text"
import gometry as gm

g = gm.from_geojson({"type": "Point", "coordinates": [1.0, 2.0]})
print(g.to_wkt(), g.crs)
fc = {
    "type": "FeatureCollection",
    "features": [
        {"type": "Feature", "id": 7, "properties": {"name": "A"},
         "geometry": {"type": "Point", "coordinates": [1.0, 2.0]}},
        {"type": "Feature", "properties": {"name": "B"},
         "geometry": {"type": "Point", "coordinates": [3.0, 4.0]}},
    ],
}
print(gm.from_geojson(fc))
features = gm.from_features(fc)  # validated Features record
geoms, props, ids = features
print(features.properties, ids, len(features.geometries))
```

`from_features` returns a `Features` container with three parallel fields.
Destructure it as `geoms, props, ids = gm.from_features(...)`; it deliberately
has no `len()` (the old tuple's `len() == 3` field count read like a feature
count) — `len(features.geometries)` is the row count.

The outer `properties` and `ids` columns are tuples, so their alignment cannot
drift after parsing; individual property dictionaries remain editable. A missing
`properties` member normalizes to `{}`, while an explicit GeoJSON
`"properties": null` remains `None` and round-trips as JSON null.

The inverse is [`gm.to_feature_collection`][gometry.to_feature_collection] — give it
geometries plus keyword-only `properties` (and optional `ids`) and it assembles a
`FeatureCollection` mapping whose geometry members are JSON-ready. The mapping is
suitable for `json.dumps` **only when every property value is JSON-serializable**
(strings, numbers, bools, `None`, and nested lists/dicts of those) — arbitrary
Python objects in `properties` raise `TypeError` at dump time. `to_geojson` honors
the WGS84 contract on the way out too: reproject with `to_crs(4326)` first if your
geometry is in another frame.

Pass one mapping to broadcast shared metadata across every row (each feature gets
an independent outer dictionary), or pass an aligned iterable of mappings/`None`.
Omitting `properties` creates independent empty mappings. IDs are always aligned;
scalar IDs never broadcast, avoiding accidental duplicate identifiers.

## Pickle, multiprocessing, and copies

Pickle applies to durable public value and container types: `Geometry`,
`GeometryArray` (packed point, line, and polygon columns round-trip as raw
coordinate/offset lanes), `CellArray`, `Groups`, `CRS`, `H3Cell`, and `S2Cell`.
Their CRS, epoch, and Z/M metadata remain intact, so `multiprocessing`,
`concurrent.futures`, joblib, and caching layers just work. `copy`/`deepcopy`
ride the same protocol. Spatial indexes and prepared geometries serialize their
durable input state and rebuild derived acceleration structures on load; treat
unpickling them as reconstruction work, not a snapshot of warm caches. There is
no `Coverage` object to pickle: cover factories return `CellArray` or `Groups`
containers. `Coordinates` and iterator types remain intentionally
non-picklable. `GeometryParts` is picklable as an immutable parent view; it is
reconstructed from its geometry when unpickled.

```python exec="on" source="block" result="text"
import pickle

import gometry as gm

point = gm.Point(1, 2, crs=4326, epoch=2020.5)
restored = pickle.loads(pickle.dumps(point))
print(restored == point, restored.crs, restored.epoch)
```

## Round-trip cheat sheet

| Format | Out | In | CRS survives? | Epoch survives? | Z survives? | M survives? |
| --- | --- | --- | --- | --- | --- | --- |
| ISO WKB | `to_wkb()` / `to_wkb(drop_epoch=True)` | [`from_wkb`][gometry.from_wkb] | no | no (requires `drop_epoch=True` when present) | yes (ISO type codes) | yes (ISO type codes) |
| EWKB | `to_wkb(include_srid=True, drop_epoch=True)` | [`from_wkb`][gometry.from_wkb] | yes (SRID) | no (requires `drop_epoch=True` when present) | yes | yes |
| WKT | `to_wkt(output_dimension=, drop_epoch=True)` | [`from_wkt`][gometry.from_wkt] | no | no (requires `drop_epoch=True` when present) | yes (default; `output_dimension=2` drops) | yes (`M`/`ZM` tags) |
| EWKT | `to_wkt(include_srid=True, output_dimension=, drop_epoch=True)` | [`from_wkt`][gometry.from_wkt] | yes (`SRID=` prefix) | no (requires `drop_epoch=True` when present) | yes (default) | yes |
| GeoJSON | `to_geojson(include_z=, drop_epoch=True)` | [`from_geojson`][gometry.from_geojson] | declares WGS84 on read (default attaches `OGC:CRS84`; pass `crs=4326`/`crs=None`/`crs=4979` to control); reproject before `to_geojson` | no (requires `drop_epoch=True` when present) | yes (when `include_z=True`) | no — always raises on M; clear it explicitly first |
| `__geo_interface__` | `geom.__geo_interface__` | [`from_geojson`][gometry.from_geojson] | no | no — raises when present | yes (when present) | no — raises when present |
| GeoArrow | [`to_arrow`][gometry.Geometry.to_arrow] | [`gm.from_arrow`][gometry.from_arrow] | yes (PROJJSON) | yes, in gometry's extension metadata (gometry or extension-aware consumers; not standard GeoArrow metadata) | yes | yes (packed Z/M children for homogeneous axes; WKB fallback for mixed axes) |
| pandas | [`to_pandas`][gometry.GeometryArray.to_pandas] | adapter Series of `Geometry` | yes (on each geometry) | yes (on each geometry object) | yes | yes |
| GeoPandas | [`to_geopandas`][gometry.GeometryArray.to_geopandas] | GeoSeries | yes | no (requires `drop_epoch=True` when present) | yes | yes where the peer stores them |
| polars | [`to_polars`][gometry.GeometryArray.to_polars] | [`from_polars`][gometry.from_polars] | optional (`drop_crs`) | no (requires `drop_epoch=True` when present) | yes (EWKB) | yes (EWKB) |
| Pickle (durable values/containers) | `pickle.dumps` | `pickle.loads` | yes | yes | yes | yes (trusted Python only) |

!!! note "Dimensional empties"
    WKT and WKB preserve empty axes (`POINT Z EMPTY` stays XYZ). GeoJSON has no
    dimensional-empty encoding — `to_geojson()` emits a coordinate-less empty and
    flattens Z/M identity. Prefer WKT/WKB/`equals_identical` when axes matter.

!!! note "Epoch portability"
    The coordinate epoch is not representable in (E)WKB, (E)WKT, or GeoJSON, and
    the five lossy serializers (`to_wkb`, `to_wkt`, `to_geojson`, `to_polars`,
    `to_geopandas`) refuse to drop it silently — pass `drop_epoch=True` to
    acknowledge the loss. [Arrow & storage](arrow.md) carries epoch across
    gometry-to-gometry process boundaries, or to an extension-aware consumer,
    using gometry's extension metadata; epoch is not portable standard GeoArrow
    metadata and other producers may omit it. pandas and pickle also preserve
    epoch inside trusted Python persistence.

Coming from Shapely? See [Migrating](../migrating/index.md#coming-from-shapely).

## See also

- [Arrow & storage](arrow.md) — the columnar boundary that carries gometry's epoch extension, plus GeoParquet and lonboard.
- [DataFrames](dataframes.md) — polars stores the EWKB this page produces.
- [Ecosystem & interoperability](index.md) — the partner matrix.
