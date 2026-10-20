---
description: gometry text and binary IO — WKB/EWKB for databases, WKT/EWKT, GeoJSON Z/M rules, Features.__geo_interface__, pickle, and CRS/epoch round-trip behavior.
---

# Text & binary formats

gometry moves geometry in and out as WKB, EWKB, WKT, EWKT, GeoJSON, the
`__geo_interface__` protocol, and [pickle](https://docs.python.org/3/library/pickle.html). Each format carries CRS, Z, and M
differently. Z is preserved unless you ask to drop it; M is preserved in formats
that can hold it and **rejected** in formats that can't (rather than silently
discarded); SRID is embedded only when you
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
    [`from_wkb(to_wkb(...))`][gometry.from_wkb] returns geometry with **no CRS** — ISO WKB has nowhere
    to store one. EWKB can embed an integer EPSG SRID when the geometry's CRS has
    an EPSG authority code, or when it is one of the exact **PostGIS wire aliases**
    `OGC:CRS84` → SRID 4326 and `OGC:CRS84h` → SRID 4979
    (`to_wkb(include_srid=True)` rejects other non-EPSG CRS values). These aliases
    are a deliberate PostGIS serialization convention (lon/lat storage matches
    EPSG:4326 on the wire), **not** universal CRS identity: object equality
    [`CRS(4326) == CRS("OGC:CRS84")`][gometry.CRS] stays `False`, and a bare EWKB round-trip
    therefore loses the OGC spelling:

    ```python title="partial: requires the preceding WKB setup"
    from_wkb(Point(10, 50, crs="OGC:CRS84").to_wkb(include_srid=True)).crs
    ```

    Pass `crs="OGC:CRS84"` at decode to restore the alias when the embedded SRID
    is exactly 4326 (a genuine SRID/CRS conflict still raises). Epoch never rides
    either WKB flavour.

### The database boundary

The **portable** binary boundary is ISO WKB with CRS/epoch in side metadata, or
GeoArrow with CRS plus gometry's epoch extension when the peer understands it.
Use **EWKB** with PostGIS or another system that documents EWKB SRID support.
gometry ships no database adapters. Keep the boundary
binary and columnar — write bytes out, read bytes back, and decode the whole
 column in one `gm.from_wkb` call rather than a per-row geometry
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

```python exec="on" source="block" result="text"
import gometry as gm

geoms = gm.points([21.0, 22.0], [52.0, 53.0], crs=4326)
wkb_arrow = geoms.to_arrow(encoding="wkb")
print(wkb_arrow.type)
```


## WKT and EWKT

WKT is the human-readable format. gometry reads and writes the `Z`, `M`, and
`ZM` dimensional tags, preserving the ordinates the geometry carries. Pass
`include_srid=True` to emit **EWKT** with an `SRID=` prefix so the CRS survives
the text round-trip.

```python exec="on" source="block" result="text"
import gometry as gm

pz = gm.Point(21.0, 52.0, z=100.0, crs=4979)
print("default:", pz.to_wkt())
print("force 2D:", pz.force_2d().to_wkt())
print("EWKT:", pz.to_wkt(include_srid=True))

print("parse:", gm.from_wkt("POINT ZM (1 2 3 4)").coordinate_axes)
```

!!! note "WKT is for humans, not throughput"
    Use WKT for debugging, logs, and copy-paste. For high-volume storage prefer
    WKB/EWKB or GeoArrow — parsing decimal text is far slower than reading packed
    binary.

## GeoJSON

[GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946) (RFC 7946) defines
positions as `[lon, lat]` with an optional third altitude element. It has **no place
for M or coordinate epoch metadata**. [`to_geojson`][gometry.Geometry.to_geojson]
and `__geo_interface__` raise when those values cannot be represented.

GeoJSON uses OGC:CRS84 longitude/latitude order and carries no source CRS label.
A tagged geometry must already be in the WGS 84 geographic family; serialization
does not reproject it. A CRS-free geometry is an explicit assumption that its
coordinates already have OGC:CRS84 semantics: output validates longitude in
`[-180, 180]` and latitude in `[-90, 90]`, and splits antimeridian-crossing
geometry according to RFC 7946. CRS-free projected or other planar coordinates
must be transformed or written with another format. On read, the default
`crs='OGC:CRS84'` records these semantics; pass `crs=None` only when you
intentionally want to discard the CRS metadata.

An antimeridian-crossing line is written as separate pieces with corresponding
vertices at `180` and `-180`. For XYZ input, each inserted seam vertex carries
altitude from the original CRS84 edge by ellipsoidal arclength, not by the
longitude/latitude crossing parameter; see
[Z/M under operations](../guide/geometry.md#zm-under-operations).

```python exec="on" source="block" result="text"
import json

import gometry as gm

crossing = gm.LineString([(170, 10, 10), (-170, 10, 30)], crs="OGC:CRS84")
geojson = json.loads(crossing.to_geojson())
print(geojson["type"])
print(geojson["coordinates"])
```

```python exec="on" source="block" result="text"
import gometry as gm

p = gm.Point(21.0, 52.0, crs=4326)
print("2D:", p.to_geojson())

pz = gm.Point(21.0, 52.0, z=100.0, crs=4979)
print("with Z:", pz.to_geojson())
```

Construction accepts M; the restriction applies at **serialization** time:

```python exec="on" source="block" result="text"
import gometry as gm

pm = gm.Point(21.0, 52.0, m=5.0, crs=4326)
try:
    pm.to_geojson()
except gm.InvalidGeometryError as e:
    print("M ordinate raises on to_geojson:", type(e).__name__)
```

!!! warning "GeoJSON cannot carry M or epoch"
    `to_geojson` and `__geo_interface__` raise on M; epoch metadata must be
    explicitly dropped with `drop_epoch=True`. Clear M with [`with_coordinates(m=None)`][gometry.Geometry.with_coordinates] or
    [`force_2d()`][gometry.Geometry.force_2d], and
    epoch with [`with_epoch(None)`][gometry.Geometry.with_epoch] only when that loss is intended. WKB/EWKB
    preserves M but not epoch. For lossless M/epoch interchange, use gometry's
    GeoArrow extension metadata with gometry or an extension-aware consumer;
    standard GeoArrow metadata alone does not carry epoch.

## `__geo_interface__`

gometry geometries implement the `__geo_interface__` protocol, so they interoperate
with any library that speaks it. [`gm.from_geojson`][gometry.from_geojson] is the
inverse — it ingests anything exposing that protocol (or a raw GeoJSON-like
mapping), including a GeoJSON `Feature`. By default it returns the geometry and
drops feature side data; pass `features=True` to retain properties and IDs.

```python exec="on" source="block" result="text"
import gometry as gm

p = gm.Point(21.0, 52.0, crs=4326)
mapping = p.__geo_interface__
print("mapping:", mapping)

back = gm.from_geojson(mapping, crs=4326)
feature = gm.from_geojson(
    {"type": "Feature", "properties": {"name": "site"}, "geometry": mapping},
    crs=4326,
    features=True,
)
print("round-trip:", back.to_wkt(), "| Feature:", feature.geometries[0].to_wkt())
```

[`gm.from_geojson`][gometry.from_geojson] is the one entry point for everything
GeoJSON-shaped: JSON strings, mappings, Features, FeatureCollections, and any object
exposing `__geo_interface__`. GeoJSON is WGS 84 lon/lat by specification
([RFC 7946](https://www.rfc-editor.org/info/rfc7946) / OGC:CRS84), so decoded
geometry declares `OGC:CRS84` by default (matching GeoParquet); pass
`crs=4326` for EPSG:4326, `crs=None` to opt out, or `crs=4979` for a
WGS84-family 3D frame. A `FeatureCollection` is a feature *set*: by default it
decodes to a [`GeometryArray`][gometry.GeometryArray], one geometry per feature.
Set `features=True` on the same reader to return [`Features`][gometry.Features]
with the aligned properties and IDs.

!!! note "Uniform axes per coordinate sequence"
    RFC 7946 makes the third ordinate optional per *position*, but a gometry
    coordinate sequence is one set of columns: every vertex in a [`LineString`][gometry.LineString],
    [`MultiPoint`][gometry.MultiPoint], or ring must share the same axes (all XY or all XYZ). Mixed
    positions such as `[[0, 0], [1, 1, 5]]` raise [`ParseError`][gometry.ParseError] rather than
    inventing elevation (`0`) or using non-finite sentinels (NaN is rejected
    by the finite-coordinate contract). Distinct members of a
    [`GeometryCollection`][gometry.GeometryCollection] may still differ in axes.

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
features = gm.from_geojson(fc, features=True)
geoms, props, ids = features
print(features.properties, ids, len(features.geometries))
```

`features=True` returns a [`Features`][gometry.Features] container with three
parallel fields. Destructure it as `geoms, props, ids = gm.from_geojson(..., features=True)`. It has no
`len()`, so a three-field count cannot be misread as a feature count;
`len(features.geometries)` is the row count.

The outer [`properties`][gometry.Features.properties] and [`ids`][gometry.Features.ids] columns are tuples, so their alignment cannot
drift after parsing; individual property dictionaries remain editable. A missing
`properties` member normalizes to `{}`, while an explicit GeoJSON
`"properties": null` remains `None` and round-trips as JSON null.
When constructing [`Features`][gometry.Features] directly, omitted or explicit
`properties=None` records `None` for every row; pass `properties={}` when
independently editable empty mappings are wanted.

### `Features.__geo_interface__`

`features.__geo_interface__` returns a fresh `FeatureCollection` with the
record's geometries, properties, and IDs. Missing geometries and properties
remain `null`, and rows without an ID omit `id`; encoding uses the same WGS 84,
epoch, and M constraints as
[`gm.to_feature_collection`][gometry.to_feature_collection].

[`gm.to_feature_collection`][gometry.to_feature_collection] assembles a
`FeatureCollection` from separate geometry and metadata columns, with keyword-only
`properties` (and optional `ids`). Its geometry members are JSON-ready. The mapping is
suitable for `json.dumps` **only when every property value is JSON-serializable**
(strings, numbers, bools, `None`, and nested lists/dicts of those) — arbitrary
Python objects in `properties` raise `TypeError` at dump time. `to_geojson` honors
the WGS84 contract on the way out too: reproject with [`to_crs(4326)`][gometry.Geometry.to_crs] first if your
geometry is in another frame.
For CRS-free geometry, the same contract assumes that the stored coordinates
already are WGS84 longitude/latitude.

Pass one mapping to broadcast shared metadata across every row (each feature gets
an independent outer dictionary), or pass an aligned iterable of mappings/`None`.
Omitting `properties` creates independent empty mappings. IDs are always aligned;
scalar IDs never broadcast, avoiding accidental duplicate identifiers.

## Pickle, multiprocessing, and copies

Pickle applies to durable public value and container types: [`Geometry`][gometry.Geometry],
`GeometryArray`, [`CellArray`][gometry.CellArray], [`Groups`][gometry.Groups],
[`Features`][gometry.Features], `CRS`, [`H3Cell`][gometry.H3Cell], and
[`S2Cell`][gometry.S2Cell].
Their CRS, epoch, and Z/M metadata remain intact through pickle. The same value
protocol supports [`multiprocessing`](https://docs.python.org/3/library/multiprocessing.html),
[`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html), joblib, and cache
storage; [`copy`](https://docs.python.org/3/library/copy.html#copy.copy)/[`deepcopy`](https://docs.python.org/3/library/copy.html#copy.deepcopy) use it as well. Spatial indexes and prepared
geometries serialize durable input state and rebuild derived acceleration
structures when loaded. [`Coordinates`][gometry.Coordinates] is picklable as an owner-retaining
view, preserving its selected layout and flattened row indexes; iterator types
are not picklable. [`GeometryParts`][gometry.GeometryParts] is picklable as an
immutable parent view; it is reconstructed from its geometry when unpickled.

`copy.copy(features)` returns a new record sharing its geometry, property, and ID
columns, so edits to a shared property dictionary are visible through either
record. `copy.deepcopy(features)` recursively copies property mappings and their
values. On Python 3.13 and newer, `copy.replace(features, ...)` preserves each
omitted column by identity and validates supplied columns.

With pickle protocol 5, packed `GeometryArray` payload columns and their offsets
can be sent out of band when the pickler uses `buffer_callback`. Those buffers
are read-only `PickleBuffer` values; the unpickler must receive the collected
buffers. Pickle protocol 5 alone does not make ordinary process-pool transfers
zero-copy or create shared memory.

### Stored-format compatibility

New releases retain support for reading released pickle formats.
Older releases need not read pickles written by newer ones.

The durable state includes coordinate dimensions, CRS definitions, coordinate
epochs, missing rows, group offsets, and spatial-index handles, including removed
handles. Packed numeric columns use little-endian values with fixed-width
offsets, independent of the host architecture. Derived indexes, prepared
geometry, CRS caches, and validation verdicts are rebuilt, so numerical results
and diagnostics can reflect bug fixes and updated CRS authority data.

WKB, WKT, GeoJSON, GeoArrow, and GeoParquet retain their standard formats rather
than acquiring a gometry-specific envelope. GeoParquet carries its own format
version; unknown major versions are rejected. For storage shared with other
applications, use [Arrow and GeoParquet](arrow.md) and check their documented
CRS, epoch, and dimension support. Pickles require gometry and any optional
packages used by the stored objects; only load pickles from trusted sources.

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
| ISO WKB | [`to_wkb()` / `to_wkb(drop_epoch=True)`][gometry.Geometry.to_wkb] | [`from_wkb`][gometry.from_wkb] | no | no (requires `drop_epoch=True` when present) | yes (ISO type codes) | yes (ISO type codes) |
| EWKB | `to_wkb(include_srid=True, drop_epoch=True)` | [`from_wkb`][gometry.from_wkb] | yes (SRID) | no (requires `drop_epoch=True` when present) | yes | yes |
| WKT | [`to_wkt(drop_epoch=True)`][gometry.Geometry.to_wkt] | [`from_wkt`][gometry.from_wkt] | no | no (requires `drop_epoch=True` when present) | yes, when carried | yes (`M`/`ZM` tags) |
| EWKT | `to_wkt(include_srid=True, drop_epoch=True)` | [`from_wkt`][gometry.from_wkt] | yes (`SRID=` prefix) | no (requires `drop_epoch=True` when present) | yes, when carried | yes |
| GeoJSON | [`to_geojson(drop_epoch=True)`][gometry.Geometry.to_geojson] | [`from_geojson`][gometry.from_geojson] | assumes WGS84 lon/lat on write and declares it on read (default attaches `OGC:CRS84`; pass `crs=4326`/`crs=None`/`crs=4979` to control); reproject before `to_geojson` | no (requires `drop_epoch=True` when present) | yes, when carried | no — raises on M; use `with_coordinates(m=None)` or `force_2d()` first |
| `__geo_interface__` | `geom.__geo_interface__` | [`from_geojson`][gometry.from_geojson] | no | no — raises when present | yes (when present) | no — raises when present |
| GeoArrow | [`to_arrow`][gometry.Geometry.to_arrow] | [`gm.from_arrow`][gometry.from_arrow] | yes (PROJJSON) | yes, in gometry's extension metadata (gometry or extension-aware consumers; not standard GeoArrow metadata) | yes | yes (packed Z/M children for homogeneous axes without explicit empty members; WKB fallback for mixed axes or explicit empty members) |
| pandas | [`to_pandas`][gometry.GeometryArray.to_pandas] | adapter Series of [`Geometry`][gometry.Geometry] | yes (on each geometry) | yes (on each geometry object) | yes | yes |
| GeoPandas | [`to_geopandas`][gometry.GeometryArray.to_geopandas] | GeoSeries | yes | no (requires `drop_epoch=True` when present) | yes | yes where the peer stores them |
| polars | [`to_polars`][gometry.GeometryArray.to_polars] | [`from_polars`][gometry.from_polars] | optional (`drop_crs`) | no (requires `drop_epoch=True` when present) | yes (EWKB) | yes (EWKB) |
| Pickle (durable values/containers) | [`pickle.dumps`](https://docs.python.org/3/library/pickle.html#pickle.dumps) | [`pickle.loads`](https://docs.python.org/3/library/pickle.html#pickle.loads) | yes | yes | yes | yes (trusted Python only) |

!!! note "Dimensional empties"
    WKT and WKB preserve empty axes (`POINT Z EMPTY` stays XYZ). GeoJSON has no
    dimensional-empty or explicit empty-multipart encoding — `to_geojson()` emits
    a coordinate-less top-level empty, and raises rather than dropping a member
    or flattening Z/M identity. Prefer WKT/WKB/[`equals_identical`][gometry.equals_identical]
    when axes or multipart member positions matter.

Coming from Shapely? See [Migrating](../migrating/index.md#coming-from-shapely).

## See also

- [Arrow & storage](arrow.md) — the columnar boundary that carries gometry's epoch extension, plus GeoParquet and lonboard.
- [DataFrames](dataframes.md) — Polars uses the WKB/EWKB binary storage boundary.
- [Ecosystem & interoperability](index.md) — the partner matrix.
