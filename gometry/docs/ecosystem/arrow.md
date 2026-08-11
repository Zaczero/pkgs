---
description: Arrow C Data Interface and GeoArrow interchange for gometry — dependency-free PyCapsules, native packed layouts, pyarrow via gometry[arrow], plus durable GeoParquet storage and lonboard maps.
---

# Arrow & storage

This page covers the columnar end of the pipeline: the Arrow C Data Interface and
[GeoArrow](https://geoarrow.org/) layouts that `Geometry` and `GeometryArray`
export, and the two durable-storage extras that build on them — **GeoParquet**
files and **lonboard** maps. Read the capsule and round-trip sections first for
the in-memory boundary, then the GeoParquet and lonboard sections for the two ends
of a persisted pipeline.

## PyCapsules first, pyarrow when you ask for it

`Geometry` and `GeometryArray` implement the Arrow PyCapsule protocol with no
optional dependency:

- `__arrow_c_schema__` → a capsule named `arrow_schema`
- `__arrow_c_array__` → a capsule named `arrow_array`
- `__arrow_c_stream__` → a capsule named `arrow_array_stream`

Any capsule-aware consumer — Polars, DuckDB, lonboard — imports the GeoArrow
buffers straight from these capsules **without pyarrow installed**. Install
`gometry[arrow]` only when you want `(...).to_arrow()` to hand back a concrete
pyarrow array, or when you need to ingest an older pyarrow-only object.

```bash
pip install 'gometry[arrow]'
```

## Round-trip

A [`GeometryArray`][gometry.GeometryArray] converts to an Arrow array with
[`to_arrow`][gometry.Geometry.to_arrow] and back with [`gm.from_arrow`][gometry.from_arrow],
preserving geometry, CRS, and coordinate epoch:

```python exec="on" source="block" result="text"
import gometry as gm

arr = gm.GeometryArray([
    gm.Point(21.0, 52.0, crs=4326),
    gm.Point(22.0, 52.0, crs=4326),
])

ga = arr.to_arrow()           # pyarrow array, GeoArrow layout
print("arrow type:", ga.type)    # packed point layout

restored = gm.from_arrow(ga)
print("round-trip:", [g.to_wkt() for g in restored])
print("crs preserved:", restored.crs.to_authority())
```

## Native packed layouts vs WKB fallback

Homogeneous **XY / XYZ / XYM / XYZM** point, multipoint, linestring,
multilinestring, polygon, and multipolygon arrays use the native GeoArrow
coordinate layout with separated `x`/`y`/`z`/`m` children. Gometry exports may
share immutable gometry-owned buffers with a consumer. Import does not retain
shared views of foreign memory. PyArrow arrays, chunked arrays, tables, and
record batches are imported through PyArrow’s buffer-copy APIs. Other
`__arrow_c_array__` and `__arrow_c_stream__` providers use the native Arrow-C
path: gometry moves the one-shot base structure, snapshots the selected schema
plus visible validity, offset, view, referenced variadic-size, coordinate, and WKB spans,
invokes the producer release callback, and validates and decodes only the owned
snapshot. Unrelated table columns, BinaryView size entries, and payload ranges
are not retained. Mixed geometry types, mixed coordinate axes, and geometry
collections fall back to GeoArrow **WKB** encoding so nothing is lost.

`from_arrow` handles PyArrow-shaped arrays, chunked arrays, tables, and record
batches through the PyArrow path. Other objects implementing
`__arrow_c_array__` or `__arrow_c_stream__` use the dependency-free native
capsule path.

### Arrow C capsule trust model

The [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
carries **no buffer capacity except BinaryView's mandatory variadic-sizes
buffer**, which gometry decodes and enforces. A producer that lies about other
buffer sizes or metadata cannot be made fully memory-safe from inside the
consumer alone. Every Arrow consumer works this way (including pyarrow).

gometry therefore draws an explicit line:

- **ABI-conforming means** that the exported structures, pointer tables,
  layout-implied readable capacities, and release callbacks obey the Arrow C
  Data Interface. It does **not** mean that gometry has exclusive buffer
  ownership or that retained aliases are immutable. A native producer must not
  modify exported memory before its release callback runs; a PyArrow object
  must not be modified during `from_arrow`. Changes after native admission and
  release cannot affect the result. A provider that forges pointers or
  capacities is outside the threat model.
- **Data and layout are validated defensively on the owned snapshot.** Schema
  formats, nested offset monotonicity **including null slots**, null bitmaps,
  BinaryView prefix and inline padding, coordinate finiteness, and GeoArrow
  encoding/storage pairs are checked; malformed *owned* data raises a clean
  domain error rather than panicking or accepting garbage. Zero-length
  chunks/batches are discarded after type/frame validation so empty storage is
  not retained for a zero-row result.
- **No amplification.** Import work and retained memory stay bounded relative
  to the input you supplied. Like every native extension, a proportionally
  huge valid column can still OOM the process — callers parsing untrusted
  Arrow must bound **input size** at the trust boundary. See
  [Security & untrusted input](../about/security.md#ingress-threat-model).
- **`__arrow_c_array__` / `__arrow_c_stream__` producers are trusted to be
  ABI-conforming.** A deliberately hostile duck-typed object that forges
  capsules or lies about its own buffers, `column_names`,
  `__arrow_ext_serialize__`, or `type.names` is **out of the threat model** —
  the same posture as pyarrow itself. Do not expect gometry to harden against
  that class of producer.

## CRS as PROJJSON, epoch as a gometry extension

The GeoArrow extension metadata writes the CRS as **PROJJSON** — GeoArrow's
recommended encoding, and the one [GeoParquet](https://geoparquet.org/) 1.1
requires when columns land in a lakehouse. It reads back both PROJJSON objects and
plain authority strings from other producers.

The **coordinate epoch** is **not** part of the GeoArrow extension-types
standard; gometry stores it as an additional JSON member in the same extension
metadata blob so it can survive a columnar round-trip when both sides are
gometry (or another reader that understands this extension). Other producers may
omit `epoch` entirely. WKB/EWKB and GeoJSON still cannot carry epoch — only this
gometry GeoArrow extension field and GeoParquet column metadata do.

## Capsule consumers without pyarrow

For dependency-free producers, pass the gometry object itself to any consumer that
understands the Arrow PyCapsule protocol; the consumer calls `__arrow_c_array__`
or `__arrow_c_stream__` and imports the GeoArrow buffers **zero-copy**. When
gometry is the consumer, `from_arrow` accepts those capsules too and copies the
selected schema plus visible validity, offset, view, variadic-size, coordinate,
and WKB spans into gometry-owned storage before validation and decode.

DuckDB, raw `polars.from_arrow(...)` calls, and lonboard can consume these
capsules directly. gometry's supported `to_polars()` / `from_polars()` adapter
instead uses a batched WKB/EWKB `Binary` Series so it remains independent of
PyArrow; that boundary is covered in [DataFrames](dataframes.md). The lonboard
map path below uses the direct capsule handoff.

## GeoParquet — durable columnar storage

Install `gometry[arrow]` for `GeometryArray.to_geoparquet()` /
`gm.from_geoparquet`. Write a [`GeometryArray`][gometry.GeometryArray] to a
[GeoParquet](https://geoparquet.org/) file and read it back — CRS metadata is
embedded as **PROJJSON** in the column header, geometry is written with **WKB**
encoding (the GeoParquet 1.1 default), and gometry's missing rows travel as Arrow
validity bits instead of sentinel geometries. GeoParquet builds on pyarrow (the
`arrow` extra provides it); for an in-memory handoff without a file, use
[`to_arrow`][gometry.Geometry.to_arrow] above.

```python exec="on" source="block" result="text"
import tempfile

import gometry as gm

geoms = gm.GeometryArray([
    gm.Point(21.0, 52.0, crs=4326),
    None,
    gm.Point(22.0, 52.5, crs=4326),
])

arrow = geoms.to_arrow()
from_arrow = gm.from_arrow(arrow)
print("GeoArrow nulls:", arrow.null_count)
print("Arrow mask:", from_arrow.is_missing.tolist())

with tempfile.TemporaryDirectory() as tmp:
    path = f"{tmp}/places.parquet"
    geoms.to_geoparquet(path)
    # A GeoParquet file is a feature table: geometry column + attribute table.
    from_file, attributes = gm.from_geoparquet(path)

print("GeoParquet:", from_file, from_file.crs.to_authority())
print("file mask:", from_file.is_missing.tolist())
print("attribute columns:", attributes.column_names)

```

## lonboard — maps in notebooks

Install `gometry[viz]` for interactive maps.
[`GeometryArray._repr_html_`][gometry.GeometryArray] embeds a lonboard preview in
Jupyter when lonboard is available; it uses lonboard's standalone HTML export so the
same preview works wherever notebook HTML is rendered. Otherwise it falls back to the
SVG grid used elsewhere in this documentation.

```python exec="on" source="block" result="text"
import gometry as gm

arr = gm.points([21.0, 22.0], [52.0, 52.5], crs=4326)
html = arr._repr_html_()
print("notebook HTML bytes:", len(html) if html else 0)
print("explore available:", callable(gm.explore))
```

`gm.explore` hands lonboard a **zero-copy GeoArrow capsule** when possible (the
same capsule path as above); a GeoJSON feature-collection fallback runs only if the
capsule handoff fails. Inputs need a CRS and finite bounds. By default it presents
a map where points, lines, and polygons remain distinct and features are inspectable
on hover. Pass lonboard's `scatterplot_kwargs`, `path_kwargs`,
`polygon_kwargs`, or `map_kwargs` to tailor individual settings.

!!! note "Reprojection for display"
    lonboard expects WGS 84 longitude/latitude display coordinates. Arrays whose
    CRS is not equivalent to EPSG:4326 / OGC:CRS84 (including non-WGS84
    geographic frames such as NAD83, and all projected frames) should be
    reprojected with `to_crs(4326)` before display. The viz helper normalizes
    non-WGS84 geographic CRS the same way as projected ones.

## See also

- [DataFrames](dataframes.md) — pandas / polars / GeoPandas, built on this layer.
- [Text & binary formats](text-formats.md) — WKB/EWKB for database boundaries.
- [NumPy arrays & coordinates](numpy.md) — extracting raw coordinate buffers.
- [Ecosystem & interoperability](index.md) — the partner matrix.
