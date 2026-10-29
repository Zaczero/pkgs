---
description: Arrow C Data Interface and GeoArrow interchange in gometry — dependency-free PyCapsules, packed layouts, pyarrow via gometry[arrow], and GeoParquet.
---

# Arrow & storage

Exchange geometry columns through [GeoArrow](https://geoarrow.org/), store them
in GeoParquet, or export cell IDs as Arrow columns. Install PyArrow support for
the conversion and file examples:

```bash
uv add "gometry[arrow]"
```

Consumers that use the [Arrow PyCapsule protocol](#arrow-pycapsules-without-pyarrow)
can exchange data without PyArrow installed.

## Round-trip

A `GeometryArray` converts to an Arrow array with
`to_arrow` and back with `gm.from_arrow`, retaining CRS and coordinate epoch:

```python exec="on" source="block" result="text"
import gometry as gm

arr = gm.GeometryArray([
    gm.Point(21.0, 52.0, crs=4326),
    gm.Point(22.0, 52.0, crs=4326),
])

ga = arr.to_arrow()
print("arrow type:", ga.type)

restored = gm.from_arrow(ga)
print("round-trip:", [g.to_wkt() for g in restored])
print("crs preserved:", restored.crs.to_authority())
```

### Geographic drawings

GeoArrow and GeoParquet use literal planar edges. Typed conversion adapts
geographic branches and regions to that model: export may canonicalize longitude
aliases, split parts at the seam, and add pole closures; import may subdivide long
literal edges before admitting them to gometry's periodic chart. Required new
vertices have finite, rounded coordinates. A split line or polygon can become its
multi-part counterpart, including other rows in a homogeneous column.

Original Z/M occurrences are retained. New export seam vertices use native
source-arclength interpolation; new import vertices use affine interpolation along
the wire edge. Synthetic pole closures extend the incident endpoint's Z/M values.
These finite drawings do not promise identical point incidence, boundary
relationships, or continuous Z/M interpolation across conversion. Arrow capsules,
PyArrow, and all GeoParquet encodings follow the same conversion rules.
Use [raw WKT/WKB](text-formats.md) when you need literal stored coordinates
without this typed geographic adaptation.

## Feature tables

`gm.from_arrow` returns a `GeometryArray` for a geometry column and `Features`
for a geometry-bearing table, record batch or reader. It also accepts providers
of the Arrow C array and stream interfaces without requiring PyArrow for import.
The feature snapshot keeps attributes and explicit feature IDs aligned with its
geometries:

```python exec="on" source="block" result="text"
import gometry as gm
import numpy as np
import pyarrow as pa

features = gm.Features.from_columns(
    gm.points([10, 20, 30], [40, 50, 60], crs=3857),
    columns={
        "name": pa.array(["west", "center", "east"]),
        "rank": np.array([2, 4, 6], dtype=np.int16),
    },
    ids=np.array([1000, 1001, 1002], dtype=np.uint64),
)
selected = features[[2, 0]].with_columns(
    {"rank": np.array([7, 9], dtype=np.int16)}
)
table = selected.to_arrow()
restored = gm.from_arrow(table)

print("names:", list(restored.column("name")))
print("IDs:", list(restored.ids))
print("rank type:", table.schema.field("rank").type)
```

Input buffers are snapshotted at admission, so later changes to the producer
cannot change the features. Replacements share unchanged columns. Whole-column
and contiguous-window exports share compatible owned buffers; gathers may
allocate. Use `features.materialize()` to detach a small selection from its
larger backing storage.

For a record-batch stream, `gm.from_arrow_batches(reader)` yields one feature
snapshot at a time. Closing the iterator releases its stream; the caller owns
the original reader.

Values with no faithful ordinary Python representation use `ArrowValue`.
For example, a nanosecond timestamp keeps its exact unit and value rather than
being truncated to Python's microsecond precision:

```python exec="on" source="block" result="text"
import gometry as gm
import pyarrow as pa

times = gm.Features.from_columns(
    gm.points([1], [2]),
    columns={"observed": pa.array([1], type=pa.timestamp("ns", tz="UTC"))},
)
observed = times.column("observed")[0]
print(observed.type, observed.value)
print(pa.array(observed).type)
```

An `ArrowValue` exports a one-element Arrow array and supports `as_py()` when
conversion preserves its meaning and precision. The result does not depend on
which optional Python Arrow provider is installed.

Feature IDs have an explicit role. Export records the ID column's name in the
schema metadata key `gometry:feature_id`; import honors that declaration. A
column merely named `id` remains an ordinary property unless declared as the
ID column. Use `gm.from_arrow(table, id_column="fid")` to select the role, or
`id_column=None` to import all non-geometry columns as properties. ID zero is
preserved.

When a table contains several recognized geometry columns, pass
`geometry="column_name"` to select the active geometry. Other geometry columns
remain typed attributes. Sparse Python properties become rectangular columns
on Arrow export: an absent key and an explicitly null value both become an
Arrow null. The original feature snapshot retains that distinction.

## GeoParquet — durable columnar storage

[`GeometryArray.to_geoparquet()`][gometry.GeometryArray.to_geoparquet] and
`Features.to_geoparquet()` write geometry and aligned attributes to a file.
[`gm.from_geoparquet`][gometry.from_geoparquet] returns `Features`, retaining
CRS, coordinate epoch, missing rows and any declared ID role.

```python exec="on" source="block" result="text"
import tempfile

import gometry as gm

geoms = gm.GeometryArray([
    gm.Point(21.0, 52.0, crs=4326),
    None,
    gm.Point(22.0, 52.5, crs=4326),
])
attributes = {"name": ["west", None, "east"]}

with tempfile.TemporaryDirectory() as tmp:
    path = f"{tmp}/places.parquet"
    geoms.to_geoparquet(path, attributes=attributes)
    features = gm.from_geoparquet(path)

print("GeoParquet:", features.geometries, features.geometries.crs.to_authority())
print("file mask:", features.geometries.is_missing.tolist())
print("names:", list(features.column("name")))

```

An explicit feature ID is stored as a named column and its role is recorded
in `gometry:feature_id` schema metadata. `columns=` selects properties while
retaining the active geometry and ID role. Pass `id_column=None` when reading
to treat that column as an ordinary property instead.

### Encodings

The default is WKB with GeoParquet 1.1 metadata.
Choose `encoding='geoarrow'` for separated coordinates in a homogeneous
GeoParquet 1.1 column, or `encoding='geometry'` for native Parquet `GEOMETRY`
with GeoParquet 2.0 metadata. Native geometry preserves XY, XYZ, XYM and XYZM;
the 1.1 encodings support XY and XYZ and reject M rather than silently dropping
it. All encodings use the finite geographic drawings described above, rather
than spherical or ellipsoidal geodesic curves. Nonplanar `GEOGRAPHY` curves
require explicit conversion before import.

### Spatial reads

The 1.1 writer includes a per-row bbox covering; native geometry uses Parquet's
row-group geospatial statistics.

Read a spatial subset with
`gm.from_geoparquet(path, bbox=(xmin, ymin, xmax, ymax))`. The rectangle uses the
selected column's CRS, includes boundary contact, and can cross the geographic
longitude seam. Coverings or statistics prune candidate reads, then exact
intersection refinement selects geometry and attributes together. Missing and
empty geometries do not match. Attribute `filters`, `columns` and `row_groups`
compose with the spatial selection; requested row-group order and repetitions
are retained. Geographic pruning uses conservative latitude bounds; native
coordinate statistics do not exclude potentially polar areal geometries.

### Streams and metadata

Native Parquet geometry can also be read without `geo` metadata. When several
geometry columns have no declared primary, select one with `geometry='name'`.
Native logical types, stored Arrow metadata and any GeoParquet declarations
must describe the same frame.

The GeoParquet reader and writer also accept binary streams, including
`io.BytesIO` and `pyarrow.NativeFile`. Readers require a seekable binary source;
writers require a writable binary sink. Both leave caller-owned streams open.
Text streams are unsupported.

## Arrow PyCapsules without pyarrow

`Geometry` and `GeometryArray` implement the Arrow PyCapsule protocol with no
optional dependency:

- `__arrow_c_schema__` → a capsule named `arrow_schema`
- `__arrow_c_array__` → a `(schema_capsule, array_capsule)` tuple named
  `arrow_schema` and `arrow_array`
- `__arrow_c_stream__` → a capsule named `arrow_array_stream`

Cell identity arrays also implement `__arrow_c_array__`. H3, S2, tile, H3 edge,
and H3 vertex IDs use `uint64`; geohash cells use UTF-8 tokens. `CellArray`
missing rows are Arrow nulls. These primitive columns do not carry the grid
kind: pass the known cell type to [`CellArray`][gometry.CellArray] when importing
IDs, or construct an [`H3EdgeArray`][gometry.H3EdgeArray] or
[`H3VertexArray`][gometry.H3VertexArray] from its IDs.

A consumer that implements the Arrow C Data Interface can import the GeoArrow
buffers straight from these capsules **without pyarrow installed**. Whether a
consumer preserves GeoArrow extension metadata is version- and adapter-specific;
see the [compatibility matrix](../about/compatibility.md) for deployed
integration details. Install `gometry[arrow]` for [`(...).to_arrow()`][gometry.Geometry.to_arrow] to return
a concrete pyarrow array or to ingest an older pyarrow-only object.

The tuple is the Arrow C Data Interface boundary; it is not a geometry result
that can be passed to [`from_arrow`][gometry.from_arrow] directly. A consumer receives the object
implementing `__arrow_c_array__`, calls it, and owns the two capsules for the
duration of the import.

```python exec="on" source="block" result="text"
import gometry as gm

values = gm.points([21.0, 22.0], [52.0, 52.5], crs=4326)
schema_capsule, array_capsule = values.__arrow_c_array__()
print('schema:', 'arrow_schema' in repr(schema_capsule))
print('array:', 'arrow_array' in repr(array_capsule))
```

## Cell IDs and geohash tokens

Install `gometry[arrow]` to use PyArrow with the cell-array protocol. Numeric
cell arrays become `uint64` columns; geohash cells become UTF-8 token columns.
Missing `CellArray` entries remain Arrow nulls.

```python exec="on" source="block" result="text"
import pyarrow as pa

import gometry as gm

cell = gm.H3Cell(13.4, 52.5, resolution=7)
cells = gm.CellArray([cell, None])
ids = pa.array(cells)
restored = gm.CellArray(ids.to_pylist(), type=gm.H3Cell)
tokens = pa.array(gm.CellArray([gm.GeohashCell('u33d')]))

print("ID column:", ids.type, ids.to_pylist())
print("restored grid:", restored.grid)
print("geohash tokens:", tokens.type, tokens.to_pylist())
```

Contiguous numeric arrays and slices retain their ID buffers through the Arrow
capsules. Gathered selections own a compact ID buffer, and geohash export
encodes token bytes. Arrow schema requests must match the exported primitive
type.

## Native packed layouts vs WKB fallback

Homogeneous **XY / XYZ / XYM / XYZM** point, multipoint, linestring,
multilinestring, polygon, and multipolygon arrays without explicit empty
multipart members use the native GeoArrow coordinate layout with separated
`x`/`y`/`z`/`m` children. Gometry exports may
share immutable gometry-owned buffers with a consumer.

Import does not retain
shared views of foreign memory. PyArrow arrays, chunked arrays, tables, record
batches, and record batch readers are copied through PyArrow.
Other capsule providers use the dependency-free Arrow C interface; their
selected geometry buffers are copied before validation and decoding.

Mixed geometry types, mixed coordinate axes, and geometry
collections fall back to GeoArrow **WKB** encoding to retain their structure. The
PyArrow export path also uses WKB when explicit multipart member slots cannot
be represented by its selected native layout; the Arrow-C nested layout
retains explicit empty line and polygon slots.

### Arrow C capsule trust model

The [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
does not provide general buffer capacities. Providers must obey its exported
structures, pointer tables, readable spans, capsule names, and release-callback
rules; a provider that forges pointers or capacities is outside this boundary.
Native producers must keep exported memory valid and its addressed contents
quiescent until release; PyArrow objects must not be modified during
`from_arrow`. Copying a foreign span gives gometry ownership after capture but
does not synchronize a producer that writes during the copy.

Gometry copies and validates imported geometry buffers, rejecting malformed
schemas, offsets, validity data, encodings, and non-finite coordinates.
Resource budgets and untrusted-input
limits are documented in [Security & untrusted input](../about/security.md).

## CRS as PROJJSON, epoch and exact identity as gometry extensions

The GeoArrow extension metadata writes the CRS as **PROJJSON** — GeoArrow's
recommended encoding, and the one [GeoParquet](https://geoparquet.org/) 1.1
requires when columns land in a lakehouse. Gometry also writes
`gometry:crs_definition`, a namespaced string containing the exact CRS
definition stored by the producer. Its readers restore that exact identity only
after verifying that it describes the standard PROJJSON CRS. External
standard-only metadata continues to use the CRS decoded from PROJJSON or an
authority string.

This extra identity field matters because a standardized CRS can remain
spatially equivalent while its stored representation changes. For example, an
`EPSG:4326` geometry may be represented by a PROJJSON object after an
interchange round trip; gometry's exact CRS equality distinguishes those
definitions. Third-party transformations that discard `gometry:crs_definition`
retain the standard CRS meaning but cannot preserve the producer's exact
definition spelling.

The **coordinate epoch** is **not** part of the GeoArrow extension-types
standard; gometry stores it as an additional JSON member in the same extension
metadata blob so it can survive a columnar round-trip when both sides are
gometry (or another reader that understands this extension). Other producers may
omit `epoch` entirely. WKB/EWKB and GeoJSON still cannot carry epoch — only this
gometry GeoArrow extension field and GeoParquet column metadata do.

## Requested C schemas

The Arrow C array and stream exporters accept a request matching their default
physical schema and reject a different layout. Some PyArrow `DataType` requests
carry only that physical schema and omit the GeoArrow extension metadata; those
exports use the requested physical type. Use `GeometryArray.to_arrow()` when the
result must itself remain a GeoArrow extension array with CRS and epoch metadata.

## Capsule consumers without pyarrow

Pass the gometry object to a consumer that supports the Arrow PyCapsule
protocol and GeoArrow. To import, pass the provider object to `from_arrow`.

Gometry's [`to_polars()`][gometry.GeometryArray.to_polars] /
[`from_polars()`][gometry.from_polars] adapter instead uses a batched WKB/EWKB `Binary`
Series so it remains independent of PyArrow; that boundary is covered in
[DataFrames](dataframes.md). The lonboard adapter passes the geometry array, or
an Arrow-compatible table when attributes are supplied, through the direct
capsule route.

## lonboard — maps in notebooks

Install `gometry[viz]` for interactive maps.
[`GeometryArray._repr_html_`][gometry.GeometryArray] embeds a lonboard preview in
Jupyter when lonboard is available; it uses lonboard's standalone HTML export so the
same preview works wherever notebook HTML is rendered. Without lonboard, it falls
back to an SVG grid preview.

```python exec="on" source="block" result="text"
import gometry as gm

arr = gm.points([21.0, 22.0], [52.0, 52.5], crs=4326)
html = arr._repr_html_()
print("notebook HTML bytes:", len(html) if html else 0)
print("explore available:", callable(gm.explore))
```

[`gm.explore`][gometry.explore] hands lonboard the geometry through its
Arrow-compatible capsule route. Inputs need a CRS and finite bounds. By default it presents
a map where points, lines, and polygons remain distinct and features are inspectable
on hover. Pass lonboard's `scatterplot_kwargs`, `path_kwargs`,
`polygon_kwargs`, or `map_kwargs` to tailor individual settings.

!!! note "Reprojection for display"
    lonboard expects WGS 84 longitude/latitude display coordinates. Arrays whose
    CRS is not equivalent to EPSG:4326 / OGC:CRS84 (including non-WGS84
    geographic frames such as NAD83, and all projected frames) should be
    reprojected with [`to_crs(4326)`][gometry.Geometry.to_crs] before display. The viz helper normalizes
    non-WGS84 geographic CRS the same way as projected ones.

## See also

- [DataFrames](dataframes.md) — pandas / polars / GeoPandas storage boundaries.
- [Text & binary formats](text-formats.md) — WKB/EWKB for database boundaries.
- [NumPy arrays & coordinates](numpy.md) — extracting raw coordinate buffers.
- [Ecosystem & interoperability](index.md) — the partner matrix.
