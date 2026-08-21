---
description: GeometryArray, CellArray, Groups, missing rows, and the batch-result contract.
---

# Arrays & performance

Many gometry operations on one geometry also accept a whole column and keep the
per-row traversal in native code.

The core symmetry is that a scalar call and its array form run the **same kernel** —
only the receiver and the return shape change. State is a property (`geom.area`), a
unary transform or measure is a method (`geom.buffer(d)`, `arr.buffer(d)`), and a binary
relationship is a free function (`gm.contains(a, b)`):

| Call shape | Input | Output |
| --- | --- | --- |
| `geom.area` | scalar `Geometry` | `float` |
| `arr.area` | `GeometryArray` | `float64` ndarray |
| `geom.buffer(d)` | scalar | `Polygon` / `MultiPolygon` |
| `arr.buffer(d)` | array | `GeometryArray[Polygon \| MultiPolygon]` |
| `gm.contains(poly, pt)` | scalar pair | `bool` |
| `gm.contains(poly, pts)` | scalar × array | `bool_` ndarray |
| `idx.query(scalar)` | one geometry | `int64` ndarray |
| `idx.query(arr)` | `GeometryArray` | `Groups[int64]` |

## GeometryArray

[`gm.GeometryArray`][gometry.GeometryArray] builds a packed, typed container of
geometries that every vectorized function consumes. It behaves like a sequence
(indexing, iteration, `len`) while keeping geometry storage in Rust-owned objects rather
than a Python list. It carries one CRS for the column. `coordinate_axes` reports the
layout of each row; `common_coordinate_axes` reports
one shared layout for present rows, or `None` when those layouts differ:

```python exec="on" source="block" result="text"
import gometry as gm

arr = gm.GeometryArray([
    gm.box(20.0, 51.0, 22.0, 53.0, crs=4326),
    gm.box(29.0, 51.0, 31.0, 53.0, crs=4326),
])
print("len:", len(arr))
print("crs:", arr.crs)
print("coordinate_axes:", arr.coordinate_axes)
print("common_coordinate_axes:", arr.common_coordinate_axes)
print("element types:", arr.geometry_type)
print("combined bounds:", arr.total_bounds)
print("areas (geodesic m^2):", arr.area)

```

Read its state as a property (`arr.area`), call a method (`arr.buffer(d)`), or pass it to
a binary free function (`gm.contains(arr, pts)`) — each runs over the whole batch in one
pass. `GeometryArray.total_bounds` is the combined `(minx, miny, maxx, maxy)` tuple, or
`None` for an empty/fully-empty array.

### Packed constructors

The packed constructors take parallel coordinate arrays and build a
`GeometryArray` directly. [`gm.points`][gometry.points] accepts optional `z=` and `m=`
arrays, so you can construct XY, XYZ, XYM, or XYZM arrays in one call — the bulk analogue
of `gm.Point(..., z=..., m=...)`:

```python exec="on" source="block" result="text"
import gometry as gm

# 2D points from lon/lat arrays:
pts = gm.points([21.0, 22.0, 23.0], [52.0, 52.5, 53.0], crs=4326)
print("2D per row:", pts.coordinate_axes, "shared:", pts.common_coordinate_axes, "len", len(pts))

# XYZ points: add a parallel z array (note crs=4979, the 3D WGS84 CRS):
pts_z = gm.points([21.0, 22.0], [52.0, 52.5], z=[100.0, 200.0], crs=4979)
print("XYZ per row:", pts_z.coordinate_axes, "shared:", pts_z.common_coordinate_axes)

```

The sibling column factories `gm.line_strings`, `gm.polygons`, and the rest skip the
per-row Python object loop the same way; IO parsers (`from_wkb`, `from_wkt`,
`from_geojson`) also return packed arrays directly.

### Per-row axes and missing rows

`coordinate_axes` is row-aligned and uses `None` for a missing row.
`common_coordinate_axes` reports one shared layout for formats that require it:

```python exec="on" source="block" result="text"
import gometry as gm

mixed = gm.GeometryArray([
    gm.from_wkt("POINT Z (1 2 3)"),
    None,
    gm.from_wkt("POINT M (1 2 9)"),
])
print("per row:", mixed.coordinate_axes)
print("shared:", mixed.common_coordinate_axes)
print("missing:", mixed.is_missing.tolist())
```

### Indexing, slicing, and element types

Indexing with an integer returns a single typed `Geometry`; **slicing** returns a new
`GeometryArray`. Iteration yields typed elements:

```python exec="on" source="block" result="text"
import gometry as gm

arr = gm.GeometryArray([gm.Point(i, i) for i in range(5)])
print("element:", type(arr[0]).__name__, arr[0].x)         # Point
print("slice:  ", type(arr[1:4]).__name__, len(arr[1:4]))  # GeometryArray of 3
print("strided:", [g.x for g in arr[::2]])

```

The container is **generic over its element type**: `gm.points(...)` and
`gm.GeometryArray([pt, ...])` are typed `GeometryArray[Point]`, so a type checker knows
`arr[0]` is a `Point` and `for g in arr` yields `Point`. Heterogeneous arrays widen to
`GeometryArray[Geometry]`. Operations narrow too: `arr.buffer(...)` is a
`GeometryArray[Polygon | MultiPolygon]`, `arr.area` is a `float64` ndarray while
`geom.area` is `float`, and so on across the whole surface.

The public classes and cross-grid protocols can be used directly in your own
annotations. Accepted string tokens are already narrowed in every gometry signature,
so IDEs and type checkers flag misspellings at the call site:

```python exec="on" source="block" result="text"
import gometry as gm
def cell_area(cell: gm.Cell) -> float:
    return cell.area

print(round(cell_area(gm.H3Cell("871fb4662ffffff"))))

```

### NumPy bridge and preview

[`to_numpy`][gometry.GeometryArray.to_numpy] materializes an `object` ndarray of geometry
handles; [`gm.GeometryArray`][gometry.GeometryArray] is the inverse for object ndarrays
and `__geo_interface__` rows. Coordinate columns live on `.coords` (`.x`/`.y`/`.z`/`.m`
are read-only `float64` ndarrays):

```python exec="on" source="block" result="text"
import numpy as np
import gometry as gm

arr = gm.points([0.0, 1.0, 2.0], [0.0, 1.0, 2.0])
obj = arr.to_numpy()
back = gm.GeometryArray(obj)
print("round-trip len:", len(back), "| x column:", np.asarray(back.coords.x))

```

Like scalar geometries, a geographic array previews itself with lonboard when the
optional visualization extra is installed; otherwise it uses a capped SVG grid,
so the preview remains bounded for large arrays:

```python exec="on" html="true"
import gometry as gm

arr = gm.GeometryArray([
    gm.box(0, 0, 2, 2, crs=4326),
    gm.Point(1, 1, crs=4326).buffer(10_000),
    gm.LineString([(0, 0), (1, 2), (2, 0)], crs=4326),
    gm.Polygon([(0, 0), (2, 0), (1, 2)], crs=4326),
], crs=4326)
print(arr._repr_html_())

```

## Vectorized calls & broadcasting

Array methods and binary free functions accept arrays and return arrays. A scalar
polygon tested against an array of points is evaluated in one native pass, returning a read-only
`numpy.ndarray` that shares Rust-owned storage with no copy:

```python exec="on" source="block" result="text"
import gometry as gm

poly = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
points = gm.points([21.0, 30.0, 21.5], [52.0, 52.0, 52.1], crs=4326)

mask = gm.contains(poly, points)   # -> ndarray[bool]
print(type(mask).__name__, mask)

```

Vectorized bulk lanes follow a fixed return contract:

| Kind | Return shape | Examples |
| --- | --- | --- |
| Numbers | `float64` / `int64` / `uint64` ndarray | `area`, `distance`, `line_locate`, spatial keys |
| Masks | `bool_` ndarray | `contains`, `intersects`, `within`, … |
| Bounds | `(n, 4)` `float64` ndarray | `bounds` rows (`nan` for empty) |
| Id pairs | `(left, right)` pair of `int64` ndarrays | `join`, `self_join` |
| Ragged matches | [`Groups`][gometry.Groups] (CSR) | array-form `index.query` |
| Geometries | [`GeometryArray`][gometry.GeometryArray] | `buffer`, `to_crs`, …; overlays via `&` / `|` |
| Coordinates | [`Coordinates`][gometry.Coordinates] view or [`gm.get_coordinates`][gometry.get_coordinates] | `geom.coords`, packed point columns |
| Witness pairs | `(left, right)` pair of point `GeometryArray` columns | `nearest_points` |

Text/bytes and ragged diagnostic lanes stay ordinary Python lists because they have no
single dense ndarray representation.

gometry adopts NumPy's length-preserving
[broadcasting](https://numpy.org/doc/stable/user/basics.broadcasting.html) cases and
refuses everything else — there is no implicit Cartesian product. Exactly three operand
shapes are allowed:

| Left | Right | Result | Meaning |
| --- | --- | --- | --- |
| scalar | array (n) | array (n) | test the scalar against each element |
| array (n) | scalar | array (n) | test each element against the scalar |
| array (n) | array (n) | array (n) | **pairwise** — element *i* vs element *i* |
| array (n) | array (m), n≠m | **`GeometryError`** | refused — use [`index`](indexing.md)/[`join`](indexing.md) |

### Scalar × array

```python exec="on" source="block" result="text"
import gometry as gm

poly = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
points = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
print(gm.contains(poly, points))   # one polygon vs each point

```

### Equal-length pairwise

When both sides are arrays of the same length, the operation is **pairwise**, not a
cross product: result *i* compares left *i* with right *i*.

```python exec="on" source="block" result="text"
import gometry as gm

polys = gm.GeometryArray([
    gm.box(20.0, 51.0, 22.0, 53.0, crs=4326),
    gm.box(29.0, 51.0, 31.0, 53.0, crs=4326),
])
points = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)

# polys[0] vs points[0], polys[1] vs points[1]
print("pairwise contains:", gm.contains(polys, points))

```

### Mismatched lengths are refused

Two non-scalar arrays of different lengths raise instead of broadcasting into an
n×m grid. The exception identifies the mismatch:

```python exec="on" source="block" result="text"
import gometry as gm

polys = gm.GeometryArray([
    gm.box(20.0, 51.0, 22.0, 53.0, crs=4326),
    gm.box(29.0, 51.0, 31.0, 53.0, crs=4326),
])
points = gm.points([21.0, 30.0, 40.0], [52.0, 52.0, 52.0], crs=4326)  # length 3

try:
    gm.contains(polys, points)   # 2 vs 3 -> error
except gm.GeometryError as e:
    print("GeometryError:", e)

```

!!! note "Many-to-many uses an index or join"
    `gm.join(polys, points, predicate="contains")` runs a bbox prefilter plus
    exact refine in Rust and returns matching pairs. See [Indexing & joins](indexing.md).

Coming from Shapely? See [Migrating](../migrating/index.md#coming-from-shapely).
Shapely 2.x already follows NumPy broadcasting and raises on incompatible 1-D
shapes (e.g. lengths 2 and 3) rather than a Cartesian product. gometry's
differences that matter here are attaching a **CRS to arrays** and using the same
strict equal-length / scalar-vs-array broadcast rules without n×m expansion.

## CellArray

[`CellArray`][gometry.CellArray] is a homogeneous sequence of typed cells. Hierarchy ops
on a cell object return one — `H3Cell.children()`, `.neighbors`, and the S2/geohash/tile
mirrors materialize `CellArray[H3Cell]` (or the matching cell type) instead of a bare
Python list. Accessors mirror the scalar cell name: `cells.token`, `cells.center`, and
`cells.area` stay singular while returning columnar/list results. Cell geometry is
explicit through accessors such as `.polygon`, `.center`, and `.to_polygon()`; cells do
not carry an OGC `geometry_type` because they are grid identifiers, not geometry objects.

Construct one directly from a non-empty homogeneous iterable of cell objects and gometry
infers the grid type: `gm.CellArray([cell_a, cell_b])`. Raw integer IDs, string tokens,
NumPy arrays, buffers, and empty inputs have no reliable type evidence, so make the grid
explicit with `type=gm.H3Cell` (or the corresponding S2/geohash/tile class). Mixed
cell classes and an explicit `type` that disagrees with a cell object are errors.
Construction preserves input order and duplicates; `CellArray` is a sequence, not a set.
Bulk cell factories preserve missing input rows as `None`. Missing rows are
included by slicing, reversing, and fancy indexing; `None in cells`,
`cells.count(None)`, and `cells.index(None)` follow normal Python sequence
semantics, while cell/id searches ignore missing rows. Pickles contain only
public cell identities plus the logical missing mask, never an internal id
sentinel.

Row-preserving `CellArray` operations keep one result per logical input row:
hierarchy predicates return `False` for either missing side, geometry accessors
retain the geometry-array missing mask, numeric results use `NaN`, `parent`
retains its mask, ragged hierarchy results contain an empty row, and `token`
contains `None`. Set-like `compact`, `uncompact`, and `to_polygon`
operate on present cells only.

```python exec="on" source="block" result="text"
import gometry as gm

cell = gm.H3Cell(gm.Point(21.0, 52.0, crs=4326), resolution=6)
children = cell.children()
print(type(children).__name__, len(children), type(children[0]).__name__)
print("ids:", [c.id for c in children[:3]])

```

H3 topological vertices and directed edges are not cells: `H3Cell.vertices` returns
`H3VertexArray` and `H3Cell.edges` returns `H3EdgeArray`. Those arrays keep the same
sequence and uint64 id-column ergonomics but only expose valid topology operations such
as `vertices.point`, `edges.origin`, `edges.destination`, `edges.line`, and
`edges.length`. Bulk point→cell construction uses the prefixed plural builders —
[`gm.h3_cells`][gometry.h3_cells], `gm.s2_cells`, and the geohash/tile twins. Grid cover
factories return `CellArray` for one geometry and `Groups` of `CellArray` for geometry
arrays; hierarchy transforms on that set may return `CellArray` when the engine can keep
a packed id column. For exact membership, keep the source geometry separately and use
free predicates such as `gm.covers` or `gm.contains`.

## Groups (ragged CSR)

Some operations return **one variable-length result per input row** — spatial index
queries against an array of probes, or multi-hit predicate scans. A flat `list[list[int]]`
would work in Python but loses ndarray zero-copy sharing. [`Groups`][gometry.Groups]
stores those results in **CSR form**: a single `int64` `.values` vector plus `.offsets`
that bound each row.

```python exec="on" source="block" result="text"
import gometry as gm

pts = gm.points([21.0, 30.0, 40.0], [52.0, 52.0, 52.0], crs=4326)
zones = gm.GeometryArray([
    gm.box(20.0, 51.0, 22.0, 53.0, crs=4326),
    gm.box(29.0, 51.0, 31.0, 53.0, crs=4326),
])
idx = gm.SpatialIndex(pts)
groups = idx.query(zones, predicate="contains")

print(type(groups).__name__, "rows:", len(groups))
print("row 0 ids:", groups[0].tolist())
print("row 1 ids:", groups[1].tolist())
print("offsets:", groups.offsets.tolist())

```

`Groups` is generic over the value type: index hits store `int64` rows (read-only
`.values` / `.offsets` ndarray views over CSR storage); geometry-valued ragged
results wrap per-row [`GeometryArray`][gometry.GeometryArray] slices.

!!! tip "Scalar vs array index query"
    A scalar `idx.query(geom, predicate=...)` returns a dense `int64` ndarray. Pass an
    array of query geometries and you get `Groups` — one result row per element. See
    [Spatial indexing](indexing.md).

## Missing rows

`None` is a missing row; empty geometry is a real geometry value. `GeometryArray` carries
missing rows natively using a validity model. A dense array stores packed columns
without a missing-row marker when no row is missing. This
distinction is preserved across arrays, dataframe handoff, Arrow, GeoParquet, and most
vectorized operations. Batch ingest (`from_wkt` / `from_wkb` / `from_geojson` /
`GeometryArray([...])`) keeps native GeoArrow typing when some rows are missing rather
than downgrading the whole column to WKB:

```python exec="on" source="block" result="text"
import gometry as gm

geoms = gm.GeometryArray([gm.box(0, 0, 1, 1), None, gm.box(1, 1, 2, 2)])
window = gm.box(-1, -1, 2, 2)

print("missing:", geoms.is_missing.tolist())
print("wkt:    ", geoms.to_wkt())                       # exports emit None
print("area:   ", geoms.area.tolist())                  # measures -> NaN
print("contains:", gm.contains(window, geoms).tolist()) # predicates -> False
print("buffer: ", geoms.buffer(0.5).is_missing.tolist())# geometry ops propagate
print("union:  ", geoms.union_all().geometry_type)       # aggregates skip
print("drop:   ", geoms.drop_missing())
print("fill:   ", geoms.fill_missing(gm.box(9, 9, 10, 10)).to_wkt())

```

One rule set everywhere: **predicates → `False`, measures → `NaN`, geometry results stay
missing, aggregates skip, exports emit `None`.** Fixed-width fact lanes that cannot hold
`None` keep their ndarray shape with **sentinels** — for example bounds use `NaN`,
topological dimensions use `-1`, and some curve-key lanes use `0`. Those sentinels can
collide with valid values, so **`is_missing` is authoritative**. Flattening and
explode-style operations (`parts`, `rings`, triangulations, sampled points) skip missing
rows entirely.

Missing rows enter through any constructor or importer (`GeometryArray([g, None])`,
`from_wkt`/`from_wkb` with `None` entries, RFC 7946 `"geometry": null` features, Arrow
validity bitmaps, geopandas/pandas/polars nulls) and survive every row-aligned operation,
slice, fancy indexing (`arr[[0, 2]]`), pickle, and Arrow round-trip.

!!! note "Coordinates flatten present rows"
    `arr.coords` and [`gm.get_coordinates(arr)`][gometry.get_coordinates] are vertex
    streams, not row-aligned arrays. Missing rows contribute no vertices. Use
    `return_index=True` with `get_coordinates` when you need to map each coordinate back
    to its source row, or call `drop_missing()` first when a dense coordinate-only
    workflow is clearer.

`drop_missing()` and `fill_missing(value)` convert back to dense. `fill_missing` accepts
one scalar geometry or a row-aligned `GeometryArray` fill source; indexes and
`GeometryCollection` construction require dense input (their error says so). Drop or fill
Missingness must be dropped or filled when the next boundary cannot represent it.

## Batching choices

Gometry keeps geometry kernels in Rust and exposes packed, vectorized entry points.

### Vectorize instead of looping

A Python loop calls the scalar API once per object; a vectorized call accepts the
whole batch and returns one aligned result.

```python exec="on" source="block" result="text"
import gometry as gm

poly = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
lons = [20.0, 20.5, 21.0, 30.0]
lats = [51.0, 51.5, 52.0, 10.0]

# Per-item form: build Python geometry objects and call the scalar API.
sample = [gm.Point(lo, la, crs=4326) for lo, la in zip(lons, lats)]
loop_result = [gm.contains(poly, g) for g in sample]

# Batched form: one packed array and one vectorized call.
pts = gm.points(lons, lats, crs=4326)
vectorized_result = gm.contains(poly, pts)

print("same answer:", loop_result == vectorized_result.tolist())
print("rows evaluated:", len(vectorized_result))

```

### Prepare geometry for repeated predicates

When you test **one** geometry against **many** others with the same predicate, build a
[`PreparedGeometry`][gometry.PreparedGeometry] once with
[`geom.prepare()`][gometry.Geometry.prepare]. The prepared object keeps
[`geometry`][gometry.PreparedGeometry.geometry] as a handle back to the source.
It precomputes
an edge index so each subsequent predicate reuses prepared geometry state instead of
re-analysing the geometry from scratch.

```python exec="on" source="block" result="text"
import gometry as gm

boundary = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
probes = gm.points([21.0, 40.0], [52.0, 10.0], crs=4326)

prepared = boundary.prepare()           # build the edge index once
print(gm.contains(prepared, probes).tolist())  # array path — no Python loop

```

[`PreparedGeometry`][gometry.PreparedGeometry] is an operand for the free predicate
family and `contains_xy`; pass it as either argument to `gm.*` (including XY
predicates), never as a method receiver. These functions accept a single geometry or
an array (returning `bool` or a `bool_` ndarray mask) and reuse one cached prepared
spatial/segment index. Prepare when the
*same* complex geometry is tested against many others; for two arrays, prefer the
vectorized predicates or a [join](indexing.md).

### Index or join for many-to-many work

A nested loop over two collections is O(N·M) and allocates per test. The
[`SpatialIndex`][gometry.SpatialIndex] and [`gm.join`][gometry.join] replace it with a
bbox prefilter plus exact refine in Rust. Cost depends on index construction, query count,
bounding-box selectivity, candidate count, and predicate complexity; adversarial
overlapping boxes can still approach a Cartesian product.

```python exec="on" source="block" result="text"
import gometry as gm

points = gm.points([21.0, 30.0, 21.5], [52.0, 52.0, 52.1], crs=4326)
areas = gm.GeometryArray([
    gm.box(20.0, 51.0, 22.0, 53.0, crs=4326),
    gm.box(29.0, 51.0, 31.0, 53.0, crs=4326),
])

# Instead of: for a in areas: for p in points: gm.contains(a, p)
pairs = gm.join(points, areas, predicate="within")
print("matched pairs:", pairs)

```

For repeated queries against a fixed set, build the index once and reuse it; see
[Indexing & joins](indexing.md).

### Prefer `equals_identical` for array value identity

For array **value identity**, use [`equals_identical`][gometry.equals_identical]. It is
the vectorized form of scalar `==`; frame differences return `False`, and dimensional
empties remain axes-sensitive.

### Ingest in bulk, and use the direct parser form

Batch readers accept a whole iterable in one call. Direct string or bytes input is scanned
in Rust instead of being expanded into a Python object tree first.

```python exec="on" source="block" result="text"
import gometry as gm

wkb = [gm.Point(i * 0.01, i * 0.02, crs=4326).to_wkb() for i in range(5)]

# Per-item form: one scalar parse per row, in a Python loop.
loop_result = [gm.from_wkb(b) for b in wkb]

# Batched form: one call returning a packed array.
batch_result = gm.from_wkb(wkb)

print("same rows:", [g.to_wkt() for g in loop_result] == batch_result.to_wkt())
print("batch result:", batch_result)

```

The same holds for every reader. `from_wkb`/`from_wkt`/`from_geojson` all take an
**iterable** and return a packed [`GeometryArray`][gometry.GeometryArray] in one pass; the
geodesic/CRS batches (`destination`, `point_between`, `CRS.geodesic*`,
`gm.crs_transform_bounds`) are vectorized the same way, so a whole column of ingest
and downstream transform stays in Rust:

```python exec="on" source="block" result="text"
import numpy as np

import gometry as gm

wkb = gm.points([0.0, 1.0, 2.0], [50.0, 51.0, 52.0], crs=4326).to_wkb(
    include_srid=True
)
geoms = gm.from_wkb(wkb)

destinations = geoms.destination(
    np.array([90.0, 180.0, 270.0]),
    np.array([1000.0, 2000.0, 3000.0]),
)

bounds = gm.crs_transform_bounds(
    4326,
    3857,
    [(0, 50, 1, 51), (1, 51, 2, 52)],
)

print("ingested:", geoms)
print("destinations:", destinations.geometry_type)
print("bounds shape:", bounds.shape)

```

For very large files, stream at the file/database layer into bounded batches, then call
gometry once per batch — that keeps memory bounded without falling back to per-row parser
overhead.

!!! note "Pass raw GeoJSON to the parser"
    `gm.from_geojson(text)` parses the JSON in Rust and skips the discarded `properties`
    entirely. Calling `json.loads` first and passing the resulting `dict` forces a full
    Python object tree to be built and re-walked before the native parser sees it.
    Pass the raw `str`/`bytes` (a file's contents, a `polars`/`duckdb` binary column) and
    let the Rust reader do the scan.

| Ingest | Per-item or expanded form | Batched/direct form |
| --- | --- | --- |
| GeoJSON | `from_geojson(json.loads(s))` | `from_geojson(s)` — `str`/`bytes` |
| Many points | `GeometryArray([Point(x, y) for ...])` | `gm.points(xs, ys)` |
| WKB/WKT | `[from_wkb(b) for b in blobs]` | `from_wkb(blobs)` — one batch |
| Features + attributes | `json.loads` + a Python feature loop | [`gm.from_features(text)`][gometry.from_features] |

When you need the per-feature `properties`/`id` alongside geometry,
[`gm.from_features`][gometry.from_features] parses the whole FeatureCollection in Rust and
hands back a validated `Features(geometries, properties, ids)` record — one crossing instead of
a Python loop.

## Crossing the dataframe / Arrow boundary

Per-row interop conversion changes a columnar boundary into Python object work. Bulk
converters accept whole columns.

- **pandas:** `arr.to_pandas()` stores the native array behind a concrete extension dtype;
  `gm.from_pandas()` returns that same `GeometryArray` **zero-copy**.
- **Polars:** `arr.to_polars()` / `gm.from_polars()` encode and decode a whole WKB/EWKB Binary
  Series in native batched calls. The boundary does not require PyArrow.
- **GeoPandas:** `gm.from_geopandas()` / `arr.to_geopandas()` convert a `GeoSeries` in one
  vectorized WKB step. Avoid rebuilding a `GeoSeries` element by element (`[g.wkb for g in
  series]`); that per-row hop into Shapely is the per-item path the vectorized helpers exist to
  replace.
- **Arrow:** `array.to_arrow()` and [`gm.from_arrow`][gometry.from_arrow] move whole arrays
  as GeoArrow through the C data interface — no WKB round-trip and no `pyarrow` needed on
  the import side for capsule-producing sources (`polars`, `duckdb`, `nanoarrow`). It avoids
  decoding row-by-row through WKB.

## See also

- [Mental model](../get-started/mental-model.md) — the three operation models.
- [Spatial indexing & joins](indexing.md) — `Groups` from array queries, index reuse.
- [CRS, units & measurement](crs.md) — projecting for planar speed and metric units.
- [Grids](grids.md) — `CellArray`, coverages, and grid pre-filtering.
- [Arrow & interop](../ecosystem/index.md) — zero-copy GeoArrow / dataframe handoff.
- [NumPy handoff](../ecosystem/numpy.md) — zero-copy numeric lanes.
- [Benchmarks](../about/benchmarks.md) — current release status.
- [API: GeometryArray][gometry.GeometryArray] · [points][gometry.points] · [Groups][gometry.Groups] · [CellArray][gometry.CellArray] · [from_wkb][gometry.from_wkb]
