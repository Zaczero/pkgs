---
description: GeometryArray, CellArray, Groups, missing rows, and the performance doctrine — one Rust pass over a whole column instead of a Python loop.
---

# Arrays & performance

Everything gometry does to one geometry it does to a whole column in a single Rust
pass — no per-row Python loop. This page is the batch surface end to end: the typed
containers ([`GeometryArray`][gometry.GeometryArray], [`CellArray`][gometry.CellArray],
[`Groups`][gometry.Groups]), how vectorized calls broadcast, how missing rows travel,
and the performance doctrine that keeps work in Rust. Read it top to bottom the first
time; the checklist at the end is the reference card afterwards.

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

!!! tip "Rule of thumb"
    If you find yourself writing a Python `for` loop over geometry objects, stop —
    pass a [`GeometryArray`][gometry.GeometryArray] to the same method or function. The
    [performance doctrine](#performance-doctrine) below measures why.

## GeometryArray

[`gm.GeometryArray`][gometry.GeometryArray] builds a packed, typed container of
geometries that every vectorized function consumes. It behaves like a sequence
(indexing, iteration, `len`) while keeping geometry storage in Rust-owned objects rather
than a Python list. It carries a single CRS and, for homogeneous data, a single
`coordinate_axes`:

```python exec="on" source="block" result="text"
import gometry as gm

arr = gm.GeometryArray([
    gm.box(20.0, 51.0, 22.0, 53.0, crs=4326),
    gm.box(29.0, 51.0, 31.0, 53.0, crs=4326),
])
print("len:", len(arr))
print("crs:", arr.crs)
print("coordinate_axes:", arr.coordinate_axes)
print("element types:", arr.geometry_type)
print("combined bounds:", arr.total_bounds)
print("areas (geodesic m^2):", arr.area)

```

Read its state as a property (`arr.area`), call a method (`arr.buffer(d)`), or pass it to
a binary free function (`gm.contains(arr, pts)`) — each runs over the whole batch in one
pass. `GeometryArray.total_bounds` is the combined `(minx, miny, maxx, maxy)` tuple, or
`None` for an empty/fully-empty array.

### Packed constructors

Building geometries one object at a time and stuffing them in a list defeats the point of
vectorization. The packed constructors take parallel coordinate arrays and build a
`GeometryArray` directly. [`gm.points`][gometry.points] accepts optional `z=` and `m=`
arrays, so you can construct XY, XYZ, XYM, or XYZM arrays in one call — the bulk analogue
of `gm.Point(..., z=..., m=...)`:

```python exec="on" source="block" result="text"
import gometry as gm

# 2D points from lon/lat arrays:
pts = gm.points([21.0, 22.0, 23.0], [52.0, 52.5, 53.0], crs=4326)
print("2D:", pts.coordinate_axes, "len", len(pts))

# XYZ points: add a parallel z array (note crs=4979, the 3D WGS84 CRS):
pts_z = gm.points([21.0, 22.0], [52.0, 52.5], z=[100.0, 200.0], crs=4979)
print("XYZ:", pts_z.coordinate_axes)

```

The sibling column factories `gm.line_strings`, `gm.polygons`, and the rest skip the
per-row Python object loop the same way; IO parsers (`from_wkb`, `from_wkt`,
`from_geojson`) also return packed arrays directly.

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
optional visualization extra is installed; otherwise it uses a capped SVG grid, so a
million-row array never renders a million elements:

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

Array methods and binary free functions accept arrays and return arrays. The polygon
below is tested against every point in a single Rust pass, handing back a read-only
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
| Id pairs | `(left, right)` pair of `int64` ndarrays | `join`, `query_pairs` |
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

This is the safety rail. Two non-scalar arrays of different lengths do **not** silently
broadcast into an n×m grid — that is almost always a bug and would allocate a huge
result. gometry raises and points you at the right tool:

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

!!! warning "Many-to-many means index or join, never implicit cross products"
    If you genuinely want every (polygon, point) pair that matches, you want a *spatial
    join*, not broadcasting. `gm.join(polys, points, predicate="contains")` runs a bbox
    prefilter plus exact refine in Rust and returns only the matching pairs — see
    [Indexing & joins](indexing.md). Forcing a full n×m broadcast would compute and store
    comparisons you don't need.

Coming from Shapely? See [Migrating](../migrating/index.md#coming-from-shapely).
Shapely 2.x already follows NumPy broadcasting and raises on incompatible 1-D
shapes (e.g. lengths 2 and 3) rather than a Cartesian product. gometry's
differences that matter here are attaching a **CRS to arrays** (so units are never
ambiguous) and the same strict equal-length / scalar-vs-array broadcast rules with
no silent n×m expansion.

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
[`gm.h3_cells`][gometry.h3_cells], `gm.s2_cells`, and the geohash/tile twins. Coverage
objects expose `.cells` as a sequence of cell objects for keys and joins; hierarchy
transforms on that set may return `CellArray` when the engine can keep a packed id column.
See [Grids](grids.md) for the full cell/coverage surface.

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
    array of query geometries and you get `Groups` — one candidate row per element. See
    [Spatial indexing](indexing.md).

## Missing rows

`None` is a missing row; empty geometry is a real geometry value. `GeometryArray` carries
missing rows natively — the pyarrow/shapely-2 model, with zero overhead when no row is
missing (a dense array stores packed columns without a validity bitmap). This
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
missingness explicitly when the next boundary cannot represent it.

## Performance doctrine

gometry's hot paths are Rust; the way to make code slow is to keep work in Python. These
are the recurring footguns and their fixes, each as a concrete before/after. One principle
runs through all of them: **let one Rust kernel do the batch, and never pay per-geometry
Python overhead.**

### 1. Vectorize instead of looping

The single biggest win. A Python loop calling a scalar method pays interpreter and
object-construction overhead on every iteration; a vectorized call does the whole batch in
one Rust pass.

```python exec="on" source="block" result="text"
import gometry as gm

poly = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
lons = [20.0, 20.5, 21.0, 30.0]
lats = [51.0, 51.5, 52.0, 10.0]

# SLOW: build Python geometry objects, loop, call the scalar method each time.
sample = [gm.Point(lo, la, crs=4326) for lo, la in zip(lons, lats)]
loop_result = [gm.contains(poly, g) for g in sample]

# FAST: one packed array, one vectorized call.
pts = gm.points(lons, lats, crs=4326)
vectorized_result = gm.contains(poly, pts)

print("same answer:", loop_result == vectorized_result.tolist())
print("rows evaluated:", len(vectorized_result))

```

The vectorized call batches work in Rust and avoids per-element Python call
overhead; measure with the [benchmark harness](../about/benchmarks.md) for your
workload rather than assuming a fixed speedup. The gap
*grows* with array size because the fixed Python overhead is amortized away. See
[Benchmarks](../about/benchmarks.md) for measured release evidence.

### 2. Reproject once, then measure planar in a loop

On a **geographic** CRS, every `area`/`length`/`distance` is a geodesic computation on the
ellipsoid — correct, but heavier than planar arithmetic. When you measure the same
geometries many times in a hot loop and want planar speed, reproject **once** to a metric
CRS up front and reuse the projected geometry:

```python exec="on" source="block" result="text"
import gometry as gm

line = gm.LineString([(20.0, 51.0), (21.0, 52.0), (22.0, 51.5)], crs=4326)

# Geographic CRS -> geodesic length (ellipsoidal meters), no projection chosen.
length_m = line.length
midpoint = line.line_interpolate(0.5, normalized=True)
print("length m:", round(length_m), "| midpoint:", midpoint.to_wkt())

# For repeated planar measurement, reproject once and reuse `metric`.
metric = line.to_crs(line.estimate_local_crs())
print("planar length m:", round(metric.length))

```

The CRS is the single metric knob — a geographic geometry measures geodesically, a
projected one measures planar. See the [mental model](../get-started/mental-model.md) for
how units flow from the CRS.

### 3. Prepare geometry for repeated predicates

When you test **one** geometry against **many** others with the same predicate, build a
[`PreparedGeometry`][gometry.PreparedGeometry] once with
[`geom.prepare()`][gometry.Geometry.prepare]. The prepared object keeps
[`geometry`][gometry.PreparedGeometry.geometry] as a handle back to the source.
It precomputes
an internal index of the geometry's edges so each subsequent predicate is much cheaper than
re-analysing the geometry from scratch.

```python exec="on" source="block" result="text"
import gometry as gm

boundary = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
probes = gm.points([21.0, 40.0], [52.0, 10.0], crs=4326)

prepared = boundary.prepare()           # build the edge index once
print(prepared.contains(probes).tolist())  # array path — no Python loop

```

[`PreparedGeometry`][gometry.PreparedGeometry] exposes the full predicate family plus
`contains_xy` and an `explain` plan. Each accepts a single geometry or an array (returning
`bool` or a `bool_` ndarray mask) and reuses one cached prepared spatial/segment
index. Prepare when the
*same* complex geometry is tested against many others; for two arrays, prefer the
vectorized predicates or a [join](indexing.md).

### 4. Index / join instead of nested loops

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
[Indexing & joins](indexing.md). For coarse pre-filtering at scale, a grid cover (see
[Grids](grids.md)) can shrink the candidate set before the exact refine.

### 5. Let the CRS carry the units

The buffer distance is read through the geometry's CRS, so picking the right CRS is both a
correctness and a performance decision. On a **geographic** CRS, `poly.buffer(1000)` is a
thousand *meters* (1 km) through a local projection — not a thousand degrees:

```python exec="on" source="block" result="text"
import gometry as gm

poly = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)

# Geographic CRS: a CRS-aware local-projection meter buffer.
buffered = poly.buffer(1000.0)            # 1000 meters = 1 km
print("geographic 1 km buffer:", buffered.geometry_type)

```

For hot loops that buffer repeatedly, the reproject-once recipe from
[footgun 2](#2-reproject-once-then-measure-planar-in-a-loop) applies unchanged.

The units doctrine — geographic → geodesic meters, projected → native linear units,
CRS-free → coordinate units, `unit='meters'` / `unit='planar'` overrides — lives in the
[mental model](../get-started/mental-model.md). Pick a metric CRS with
[`geom.estimate_local_crs()`][gometry.Geometry.estimate_local_crs] when you want planar
speed.

### 6. Use the right interchange format

For bulk data movement, packed binary beats text by a wide margin. Prefer
[GeoArrow](https://geoarrow.org/) or [WKB](https://www.ogc.org/standard/sfa/)/EWKB over
WKT/[GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946) in hot paths, and use
`quantize` / [`simplify`][gometry.Geometry.simplify] to cut coordinate precision and vertex
count when full precision isn't needed:

```python exec="on" source="block" result="text"
import gometry as gm
p = gm.Point(21.0123456789, 52.0987654321, crs=4326)
print('full:     ', p.to_wkt())
print('quantized:', (p.quantize(4)).to_wkt())

```

Quantizing before serialization improves **compressibility** (and can shorten
WKT/GeoJSON text); fixed-width WKB and GeoArrow coordinate buffers keep the same
byte width and vertex count after quantize alone. **Simplifying** drops redundant
vertices and is what actually shrinks binary payloads. Both trade a controlled
amount of precision for size and speed. See
[Text & binary formats](../ecosystem/text-formats.md) for format selection.

### 7. Prefer `equals_identical` for array value identity

Topological [`equals`][gometry.equals] and coordinate [`equals_exact`][gometry.equals_exact]
stay free functions. For **value identity** (coordinates + CRS + epoch), use
[`equals_identical`][gometry.equals_identical] — the vectorized form of scalar
`==` — so frame differences are `False` rather than raises, and dimensional
empties stay axes-sensitive.

### 8. Columns beat object lists at bulk scale

Dense bulk lanes stay columnar. `array.bounds` returns a read-only `(rows, 4)` `float64`
ndarray (`nan` rows for empty geometries); `bounds[i]` is a length-4 row view.
Materializing row-shaped Python objects (`array.bounds.tolist()` and list-returning
ragged/diagnostic getters) builds one Python object per element — convenient, but the
object churn is the cost floor at 100k+ rows. The ndarray lanes skip it entirely:

- `array.coords` exposes packed coordinate columns as `float64` ndarrays;
  `np.asarray(coords)` materializes an `(N, dims)` matrix. For point arrays, bounds are
  cheaply derived by duplicating each point's X/Y into min/max (shape `(n, 4)`, not the
  coordinate matrix itself).
- `to_arrow()` moves whole arrays as GeoArrow without materializing rows.
- Bulk coordinate lanes return ndarrays too: `gm.crs_transform` hands back one frozen
  row-major `(N, 2)` / `(N, 3)` `float64` matrix; `gm.crs_apply`, `gm.crs_roundtrip`, and the
  `CRS.geodesic*`/`CRS.factors` batches hand back `float64` lanes.
- Homogeneous point, line, and polygon arrays store as packed coordinate columns
  automatically (lines/polygons add CSR offsets), so supported operations run
  over columns without materializing Python geometry rows.

```python exec="on" source="block" result="text"
import numpy as np

import gometry as gm

pts = gm.points(np.arange(5.0), np.arange(5.0) * 2)
xs = pts.coords.x                   # zero-copy X column
ys = pts.coords.y                   # zero-copy Y column
print(xs[:3], ys[:3])

```

### 9. Ingest in bulk, and feed the parser its fastest form

Reading data in is a hot path too. Two rules: **parse a whole batch in one call**, and
**hand the parser the form it reads fastest** — a string/bytes blob it can scan in Rust,
not a pre-exploded Python structure it has to walk.

```python exec="on" source="block" result="text"
import gometry as gm

wkb = [gm.Point(i * 0.01, i * 0.02, crs=4326).to_wkb() for i in range(5)]

# SLOW: one scalar parse per row, in a Python loop.
loop_result = [gm.from_wkb(b) for b in wkb]

# FAST: one batch call — the array is packed in a single Rust pass.
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

destinations = gm.destination(
    geoms,
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

!!! tip "Feed `from_geojson` a string, not `json.loads(...)`"
    `gm.from_geojson(text)` parses the JSON in Rust and skips the discarded `properties`
    entirely. Calling `json.loads` first and passing the resulting `dict` forces a full
    Python object tree to be built and re-walked — markedly slower for the same result.
    Pass the raw `str`/`bytes` (a file's contents, a `polars`/`duckdb` binary column) and
    let the Rust reader do the scan.

| Ingest | Slow form | Fast form |
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

Interop glue is where a fast core quietly bleeds speed: a per-row conversion loop hidden
behind a tidy call. Move whole columns at once.

- **pandas:** `arr.to_pandas()` stores the native array behind a concrete extension dtype;
  `gm.from_pandas()` returns that same `GeometryArray` **zero-copy**.
- **Polars:** `arr.to_polars()` / `gm.from_polars()` encode and decode a whole WKB/EWKB Binary
  Series in native batched calls. The boundary does not require PyArrow.
- **GeoPandas:** `gm.from_geopandas()` / `arr.to_geopandas()` convert a `GeoSeries` in one
  vectorized WKB step. Avoid rebuilding a `GeoSeries` element by element (`[g.wkb for g in
  series]`); that per-row hop into Shapely is the slow path the vectorized helpers exist to
  replace.
- **Arrow:** `array.to_arrow()` and [`gm.from_arrow`][gometry.from_arrow] move whole arrays
  as GeoArrow through the C data interface — no WKB round-trip and no `pyarrow` needed on
  the import side for capsule-producing sources (`polars`, `duckdb`, `nanoarrow`). Prefer it
  over decoding row-by-row through WKB.

!!! tip "A loop in your glue is a loop in your hot path"
    If an interop helper iterates geometries in Python (`for g in series`, a WKB list
    comprehension, a per-row `to_arrow`), it forfeits the Rust batch. Reach for the bulk
    converters, or keep the data on gometry's extension dtype so it never has to convert.

The full round-trip surface — GeoArrow, GeoParquet, dataframe handoff — lives in
[Arrow & interop](../ecosystem/index.md).

## Profile & benchmark your workload

Do not publish speedups from one `time.perf_counter()` loop or a best-of-N sample. Use
three levels of evidence:

- a profiler to find where time goes;
- a small case script for the exact hot path you own;
- `benches/drivers/bench.py` only when you need to compare against the maintained release
  benchmark surface.

For A/B work, make the case script print one float — elapsed seconds for the measured
region — then run the two builds interleaved so machine drift does not favor either side:

```bash
.venv/bin/python benches/drivers/bench_ab.py \
  --a /path/to/baseline/.venv/bin/python \
  --b .venv/bin/python \
  --case benches/cases/case_import_wkb.py \
  --rounds 9 \
  --warmup 2 \
  --seed 20260709 \
  --cpu 1 \
  --json-out /tmp/gometry-import-wkb-ab.json
```

Choose an otherwise idle CPU; a kernel-isolated CPU is best. The harness pins itself and
both children, alternates `A/B` and `B/A` lead order, records the
seed/governor/frequency/affinity, and reports median (p50), IQR, max block time, and
bootstrap median confidence intervals. If it must fall back to a non-isolated CPU, it
marks the run as unsuitable for release evidence.

For release-surface checks, use the bounded harness:

```bash
env -u RUSTC_WRAPPER .venv/bin/python benches/drivers/bench.py --profile smoke
env -u RUSTC_WRAPPER .venv/bin/python benches/drivers/bench.py --profile release --plan-only
```

Keep raw artifacts, record hardware/contention warnings, and treat differences inside the
IQR/bootstrap noise floor as no result. See [Benchmarks](../about/benchmarks.md) for the
current public evidence and interpretation rules.

## Checklist

| Symptom | Fix |
| --- | --- |
| `for geom in ...: gm.contains(geom, other)` | vectorized `gm.contains(array, other)` |
| geodesic metrics in a hot loop | `to_crs` to a metric CRS once, then measure planar |
| one geometry vs many, same predicate | [`geom.prepare()`][gometry.Geometry.prepare] |
| nested loops over two collections | [`gm.SpatialIndex`][gometry.SpatialIndex] / [`gm.join`][gometry.join] |
| want planar `buffer`/`distance` on lon/lat | `to_crs(geom.estimate_local_crs())` first |
| huge WKT/GeoJSON payloads | WKB/EWKB or GeoArrow + `quantize`/[`simplify`][gometry.Geometry.simplify] |
| `for b in blobs: from_wkb(b)` | one batch `from_wkb(blobs)` (also `from_wkt`/`from_geojson`) |
| `from_geojson(json.loads(s))` | `from_geojson(s)` — pass the `str`/`bytes` |
| `GeometryArray([Point(x, y) for ...])` | [`gm.points(xs, ys)`][gometry.points] |
| `[g.wkb for g in geoseries]` / per-row interop | `gm.from_geopandas` / `to_arrow` whole-column |

## See also

- [Mental model](../get-started/mental-model.md) — the CRS-units and candidate-vs-exact doctrines.
- [Spatial indexing & joins](indexing.md) — `Groups` from array queries, index reuse.
- [CRS, units & measurement](crs.md) — projecting for planar speed and metric units.
- [Grids](grids.md) — `CellArray`, coverages, and grid pre-filtering.
- [Arrow & interop](../ecosystem/index.md) — zero-copy GeoArrow / dataframe handoff.
- [NumPy handoff](../ecosystem/numpy.md) — zero-copy numeric lanes.
- [Benchmarks](../about/benchmarks.md) — release evidence and interpretation.
- [API: GeometryArray][gometry.GeometryArray] · [points][gometry.points] · [Groups][gometry.Groups] · [CellArray][gometry.CellArray] · [from_wkb][gometry.from_wkb]
