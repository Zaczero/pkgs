---
description: gometry public API design rules for canonical names, immutable values, pooled results, Groups, CRS, spatial-index candidates, exact queries, Python protocols, and GeoArrow.
---

# Design principles

gometry exposes a typed Python facade over Rust-owned geometry semantics. Its
public API follows nine rules for operation placement, input and result naming,
typing, and integration with Python and its data ecosystem.

## API design rules

### 1. Place work on the value or relationship it describes

Facts about one value are properties, unary geometry work is a method, and
relationships between two geometries are free functions. This makes an
operation's owner predictable. See the [`Geometry.area` property][gometry.Geometry.area],
[`Geometry.buffer` method][gometry.Geometry.buffer], and
[`distance` function][gometry.distance].

```python exec="on" source="block" result="text"
import gometry as gm

point = gm.Point(1, 2)
print(point.area, type(point.buffer(1)).__name__, gm.distance(point, gm.Point(4, 6)))

```

The example has no CRS, so its distance and buffer use raw coordinate units.

### 2. Name operations by their effect on immutable values

Copy-returning state replacements use `with_<field>` names. `to_<x>` names
transform or serialize, `from_<x>` names parse, and `force_<dim>` names project
coordinate dimensions. The name tells callers whether an operation constructs
from an encoding, transforms a value, or returns a changed value. See
[`Geometry.to_crs`][gometry.Geometry.to_crs],
[`Geometry.to_wkt`][gometry.Geometry.to_wkt], [`from_wkt`][gometry.from_wkt],
and [`Geometry.force_2d`][gometry.Geometry.force_2d].

```python exec="on" source="block" result="text"
import gometry as gm

wire = gm.Point(1, 2).to_wkt()
parsed = gm.from_wkt(wire)
projected = gm.Point(-73.9, 40.7, crs=4326).to_crs(3857)
flattened = gm.Point(1, 2, z=3).force_2d()
print(parsed.to_wkt(), projected.crs.to_epsg(), flattened.to_wkt())

```

### 3. Use `_all` for pooled many-to-one operations

An `<op>_all` function pools many input geometries into one aggregate result;
row-wise array methods keep results associated with their source rows. This
distinction makes aggregation explicit. See [`union_all`][gometry.union_all],
[`polygonize_all`][gometry.polygonize_all], and
[`coverage_union_all`][gometry.coverage_union_all].

```python exec="on" source="block" result="text"
import gometry as gm

points = [gm.Point(0, 0), gm.Point(1, 1)]
print(gm.union_all(points).to_wkt())

```

### 4. Return `Groups` when each input row can produce a different number of results

Ragged array operations return one [`Groups`][gometry.Groups] entry per source
row, preserving row association; a missing input row has an empty group. Callers
can use each result without guessing which input produced it. See
[`split`][gometry.split] and the [`Groups` reference][gometry.Groups].

```python exec="on" source="block" result="text"
import gometry as gm

line = gm.LineString([(0, 0), (2, 0)])
groups = gm.split(gm.GeometryArray([line, None, line]), gm.Point(1, 0))
print(type(groups).__name__, groups.offsets.tolist(), groups[1].to_wkt())

```

The cutter lies exactly on these CRS-free lines, and the default split
tolerance is zero.

### 5. Select an operation's representation or added output with a typed keyword

A representation, output, or algorithm choice that belongs to one operation is
a keyword option on that operation, not a second operation name. Literal
overloads let type checkers narrow results for explicit choices. This keeps
related behavior discoverable together. See
[`CRS.to_projjson`][gometry.CRS.to_projjson],
[`Geometry.voronoi`][gometry.Geometry.voronoi],
[`distance`][gometry.distance], and
[`polygonize_all`][gometry.polygonize_all].

```python exec="on" source="block" result="text"
import gometry as gm

mapping = gm.CRS(4326).to_projjson(as_dict=True)
print(type(mapping).__name__, mapping["type"])

```

### 6. Keep supported input forms under one operation name

When an operation accepts scalar and batch inputs, overloads describe those
forms under the same name and preserve the corresponding result shape.
Coordinate-pair probes are keyword overloads of `contains` and `intersects`
(`x=` and `y=`), not separate operations. Batch
operations are first-class and are designed to keep per-row work in native code,
avoiding accidental Python loops, object churn, and hidden copies; scalar calls
are convenient forms of the same operation. Missing values admitted in a batch
remain `None` rows. Callers can use the form that matches their data without
converting it to a separate API. See
[`from_wkt`][gometry.from_wkt] and the
[array result contract](../guide/arrays.md).

```python exec="on" source="block" result="text"
import gometry as gm

scalar = gm.from_wkt("POINT (1 2)")
batch = gm.from_wkt(["POINT (1 2)", None])
print(type(scalar).__name__, type(batch).__name__, batch.to_wkt())

```

### 7. Preserve known facts in public types

Annotations retain geometry kind, nullability, and scalar or container result
shape when the inputs establish them; they do not promise inputs that runtime
admission rejects. Precise types avoid unnecessary checks and make invalid
calls visible to type checkers. See [`GeometryArray`][gometry.GeometryArray]
and its [`buffer` method][gometry.GeometryArray.buffer].

```python exec="on" source="block" result="text"
import gometry as gm

point = gm.Point(1, 2)
single: gm.Polygon = point.buffer(1)
dense: gm.GeometryArray[gm.Polygon] = gm.GeometryArray([point]).buffer(1)
nullable_input: gm.GeometryArray[gm.Point | None] = gm.GeometryArray([point, None])
nullable: gm.GeometryArray[gm.Polygon | None] = nullable_input.buffer(1)
print(type(single).__name__, type(dense).__name__, nullable.is_missing.tolist())

```

### 8. Prefer one obvious way, with exceptions for distinct capabilities

A second spelling is justified when it adds a distinct capability or is the
natural idiom in its context, not when it merely renames the same operation.
For example, [`SpatialIndex.candidates`][gometry.SpatialIndex.candidates]
returns a bounding-box prefilter, while
[`SpatialIndex.query`][gometry.SpatialIndex.query] refines candidates with an
exact predicate. Keeping them separate makes that difference visible.

```python exec="on" source="block" result="text"
import gometry as gm

index = gm.SpatialIndex([gm.Polygon([(0, 0), (2, 0), (0, 2)])])
point = gm.Point(1.8, 1.8)
print(index.candidates(point).tolist(), index.query(point).tolist())

```

The CRS-free point lies inside the polygon's bounds but outside the triangle.

### 9. Treat Python and ecosystem protocols as public API

Python dunders, standard-library protocols, and NumPy, pandas, Arrow, and Polars
integrations are designed, typed, and tested as part of the public surface.
Protocol support lets geometry values compose with ordinary Python and
ecosystem tools. See [`GeometryArray`][gometry.GeometryArray],
[NumPy arrays & coordinates](../ecosystem/numpy.md), and
[Ecosystem & interoperability](../ecosystem/index.md).

```python exec="on" source="block" result="text"
import numpy as np
import gometry as gm

array = gm.GeometryArray([gm.Point(1, 2), gm.Point(3, 4)])
print(len(array), array[0].to_wkt(), np.asarray(array, dtype=object).dtype)

```

## API structure and domain conventions

Callable geometry classes construct one geometry; lowercase plural free
functions construct a packed [`GeometryArray`][gometry.GeometryArray]. Classes
are called directly, without classmethods. For example,
[`gm.Point`][gometry.Point] constructs one point, while
[`gm.points`][gometry.points] constructs a point array.

Array methods with no scalar counterpart operate on the array as a container or
group rows by aligned keys. Examples include
[`GeometryArray.concat`][gometry.GeometryArray.concat],
[`GeometryArray.drop_missing`][gometry.GeometryArray.drop_missing], and
[`GeometryArray.dissolve`][gometry.GeometryArray.dissolve].

An array method that shares a scalar method's name applies that operation
row-wise; for example, [`GeometryArray.polygonize`][gometry.GeometryArray.polygonize]
polygonizes each row. Pooling all rows' linework into one graph is the separate
[`polygonize_all`][gometry.polygonize_all] operation. Stateful work such as
[`SpatialIndex.query`][gometry.SpatialIndex.query] remains a method on its
typed object.

Geometry values serialize through receiver methods such as
[`Geometry.to_wkb`][gometry.Geometry.to_wkb] and
[`Geometry.to_geojson`][gometry.Geometry.to_geojson]; format readers such as
[`from_wkb`][gometry.from_wkb] and [`from_geojson`][gometry.from_geojson] are
top-level functions. When geometry is combined with feature properties, the
composite result is built with the free
[`to_feature` function][gometry.to_feature]. There is no separate free-function
scalar encoder.

Global function families use the prefixes `crs_`, `h3_`, `s2_`, `geohash_`,
`tile_`, `pluscode_`, and `osm_shortlink_`. Each grid family has free functions
for covers, cell construction, and bounding-cell lookup such as
[`h3_bounding_cell`][gometry.h3_bounding_cell], plus set algebra such as
[`union`][gometry.union], [`intersection`][gometry.intersection], and
[`difference`][gometry.difference], while operations on one cell or
[`CellArray`][gometry.CellArray] remain on that typed value. Grid terms retain
their domain meanings: H3 uses `resolution`, S2 uses `level`, geohash uses
`precision`, and tiles use `zoom`. These prefixes group functions in `gm.`
completion and alphabetical reference lists.

The four cell systems share the `cell`/`cells` operation shape, boundaries, set
operations, and compact/uncompact behavior.

Scalar cell operations such as [`Cell.polygon`][gometry.Cell.polygon] and
[`Cell.children`][gometry.Cell.children] stay on the cell; array operations such
as [`CellArray.compact`][gometry.CellArray.compact] and
[`CellArray.to_polygon`][gometry.CellArray.to_polygon] stay on `CellArray`.
CRS and epoch state are properties; for example,
[`Geometry.crs`][gometry.Geometry.crs] and
[`Geometry.epoch`][gometry.Geometry.epoch].
One-column bulk builders and aggregates name their collection input `values`,
as in [`line_strings`][gometry.line_strings] and [`union_all`][gometry.union_all].

## Canonical entry points

| Concept | Canonical spelling |
|---|---|
| make-valid | [`repair`][gometry.Geometry.repair] |
| spatial index | [`gm.SpatialIndex`][gometry.SpatialIndex] |
| spatial join | [`gm.join`][gometry.join] |
| polyfill | [`gm.h3_cover(geom, ...)`][gometry.h3_cover] |
| geodesy | [`gm.distance(a, b)`][gometry.distance] / [`gm.bearing(a, b)`][gometry.bearing] / [`point.destination(...)`][gometry.Point.destination] / [`gm.point_between(a, b, d)`][gometry.point_between] on points in a geographic CRS, which selects the ellipsoid |
| CRS transform | [`geom.to_crs`][gometry.Geometry.to_crs] / [`gm.crs_transform`][gometry.crs_transform] |
| dissolve / unary union | [`gm.union_all`][gometry.union_all] |

[`gm.Point`][gometry.Point] accepts `crs=4326` to make geographic axis order
explicit while storing coordinates as X/Y. Geometry GeoJSON readers use
[`gm.from_geojson`][gometry.from_geojson]; feature properties and IDs are
available through [`gm.from_geojson(..., features=True)`][gometry.from_geojson].

The GeoPandas `clip(mask)` task is
[`gm.intersection`][gometry.intersection], with
[`Geometry.clip_by_rect`][gometry.Geometry.clip_by_rect] for the rectangular
case; `explode` corresponds to [`GeometryArray.parts`][gometry.GeometryArray.parts].
Multipart traversal uses `parts`, tile corners come from `tile.polygon.bounds`,
and geohash expansion is `[cell, *cell.neighbors]`. Recursive
`GeometryCollection.flatten()` and `Tile.ul`/`Tile.lr` accessors are not part of
the public surface.
Geometry clustering and scattered-data interpolation belong to specialized
analysis packages; gometry exposes coordinates through
[`Geometry.coords`][gometry.Geometry.coords] and NumPy conversion.

Three domain-specific choices are deliberate: [`dwithin`][gometry.dwithin]
uses the standard distance-within predicate name; NaN X/Y coordinates are
invalid geometry input rather than missing rows; and [`CellArray.to_polygon`][gometry.CellArray.to_polygon]
can return a [`MultiPolygon`][gometry.MultiPolygon] when the covered cells are
disjoint.

[`Geometry.normalize`][gometry.Geometry.normalize] returns a canonical form:
the lexicographically smallest equivalent presentation (pointwise coordinates,
then shorter-first; parts ascending; open lines by the smaller direction;
closed lines by the smallest rotation and direction; polygon rings begin at
their minimum vertex under [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946)
winding, with exterior rings counterclockwise and holes clockwise). This form
need not be textually equivalent to Shapely/GEOS `normalize()` to belong to the
same equivalence class.

Bing quadkeys are exposed as `tile.token`; slippy-map zoom is `tile.zoom`.

**Equality has three distinct meanings.**
[`equals(a, b)`][gometry.equals] tests topological equality;
[`equals_exact(a, b, tolerance)`][gometry.equals_exact] compares coordinates
within a tolerance and requires compatible frames; and
[`equals_identical(a, b)`][gometry.equals_identical] tests value identity,
including CRS and epoch. Scalar `geom == other` and elementwise pandas
`Series == Series` use `equals_identical` semantics.
[`GeometryArray`][gometry.GeometryArray] compares whole arrays as
values, returning one `bool`; geometry arrays remain hashable and usable as
dictionary keys. Use `equals_identical(left, right)` for elementwise geometry
comparison.

Geometry values use Python's established protocols: `a & b`, `str(geom)`,
`format(geom, '.2f')`, `bool(geom)`, and class patterns. WKB serialization
requires choosing a flavor and deciding whether to carry CRS information, so
use [`Geometry.to_wkb`][gometry.Geometry.to_wkb].
There is no `bytes(geom)` because WKB flavor and CRS handling must be explicit.
[`Geometry.quantize`][gometry.Geometry.quantize] rounds coordinates, so
`round(geom, n)` would conflate a geometry with a numeric value. Cell-set
operations are distinct from exact geometry algebra; use functions such as
[`gm.s2_union`][gometry.s2_union] instead of ambiguous cell-set operator
aliases. Empty geometries are ordinary values rather than interned singletons.
`CellArray` slices return another
[`CellArray`][gometry.CellArray], and [`SpatialIndex`][gometry.SpatialIndex]
implements mapping and iteration protocols.

CRS equality also follows Python's hash contract: `CRS` values compare to
matching input representations such as `4326` and `"EPSG:4326"`, so `CRS` is
unhashable. Use [`CRS.to_authority`][gometry.CRS.to_authority] or
[`CRS.to_epsg`][gometry.CRS.to_epsg] when a dictionary or set key is needed.

## CRS and candidate boundaries

CRS metadata, precision controls, edge models, and coverage rules are explicit
inputs. [`set_crs`][gometry.Geometry.set_crs] declares existing coordinates, [`to_crs`][gometry.Geometry.to_crs] transforms them, and
metric behavior follows the frame described in [CRS, units & measurement](../guide/crs.md).

Spatial indexes expose bounding-box candidates and exact predicate queries; grid
covers return typed cell arrays and exact membership remains a predicate on the
source geometry. The canonical operational descriptions are [Spatial indexing &
joins](../guide/indexing.md) and [Grids & geocodes](../guide/grids.md).

Spatial-index results refer to input row handles, and cell arrays retain their
grid type and identity representation. These meanings stay visible in the
returned array and group types.

Grid cover factories return a [`CellArray`][gometry.CellArray] for scalar
geometry and [`Groups`][gometry.Groups] of `CellArray` for geometry arrays.
Coverage objects and interior/boundary partitions are not public API. Exact
geometry membership uses top-level predicates such as
[`gm.contains(source, probe)`][gometry.contains]. A CRS-free geometry uses raw
coordinate units for metric operations; CRS-aware metric behavior is described
in [CRS, units & measurement](../guide/crs.md).

## Scalar and vectorized forms

Broadcasting accepts scalar/array and equal-length array pairs, and rejects
mismatched non-scalar lengths. See
[Arrays & performance](../guide/arrays.md) for the batch contract.

## NumPy-native bulk outputs

NumPy is a mandatory runtime dependency. Dense numeric results are read-only
`numpy.ndarray`s, geometry results are [`GeometryArray`][gometry.GeometryArray],
and ragged results are [`Groups`][gometry.Groups]. Text and diagnostic outputs
remain lists when no dense shape exists. Coordinate and cell-array details are in
[NumPy arrays & coordinates](../ecosystem/numpy.md) and [Arrays & performance](../guide/arrays.md).

Numeric-id [`CellArray`][gometry.CellArray] values expose
[`to_numpy`][gometry.CellArray.to_numpy] and NumPy's `__array__` protocol;
`CellArray` does not provide a `values` property.

## Errors as API

Exceptions are part of the public surface, designed once: a small hierarchy
rooted at [`GeometryError(ValueError)`][gometry.GeometryError] (so broad `except ValueError` remains
compatible),
with precise classes only where users genuinely catch differently —
[`ParseError`][gometry.ParseError] for malformed serialized input (including cell ids/tokens),
[`CRSMismatchError`][gometry.CRSMismatchError] for frame conflicts, [`TransformError`][gometry.TransformError] for projection
failures, [`InvalidGeometryError`][gometry.InvalidGeometryError] for structural rules, and a dual-base
[`GeometryTypeError(GeometryError, TypeError)`][gometry.GeometryTypeError] for wrong-kind operands (the
`numpy.exceptions.AxisError` pattern). Finer taxonomies (per-format parse
classes, `TokenError`, `TopologyError`, per-engine classes, and a dedicated
`GridError`) are not part of the public hierarchy; the message carries the
remaining detail. Out-of-range depth parameters are ordinary
`GeometryError` value lanes; invalid cell ids/tokens are `ParseError` parse
lanes. Python
protocol failures stay builtin — domain wrong-kind failures use
`GeometryTypeError(GeometryError, TypeError)` (dual-base); pure Python protocol
`TypeError`s stay builtin,
`IndexError`, or `StopIteration` for protocol semantics, and `__format__` /
[`GeometryArray.index`][gometry.GeometryArray.index] keep plain `ValueError` by stdlib convention. Messages
follow one gate-enforced grammar; `Raises` sections name the real class and are
contract-checked, so the docs cannot drift from the raise sites.

## Z and M layouts

XY, XYZ, XYM, and XYZM are supported layouts. Z/M behavior by operation family
and the explicit recovery methods are documented in [Geometry & dimensions](../guide/geometry.md).

WKT, WKB/EWKB, and GeoArrow preserve both ordinates. [`to_geojson()`][gometry.Geometry.to_geojson] raises on M, and `__geo_interface__` raises on M or a
coordinate epoch because GeoJSON has no slots for them. WKB/EWKB preserve M but
not epoch; GeoArrow preserves epoch metadata. WKT/WKB preserve dimensional-empty
axes, while GeoJSON does not. Format details are in [Text & binary
formats](../ecosystem/text-formats.md); GeoArrow's columnar metadata is described
in [Arrow & storage](../ecosystem/arrow.md).

## Overlay linework and point-set presentation

General overlays node their inputs into atomic segments to compute the result,
then dissolve selected linework into maximal [`LineString`s][gometry.LineString],
splitting only at genuine junctions (a node of degree `>= 3`) and never fusing
two arms through one. A line clipped to a covering polygon is one line, and a
line crossed by a *subtracted* line keeps flowing through the crossing vertex.
The public result follows the OGC point set, not the partition used to compute
it. [`line_merge`][gometry.Geometry.line_merge] applies the same degree-two and
junction rules when callers start with independent linework.

When a binary point-set overlay retains a non-empty higher-dimensional
operand—as `union`, `symmetric_difference`, or `difference` with that operand
on the left does—its line and polygon coordinates pass through in their stored
presentation. Ring start, orientation, and collinear vertices are not rewritten,
and absorbed point ordinates do not change the retained operand's Z/M values.

## The Arrow / GeoArrow boundary

The public columnar geometry format is [GeoArrow](https://geoarrow.org/)-compatible:
homogeneous arrays without explicit empty multipart members export separated
`x`/`y`/`z`/`m` children, while mixed types, mixed axes, collections, and adapter
paths that cannot represent explicit member slots use GeoArrow WKB fallback.
Scalar callers do not need
the columnar boundary. Gometry owns geometry kernels and the Arrow/WKB boundary;
optional Python adapters own focused GeoParquet conversions. Dataframe, SQL, and
database engines remain outside the core. The portable database boundary is ISO
WKB (or EWKB for PostGIS-compatible peers) plus side metadata.

## Reliability and supply-chain posture

Robust predicates, **precision controls** ([`quantize`][gometry.Geometry.quantize], [`snap_to_grid`][gometry.Geometry.snap_to_grid] — not a
JTS-style snap-rounding precision model), and structured validation reports are
core features, not extras. The architecture keeps the CRS authority boundary
explicit and records the bundled notices in the [license page](license.md).
