---
description: gometry's design principles and public-API naming rules — one canonical spelling, model explicitness, candidate/refine, CRS safety, the Arrow boundary, and supply-chain posture.
---

# Design principles

gometry exposes a typed Python facade over Rust-owned geometry semantics. The
public API uses explicit rules for operation placement, data models, result
shapes, and interchange boundaries.

Grid cover factories return a `CellArray` for scalar geometry and `Groups` of
`CellArray` for geometry arrays; coverage objects and interior/boundary partitions
are not public API.
Exact geometry membership uses top-level predicates such as
`gm.contains(source, probe)`. Prepared geometries are operands accepted on
either side of top-level predicates and XY predicates; plan- and probe-aware
selection never builds a tester for linear shapes. Numeric-id arrays expose
`to_numpy()` and `__array__`, not `values`. CRS metadata collections are
properties, bulk constructors and operations use `values` for their input
column, and `__geo_interface__` raises when M or coordinate epoch cannot be
represented by GeoJSON.

The [migration guide](../migrating/index.md) shows how these rules cash out
against the old stack.

## The constitution

1. **Pythonic surface, Rust-owned semantics and native kernels.** Geometry
   kernels, predicate robustness, and the memory model are Rust; the API is
   Pythonic.
2. **One canonical spelling per operation.** No synonym pairs; migration docs
   list familiar ecosystem names beside the canonical spelling.
3. **A class constructs one value; a plural free function builds an array.**
   `gm.Point(x, y)` / `gm.LineString(coords)` / `gm.Polygon(shell, holes)`
   construct a single geometry; `gm.points(...)` / `gm.line_strings(...)` /
   `gm.polygons(...)` build a packed `GeometryArray`. Classes are callable directly,
   with no classmethods.
4. **Unary operations are methods; binary operations are free functions; state is properties.**
   `g.buffer(d)`, `g.centroid()` are methods on the geometry; `gm.distance(a, b)`,
   `gm.intersection(a, b)` relate two operands; `g.area`, `g.length`, `pt.x` are properties.
   Stateful work (`idx.query(...)`) stays a method on its typed object.
5. **Batched/vectorized operations are canonical; scalar calls are convenience.**
6. **Operations avoid accidental Python loops, object churn, and hidden copies.**
7. **Earth model is explicit when correctness depends on it.**
8. **Index and cell results carry semantics**, not naked low-level values.
9. **Arrow/GeoArrow is the public columnar boundary**, not a beginner concept.

Three carve-outs are deliberate API clarity, not exceptions by accident:
`dwithin` stays as the standard word-of-art for distance-within predicates;
NaN X/Y coordinates raise as invalid geometry input rather than becoming missing
rows, consistently across the library; grid `to_polygon` dissolves cells into an
outline that may be a `MultiPolygon` when the covered cells are disjoint.

## API placement doctrine

The receiver tells you where an operation lives. Facts are properties:
`geom.area`, `geom.bounds`, `cell.center`. Unary work on one value is a method on
that value, and the array has the same spelling: `geom.buffer(distance)`,
`arr.buffer(distance)`, `geom.to_crs(3857)`. Binary relationships and overlays are
free functions because neither operand owns the question: `contains(a, b)`,
`distance(a, b)`, `intersection(a, b)`.

I/O follows the same rule. If you already hold a geometry, serialize from it:
`geom.to_wkb()`, `geom.to_geojson()`. If bytes or text create a geometry, use a
constructor-style free function: `from_wkb(...)`, `from_geojson(...)`. If a
geometry combines with side data, the composite builder is free:
`to_feature(geom, properties=...)`. There is no free scalar encoder: construct
the geometry and serialize from it
(`gm.Point(x, y, crs=4326).to_wkb(include_srid=True, precision=7)`).

Global prefix families keep domain functions together in both `gm.` completion and
alphabetical reference lists: `crs_*`, `h3_*`, `s2_*`, `geohash_*`, `tile_*`,
`pluscode_*`, and `osm_shortlink_*`. Grid families hold factories and set algebra
(`cover`, `cells`,
`bounding_cell`, `union`, `intersection`, `difference`); receiver operations stay
on the cell or `CellArray`: `cell.polygon`, `cell.children()`,
`cells.compact(depth)`, `cells.to_polygon()`.

## One canonical spelling per operation

Every operation has exactly one name. Historical names from the old stack are
**not** registered in the public API — they live in the searchable
[cheatsheet](../migrating/cheatsheet.md) instead. This keeps the API small and
discoverable: there is no `make_valid` *and* `repair`, no `STRtree` *and* `Rtree`
*and* `sjoin`.

| Concept | Canonical spelling |
|---|---|
| make-valid | [`repair`][gometry.Geometry.repair] |
| spatial index | [`gm.SpatialIndex`][gometry.SpatialIndex] |
| spatial join | [`gm.join`][gometry.join] |
| polyfill | `gm.h3_cover(geom, ...)` |
| geodesy | `gm.bearing(a, b)` / `point.destination(...)` (geographic CRS) / `gm.CRS(c).geodesic` |
| CRS transform | `geom.to_crs` / `gm.crs_transform` |
| dissolve / unary union | [`gm.union_all`][gometry.union_all] |

Prefixes are consistent, so the API filters and searches without a second
attribute hop. An alias carrying a name from another library does not ship.
Helpers are allowed when they add semantics rather than synonyms: [`gm.Point`][gometry.Point] with `crs=4326` makes geographic axis order explicit
while still storing coordinates as X/Y. A name that adds nothing over
an existing entry point does not ship — `__geo_interface__` / GeoJSON ingestion
is [`gm.from_geojson`][gometry.from_geojson]. The same rule rejects the
GeoPandas verbs that are aliases by another name: `clip(mask)` is
[`gm.intersection`][gometry.intersection] (with
[`clip_by_rect`][gometry.Geometry.clip_by_rect] for the cheap rectangular case) and
`explode` is [`gm.parts`][gometry.parts]. Convenience accessors that are
one-liners over existing surface stay out too: no recursive
`GeometryCollection.flatten()` (recurse over `parts`), no `Tile.ul`/`Tile.lr`
corner properties (read `tile.polygon.bounds`), and no geohash
`expand` (`[cell, *cell.neighbors]`). Geometry clustering and scattered-data
interpolation are out of scope: scikit-learn/scipy and scikit-gstat
own those domains, and gometry hands off through
[`get_coordinates`][gometry.get_coordinates] NumPy views. Cheap pre-grouping
before exact predicate work is what [`SpatialIndex.query`][gometry.SpatialIndex.query]
candidates are for.

`normalize()` is gometry's own canonical form: the lexicographically smallest
equivalent presentation (pointwise coordinates, then shorter-first; parts
ascending; open lines by the smaller direction; closed lines by the smallest
rotation×direction over the orbit; polygon rings min-vertex-first under
[RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946) winding — exterior
CCW, holes CW). The result is not textually equivalent to Shapely/GEOS
`normalize()` while remaining in the same equivalence class.

Cross-system *uniformity* stops where domain vocabulary starts. The four cell
systems share every operation shape (`cell`/`cells`, boundaries, set ops,
compact/uncompact), but the granularity knob keeps each system's own name —
H3 `resolution`, S2 `level`, geohash `precision`, tile `zoom` — because those
are the terms every upstream API, paper, and dataset uses; renaming them to
one invented word would make gometry consistent with itself and inconsistent
with everything its users read. Bing quadkeys are `tile.token` (no `.quadkey`
alias); slippy-map zoom is `tile.zoom` (no `.z` alias). Two deliberate gaps in the symmetry: only `contains_xy` / `intersects_xy` get coordinate-pair
specialized paths (the point-probe operations; the other predicates against a point
degenerate to these or to `touches`, and a full `_xy` matrix would multiply
surface without a workload behind it).

**Equality is a three-way vocabulary, each axis real.** `equals(a, b)` is the
*topological* test (order-independent, spatial); `equals_exact(a, b, tolerance)`
is *coordinate* comparison within a tolerance (frame-checked like every metric
op — comparing across CRS frames is an error); `equals_identical(a, b)` is
*value identity* — the vectorized `==` — where the CRS/epoch frame is part of
the value, so a frame difference is `False`, never an error. Scalar
`geom == other` and elementwise pandas `Series == Series` are both
`equals_identical` semantics. `GeometryArray.__eq__` itself stays *container*
value equality (whole-array `bool`, keeping arrays hashable and dict-usable
like tuples); the elementwise spelling is `equals_identical(left, right)` —
one spelling per meaning, with no overloaded middle ground.

Python *protocol* integrations are not aliases — `a & b`, `str(geom)`,
`format(geom, '.2f')`, `bool(geom)`, and `match` patterns are the language's
own spellings, adopted where the ecosystem already agrees on the meaning — the
operators, WKT `str`, format specs, and emptiness truthiness are established
Python-geometry conventions. Sugar that would *hide* a choice is rejected because
the wire or operation semantics would be implicit: no `bytes(geom)` (which WKB flavor? the CRS would vanish
silently — say `to_wkb(...)`), no `round(geom, n)` (`quantize` is coordinate
quantization, not numeric rounding), no cell-set operator aliases (cell-set vs
exact-geometry algebra is a real choice — `gm.s2_union` and `set(cells)` keep it
visible), no interned "forever-alive" empty-geometry singletons (identity
surprises for no measurable win), `CellArray` slicing returns another
[`CellArray`][gometry.CellArray], and `SpatialIndex` exposes mapping/iteration helpers
but candidates vs exact answers stay named methods —
`candidates(...)`/`query(...)`.
Cross-type `CRS == 4326` (and `CRS == "EPSG:4326"`) is supported for ergonomics,
so `CRS` is **unhashable** (`hash(CRS(...))` raises `TypeError`). Use
`crs.to_authority()` / `crs.to_epsg()` when a dict or set key is needed.

## CRS and candidate boundaries

CRS metadata, precision controls, edge models, and coverage rules are explicit
inputs. `set_crs` declares existing coordinates, `to_crs` transforms them, and
metric behavior follows the frame described in [CRS, units & measurement](../guide/crs.md).

Spatial indexes expose bounding-box candidates and exact predicate queries; grid
covers return typed cell arrays and exact membership remains a predicate on the
source geometry. The canonical operational descriptions are [Spatial indexing &
joins](../guide/indexing.md) and [Grids & geocodes](../guide/grids.md).

## Scalar and vectorized forms

Unary work stays on the receiver (`poly.buffer(...)`, `array.buffer(...)`), while
binary relationships and overlays stay free (`gm.contains(a, b)`). Scalar calls
return scalar values and array calls return the corresponding columnar result;
the stubs model both forms with overloads. Broadcasting accepts scalar/array and
equal-length array pairs, and rejects mismatched non-scalar lengths. See
[Arrays & performance](../guide/arrays.md) for the batch contract.

## NumPy-native bulk outputs

NumPy is a mandatory runtime dependency. Dense numeric results are read-only
`numpy.ndarray`s, geometry results are [`GeometryArray`][gometry.GeometryArray],
and ragged results are [`Groups`][gometry.Groups]. Text and diagnostic outputs
remain lists when no dense shape exists. Coordinate and cell-array details are in
[NumPy arrays & coordinates](../ecosystem/numpy.md) and [Arrays & performance](../guide/arrays.md).

## Errors as API

Exceptions are part of the public surface, designed once: a small hierarchy
rooted at `GeometryError(ValueError)` (so broad `except ValueError` remains
compatible),
with precise classes only where users genuinely catch differently —
`ParseError` for malformed serialized input (including cell ids/tokens),
`CRSMismatchError` for frame conflicts, `TransformError` for projection
failures, `InvalidGeometryError` for structural rules, and a dual-base
`GeometryTypeError(GeometryError, TypeError)` for wrong-kind operands (the
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
`GeometryArray.index` keep plain `ValueError` by stdlib convention. Messages
follow one gate-enforced grammar; `Raises` sections name the real class and are
contract-checked, so the docs cannot drift from the raise sites.

## Z and M layouts

XY, XYZ, XYM, and XYZM are supported layouts. Z/M behavior by operation family
and the explicit recovery methods are documented in [Geometry & dimensions](../guide/geometry.md).

## Overlay returns dissolved maximal lines

Overlays node their inputs into atomic per-vertex segments
to compute the result; gometry then dissolves that linework back into the fewest
maximal `LineString`s, splitting only at genuine junctions (a node of degree
`>= 3`) and never fusing two arms through one. So `difference`, `intersection`,
`symmetric_difference`, and `union` of lines — and the shared-boundary linework
of polygon overlays — come back as whole lines: a line minus a disjoint line is
returned intact, a line clipped to a covering polygon is one `LineString`, and a
line crossed by a *subtracted* line keeps flowing through the crossing vertex.
The OGC point-set semantics constrain the geometry, not its partition into
parts, so the public result does not expose the intermediate noding arrangement.
`line_merge` is available when a caller starts with independent linework: it
joins at degree-2 nodes and splits at genuine Y/T/X junctions.

## The Arrow / GeoArrow boundary

The public columnar geometry format is [GeoArrow](https://geoarrow.org/)-compatible:
homogeneous arrays export separated `x`/`y`/`z`/`m` children, while mixed types,
mixed axes, and collections use GeoArrow WKB fallback. Scalar callers do not need
the columnar boundary. Gometry owns geometry kernels and the Arrow/WKB boundary;
optional Python adapters own focused GeoParquet conversions. Dataframe, SQL, and
database engines remain outside the core. The portable database boundary is ISO
WKB (or EWKB for PostGIS-compatible peers) plus side metadata.

## Reliability and supply-chain posture

Robust predicates, **precision controls** (`quantize`, `snap_to_grid` — not a
JTS-style snap-rounding precision model), and structured validation reports are
core features, not extras. The architecture keeps the CRS authority boundary
explicit and records the bundled notices in the [license page](license.md).
