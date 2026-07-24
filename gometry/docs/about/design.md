---
description: gometry's design principles and public-API naming rules — one canonical spelling, model explicitness, candidate/refine, CRS safety, the Arrow boundary, and supply-chain posture.
---

# Design principles

gometry is not a faster reimplementation of the legacy stack. It is a rethink of
the Python geospatial developer experience around one idea:

> The old stack forces users to remember which *model* they are in — planar
> geometry, spherical geography, ellipsoidal geodesy, CRS transformation,
> bounding-box indexing, or discrete cell grids. gometry makes the model explicit,
> composes the pieces, and provides a documented migration path without inheriting
> legacy API baggage.

This page is the API constitution — the rules that decide the public surface and
how it is spelled. The [migration guide](../migrating/index.md) shows how these
rules cash out against the old stack.

## The constitution

1. **Pythonic surface, Rust-owned semantics and hot paths.** The kernels,
   robustness, and memory model are Rust; the API is Pythonic.
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
8. **Index/cell/coverage results carry semantics**, not naked low-level values.
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
`to_feature(geom, properties=...)`. There is no free scalar encoder to remember
either — construct the geometry and serialize from it
(`gm.Point(x, y, crs=4326).to_wkb(include_srid=True, precision=7)`).

Global prefix families keep domain functions together in both `gm.` completion and
alphabetical reference lists: `crs_*`, `h3_*`, `s2_*`, `geohash_*`, `tile_*`,
`pluscode_*`, and `osm_shortlink_*`. Grid families hold factories and set algebra
(`cover`, `cells`,
`bounding_cell`, `union`, `intersection`, `difference`); receiver operations stay
on the cell, `CellArray`, or coverage: `cell.polygon`, `cell.children()`,
`cells.compact(depth)`, `cells.to_polygon()`.

## One canonical spelling per operation

Every operation has exactly one name. Historical names from the old stack are
**not** registered in the public API — they live in the searchable
[cheatsheet](../migrating/cheatsheet.md) instead. This keeps the API small and
discoverable: there is no `make_valid` *and* `repair`, no `STRtree` *and* `Rtree`
*and* `sjoin`. The canonical choices:

| Concept | Canonical spelling |
|---|---|
| make-valid | [`repair`][gometry.Geometry.repair] |
| spatial index | [`gm.SpatialIndex`][gometry.SpatialIndex] |
| spatial join | [`gm.join`][gometry.join] |
| polyfill | `gm.h3_cover(geom, ...)` |
| geodesy | `gm.bearing(a, b)` / `gm.destination(...)` (geographic CRS) / `gm.CRS(c).geodesic` |
| CRS transform | `geom.to_crs` / `gm.crs_transform` |
| dissolve / unary union | [`gm.union_all`][gometry.union_all] |

Consistent prefixes make the full API easy to learn, filter, and search without a
second attribute hop. Adding an alias "because users know that name" is explicitly rejected.
Helpers that remove genuine footguns are allowed when they add semantics rather
than synonyms: [`gm.Point`][gometry.Point] with `crs=4326` makes geographic axis order explicit
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
interpolation are intentionally out of scope: scikit-learn/scipy and scikit-gstat
own those domains exhaustively, and gometry hands off through
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
fast paths (the point-probe hot loops; the other predicates against a point
degenerate to these or to `touches`, and a full `_xy` matrix would multiply
surface without a workload behind it).

**Equality is a three-way vocabulary, each axis real.** `equals(a, b)` is the
*topological* test (order-independent, spatial); `equals_exact(a, b, tolerance)`
is *coordinate* comparison within a tolerance (frame-checked like every metric
op — comparing across CRS frames is an error); `equals_identical(a, b)` is
*value identity* — the vectorized `==` — where the CRS/epoch frame is part of
the value, so a frame difference is simply `False`, never an error. Scalar
`geom == other` and elementwise pandas `Series == Series` are both
`equals_identical` semantics. `GeometryArray.__eq__` itself stays *container*
value equality (whole-array `bool`, keeping arrays hashable and dict-usable
like tuples); the elementwise spelling is `equals_identical(left, right)` —
one obvious way per meaning, no overloaded middle ground.

Python *protocol* integrations are not aliases — `a & b`, `str(geom)`,
`format(geom, '.2f')`, `bool(geom)`, and `match` patterns are the language's
own spellings, adopted where the ecosystem already agrees on the meaning — the
operators, WKT `str`, format specs, and emptiness truthiness are established
Python-geometry conventions. Sugar that would *hide* a decision is rejected on the same
no-footgun grounds: no `bytes(geom)` (which WKB flavor? the CRS would vanish
silently — say `to_wkb(...)`), no `round(geom, n)` (`quantize` is coordinate
quantization, not numeric rounding), no coverage set-operators (cell-set vs
exact-geometry algebra is a real choice — `gm.s2_union` and `set(cov.cells)`
keep it visible), no interned "forever-alive" empty-geometry singletons
(identity surprises for no measurable win), coverage slicing returns a
[`CellArray`][gometry.CellArray] of the visible cells (`cov[i:j]`), never a
sliced "coverage" object (a partial covering would no longer answer the exact
predicate it was built for), `SpatialIndex` exposes mapping/iteration helpers
but candidates vs exact answers stay named methods —
`candidates(...)`/`query(...)`.
Cross-type `CRS == 4326` (and `CRS == "EPSG:4326"`) is supported for ergonomics,
so `CRS` is **unhashable** (`hash(CRS(...))` raises `TypeError`). Use
`crs.to_authority()` / `crs.to_epsg()` when a dict or set key is needed.

## The CRS is the single knob

The most dangerous habit in legacy planar geometry is the ambiguous bare metric.
On lon/lat data a planar `area` is degrees², `distance` is degrees, and
`buffer(100)` is 100 degrees. gometry keeps one of each operation and lets the geometry's CRS decide,
returning **native units** for that CRS:

```python
geom.area
geom.to_crs(target).area
gm.h3_cover(geom, resolution=9)

```

[`area`][gometry.Geometry.area] / [`gm.distance`][gometry.distance] never return
"square degrees": a geographic CRS measures geodesically in meters, a projected
CRS uses its **native linear units** (feet stay feet; meters stay meters), and
CRS-free geometry stays in coordinate units. [`buffer`][gometry.Geometry.buffer]
uses the same CRS-native distance rule; on a geographic CRS the distance is
meters via a **local projection** approximation. Pass `unit='meters'` to force SI
or `unit='planar'` for raw coordinate math.
CRS, dimension, precision controls (`quantize` / `snap_to_grid`), edge model, and
coverage cell rule are always metadata or explicit options — never hidden global
conventions.

## Candidate / refine is first-class

Spatial indexes and grids produce *candidates*, not exact matches. The old stack
hides this; the classic bug is treating a `STRtree.query` result or an H3 polyfill
as exact. gometry makes the two steps separate, obviously-named operations:

```python
import gometry as gm
idx = gm.SpatialIndex(geoms)
candidates = idx.candidates(query)
matches = idx.query(query, predicate='intersects')
idx.explain(query, predicate='intersects')
coverage = gm.h3_cover(geom, resolution=9, cell_rule='center')
(coverage.cell_rule, coverage.interior_cells)
coverage.covers(points)

```

Coverages and `Groups` are typed objects that carry their own semantics.
Index results are ordinary `int64` ndarrays — the **method name** encodes the
stage (`candidates` vs `query` vs `nearest`), not a distinct return type, so a
candidate set is not silently typed as ground truth.

## Scalar ergonomics, vectorized performance

Two users must both be served without either paying for the other:

```python
import gometry as gm
poly = gm.Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
gm.contains(poly, point)
gm.contains(poly, millions)

```

The canonical spelling follows the operation shape: unary work stays on the
receiver (`poly.buffer(...)`, `array.buffer(...)`), while binary relationships
and overlays stay free (`gm.contains(poly, point)`,
`gm.contains(poly, millions)`). Users are never taught to write Python loops
over scalar geometry objects when a batched receiver method or broadcasted free
function exists.

**The return type follows the input.** One operation name serves both users and
hands each the shape it wants: a scalar geometry yields a scalar result (a
`float`, a `bool`, a single `Geometry`, or a plain tuple such as
`nearest_points`'s `(Point, Point)`), while an array yields the columnar form (a
read-only `ndarray`, a `GeometryArray`, or a `(left, right)` pair of point
columns for `nearest_points`). This input-driven polymorphism is deliberate, not an
inconsistency — and the type stubs model every form with `@overload`s, so editors
and type checkers stay precise on both the scalar and the array path. The forms
are never collapsed to one type "for consistency": doing so would tax one user to
serve the other. Broadcasting is strict —
scalar × array is fine, equal-length array × array is pairwise, and mismatched
non-scalar lengths raise — so accidental Cartesian blowups are impossible. For
many-to-many work, [`gm.SpatialIndex`][gometry.SpatialIndex] / [`gm.join`][gometry.join] are the answer.

The same broadcast rule extends to the *magnitude* parameters of array transforms
— `buffer(distance)`, `simplify(tolerance)`, `segmentize(max_length)`,
`line_interpolate(distance)`, `line_substring(...)`, the measured-LRS and
hull-tolerance kin: each takes a scalar (applied to every geometry) **or** one
value per geometry (a sequence/`ndarray`, length == the array). A scalar is just
the 0-d case of the same idiom, so it stays one obvious spelling, and the scalar
path keeps its columnar/packed fast lane untouched. This makes variable-width
buffers and the `line_locate` → `line_interpolate` round-trip natural
without a Python loop. Binary magnitude operations follow the same rule:
`dwithin`'s distance and `snap`'s tolerance may be scalar or row-aligned after
the geometry operands have established the result length.

## NumPy-native bulk outputs

NumPy is a mandatory runtime dependency. Every dense bulk result lands in one of
three shapes — **numbers**, **geometries**, or **ragged** — not a Python list:

- **Numbers** → read-only `numpy.ndarray` (`float64`, `bool_`, `int64`, `uint64`).
  Metrics, predicate masks, curve keys, bounds (`(n, 4)` with `nan` empty rows),
  and join/query-pair id columns all follow this rule.
- **Geometries** → [`GeometryArray`][gometry.GeometryArray]. Constructive and
  transform ops stay columnar in Rust; the explicit object-array bridge is
  [`GeometryArray.to_numpy`][gometry.GeometryArray.to_numpy] /
  [`gm.GeometryArray`][gometry.GeometryArray] (also accepts `__geo_interface__` objects).
- **Ragged** → [`Groups`][gometry.Groups] (CSR). Array-form index
  queries keep one values buffer + offsets; `.values`/`.offsets`/row views are `int64`
  ndarrays. Joins and `query_pairs` return a plain `(left, right)` pair of int64
  ndarrays instead of a bespoke pair container.

The rationale:

- **Real ndarrays, not adapter wrappers.** Bulk kernels write straight into
  NumPy-owned buffers — `np.asarray(result)` is a zero-copy view, and the
  ecosystem (pandas, polars, sklearn) sees ordinary array dtypes.
- **No boxing floor.** Building a million `PyFloat`/`PyBool` objects used to
  dominate the cheap kernels; one typed ndarray removed the last structural
  overhead the engine didn't own.
- **GeometryArray stays explicit.** `np.asarray(geoms)` and
  `geoms.to_numpy()` allocate an object ndarray bridge; `copy=False`
  is rejected because object boxing is not zero-copy. `__array_ufunc__ = None`
  keeps ufuncs from coercing geometry containers.
- **One dtype per element kind.** No per-operation bespoke numeric containers;
  `Groups` is the only ragged index wrapper because CSR grouping has no
  fixed-width shape. Geometry witnesses stay plain tuples of `GeometryArray`
  columns.

Text bulk outputs (`to_wkt`, `relate`, geohash tokens) stay lists — strings
have no dense ndarray form. Ragged diagnostics stay lists when they have no single
dense representation. Dense paired geometry witnesses are a plain `(left, right)`
tuple of parallel point `GeometryArray` columns.
Coordinate views expose `.x`/`.y`/`.z`/`.m` as read-only `float64` ndarrays;
[`gm.get_coordinates`][gometry.get_coordinates] bulk-extracts `(m, k)` matrices.

Cell arrays follow the same scalar-mirror rule as geometry arrays: the accessor
name stays at the scalar fact's altitude even when the receiver has many rows.
So the [`Cell`][gometry.Cell] `token` fact becomes
[`CellArray.token`][gometry.CellArray.token], and cell facts such as `center`,
`polygon`, and `area` stay singular while returning vectorized results. Cells
are identifiers, not OGC geometries — there is no `CellArray.geometry_type`.

## CRS safety as a product feature

The `set_crs` / `to_crs` distinction is unavoidable: `set_crs` declares what
existing coordinates mean, `to_crs` transforms them. CRS is lightweight metadata on
the geometry (`geom.crs`), not a heavyweight object you carry. The
transform layer is **always X/Y** even when an authority declares latitude-first
axis order, eliminating the `always_xy=True` class of bugs. [PROJ](https://proj.org/) is the deliberate
authority backend — bundled behind the API so release wheels need no system PROJ
shared library — and gometry owns the metadata invariants, the Z/M preservation
policy, the fast common transforms, and the error model.

## Errors as API

Exceptions are part of the public surface, designed once: a small hierarchy
rooted at `GeometryError(ValueError)` (so broad `except ValueError` never breaks),
with precise classes only where users genuinely catch differently —
`ParseError` for malformed serialized input (including cell ids/tokens),
`CRSMismatchError` for frame conflicts, `TransformError` for projection
failures, `InvalidGeometryError` for structural rules, and a dual-base
`GeometryTypeError(GeometryError, TypeError)` for wrong-kind operands (the
`numpy.exceptions.AxisError` pattern). Finer taxonomies (per-format parse
classes, `TokenError`, `TopologyError`, per-engine classes, and a dedicated
`GridError`) were rejected: nobody catches at that granularity, and the
message carries the rest. Out-of-range depth parameters are ordinary
`GeometryError` value lanes; invalid cell ids/tokens are `ParseError` parse
lanes. Python
protocol failures stay builtin — domain wrong-kind failures use
`GeometryTypeError(GeometryError, TypeError)` (dual-base); pure Python protocol
`TypeError`s stay builtin,
`IndexError`, or `StopIteration` for protocol semantics, and `__format__` /
`GeometryArray.index` keep plain `ValueError` by stdlib convention. Messages
follow one gate-enforced grammar; `Raises` sections name the real class and are
contract-checked, so the docs cannot drift from the raise sites.

## Z / M as a real contract

gometry supports XY / XYZ / XYM / XYZM from the data model and IO layers. `M` is a
measure ordinate (linear referencing, timestamps, route measures) and is never
silently treated as `Z`. The fixed behavior is documented per operation family:
accessors and IO preserve Z/M exactly; XY topology predicates ignore them; overlay
and triangulation interpolate Z/M onto resolvable vertices; vertex-subset
operations keep the Z/M of every surviving input vertex; and operations that
*invent* new vertices (buffer, centroid, Voronoi, the bounding circles) cannot
fabricate Z/M, so they return a 2D result. This is fixed behavior, not a policy
matrix. The explicit recovery is `set_z` / `interpolate_m` after the operation.

## Overlay returns dissolved maximal lines

A set operation returns the *cleanest* geometry for its point set, not the
internal arrangement. Overlays node their inputs into atomic per-vertex segments
to compute the result; gometry then dissolves that linework back into the fewest
maximal `LineString`s, splitting only at genuine junctions (a node of degree
`>= 3`) and never fusing two arms through one. So `difference`, `intersection`,
`symmetric_difference`, and `union` of lines — and the shared-boundary linework
of polygon overlays — come back as whole lines: a line minus a disjoint line is
returned intact, a line clipped to a covering polygon is one `LineString`, and a
line crossed by a *subtracted* line keeps flowing through the crossing vertex.
Legacy overlay engines ([JTS](https://github.com/locationtech/jts) / GEOS) instead
leak the noded arrangement (one part per input span) and make
callers run `line_merge` afterward; the OGC point-set semantics constrain the
geometry, not its partition into parts, so dissolving is spec-faithful and the
friendlier default. `line_merge` itself is the same degree-2 dissolve exposed
directly — it joins at degree-2 nodes and splits every Y/T/X junction, the JTS
`LineMerger` contract — so one engine drives both and there is one obvious way to
get maximal lines.

## The Arrow / GeoArrow boundary

The public columnar geometry format is [GeoArrow](https://geoarrow.org/)-compatible: homogeneous point,
line, polygon, and multi-geometry arrays export separated `x`/`y`/`z`/`m`
children, while mixed geometry types, mixed coordinate axes, and collections use
GeoArrow WKB fallback. It is the interchange boundary for lakehouse and dataframe
users — but it is never forced on beginners, who work with scalar geometry and
never touch Arrow. gometry owns geometry kernels and the Arrow/WKB boundary in
Rust; an optional Python integration layer owns focused
`from_geoparquet` / `to_geoparquet` adapters. It does **not** own dataframe
engines, SQL engines, or database adapters. The portable database boundary is
ISO WKB (or EWKB for PostGIS-compatible peers) plus side metadata.

## Reliability and supply-chain posture

Robust predicates, **precision controls** (`quantize`, `snap_to_grid` — not a
JTS-style snap-rounding precision model), structured validation reports,
and differential tests against the established oracles (GEOS/JTS,
[GeographicLib](https://geographiclib.sourceforge.io/), PROJ)
are core features, not extras. The dependency posture prefers pure-Rust,
permissively-licensed crates; heavy C/C++ libraries are avoided except where an
authority backend is the correct product boundary, as with PROJ for CRS semantics.
Official C/C++ libraries (GEOS, JTS, H3, GeographicLib, S2) remain semantic
oracles; pure-Rust ports (`h3o`, `geographiclib-rs`, gometry's own S2) ship in
production. See the [license page](license.md) for the third-party inventory.

## Public-API naming rules

A condensed restatement of the rules that govern new API:

- **Scalar construction:** callable classes — `Point`, `LineString`, `Polygon`,
  `MultiPoint`, `MultiLineString`, `MultiPolygon`, `GeometryCollection`, `box`.
  Explicit Z/M `z=` / `m=` keywords on `Point`. No classmethods.
- **Array construction:** plural free functions — `points`, `line_strings`,
  `polygons`, `boxes`, `multi_points`, `multi_line_strings`, `multi_polygons` — each
  returning a packed `GeometryArray`; the generic builder is `GeometryArray([...])`.
- **Coordinate order:** explicit `Point(lon, lat, crs=4326)`; one documented internal
  order (X/Y).
- **Migration aliases:** none in the public API; mappings in docs/tooling.
- **Operations:** unary → methods (`g.buffer(d)`, `g.centroid()`); binary → free functions
  (`gm.intersection(a, b)`, `gm.distance(a, b)`).
- **Measurement:** the CRS is the single knob and results are **native** (geographic →
  geodesic meters; projected → native linear units; none → coordinate units) — state as
  properties (`g.area` / `g.length`), free `gm.area`/`gm.length` only with `unit=`,
  and free functions for binary operands (`gm.distance(a, b)`); `unit=` overrides the
  unit system, `to_crs(...)` changes the frame.
- **Index/grid results:** typed and semantic — `candidates` is the prefilter,
  `query` is refined, coverage answers `covers`/`contains`/`intersects` exactly.
- **Validation/repair:** `validate()` returns a structured report; `repair` is the
  canonical make-valid name.
- **IO:** construction is free (`from_wkb`, `from_wkt`, `from_geojson`,
  `from_arrow`); serialization is a receiver method (`geom.to_wkb()`,
  `geom.to_wkt()`, `geom.to_geojson()`, `geom.to_arrow()`).
