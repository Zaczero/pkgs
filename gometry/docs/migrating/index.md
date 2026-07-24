---
description: Migrating to gometry from Shapely, pyproj, h3-py, s2sphere, and rtree — philosophy, the per-library mapping, and your first ten minutes.
---

# Migrating to gometry

gometry replaces the practical day-to-day Python geospatial stack — **Shapely + pyproj + h3-py + s2sphere + rtree** — with one Rust-backed package and one coherent API:

```python
import gometry as gm

```

Every old pattern maps to one canonical gometry spelling, and the mapping is
exhaustive and searchable. If you want a single lookup table, jump to the
[cheatsheet](cheatsheet.md). Otherwise, retrain the five habits below, then read
the per-library section — [Shapely](#coming-from-shapely),
[pyproj](#coming-from-pyproj), [h3-py & s2sphere](#coming-from-h3-py-s2sphere),
[rtree & STRtree](#coming-from-rtree-strtree) — for side-by-side examples.

## Why a clean break

The incumbent stack forces you to track which *model* every call assumes: planar geometry, ellipsoidal geodesy, CRS transforms, bounding-box indexing,
or discrete cell grids. The cost of a wrong guess is silent: `.area` in degrees²,
`.buffer(100)` in degree-units, an index `query` that returned only bounding-box
candidates, an H3 polyfill that meant "center containment" when you assumed "covers".

gometry's design makes the model explicit and lets the pieces compose:

- **One canonical spelling per operation.** No `make_valid` *and* `repair`; it is
  [`repair`][gometry.Geometry.repair]. No `STRtree` *and* `Rtree` *and* `sjoin`; it is
  [`gm.SpatialIndex`][gometry.SpatialIndex] and [`gm.join`][gometry.join].
- **The CRS is the single knob; metrics are native.** There is one `geom.area`, one
  `geom.length`, one `gm.distance(a, b)`. The geometry's CRS decides — geographic →
  geodesic meters, projected → planar **native linear units** (feet stay feet),
  none → coordinate units — so the dangerous "square degrees" result of measuring
  lon/lat as if it were planar cannot happen. Pass `unit='meters'` for forced SI.
  For discrete cells, `gm.h3_cover(geom, ...)` / `gm.s2_cover(geom, ...)`.
- **Explicit CRS.** CRS is metadata you attach (`set_crs`) or transform through
  (`to_crs`) — never an opaque object you carry around. The transform layer is
  always **X/Y** regardless of the authority's declared axis order.
- **Candidate / refine is first-class.** Spatial indexes expose
  `idx.candidates(...)` (bbox prefilter) and `idx.query(..., predicate=...)`
  (exact refine) as separate, obviously-named operations, plus `idx.explain(...)`.
- **Coverage answers exactly.** A grid coverage names its `cell_rule` and answers
  exact membership itself (`covers`/`contains`/`intersects`) — no separate refine
  step to remember.

## First ten minutes coming from the old stack

If you have written the old stack, these are the five habits to retrain. Each
gometry block below is executed and its output captured at build time.

### 1. Attach a CRS at construction

Shapely geometry is CRS-blind. gometry geometry carries CRS metadata, and metric
operations rely on it.

=== "Shapely"

    ```python
    from shapely.geometry import Point, box
    pt = Point(21.0, 52.0)          # no CRS; "what do these numbers mean?"
    area = box(20.0, 51.0, 22.0, 53.0)
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    pt = gm.Point(21.0, 52.0, crs=4326)
    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    print(pt.crs, area.geometry_type)
    ```

### 2. The CRS decides the measurement

There is one `geom.area`. The geometry's CRS alone decides how it is measured, and
the result is native by default — no per-call model choice.

=== "Shapely + pyproj"

    ```python
    geom.area                       # degrees² on lon/lat — almost always wrong
    from pyproj import Geod
    Geod(ellps="WGS84").geometry_area_perimeter(geom)  # the value you wanted
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    # Geographic CRS -> geodesic m^2, automatically.
    print("geodesic (m^2):  ", round(area.area))
    # Reproject to measure planar meters instead.
    print("projected (m^2): ", round(area.to_crs(area.estimate_local_crs()).area))
    ```

### 3. `set_crs` declares, `to_crs` transforms

=== "GeoPandas / pyproj"

    ```python
    gdf = gdf.set_crs(4326)         # declare meaning of existing coords
    gdf = gdf.to_crs(3857)          # transform coords
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    raw = gm.Point(21.0, 52.0)        # CRS-free
    declared = raw.set_crs(4326)       # same numbers, now they mean lon/lat
    transformed = declared.to_crs(3857)  # numbers change
    print(declared.to_wkt())
    print(transformed.to_wkt())
    ```

### 4. Index queries name the stage — candidates vs refined

Shapely's `STRtree.query` already has two modes: **no predicate** returns bbox
candidates; **`predicate=`** (Shapely 2.x) refines exactly. gometry makes that
split explicit with two method names so you cannot confuse them.

=== "Shapely STRtree"

    ```python
    from shapely import STRtree
    idx = STRtree(geoms)
    candidates = idx.query(area)                          # bbox candidates
    hits = idx.query(area, predicate="intersects")        # exact refine (2.x)
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    pts = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    idx = gm.SpatialIndex(pts)
    print("candidates:", idx.candidates(area))               # bbox prefilter
    print("refined:   ", idx.query(area, predicate="contains"))  # exact
    ```

### 5. Grid coverage names its rule — membership is exact

=== "h3-py"

    ```python
    import h3
    cells = h3.polygon_to_cells(h3.LatLngPoly(shell), 8)  # which rule? (implicit)
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    cov = gm.h3_cover(area, resolution=5)
    print("cell_rule:", cov.cell_rule, "| cells:", len(cov.cells))
    print("exact membership:", cov.covers(gm.Point(21.0, 52.0, crs=4326)))
    ```

Once these five are reflexes, the per-library sections below and the
[cheatsheet](cheatsheet.md) cover the rest of the surface.

## Coming from Shapely

gometry keeps Shapely's role as the in-process geometry type system — the same
seven [Simple Features](https://www.ogc.org/standard/sfa/) families, the same
predicates and constructive operations, the same WKB/WKT/GeoJSON interop and
`__geo_interface__`. What changes is what made Shapely error-prone: scalar
predicates become **free functions** (`gm.contains(a, b)`, vectorized over
arrays), metrics read the CRS, and the kernels are GEOS-free. Three changes bite
most often — validity, the `.area` footgun, and the STRtree refine step.

### `make_valid` → `repair`

[`repair`][gometry.Geometry.repair] is the canonical spelling for the ecosystem
concept called `make_valid` / `ST_MakeValid` / `GEOSMakeValid`. `validate()`
returns a structured report with the reason and location, where Shapely splits the
job across `is_valid`, `explain_validity`, and `make_valid`.

=== "Shapely"

    ```python
    from shapely import make_valid
    from shapely.validation import explain_validity
    if not geom.is_valid:
        print(explain_validity(geom))
    fixed = make_valid(geom)
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    bowtie = gm.from_wkt("POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))")
    print("valid:", bowtie.is_valid)        # the quick bool, like Shapely
    report = bowtie.validate()              # the reason and location
    print("reason:", report.reason)
    fixed = bowtie.repair()
    print("repaired:", fixed.geometry_type, "| now valid:", fixed.is_valid)
    ```

### The `.area` / `.distance` / `.buffer` footgun

The single most important behavioral change. On lon/lat data Shapely's bare
`.area` returns degrees², `.distance` returns degrees, and `.buffer(100)` buffers
by 100 *degrees*. gometry has one `geom.area` (and one `gm.distance` /
`Geometry.buffer`) whose answer is decided by the geometry's CRS and is native by
default — a geographic CRS measures geodesically (meters), a projected CRS in its
**native linear units**, CRS-free geometry in bare coordinate units.

=== "Shapely (silently wrong on lon/lat)"

    ```python
    poly.area          # degrees^2 — meaningless as an area
    a.distance(b)      # degrees
    poly.buffer(100)   # 100 degrees ≈ the whole hemisphere
    ```

=== "gometry (the CRS decides)"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    a = gm.Point(21.0, 52.0, crs=4326)
    b = gm.Point(22.0, 52.0, crs=4326)
    # Geographic CRS -> geodesic meters, automatically.
    print("geodesic area (m^2):  ", round(area.area))
    print("geodesic distance (m):", round(gm.distance(a, b)))
    # Reproject to measure planar native units (meters for UTM) instead.
    print("projected area (m^2): ", round(area.to_crs(area.estimate_local_crs()).area))
    ```

### STRtree → SpatialIndex (+ PreparedGeometry)

Shapely already refines when you pass `predicate=` to `STRtree.query`. gometry
keeps **two acceleration shapes**, not one collapse:

- [`SpatialIndex`][gometry.SpatialIndex] — many geometries, query/join/nearest
  with explicit `candidates` vs `query(..., predicate=...)`, plus frame checks,
  missing-row semantics, and geodesic nearest lanes.
- [`geom.prepare()`][gometry.Geometry.prepare] — one geometry tested many times
  (cached segment index); still a separate object, not folded into the index.

=== "Shapely"

    ```python
    from shapely import STRtree, prepare
    tree = STRtree(geoms)
    candidates = tree.query(area)                       # bbox candidates
    hits = tree.query(area, predicate="contains")       # refined (2.x)

    prepare(poly)
    poly.contains(pt)                                   # prepared fast path
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    pts = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    idx = gm.SpatialIndex(pts)
    print("candidates:", idx.candidates(area))                  # bbox prefilter
    print("refined:   ", idx.query(area, predicate="contains")) # exact
    prep = area.prepare()
    print("prepared contains first point:", prep.contains(pts[0]))
    ```

Construction and IO map across directly. Build through the geometry classes
(`gm.Point`, `gm.Polygon`, …) with explicit `crs=`, pack arrays with the plural
builders (`gm.points`, `gm.polygons`) or `gm.GeometryArray([...])`, and ingest
GeoJSON / `__geo_interface__` with `gm.from_geojson`. Readers are verb-prefixed
free functions (`gm.from_wkb`, `gm.from_wkt`), and encoders are methods on the
geometry (`geom.to_wkb(include_srid=True)`, `geom.to_wkt()`, `geom.to_geojson()`,
`geom.to_arrow()` for [GeoArrow](https://geoarrow.org/) columnar interchange).
Vertex-subset ops (`simplify`, `convex_hull`, triangulation) preserve Z/M where
Shapely drops it. The full Shapely symbol table — construction, inspection,
predicates, constructive ops, linear referencing, validation, and IO — is in the
[cheatsheet](cheatsheet.md#shapely).

## Coming from pyproj

pyproj owns two jobs: **CRS transformation** (the `CRS` / `Transformer` objects
over libPROJ) and **geodesy** (the `Geod` object). gometry keeps
[PROJ](https://proj.org/) as the authority backend — bundled behind the API — but
drops the objects you carried around. Transform through the geometry (`to_crs`) or
the stateless [`gm.crs_transform`][gometry.crs_transform]; get geodesy from
point-navigation free functions on any geographic CRS. The boundary is always
X/Y — no `always_xy=True` ceremony.

### `Transformer` → `to_crs` and `gm.crs_transform`

For geometry, transform via the geometry. For raw coordinate values, use the
stateless [`gm.crs_transform`][gometry.crs_transform] — there is no transformer
object to construct, cache, or reuse.

=== "pyproj"

    ```python
    t = Transformer.from_crs(4326, 3857, always_xy=True)
    x2, y2 = t.transform(21.0, 52.0)
    minx2, miny2, maxx2, maxy2 = t.transform_bounds(20, 51, 22, 53)
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    x2, y2 = gm.crs_transform(4326, 3857, 21.0, 52.0)
    print("point:", round(x2, 1), round(y2, 1))
    print("bounds:", [round(v) for v in gm.crs_transform_bounds(4326, 3857, (20, 51, 22, 53))])
    ```

Scalar inputs return a tuple; bulk inputs return one row-major `(N, 2)` / `(N, 3)`
NumPy matrix. `transform_bounds` takes the bounds as one `(minx, miny, maxx, maxy)`
tuple and accepts a `densify=` count for curved CRS edges. To pick a working
projection without hand-choosing a UTM zone, ask
[`geom.estimate_local_crs()`][gometry.Geometry.estimate_local_crs]. When every
coordinate has Z and the operation consumes it (geocentric, vertical, compound
CRS), `to_crs` transforms Z through the same pipeline; otherwise Z and M are
preserved unchanged — never invented, never silently dropped.

### `Geod` → point navigation

pyproj's `Geod` (Karney/[GeographicLib](https://geographiclib.sourceforge.io/)
WGS 84 ellipsoidal geodesy) maps onto `gm.bearing`, `gm.destination`,
`gm.point_between`, and the geometry metrics. On a **geographic** CRS these compute
geodesically on the ellipsoid; there is no `Geod` object to construct. For a
non-WGS 84 ellipsoid, [`gm.CRS(code)`][gometry.CRS]`.geodesic(...)` runs the
geodesic problem on that CRS's own ellipsoid.

=== "pyproj Geod"

    ```python
    from pyproj import Geod
    g = Geod(ellps="WGS84")
    dist = g.inv(lon1, lat1, lon2, lat2)[2]          # distance (m)
    az = g.inv(lon1, lat1, lon2, lat2)[0]            # forward azimuth
    lon2, lat2, _ = g.fwd(lon1, lat1, az, dist)      # direct problem
    length = g.line_length(lons, lats)               # polyline length
    area, perim = g.geometry_area_perimeter(poly)    # signed area, perimeter
    pts = g.npts(lon1, lat1, lon2, lat2, n)          # intermediate points
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    a = gm.Point(21.0, 52.0, crs=4326)
    b = gm.Point(22.0, 52.0, crs=4326)
    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)

    print("distance (m): ", round(gm.distance(a, b)))            # geographic -> geodesic
    print("bearing (deg):", round(gm.bearing(a, b), 1))
    print("destination:  ", gm.destination(a, 90.0, 1000.0))
    print("midpoint:     ", gm.point_between(a, b, 0.5, normalized=True))
    print("area (m^2):   ", round(area.area))
    print("perimeter (m):", round(area.length))
    ```

The full `gm.CRS` object surface (`is_*` classification, `to_wkt`/`to_proj`/
`to_projjson`, catalog discovery, coordinate operations) and the complete `Geod`
symbol map are in the [cheatsheet](cheatsheet.md#pyproj).

## Coming from h3-py & s2sphere

h3-py and s2sphere both turn geometry into compact cell IDs for aggregation,
joins, partitioning, and coarse pre-filtering. gometry unifies
[H3](https://h3geo.org/) and [S2](https://s2geometry.io/) under typed cells and
coverages, and fixes the recurring trap: **polygon coverage is approximate, and
the rule that picked the cells is usually implicit** — with exact membership then
your problem. gometry's coverages name the `cell_rule` and answer exact membership
themselves.

### `polyfill` → `cover` with an explicit rule

h3-py's `polygon_to_cells` (formerly `polyfill`) has an implicit containment rule,
and exact membership afterwards is a hand-written per-cell geometry check. gometry
names the rule and builds the exact step in.

=== "h3-py"

    ```python
    import h3
    cells = h3.polygon_to_cells(h3.LatLngPoly(shell), 8)  # which rule? center? overlap?
    # exact membership? do it yourself, per point, with another library.
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    cov = gm.h3_cover(area, resolution=5)
    print("cell_rule:", cov.cell_rule)   # "overlap" — superset join keys
    print("cells:", len(cov.cells))

    # exact membership is built in — no separate refine step to remember:
    print("covers:", cov.covers(gm.Point(21.0, 52.0, crs=4326)))
    ```

`cell_rule` is `"center"` (h3-py's binning behavior), `"within"` (cells the area
fully owns), `"overlap"` (default — a complete-coverage superset, safe candidate
keys), or `"bbox"` (loosest). Exact membership is one method away on the same
object — `cov.covers(...)` / `.contains(...)` / `.intersects(...)`, plus the
`_xy` spellings for raw lon/lat streams — always answered against the source
geometry, never the cells. `gm.s2_cover(geom, level=..., max_cells=...)` is the S2
analogue.

### Cell-set algebra

Every grid family exposes its own prefixed `union` / `intersection` /
`difference` functions (`h3_*`, `s2_*`, `geohash_*`, `tile_*`). Coverages and
`CellArray`s also carry `compact()` / `uncompact(...)` for multi-resolution
rollups. Prefer the family algebra over plain Python `set`s so hierarchy-aware
behavior stays consistent.

```python
import gometry as gm

a = gm.s2_cover(gm.box(0.0, 0.0, 2.0, 2.0, crs=4326), level=8).cells
b = gm.s2_cover(gm.box(1.0, 1.0, 3.0, 3.0, crs=4326), level=8).cells
shared = gm.s2_intersection(a, b)     # also gm.s2_union / gm.s2_difference
rollup = shared.compact()             # coarsen full parents to one cell

h3_a = gm.h3_cover(gm.box(0.0, 0.0, 2.0, 2.0, crs=4326), resolution=5).cells
h3_b = gm.h3_cover(gm.box(1.0, 1.0, 3.0, 3.0, crs=4326), resolution=5).cells
_ = gm.h3_union(h3_a, h3_b)

```

Beyond coverages, the typed cells carry the rest of the ecosystem surface:
`H3Cell` exposes adjacency and local indexing (`neighbors`, `is_neighbor`,
`local_ij`, `base_cell`), with resolution metadata in the H3 function family
(`gm.h3_pentagons(resolution)`, `gm.h3_base_cells()`); `.polygon` returns one
cell's boundary polygon and `cells.polygon` a whole [`GeometryArray`][gometry.GeometryArray].
The h3-py and s2sphere symbol tables — point → cell, boundaries,
compact/uncompact, coverage membership — are in the
[cheatsheet](cheatsheet.md#h3-py).

## Coming from rtree & STRtree

rtree, Shapely's `STRtree`, and GeoPandas `sjoin` all accelerate spatial
predicates with a bounding-box prefilter. **Shapely 2.x and GeoPandas already
refine exactly** when you pass a predicate (`STRtree.query(..., predicate=...)`,
`sjoin(..., predicate=...)`); rtree's classic `intersection` path is bbox-only
unless you refine yourself. gometry's differentiators are not "we refine and they
don't" — they are:

- **Explicit stage names** — `candidates` (bbox) vs `query(..., predicate=...)`
  (exact), so the stage cannot be ambiguous.
- **Strict frame checks** — CRS and epoch must match; mixed frames raise rather
  than silently comparing across datums.
- **Missing-row semantics** — sparse/right-missing handles preserve row identity
  instead of dropping identity in the result.
- **Prepared caches** on boxed rows, plus a separate `geom.prepare()` for the
  one-geometry-many-tests shape.
- **Geodesic nearest / dwithin lanes** when the index frame is geographic
  (`unit=` selects CRS-natural meters vs planar).
- **`idx.explain(...)`** — a query plan for debugging candidate vs refine cost.

[`gm.SpatialIndex`][gometry.SpatialIndex] and [`gm.join`][gometry.join] are the
unified surfaces ([habit 4](#4-index-queries-name-the-stage-candidates-vs-refined)).

### Nearest

rtree (`Index.nearest`), Shapely (`STRtree.nearest` / `query_nearest`), and
gometry all expose nearest-neighbour APIs. Compare on ties, distances, return
shapes, mutability, and geodesic behavior — not on whether nearest exists.

=== "rtree / Shapely"

    ```python
    # rtree — bbox-based nearest by envelope
    nearest = list(idx.nearest(point.bounds, num_results=5))

    # Shapely 2.x — geometry nearest (+ distances via return_distance=)
    from shapely import STRtree
    tree = STRtree(geoms)
    idxs = tree.query_nearest(query, return_distance=True)
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    pts = gm.points([21.0, 30.0, 21.2], [52.0, 52.0, 52.1], crs=4326)
    target = gm.Point(21.0, 52.0, crs=4326)
    idx = gm.SpatialIndex(pts)
    print("nearest 2 (planar):", idx.nearest(target, k=2, unit="planar"))
    ```

`idx.nearest(query, k=..., unit=...)` returns the `k` closest indices
(`num_results` → `k`). Use `unit="planar"` for projected or CRS-free coordinates;
on a geographic CRS, metric units select geodesic point-nearest ordering. The
index is mutable (`insert`/`remove`) while still sharing one frame; Shapely's
`STRtree` is bulk-built and immutable after construction.

### Explainable plans

`idx.explain(...)` reports the steps the query planner took — candidate count,
refine predicate, geodesic vs planar path. That debugging surface has no direct
rtree/STRtree equivalent.

```python exec="on" source="block" result="text"
import gometry as gm

pts = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
idx = gm.SpatialIndex(pts)
for step in idx.explain(area, predicate="contains"):
    print("-", step)

```

### Spatial joins (`sjoin`)

GeoPandas [`sjoin`](https://geopandas.org/en/latest/docs/reference/api/geopandas.sjoin.html)
**does** apply the binary `predicate=` exactly (it is not a candidates-only API).
[`gm.join`][gometry.join] is the geometry-array analogue: prefilter → exact refine,
returning matched **index pairs** (not a joined DataFrame). Real differentiators
vs GeoPandas `sjoin`:

- pure geometry inputs / index-pair outputs (bring your own table join);
- CRS/epoch frame checks on both sides;
- query-plan diagnostics live on `SpatialIndex.explain(...)`;
- no pandas/GeoPandas dependency for the join itself.

=== "GeoPandas"

    ```python
    import geopandas
    result = geopandas.sjoin(points, polygons, predicate="within")
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    pts = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
    polys = gm.GeometryArray([gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)])
    print("pairs:", gm.join(pts, polys, predicate="within"))
    ```

`gm.SpatialIndex(geometries)` bulk-loads a balanced in-memory
[R-tree](https://en.wikipedia.org/wiki/R-tree) over geometry envelopes — suited to
static, query-heavy workloads — while `idx.insert(geometry)` / `idx.remove(...)`
handle dynamic sets without a full rebuild. The index is always in memory; gometry
never writes index files, so persisting across processes means rebuilding from the
source geometries (or their stored WKB/EWKB bytes). The full index/join symbol
table is in the [cheatsheet](cheatsheet.md#rtree-strtree-geopandas).

## What gometry covers

The table maps common specialized tools to the gometry surface that owns the
same **workflow**. Data-modeling layers (GeoJSON object models, dataframe
integrations like GeoPandas) stay adjacent: gometry is the engine they sit on.

| Package | What it does | gometry surface for the same workflow |
| --- | --- | --- |
| `shapely` | planar geometry, predicates, overlay | the whole geometry surface — `gm.Point`, `geom.buffer(...)`, `gm.intersection`, `gm.contains`, … |
| `pyproj` | CRS transforms, projection | `geom.to_crs`, [`gm.CRS`][gometry.CRS], `gm.crs_transform` |
| `geographiclib` | geodesic & rhumb solutions | `gm.rhumb_distance(point, other)`; use `path='rhumb'` with `gm.bearing`, `gm.destination`, or `gm.point_between` for constant-bearing navigation. |
| `pyclipper` | polygon buffer / offset / clip | `Geometry.buffer`, `Geometry.offset_curve`, `Geometry.clip_by_rect`, the overlay ops |
| `pygeohash` family | geohash encode/decode | `gm.GeohashCell`, `cell.polygon`, `gm.geohash_cover(geom, ...)` |
| `haversine` | point-to-point distance | `gm.distance(a, b)` on a geographic CRS ([exact geodesic](https://geographiclib.sourceforge.io/), not the haversine sphere) |
| `polyline` | Google encoded polyline | `gm.from_polyline`, `Geometry.to_polyline` |
| `mercantile` | XYZ tiles / quadkeys | `gm.Tile`, `gm.tile_cover(geom, ...)` |
| `s2sphere` | S2 cells & coverings | `gm.s2_*`, `gm.s2_cover(geom, ...)` |
| `h3` | H3 hexagonal cells | `gm.h3_*`, `gm.h3_cover(geom, ...)` |
| `openlocationcode` | plus codes | `gm.pluscode_encode`, `gm.pluscode_polygon`, `gm.pluscode_shorten`/`recover` |
| `utm` | lat/lon ↔ UTM | `geom.estimate_local_crs()` + `geom.to_crs(...)` |
| `rtree` / Shapely `STRtree` | bounding-box index | [`gm.SpatialIndex`][gometry.SpatialIndex], [`gm.join`][gometry.join] |

Every spelling on the right is a runnable call in this documentation — the
[cheatsheet](cheatsheet.md) lists the per-name mapping, and the sections above
walk the migration with side-by-side examples.
