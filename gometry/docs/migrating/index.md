---
description: Migrating to gometry from Shapely, pyproj, h3-py, s2sphere, and rtree with per-library symbol mappings and behavior notes.
---

# Migrating to gometry

gometry provides geometry, CRS, geodesy, grid, and spatial-index APIs for workflows
commonly split across **Shapely + pyproj + h3-py + s2sphere + rtree**. Canonical
symbol mappings are in the [cheatsheet](cheatsheet.md).

[CRS, units & measurement](../guide/crs.md), [Spatial indexing & joins](../guide/indexing.md),
and [Grids & geocodes](../guide/grids.md) define the core contracts.

## Coming from Shapely

gometry keeps Shapely's role as the in-process geometry type system — the same
seven [Simple Features](https://www.ogc.org/standard/sfa/) families, the same
predicates and constructive operations, the same WKB/WKT/GeoJSON interop and
`__geo_interface__`. Scalar predicates become **free functions** (`gm.contains(a, b)`, vectorized over
arrays), metrics read the CRS, and the kernels are GEOS-free. Behavior differences
are validity, CRS-aware metrics, and index refinement.

### `make_valid` → `repair`

[`repair`][gometry.Geometry.repair] is the canonical spelling for the ecosystem
concept called `make_valid` / `ST_MakeValid` / `GEOSMakeValid`. `validate()`
returns a structured report with the reason and location, where Shapely splits the
job across `is_valid`, `explain_validity`, and `make_valid`.

=== "Shapely"

    ```python title="partial: source ecosystem example"
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

### CRS-aware `.area`, `.distance`, and `.buffer`

On lon/lat data Shapely's bare
`.area` returns degrees², `.distance` returns degrees, and `.buffer(100)` buffers
by 100 *degrees*. gometry has one `geom.area` (and one `gm.distance` /
`Geometry.buffer`) whose answer is decided by the geometry's CRS and is native by
default — a geographic CRS measures geodesically (meters), a projected CRS in its
**native linear units**, CRS-free geometry in bare coordinate units.

=== "Shapely (silently wrong on lon/lat)"

    ```python title="partial: source ecosystem example"
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
exposes **two acceleration paths**:

- [`SpatialIndex`][gometry.SpatialIndex] — many geometries, query/join/nearest
  with explicit `candidates` vs `query(..., predicate=...)`, plus frame checks,
  missing-row semantics, and geodesic nearest lanes.
- [`geom.prepare()`][gometry.Geometry.prepare] — one geometry tested many times
  (cached segment index); still a separate object, not folded into the index.

=== "Shapely"

    ```python title="partial: source ecosystem example"
    from shapely import STRtree, prepare
    tree = STRtree(geoms)
    candidates = tree.query(area)                       # bbox candidates
    hits = tree.query(area, predicate="contains")       # refined (2.x)

    prepare(poly)
    poly.contains(pt)                                   # prepared predicate path
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
    print("prepared contains first point:", gm.contains(prep, pts[0]))
    ```

Build through the geometry classes
(`gm.Point`, `gm.Polygon`, …) with explicit `crs=`, pack arrays with the plural
builders (`gm.points`, `gm.polygons`) or `gm.GeometryArray([...])`, and ingest
GeoJSON / `__geo_interface__` with `gm.from_geojson`. Readers are verb-prefixed
free functions (`gm.from_wkb`, `gm.from_wkt`), and encoders are methods on the
geometry (`geom.to_wkb(include_srid=True)`, `geom.to_wkt()`, `geom.to_geojson()`,
`geom.to_arrow()` for [GeoArrow](https://geoarrow.org/) columnar interchange).
Vertex-subset ops (`simplify`, `convex_hull`, triangulation) preserve Z/M where
Shapely drops it. The Shapely symbol table — construction, inspection,
predicates, constructive ops, linear referencing, validation, and IO — is in the
[cheatsheet](cheatsheet.md#shapely).

## Coming from pyproj

pyproj owns two jobs: **CRS transformation** (the `CRS` / `Transformer` objects
over libPROJ) and **geodesy** (the `Geod` object). gometry keeps
[PROJ](https://proj.org/) as the authority backend, bundled behind the API, and
exposes transformations through `to_crs` or the stateless
[`gm.crs_transform`][gometry.crs_transform]. Point-navigation free functions
provide geodesy on geographic CRSs. Coordinate boundaries are always X/Y.

### `Transformer` → `to_crs` and `gm.crs_transform`

For geometry, transform via the geometry. For raw coordinate values, use the
stateless [`gm.crs_transform`][gometry.crs_transform] — there is no transformer
object to construct, cache, or reuse.

=== "pyproj"

    ```python title="partial: source ecosystem example"
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
WGS 84 ellipsoidal geodesy) maps onto `gm.bearing`, `point.destination`,
`gm.point_between`, and the geometry metrics. On a **geographic** CRS these compute
geodesically on the ellipsoid; there is no `Geod` object to construct. For a
non-WGS 84 ellipsoid, [`gm.CRS`][gometry.CRS]`(code).geodesic_inverse(...)` runs the
geodesic problem on that CRS's own ellipsoid.

=== "pyproj Geod"

    ```python title="partial: source ecosystem example"
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
    print("destination:  ", a.destination(90.0, 1000.0))
    print("midpoint:     ", gm.point_between(a, b, 0.5, normalized=True))
    print("area (m^2):   ", round(area.area))
    print("perimeter (m):", round(area.length))
    ```

The `gm.CRS` surface (`is_*` classification, standards export, catalog discovery,
and coordinate operations) and the common `Geod` mappings are in the
[cheatsheet](cheatsheet.md#pyproj).

## Coming from h3-py & s2sphere

h3-py and s2sphere both turn geometry into compact cell IDs for aggregation,
joins, partitioning, and coarse pre-filtering. gometry unifies
[H3](https://h3geo.org/) and [S2](https://s2geometry.io/) under typed cells and
CellArray, and makes the cell-selection rule explicit. The returned cells are
candidate keys; keep the source geometry for exact predicates.

### `polyfill` → `cover` with an explicit rule

h3-py's `polygon_to_cells` (formerly `polyfill`) has an implicit containment rule.
gometry names the rule and returns a typed `CellArray`.

=== "h3-py"

    ```python title="partial: h3-py example"
    import h3
    cells = h3.polygon_to_cells(h3.LatLngPoly(shell), 8)  # which rule? center? overlap?
    # exact membership? do it yourself, per point, with another library.
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    cov = gm.h3_cover(area, resolution=5)
    print("cells:", len(cov))             # overlap is the default factory rule

    # Keep the source geometry for exact checks:
    print("covers:", gm.covers(area, gm.Point(21.0, 52.0, crs=4326)))
    ```

`cell_rule` is `"center"` (h3-py's binning behavior), `"within"` (cells the area
fully owns), `"overlap"` (default — a complete-coverage superset, safe candidate
keys), or `"bbox"` (loosest). Use the free predicates
`gm.covers(...)` / `gm.contains(...)` / `gm.intersects(...)` against the source
geometry when exact geometry answers are needed. `gm.s2_cover(geom, level=...)`
is the S2 analogue.

### Cell-set algebra

Every grid family exposes its own prefixed `union` / `intersection` /
`difference` functions (`h3_*`, `s2_*`, `geohash_*`, `tile_*`). `CellArray`s
also carry `compact()` / `uncompact(...)` for multi-resolution
rollups. Family algebra preserves hierarchy-aware behavior that plain Python
`set`s do not.

```python title="partial: source ecosystem example"
import gometry as gm

a = gm.s2_cover(gm.box(0.0, 0.0, 2.0, 2.0, crs=4326), level=8)
b = gm.s2_cover(gm.box(1.0, 1.0, 3.0, 3.0, crs=4326), level=8)
a_present = [cell for cell in a if cell is not None]
b_present = [cell for cell in b if cell is not None]
shared = gm.s2_intersection(a_present, b_present)  # also gm.s2_union / gm.s2_difference
rollup = shared.compact()             # coarsen full parents to one cell

h3_a = gm.h3_cover(gm.box(0.0, 0.0, 2.0, 2.0, crs=4326), resolution=5)
h3_b = gm.h3_cover(gm.box(1.0, 1.0, 3.0, 3.0, crs=4326), resolution=5)
h3_a_present = [cell for cell in h3_a if cell is not None]
h3_b_present = [cell for cell in h3_b if cell is not None]
_ = gm.h3_union(h3_a_present, h3_b_present)

```

`H3Cell` exposes adjacency and local indexing (`neighbors`, `is_neighbor`,
`local_ij`, `base_cell`), with resolution metadata in the H3 function family
(`gm.h3_pentagons(resolution)`, `gm.h3_base_cells()`); `.polygon` returns one
cell's boundary polygon and `cells.polygon` a whole [`GeometryArray`][gometry.GeometryArray].
The h3-py and s2sphere symbol tables — point → cell, boundaries, and
compact/uncompact — are in the
[cheatsheet](cheatsheet.md#h3-py).

## Coming from rtree & STRtree

rtree, Shapely's `STRtree`, and GeoPandas `sjoin` all accelerate spatial
predicates with a bounding-box prefilter. **Shapely 2.x and GeoPandas already
refine exactly** when you pass a predicate (`STRtree.query(..., predicate=...)`,
`sjoin(..., predicate=...)`); rtree's `intersection` path is bbox-only
unless you refine yourself. gometry exposes the corresponding stages and adds
frame, row-identity, and plan diagnostics:

- **Explicit stage names** — `candidates` (bbox) vs `query(..., predicate=...)`
  (exact).
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
unified surfaces; see [Spatial indexing & joins](../guide/indexing.md) for the
candidate/refine contract.

### Nearest

rtree (`Index.nearest`), Shapely (`STRtree.nearest` / `query_nearest`), and
gometry all expose nearest-neighbour APIs.

=== "rtree / Shapely"

    ```python title="partial: source ecosystem example"
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
(`num_results` → `k`). `unit="planar"` selects projected or CRS-free coordinate
units; on a geographic CRS, metric units select geodesic point-nearest ordering.
The index is mutable (`insert`/`remove`) while still sharing one frame; Shapely's
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
returning matched **index pairs** (not a joined DataFrame). `gm.join` provides:

- pure geometry inputs and index-pair outputs (bring your own table join);
- CRS/epoch frame checks on both sides;
- query-plan diagnostics live on `SpatialIndex.explain(...)`;
- no pandas/GeoPandas dependency for the join itself.

=== "GeoPandas"

    ```python title="partial: source ecosystem example"
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
source geometries (or their stored WKB/EWKB bytes). The index/join symbol
table is in the [cheatsheet](cheatsheet.md#rtree-strtree-geopandas).

## What gometry covers

Data-modeling layers (GeoJSON object models and dataframe integrations such as
GeoPandas) remain adjacent; gometry is the geometry engine they use.

| Package | What it does | gometry surface for the same workflow |
| --- | --- | --- |
| `shapely` | planar geometry, predicates, overlay | the whole geometry surface — `gm.Point`, `geom.buffer(...)`, `gm.intersection`, `gm.contains`, … |
| `pyproj` | CRS transforms, projection | `geom.to_crs`, [`gm.CRS`][gometry.CRS], `gm.crs_transform` |
| `geographiclib` | geodesic & rhumb solutions | `gm.rhumb_distance(point, other)`; use `path='rhumb'` with `gm.bearing`, `point.destination`, or `gm.point_between` for constant-bearing navigation. |
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
