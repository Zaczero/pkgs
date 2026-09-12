---
description: Migrating to gometry from Shapely, pyproj, h3-py, s2sphere, and rtree with per-library symbol mappings and behavior notes.
---

# Migrating to gometry

gometry provides geometry, CRS, geodesy, grid, and spatial-index APIs for workflows
commonly split across **Shapely + pyproj + h3-py + s2sphere + rtree**.

[:octicons-arrow-right-24: Migration cheatsheet](cheatsheet.md){ .md-button .md-button--primary }

The cheatsheet is the canonical old-symbol → gometry-name lookup. This guide owns
workflow migration and links the behavior contracts for [CRS, units & measurement](../guide/crs.md),
[Spatial indexing & joins](../guide/indexing.md), and [Grids & geocodes](../guide/grids.md).

## Coming from Shapely

gometry keeps Shapely's role as the in-process geometry type system — the same
seven [Simple Features](https://www.ogc.org/standard/sfa/) families, the same
predicates and constructive operations, the same WKB/WKT/GeoJSON interop and
`__geo_interface__`. Scalar predicates become **free functions** ([`gm.contains(a, b)`][gometry.contains], vectorized over
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
    print("valid:", bowtie.is_valid)
    report = bowtie.validate()
    print("reason:", report.reason)
    fixed = bowtie.repair()
    print("repaired:", fixed.geometry_type, "| now valid:", fixed.is_valid)
    ```

### CRS-aware `.area`, `.distance`, and `.buffer`

On degree-valued lon/lat data Shapely's bare
`.area` returns degrees², `.distance` returns degrees, and `.buffer(100)` buffers
by 100 *degrees*. gometry has one [`geom.area`][gometry.Geometry.area] (and one [`gm.distance`][gometry.distance] /
[`Geometry.buffer`][gometry.Geometry.buffer]) whose answer is decided by the geometry's CRS and is native by
default — a geographic CRS measures geodesically (meters), a projected CRS in its
**native linear units**, CRS-free geometry in bare coordinate units.

=== "Shapely (silently wrong on lon/lat)"

    ```python title="partial: source ecosystem example"
    poly.area
    a.distance(b)
    poly.buffer(100)
    ```

=== "gometry (the CRS decides)"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    a = gm.Point(21.0, 52.0, crs=4326)
    b = gm.Point(22.0, 52.0, crs=4326)
    print("geodesic area (m²):", round(area.area))
    print("geodesic distance (m):", round(gm.distance(a, b)))
    print("projected area (m²):", round(area.to_crs(area.estimate_local_crs()).area))
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
    candidates = tree.query(area)
    hits = tree.query(area, predicate="contains")

    prepare(poly)
    poly.contains(pt)
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    pts = gm.points([20.25, 21.0, 30.0], [51.25, 52.0, 52.0], crs=4326)
    area = gm.Polygon(
        [(20.0, 51.0), (22.0, 51.0), (22.0, 53.0), (20.0, 53.0), (20.0, 51.0)],
        holes=[[(20.5, 51.5), (21.5, 51.5), (21.5, 52.5), (20.5, 52.5), (20.5, 51.5)]],
        crs=4326,
    )
    idx = gm.SpatialIndex(pts)
    print("candidates:", idx.candidates(area).tolist())
    print("refined:", idx.query(area, predicate="contains").tolist())
    prep = area.prepare()
    print("prepared contains first point:", gm.contains(prep, pts[0]))
    ```

Build through the geometry classes
([`gm.Point`][gometry.Point], [`gm.Polygon`][gometry.Polygon], …) with explicit `crs=`, pack arrays with the plural
builders ([`gm.points`][gometry.points], [`gm.polygons`][gometry.polygons]) or [`gm.GeometryArray([...])`][gometry.GeometryArray], and ingest
GeoJSON / `__geo_interface__` with [`gm.from_geojson`][gometry.from_geojson]. Readers are verb-prefixed
free functions ([`gm.from_wkb`][gometry.from_wkb], [`gm.from_wkt`][gometry.from_wkt]), and encoders are methods on the
geometry ([`geom.to_wkb(include_srid=True)`][gometry.Geometry.to_wkb], [`geom.to_wkt()`][gometry.Geometry.to_wkt], [`geom.to_geojson()`][gometry.Geometry.to_geojson],
[`geom.to_arrow()`][gometry.Geometry.to_arrow] for [GeoArrow](https://geoarrow.org/) columnar interchange).
Vertex-subset ops (`simplify`, `convex_hull`, triangulation) preserve Z/M where
Shapely drops it. Hulls copy the first input vertex at a duplicated XY.

## Coming from pyproj

pyproj owns two jobs: **CRS transformation** (the `CRS` / `Transformer` objects
over libPROJ) and **geodesy** (the `Geod` object). gometry keeps
[PROJ](https://proj.org/) as the authority backend, bundled behind the API, and
exposes transformations through `to_crs` or the stateless
[`gm.crs_transform`][gometry.crs_transform]. Point-navigation free functions
provide geodesy on geographic CRSs. Coordinate boundaries are always X/Y.

### `Transformer` → `to_crs` and `gm.crs_transform`

For geometry, transform via the geometry. For raw coordinate values, use the
stateless `gm.crs_transform` — there is no transformer
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
WGS 84 ellipsoidal geodesy) maps onto [`gm.bearing`][gometry.bearing], `point.destination`,
[`gm.point_between`][gometry.point_between], and the geometry metrics. On a **geographic** CRS these compute
geodesically on the ellipsoid; there is no `Geod` object to construct. For a
non-WGS 84 ellipsoid, [`gm.CRS`][gometry.CRS]`(code).geodesic_inverse(...)` runs the
geodesic problem on that CRS's own ellipsoid.

=== "pyproj Geod"

    ```python title="partial: source ecosystem example"
    from pyproj import Geod
    g = Geod(ellps="WGS84")
    dist = g.inv(lon1, lat1, lon2, lat2)[2]
    az = g.inv(lon1, lat1, lon2, lat2)[0]
    lon2, lat2, _ = g.fwd(lon1, lat1, az, dist)
    length = g.line_length(lons, lats)
    area, perim = g.geometry_area_perimeter(poly)
    pts = g.npts(lon1, lat1, lon2, lat2, n)
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    a = gm.Point(21.0, 52.0, crs=4326)
    b = gm.Point(22.0, 52.0, crs=4326)
    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)

    print("distance (m):", round(gm.distance(a, b)))  # geographic → geodesic
    print("bearing (deg):", round(gm.bearing(a, b), 1))
    print("destination:", a.destination(90.0, 1000.0))
    print("midpoint:", gm.point_between(a, b, 0.5, normalized=True))
    print("area (m²):", round(area.area))
    print("perimeter (m):", round(area.length))
    ```

The `gm.CRS` surface includes `is_*` classification, standards export, catalog
discovery, and coordinate operations.

## Coming from h3-py & s2sphere

h3-py and s2sphere both turn geometry into compact cell IDs for aggregation,
joins, partitioning, and coarse pre-filtering. gometry unifies
[H3](https://h3geo.org/) and [S2](https://s2geometry.io/) under typed cells and
CellArray, and makes the cell-selection rule explicit. The returned cells are
candidate keys; keep the source geometry for exact predicates.

### `polyfill` → `cover` with an explicit rule

h3-py's `polygon_to_cells` (formerly `polyfill`) has an implicit containment rule.
gometry names the rule and returns a typed [`CellArray`][gometry.CellArray].

=== "h3-py"

    ```python title="partial: h3-py example"
    import h3
    cells = h3.polygon_to_cells(h3.LatLngPoly(shell), 8)  # containment rule is implicit
    # Exact membership is not provided.
    ```

=== "gometry"

    ```python exec="on" source="block" result="text"
    import gometry as gm

    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    cov = gm.h3_cover(area, resolution=5)
    print("cells:", len(cov))

    # A cover is a candidate set; retain the source for exact predicates.
    print("covers:", gm.covers(area, gm.Point(21.0, 52.0, crs=4326)))
    ```

`cell_rule` is `"within"` (cells the area fully owns), `"center"` (h3-py's
binning behavior), `"overlap"` (default — a complete-coverage superset, safe
candidate keys), or `"bbox"` (loosest). Use the free predicates
[`gm.covers(...)`][gometry.covers] / [`gm.contains(...)`][gometry.contains] / [`gm.intersects(...)`][gometry.intersects] against the source
geometry when exact geometry answers are needed. [`gm.s2_cover(geom, level=...)`][gometry.s2_cover]
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
shared = gm.s2_intersection(a_present, b_present)
rollup = shared.compact()

h3_a = gm.h3_cover(gm.box(0.0, 0.0, 2.0, 2.0, crs=4326), resolution=5)
h3_b = gm.h3_cover(gm.box(1.0, 1.0, 3.0, 3.0, crs=4326), resolution=5)
h3_a_present = [cell for cell in h3_a if cell is not None]
h3_b_present = [cell for cell in h3_b if cell is not None]
_ = gm.h3_union(h3_a_present, h3_b_present)

```

[`H3Cell`][gometry.H3Cell] exposes adjacency and local indexing (`neighbors`, `is_neighbor`,
`local_ij`, `base_cell`), with resolution metadata in the H3 function family
([`gm.h3_pentagons(resolution)`][gometry.h3_pentagons], [`gm.h3_base_cells()`][gometry.h3_base_cells]); `.polygon` returns one
cell's boundary polygon and `cells.polygon` a whole [`GeometryArray`][gometry.GeometryArray].
The cell classes provide point-to-cell construction, boundaries, and
compact/uncompact operations.

## Coming from rtree & STRtree

rtree, Shapely's `STRtree`, and GeoPandas `sjoin` all accelerate spatial
predicates with a bounding-box prefilter. **Shapely 2.x and GeoPandas already
refine exactly** when you pass a predicate (`STRtree.query(..., predicate=...)`,
`sjoin(..., predicate=...)`); rtree's `intersection` path is bbox-only
unless you refine yourself. gometry exposes the corresponding stages and adds
frame and row-identity checks:

- **Explicit stage names** — `candidates` (bbox) vs `query(..., predicate=...)`
  (exact).
- **Strict frame checks** — CRS and epoch must match; mixed frames raise rather
  than silently comparing across datums.
- **Missing-row semantics** — sparse/right-missing handles preserve row identity
  instead of dropping identity in the result.
- **Prepared caches** on boxed rows, plus a separate [`geom.prepare()`][gometry.Geometry.prepare] for the
  one-geometry-many-tests shape.
- **Geodesic nearest / dwithin lanes** when the index frame is geographic
  (`unit=` selects CRS-natural meters vs planar).

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
    print("nearest 2 (planar):", idx.nearest(target, k=2, unit="planar").tolist())
    ```

[`idx.nearest(query, k=..., unit=...)`][gometry.SpatialIndex.nearest] returns the `k` closest indices
(`num_results` → `k`). `unit="planar"` selects projected or CRS-free coordinate
units; on a geographic CRS, metric units select geodesic point-nearest ordering.
The index is mutable (`insert`/`remove`) while still sharing one frame; Shapely's
`STRtree` is bulk-built and immutable after construction.

### Candidate selectivity

`idx.candidates(...)` exposes the bounding-box prefilter, while
`idx.query(..., predicate=...)` performs exact refinement. Compare their lengths
to see how much work the envelope filter leaves for the predicate:

```python exec="on" source="block" result="text"
import gometry as gm

pts = gm.points([20.25, 21.0, 30.0], [51.25, 52.0, 52.0], crs=4326)
area = gm.Polygon(
    [(20.0, 51.0), (22.0, 51.0), (22.0, 53.0), (20.0, 53.0), (20.0, 51.0)],
    holes=[[(20.5, 51.5), (21.5, 51.5), (21.5, 52.5), (20.5, 52.5), (20.5, 51.5)]],
    crs=4326,
)
idx = gm.SpatialIndex(pts)
candidate_count = len(idx.candidates(area))
refined_count = len(idx.query(area, predicate="contains"))
print("candidate count:", candidate_count)
print("refined count:", refined_count)

```

### Spatial joins (`sjoin`)

GeoPandas [`sjoin`](https://geopandas.org/en/latest/docs/reference/api/geopandas.sjoin.html)
**does** apply the binary `predicate=` exactly (it is not a candidates-only API).
`gm.join` is the geometry-array analogue: prefilter → exact refine,
returning matched **index pairs** (not a joined DataFrame). `gm.join` provides:

- pure geometry inputs and index-pair outputs (bring your own table join);
- CRS/epoch frame checks on both sides;
- candidate and refined counts are directly observable with `len(idx.candidates(query))` and `len(idx.query(query, predicate=...))`;
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
    left, right = gm.join(pts, polys, predicate="within")
    print("pairs:", list(zip(left.tolist(), right.tolist(), strict=True)))
    ```

`gm.SpatialIndex(geometries)` bulk-loads a balanced in-memory
[R-tree](https://en.wikipedia.org/wiki/R-tree) over geometry envelopes — suited to
static, query-heavy workloads — while [`idx.insert(geometry)`][gometry.SpatialIndex.insert] / [`idx.remove(...)`][gometry.SpatialIndex.remove]
handle dynamic sets without a full rebuild. The index is always in memory; gometry
never writes index files, so persisting across processes means rebuilding from the
source geometries (or their stored WKB/EWKB bytes).

## What gometry covers

Data-modeling layers (GeoJSON object models and dataframe integrations such as
GeoPandas) remain adjacent; gometry is the geometry engine they use.

| Package | What it does | gometry surface for the same workflow |
| --- | --- | --- |
| `shapely` | planar geometry, predicates, overlay | the whole geometry surface — [`gm.Point`][gometry.Point], [`geom.buffer(...)`][gometry.Geometry.buffer], [`gm.intersection`][gometry.intersection], [`gm.contains`][gometry.contains], … |
| `pyproj` | CRS transforms, projection | [`geom.to_crs`][gometry.Geometry.to_crs], [`gm.CRS`][gometry.CRS], [`gm.crs_transform`][gometry.crs_transform] |
| `geographiclib` | geodesic & rhumb solutions | [`gm.rhumb_distance(point, other)`][gometry.rhumb_distance]; use `path='rhumb'` with [`gm.bearing`][gometry.bearing], `point.destination`, or [`gm.point_between`][gometry.point_between] for constant-bearing navigation. |
| `pyclipper` | polygon buffer / offset / clip | `Geometry.buffer`, [`Geometry.offset_curve`][gometry.Geometry.offset_curve], [`Geometry.clip_by_rect`][gometry.Geometry.clip_by_rect], the overlay ops |
| `pygeohash` family | geohash encode/decode | [`gm.GeohashCell`][gometry.GeohashCell], [`cell.polygon`][gometry.Cell.polygon], [`gm.geohash_cover(geom, ...)`][gometry.geohash_cover] |
| `haversine` | point-to-point distance | [`gm.distance(a, b)`][gometry.distance] on a geographic CRS ([ellipsoidal geodesic](https://geographiclib.sourceforge.io/), not the haversine sphere) |
| `polyline` | Google encoded polyline | [`gm.from_polyline`][gometry.from_polyline], [`Geometry.to_polyline`][gometry.Geometry.to_polyline] |
| `mercantile` | XYZ tiles / quadkeys | [`gm.Tile`][gometry.Tile], [`gm.tile_cover(geom, ...)`][gometry.tile_cover] |
| `s2sphere` | S2 cells & coverings | `gm.s2_*`, [`gm.s2_cover(geom, ...)`][gometry.s2_cover] |
| `h3` | H3 hexagonal cells | `gm.h3_*`, [`gm.h3_cover(geom, ...)`][gometry.h3_cover] |
| `openlocationcode` | plus codes | [`gm.pluscode_encode`][gometry.pluscode_encode], [`gm.pluscode_polygon`][gometry.pluscode_polygon], [`gm.pluscode_shorten`][gometry.pluscode_shorten]/`recover` |
| `utm` | lat/lon ↔ UTM | [`geom.estimate_local_crs()`][gometry.Geometry.estimate_local_crs] + `geom.to_crs(...)` |
| `rtree` / Shapely `STRtree` | bounding-box index | [`gm.SpatialIndex`][gometry.SpatialIndex], [`gm.join`][gometry.join] |
