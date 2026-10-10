---
description: Searchable old-symbol → gometry-spelling reference tables, grouped by source library (Shapely, pyproj, h3-py, s2sphere, rtree).
---

# Migration cheatsheet

Use this page only to look up an old source-library symbol and its gometry name.
For a migration workflow, read [Migrating to gometry](index.md). For behavior,
defaults, units, errors, and output shapes, use the canonical owners:
[CRS, units & measurement](../guide/crs.md),
[Spatial indexing & joins](../guide/indexing.md), [Grids & geocodes](../guide/grids.md),
[Arrays & performance](../guide/arrays.md), and the linked generated API reference.

Conventions: `import gometry as gm`; `geom` is a gometry geometry; and
`idx = gm.SpatialIndex(...)`.

## Shapely

### Construction

| Shapely | gometry | Notes |
|---|---|---|
| `Point(x, y)` | [`gm.Point(x, y, crs=...)`][gometry.Point] | Same constructor always; under a geographic CRS, `(x, y)` is `(lon, lat)`. `z=` / `m=` for Z/M. |
| `LineString(coords)` | [`gm.LineString(coords, z=..., m=..., crs=...)`][gometry.LineString] | |
| `Polygon(shell, holes)` | [`gm.Polygon(shell, holes, crs=...)`][gometry.Polygon] | |
| `MultiPoint(...)` | [`gm.MultiPoint(...)`][gometry.MultiPoint] | |
| `MultiLineString(...)` | [`gm.MultiLineString(...)`][gometry.MultiLineString] | |
| `MultiPolygon(...)` | [`gm.MultiPolygon(...)`][gometry.MultiPolygon] | |
| `GeometryCollection(...)` | [`gm.GeometryCollection(...)`][gometry.GeometryCollection] | |
| `box(minx, miny, maxx, maxy)` | [`gm.box(minx, miny, maxx, maxy, crs=..., wrap=...)`][gometry.box] | `wrap="split"` for antimeridian boxes. |
| `shape(geojson)` | [`gm.from_geojson(obj)`][gometry.from_geojson] | GeoJSON / `__geo_interface__`. |
| `shapely.points(...)` | [`gm.points(lons, lats, z=..., m=..., crs=...)`][gometry.points] | Packed array. |
| mixed array | [`gm.GeometryArray([...])`][gometry.GeometryArray] | Rust-owned container for vectorized ops. |

### Inspection

| Shapely | gometry | Notes |
|---|---|---|
| `geom.geom_type` | [`geom.geometry_type`][gometry.Geometry.geometry_type] | |
| `geom.bounds` | [`geom.bounds`][gometry.Geometry.bounds] | An array gives an `(n, 4)` matrix through [`arr.bounds`][gometry.GeometryArray.bounds]. |
| `geom.has_z` | [`geom.has_z`][gometry.Geometry.has_z] / [`geom.has_m`][gometry.Geometry.has_m] | |
| — | [`geom.coordinate_axes`][gometry.Geometry.coordinate_axes] | |
| — | [`geom.epoch`][gometry.Geometry.epoch] | |
| `geom.crs` (n/a in Shapely) | [`geom.crs`][gometry.Geometry.crs] | |
| `shapely.get_coordinates(geom)` | [`geom.coords`][gometry.Geometry.coords] (`list(...)`, `.x`/`.y`, `.row_index`) | Use `np.asarray(view, dtype=np.float64)` for the flat matrix; `view.row_index` maps array coordinates to source rows. |
| `get_parts(...)` | [`geom.parts`][gometry.Geometry.parts] | A scalar gives a sequence view; an array gives row-preserving `Groups`. |
| `get_rings(...)` | [`polygon.exterior`][gometry.Polygon.exterior] / [`polygon.interiors`][gometry.Polygon.interiors] | These expose stored shell and hole rings. For a MultiPolygon, inspect each polygon in `geom.parts`; use [`geom.boundary()`][gometry.Geometry.boundary] for the topological boundary of occupied support. |
| `str(geom)` / `f"{geom:.2f}"` / `f"{geom:x}"` | same | WKT text; `.Nf`/`.Ng` round for display, `x`/`X` hex WKB. |
| `bool(geom)` | same | Falsy when empty. |
| `match geom: case Point(x, y):` | `match geom: case gm.Point(x, y):` | **gometry addition** — leaves carry `__match_args__`; Shapely 2.x has no positional match on `Point`. Arrays match `[a, *rest]`. |

### Predicates

| Shapely | gometry |
|---|---|
| `a.contains(b)` / `shapely.contains(a, b)` | [`gm.contains(a, b)`][gometry.contains] |
| `a.contains_properly(b)` / [`gm.contains_properly(prep, b)`][gometry.contains_properly] | `gm.contains_properly(a, b)` (also prepared, and as an index/join predicate) |
| `a.within(b)` / `shapely.within(a, b)` | [`gm.within(a, b)`][gometry.within] |
| `a.intersects(b)` / `shapely.intersects(a, b)` | [`gm.intersects(a, b)`][gometry.intersects] |
| `a.covers(b)` / `a.covered_by(b)` | [`gm.covers(a, b)`][gometry.covers] / [`gm.covered_by(a, b)`][gometry.covered_by] |
| `a.disjoint(b)` | [`gm.disjoint(a, b)`][gometry.disjoint] |
| `a.touches(b)` / `a.crosses(b)` / `a.overlaps(b)` | [`gm.touches(a, b)`][gometry.touches] / [`gm.crosses(a, b)`][gometry.crosses] / [`gm.overlaps(a, b)`][gometry.overlaps] |
| `a.equals(b)` | [`gm.equals(a, b)`][gometry.equals] |
| normalized equality | [`gm.equals_exact(a.normalize(), b.normalize())`][gometry.equals_exact] |
| `a.relate(b)` / `a.relate_pattern(b, p)` | [`gm.relate(a, b)`][gometry.relate] / [`gm.relate_pattern(a, b, p)`][gometry.relate_pattern] |
| `shapely.contains_xy(g, x, y)` | [`gm.contains(g, x=x, y=y)`][gometry.contains] |
| `shapely.intersects_xy(g, x, y)` | [`gm.intersects(g, x=x, y=y)`][gometry.intersects] |
| `a.dwithin(b, d)` | [`gm.dwithin(a, b, distance, unit=…)`][gometry.dwithin] / [`idx.query(..., predicate="dwithin", distance=..., unit=...)`][gometry.SpatialIndex.query] |

### Measurement

| Shapely | gometry | Notes |
|---|---|---|
| `geom.area` | [`geom.area`][gometry.Geometry.area] / [`geoms.area`][gometry.GeometryArray.area] | A `GeometryCollection` counts overlapping members once (the area of their union); Shapely sums member areas. |
| `geom.length` | [`geom.length`][gometry.Geometry.length] / [`geoms.length`][gometry.GeometryArray.length] | For a `GeometryCollection`, sums every member's length or perimeter, including overlaps; Shapely agrees. |
| `a.distance(b)` / `shapely.distance(a, b)` | [`gm.distance(a, b)`][gometry.distance] |
| change the metric frame / units | [`geom.to_crs(target)`][gometry.Geometry.to_crs] / [`gm.area(geom, unit=…)`][gometry.area] |
| `minimum_clearance(g)` | [`g.minimum_clearance()`][gometry.Geometry.minimum_clearance] |
| `hausdorff_distance` / `frechet_distance` | [`gm.hausdorff_distance(...)`][gometry.hausdorff_distance] / [`gm.frechet_distance(...)`][gometry.frechet_distance] | Planar Hausdorff uses a certified relative default; a supplied positive finite `max_error` is an absolute output-unit bound. Geographic Hausdorff measures point and linework supports over straight chart edges and requires `max_error` to be omitted. |

### Constructive operations

| Shapely | gometry | Notes |
|---|---|---|
| `a.intersection(b)` / `shapely.intersection(a, b)` / `a & b` | [`gm.intersection(a, b)`][gometry.intersection] / `a & b` | |
| `a.union(b)` / `shapely.union(a, b)` / <code>a \| b</code> | [`gm.union(a, b)`][gometry.union] / <code>a \| b</code> | |
| `a.difference(b)` / `shapely.difference(a, b)` / `a - b` | [`gm.difference(a, b)`][gometry.difference] / `a - b` | |
| `a.symmetric_difference(b)` / `a ^ b` | [`gm.symmetric_difference(a, b)`][gometry.symmetric_difference] / `a ^ b` | |
| `unary_union(geoms)` | [`gm.union_all(geoms)`][gometry.union_all] | Rejects conflicting CRS. |
| `intersection_all(geoms)` | [`gm.intersection_all(geoms)`][gometry.intersection_all] | Common region of all inputs. |
| `symmetric_difference_all(geoms)` | [`gm.symmetric_difference_all(geoms)`][gometry.symmetric_difference_all] | Region in an odd number of inputs. |
| `geom.buffer(d)` / `shapely.buffer(geom, d)` | [`geom.buffer(d)`][gometry.Geometry.buffer] | `quadrant_segments` corresponds to Shapely's `quad_segs` and GeoPandas' `resolution`; `miter_limit` corresponds to Shapely's `mitre_limit`. gometry defaults to 8 segments per quadrant; GeoPandas defaults to 16, so use `quadrant_segments=16` to retain that approximation. |
| `geom.buffer(d, single_sided=True)` | `geom.buffer(abs(d), side='left')` for positive `d`; `geom.buffer(abs(d), side='right')` for negative `d` | Sided buffers use flat ends and honor `join_style`, `quadrant_segments`, and `miter_limit`; gometry expresses the side explicitly and requires a non-negative distance. |
| `geom.simplify(t)` / `shapely.simplify(geom, t)` | [`geom.simplify(t)`][gometry.Geometry.simplify] | |
| `geom.offset_curve(d)` | [`geom.offset_curve(d)`][gometry.Geometry.offset_curve] | |
| `clip_by_rect(g, ...)` | `g.clip_by_rect(...)` | |
| `geom.convex_hull` | [`geom.convex_hull()`][gometry.Geometry.convex_hull] | |
| `concave_hull(g, ratio=...)` | [`geom.concave_hull(concavity=..., length_threshold=...)`][gometry.Geometry.concave_hull] | Different algorithm and knobs: `concavity` is an edge-length ratio, not Shapely's `ratio`. |
| `geom.centroid` / `geom.envelope` | `g.centroid()` / `g.envelope()` | |
| `geom.point_on_surface()` | `g.point_on_surface()` | |
| `polylabel(g)` | `g.polylabel()` | [Pole of inaccessibility](https://en.wikipedia.org/wiki/Pole_of_inaccessibility). |
| `maximum_inscribed_circle(g)` | `g.maximum_inscribed_circle()` | Shapely returns a two-point `LineString` from center to boundary; gometry returns a filled polygonal approximation (or a `Point` for a degenerate radius). Use `disk.centroid()` for the returned disk's center and `g.maximum_inscribed_radius()` for its numerical radius; pass the same `tolerance` and `unit` to both. |
| `shortest_line(a, b)` | [`gm.shortest_line(a, b)`][gometry.shortest_line] | Connects the nearest witnesses. On a geographic CRS the default chart-edge length is no shorter than their geodesic separation, subject to binary64 rounding; measure with the same `unit=` choice and expect computed publication to add vertices when needed. |
| `ops.nearest_points(a, b)` | [`gm.nearest_points(a, b)`][gometry.nearest_points] | The `(pa, pb)` pair behind `shortest_line`. |
| `delaunay_triangles(g)` | `g.triangulate(method='delaunay')` | Use `method='constrained'` for constrained edges or `method='earcut'` for polygon interiors. |
| Voronoi diagram output | [`g.voronoi()`][gometry.Geometry.voronoi] / `g.voronoi(output='edges')` | Use the `output` keyword to select cells or internal edges. |
| `shapely.ops.polygonize(...)` / `shapely.ops.polygonize_full(...)` | [`gm.polygonize_all(values)`][gometry.polygonize_all], with `diagnostics=True` for pooled cut edges, dangles, and invalid rings; `array.polygonize()` for independent rows | The pooled function accepts a `GeometryArray` directly and pools its present rows. |
| `build_area(...)` (GEOS) | `(...).build_area()` | Even-odd fill; unlike `polygonize`. |
| `node(...)` (GEOS 3.7+) | `(...).node()` | Noded linework as `MultiLineString`. |
| `line_merge(...)` | `(...).line_merge()` | |
| `shared_paths(...)` | [`gm.shared_paths(...)`][gometry.shared_paths] | |
| `snap(g, ref, tol)` | [`gm.snap(g, ref, tol)`][gometry.snap] | GEOS's vertex pass moves at most one source vertex per reference vertex and requires distance `< tol`; gometry independently snaps every source vertex at distance `<= tol` to its nearest reference (ties by smallest X, then Y) and collapses consecutive equal-X/Y line coordinates. |
| `segmentize(g, max_len)` | `g.segmentize(max_len)` | |
| `remove_repeated_points(...)` | `(...).remove_repeated_points()` | |
| `minimum_rotated_rectangle(g)` | `g.minimum_rotated_rectangle()` | |
| `geom.reverse()` / `shapely.reverse(geom)` | [`geom.reverse()`][gometry.Geometry.reverse] | |
| `orient(g)` | `g.orient_polygons()` | |
| `normalize(g)` | `g.normalize()` | |
| affine (`affinity.*`) | [`geom.translate(...)`][gometry.Geometry.translate] / `rotate(...)` / `scale(...)` / `skew(...)` / `affine_transform(...)` | |

### Linear referencing

| Shapely | gometry | Notes |
|---|---|---|
| `line.interpolate(d)` | `line.line_interpolate(d)` | |
| `line.project(pt)` | `line.line_locate(pt)` | |
| `substring(line, a, b)` | `line.line_substring(a, b)` | |
| `split(geom, splitter)` | [`gm.split(geom, splitter)`][gometry.split] | Binary free function; array line inputs return one `Groups` row per source line. |

### Validation, repair, precision

| Shapely | gometry | Notes |
|---|---|---|
| `geom.is_valid` | [`geom.is_valid`][gometry.Geometry.is_valid] | Unchanged; [`geom.validate()`][gometry.Geometry.validate] adds the reason/location. |
| `explain_validity(geom)` | `report.reason` / `report.location` | |
| `make_valid(geom)` | [`geom.repair()`][gometry.Geometry.repair] | Canonical spelling. |
| `set_precision(g, grid_size)` | [`geom.snap_to_grid(grid_size=..., repair=...)`][gometry.Geometry.snap_to_grid] | Closest fixed-grid snap. `grid_size` must be positive and finite; unlike `coverage_clean(grid_size=0)`, zero is invalid. Optional `repair=`. No JTS snap-rounding precision model in v1. |
| — | [`geom.quantize(precision)`][gometry.Geometry.quantize] | Decimal-place rounding of coordinates (different model from `set_precision`). |
| `require(...)` at boundary | [`gm.require(obj, crs=..., axes=...)`][gometry.require] | Storage contract. |
| `geom.is_empty` / `is_ring` / `is_simple` / `is_closed` | [`geom.is_empty`][gometry.Geometry.is_empty] / [`geom.is_ring`][gometry.Geometry.is_ring] / [`geom.is_simple`][gometry.Geometry.is_simple] / [`geom.is_closed`][gometry.Geometry.is_closed] | |
| `is_ccw(ring)` | `g.is_ccw` / [`geom.is_ccw`][gometry.Geometry.is_ccw] | Closed `LineString` winding. |
| `force_2d(g)` / `force_3d(g, z)` | `g.force_2d()` / `g.force_3d(z)` | `force_3d` fills only missing Z. Replace values everywhere with `g.with_coordinates(z=z, m=m)`; `None` clears an axis. |

### IO

| Shapely | gometry |
|---|---|
| [WKB](https://www.ogc.org/standard/sfa/) `from_wkb(b)` / `to_wkb(g)` | [`gm.from_wkb(b)`][gometry.from_wkb] / `g.to_wkb(include_srid=...)` |
| EWKB | `gm.from_wkb(b)` / `g.to_wkb(include_srid=True)` |
| [WKT](https://www.ogc.org/standard/sfa/) `from_wkt(s)` / `to_wkt(g)` | [`gm.from_wkt(s)`][gometry.from_wkt] / `g.to_wkt()` |
| [GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946) | `gm.from_geojson(obj)` / `g.to_geojson()` |
| GeoJSON features w/ attributes | [`gm.from_geojson(fc, features=True)`][gometry.from_geojson] → validated [`Features`][gometry.Features] record; serialize it with [`Features.__geo_interface__`](../ecosystem/text-formats.md#features__geo_interface__), or use [`gm.to_feature_collection(geoms, properties=..., ids=...)`][gometry.to_feature_collection] for separate columns |
| `geom.__geo_interface__` | [`geom.__geo_interface__`](../ecosystem/text-formats.md#__geo_interface__) |
| [GeoArrow](https://geoarrow.org/) | [`gm.from_arrow(obj)`][gometry.from_arrow] / [`geom.to_arrow()`][gometry.Geometry.to_arrow] |
| `shapely.to_wkb(Point(...))` / `shapely.to_wkb(box(...))` | [`gm.Point(x, y).to_wkb(precision=...)`][gometry.Geometry.to_wkb] / `gm.box(...).to_wkb(include_srid=..., precision=...)` |
| `shapely.to_wkt(Point(...))` / `shapely.to_wkt(box(...))` | [`gm.Point(x, y).to_wkt(precision=...)`][gometry.Geometry.to_wkt] / `gm.box(...).to_wkt(precision=...)` |

Shapely's `to_wkt(output_dimension=2)` becomes `geom.force_2d().to_wkt()` in
gometry. Remove geometry axes before writing with `force_2d()`,
`with_coordinates(z=None)`, or `with_coordinates(m=None)`; use `coords.select(...)` to choose a coordinate-view layout
for coordinate exports without changing the geometry.

### Indexing & prepared

| Shapely | gometry | Notes |
|---|---|---|
| `STRtree(geoms)` | [`gm.SpatialIndex(geoms)`][gometry.SpatialIndex] | |
| `tree.query(g)` | [`idx.candidates(g)`][gometry.SpatialIndex.candidates] | |
| `tree.query(g, predicate=...)` | [`idx.query(g, predicate=...)`][gometry.SpatialIndex.query] | |
| `tree.nearest(g)` | [`idx.nearest(g, unit=...)`][gometry.SpatialIndex.nearest] | |
| `tree.query_nearest(g, exclusive=True)` | `idx.nearest(g, exclusive=True)` | skip self-matches |
| `prepare(g)` + predicate | `gm.<predicate>(g, geom)` or `idx.query(g, predicate=...)` | A geometry indexes itself once it is probed repeatedly. |

## pyproj

| pyproj | gometry | Notes |
|---|---|---|
| `CRS.from_epsg(4326)` | [`gm.CRS(4326)`][gometry.CRS] | CRS object; [`geom.crs`][gometry.Geometry.crs] returns one. |
| `gdf.set_crs(c)` | [`geom.with_crs(c)`][gometry.Geometry.with_crs] | |
| `gdf.to_crs(c)` | [`geom.to_crs(c)`][gometry.Geometry.to_crs] | |
| `Transformer.from_crs(a, b)` + `.transform(x, y)` | [`gm.crs_transform(a, b, x, y, z=..., t=...)`][gometry.crs_transform] | |
| `transformer.transform_bounds(...)` | [`gm.crs_transform_bounds(...)`][gometry.crs_transform_bounds] | |
| `always_xy=True` | gometry X/Y coordinates | |
| `query_utm_crs_info(...)` | [`geom.estimate_local_crs()`][gometry.Geometry.estimate_local_crs] / `geoms.estimate_local_crs()` | Scalar and array receiver methods → `CRS`. |
| polar/UPS selection | `geom.estimate_local_crs()` | |
| `crs.is_geographic` / `is_projected` | [`gm.CRS(c).is_geographic`][gometry.CRS.is_geographic] / `.is_projected` | `is_*` property family. |
| `crs.axis_info` | [`gm.CRS(c).axis_order`][gometry.CRS.axis_order] | Diagnostic only; boundary stays X/Y. |
| `crs.to_authority()` / `to_epsg()` | [`gm.CRS(c).to_authority()`][gometry.CRS.to_authority] / `.to_epsg()` | |
| `crs.to_wkt()` / `to_proj4()` / `to_json()` | [`gm.CRS(c).to_wkt()`][gometry.CRS.to_wkt] / `.to_proj()` / `.to_projjson()`; pass `as_dict=True` for a mapping | |
| `crs == other` | `gm.CRS(c) == gm.CRS(other)` | Structural equality compares stored definitions; use `same_as` for operational equivalence. |
| `CRS.from_user_input(...)` normalize | [`gm.CRS(c).definition`][gometry.CRS.definition] / [`gm.CRS(c).identify()`][gometry.CRS.identify] | `definition` retains normalized authored text; `identify()` supplies authority metadata. |
| `crs.to_2d()` / `to_3d()` | [`gm.CRS(c).to_2d()`][gometry.CRS.to_2d] / `.to_3d()` | Return a `CRS`. |
| unit / factors | [`gm.crs_unit(c)`][gometry.crs_unit] / [`gm.crs_units(c)`][gometry.crs_units] / [`gm.CRS(c).factors(lon, lat)`][gometry.CRS.factors] | |
| database / search | [`gm.crs_authorities()`][gometry.crs_authorities] / [`gm.crs_codes(...)`][gometry.crs_codes] / [`gm.crs_catalog(name=...)`][gometry.crs_catalog] | Global `crs_*` functions (not `CRS` classmethods). There is no `gm.crs_list`. |
| catalogs | [`gm.crs_ellipsoids()`][gometry.crs_ellipsoids] / [`gm.crs_prime_meridians()`][gometry.crs_prime_meridians] / [`gm.crs_proj_operations()`][gometry.crs_proj_operations] / [`gm.crs_utm_zones(...)`][gometry.crs_utm_zones] / [`gm.crs_celestial_bodies()`][gometry.crs_celestial_bodies] | |
| operations | [`gm.CRS(a).operation(b)`][gometry.CRS.operation] / `.operations(b)` / `.operation(b, at=(x, y))` / [`gm.crs_apply(...)`][gometry.crs_apply] | |
| grid lookup | [`gm.crs_grid(...)`][gometry.crs_grid] | Local grid metadata and availability. |

### Geodesy (`Geod`)

| pyproj `Geod` | gometry (geometry on a geographic CRS) | Notes |
|---|---|---|
| `g.inv(...)` distance | [`gm.distance(a, b)`][gometry.distance] | |
| `g.inv(...)` azimuth | [`gm.bearing(a, b)`][gometry.bearing] | |
| `g.inv(...)` back azimuth | `gm.bearing(b, a)` | |
| `g.fwd(lon, lat, az, d)` | `point.destination(bearing, distance)` | |
| `g.line_length(...)` | `line.length` | |
| `g.geometry_area_perimeter(...)` | `poly.area` / `poly.length` | |
| `g.npts(...)` | [`gm.point_between(a, b, 0.5, normalized=True)`][gometry.point_between] | |
| geographic meter buffer | `point.buffer(meters)` on a geographic CRS (local-projection approximation) | |
| non-WGS 84 ellipsoid | The same calls on points in that geographic CRS, e.g. `gm.Point(lon, lat, crs=4230)`; [`geom.area`][gometry.Geometry.area] / [`geom.length`][gometry.Geometry.length] likewise | |

## h3-py

| h3-py | gometry | Notes |
|---|---|---|
| `latlng_to_cell(lat, lng, r)` | [`gm.H3Cell(point, resolution=r)`][gometry.H3Cell] | |
| `cell_to_boundary(h)` | [`cell.polygon`][gometry.Cell.polygon] / [`cells.polygon`][gometry.CellArray.polygon] | |
| `polygon_to_cells(poly, r)` / `polyfill` | [`gm.h3_cover(geom, resolution=r, cell_rule="center")`][gometry.h3_cover] | |
| `compact_cells` / `uncompact_cells` | `cells.compact()` / `cells.uncompact(r)` | |
| per-point membership | [`gm.covers(geom, points)`][gometry.covers] | |

## s2sphere

| s2sphere | gometry | Notes |
|---|---|---|
| `CellId.from_lat_lng(...).parent(level)` | [`gm.S2Cell(point, level=...)`][gometry.S2Cell] | |
| `Cell(cell_id)` boundary | [`cell.polygon`][gometry.Cell.polygon] / [`cells.polygon`][gometry.CellArray.polygon] | |
| `RegionCoverer().get_covering(region)` | [`gm.s2_cover(geom, ...)`][gometry.s2_cover] | `target_cells` guides refinement; `max_cells` is the hard output cap. |
| `RegionCoverer(min_level=L, max_level=L).get_covering(region)` | [`gm.s2_cover(geom, level=L, max_cells=...)`][gometry.s2_cover] | The requested level is never coarsened. |
| `CellUnion` set ops | [`gm.s2_union(a, b)`][gometry.s2_union] / [`gm.s2_intersection(a, b)`][gometry.s2_intersection] / [`gm.s2_difference(a, b)`][gometry.s2_difference] | |
| `CellUnion.normalize()` / expand | `cells.compact()` / `cells.uncompact(level)` | |
| geometry membership | [`gm.covers(geometry, probe)`][gometry.covers] | |

## rtree / STRtree / GeoPandas

| Old | gometry | Notes |
|---|---|---|
| `rtree.index.Index()` + `insert(id, bbox)` | [`gm.SpatialIndex(geoms)`][gometry.SpatialIndex] | Geometry-aware, bulk-loaded. |
| `list(idx.intersection(bbox))` | [`idx.candidates(geom)`][gometry.SpatialIndex.candidates] | |
| candidates + manual refine | [`idx.query(geom, predicate=...)`][gometry.SpatialIndex.query] | |
| `idx.nearest(bbox, n)` | [`idx.nearest(geom, k=n, unit=...)`][gometry.SpatialIndex.nearest] | |
| `STRtree(geoms)` | `gm.SpatialIndex(geoms)` | |
| `geopandas.sjoin(a, b, predicate=...)` | [`gm.join(a, b, predicate=...)`][gometry.join] | |

## python-geohash / pygeohash

| Old | gometry | Notes |
|---|---|---|
| `geohash.encode(lat, lon, precision=p)` | [`gm.GeohashCell(lon, lat, precision=p).token`][gometry.GeohashCell] | |
| `geohash.decode(token)` | [`cell.center`][gometry.Cell.center] | |
| `geohash.bbox(token)` | [`cell.polygon.bounds`][gometry.Cell.polygon] | |
| `geohash.neighbors(token)` | [`cell.neighbors`][gometry.Cell.neighbors] | 8-neighborhood, seam-wrapping. |
| coverage of a polygon | [`gm.geohash_cover(geom, precision=p)`][gometry.geohash_cover] | |

## mercantile

| mercantile | gometry | Notes |
|---|---|---|
| `mercantile.tile(lng, lat, zoom)` | [`gm.Tile(lon=lng, lat=lat, zoom=z)`][gometry.Tile] | Typed `Tile` with `.zoom/.x/.y`. |
| `mercantile.quadkey(tile)` | `tile.token` | |
| `mercantile.quadkey_to_tile(qk)` | `gm.Tile(qk)` | |
| `mercantile.bounds(tile)` | `tile.polygon.bounds` | Tile footprint bounds. |
| `mercantile.parent/children(tile)` | `tile.parent()` / `tile.children()` | |
| `mercantile.tiles(*bbox, zooms)` | [`gm.tile_cover(geom, zoom=z)`][gometry.tile_cover] | |

## polyline / openlocationcode / OSM shortlink

| Old | gometry | Notes |
|---|---|---|
| `polyline.decode(s)` | [`gm.from_polyline(s)`][gometry.from_polyline] | Returns a `Point` for one coordinate, otherwise a `LineString` (or array). |
| `polyline.encode(coords)` | `line.to_polyline()` / [`geom.to_polyline()`][gometry.Geometry.to_polyline] | |
| `openlocationcode.encode(lat, lng)` | [`gm.pluscode_encode(lng, lat)`][gometry.pluscode_encode] | |
| `openlocationcode.decode(code)` | [`gm.pluscode_polygon(code)`][gometry.pluscode_polygon] | Returns the cell `Polygon`. |
| `shorten` / `recoverNearest` | [`gm.pluscode_shorten`][gometry.pluscode_shorten] / [`gm.pluscode_recover`][gometry.pluscode_recover] | |
| OSM `ShortLink.encode/decode` | [`gm.osm_shortlink_encode`][gometry.osm_shortlink_encode] / [`gm.osm_shortlink_location`][gometry.osm_shortlink_location] | Legacy `@` accepted. |

## scikit-learn / scipy clustering

| Old | gometry | Notes |
|---|---|---|
| `DBSCAN(...).fit(coords)` / k-means / hierarchical clustering | feed `np.asarray(geom.coords, dtype=np.float64)` (or `.coords.x`/`.y`) to scikit-learn / scipy | gometry stays a geometry engine; cluster in coordinate space via NumPy handoff. |
| scattered-data interpolation (IDW, kriging, …) | scipy / scikit-gstat over `np.asarray(geom.coords, dtype=np.float64)` | Project to a suitable CRS first when planar XY is wrong. |
