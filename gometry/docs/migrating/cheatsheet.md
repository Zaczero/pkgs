---
description: Searchable old-symbol → gometry-spelling reference tables, grouped by source library (Shapely, pyproj, h3-py, s2sphere, rtree).
---

# Migration cheatsheet

One scannable table per source library, followed by the utility codecs (polyline,
plus codes, tiles, geohash). Use your browser's find
(or the search box) to jump to a symbol. Where a behavior changes, the Notes
column says so; the per-library pages have side-by-side examples.

Conventions:

- `import gometry as gm`
- `geom` is any gometry geometry; `idx = gm.SpatialIndex(...)`.
- Binary operations are **free functions** (`gm.contains(a, b)`); unary operations are **methods** (`geom.buffer(d)`).
  Measurement accessors (`.area`, `.length`) stay on the geometry object.

## Shapely

### Construction

| Shapely | gometry | Notes |
|---|---|---|
| `Point(x, y)` | `gm.Point(x, y, crs=...)` | Same constructor always; under a geographic CRS, `(x, y)` is `(lon, lat)`. `z=` / `m=` for Z/M. |
| `LineString(coords)` | `gm.LineString(coords, z=..., m=..., crs=...)` | |
| `Polygon(shell, holes)` | `gm.Polygon(shell, holes, crs=...)` | |
| `MultiPoint(...)` | `gm.MultiPoint(...)` | |
| `MultiLineString(...)` | `gm.MultiLineString(...)` | |
| `MultiPolygon(...)` | `gm.MultiPolygon(...)` | |
| `GeometryCollection(...)` | `gm.GeometryCollection(...)` | |
| `box(minx, miny, maxx, maxy)` | `gm.box(minx, miny, maxx, maxy, crs=..., wrap=...)` | `wrap="split"` for antimeridian boxes. |
| `shape(geojson)` | `gm.from_geojson(obj)` | GeoJSON / `__geo_interface__`. |
| `shapely.points(...)` | `gm.points(lons, lats, z=..., m=..., crs=...)` | Packed array. |
| mixed array | `gm.GeometryArray([...])` | Rust-owned container for vectorized ops. |

### Inspection

| Shapely | gometry | Notes |
|---|---|---|
| `geom.geom_type` | `geom.geometry_type` | |
| `geom.bounds` | `geom.bounds` | Bulk collections use `gm.bounds(values)`. |
| `geom.has_z` | `geom.has_z` / `geom.has_m` | |
| — | `geom.coordinate_axes` | `"XY"`, `"XYZ"`, `"XYM"`, `"XYZM"`. |
| — | `geom.epoch` | Coordinate epoch. |
| `geom.crs` (n/a in Shapely) | `geom.crs` | CRS is first-class metadata. |
| `get_coordinates(geom)` | `geom.coords` (`list(...)`, `.x`/`.y`, `.index`) / `gm.get_coordinates(...)` | |
| `parts(...)` / `rings(...)` | `gm.parts(...)` / `gm.rings(...)` | |
| `str(geom)` / `f"{geom:.2f}"` / `f"{geom:x}"` | same | WKT text; `.Nf`/`.Ng` round for display, `x`/`X` hex WKB. |
| `bool(geom)` | same | Falsy when empty. |
| `match geom: case Point(x, y):` | `match geom: case gm.Point(x, y):` | **gometry addition** — leaves carry `__match_args__`; Shapely 2.x has no positional match on `Point`. Arrays match `[a, *rest]`. |

### Predicates

| Shapely | gometry |
|---|---|
| `a.contains(b)` / `shapely.contains(a, b)` | [`gm.contains(a, b)`][gometry.contains] |
| `a.contains_properly(b)` / `prep.contains_properly(b)` | [`gm.contains_properly(a, b)`][gometry.contains_properly] (also prepared, and as an index/join predicate) |
| `a.within(b)` / `shapely.within(a, b)` | [`gm.within(a, b)`][gometry.within] |
| `a.intersects(b)` / `shapely.intersects(a, b)` | [`gm.intersects(a, b)`][gometry.intersects] |
| `a.covers(b)` / `a.covered_by(b)` | `gm.covers(a, b)` / `gm.covered_by(a, b)` |
| `a.disjoint(b)` | `gm.disjoint(a, b)` |
| `a.touches(b)` / `a.crosses(b)` / `a.overlaps(b)` | `gm.touches(a, b)` / `gm.crosses(a, b)` / `gm.overlaps(a, b)` |
| `a.equals(b)` | `gm.equals(a, b)` |
| normalized equality | `gm.equals_exact(a.normalize(), b.normalize())` |
| `a.relate(b)` / `a.relate_pattern(b, p)` | `gm.relate(a, b)` / `gm.relate_pattern(a, b, p)` |
| `shapely.contains_xy(g, x, y)` | `gm.contains_xy(g, x, y)` |
| `shapely.intersects_xy(g, x, y)` | `gm.intersects_xy(g, x, y)` |
| `a.dwithin(b, d)` | [`gm.dwithin(a, b, distance, unit=…)`][gometry.dwithin] — CRS-native by default (geodesic m on geographic); optional `unit=`. Indexed: `idx.query(..., predicate="dwithin", distance=..., unit=...)` |

### Measurement (the CRS decides; results are native)

| Shapely | gometry | Notes |
|---|---|---|
| `geom.area` | [`geom.area`][gometry.Geometry.area] / [`geoms.area`][gometry.GeometryArray.area] | Geographic CRS → geodesic m²; projected → planar **native linear units** (feet stay feet); none → coordinate units. Free [`gm.area(geom, unit=…)`][gometry.area] is override-only. |
| `geom.length` | [`geom.length`][gometry.Geometry.length] / [`geoms.length`][gometry.GeometryArray.length] | Same CRS rule. Free [`gm.length(..., unit=…)`][gometry.length] is override-only. |
| `a.distance(b)` / `shapely.distance(a, b)` | [`gm.distance(a, b)`][gometry.distance] | Same CRS rule. |
| change the metric frame / units | [`geom.to_crs(target)`][gometry.Geometry.to_crs] | Reproject for a new frame; or `unit='planar'` / `unit='meters'` for unit overrides. |
| `minimum_clearance(g)` | [`g.minimum_clearance()`][gometry.Geometry.minimum_clearance] | CRS-aware; `unit='planar'` for coordinate units. |
| `hausdorff_distance` / `frechet_distance` | [`gm.hausdorff_distance(...)`][gometry.hausdorff_distance] / [`gm.frechet_distance(...)`][gometry.frechet_distance] | CRS-aware; `unit='planar'` forces coordinate units. |

### Constructive operations

| Shapely | gometry | Notes |
|---|---|---|
| `a.intersection(b)` / `shapely.intersection(a, b)` / `a & b` | `gm.intersection(a, b)` / `a & b` | |
| `a.union(b)` / `shapely.union(a, b)` / `a \| b` | `gm.union(a, b)` / `a \| b` | |
| `a.difference(b)` / `shapely.difference(a, b)` / `a - b` | `gm.difference(a, b)` / `a - b` | |
| `a.symmetric_difference(b)` / `a ^ b` | `gm.symmetric_difference(a, b)` / `a ^ b` | |
| `unary_union(geoms)` | `gm.union_all(geoms)` | Rejects conflicting CRS. |
| `intersection_all(geoms)` | `gm.intersection_all(geoms)` | Common region of all inputs. |
| `symmetric_difference_all(geoms)` | `gm.symmetric_difference_all(geoms)` | Region in an odd number of inputs. |
| `geom.buffer(d)` / `shapely.buffer(geom, d)` | [`geom.buffer(d)`][gometry.Geometry.buffer] | Distance follows the CRS (geographic → meters; projected → native linear units). |
| `geom.simplify(t)` / `shapely.simplify(geom, t)` | `geom.simplify(t)` | Tolerance is coordinate units. |
| `geom.offset_curve(d)` | `geom.offset_curve(d)` | Distance follows the CRS. |
| `clip_by_rect(g, ...)` | `g.clip_by_rect(...)` | |
| `geom.convex_hull` | `geom.convex_hull()` | |
| `concave_hull(g, ratio=...)` | `geom.concave_hull(concavity=..., length_threshold=...)` | Different algorithm and knobs: `concavity` is an edge-length ratio, not Shapely's `ratio`. |
| `geom.centroid` / `geom.envelope` | `g.centroid()` / `g.envelope()` | |
| `geom.point_on_surface()` | `g.point_on_surface()` | |
| `polylabel(g)` | `g.polylabel()` | [Pole of inaccessibility](https://en.wikipedia.org/wiki/Pole_of_inaccessibility). |
| `maximum_inscribed_circle(g)` | `g.maximum_inscribed_circle()` | Filled inscribed disk (`Polygon`); `g.maximum_inscribed_radius()` is the radius. |
| `shortest_line(a, b)` | `gm.shortest_line(a, b)` | |
| `ops.nearest_points(a, b)` | `gm.nearest_points(a, b)` | The `(pa, pb)` pair behind `shortest_line`. |
| `delaunay_triangles(g)` | `g.triangulate(method='delaunay')` | Use `method='constrained'` for constrained edges or `method='earcut'` for polygon interiors. |
| `voronoi_polygons(g)` | `g.voronoi_polygons()` / `g.voronoi_edges()` | |
| `polygonize(...)` | `gm.polygonize(values)` to pool one graph; `array.polygonize()` for independent rows; `gm.polygonize_full(values)` for pooled diagnostics | The free aggregates accept `GeometryArray` directly and pool its present rows. |
| `build_area(...)` (GEOS) | `(...).build_area()` | Even-odd fill; unlike `polygonize`. |
| `node(...)` (GEOS 3.7+) | `(...).node()` | Noded linework as `MultiLineString`. |
| `line_merge(...)` | `(...).line_merge()` | |
| `shared_paths(...)` | `gm.shared_paths(...)` | |
| `snap(g, ref, tol)` | `gm.snap(g, ref, tol)` | |
| `segmentize(g, max_len)` | `g.segmentize(max_len)` | |
| `remove_repeated_points(...)` | `(...).remove_repeated_points()` | |
| `minimum_rotated_rectangle(g)` | `g.minimum_rotated_rectangle()` | |
| `geom.reverse()` / `shapely.reverse(geom)` | `geom.reverse()` | |
| `orient(g)` | `g.orient_polygons()` | |
| `normalize(g)` | `g.normalize()` | |
| affine (`affinity.*`) | `geom.translate(...)` / `rotate(...)` / `scale(...)` / `skew(...)` / `affine_transform(...)` | |

### Linear referencing

| Shapely | gometry |
|---|---|
| `line.interpolate(d)` | `line.line_interpolate(d)` |
| `line.project(pt)` | `line.line_locate(pt)` |
| `substring(line, a, b)` | `line.line_substring(a, b)` |
| `split(geom, splitter)` | [`gm.split(geom, splitter)`][gometry.split] | Binary free function (no instance method). |

### Validation, repair, precision

| Shapely | gometry | Notes |
|---|---|---|
| `geom.is_valid` | `geom.is_valid` | Unchanged; `geom.validate()` adds the reason/location. |
| `explain_validity(geom)` | `report.reason` / `report.location` | |
| `make_valid(geom)` | [`geom.repair()`][gometry.Geometry.repair] | Canonical spelling. |
| `set_precision(g, grid_size)` | `geom.snap_to_grid(size=..., repair=...)` | Closest fixed-grid snap (spacing `size`, not decimal places). Optional `repair=`. No JTS snap-rounding precision model in v1. |
| — | `geom.quantize(precision)` | Decimal-place rounding of coordinates (different model from `set_precision`). |
| `require(...)` at boundary | `gm.require(obj, crs=..., axes=...)` | Storage contract. |
| `geom.is_empty` / `is_ring` / `is_simple` / `is_closed` | `geom.is_empty` / `geom.is_ring` / `geom.is_simple` / `geom.is_closed` | |
| `is_ccw(ring)` | `g.is_ccw` / `geom.is_ccw` | Closed `LineString` winding. |
| `force_2d(g)` / `force_3d(g, z)` | `g.force_2d()` / `g.force_3d(z)` | `force_3d` fills only missing Z. Set values everywhere with `g.set_z(z)` / `g.set_m(m)`; `None` clears an axis. |

### IO

| Shapely | gometry |
|---|---|
| [WKB](https://www.ogc.org/standard/sfa/) `from_wkb(b)` / `to_wkb(g)` | `gm.from_wkb(b)` / `g.to_wkb(include_srid=...)` |
| EWKB | `gm.from_wkb(b)` / `g.to_wkb(include_srid=True)` |
| [WKT](https://www.ogc.org/standard/sfa/) `from_wkt(s)` / `to_wkt(g)` | `gm.from_wkt(s)` / `g.to_wkt(output_dimension=...)` |
| [GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946) | `gm.from_geojson(obj)` / `g.to_geojson(include_z=...)` |
| GeoJSON features w/ attributes | `gm.from_features(fc)` → validated `Features` record; inverse `gm.to_feature_collection(geoms, properties=..., ids=...)` |
| `geom.__geo_interface__` | `geom.__geo_interface__` |
| [GeoArrow](https://geoarrow.org/) | `gm.from_arrow(obj)` / `geom.to_arrow()` |
| `shapely.to_wkb(Point(...))` / `shapely.to_wkb(box(...))` | `gm.Point(x, y).to_wkb(precision=...)` / `gm.box(...).to_wkb(include_srid=..., precision=...)` |
| `shapely.to_wkt(Point(...))` / `shapely.to_wkt(box(...))` | `gm.Point(x, y).to_wkt(precision=...)` / `gm.box(...).to_wkt(precision=...)` |

### Indexing & prepared

| Shapely | gometry | Notes |
|---|---|---|
| `STRtree(geoms)` | [`gm.SpatialIndex(geoms)`][gometry.SpatialIndex] | |
| `tree.query(g)` | [`idx.candidates(g)`][gometry.SpatialIndex.candidates] | bbox candidates |
| `tree.query(g, predicate=...)` | [`idx.query(g, predicate=...)`][gometry.SpatialIndex.query] | exact refine |
| `tree.nearest(g)` | [`idx.nearest(g, unit=...)`][gometry.SpatialIndex.nearest] | |
| `tree.query_nearest(g, exclusive=True)` | `idx.nearest(g, exclusive=True)` | skip self-matches |
| `prepare(g)` + predicate | `g.prepare().<predicate>(geom)` or `idx.query(g, predicate=...)` | |

## pyproj

| pyproj | gometry | Notes |
|---|---|---|
| `CRS.from_epsg(4326)` | `gm.CRS(4326)` | First-class CRS object; `geom.crs` returns one. |
| `gdf.set_crs(c)` | `geom.set_crs(c)` | Declare meaning. |
| `gdf.to_crs(c)` | `geom.to_crs(c)` | Transform coordinates. |
| `Transformer.from_crs(a, b)` + `.transform(x, y)` | `gm.crs_transform(a, b, x, y, z=..., t=...)` | Stateless; always X/Y. Scalar inputs return a tuple; bulk inputs return one `(N, 2)` / `(N, 3)` NumPy matrix (`t` is input-only). |
| `transformer.transform_bounds(...)` | `gm.crs_transform_bounds(a, b, (xmin, ymin, xmax, ymax), densify=...)` | |
| `always_xy=True` | (default) | gometry is always X/Y. |
| `query_utm_crs_info(...)` | `geom.estimate_local_crs()` / `geoms.estimate_local_crs()` | Scalar and array receiver methods → `CRS`. |
| polar/UPS selection | `geom.estimate_local_crs()` | |
| `crs.is_geographic` / `is_projected` | `gm.CRS(c).is_geographic` / `.is_projected` | Full `is_*` property family. |
| `crs.axis_info` | `gm.CRS(c).axis_order` | Diagnostic only; boundary stays X/Y. |
| `crs.to_authority()` / `to_epsg()` | `gm.CRS(c).to_authority()` / `.to_epsg()` | |
| `crs.to_wkt()` / `to_proj4()` / `to_json()` | `gm.CRS(c).to_wkt()` / `.to_proj()` / `.to_projjson()` / `.to_projjson_dict()` | |
| `crs == other` | `gm.CRS(c) == other` | `CRS` compares by code/string. |
| `CRS.from_user_input(...)` normalize | `gm.CRS(c).canonical` / `gm.CRS(c).identify()` | |
| `crs.to_2d()` / `to_3d()` | `gm.CRS(c).to_2d()` / `.to_3d()` | Return a `CRS`. |
| unit / factors | `gm.crs_unit(c)` / `gm.crs_units(c)` / `gm.CRS(c).factors(lon, lat)` | |
| database / search | `gm.crs_authorities()` / `gm.crs_codes(...)` / `gm.crs_search(...)` / `gm.crs_catalog(...)` | Global `crs_*` functions (not `CRS` classmethods). There is no `gm.crs_list`. |
| catalogs | `gm.crs_ellipsoids()` / `gm.crs_prime_meridians()` / `gm.crs_proj_operations()` / `gm.crs_utm_zones(...)` / `gm.crs_celestial_bodies()` | |
| operations | `gm.CRS(a).operation(b)` / `.operations(b)` / `.operation(b, at=(x, y))` / `gm.crs_apply(...)` | |
| grid lookup | `gm.crs_grid(...)` | Local grid metadata and availability. |

### Geodesy (`Geod`)

On a **geographic** CRS the geometry metrics and point navigation free functions are
geodesic, so there is no `Geod` object to construct.

| pyproj `Geod` | gometry (geometry on a geographic CRS) | Notes |
|---|---|---|
| `g.inv(...)` distance | `gm.distance(a, b)` | Ellipsoidal meters. |
| `g.inv(...)` azimuth | `gm.bearing(a, b)` | |
| `g.fwd(lon, lat, az, d)` | `gm.destination(point, bearing, distance)` | |
| `g.line_length(...)` | `line.length` | |
| `g.geometry_area_perimeter(...)` | `poly.area` / `poly.length` | |
| `g.npts(...)` | `gm.point_between(a, b, 0.5, normalized=True)` / `gm.CRS(4326).geodesic_interpolate(...)` | |
| geographic meter buffer | `point.buffer(meters)` on a geographic CRS (local-projection approximation) | |
| non-WGS 84 ellipsoid | `gm.CRS(code).geodesic(...)` / `.geodesic_direct(...)` / `.geodesic_interpolate(...)`; `geom.area` / `geom.length` under that CRS | |

## h3-py

| h3-py | gometry | Notes |
|---|---|---|
| `latlng_to_cell(lat, lng, r)` | `gm.H3Cell(point, resolution=r)` | Pass a `Point`; returns a typed `H3Cell`. |
| `cell_to_boundary(h)` | `cell.polygon` / `cells.polygon` | Cell → polygon geometry; array form on `CellArray`. |
| `polygon_to_cells(poly, r)` / `polyfill` | `gm.h3_cover(geom, resolution=r, cell_rule="center").cells` | Rule explicit; default `"overlap"` gives complete-coverage superset keys. |
| `compact_cells` / `uncompact_cells` | `cells.compact()` / `cells.uncompact(r)` | On `CellArray` and coverages; hierarchy-aware. |
| per-point exact membership | `coverage.covers(points)` | Built in — exact, no refine step. |

## s2sphere

| s2sphere | gometry | Notes |
|---|---|---|
| `CellId.from_lat_lng(...).parent(level)` | `gm.S2Cell(point, level=...)` | Typed `S2Cell` with `.level`. |
| `Cell(cell_id)` boundary | `cell.polygon` / `cells.polygon` | Cell → polygon geometry; array form on `CellArray`. |
| `RegionCoverer().get_covering(region)` | `gm.s2_cover(geom, level=..., max_cells=...)` | |
| `CellUnion` set ops | `gm.s2_union(a, b)` / `gm.s2_intersection(a, b)` / `gm.s2_difference(a, b)` | Hierarchy-aware cell-set algebra. |
| `CellUnion.normalize()` / expand | `cells.compact()` / `cells.uncompact(level)` | On `CellArray` and coverages. |
| coverage membership | `coverage.covers(geometry)` | Exact, built in. |

## rtree / STRtree / GeoPandas

| Old | gometry | Notes |
|---|---|---|
| `rtree.index.Index()` + `insert(id, bbox)` | `gm.SpatialIndex(geoms)` | Geometry-aware, bulk-loaded. |
| `list(idx.intersection(bbox))` | `idx.candidates(geom)` | bbox candidates |
| candidates + manual refine | `idx.query(geom, predicate=...)` | exact refine |
| `idx.nearest(bbox, n)` | `idx.nearest(geom, k=n, unit=...)` | |
| `idx.explain` (n/a) | `idx.explain(geom, predicate=...)` | Query plan steps. |
| `STRtree(geoms)` | `gm.SpatialIndex(geoms)` | |
| `geopandas.sjoin(a, b, predicate=...)` | `gm.join(a, b, predicate=...)` | Prefilter + refine; returns index pairs. |

## python-geohash / pygeohash

| Old | gometry | Notes |
|---|---|---|
| `geohash.encode(lat, lon, precision=p)` | `gm.GeohashCell(lon, lat, precision=p).token` | Pass `Point`/array too; returns a typed `GeohashCell`. |
| `geohash.decode(token)` | `cell.center` | Cell centroid as a `Point`. |
| `geohash.bbox(token)` | `cell.polygon.bounds` | |
| `geohash.neighbors(token)` | `cell.neighbors` | 8-neighborhood, seam-wrapping. |
| coverage of a polygon | `gm.geohash_cover(geom, precision=p).cells` | Cell set exact w.r.t. `cell_rule`; membership exact vs source. |

## mercantile

| mercantile | gometry | Notes |
|---|---|---|
| `mercantile.tile(lng, lat, zoom)` | `gm.Tile(lon=lng, lat=lat, zoom=z)` | Typed `Tile` with `.zoom/.x/.y`. |
| `mercantile.quadkey(tile)` | `tile.token` | |
| `mercantile.quadkey_to_tile(qk)` | `gm.Tile(qk)` | |
| `mercantile.bounds(tile)` | `tile.polygon.bounds` | Tile footprint bounds. |
| `mercantile.parent/children(tile)` | `tile.parent()` / `tile.children()` | |
| `mercantile.tiles(*bbox, zooms)` | `gm.tile_cover(geom, zoom=z).cells` | Cell set exact w.r.t. `cell_rule`; membership via `cover.covers` is exact vs source (cells ≠ the region). |

## polyline / openlocationcode / OSM shortlink

| Old | gometry | Notes |
|---|---|---|
| `polyline.decode(s)` | `gm.from_polyline(s)` | Returns a `Point` for one coordinate, otherwise a `LineString` (or array). |
| `polyline.encode(coords)` | `line.to_polyline()` / `geom.to_polyline()` | |
| `openlocationcode.encode(lat, lng)` | `gm.pluscode_encode(lng, lat)` | |
| `openlocationcode.decode(code)` | `gm.pluscode_polygon(code)` | Returns the cell `Polygon`. |
| `shorten` / `recoverNearest` | `gm.pluscode_shorten` / `gm.pluscode_recover` | |
| OSM `ShortLink.encode/decode` | `gm.osm_shortlink_encode` / `gm.osm_shortlink_location` | Legacy `@` accepted. |

## scikit-learn / scipy clustering

| Old | gometry | Notes |
|---|---|---|
| `DBSCAN(...).fit(coords)` / k-means / hierarchical clustering | feed [`gm.get_coordinates`][gometry.get_coordinates] (or `.coords.x`/`.y`) to scikit-learn / scipy | gometry stays a geometry engine; cluster in coordinate space via NumPy handoff. |
| scattered-data interpolation (IDW, kriging, …) | scipy / scikit-gstat over [`gm.get_coordinates`][gometry.get_coordinates] | Project to a suitable CRS first when planar XY is wrong. |

## No-alias policy

These names are **not** registered in the public API. Use the canonical
spelling:

| You typed | Use instead |
|---|---|
| `make_valid` | `g.repair()` |
| `STRtree` / `Rtree` | `gm.SpatialIndex` |
| `polyfill` | `gm.h3_cover(geom, ...)` |
| `Geod` | `gm.bearing` / `gm.destination` (geographic CRS) / `gm.CRS(c).geodesic` |
| `Transformer` | `geom.to_crs` / `gm.crs_transform` |
| `sjoin` | `gm.join` |
| `unary_union` | `gm.union_all` |
| `polyfill` (tiles/geohash) | `gm.tile_cover(geom, ...)` / `gm.geohash_cover(geom, ...)` |

The reasoning behind these choices is on the [design page](../about/design.md).
