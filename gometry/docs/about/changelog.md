---
description: gometry changelog — the 1.0.0 release notes (geometry, predicates, overlay, measurement, geodesy, CRS, discrete grids, indexing, validation, IO, and the dataframe/Arrow integration layer).
---

# Changelog

## 1.0.0 — Unreleased

gometry 1.0.0 is the first public release. It is designed to replace the
practical day-to-day geospatial stack — Shapely + pyproj + h3-py + s2sphere +
rtree — with one coherent, Rust-backed package and a documented
[migration path](../migrating/index.md). The API is designed so the obvious call
is the correct one, and there is one way to do each thing — see
[design principles](design.md) for the constitution and rationale.

### Geometry model

- Scalar and collection construction for the seven geometry families via the
  callable classes (`Point`/`LineString`/`Polygon`/`MultiPoint`/`MultiLineString`/
  `MultiPolygon`/`GeometryCollection`) and the plural array builders
  (`points`/`line_strings`/…); `Point(lon, lat, crs=4326)` for geographic axis order.
- Explicit `XY` / `XYZ` / `XYM` / `XYZM` layouts via `z=` / `m=` keywords; topology stays X/Y.
- Typed empties (`POINT EMPTY`, `POLYGON EMPTY`, …) that round-trip through WKT/WKB with
  empty bounds/coordinates. Dimensional empties (`POINT Z EMPTY`, …) preserve axes on
  WKT/WKB and compare axes-sensitively under `equals_identical`; GeoJSON flattens them to
  coordinate-less empties (no Z/M tag). Predicate defaults are total: `equals` is true for
  two empties of compatible topology, `disjoint` is true for every empty pairing, and other
  DE-9IM predicates are false.
- First-class CRS metadata with a strict `set_crs` (declare) vs `to_crs` (transform) split;
  CRS values validated through [PROJ](https://proj.org/) before storage, and re-tagging
  requires `set_crs(..., overwrite=True)`.
- Per-geometry coordinate epochs for dynamic reference frames: `geom.epoch`, `set_epoch`,
  and `to_crs(..., epoch=...)`; every ingress enforces `epoch ⟹ crs`.
- `pickle` / `copy` / `deepcopy` on the data types (`Geometry`, `GeometryArray`, `CRS`,
  `H3Cell`, `S2Cell`); copies short-circuit to identity (immutable values), so copying a
  200k-point array is free. Derived engines (indexes, prepared geometries, coverages) rebuild.
- Frame-visible reprs (`<POINT (1 2) EPSG:4326>`), `str(geom)` as bare WKT, `format(geom, '.2f')`
  rounding for display (`x`/`X` for hex WKB), and `bool(geom)` as "not empty".
- Generous input containers: float lanes take any float iterable or zero-copy buffer (numpy
  incl. float32, `array.array`, memoryviews); mapping boundaries take any `Mapping`. A dedicated
  runtime parser tests and static typing fixtures keep the stubs and behavior in sync.
- A purpose-built top-level exception hierarchy: [`GeometryError(ValueError)`][gometry.GeometryError]
  rooting `InvalidGeometryError`, `GeometryTypeError` (also a `TypeError`), `CRSError` with
  `CRSMismatchError` / `TransformError`, and `ParseError` — picklable and re-exported flat.
- Full CPython protocol surface: `& | - ^` overlay operators, structural pattern matching,
  value-equal/hashable arrays, the sequence protocol, sortable grid cells, `weakref`, runtime
  `GeometryArray[Point]` subscription, and `nbytes` / `sys.getsizeof` accounting.
- Accessors: `geometry_type`, `topological_dimension`, `coordinate_axes`, `has_z`, `has_m`,
  `epoch`, `crs`, `bounds`, `coords`, `parts`, `rings`.

### Predicates and relationships

- Structural predicates `contains`, `contains_properly`, `intersects`, `within`, `covers`,
  `covered_by`, `disjoint`, `touches`, `crosses`, `overlaps`, plus `equals` and `equals_exact` —
  one shared predicate table behind functions, prepared geometries, and the index,
  with automatic prepared/cached acceleration for scalar-vs-array batches.
- [DE-9IM](https://en.wikipedia.org/wiki/DE-9IM) `relate` / `relate_pattern`.
- Vectorized `contains_xy` / `intersects_xy` and `dwithin`.
- Prepared geometry (`prepare`) and prepared point-array predicates; spatial-index diagnostics
  are available through `SpatialIndex.explain(...)`.
- Prepared point-in-polygon queries lazily build a conservative 64×64 cell classification
  after 10,000 probes, skipping exact evaluation for cells with a certified predicate
  result. For prepared-geometry queries against a 64-edge regular polygon, paired,
  interleaved retired user-mode instruction trials reduced instructions by 82.388% at
  10,000 probes and 74.247% at 24,000; below the threshold, trials stayed at parity.

### Overlay and constructive geometry

- Polygonal overlay `intersection`, `union`, `difference`, and
  `symmetric_difference`, as top-level binary functions with strict same-length
  broadcasting and CRS-conflict rejection.
- Many-geometry reductions: `GeometryArray.union_all()` plus the raw-iterable
  `union_all`, `intersection_all`, and `symmetric_difference_all` functions.
- `intersection` keeps every dimension of contact: boundary-only contact degenerates to the
  shared linework or touch points rather than an empty result.
- A vanishing overlay carries the typed empty for the highest dimension its result could hold,
  keeping an array of results dimensionally homogeneous.
- Summary geometries: `centroid`, `point_on_surface`, `envelope`, `convex_hull`, `concave_hull`,
  `polylabel`, `minimum_rotated_rectangle`, `maximum_inscribed_circle` (filled disk; the
  radius alone is `maximum_inscribed_radius`).
- Stable `boundary` contract per type (a line's boundary is a `MultiPoint`, a polygon's its rings),
  holding for typed empties; single-part constructive output collapses to the narrowest type.
- Z/M preserved wherever derivable: vertex-subset ops keep ordinates, overlay
  and `clip_by_rect` interpolate along source segments, and synthesized vertices
  return an honest 2D result without a policy matrix.
- `shortest_line`, `is_ccw`, and the `set_z` / `set_m` ordinate setters.
- Triangulation and decomposition: `triangulate(method='delaunay'|'constrained'|'earcut')`,
  `voronoi_polygons`, `voronoi_edges`, `polygonize`, `polygonize_full`.
  Array `.polygonize()` is row-wise; the free functions pool an array or iterable into one noding
  universe, so rings spanning inputs close only when pooling is explicit.
- `buffer`, `offset_curve`, `simplify`, `clip_by_rect`, `snap`, `segmentize`,
  `remove_repeated_points`, `reverse`, `normalize`, `orient_polygons`, `line_merge`,
  `shared_paths`, and affine transforms (`affine_transform`, `translate`, `rotate`, `scale`, `skew`).
- `simplify(method=...)` selects [Visvalingam–Whyatt](https://en.wikipedia.org/wiki/Visvalingam%E2%80%93Whyatt_algorithm)
  (`'vw'`, default) or [Douglas–Peucker](https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm)
  (`'dp'`) on a shared distance-scale tolerance and topology contract.
- One-sided `buffer(side='left'|'right')`, `minimum_bounding_circle` (Welzl smallest enclosing
  circle), `minimum_bounding_radius`, `subdivide` (recursive bbox split under a vertex budget),
  `snap_to_grid`, `swap_xy`, `self_intersections`, and `sample_points(n, *, seed)` (deterministic
  uniform sampling).
- `arr.dissolve(by)` groups and unions per key.
- `split_antimeridian` splits geometries crossing the ±180° meridian into seam-following
  multiparts (great-circle crossing latitudes, automatic pole closure), a port of the
  [JOSS antimeridian algorithm](https://joss.theoj.org/papers/10.21105/joss.07530).

### Measurement and geodesy

- CRS-driven measurement with native-by-default results: on a geographic CRS,
  `area`/`length`/`distance`/`dwithin` are exact geodesic
  ([Karney's algorithm](https://geographiclib.sourceforge.io/), meters); `buffer`/`offset_curve`
  (and similar meter-distance constructive metrics) use a **local projection** approximation.
  Projected CRSs measure planarly in **native linear units**; CRS-free geometry uses coordinate
  units. Pass `unit='meters'` for SI; `to_crs` changes the frame.
- CRS-aware `Point` geodesy: `bearing`, `destination`, `point_between`.
- Coordinate-space tolerances (raw coordinate units): `simplify`, `segmentize`, `snap`,
  `remove_repeated_points`.
- 3D/M: `length_3d`, `distance_3d`, `min_z` / `max_z` / `z_range`, `bounds_3d`, and M linear
  referencing through `line_interpolate(..., basis='m')`, `line_locate(..., basis='m')`, and
  `line_substring(..., basis='m')`.
- Linear referencing: `line_interpolate`, `line_locate`, `line_substring`, `split`, and
  `interpolate_m`.
- Rhumb distance plus `path='rhumb'` navigation on `bearing`, `destination`, and `point_between` (ellipsoidal, order-6
  series) and signed `cross_track_distance`.
- Shape predicates `is_convex` and `extremes` (west/south/east/north as `Extremes`).
- Coordinate clustering composes with scikit-learn/scipy via
  [`get_coordinates`][gometry.get_coordinates] and the array's zero-copy column views.
- Proximity: `nearest`, `nearest_points`.

### CRS via PROJ

- Bundled libPROJ backend (no system PROJ in wheels) with direct Rust fast paths for WGS 84
  lon/lat, Web Mercator, and WGS 84 UTM, falling back to libPROJ for the broader
  [EPSG](https://epsg.org/) / PROJ-string / WKT / PROJJSON / datum-pipeline / grid catalog.
- Always-X/Y transform API regardless of authority axis order; Z transformed when consumed, Z/M
  otherwise preserved.
- First-class [`gm.CRS`][gometry.CRS] from `geom.crs`: comparison against EPSG ints/strings,
  classification (`is_geographic`, `is_projected`, `kind`), canonicalization (`to_authority`,
  `to_epsg`, `to_2d`, `to_3d`, `same_as`, `identify`), units/axes, standards export (`to_wkt`,
  `to_proj`, `to_projjson`), and per-ellipsoid geodesy (`geodesic`, `geodesic_direct`, `geodesic_interpolate`).
- Extent-aware `Geometry.estimate_local_crs()` / `GeometryArray.estimate_local_crs()` planning.
- Global `gm.crs_*` utilities: `transform`, `transform_bounds`, `apply`, `unit`,
  `units`, `info`, catalog search (`authorities`, `codes`, `search`, `catalog`, `utm_zones`,
  `celestial_bodies`, `ellipsoids`, `prime_meridians`, `proj_operations`), and local grid lookup
  (`grid`). There is no `CRS.list` / `gm.crs_list` — catalogs live on the
  global `crs_*` family, not as `CRS` classmethods.

### Discrete grids

- H3: point cells, boundaries, typed `H3Cell` with hierarchy/adjacency helpers and local-IJ
  indexing, resolution metadata, and polygon coverage via `gm.h3_cover(geom, resolution=..., cell_rule=...)`.
  Directed edges and vertices are first-class types (`gm.H3Edge`, `gm.H3Vertex`).
- S2: point cells, boundaries, typed `S2Cell`, hierarchy-aware cell-set algebra (`gm.s2_union`,
  `gm.s2_intersection`, `gm.s2_difference`), and coverage via `gm.s2_cover(geom, level=..., max_cells=...)`.
- Cell-set algebra (`union` / `intersection` / `difference`) is in **all four** grid families
  (H3, S2, geohash, tiles), not S2-only.
- Geohash (full DGGS): point cells, boundaries, typed `GeohashCell` (text base-32 token), cell-set algebra
  (`gm.geohash_union` / `intersection` / `difference`; compaction on the `CellArray`), and
  `gm.geohash_cover(geom, precision=..., cell_rule=...)`.
- XYZ web-mercator tiles (full DGGS): `gm.Tile` (quadkey/id/lon-lat constructor),
  and `gm.tile_cover(geom, zoom=..., cell_rule=...)`.
- All four grids expose typed cells and packed `CellArray` columns with `cell_rule`-selected
  candidates, `.polygon`, `.to_polygon()`, `.compact()` / `.uncompact()`, indexing, and
  slicing, captured by the `gm.Cell` protocol. The source geometry remains caller-owned;
  free predicates (`covers` / `contains` / `intersects` and `_xy` spellings) own exact
  relation semantics. Ragged results use the CSR-backed `Groups` container.
- Generic `CellArray.uncompact()` expands hierarchical cells in range order and avoids
  normalization when the input is already canonical. H3 keeps its native
  `h3o::CellIndex::uncompact` path.
- Geohash cells are identified by string tokens; unlike H3, S2, and tile cells, they do not
  accept numeric identifiers.
- Point geocodes: [Open Location Code](https://github.com/google/open-location-code)
  plus codes (`pluscode_encode`, `pluscode_polygon`, `pluscode_shorten`, `pluscode_recover`) and OSM
  shortlinks (`osm_shortlink_encode`, `osm_shortlink_location`).
- Space-filling-curve keys `spatial_key(curve='hilbert'|'morton')` (bbox-center keys,
  compatible with the [Hilbert](https://en.wikipedia.org/wiki/Hilbert_curve) ordering GeoPandas
  and DuckDB write) and `sort_by_spatial_key(curve=...)`.
- Polyline codec `from_polyline` / `to_polyline`
  ([Google Encoded Polyline](https://developers.google.com/maps/documentation/utilities/polylinealgorithm),
  precision 5/6).

### Indexing and joins

- Bulk-loaded in-memory [R-tree](https://en.wikipedia.org/wiki/R-tree) over geometry envelopes
  via `gm.SpatialIndex(...)`, with candidate retrieval (`candidates`), exact predicate refinement
  (`query`), nearest queries (`nearest`, with `max_distance` / `exclusive` and geodesic lower-bound
  pruning), explainable plans (`explain`), and dynamic `insert` / `remove`.
- Rust-backed spatial joins via `gm.join(...)` with exact predicate refinement;
  `SpatialIndex.explain(...)` is the dedicated query-plan diagnostic.

### Validation and repair

- Structured validation reports (`validate`) with `valid` / `reason` / `location` / `path`
  diagnostics and a `repair` shortcut, plus the universal `gm.require(...)` boundary contract.
- Validity is exact topology: repeated consecutive vertices are removable redundancy (valid and
  simple under [OGC](https://www.ogc.org/standard/sfa/) validity, not a self-intersection), while
  real defects (self-intersections, disconnected interiors, nested holes, collapsed rings) are
  caught exactly, with no tolerance heuristics.
- Deterministic polygon `repair` on all four surfaces with two strategies — `linework` (even-odd
  over the noded boundary) and `structure` (shells unioned minus holes) — Z/M carried through, with
  a validate-first fast path.
- Coordinate `quantize`; dimension moves via `set_z` / `set_m` (`None` clears, numeric assigns).

### Typed columnar containers

- [`GeometryArray`][gometry.GeometryArray] — the packed geometry column, with runtime
  `GeometryArray[Point]` subscription.
- `CellArray` — a packed, homogeneous column of discrete-grid cells (H3 / S2 / geohash / tile),
  sharing the cell operation surface columnar-wide.
- `Groups` — a CSR-backed grouped result (offsets + ids) for ragged group-by output.
- `Cell.children_count` is part of the public cell protocol.

### Integration layer

- **pandas** — a geometry extension dtype and array, so a `GeometryArray` lives in a
  `DataFrame`/`Series` column natively.
- **polars** — geometry columns store **EWKB** binary through explicit
  `to_polars` / `from_polars` conversion; CRS restore is limited to an integer
  EPSG SRID when present, and epoch does not ride the EWKB path.
- **GeoPandas** — `arr.to_geopandas()` / `gm.from_geopandas` bridge a `GeometryArray` to and from a
  `GeoSeries`/`GeoDataFrame`.
- **GeoParquet** — `arr.to_geoparquet()` / `gm.from_geoparquet` write and read the columnar file
  format, CRS encoded as the spec requires.
- **lonboard** — `gm.explore(...)` renders geometry to an interactive deck.gl map.
- **Native Arrow** — homogeneous arrays export/import separated `x`/`y`/`z`/`m` GeoArrow children,
  with WKB fallback for mixed types ([`from_arrow`][gometry.from_arrow] / [`to_arrow`][gometry.Geometry.to_arrow]).
- In pandas integration, `GeometryDtype.__from_arrow__` accepts `pyarrow.Array` and
  `pyarrow.ChunkedArray`; `GeometryExtensionArray.__setitem__` accepts Python integers,
  NumPy integer scalars, and pandas `ListLike` indexers.

### IO and interop

- WKT (modern Z/M/ZM tags, `output_dimension` control), ISO WKB and EWKB (Z/M/SRID),
  [GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946) (RFC 7946, Z roundtrip, M rejected),
  and `__geo_interface__`.
- Feature-level GeoJSON: `from_features` into a validated `Features` record;
  `to_feature_collection(..., ids=)` writes them back.
- ISO WKB as the portable binary boundary; PostGIS-style EWKB via
  `to_wkb(include_srid=True)` when an integer EPSG SRID must ride the bytes;
  `from_wkt` reads EWKT with a SRID-wins contract.
- Packed [GeoArrow](https://geoarrow.org/) export/import for homogeneous XY/XYZ/XYM/XYZM arrays,
  with WKB fallback for mixed geometry types, mixed axes, and collections (`from_arrow`, `to_arrow`).
- Precision-controlled serialization from a geometry — `geom.to_wkb(include_srid=..., precision=...)`
  and `geom.to_wkt(precision=...)` on `Geometry` and `GeometryArray`, so `gm.Point(x, y,
  crs=4326).to_wkb(include_srid=True, precision=7)` encodes wire bytes without a separate scalar codec.
- Antimeridian support: topology, relate, overlay, distance, bounds, centroid,
  point-on-surface, prepared geometry, spatial indexes, validation/`require`,
  `repair`, and `snap_to_grid(..., repair=True)` auto-normalize seam-crossing
  geographic input; `box(..., wrap="split")`, `crosses_antimeridian`, and
  `split_antimeridian` remain for planar constructive ops (`buffer`, `convex_hull`,
  `simplify`, `offset_curve`) that stay coordinate-planar.
- NumPy-native results: every bulk metric/predicate/index/bounds op returns a read-only
  `numpy.ndarray` (`float64` / `bool_` / `int64` / `uint64`); `bounds` is `(n, 4)`, joins and
  `self_join` return a `(left, right)` pair of int64 arrays, and grouped matches use the CSR `Groups`
  container. Geometry results stay a packed `GeometryArray` (with `to_numpy` bridges
  and `__array_ufunc__ = None`); coordinates expose `coords.x/.y/.z/.m` as float64 ndarrays.
  Strict broadcasting (scalar×array, equal-length pairwise; mismatched lengths raise).

### Runtime

- Built with `gil_used = false`: free-threaded CPython **cp314t** runs gometry
  without forcing the GIL back on (cp313t is neither tested nor published). See
  [Free-threaded Python](internals.md#free-threaded-python).

### Known boundaries

- GeoArrow packed layout covers homogeneous XY/XYZ/XYM/XYZM simple and multi arrays plus WKB
  fallback for mixed axes and unsupported families.
- S2 covers typed cells and explicit cell-selection rules, not full S2 boolean topology.
- Antimeridian auto-normalization covers topology, metrics, indexing, and prepared paths;
  planar constructive ops (`buffer`, `convex_hull`, `simplify`, `offset_curve`) still need
  an explicit `split_antimeridian` (or projected frame) on crossing input.
- No precision model: `quantize` rounds decimal places; overlay has no `grid_size` snap-rounding.
- Coverage validation/simplify/clean/union, linework `node`, and `build_area` are included; full
  snap-rounded overlay with a `grid_size` precision model is not.
- `split` accepts point splitters. Splitting lines by lines or polygons by lines is not included.
- No database adapters. A conservative closed-form registry accelerates common
  admitted projections, while bundled PROJ remains the authority database,
  CRS parser, datum/grid pipeline engine, and fallback. GeoArrow metadata
  writes PROJJSON, so columns handed to a GeoParquet writer carry the CRS
  encoding the spec requires.
