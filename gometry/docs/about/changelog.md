---
description: gometry 1.0.0 user-visible release notes for geometry, CRS, geodesy, grids, indexing, validation, and interoperability.
---

# Changelog

## 1.0.0 — Unreleased

gometry 1.0.0 is the first public release. It provides one typed Python surface
for geometry values and columns, CRS-aware measurement, geodesy, discrete grids,
spatial indexing, validation, and standards-based interchange.

### Geometry and dimensions

- Callable `Point`, `LineString`, `Polygon`, multi-geometry, and
  `GeometryCollection` classes construct scalar values; plural builders construct
  packed `GeometryArray` columns.
- `XY`, `XYZ`, `XYM`, and `XYZM` layouts are explicit. Scalar
  `coordinate_axes` reports one layout; array `coordinate_axes` is row-aligned and
  `common_coordinate_axes` reports a shared layout when one exists.
- Typed empty geometries preserve their declared axes through WKT and WKB.
- `set_crs` declares the frame of existing coordinates and `to_crs` transforms
  coordinates. Coordinate epochs require a CRS and can be carried through
  supported transforms.
- Geometry, array, CRS, cell, and grouped-container values support the documented
  Python value protocols, including copying and trusted pickle persistence.
- Structured geometry, CRS, transform, and parse exceptions are re-exported at
  the package top level.

### Predicates and constructive operations

- Top-level predicates cover DE-9IM relationships, coordinate probes, distance
  predicates, and prepared-geometry operands.
- Array operations use strict scalar/array broadcasting: scalar-to-array and
  equal-length pairwise inputs are accepted, while mismatched non-scalar lengths
  raise rather than creating an implicit Cartesian product.
- Overlay, noding, polygonization, buffering, offsets, simplification, snapping,
  segmentization, affine transforms, hulls, triangulation, Voronoi, and linear
  referencing are available on their documented scalar and array surfaces.
- `segmentize(max_length)` follows the CRS metric for its length bound and only
  inserts vertices; `fraction=` is a unitless per-segment subdivision.
- Constructive results preserve Z/M only where the source vertices determine
  them. Operations that invent new vertices return an honest two-dimensional
  result.

### CRS, measurement, and geodesy

- Geographic CRS metrics (`area`, `length`, `distance`, and `dwithin`) use
  ellipsoidal metres; projected CRS metrics use native linear units; CRS-free
  metrics use coordinate units.
- `unit='meters'` and `unit='planar'` provide explicit metric overrides where the
  operation supports them. Geographic `buffer` and `offset_curve` use metre
  inputs with a documented local-projection construction rather than claiming an
  exact ellipsoidal offset.
- Point bearing, destination, interpolation, rhumb navigation, cross-track
  distance, 3D measures, and M-based linear referencing are available with the
  documented frame rules.
- CRS construction, introspection, standards export, operation discovery, local
  frame estimation, and raw coordinate transforms use the bundled PROJ authority
  boundary.
- Geographic topology, indexing, and validation document their antimeridian
  normalization behavior; planar constructive operations expose explicit seam
  splitting where required.

### Grids and indexing

- H3, S2, geohash, and XYZ tile cells share typed cell and `CellArray` protocols,
  while retaining each system's native depth name.
- Grid cover factories return `CellArray` for scalar input and `Groups` of
  `CellArray` for array input. `cell_rule` makes candidate selection explicit;
  exact membership remains a predicate against the source geometry.
- Cell hierarchy, set algebra, compaction, uncompact, point geocodes, and spatial
  keys are available through the documented family functions and cell methods.
- `SpatialIndex` exposes bounding-box `candidates`, exact predicate `query`,
  nearest-neighbor queries, explainable plans, joins, and mutable insert/remove
  operations. Row identity and missing rows remain explicit.

### Validation and safety

- `validate` returns structured validity information; `repair` provides deliberate
  linework and structure strategies.
- `require` combines parsing, frame, axes, and validity contracts at an ingress
  boundary.
- WKT, WKB, EWKB, GeoJSON, feature records, and Arrow imports reject malformed
  structures with typed exceptions and preserve documented null and missing-row
  lanes.
- Pickle is documented for trusted Python persistence only. Arrow providers must
  conform to the C Data Interface; imported buffer contents are copied and
  validated within the documented resource boundaries.

### Interoperability

- Native Arrow C capsules are available without PyArrow. `__arrow_c_array__`
  returns a `(schema_capsule, array_capsule)` pair; `to_arrow` and `from_arrow`
  provide the optional PyArrow object path.
- GeoArrow carries packed homogeneous layouts and a WKB fallback for mixed
  geometry or axes. CRS metadata uses PROJJSON; gometry's epoch extension is
  preserved only by extension-aware consumers.
- Explicit pandas, Polars, GeoPandas, GeoParquet, and lonboard adapters are
  opt-in; see [Ecosystem & interoperability](../ecosystem/index.md) for their
  boundary contracts.
- ISO WKB is the portable binary boundary; EWKB/EWKT SRIDs are opt-in, and
  GeoJSON follows its WGS 84 and Z/M limitations.

### Runtime support

- The supported standard interpreters are CPython 3.11 through 3.14.
- The supported free-threaded wheel target is CPython `cp314t`.
- Core wheels bundle the CRS authority resources and require NumPy, not a system
  GEOS, GDAL, or PROJ installation.

See [Compatibility](compatibility.md) for the complete runtime and optional
integration matrix, and [License](license.md) for bundled notices.
