---
description: A geospatial engine for Python, written in Rust. Geometry, geodesy, CRS, and H3/S2 grids with no GEOS, GDAL, or system PROJ to install.
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

<img src="assets/logo.svg" alt="gometry" class="hero-logo">

# One engine, one mental model

A geospatial engine for Python, written in Rust. gometry replaces the
day-to-day stack of **Shapely + pyproj + h3-py + s2sphere + rtree** with one coherent
package — no GEOS, no GDAL, no system PROJ.

[Get started :material-arrow-right:](get-started/quickstart.md){ .md-button .md-button--primary }
[Installation](get-started/installation.md){ .md-button }
[User guide](guide/index.md){ .md-button }

</div>

```bash
uv add gometry
```

```python exec="on" source="block" result="text"
import gometry as gm

# A one-degree tile over Paris. The CRS travels with the geometry.
tile = gm.box(2.0, 48.0, 3.0, 49.0, crs=4326)

print(f"ellipsoidal area: {tile.area / 1e6:>8,.0f} km²")
print(f"planar area:    {gm.area(tile, unit='planar'):>8.4f} degree²")
print(f"Web Mercator:   {tile.to_crs(3857).area / 1e6:>8,.0f} km²")

```

One square degree is three different answers, and the CRS carried by the
geometry decides which one you asked for. On a geographic CRS,
[`area`][gometry.Geometry.area] measures the ellipsoidal area bounded by
straight longitude/latitude edges; `unit="planar"` measures the raw coordinates
as they stand; and reprojecting first returns that frame's own answer, which at
this latitude Web Mercator inflates 2.3 times. Geographic [`length`][gometry.Geometry.length]
uses the same chart-edge model. Point-to-point geographic distance follows an
ellipsoidal geodesic; shape distance minimizes geodesic separation over closed
chart-edge supports, including filled polygon interiors.

<div class="grid cards" markdown>

-   :material-earth:{ .lg .middle } **Explicit Earth model**

    ---

    The CRS decides every metric, and the result is **native** for that CRS. No guessing whether
    [`area`][gometry.Geometry.area] is degrees² or m², or whether [`buffer(100)`][gometry.Geometry.buffer] means meters.
    Geographic coordinates also retain their CRS-native angular units and periodic seam.

    [:octicons-arrow-right-24: Measurement & the CRS](guide/crs.md)

-   :material-flash:{ .lg .middle } **Native, vectorized**

    ---

    Batched Rust kernels, [GeoArrow](https://geoarrow.org/)-compatible interchange, and
    vectorized array methods. Scalar calls stay a single, ordinary method call.

    [:octicons-arrow-right-24: Arrays & performance](guide/arrays.md)

-   :material-vector-polygon:{ .lg .middle } **Grids, indexes, geodesy**

    ---

    H3, S2, geohash, and tile covers with explicit `cell_rule`, spatial indexing,
    and [ellipsoidal geodesics](https://geographiclib.sourceforge.io/) — all on one geometry type.

    [:octicons-arrow-right-24: Discrete grids](guide/grids.md)

</div>

## Why gometry

<div class="rationale" markdown>

### The CRS is the single knob

The usual stack makes you remember whether a metric means degrees or meters. gometry has one
[`geom.area`][gometry.Geometry.area], one [`geom.length`][gometry.Geometry.length], one [`gm.distance(a, b)`][gometry.distance] — the geometry's CRS decides how
they are computed and the result is **native** for that CRS: by default,
geographic shape area, length, and linear referencing measure straight
longitude/latitude chart edges on the ellipsoid, while point-to-point distances
follow geodesics. A projected CRS uses its native linear units (feet stay feet;
meters stay meters), and a CRS-free geometry stays in raw coordinate units.
`unit='planar'` selects Cartesian measurement in stored coordinates. Geographic
longitude and latitude values remain in the CRS's native angular units; the
periodic seam, pole, and prime meridian come from that same frame. Pass
`unit='meters'` for forced SI, or reproject with [`geom.to_crs(...)`][gometry.Geometry.to_crs] to change the frame.

### Batched without hidden loops

Geometry kernels are implemented in Rust. Predicates, overlay, buffering, geodesics,
and coverage run as
batched kernels over packed, GeoArrow-compatible buffers, so bulk
[`contains`][gometry.contains] work is one call rather than a Python loop. Grid
`cover` factories materialize typed cell arrays;
use free geometry predicates for exact source checks.

### Declare, transform, refine

[`with_crs`][gometry.Geometry.with_crs] declares metadata without moving numeric
coordinates; [`to_crs`][gometry.Geometry.to_crs] transforms X/Y into another frame.
See [when to declare or transform](guide/crs.md#set-crs-vs-to-crs). Spatial indexes
expose [`candidates`][gometry.SpatialIndex.candidates] as a bbox prefilter and
[`query`][gometry.SpatialIndex.query] as exact refinement. Grid cover factories
materialize cells with an explicit `cell_rule`. A cover or index candidate narrows
the search; it is not an exact geometry answer. Retain the source geometry and use
an exact predicate to refine those candidates into exact matches. See [index candidates and queries](guide/indexing.md#candidates-vs-query)
and [grid source membership](guide/grids.md#exact-membership-with-the-source-geometry).

### One spelling per operation

Every important pattern from the tools it replaces maps to one obvious gometry
spelling, documented and searchable in the migration guide.

</div>

## Start with what you need

<nav class="docs-map" aria-label="Documentation starting points">
  <a href="get-started/quickstart/">
    <strong>Quickstart tutorial</strong>
    <span>Build your first spatial query.</span>
    <small>Installation, geometry, indexing, and exact refinement</small>
  </a>
  <a href="guide/">
    <strong>User guide</strong>
    <span>Solve a geometry task.</span>
    <small>Focused guides for everyday operations</small>
  </a>
  <a href="get-started/mental-model/">
    <strong>Mental model</strong>
    <span>Understand CRS, units, and refinement.</span>
    <small>The concepts that govern results</small>
  </a>
  <a href="api/">
    <strong>API reference</strong>
    <span>Look up a public callable.</span>
    <small>Signatures and behavior grouped by concept</small>
  </a>
</nav>

For questions, bug reports, and security disclosures, see
[Support](about/support.md) and the [issue tracker](https://github.com/Zaczero/pkgs/issues).
