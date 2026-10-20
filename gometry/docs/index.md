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
day-to-day stack of **Shapely + pyproj + h3-py + s2sphere + rtree** with one coherent
package — no GEOS, no GDAL, no system PROJ.

[Get started :material-arrow-right:](get-started/quickstart.md){ .md-button .md-button--primary }
[User guide](guide/index.md){ .md-button }

</div>

```bash
uv add gometry
```

```python exec="on" source="block" result="text"
import gometry as gm

origin = gm.Point(2.2945, 48.8584, crs=4326)
landmarks = {
    "Arc de Triomphe": (2.2950, 48.8738),
    "Louvre": (2.3376, 48.8606),
    "Notre-Dame": (2.3499, 48.8530),
    "Versailles": (2.1204, 48.8049),
}

print("Within 5 km of the Eiffel Tower")
for name, coordinates in landmarks.items():
    distance = gm.distance(origin, gm.Point(*coordinates, crs=4326))
    if distance <= 5_000:
        print(f"{name:<18} {distance / 1_000:>4.1f} km")
```

Find Paris landmarks within 5 km of the Eiffel Tower. With EPSG:4326 attached,
[`distance`][gometry.distance] measures the geodesic distance in metres, so the
filter works directly in familiar units.

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
    [vectorized array methods](guide/arrays.md). Scalar calls stay a single, ordinary method call.

    [:octicons-arrow-right-24: Benchmarks](about/benchmarks.md)

-   :material-vector-polygon:{ .lg .middle } **Grids, indexes, geodesy**

    ---

    H3, S2, geohash, and tile covers with explicit `cell_rule`, spatial indexing,
    and [ellipsoidal geodesics](https://geographiclib.sourceforge.io/) — all on one geometry type.

    [:octicons-arrow-right-24: Discrete grids](guide/grids.md)

</div>

## Why gometry

<div class="rationale" markdown>

### The CRS is the single knob

A geographic CRS measures on its ellipsoid; a projected CRS uses native linear
units, and a CRS-free geometry uses raw coordinate units. Override with
`unit='planar'` or `unit='meters'` when you need another interpretation.
The [measurement guide](guide/crs.md) explains edge models and units.

### Batched without hidden loops

Pass an array to [`contains`][gometry.contains], buffer a whole column, or
transform thousands of coordinates in one call. Packed arrays keep per-row work
in Rust.

### Declare, transform, refine

[`with_crs`][gometry.Geometry.with_crs] labels coordinates;
[`to_crs`][gometry.Geometry.to_crs] transforms them.
[Index candidates](guide/indexing.md#candidates-vs-query) and grid cells narrow
a search; exact predicates on the source geometry decide membership.

### One spelling per operation

Facts are properties, unary operations are methods, and binary relationships
are free functions. Find familiar operations in the
[migration cheatsheet](migrating/cheatsheet.md).

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
