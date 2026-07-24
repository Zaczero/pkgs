---
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

<img src="assets/logo.svg" alt="gometry" class="hero-logo">

# One geospatial engine, one mental model

A high-performance **Rust** geometry engine with a **Pythonic** API. gometry replaces the
day-to-day stack of **Shapely + pyproj + h3-py + s2sphere + rtree** with one coherent,
fast, hard-to-misuse package — no GEOS, no GDAL, no system PROJ.

[Get started :material-arrow-right:](get-started/quickstart.md){ .md-button .md-button--primary }
[Why gometry](#why-gometry){ .md-button }

</div>

```bash
pip install gometry
```

```python exec="on" source="block" result="text"
import gometry as gm

stop = gm.Point(2.3479, 48.8589, crs=4326)
catchment = stop.buffer(500)
neighborhoods = gm.GeometryArray([
    gm.box(2.344, 48.857, 2.349, 48.861, crs=4326),
    gm.box(2.360, 48.865, 2.366, 48.870, crs=4326),
])
nearby = gm.SpatialIndex(neighborhoods).query(catchment, predicate="intersects")

print("catchment area m^2:", round(catchment.area))
print("nearby neighborhood rows:", nearby.tolist())

```

<div class="grid cards" markdown>

-   :material-earth:{ .lg .middle } **Explicit Earth model**

    ---

    The CRS decides every metric, and the result is **native** for that CRS. No guessing whether
    `area` is degrees² or m², or whether `buffer(100)` means meters.

    [:octicons-arrow-right-24: Measurement & the CRS](guide/crs.md)

-   :material-flash:{ .lg .middle } **Rust-fast, vectorized**

    ---

    Batched Rust kernels, [GeoArrow](https://geoarrow.org/)-compatible interchange, and
    vectorized array methods. Scalar calls stay a single, ordinary method call.

    [:octicons-arrow-right-24: Arrays & performance](guide/arrays.md)

-   :material-vector-polygon:{ .lg .middle } **Grids, indexes, geodesy**

    ---

    H3 & S2 coverages with explicit `cell_rule` and built-in exact membership, spatial indexing with
    `explain`, and millimeter-accurate [ellipsoidal geodesics](https://geographiclib.sourceforge.io/) — all on one geometry type.

    [:octicons-arrow-right-24: Discrete grids](guide/grids.md)

</div>

## Why gometry

<div class="rationale" markdown>

### The CRS is the single knob

The usual stack makes you remember whether a metric means degrees or meters. gometry has one
`geom.area`, one `geom.length`, one `gm.distance(a, b)` — the geometry's CRS decides how
they are computed and the result is **native** for that CRS: a geographic CRS measures
geodesically in meters, a projected CRS uses its native linear units (feet stay feet;
meters stay meters), and a CRS-free geometry stays in coordinate units. Pass
`unit='meters'` for forced SI, or reproject with `geom.to_crs(...)` to change the frame.
There is no "square degrees" footgun to commit.

### Fast without awkwardness

The hot paths are Rust. Predicates, overlay, buffering, geodesics, and coverage run as
batched kernels over packed, GeoArrow-compatible buffers — so a million-point `contains`
is one call, not a Python loop. Grid `cover` factories return coverages with
exact membership against the source geometry.

### Hard to misuse

`set_crs` declares; `to_crs` transforms — and they can't be confused. Spatial indexes
expose `candidates` vs `query` (exact refine) instead of hiding the prefilter. Grid
coverages carry their `cell_rule` and exact membership predicates so you never mistake a
cover for exact geometry.

### One spelling per operation

Every important pattern from the tools it replaces maps to one obvious gometry
spelling, documented and searchable in the migration guide.

</div>

## Find your way

<div class="grid cards" markdown>

-   **New here?** :material-rocket-launch:

    ---

    Build a real spatial query step by step.

    [:octicons-arrow-right-24: Quickstart tutorial](get-started/quickstart.md)

-   **Get something done** :material-wrench:

    ---

    The day-to-day API, one guide per topic.

    [:octicons-arrow-right-24: User guide](guide/geometry.md)

-   **Understand it** :material-lightbulb:

    ---

    The models and design behind the API.

    [:octicons-arrow-right-24: The mental model](get-started/mental-model.md)

-   **Look it up** :material-book-open-variant:

    ---

    Every public callable, grouped by concept with precise signatures and links.

    [:octicons-arrow-right-24: API reference](api/index.md)

</div>
