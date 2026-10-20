---
description: How gometry's native engine, bundled PROJ, and concurrency model affect Python applications.
---

# Engine and concurrency {#internals}

gometry runs geometry operations in Rust and uses Python for ecosystem
integration. This keeps array operations out of Python loops and lets the
package bundle its geometry and CRS engines together.

## Native and Python responsibilities {#ownership-layers}

| Layer | Responsibility |
| --- | --- |
| Native geometry core | Construction, predicates, overlay, measurement, CRS handling, geodesy, grids, indexing, validation, repair, and geometry I/O. |
| Python interface | Python protocols, type annotations, and optional adapters. |
| CRS authority | The bundled PROJ backend supplies CRS definitions, datum pipelines, grid-aware transforms, and standards parsing. Gometry owns frame checks, X/Y ordering, Z/M policy, and Python errors around that boundary. |
| Interchange adapters | Arrow/GeoArrow, WKB, dataframe, GeoParquet, and map adapters translate at explicit boundaries and state when they copy or lose metadata. |

## Bundled CRS transformations {#crs-authority-boundary}

Gometry bundles PROJ and its CRS database; no system installation is needed.
The Python API uses X/Y even when an authority definition declares a different
native axis order. See [CRS, units & measurement](../guide/crs.md) for frame
matching, units, and transformation resources.

## Geographic topology {#geographic-quotient-embedding}

Geographic topology treats seam-equivalent longitudes and longitude aliases at
the poles as the same locations. Geometry edges remain straight in their source
coordinate chart; geodesic measurement is a separate operation. Results use
binary64 coordinates, and an operation reports an error rather than silently
changing topology when coordinate publication cannot preserve the required
relationships. Z and M behavior remains specific to each operation; see
[geometry and coordinates](../guide/geometry.md#zm-under-operations).

## Data interchange {#hard-boundaries}

Exchange geometry with dataframes and databases through Arrow, GeoArrow, or
WKB. gometry does not provide a storage or SQL engine.

The Arrow C Data Interface is an ABI boundary. Providers must conform to the
interface; gometry copies and validates admitted data before decoding it. See
[Arrow & storage](../ecosystem/arrow.md) and [Security & untrusted input](security.md)
for the trust and resource boundaries.

## Runtime concurrency

On standard CPython, native work can release the interpreter lock after Python
inputs are admitted, including bulk array work and scalar topology predicate and
nearest-point evaluation. Lock release is an implementation detail, not a
per-API parallelism guarantee. On a [free-threaded build](compatibility.md#runtime-and-platform-matrix),
the same public API is used without separate thread-specific entry points.

Immutable geometries, arrays, cells, CRS values, and grouped containers can be
shared after construction, including the indexes a geometry builds for itself
when it is probed repeatedly. [`SpatialIndex`][gometry.SpatialIndex] supports concurrent
query-only use; callers should serialize writes and queries that must observe a
particular write boundary.

Use processes when hard isolation, independent resource budgets, or standard
CPython process-level parallelism is the requirement. [`pickle`](https://docs.python.org/3/library/pickle.html) carries the durable
geometry values and containers, while derived index or prepared state is
reconstructed when loaded.

## See also

- [API conventions](design.md) — naming and result conventions.
- [Compatibility](compatibility.md) — runtime and optional-integration matrix.
- [Arrow & storage](../ecosystem/arrow.md) — columnar interchange behavior.
- [Security & untrusted input](security.md) — ingress trust boundaries and limits.
