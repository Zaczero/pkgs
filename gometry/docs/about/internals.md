---
description: Durable gometry architecture — Rust-owned geometry, a typed Python facade, bundled CRS authority, GeoArrow interchange, and concurrency boundaries.
---

# Internals

gometry has a Rust-owned geometry core and a typed Python facade. That ownership
split is the durable architecture behind the public API: geometry semantics,
storage, and resource transitions belong to the native core, while Python owns
presentation and optional ecosystem adapters.

## Ownership layers

| Layer | Responsibility |
| --- | --- |
| Native geometry core | Construction, predicates, overlay, measurement, CRS handling, geodesy, grids, indexing, validation, repair, and geometry I/O. |
| Python facade | Curated public exports, Python protocols, type-facing declarations, and optional adapters. It does not reimplement geometry kernels or per-coordinate loops. |
| CRS authority | The bundled PROJ backend supplies CRS definitions, datum pipelines, grid-aware transforms, and standards parsing. Gometry owns frame checks, X/Y ordering, Z/M policy, and Python errors around that boundary. |
| Interchange adapters | Arrow/GeoArrow, WKB, dataframe, GeoParquet, and map adapters translate at explicit boundaries and state when they copy or lose metadata. |

The public API therefore has one operation spelling and one semantic owner. A
scalar method and its array form may return different shapes, but both remain
typed entry points to the same operation family.

## CRS authority boundary

CRS transformation is a standards-heavy domain. Gometry bundles libPROJ rather
than asking the host to provide a shared library, and delegates CRS database,
datum, grid, and general pipeline semantics to it. The Python API remains
always X/Y even when an authority definition declares a different native axis
order. See [CRS, units & measurement](../guide/crs.md) for the user-facing
frame and operation-family rules.

## Geographic quotient embedding

Geographic overlay selects boundaries in an exact quotient graph, where seam
aliases and pole ports share their topological identities. Coordinate emission
is a separate step: the nearest-even binary64 table is tried first and is
serialized unchanged when it preserves the exact chart. The Q certificate
checks live-node identity, exact port lifts and cut pairing, segment incidence,
cyclic fan order, global face and host correspondence, and shell/hole roles.

If nearest rounding fails those checks, the shared deterministic embedding
search moves constructed nodes among adjacent binary64 coordinates. Stored
source vertices and graph segmentation remain fixed. One accepted node table
feeds polygon, line, and point publication; no alternate rounded noder or
coordinate grid is used.

Ordinary cycle roles use an independent winding proof over continuously lifted
exact ports. Quotient closures use the shared chart-turn and bounded-pole
classification. A full-period non-seam edge retains its directed circle, while
the two seam spellings identify one quotient point. Candidate roles are checked
against the published ring's shared domain facts.

Frame-aware validity checks the quotient topology without recharting stored
coordinates. It therefore preserves narrow edges whose distinct binary64
longitudes would merge after a period translation.

## Hard boundaries

Gometry owns geometry semantics and kernels, not application storage or query
engines. Dataframes, SQL databases, and lakehouse orchestration remain outside
the core. The public boundary is standards-based:

```text
application or data-system producer
            |
     Arrow / GeoArrow / WKB
            |
     gometry geometry core
            |
     Arrow / GeoArrow / WKB
            |
application or data-system consumer
```

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

- [Design principles](design.md) — public naming and ownership decisions.
- [Compatibility](compatibility.md) — runtime and optional-integration matrix.
- [Arrow & storage](../ecosystem/arrow.md) — columnar interchange behavior.
- [Security & untrusted input](security.md) — ingress trust boundaries and limits.
