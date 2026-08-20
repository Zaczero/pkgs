---
description: How gometry is built — a Rust core that owns all logic and hot paths, a thin Python facade of native aliases, bundled libPROJ, a GeoArrow interchange boundary, and free-threaded CPython support.
---

# Internals

gometry is a Rust engine with a thin Python skin, and that split explains its
performance, its small dependency surface, and the shape of its API. This page
walks the boundaries the codebase holds as invariants, from the inside out: how
the Rust core and Python facade divide responsibility, why CRS work is delegated
to a bundled libPROJ, what is deliberately kept out of the core, how geometry
columns cross the GeoArrow interchange boundary, and how the extension behaves
under free-threaded CPython.

## Rust owns the logic; Python is a facade

**All geometry kernels and every hot path live in Rust.** The PyO3-backed native extension
implements construction, predicates, overlay, measurement, CRS handling, geodesy, H3/S2
grids, spatial indexing, validation/repair, and geometry IO. Python owns thin
presentation and optional-integration glue, including feature-property assembly
and adapter validation; it does not own geometry kernels or per-coordinate hot loops.

The Python package is a thin, curated facade over the compiled extension (`_lib`):

| Layer | Role |
|---|---|
| `gometry/__init__.py` | The curated top-level surface: constructors, operations, predicates, IO, geometry/index classes, and globally discoverable domain families (`crs_*`, `h3_*`, …). |
 | `gometry/_types.py` | Private support for the stub's precise token, protocol, and structured-return types; `Cell` is re-exported at top level. |
| `gometry/_lib.pyi` | Type stub for the compiled extension; the one place typed signatures live. |
| `gometry/_arrow.py`, `_optional.py`, `_pandas.py`, `_polars.py`, `_geopandas.py`, `_geoparquet.py`, `_viz.py` | Optional, lazily-imported integration glue and shared missing-dependency handling (pyarrow, pandas, polars, GeoPandas, GeoParquet, lonboard). |

Because the facade exports direct native aliases, there is exactly one implementation
and one set of typed signatures per operation. `gm.crs_transform(...)` *is* the native
`crs_transform` — no Python indirection sits between you and the kernel.

The rules that decide how each operation is *spelled* — one canonical name, the
unary-method / binary-free placement, and the flat family taxonomy — live in
[Design principles](design.md), which owns that doctrine.

!!! note "Prefix families are a presentation layer, not a cost layer"
    `gm.h3_cover` / `gm.s2_cover` / `gm.geohash_cover` / `gm.tile_cover` are
    global family factories that return a typed cell array from a geometry. Metric operations
    dispatch on the geometry's CRS in the Rust kernel, so the CRS-driven API is
    essentially free at runtime.

## The CRS backend: bundled libPROJ

CRS transformation is deep, standards-heavy, and politically fraught, so gometry does **not**
reimplement it. It uses **[libPROJ](https://proj.org/) as the authority backend, bundled into the wheel** via
GeoRust `proj-sys`. PROJ owns the CRS database, datum pipelines, grid-aware transforms,
area-of-use selection, and WKT/PROJJSON parsing; gometry owns the Pythonic geometry API, the
CRS metadata invariants, the Z/M preservation policy, the fast common transforms, and the
error model.

```python exec="on" source="block" result="text"
import gometry as gm

engine = gm.crs_engine()
print("PROJ version:", engine["version"])
print("4326 is geographic:", gm.CRS(4326).is_geographic)
print("3857 is projected: ", gm.CRS(3857).is_projected)

```

A conservative **in-core** projection registry admits exact closed-form kernels for
selected horizontal methods (Transverse Mercator including UTM, spherical
Pseudo-Mercator, LCC 1SP/2SP, polar stereographic, Lambert azimuthal equal-area,
Albers equal-area, and oblique stereographic) when datum, units, and method
gates pass — including datum-matched NAD83/ETRS cases, not only WGS 84. Anything
outside that admission gate (grids, Helmert, or unlisted methods)
falls back to the bundled PROJ database. Because PROJ travels inside the wheel,
there is no system PROJ shared library and no `PROJ_LIB` to configure. See the
[CRS, units & measurement](../guide/crs.md).

## Hard boundaries: what is *not* in the core

gometry owns geometry semantics and kernels — not the storage and analytics ecosystem. The
core deliberately excludes:

- **GEOS / GDAL** — the geometry kernels are gometry-owned Rust, not GEOS/GDAL bindings.
- **PostGIS** — the portable database boundary is ISO WKB (or PostGIS EWKB when
  requested); drivers and SQL belong in application-owned adapters.
- **Polars / DataFusion / DuckDB** — dataframe engines live outside the Rust core
  (optional Python adapters may speak EWKB/Arrow).
- **Full GeoParquet engines** — the Rust core stops at Arrow/WKB; a focused optional
  Python `from_geoparquet` / `to_geoparquet` layer exists, not a general parquet engine.

This keeps the dependency surface — and the supply-chain attack surface — small: NumPy is
the required Python runtime dependency for native bulk numeric outputs, the integration
extras (`arrow`/`pandas`/`polars`/`geopandas`/`viz`) are opt-in and lazily imported, and every
Rust dependency is built with `default-features = false`.

## The interchange boundary: GeoArrow

The public columnar contract is **[Apache Arrow](https://arrow.apache.org/) / [GeoArrow](https://geoarrow.org/)-compatible buffers**, not a
gometry-private array format. This is the import/export boundary across which
geometry columns move between systems without being exploded into millions of
Python objects:

```text
PyArrow / dataframe / dataset producer
        |
   GeoArrow-compatible buffers   <-- the public ABI
        |
   gometry Rust kernels
        |
   GeoArrow-compatible buffers
        |
PyArrow / dataframe / dataset consumer
```

```python
import gometry as gm

points = gm.points(lons, lats, crs=4326)
ga = points.to_arrow()           # GeoArrow-compatible layout (requires gometry[arrow])
roundtrip = gm.from_arrow(ga)

```

Internally gometry uses owned geometry storage or specialized scratch layouts
for overlay, noding, triangulation, or indexing — Arrow is the *public*
interchange ABI, not the internal algorithm religion. Homogeneous simple and
multi-geometry output uses separated `x`/`y`/`z`/`m` child arrays, which are easy
to scan, filter, and map to column statistics; mixed families use GeoArrow WKB
fallback. See [Arrow & storage](../ecosystem/arrow.md).

## Free-threaded Python

CPython ships an optional **free-threaded** build where the global interpreter
lock (GIL) can be disabled. gometry declares support for that mode: the extension
module is built with `#[pymodule(gil_used = false)]`, so PyO3 does not force the
GIL back on when you run a no-GIL interpreter. The tested and published
free-threaded target is **cp314t**; `cp313t` is neither tested nor published.

### Three separate guarantees

- **Standard CPython:** vectorized array and broadcast kernels detach the GIL for
  the duration of the Rust pass (including packed interpolation, substring, count
  interpolation, and unique-points). Scalar per-geometry calls do **not** release
  the GIL. Threads can therefore overlap bulk array work on a normal GIL-enabled
  interpreter without waiting on per-call Python entry.
- **Free-threaded source builds:** `#[pymodule(gil_used = false)]` keeps a locally
  built extension from re-enabling the GIL on a no-GIL interpreter. PyO3 also
  checks that exposed classes satisfy its `Send + Sync` requirements.
- **Published wheels:** the release matrix builds cibuildwheel's supported
  `cp314t` tags. It does not publish `cp313t`; a standard `cp313`/`cp314` wheel
  is not a no-GIL wheel.

Source compatibility proves memory-safety eligibility, not the absence of logic
races in mutable workflows. The current verification includes shared-state stress
tests; Linux `cp314t` is an explicit CI lane and cibuildwheel 4 builds the
supported `cp314t` artifacts.

In practice:

- A source-built no-GIL extension can run geometry kernels from multiple Python
  threads without the GIL serializing every FFI entry.
- The **same API** applies — callable geometry classes, method + free-function operations,
  and typed arrays; no separate "thread-safe" entry points.

```python exec="on" source="block" result="text"
import sys
import threading

import gometry as gm

def worker(lon: float) -> float:
    pt = gm.Point(lon, 52.0, crs=4326)
    box = gm.box(lon - 0.5, 51.5, lon + 0.5, 52.5, crs=4326)
    return box.area

threads = [threading.Thread(target=worker, args=(21.0 + i * 0.1,)) for i in range(4)]
for t in threads:
    t.start()
for t in threads:
    t.join()

print("free-threaded build:", bool(getattr(sys.flags, "gil", 1) == 0))
print("sample area (m^2):", round(worker(21.0)))

```

### Prepared point classification

Prepared point-in-polygon batches retain exact predicate semantics while
avoiding the exact kernel for certified cells. After 10,000 probes, an eligible
prepared polygonal geometry lazily builds a conservative 64×64 classification
grid. Cells are `Inside`, `Outside`, or `Maybe`; only `Maybe` cells continue to
exact evaluation. The grid is therefore a skip structure, not a predicate
approximation.

### Hierarchical uncompact

Generic `CellArray.uncompact()` walks cell ranges in order and emits leaves
directly. It normalizes only non-canonical input rather than always sorting a
leaf set after expansion. Canonicality falls out of a single linear scan: when
every adjacent pair satisfies `previous.range_max() < next.range_min()`, the
input is at once sorted, duplicate-free and free of ancestor overlap, so the
normalize pass is skipped. Input that scan cannot prove canonical is normalized
anyway, since a false positive would produce wrong output while a false negative
only costs time. The budget estimate is taken from the raw input before either
step, so duplicate and overlapping cells still count toward rejection. H3 remains
separate because `h3o::CellIndex::uncompact` owns that expansion.

### Shared mutable engines

[`PreparedGeometry`][gometry.PreparedGeometry] is safe to share and query from
many threads without extra synchronization: it wraps immutable geometry state,
and its lazy prepared caches use thread-safe slots internally. Grid cover
factories return caller-owned `CellArray` values for scalar inputs, or `Groups`
of `CellArray` values for array inputs. Keep the source geometry and use free
predicates for exact membership; the returned cells do not retain the source.

[`SpatialIndex`][gometry.SpatialIndex] has real mutation through `insert` and
`remove`. Concurrent query-only use is supported, but callers should serialize
writes, and serialize any query that must observe a particular write boundary,
just as they would with any shared mutable index.

Immutable values and containers (`Geometry`, `GeometryArray`, `CellArray`,
`Groups`, `CRS`, and cells) are safe to share across threads once constructed.

### When to prefer processes

CPU-heavy batch work still benefits from **process-level** parallelism when you
want to bypass the GIL entirely on the standard (GIL-enabled) CPython build, or
when you need hard isolation between jobs. gometry geometries pickle cleanly —
spawn workers, ship `GeometryArray` payloads, collect results.

For dataframe workflows, prefer the batched Rust kernels over splitting a single `GeometryArray` across
unsynchronized writers.

## See also

- [Design principles](design.md) — the one-obvious-way API constitution.
- [Mental model](../get-started/mental-model.md) — the user-facing consequence of this architecture.
- [Arrow & storage](../ecosystem/arrow.md) — the columnar interchange boundary in use.
