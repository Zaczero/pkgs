---
description: gometry spatial indexing — candidates (bbox prefilter) vs query (exact refine), nearest with units, explain plans, and join for many-to-many work.
---

# Spatial indexing & joins

A spatial index makes brute-force geometry practical: instead of testing a query
against every feature, you test it against the few whose bounding boxes could
possibly match. [`gm.SpatialIndex`][gometry.SpatialIndex] builds a bulk-loaded in-memory [R-tree](https://en.wikipedia.org/wiki/R-tree) over geometry
envelopes and returns a [`SpatialIndex`][gometry.SpatialIndex]. The index exposes
[`crs`][gometry.SpatialIndex.crs] and [`epoch`][gometry.SpatialIndex.epoch] for
the shared frame of its rows (the same metadata gate that binary ops enforce).

The index exposes the distinction between *candidates* (bounding-box matches) and
*exact* matches.

!!! warning "Candidates are not matches"
    `idx.candidates(q)` returns everything whose **bounding box** overlaps the
    query's bounding box. Bounding boxes overlap far more often than geometries
    actually intersect — especially for sparse multipolygons and diagonal lines. If
    you treat candidates as answers, you get false positives. `idx.query(q,
    predicate=...)` runs the **exact** predicate on those candidates and returns only
    true matches. Use `query` when exact matches are required.

## Candidates vs query

```python exec="on" source="block" result="text"
import gometry as gm

query = gm.box(0.0, 0.0, 1.0, 1.0)
geoms = gm.GeometryArray([
    gm.box(0.2, 0.2, 0.8, 0.8),                        # 0: fully inside
    gm.Polygon([(0.5, 2.0), (2.0, 2.0), (2.0, 0.5)]),  # 1: bbox overlaps, shape misses
    gm.box(0.9, 0.9, 1.6, 1.6),                         # 2: clips the corner
    gm.box(3.0, 3.0, 4.0, 4.0),                         # 3: far away
])
idx = gm.SpatialIndex(geoms)

cands = idx.candidates(query)                     # bbox prefilter — candidate superset
hits = idx.query(query, predicate="intersects")   # exact refine — matching rows

print("candidates (bbox):", list(cands))   # [0, 1, 2]
print("exact intersects :", list(hits))    # [0, 2]

```

`candidates` returns positional indices into the input array whose envelopes overlap
the query envelope. `query` takes that same candidate set and applies the exact
predicate, so it is always a subset. In this example, geometry **1** is a false
positive because its diagonal triangle's bounding box overlaps the query while the
shape does not.

```python exec="on" html="true"
from _figures import panels
import gometry as gm

query = gm.box(0.0, 0.0, 1.0, 1.0)
geoms = gm.GeometryArray([
    gm.box(0.2, 0.2, 0.8, 0.8),
    gm.Polygon([(0.5, 2.0), (2.0, 2.0), (2.0, 0.5)]),
    gm.box(0.9, 0.9, 1.6, 1.6),
    gm.box(3.0, 3.0, 4.0, 4.0),
])
idx = gm.SpatialIndex(geoms)

print(panels([
    (label, [query.exterior, *(geoms[i] for i in picks)])
    for label, picks in [
        ("candidates", idx.candidates(query)),
        ("query intersects", idx.query(query, predicate="intersects")),
    ]
]))

```

`candidates` supplies rows for caller-owned refinement or a coarse spatial key.
`query` performs the exact predicate refinement.

## Predicates

[`query`][gometry.SpatialIndex.query] accepts any of the standard spatial predicates via
`predicate=`. The default is `"intersects"`.

The operand order is always `predicate(query_geom, indexed_row)`. For example,
`SpatialIndex(polygons).query(point, predicate="within")` finds polygons where
`gm.within(point, polygon)` is true; `predicate="contains"` would ask whether
the point contains each polygon.

```python exec="on" source="block" result="text"
import gometry as gm

pts = gm.points([21.0, 30.0, 21.5], [52.0, 52.0, 52.1], crs=4326)
idx = gm.SpatialIndex(pts)
box = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)

for pred in ("intersects", "contains", "contains_properly", "covers"):
    print(f"{pred:>17}:", list(idx.query(box, predicate=pred)))

```

The full vocabulary is the same as the [top-level predicates](predicates.md)
— `intersects` (the default), `contains`, `contains_properly`, `covers`,
`within`, `covered_by`, `equals`, `touches`, `crosses`, `overlaps`, plus the
metric `dwithin`. Only `disjoint` is rejected: it matches everything far
away, which no envelope index can accelerate.

For distance predicates, pass `distance=` and `unit=`:

```python exec="on" source="block" result="text"
import gometry as gm

pts = gm.points([21.0, 21.5, 30.0], [52.0, 52.1, 52.0], crs=4326)
idx = gm.SpatialIndex(pts)
probe = gm.Point(21.0, 52.0, crs=4326)

# Within 1 degree (planar coordinate units) of the probe point.
print("within 1 deg:", list(idx.query(probe, predicate="dwithin", distance=1.0, unit="planar")))

```

Distance `unit=` follows the CRS metric rules in [CRS, units & measurement](crs.md).
The `unit='meters'` override forces SI, and `unit='planar'` uses raw coordinate math.

## Nearest neighbors

[`idx.nearest`][gometry.SpatialIndex.nearest] finds the `k` closest indexed geometries to a query
geometry and returns their positional indices (nearest first). Distance uses the
same `unit=` rule as `query` / free `dwithin` (CRS-natural by default).

```python exec="on" source="block" result="text"
import gometry as gm

stations = gm.points(
    [21.0, 21.05, 22.0, 19.0],
    [52.0, 52.02, 53.0, 51.0],
    crs=4326,
)
idx = gm.SpatialIndex(stations)
me = gm.Point(21.01, 52.0, crs=4326)

ids = idx.nearest(me, k=2, unit="meters")
print("nearest 2 indices (closest first):", list(ids))

```

Geographic nearest queries use a sound lower bound on geodesic distance for
candidate pruning before exact distance evaluation. The returned ordering is
exact; the number of evaluations depends on the data and query.

- `max_distance=` caps the search radius.
- `return_distance=True` returns `(indices, distances)` for a scalar query or
  `(matches, distances)` for an array query; scalar `indices` is a read-only
  `int64` NumPy array, array `matches` is CSR [`Groups`][gometry.Groups], and `distances` is a
  read-only `float64` NumPy array aligned with the ids.
- `exclusive=True` skips candidates structurally equal to the query and returns the
  nearest other features:

```python exec="on" source="block" result="text"
import gometry as gm

stations = gm.points([21.0, 21.05, 22.0], [52.0, 52.02, 53.0], crs=4326)
idx = gm.SpatialIndex(stations)

for i, station in enumerate(list(stations)):
    nearest_other = idx.nearest(station, exclusive=True)[0]
    print(f"station {i} -> nearest other station {nearest_other}")

```

The top-level function `gm.nearest(values, geometry, k=..., unit=...)` builds an
index for one-shot queries.

## Explain query plans

[`idx.explain`][gometry.SpatialIndex.explain] returns the steps the query runs:
load, bulk-load, envelope lookup, and exact refine.

```python exec="on" source="block" result="text"
import gometry as gm

pts = gm.points([21.0, 30.0, 21.5], [52.0, 52.0, 52.1], crs=4326)
idx = gm.SpatialIndex(pts)
box = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)

for step in idx.explain(box, predicate="contains"):
    print("-", step)

```

Predicate-refined explanations include the operand order for directional
predicates.

When `explain` reports refinement of nearly every geometry, the envelopes overlap
too much; large sparse multipolygons are a common cause.

## Joins: many-to-many done right

[`gm.join`][gometry.join] returns every matching pair across two collections. It
runs the bbox prefilter and exact refine in Rust and returns a
`(left, right)` pair of read-only `int64` NumPy columns. Zipping the columns
produces row pairs.

```python exec="on" source="block" result="text"
import gometry as gm

points = gm.points([21.0, 30.0, 21.5], [52.0, 52.0, 52.1], crs=4326)
areas = gm.GeometryArray([
    gm.box(20.0, 51.0, 22.0, 53.0, crs=4326),
    gm.box(29.0, 51.0, 31.0, 53.0, crs=4326),
])

left, right = gm.join(points, areas, predicate="within")
print("matched (point, area) pairs:", list(zip(left, right, strict=True)))
for li, ri in zip(left, right, strict=True):
    print(f"  point {li} within area {ri}")

```

`join` refines to exact predicates. Use `distance=`/`unit=` for a
`predicate="dwithin"` join. `SpatialIndex.explain(...)` reports candidate and
refinement cost for an index query.

!!! note "Many-to-many query cost"
    The brute-force `for a in polys: for b in points: if gm.contains(a, b)` pattern is
    O(N·M) and allocates a Python object per test. `gm.join` builds and queries
    an index in Rust, then refines candidates exactly. Its cost is driven by
    bounding-box selectivity and candidate count; worst-case overlap can still
    approach O(N·M). Vectorized predicates reject mismatched non-scalar lengths;
    `join` returns the many-to-many result.

## Insert and remove

Bulk construction is the common path, but the index is **mutable** for dynamic
sets: [`insert`][gometry.SpatialIndex.insert] appends a geometry (same frame as
the index), and [`remove`][gometry.SpatialIndex.remove] tombstones a row by
handle. Inserted rows live in a small overflow tree chained after the bulk
structure; query/nearest lanes see both. Removed rows stay sparse handles so row
identity is preserved for joins — they are not renumbered.

```python exec="on" source="block" result="text"
import gometry as gm

pts = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
idx = gm.SpatialIndex(pts)
print("bulk rows:", len(idx))
handle = idx.insert(gm.Point(21.5, 52.1, crs=4326))
print("after insert:", len(idx), "| handle:", handle)
print("nearest:", list(idx.nearest(gm.Point(21.0, 52.0, crs=4326), k=2, unit="planar")))
idx.remove(handle)
print("after remove handle", handle, "→ nearest still works:",
      list(idx.nearest(gm.Point(21.0, 52.0, crs=4326), k=2, unit="planar")))

```

!!! note "The index is in-memory"
    gometry persists nothing to disk. To carry an index across processes,
    rebuild it from the source geometries (or their stored WKB/EWKB bytes — see
    [Text & binary formats](../ecosystem/text-formats.md)). Coming from
    rtree / Shapely `STRtree` / `geopandas.sjoin`? See
    [Migrating](../migrating/index.md#coming-from-rtree-strtree).

## See also

- [Predicates](predicates.md) — the exact relationships `query` refines with.
- [Discrete grids](grids.md) — cell coverages as another candidate layer.
- [Arrays & performance](arrays.md) — array query → `Groups`, vectorized refine.
- [API: SpatialIndex][gometry.SpatialIndex] ·
  [SpatialIndex.candidates][gometry.SpatialIndex.candidates] ·
  [SpatialIndex.query][gometry.SpatialIndex.query] ·
  [SpatialIndex.nearest][gometry.SpatialIndex.nearest] ·
  [join][gometry.join]
