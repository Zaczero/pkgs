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

The defining feature of gometry's index is that it refuses to hide the most common
GIS bug: confusing *candidates* (bounding-box matches) with *exact* matches.

!!! warning "Candidates are not matches"
    `idx.candidates(q)` returns everything whose **bounding box** overlaps the
    query's bounding box. Bounding boxes overlap far more often than geometries
    actually intersect — especially for sparse multipolygons and diagonal lines. If
    you treat candidates as answers, you get false positives. `idx.query(q,
    predicate=...)` runs the **exact** predicate on those candidates and returns only
    true matches. When in doubt, use `query`.

## Candidates vs query

This is the centerpiece. Build an index, then compare the two query styles against
the same geometry.

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

cands = idx.candidates(query)                     # bbox prefilter — a SUPERSET
hits = idx.query(query, predicate="intersects")   # exact refine — the TRUTH

print("candidates (bbox):", list(cands))   # [0, 1, 2]
print("exact intersects :", list(hits))    # [0, 2]

```

`candidates` returns positional indices into the input array whose envelopes overlap
the query envelope. `query` takes that same candidate set and applies the exact
predicate, so it is always a subset. The gap between the two lists *is* the false
positives you would have shipped by trusting the prefilter — here geometry **1**, a
diagonal triangle whose bounding box overlaps the query but whose shape does not.

The candidate set (left) carries that false positive; the exact `intersects` set
(right) drops it. The query box is drawn as an outline:

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

!!! note "Why expose candidates at all?"
    Because sometimes the prefilter *is* what you want — e.g. you have your own
    refine step, or you only need a coarse "what might be nearby" set. gometry exposes
    both so the choice is explicit — a prefilter that returns bbox candidates should
    never be mistaken for exact matches.

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

!!! note "Distance `unit=` follows the same CRS-natural rule as metrics"
    Omit `unit=` (or pass `None`) for the index's CRS-natural metric: geodesic
    meters on a geographic CRS, **native linear units** on a projected one,
    coordinate units when CRS-free. Pass `unit='meters'` to force SI (raises
    without a CRS) or `unit='planar'` for raw coordinate Cartesian (on lon/lat
    that is *degrees* — a different ground distance at every latitude). Choosing
    the wrong override is the distance-query analogue of the degrees-vs-meters
    buffer trap; see [Arrays & performance](arrays.md) and
    [CRS, units & measurement](crs.md).

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

Geographic nearest queries are *accelerated*, not scanned: for point data the
index prunes with a sound lower bound on the geodesic distance (exact results,
orders of magnitude fewer [Karney](https://geographiclib.sourceforge.io/) evaluations).

- `max_distance=` caps the search radius.
- `return_distance=True` returns `(indices, distances)` for a scalar query or
  `(matches, distances)` for an array query; scalar `indices` is a read-only
  `int64` NumPy array, array `matches` is CSR [`Groups`][gometry.Groups], and `distances` is a
  read-only `float64` NumPy array aligned with the ids.
- `exclusive=True` skips candidates structurally equal to the query — "the nearest
  *other* feature", the self-join idiom:

```python exec="on" source="block" result="text"
import gometry as gm

stations = gm.points([21.0, 21.05, 22.0], [52.0, 52.02, 53.0], crs=4326)
idx = gm.SpatialIndex(stations)

for i, station in enumerate(list(stations)):
    nearest_other = idx.nearest(station, exclusive=True)[0]
    print(f"station {i} -> nearest other station {nearest_other}")

```

The equivalent top-level function is
`gm.nearest(values, geometry, k=..., unit=...)`, which builds the
index for you for one-shot queries.

## Explain: see the plan

[`idx.explain`][gometry.SpatialIndex.explain] returns the steps the query runs — load,
bulk-load, envelope lookup, exact refine — so you can confirm the prefilter is doing
its job and the refine stays selective.

```python exec="on" source="block" result="text"
import gometry as gm

pts = gm.points([21.0, 30.0, 21.5], [52.0, 52.0, 52.1], crs=4326)
idx = gm.SpatialIndex(pts)
box = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)

for step in idx.explain(box, predicate="contains"):
    print("-", step)

```

Predicate-refined explanations include the same operand rule, so directional
predicates are visible before you inspect the result rows.

If `explain` shows the index refining nearly every geometry, your envelopes overlap
too much (common with large, sparse multipolygons) — a signal to split features or
add a grid prefilter (see [Grids](grids.md)).

## Joins: many-to-many done right

When you need *every* matching pair across two collections — the spatial equivalent
of a SQL join — do **not** nest index queries in a Python loop. Use [`gm.join`][gometry.join],
which runs the bbox prefilter and the exact refine in Rust and returns a
`(left, right)` pair of read-only `int64` NumPy columns. Zip the columns when
you want row pairs.

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

`join` refines to exact predicates — there is no "candidates-only" join that
silently returns false positives. Use `distance=`/`unit=` for a
`predicate="dwithin"` join. When you need to inspect candidate and refinement
cost, build a `SpatialIndex` and call its dedicated `explain(...)` method. See
[Build a spatial join end-to-end](#build-a-spatial-join-end-to-end) below for a
full walkthrough.

!!! danger "Don't build Cartesian products by hand"
    The brute-force `for a in polys: for b in points: if gm.contains(a, b)` pattern is
    O(N·M) and allocates a Python object per test. `gm.join` builds and queries
    an index in Rust, then refines candidates exactly. Its cost is driven by
    bounding-box selectivity and candidate count; worst-case overlap can still
    approach O(N·M). For two non-scalar arrays the vectorized predicates
    *intentionally* refuse mismatched lengths (see [Arrays](arrays.md)) to
    push you toward `join`.

## Build a spatial join end-to-end

You have two sets of geometry and want every pair that satisfies a spatial
predicate — points within areas, areas intersecting a region, and so on. Use
[`gm.join`][gometry.join] for a one-shot match between two sets; build a reusable
[`gm.SpatialIndex`][gometry.SpatialIndex] when you will query the same set many
times.

### One-shot join

[`gm.join`][gometry.join] returns two parallel `int64` ndarrays
`(left_rows, right_rows)`, using a spatial index internally:

```python exec="on" source="block" result="text"
import gometry as gm

points = gm.points([2.35, 2.30, 2.50], [48.86, 48.85, 48.80], crs=4326)
areas = gm.GeometryArray([
    gm.box(2.32, 48.84, 2.40, 48.88, crs=4326),
    gm.box(2.45, 48.78, 2.55, 48.83, crs=4326),
])

left, right = gm.join(points, areas, predicate='within')
print('(point_row, area_row) pairs:', list(zip(left.tolist(), right.tolist(), strict=True)))

```

```python exec="on" html="true"
from _figures import figure
import gometry as gm

points = gm.points([2.35, 2.30, 2.50], [48.86, 48.85, 48.80], crs=4326)
areas = gm.GeometryArray([
    gm.box(2.32, 48.84, 2.40, 48.88, crs=4326),
    gm.box(2.45, 48.78, 2.55, 48.83, crs=4326),
])
print(figure([*list(areas), *list(points)], 'points and candidate areas for the spatial join'))

```

### Index for repeated queries

For many queries against the same set, build the index once with
[`gm.SpatialIndex`][gometry.SpatialIndex] and reuse it; `query(...)` does the exact predicate
refinement and `explain(...)` prints the plan:

```python exec="on" source="block" result="text"
import gometry as gm

points = gm.points([2.35, 2.30, 2.50], [48.86, 48.85, 48.80], crs=4326)
region = gm.box(2.32, 48.84, 2.40, 48.88, crs=4326)

idx = gm.SpatialIndex(points)
print('rows within region:', list(idx.query(region, predicate='contains')))

```

For a reused polygon index and point queries, invert the directional predicate:
`gm.SpatialIndex(areas).query(point, predicate='within')` tests
`gm.within(point, area)` for each indexed row.

### Insert and remove

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
