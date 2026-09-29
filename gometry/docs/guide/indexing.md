---
description: gometry spatial indexing — candidates (bbox prefilter) vs query predicate refinement, nearest with units, and joins for many-to-many work.
---

# Spatial indexing & joins

A spatial index makes brute-force geometry practical: instead of testing a query
against every feature, you test it against the few whose bounding boxes could
possibly match. [`gm.SpatialIndex`][gometry.SpatialIndex] builds a bulk-loaded in-memory [R-tree](https://en.wikipedia.org/wiki/R-tree) over geometry
envelopes. The index exposes
[`crs`][gometry.SpatialIndex.crs] and [`epoch`][gometry.SpatialIndex.epoch] for
the shared frame of its rows (the same metadata gate that binary ops enforce).

The index exposes the distinction between *candidates* (bounding-box matches) and
*exact* matches.

An index is mutable, and its integer handles are the live row identities returned
by queries and consumed by `remove` and `__getitem__`. Pickling stores the logical
sparse row table, including tombstones for removed rows, and rebuilds the search
structure on load. A round trip therefore preserves live handles and prior
mutations rather than replaying the original source rows.

!!! warning "Candidates are not matches"
    [`idx.candidates(q)`][gometry.SpatialIndex.candidates] returns everything whose **bounding box** overlaps the
    query's bounding box. Bounding boxes overlap far more often than geometries
    actually intersect — especially for sparse multipolygons and diagonal lines.
    A cover or index candidate narrows the search; it is not an exact geometry
    answer. Retain the source geometry and use an exact predicate to refine those
    candidates into exact matches.
    [`idx.query(q, predicate=...)`][gometry.SpatialIndex.query] performs that
    exact predicate refinement and returns only true matches. Both methods
    return source-row handles in ascending order; removing rows does not
    renumber the remaining handles.

## Candidates vs query

```python exec="on" source="block" result="text" session="index-candidates"
import gometry as gm

query = gm.box(0.0, 0.0, 1.0, 1.0)
geoms = gm.GeometryArray([
    gm.box(0.2, 0.2, 0.8, 0.8),
    gm.Polygon([(0.5, 2.0), (2.0, 2.0), (2.0, 0.5)]),
    gm.box(0.9, 0.9, 1.6, 1.6),
    gm.box(3.0, 3.0, 4.0, 4.0),
])
idx = gm.SpatialIndex(geoms)

cands = idx.candidates(query)                     # bbox prefilter — candidate superset
hits = idx.query(query, predicate="intersects")   # exact refine — matching rows

assert cands.tolist() == [0, 1, 2]
assert hits.tolist() == [0, 2]

print("candidates (bbox):", cands.tolist())
print("exact intersects:", hits.tolist())

```

`candidates` returns positional indices into the input array whose envelopes overlap
the query envelope. `query` takes that same candidate set and applies the exact
predicate, so it is always a subset. Both return handles in ascending source-row
order. In this example, geometry **1** is a false
positive because its diagonal triangle's bounding box overlaps the query while the
shape does not.

```python exec="on" html="true" session="index-candidates"
from _figures import figure

candidate_rows, hit_rows = cands.tolist(), hits.tolist()
print(figure(
    {
        'query': query,
        'candidate': [geoms[row].envelope() for row in candidate_rows],
        'result': [geoms[row] for row in hit_rows],
        'excluded': [geoms[row] for row in candidate_rows if row not in hit_rows],
        'source': [geoms[row] for row in range(len(geoms)) if row not in candidate_rows],
    },
    'The prefilter admits rows 0, 1, 2; refinement keeps rows 0 and 2.',
    alt=(
        'The envelopes of rows 0, 1, and 2 overlap the query envelope, so all '
        'three become candidates. Exact intersects keeps rows 0 and 2 and rejects '
        'the diagonal triangle in row 1; row 3 never becomes a candidate.'
    ),
    # Both rejected rows are 'not a match', for different reasons; naming the
    # reason is the whole point of drawing them apart.
    labels={
        'candidate': 'candidate envelopes',
        'result': 'exact matches',
        'excluded': 'bbox false positive',
        'source': 'never a candidate',
    },
))

```

`candidates` supplies rows for caller-owned refinement or a coarse spatial key.
`query` performs the exact predicate refinement.
A positive-infinite candidate expansion admits every live non-empty indexed row
for a query with bounds.

### Periodic geographic bounds

For a geographic index, the candidate envelope uses the CRS-native longitude
period and pole. Exact refinement then uses the same frame-aware topology as a
direct predicate. EPSG:4807 is a public grad-unit example: one turn is 400 grads,
and its north pole is 100 grads.

```python exec="on" source="block" result="text"
import math

import gometry as gm

pole = 100.0
# Exercise the tiny admitted floating-point sliver at the north pole.
for _ in range(7):
    pole = math.nextafter(pole, math.inf)

source = gm.Point(0.0, pole, crs=4807)
alias = gm.Point(200.0, pole, crs=4807)
index = gm.SpatialIndex([source])

print("periodic candidates:", index.candidates(alias).tolist())
print("exact intersects:", index.query(alias, predicate="intersects").tolist())
assert index.candidates(alias).tolist() == [0]
assert index.query(alias, predicate="intersects").tolist() == [0]

```

The bounds gate reaches the pole's alternate longitude spelling, and the exact
predicate confirms that both spellings identify the same geographic point.

## Predicates

`query` accepts any of the standard spatial predicates via
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
    print(f"{pred:>17}:", idx.query(box, predicate=pred).tolist())

```

The full vocabulary is the same as the [top-level predicates](predicates.md)
— [`intersects`][gometry.intersects] (the default), [`contains`][gometry.contains], [`contains_properly`][gometry.contains_properly], [`covers`][gometry.covers],
[`within`][gometry.within], [`covered_by`][gometry.covered_by], [`equals`][gometry.equals], [`touches`][gometry.touches], [`crosses`][gometry.crosses], [`overlaps`][gometry.overlaps], plus the
metric [`dwithin`][gometry.dwithin]. Only [`disjoint`][gometry.disjoint] is rejected: it matches everything far
away, which no envelope index can accelerate.

For distance predicates, pass `distance=` and `unit=`:

```python exec="on" source="block" result="text"
import gometry as gm

pts = gm.points([21.0, 21.5, 30.0], [52.0, 52.1, 52.0], crs=4326)
idx = gm.SpatialIndex(pts)
probe = gm.Point(21.0, 52.0, crs=4326)

print("within 1 deg:", idx.query(probe, predicate="dwithin", distance=1.0, unit="planar").tolist())

```

Distance `unit=` follows the CRS metric rules in [CRS, units & measurement](crs.md).
The `unit='meters'` override forces SI, and `unit='planar'` uses raw coordinate math.
`dwithin` accepts a positive-infinite threshold and matches only admitted
non-empty geometries. Array results keep one boolean per input pair; missing rows
and present empty geometries remain in their positions as `False`.

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
print("nearest 2 indices (closest first):", ids.tolist())

```

Geographic nearest queries use a sound lower bound on geodesic distance for
candidate pruning before refined distance evaluation. The returned ordering
uses those refined geodesic distances; the number of evaluations depends on
the data and query.

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
    print(f"station {i} → nearest other station {nearest_other}")

```

The top-level function [`gm.nearest(values, geometry, k=..., unit=...)`][gometry.nearest] builds an
index for one-shot queries.

## Measure candidate selectivity

Compare the number of bounding-box candidates with the exact result for the
same query and predicate:

```python exec="on" source="block" result="text"
import gometry as gm

q = gm.box(0.0, 0.0, 1.0, 1.0)
geoms = gm.GeometryArray([
    gm.box(0.2, 0.2, 0.8, 0.8),
    gm.Polygon([(0.5, 2.0), (2.0, 2.0), (2.0, 0.5)]),
    gm.box(0.9, 0.9, 1.6, 1.6),
    gm.box(3.0, 3.0, 4.0, 4.0),
])
idx = gm.SpatialIndex(geoms)

candidate_count = len(idx.candidates(q))
refined_count = len(idx.query(q, predicate="intersects"))
print(f"candidates: {candidate_count} → refined intersects: {refined_count}")

```

A large candidate count relative to the refined count means the envelopes are
not selective; large sparse multipolygons are a common cause.

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
print("matched (point, area) pairs:", list(zip(left.tolist(), right.tolist(), strict=True)))
for li, ri in zip(left, right, strict=True):
    print(f"point {li} within area {ri}")

```

`join` refines to exact predicates. Use `distance=`/`unit=` for a
`predicate="dwithin"` join. For a direct index query, compare
`len(idx.candidates(q))` with `len(idx.query(q, predicate=...))` to measure the
candidate superset and its exact refinement.

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
print("nearest:", idx.nearest(gm.Point(21.0, 52.0, crs=4326), k=2, unit="planar").tolist())
idx.remove(handle)
print("after remove handle", handle, "→ nearest still works:",
      idx.nearest(gm.Point(21.0, 52.0, crs=4326), k=2, unit="planar").tolist())

```

!!! note "The index is in-memory"
    gometry persists nothing to disk. To carry an index across processes,
    rebuild it from the source geometries (or their stored WKB/EWKB bytes — see
    [Text & binary formats](../ecosystem/text-formats.md)). Coming from
    rtree / Shapely `STRtree` / `geopandas.sjoin`? See
    [Migrating](../migrating/index.md#coming-from-rtree-strtree).

## See also

- [Predicates](predicates.md) — the exact relationships [`query`][gometry.SpatialIndex.query] refines with.
- [Discrete grids](grids.md) — cell coverages as another candidate layer.
- [Arrays & performance](arrays.md) — array query → [`Groups`][gometry.Groups], vectorized refine.
- [API: SpatialIndex][gometry.SpatialIndex] ·
  [SpatialIndex.candidates][gometry.SpatialIndex.candidates] ·
  [SpatialIndex.query][gometry.SpatialIndex.query] ·
  [SpatialIndex.nearest][gometry.SpatialIndex.nearest] ·
  [join][gometry.join]
