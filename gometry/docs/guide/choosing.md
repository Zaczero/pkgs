---
description: Decision tables for the operations that look alike — which predicate, which cleaner, which acceleration structure, which clipping call.
---

# Choosing the right tool

gometry keeps one canonical name per operation, but some families hold several
genuinely different tools. These tables answer "which one?" in one
glance; each entry links to the full reference.

## Repeated predicates: prepare, index, or graph components?

| You have | You want | Reach for |
| --- | --- | --- |
| **one** geometry probed by many candidates | the same predicate, many times | [`geom.prepare()`][gometry.Geometry.prepare] — builds the segment index once, every later call reuses it |
| **many** geometries queried by arbitrary probes | candidate retrieval + exact predicate | [`gm.SpatialIndex(values)`][gometry.SpatialIndex] then [`query(..., predicate=...)`][gometry.SpatialIndex.query] |
| two whole collections matched row-to-row | a relational join | [`gm.join`][gometry.join] (inner/left semantics, any predicate) |
| one collection grouped by mutual relationships | connected components under a geometric predicate | build a pair graph with [`gm.join`][gometry.join] / [`SpatialIndex.self_join`][gometry.SpatialIndex.self_join], then run a graph connected-components algorithm (networkx / scipy.sparse); reserve coordinate clustering ([NumPy handoff](../ecosystem/numpy.md#handing-off-to-scipy-and-scikit-learn)) for point features only |
| a cheap pre-group before exact work | bounding-box candidates only | [`SpatialIndex.candidates`][gometry.SpatialIndex.candidates] |

`prepare()` is per-geometry state; [`SpatialIndex(values)`][gometry.SpatialIndex]
is per-collection state (there is no free `gm.index`). When both fit (one polygon
vs. one big array), the vectorized predicate (`gm.contains(polygon, arr)`) already
uses the prepared path internally — measure before adding either by hand.

## Containment: `contains`, `contains_properly`, `covers`, `within`

| Question | Predicate |
| --- | --- |
| "is the candidate inside, where boundary contact still counts?" | [`covers`][gometry.covers] |
| "is the candidate inside, with at least one interior point?" ([OGC](https://www.ogc.org/standard/sfa/) contains) | [`contains`][gometry.contains] |
| "is the candidate inside without touching my boundary at all?" | [`contains_properly`][gometry.contains_properly] |
| the same questions, asked from the candidate's side | [`covered_by`][gometry.covered_by] / [`within`][gometry.within] |

The classic trap: a point **on** a polygon's edge is *not* contained —
use `covers` for boundary-inclusive membership. See
[the predicate gotchas](predicates.md#gotcha-1-boundary-points-are-not-contained).

## Clipping: `clip_by_rect` vs `intersection`

| Situation | Use |
| --- | --- |
| crop to an axis-aligned window (map tile, viewport, bbox) | [`clip_by_rect`][gometry.Geometry.clip_by_rect] — a fast rectangular cut that does not build a polygon operand |
| crop to an arbitrary mask geometry | [`intersection`][gometry.intersection] (or the `&` operator) |

There is no separate `clip(mask)`: it would be `intersection` by another name.

## Vertex cleaning: seven different jobs

| Job | Operation | What survives |
| --- | --- | --- |
| round coordinates to N decimal places (shrink WKB/WKT, stabilize comparisons) | [`quantize`][gometry.Geometry.quantize] | every vertex, rounded; topology *not* repaired |
| snap vertices onto a fixed coordinate grid | [`snap_to_grid`][gometry.Geometry.snap_to_grid] | grid-aligned vertices; collapsed parts become empty; output may be non-simple |
| snap onto a grid **and** guarantee validity | [`snap_to_grid`][gometry.Geometry.snap_to_grid] with `repair=True` | grid-aligned, valid output (snap → repair → re-snap to a fixpoint); the geometry kind may change |
| drop consecutive duplicate (or near-duplicate) vertices | [`remove_repeated_points`][gometry.Geometry.remove_repeated_points] | the geometry kind, with consecutive runs collapsed |
| reduce vertex count while keeping shape | [`simplify`][gometry.Geometry.simplify] (`method='vw'` [Visvalingam–Whyatt](https://en.wikipedia.org/wiki/Visvalingam%E2%80%93Whyatt_algorithm) by default for the smoothest cartographic look, or `method='dp'` [Douglas–Peucker](https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm) for a distance band; `preserve_topology=True` keeps polygons valid and simple lines simple) | a vertex subset within tolerance |
| collect the distinct vertices themselves | [`unique_points`][gometry.Geometry.unique_points] | a `MultiPoint` of first-occurrence distinct vertices |
| fix actual invalidity (self-intersections, bad rings) | [`repair`][gometry.Geometry.repair] | a valid geometry, rebuilt |

`quantize` and `snap_to_grid` can *create* invalidity on tight geometries —
pass `repair=True` to `snap_to_grid` (or follow `quantize` with
[`repair`][gometry.Geometry.repair]) when downstream code needs validity, and use
[`validate`][gometry.Geometry.validate] to see exactly what broke.

Vertices make the difference between these cleaners visible — dots show what each
operation keeps:

```python exec="on" html="true"
from _figures import before_after, with_vertices
import gometry as gm

zigzag = gm.LineString([(0, 0), (0.5, 2), (1, 0), (1.5, 2), (2, 0)])
simplified = zigzag.simplify(tolerance=0.5)
print(before_after(with_vertices(zigzag), with_vertices(simplified),
                   before_caption="input", after_caption="simplify(tolerance=0.5)"))

```

```python exec="on" html="true"
from _figures import before_after, with_vertices
import gometry as gm

off_grid = gm.LineString([(0.12, 0.34), (1.67, 0.89), (2.45, 1.23)])
snapped = off_grid.snap_to_grid(size=0.5)
print(before_after(with_vertices(off_grid), with_vertices(snapped),
                   before_caption="input", after_caption="snap_to_grid(size=0.5)"))

```

## Alignment between two geometries

| Goal | Operation |
| --- | --- |
| move vertices of one geometry onto a reference within a tolerance | [`snap`][gometry.snap] |
| shortest connecting segment between two geometries | [`shortest_line`][gometry.shortest_line] |
| the two closest points as a pair | [`nearest_points`][gometry.nearest_points] |
| shared linework between two lines | [`shared_paths`][gometry.shared_paths] |

## Polygonal `coverage_*` vs grid cells

Two different concepts share the word:

- **DGGS cells** — `gm.h3_cover`, `gm.s2_cover`, `gm.geohash_cover`, and
  `gm.tile_cover` return typed `CellArray` values. Keep the source geometry
  and use the top-level predicates for exact membership.
- **A polygonal coverage** — a `GeometryArray` of polygons that tile a region
  without gaps or overlaps (parcels, admin boundaries). Operations on these are
  array methods named `coverage_*`.

If you are bucketing geometry into a global grid, you want those `*_cover`
factories. If you are validating or simplifying a parcel fabric, build a
[`GeometryArray`][gometry.GeometryArray] and call the column-native methods —
`arr.coverage_is_valid()`, `arr.coverage_simplify(tolerance)`, and so on. The
free `gm.coverage_*(values)` forms accept arrays and general iterables:

```python exec="on" source="block" result="text"
import gometry as gm

grid = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)])
print("valid:", grid.coverage_is_valid())
print("same as free fn:", gm.coverage_is_valid(list(grid)))
```

### Validate, inspect, then clean

Coverage operations validate their tiling precondition themselves; callers do
not need a fragile check-then-do sequence. Use the diagnostic edge layer when
you need to understand a bad parcel fabric, and `coverage_clean` when the
intended policy is known. Snapping is opt-in: the default `grid_size=0.0`
preserves existing coordinates.

```python exec="on" html="true"
from _figures import panels, with_vertices
import gometry as gm

input_rows = gm.GeometryArray([
    gm.box(0, 0, 2.1, 2),
    gm.box(2, 0, 4, 2),
])
invalid_edges = input_rows.coverage_invalid_edges()
cleaned = input_rows.coverage_clean(overlap_rule="longest_border")

print(panels([
    ("input polygons", list(input_rows)),
    ("invalid shared edges", [*input_rows, *invalid_edges]),
    ("cleaned coverage", [item for row in cleaned for item in with_vertices(row)]),
    ("coverage union", cleaned.coverage_union()),
]))
```

The four panels deliberately separate source data, diagnostics, the
policy-driven repair, and the final aggregate. This is also the recommended
debugging order for real parcel and administrative-boundary data.

## See also

- [Geometry](geometry.md) — types and construction.
- [CRS, units & measurement](crs.md) · [Predicates](predicates.md) ·
  [Constructive](constructive.md) — the three operation families.
- [Discrete grids](grids.md) — DGGS coverages vs polygonal `coverage_*`.
- [API index](../api/index.md) — full callable inventory.
