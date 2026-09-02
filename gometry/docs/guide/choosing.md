---
description: Decision tables for the operations that look alike — which predicate, which cleaner, which acceleration structure, which clipping call.
---

# Choosing the right tool

Each operation has one public name, while some families hold several genuinely
different tools.

## Repeated predicates: prepare, index, or graph components?

| You have | You want | Reach for |
| --- | --- | --- |
| **one** geometry probed by many candidates | the same predicate, many times | [`geom.prepare()`][gometry.Geometry.prepare] — builds the segment index once, every later call reuses it |
| **many** geometries queried by arbitrary probes | candidate retrieval + exact predicate | [`gm.SpatialIndex(values)`][gometry.SpatialIndex] then [`query(..., predicate=...)`][gometry.SpatialIndex.query] |
| two whole collections matched row-to-row | a relational join | [`gm.join`][gometry.join] (inner/left semantics, any predicate) |
| one collection grouped by mutual relationships | connected components under a geometric predicate | build a pair graph with [`gm.join`][gometry.join] / [`SpatialIndex.self_join`][gometry.SpatialIndex.self_join], then run a graph connected-components algorithm (networkx / scipy.sparse); reserve coordinate clustering ([NumPy handoff](../ecosystem/numpy.md#handing-off-to-scipy-and-scikit-learn)) for point features only |
| a cheap pre-group before exact work | bounding-box candidates only | [`SpatialIndex.candidates`][gometry.SpatialIndex.candidates] |

[`prepare()`][gometry.Geometry.prepare] is per-geometry state; [`SpatialIndex(values)`][gometry.SpatialIndex]
is per-collection state (there is no free `gm.index`). For one polygon against one
large array, [`gm.contains(polygon, arr)`][gometry.contains] uses the prepared path.

## Containment: `contains`, `contains_properly`, `covers`, `within`

| Question | Predicate |
| --- | --- |
| "is the candidate inside, where boundary contact still counts?" | [`covers`][gometry.covers] |
| "is the candidate inside, with at least one interior point?" ([OGC](https://www.ogc.org/standard/sfa/) contains) | [`contains`][gometry.contains] |
| "is the candidate inside without touching my boundary at all?" | [`contains_properly`][gometry.contains_properly] |
| the same questions, asked from the candidate's side | [`covered_by`][gometry.covered_by] / [`within`][gometry.within] |

The containment family includes [`contains`][gometry.contains],
[`contains_properly`][gometry.contains_properly], [`covers`][gometry.covers],
and [`within`][gometry.within]. The boundary rule: a point **on** a polygon's
edge is *not* contained — use `covers` for boundary-inclusive membership. See
[the predicate boundary rules](predicates.md#boundary-points-are-not-contained).

## Clipping: `clip_by_rect` vs `intersection`

| Situation | Use |
| --- | --- |
| crop to an axis-aligned window (map tile, viewport, bbox) | [`clip_by_rect`][gometry.Geometry.clip_by_rect] — a rectangular cut that does not build a polygon operand; in a geographic CRS, seam-crossing, polar, or non-canonical inputs use frame-aware topology and rectangle parallels match [`box`][gometry.box] |
| crop to an arbitrary mask geometry | [`intersection`][gometry.intersection] (or the `&` operator) |

There is no separate `clip(mask)`: use [`clip_by_rect`][gometry.Geometry.clip_by_rect] for a
rectangle and [`intersection`][gometry.intersection] for an arbitrary mask.

## Vertex cleaning: seven different jobs

| Job | Operation | What survives |
| --- | --- | --- |
| round coordinates to N decimal places (shrink WKB/WKT, stabilize comparisons) | [`quantize`][gometry.Geometry.quantize] | every vertex, rounded; topology *not* repaired |
| snap vertices onto a fixed coordinate grid | [`snap_to_grid`][gometry.Geometry.snap_to_grid] | grid-aligned vertices; collapsed parts become empty; output may be non-simple |
| snap onto a grid **and** guarantee validity | [`snap_to_grid`][gometry.Geometry.snap_to_grid] with `repair=True` | grid-aligned, valid output (snap → repair → re-snap to a fixpoint); the geometry kind may change |
| drop consecutive duplicate (or near-duplicate) vertices | [`remove_repeated_points`][gometry.Geometry.remove_repeated_points] | the geometry kind, with consecutive runs collapsed |
| reduce vertex count while keeping shape | [`simplify`][gometry.Geometry.simplify] (`method='vw'` [Visvalingam–Whyatt](https://en.wikipedia.org/wiki/Visvalingam%E2%80%93Whyatt_algorithm) by default, or `method='dp'` [Douglas–Peucker](https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm) for a distance band; `preserve_topology=True` keeps polygons valid and simple lines simple) | a vertex subset within tolerance |
| collect the distinct vertices themselves | [`unique_points`][gometry.Geometry.unique_points] | a [`MultiPoint`][gometry.MultiPoint] of first-occurrence distinct vertices |
| fix actual invalidity (self-intersections, bad rings) | [`repair`][gometry.Geometry.repair] | a valid geometry, rebuilt |

[`quantize`][gometry.Geometry.quantize] and [`snap_to_grid`][gometry.Geometry.snap_to_grid] can *create* invalidity on tight geometries —
pass `repair=True` to `snap_to_grid` (or follow `quantize` with
[`repair`][gometry.Geometry.repair]) when downstream code needs validity, and use
[`validate`][gometry.Geometry.validate] to see exactly what broke.

See the [worked simplification comparison](constructive.md#simplify-and-offset)
for how tolerance removes progressively larger zigzags, and the [precision and
repeat-cleaning examples](validation.md#cleaning-quantize-and-remove_repeated_points)
for coordinate cleanup. To align one geometry to another rather than to a fixed
grid, see the [worked snapping example](constructive.md#snap-and-segmentize).

## Alignment between two geometries

| Goal | Operation |
| --- | --- |
| move vertices of one geometry onto a reference within a tolerance | [`snap`][gometry.snap] |
| shortest connecting segment between two geometries | [`shortest_line`][gometry.shortest_line] |
| the two closest points as a pair | [`nearest_points`][gometry.nearest_points] |
| shared linework between two lines | [`shared_paths`][gometry.shared_paths] |

## Polygonal `coverage_*` vs grid cells

DGGS cell covers and polygonal coverages use different representations:

- **DGGS cells** — [`gm.h3_cover`][gometry.h3_cover], [`gm.s2_cover`][gometry.s2_cover], [`gm.geohash_cover`][gometry.geohash_cover], and
  [`gm.tile_cover`][gometry.tile_cover] return typed [`CellArray`][gometry.CellArray] values. Keep the source geometry
  and use the top-level predicates for exact membership.
- **A polygonal coverage** — a [`GeometryArray`][gometry.GeometryArray] of polygons that tile a region
  without gaps or overlaps (parcels, admin boundaries). Operations on these are
  array methods named `coverage_*`.

If you are bucketing geometry into a global grid, you want those `*_cover`
factories. If you are validating or simplifying a parcel fabric, build a
  [`GeometryArray`][gometry.GeometryArray] and call the column-native methods —
[`arr.coverage_is_valid()`][gometry.GeometryArray.coverage_is_valid], [`arr.coverage_simplify(tolerance)`][gometry.GeometryArray.coverage_simplify], and so on. The
free `gm.coverage_*(values)` forms accept arrays and general iterables:

```python exec="on" source="block" result="text"
import gometry as gm

grid = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)])
print("valid:", grid.coverage_is_valid())
print("same as free fn:", gm.coverage_is_valid(list(grid)))
```

### Validate, inspect, then clean

Coverage operations validate their tiling precondition before processing.
[`coverage_invalid_edges`][gometry.coverage_invalid_edges] returns diagnostic edges for a bad parcel fabric, and
[`coverage_clean`][gometry.coverage_clean] applies the selected cleaning policy. Snapping is opt-in: the
default `grid_size=0.0` preserves existing coordinates.

```python exec="on" html="true"
from _figures import panels, with_vertices
import gometry as gm

input_rows = gm.GeometryArray([
    gm.box(0, 0, 2.1, 2),
    gm.box(2, 0, 4, 2),
])
invalid_edges = input_rows.coverage_invalid_edges()
cleaned = input_rows.coverage_clean(overlap_rule="longest_border")

print(panels(
    [
        ("two parcel rows", {'source': input_rows}, "The two input parcels overlap along a narrow strip instead of forming a valid coverage."),
        ("invalid shared edges", {'source': input_rows, 'result': invalid_edges}, "Diagnostic edges mark the mismatched parcel border around the overlap."),
        ("cleaned parcel coverage", {'source': input_rows, 'result': [item for row in cleaned for item in with_vertices(row)]}, "Coverage cleaning aligns the shared border and leaves two non-overlapping parcel rows."),
        ("coverage union", {'result': cleaned.coverage_union()}, "The cleaned parcels dissolve into one continuous polygon with no overlap or gap."),
    ],
    labels={'source': 'input parcels', 'result': 'what the call returns'},
))
```

## See also

- [Geometry](geometry.md) — types and construction.
- [CRS, units & measurement](crs.md) · [Predicates](predicates.md) ·
  [Constructive](constructive.md) — the three operation families.
- [Discrete grids](grids.md) — DGGS coverages vs polygonal `coverage_*`.
- [API index](../api/index.md) — full callable inventory.
