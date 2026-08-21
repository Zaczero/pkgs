---
description: Constructive geometry in gometry — buffers, overlays, simplify, offset curves, snap, segmentize, hulls, centroids, triangulation, and Voronoi.
---

# Constructive operations

Constructive operations take geometries and produce **new** geometries:
buffers, set-theoretic overlays, simplifications, hulls, and tessellations.
All inputs are immutable, so every operation returns a fresh value.

Overlays and shape operations use stored XY coordinates after strict frame
matching; reprojecting can change vertices, seams, and topology. [`buffer`][gometry.Geometry.buffer] and
[`offset_curve`][gometry.Geometry.offset_curve] take distances in the CRS-natural units described in [CRS, units
& measurement](crs.md). Geographic buffers use a local projection and are not
exact ellipsoidal offsets.

## Buffer

[`buffer`][gometry.Geometry.buffer] grows (positive distance) or shrinks (negative
distance) a geometry by a fixed amount, producing an areal
[`Polygon`][gometry.Polygon] or [`MultiPolygon`][gometry.MultiPolygon] result.

```python exec="on" source="block" result="text"
import gometry as gm

line = gm.LineString([(0, 0), (5, 0), (5, 5)])
corridor = line.buffer(1.0)
print(corridor.geometry_type, "area:", round(corridor.area, 3))

```

```python exec="on" html="true"
from _figures import before_after
import gometry as gm
line = gm.LineString([(0, 0), (5, 0), (5, 5)])
print(before_after(line, line.buffer(1.0), after_caption="buffer(1.0)"))

```

### Controlling the shape of the buffer

| Parameter | Default | Effect |
|-----------|---------|--------|
| `quadrant_segments` | `8` | segments per quarter-circle; higher = rounder, more vertices |
| `cap_style` | `'round'` | end-cap of open lines: `'round'`, `'flat'`, `'square'` |
| `join_style` | `'round'` | corner style: `'round'`, `'miter'`, `'bevel'` |
| `miter_limit` | `5.0` | with `join_style='miter'`, clip spikes when the miter would exceed `limit × distance` |
| `side` | `'both'` | stroke side for linework: `'both'`, `'left'`, or `'right'` |

A round cap adds a half-disc at each line end; a flat cap stops dead at the
endpoint.

```python exec="on" source="block" result="text"
import gometry as gm

line = gm.LineString([(0, 0), (5, 0)])
print("round cap:", round(line.buffer(1.0, cap_style="round").area, 3))
print("flat cap: ", round(line.buffer(1.0, cap_style="flat", join_style="miter").area, 3))

```

```python exec="on" html="true"
from _figures import panels
import gometry as gm

line = gm.LineString([(0, 0), (5, 0)])
print(panels([
    (caption, [line.buffer(1.0, **kwargs), line])
    for caption, kwargs in [
        ('cap_style="round"', {"cap_style": "round"}),
        ('cap_style="flat"', {"cap_style": "flat", "join_style": "miter"}),
    ]
]))

```

!!! warning "Geographic buffers use a local projection approximation"
    On a geographic CRS, `buffer` and `offset_curve` are **not** exact
    ellipsoidal offsets. gometry picks one bounded local frame with
    [`estimate_local_crs()`][gometry.Geometry.estimate_local_crs], reprojects, runs the
    planar constructive kernel at the requested meter distance, then
    reprojects back to the source CRS. It is intended for city-scale features
    and modest radii; distortion grows when the
    geometry spans many UTM zones, sits far from the anchor, or the buffer
    radius is large relative to the feature. For continent-scale work or a
    strict accuracy budget, reproject to an appropriate projected CRS first
    (or tile the operation) instead of relying on the automatic local frame. If a
    specific accuracy budget matters, choose and validate the projected frame
    explicitly.

## Overlay (set operations)

The four boolean overlays combine two geometries. Python operator spellings are
available for the named methods:

| Operation | Operator | Result |
|-----------|----------|--------|
| [`intersection`][gometry.intersection] | `a & b` | the shared region (`left ∩ right`) |
| [`union`][gometry.union] | `a \| b` | everything in either (`left ∪ right`) |
| [`difference`][gometry.difference] | `a - b` | `left` with `right` cut out (`left − right`) |
| [`symmetric_difference`][gometry.symmetric_difference] | `a ^ b` | in exactly one of the two |

```python exec="on" source="block" result="text"
import gometry as gm

a = gm.box(0, 0, 2, 2)
b = gm.box(1, 1, 3, 3)

print("intersection:", (a & b))
print("union:       ", (a | b))
print("difference:  ", (a - b))

```

The operators are the named **free functions** with the same behavior: scalars
and arrays broadcast identically, CRS conflicts raise the same error, and Z/M
is restored wherever the result vertex can be sourced from an input segment.

`intersection` keeps every dimension of contact: two polygons that merely
share a border intersect in that border (a [`LineString`][gometry.LineString]), and a corner-only
touch yields the corner [`Point`][gometry.Point] — so a non-empty [`intersects(a, b)`][gometry.intersects] always has
a non-empty `intersection(a, b)`.

```python exec="on" html="true"
from _figures import panels
import gometry as gm

a, b = gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)
frame = [a.exterior, b.exterior]
print(panels([
    ("a & b  (intersection)", frame + [a & b]),
    ("a | b  (union)", frame + [a | b]),
    ("a - b  (difference)", frame + [a - b]),
    ("a ^ b  (symmetric_difference)", frame + [a ^ b]),
]))

```

!!! note "Overlay guards your CRS"
    Overlay and [`union_all`][gometry.union_all] reject inputs with **conflicting** CRS metadata
    rather than silently dropping it or stamping a wrong CRS on the result.
    Reproject both operands to a common CRS first — see the [CRS, units & measurement](crs.md).

### Dissolving many geometries: `union_all`

Unioning a list pairwise grows intermediate results and misses cascaded
optimization (and can accumulate floating-point fuzz).
[`union_all`][gometry.union_all] dissolves an entire sequence in one
cascaded pass:

```python exec="on" source="block" result="text"
import gometry as gm

tiles = [gm.box(i, 0, i + 1, 1) for i in range(5)]
merged = gm.union_all(tiles)
print(merged.to_wkt())        # one 5x1 rectangle, shared edges dissolved

```

For a [`GeometryArray`][gometry.GeometryArray], [`array.union_all()`][gometry.GeometryArray.union_all] performs whole-column aggregation.
Grouped aggregation is a separate operation: [`array.dissolve(by=groups)`][gometry.GeometryArray.dissolve] returns
one geometry per first-seen group with the corresponding group keys. `dissolve`
requires `by`; whole-column aggregation uses `union_all()`.

!!! note "`union_all([])` raises"
    An empty sequence has no CRS, no dimensionality, and no natural identity
    element, so `union_all([])` raises instead of selecting default metadata.
    Filter the sequence or branch on emptiness before aggregation.

## Simplify and offset

[`simplify`][gometry.Geometry.simplify] drops vertices within a `tolerance`, keeping the
shape recognizable while shrinking it.

```python exec="on" source="block" result="text"
import gometry as gm

wiggly = gm.LineString([(0, 0), (1, 0.01), (2, -0.01), (3, 0.01), (4, 0)])
print(wiggly.simplify(0.5))   # collapses to the straight run

```

```python exec="on" html="true"
from _figures import panels, with_vertices
import gometry as gm
import numpy as np

xs = np.linspace(0, 10, 41)
ys = 0.8 * np.sin(xs * 1.2) + 0.18 * np.sin(xs * 7)
noisy = gm.LineString(list(zip(xs.tolist(), ys.tolist())))
panes = []
for tolerance in (0.0, 0.05, 0.2, 0.6):
    simple = noisy.simplify(tolerance)
    panes.append((f"tolerance={tolerance} ({len(simple.coords)} vertices)",
                  with_vertices(simple)))
print(panels(panes))

```

[`simplify`][gometry.Geometry.simplify] takes a `method`: the default `'vw'`
([Visvalingam–Whyatt](https://en.wikipedia.org/wiki/Visvalingam%E2%80%93Whyatt_algorithm))
drops the *least visually significant* vertices first (the ones spanning the
smallest effective triangle), while `'dp'`
([Douglas–Peucker](https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm))
drops vertices within a perpendicular-distance band of the retained chord. Both
use the distance-scale `tolerance` (for `'vw'` the effective-area threshold is
`tolerance**2 / 2`) and share the `preserve_topology=True` default.

[`offset_curve`][gometry.Geometry.offset_curve] produces a line parallel to a
[`LineString`][gometry.LineString] at a signed distance (positive = left of the direction of
travel), with the same corner vocabulary as [`buffer`][gometry.Geometry.buffer] — round fillet arcs
by default, `join_style='miter'`/`'bevel'` and `quadrant_segments` on demand:

```python exec="on" source="block" result="text"
import gometry as gm

print((gm.LineString([(0, 0), (5, 0)])).offset_curve(1.0))

```

A positive distance offsets to the left of travel, and a negative one to the right.

```python exec="on" html="true"
from _figures import panels
import gometry as gm

line = gm.LineString([(0, 0), (3, 0), (3, 3)])
print(panels([
    ("offset_curve(0.5)  (left)", [line, line.offset_curve(0.5)]),
    ("offset_curve(-0.5)  (right)", [line, line.offset_curve(-0.5)]),
]))

```

## Snap and segmentize

These reshape a geometry's **vertices** — aligning almost-coincident edges before
an overlay, or adding intermediate vertices so a later planar reproject or overlay
tracks a curve more closely. They do **not** promise a bit-identical representation
or an unchanged geometry kind: snapped coordinates may collapse duplicates, and
related repair paths (for example [`snap_to_grid(..., repair=True)`][gometry.Geometry.snap_to_grid]) can change kind
when a shell pinches.

[`snap`][gometry.snap] moves vertices of `geom` onto a `reference` geometry's
vertices when they fall within `tolerance` (planar units). Kind is preserved for the
typed free function ([`LineString`][gometry.LineString] in → `LineString` out); only the vertex
coordinates change. It handles two layers whose shared border is almost but not
exactly coincident:

```python exec="on" html="true"
from _figures import before_after, with_vertices
import gometry as gm

noisy = gm.LineString([(0.08, 0.05), (1.95, 0.1), (4.05, -0.06)])
grid = gm.MultiPoint([(0, 0), (2, 0), (4, 0)])
snapped = gm.snap(noisy, grid, tolerance=0.3)
print(before_after(with_vertices(noisy), with_vertices(snapped),
                   after_caption="snap(tolerance=0.3)"))

```

[`segmentize`][gometry.Geometry.segmentize] has two explicit constraints. Pass a
positional `max_length` when every output segment must be no longer than that
distance, or pass `fraction=` to split every input segment into equal pieces
(`fraction=0.25` produces four pieces). Both preserve the represented shape and
only raise vertex density. See [CRS, units & measurement](crs.md) for the
`max_length` metric. `fraction=` has no unit and cannot be combined with `unit=`:

```python exec="on" html="true"
from _figures import panels, with_vertices
import gometry as gm

line = gm.LineString([(0, 0), (10, 0), (10, 6)])
print(panels([
    (f"original ({len(line.coords)} vertices)", with_vertices(line)),
    ("segmentize(3.0)", with_vertices(line.segmentize(3.0))),
    ("segmentize(fraction=0.25)", with_vertices(line.segmentize(fraction=0.25))),
]))

```

## Hulls and bounding shapes

| Call | Result |
|------|--------|
| [`convex_hull`][gometry.Geometry.convex_hull] | smallest convex polygon enclosing the input |
| [`concave_hull`][gometry.Geometry.concave_hull] | tighter, possibly non-convex hull (`concavity`) |
| [`minimum_rotated_rectangle`][gometry.Geometry.minimum_rotated_rectangle] | smallest-area enclosing rectangle, any orientation |
| [`envelope`][gometry.Geometry.envelope] | axis-aligned bounding-box polygon |
| [`centroid`][gometry.Geometry.centroid] | the geometry's center of mass |
| [`point_on_surface`][gometry.Geometry.point_on_surface] | a point on the geometry (polygonal interior for areal input) |
| [`polylabel`][gometry.Geometry.polylabel] | the [pole of inaccessibility](https://en.wikipedia.org/wiki/Pole_of_inaccessibility) — the most-interior point |
| [`maximum_inscribed_circle`][gometry.Geometry.maximum_inscribed_circle] | largest inscribed circle as a filled disk; use [`maximum_inscribed_radius`][gometry.Geometry.maximum_inscribed_radius] for the radius |

```python exec="on" source="block" result="text"
import gometry as gm
cloud = gm.MultiPoint([(0, 0), (2, 0), (3, 2), (1, 3), (0, 2), (1, 1)])
print('convex hull:', cloud.convex_hull())
print('centroid:   ', (cloud.centroid()).to_wkt())

```

`polylabel`, `maximum_inscribed_circle`, and `maximum_inscribed_radius` use a
scale-aware `tolerance=None` default derived from each geometry's extent. Pass a
positive tolerance only when you need an explicit error bound; array methods
also accept one tolerance per row.

The circle result is a geometry, and the radius result is numeric:

```python exec="on" source="block" result="text"
import gometry as gm
room = gm.box(0, 0, 8, 4)
circle = room.maximum_inscribed_circle(tolerance=0.01)
print('center:', circle.centroid().to_wkt(), 'radius:', round(room.maximum_inscribed_radius(tolerance=0.01), 3))

```

`concave_hull`'s `concavity` controls how tightly the hull follows the points:
larger values give a smoother, more convex result; smaller values hug the
points more closely.

```python exec="on" source="block" result="text"
import gometry as gm

cloud = gm.MultiPoint([(0, 0), (4, 0), (4, 4), (0, 4), (2, 1.2), (2, 0.8)])
print("convex: ", cloud.convex_hull())
print("concave:", cloud.concave_hull(concavity=0.5))

```

```python exec="on" html="true"
from _figures import panels
import gometry as gm

cloud = gm.MultiPoint([(0, 0), (4, 0), (4, 4), (0, 4), (2, 1.2), (2, 0.8)])
print(panels([
    ("convex_hull", [cloud.convex_hull(), cloud]),
    ('concave_hull(concavity=0.5)', [cloud.concave_hull(concavity=0.5), cloud]),
]))

```

!!! tip "`centroid` can land outside the shape"
    For a C-shaped or ring polygon the center of mass may fall in a hole or
    outside the geometry entirely. [`point_on_surface`][gometry.Geometry.point_on_surface]
    returns a representative point guaranteed to lie in the interior.

## Tessellation: triangulate, Voronoi, polygonize

[`triangulate`][gometry.Geometry.triangulate] makes the triangulation choice
explicit: `method='delaunay'` triangulates input vertices,
`method='earcut'` triangulates polygon interiors, and `method='constrained'`
keeps input edges as constraints. [`voronoi_polygons`][gometry.Geometry.voronoi_polygons]
returns the dual Voronoi cells. [`polygonize`][gometry.Geometry.polygonize]
assembles polygons from a collection of noded linework (the inverse of taking
boundaries). These return a [`GeometryArray`][gometry.GeometryArray] — a
vectorized container you iterate, index, and `len()` directly.

```python exec="on" source="block" result="text"
import gometry as gm

corners = gm.MultiPoint([(0, 0), (1, 0), (1, 1), (0, 1)])

triangles = corners.triangulate(method='delaunay')
cells = corners.voronoi_polygons()
print("triangles:", len(triangles))
print("voronoi cells:", len(cells))

edges = [
    gm.LineString([(0, 0), (1, 0)]),
    gm.LineString([(1, 0), (1, 1)]),
    gm.LineString([(1, 1), (0, 0)]),
]
print("polygonized:", len(gm.polygonize(edges)))

```

[`minimum_bounding_circle`][gometry.Geometry.minimum_bounding_circle] returns the
smallest enclosing circle as geometry; [`minimum_bounding_radius`][gometry.Geometry.minimum_bounding_radius]
returns only that circle's radius. On arrays, the radius method returns a
read-only `float64` NumPy column.

!!! note "Choose the triangulation"
    `method='constrained'` accepts `refine`, `min_angle`, and `max_area` for
    constrained refinement; `method='earcut'` is the direct polygon-interior
    triangulation; `method='delaunay'` needs only the input vertices.
    [`voronoi_edges`][gometry.Geometry.voronoi_edges] returns the cell boundaries
    as lines, and
    [`polygonize_full`][gometry.polygonize_full] additionally reports dangles,
    cut edges, and invalid rings for pooled raw linework.
    [`GeometryArray.polygonize()`][gometry.GeometryArray.polygonize] is row-wise and returns
    [`Groups`][gometry.Groups]; pass the array directly to either free function to opt into one
    pooled graph. CDT
    `min_angle`/`max_area` refinement targets are best-effort on degenerate or
    sliver inputs; the result is still a valid conforming triangulation.

## Z and M through constructive ops

Constructive operation Z/M behavior follows the [geometry guide's rules](geometry.md#zm-under-operations).

## See also

- [CRS](crs.md) — same CRS metric rule for `buffer` / `offset_curve`.
- [Predicates](predicates.md) — relationships without constructing geometry.
- [Geometry & dimensions](geometry.md) — types, inspection, and the Z/M ordinate carry doctrine.
- [API: Geometry.buffer][gometry.Geometry.buffer] · [intersection][gometry.intersection] ·
  [union_all][gometry.union_all]
