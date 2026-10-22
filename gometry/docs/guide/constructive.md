---
description: Constructive geometry in gometry — buffers, overlays, simplify, offset curves, snap, segmentize, hulls, centroids, triangulation, and Voronoi.
---

# Constructive operations

Constructive operations take geometries and produce **new** geometries:
buffers, set-theoretic overlays, simplifications, hulls, and tessellations.
All inputs are immutable, so every operation returns a fresh value.

Overlays require matching CRS and coordinate epoch and preserve that frame. They
use coordinate-plane topology: in a geographic CRS, the CRS-native frame first
resolves longitude aliases, seam spellings, and pole longitudes, while
seam-crossing inputs are split into chart pieces before construction. Overlay is
not a geodesic operation, and reprojecting can change vertices, seams, and
topology. [`buffer`][gometry.Geometry.buffer] and
[`offset_curve`][gometry.Geometry.offset_curve] take distances in the CRS-natural units described in [CRS, units
& measurement](crs.md). Geographic buffers use a local projection and are not
exact ellipsoidal offsets.

## Buffer

[`buffer`][gometry.Geometry.buffer] grows (positive distance) or shrinks (negative
distance) a geometry by a fixed amount, producing an areal
[`Polygon`][gometry.Polygon] or [`MultiPolygon`][gometry.MultiPolygon] result.
For a negative distance on multipart areal input, gometry first normalizes the
complete areal support and then erodes that union. Touching or overlapping
members therefore share one erosion boundary rather than being eroded as
independent polygons.

```python exec="on" source="block" result="text" interactive="off"
import gometry as gm

line = gm.LineString([(0, 0), (5, 0), (5, 5)])
corridor = line.buffer(1.0)
print(corridor.geometry_type, "area:", round(corridor.area, 3))

```

```python exec="on" html="true"
from _figures import before_after
import gometry as gm
line = gm.LineString([(0, 0), (5, 0), (5, 5)])
print(before_after(
    line, line.buffer(1.0),
    before_caption="route centerline", after_caption="1-unit corridor",
    before_alt="The route is an L-shaped centerline in a CRS-free unit plane.",
    after_alt="A one-unit buffer surrounds both sides and both ends of the route.",
))

```

### Controlling the shape of the buffer

Use these options to control the shape; see the
[`buffer` reference][gometry.Geometry.buffer] for defaults:

| Parameter | Effect |
|-----------|---------|
| `quadrant_segments` | segments per quarter-circle for round joins in either buffer mode; higher = rounder, more vertices |
| `cap_style` | end-cap at the original first and last vertices of open lines: `'round'`, `'flat'`, `'square'`; sided buffers always use flat ends |
| `join_style` | interior corner style for both two-sided and sided buffers, including 180° reversals: `'round'`, `'miter'`, `'bevel'` |
| `miter_limit` | with `join_style='miter'` in either buffer mode, clip spikes when the miter would exceed `limit × distance` |
| `side` | stroke side for linework: `'both'`, `'left'`, or `'right'`; sided distances must be non-negative |

Point inputs always buffer to round discs, and MultiPoint buffers are unions
of round discs; `cap_style` does not affect either.

Only an open line's original first and last vertices receive end caps. An
interior reversal is a join, so `join_style` controls its shape regardless of
`cap_style`; closed lines have no end caps. A round cap adds a half-disc at
each open-line endpoint, while a flat cap stops at the endpoint. For
`side='left'` or `'right'`, the ends are always flat and the join parameters
still control the offset edge.

```python exec="on" source="block" result="text" interactive="off"
import gometry as gm

line = gm.LineString([(0, 0), (5, 0)])
print("round cap:", round(line.buffer(1.0, cap_style="round").area, 3))
print("flat cap:", round(line.buffer(1.0, cap_style="flat", join_style="miter").area, 3))

```

```python exec="on" html="true"
from _figures import panels
import gometry as gm

line = gm.LineString([(0, 0), (5, 0)])
print(panels([
    (caption, {'source': line, 'result': line.buffer(1.0, **kwargs)}, alt)
    for caption, kwargs, alt in [
        ('cap_style="round"', {"cap_style": "round"}, "The buffer extends past each endpoint with a semicircular cap."),
        ('cap_style="flat"', {"cap_style": "flat", "join_style": "miter"}, "The buffer stops perpendicular to the line at each endpoint."),
    ]
]))

```

!!! warning "Geographic buffers use a local projection approximation"
    On a geographic CRS, `buffer` and `offset_curve` are **not** exact
    ellipsoidal offsets. gometry picks one bounded local frame that holds both
    the input and the result, as
    [`estimate_local_crs()`][gometry.Geometry.estimate_local_crs] does, reprojects, runs the
    planar constructive kernel at the requested meter distance, then
    reprojects back to the source CRS. It is intended for city-scale features
    and modest radii; distortion grows when the
    geometry spans many UTM zones, sits far from the anchor, or the buffer
    radius is large relative to the feature. For continent-scale work or a
    strict accuracy budget, reproject to an appropriate projected CRS first
    (or tile the operation) instead of relying on the automatic local frame. If a
    specific accuracy budget matters, choose and validate the projected frame
    explicitly.

### Geographic local-frame input refinement

For geographic operations that use line or ring boundaries, gometry checks each
stored source-chart edge against the selected local projection before running
the planar kernel. If the projected curve's certified maximum distance from its
endpoint chord exceeds the input-refinement budget, the edge is subdivided at
equal ellipsoidal arclength and checked piece by piece. The budget is
`1e-4 × min(projected input extent, abs(distance))` for distance-bearing
operations, and `1e-4 × projected input extent` otherwise. The projected input
extent is the diagonal of its local-frame bounds.

This input-fidelity budget is separate from the frame-fit check: the chosen
frame must still hold the input and constructive result within its existing
0.1% linear-scale-error limit. It is also separate from round-arc
polygonization, whose resolution is controlled by `quadrant_segments`. These
bounds do not by themselves promise an end-to-end ellipsoidal distance error.
For open offsets and single-sided buffers, the budget bounds input fidelity rather than output endpoint displacement, because the refined chord's direction determines the endpoint normal.
Site-based operations such as `concave_hull` continue to select from original
input vertices; refinement samples represent edge support, not new sites.

## Overlay (set operations)

The four boolean overlays combine two geometries. Each function also has a
Python operator:

| Operation | Operator | Result |
|-----------|----------|--------|
| [`intersection`][gometry.intersection] | `a & b` | the shared region (`left ∩ right`) |
| [`union`][gometry.union] | <code>a \| b</code> | everything in either (`left ∪ right`) |
| [`difference`][gometry.difference] | `a - b` | `left` with `right` cut out (`left − right`) |
| [`symmetric_difference`][gometry.symmetric_difference] | `a ^ b` | in exactly one of the two |

```python exec="on" source="block" result="text" interactive="off"
import gometry as gm

a = gm.box(0, 0, 2, 2)
b = gm.box(1, 1, 3, 3)

print("intersection:", a & b)
print("union:", a | b)
print("difference:", a - b)

```

The operators are the named **free functions** with the same behavior: scalars
and arrays broadcast identically, CRS conflicts raise the same error, and Z/M
is restored wherever the result vertex can be sourced from an input segment.

`intersection` keeps every dimension of contact: two polygons that merely
share a border intersect in that border (a [`LineString`][gometry.LineString]), and a corner-only
touch yields the corner [`Point`][gometry.Point] — so a non-empty [`intersects(a, b)`][gometry.intersects] always has
a non-empty `intersection(a, b)`.

When geographic overlay output closes across a pole, it closes over the pole
selected by that region; a pole-free strip between regions remains pole-free.
Reaching the seam alone does not add a polar cap.

```python exec="on" source="block" result="text"
import gometry as gm

cap = gm.Polygon(
    [(-180, 5), (-90, 5), (0, 5), (90, 5), (180, 5), (-180, 5)],
    crs=4326,
)
connector = gm.box(-10, -20, 10, 10, crs=4326)
merged = gm.union(cap, connector)
print("selected north pole:", gm.contains(merged, gm.Point(0, 85, crs=4326)))
print("opposite south pole:", gm.contains(merged, gm.Point(0, -85, crs=4326)))
```

When one operand is a point set (a `Point`, `MultiPoint`, or point-only
collection) and the other has higher dimension, `intersection` returns the
covered points and `difference(points, other)` returns the uncovered points.
`union` and `symmetric_difference` add only uncovered points to the
higher-dimensional geometry; `difference(geometry, points)` leaves a
higher-dimensional geometry unchanged. The retained higher-dimensional part
preserves its stored line and polygon coordinates, including ring start,
orientation, collinear vertices, and Z/M values. Ordinates on absorbed points do
not rewrite it. Call [`normalize()`][gometry.Geometry.normalize]
explicitly when a canonical coordinate presentation is needed.

```python exec="on" html="true"
from _figures import panels
import gometry as gm

a, b = gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)
frame = [a.exterior, b.exterior]
print(panels(
    [
        ("a & b (intersection)", {'source': frame, 'result': a & b}, "The intersection keeps only the square shared by A and B."),
        ("a | b (union)", {'source': frame, 'result': a | b}, "The union keeps every region covered by A or B as one polygon."),
        ("a - b (difference)", {'source': frame, 'result': a - b}, "The difference removes B's overlapping corner from A."),
        ("a ^ b (symmetric difference)", {'source': frame, 'result': a ^ b}, "The symmetric difference removes the shared square and keeps the non-overlapping parts."),
    ],
    labels={'source': 'A and B outlines'},
))

```

!!! note "Overlay guards your CRS"
    Overlay and [`union_all`][gometry.union_all] reject inputs with
    **conflicting** CRS metadata. Reproject both operands to a common CRS first —
    see the [CRS, units & measurement](crs.md).

### Output coordinates

For overlays, which pieces exist and how they touch are decided from the exact
stored coordinates of the operands. Vertices that come from an operand keep
their stored coordinates. A constructed vertex, such as an edge crossing, is
published at the nearest representable binary64 coordinate to its exact
position whenever that assignment preserves the exact structure.

Occasionally two constructed overlay vertices would round to the same
coordinate, or rounding would make an edge touch a vertex it must miss. The
operation searches the nearest coordinate and its adjacent binary64
values for the constructed vertices in conflict. It publishes only an
assignment that preserves incidence, cyclic order, and faces, and verifies
that assignment before returning it. Each moved coordinate stays within 1.5
times the larger spacing to its neighboring binary64 values on that axis. A
stored vertex, or an exactly representable constructed coordinate, never
moves. Geographic overlay output does not move a vertex onto a seam or pole.
Z/M values are evaluated at the exact constructed vertex. On geographic source
edges, eligible ordinates follow ellipsoidal arclength along the retained
straight chart edge, including across later cuts; see
[Z/M under operations](geometry.md#zm-under-operations).

If no assignment in the overlay operation's admitted finite coordinate domain
passes the certificate, it raises `OverflowError` with
`code == 'output_unrepresentable'` instead of returning geometry with changed
topology. This is a refusal under that operation's drawing domain and
preservation contract, not a claim that every possible binary64 drawing of the
mathematical result is impossible. For a geographic overlay, exhausting the
bounded work budget raises [`ResourceLimitError`][gometry.ResourceLimitError] with
`code == 'work_limit'` and `operation` naming the call. No geometry is returned;
the error means the operation stopped before it could finish its checked
construction, not that no valid output exists.
Simplify the operands or [`snap`][gometry.snap] near-coincident borders to a
common tolerance first. Overlaying a published result again is a new operation
on its rounded coordinates and is not promised to reproduce the first result.

### Dissolving many geometries: `union_all`

[`union_all`][gometry.union_all] aggregates the entire sequence in one
construction, avoiding the repeated intermediate rounding of a pairwise loop:

```python exec="on" source="block" result="text"
import gometry as gm

tiles = [gm.box(i, 0, i + 1, 1) for i in range(5)]
merged = gm.union_all(tiles)
print(merged.to_wkt())

```

[`gm.union_all`][gometry.union_all] takes a [`GeometryArray`][gometry.GeometryArray] or any iterable and aggregates it
whole. Grouped aggregation is a separate operation: [`array.dissolve(by=groups)`][gometry.GeometryArray.dissolve] returns
one geometry per first-seen group with the corresponding group keys. `dissolve`
requires `by`; whole-collection aggregation is `gm.union_all(array)`.

!!! note "`union_all([])` raises"
    `union_all([])` raises because an empty sequence has no geometry metadata.
    Filter the sequence or branch on emptiness before aggregation.

## Sample random points

[`sample_points`][gometry.Geometry.sample_points] draws from the highest-dimensional
support: area for polygons, length for lines, and equal-weight point occurrences.
Geographic data defaults to ellipsoid surface area and physical arclength along
the straight source-chart edges. This includes seam crossings and polar regions;
overlapping multipart members contribute separately. Degenerate area falls back
to linework, then vertices. Empty input produces no points, and new sampled
locations are XY.

```python exec="on" source="block" result="text" interactive="off"
import gometry as gm
region = gm.box(10, 50, 11, 51, crs=4326)
print(len(region.sample_points(100, seed=42)))
```

Omit `seed` for fresh entropy, pass an integer to reproduce a stream, or pass a
NumPy `Generator` to advance that generator once per call. Array calls accept
per-row counts and integer seeds; a scalar seed derives a separate stream for
each row, with row zero matching the scalar call. `unit='planar'` selects native
chart area or length instead of physical measure.

## Simplify and offset

[`simplify`][gometry.Geometry.simplify] drops vertices within a `tolerance`, keeping the
shape recognizable while shrinking it.

```python exec="on" source="block" result="text" interactive="off"
import gometry as gm

wiggly = gm.LineString([(0, 0), (1, 0.01), (2, -0.01), (3, 0.01), (4, 0)])
print(wiggly.simplify(0.5))

```

```python exec="on" html="true"
from _figures import panels, with_vertices
import gometry as gm
import numpy as np

xs = np.linspace(0, 10, 41)
ys = 0.8 * np.sin(xs * 1.2) + 0.18 * np.sin(xs * 7)
noisy = gm.LineString(list(zip(xs.tolist(), ys.tolist())))
panes = []
for tolerance in (0.0, 0.2, 0.6):
    simple = noisy.simplify(tolerance)
    panes.append((
        f"tolerance={tolerance} ({len(simple.coords)} vertices)",
        {'source': noisy, 'result': with_vertices(simple)},
        f"At tolerance {tolerance}, simplification retains {len(simple.coords)} vertices; increasing tolerance removes progressively larger zigzags while retaining both endpoints.",
    ))
print(panels(panes, labels={'source': 'original track', 'result': 'retained vertices'}))

```

[`simplify`][gometry.Geometry.simplify] takes a `method`: the default `'vw'`
([Visvalingam–Whyatt](https://en.wikipedia.org/wiki/Visvalingam%E2%80%93Whyatt_algorithm))
drops the *least visually significant* vertices first (the ones spanning the
smallest effective triangle), while `'dp'`
([Douglas–Peucker](https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm))
drops vertices within a perpendicular-distance band of the retained chord.
Both use the distance-scale `tolerance` (for `'vw'` the effective-area
threshold is `tolerance**2 / 2`) and share the `preserve_topology=True` default.
With that default, valid polygonal components remain valid and non-collapsed,
and simple lineal components remain simple. A
[`GeometryCollection`][gometry.GeometryCollection] is checked child by child;
relationships between separate children can change.

Tolerance defaults to metres for geographic data, native linear units for a
projected CRS, and raw coordinate units without a CRS. Use `unit='planar'` for
native chart units, or `unit='meters'` to convert a projected CRS's linear units.
Geographic simplification conservatively converts the tolerance using the
ellipsoid's global maximum principal radius; it retains source-chart edges and
can keep extra detail near the poles. It does not impose a local-projection
extent limit. Selected vertices retain their original X/Y/Z/M values. VW's
effective-area threshold is not a maximum-displacement guarantee.

[`offset_curve`][gometry.Geometry.offset_curve] produces a line parallel to a
[`LineString`][gometry.LineString] at a signed distance (positive = left of the direction of
travel), with the same corner vocabulary as [`buffer`][gometry.Geometry.buffer] — round fillet arcs
by default, `join_style='miter'`/`'bevel'` and `quadrant_segments` on demand:

```python exec="on" source="block" result="text" interactive="off"
import gometry as gm

print((gm.LineString([(0, 0), (5, 0)])).offset_curve(1.0))

```

A positive distance offsets to the left of travel, and a negative one to the right.

```python exec="on" html="true"
from _figures import panels
import gometry as gm

line = gm.LineString([(0, 0), (3, 0), (3, 3)])
print(panels([
    ("offset_curve(0.5) (left)", {'source': line, 'result': line.offset_curve(0.5)}, "The positive offset follows the left side of the directed L-shaped line."),
    ("offset_curve(-0.5) (right)", {'source': line, 'result': line.offset_curve(-0.5)}, "The negative offset follows the right side of the directed L-shaped line."),
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

noisy = gm.LineString([(0.16, 0.12), (1.86, -0.14), (4.14, 0.14)])
grid = gm.MultiPoint([(0, 0), (2, 0), (4, 0)])
snapped = gm.snap(noisy, grid, tolerance=0.3)
print(before_after(with_vertices(noisy), with_vertices(snapped),
                   before_caption="misaligned parcel boundary",
                   after_caption="boundary snapped within 0.3 units",
                   before_alt="Three parcel-boundary vertices sit near, but not on, the reference points in a CRS-free unit plane.",
                   after_alt="Snapping moves all three boundary vertices exactly onto nearby reference points."))

```

[`segmentize`][gometry.Geometry.segmentize] has two explicit constraints. Pass a
positional `max_length` when every output segment must be no longer than that
distance, or pass `fraction=` to split each input edge into equal-arclength
pieces (`fraction=0.25` produces four). By default, geographic vertices stay on
the stored straight longitude/latitude edges, preserving their shape. See
[CRS, units & measurement](crs.md) for the `max_length` metric. The `max_length`
path guarantees that no output segment exceeds the requested distance and
preserves every original vertex. When binary64 rounding would otherwise break
that guarantee, it emits additional vertices so the result still satisfies the
maximum. `fraction=` has no unit and cannot be combined with `unit=`.

For a geographic geometry, `path='geodesic'` samples the shortest ellipsoidal
route and `path='rhumb'` samples a constant-bearing route. Their inserted
vertices are route-spaced, but the output remains a polyline with straight
longitude/latitude edges; its length approaches the chosen route length as the
pieces get shorter. `path='geodesic'` is planar on projected or CRS-free input;
`path='rhumb'` requires a geographic CRS. A non-`None` `path` cannot be combined
with `unit=`.

A request that would generate more than
**16,000,000** output items from one geometry is refused with a
[`ResourceLimitError`][gometry.ResourceLimitError] whose message names the operation,
produced count, limit, and controlling parameter. The limit bounds what a
parameter makes of each geometry; an array's length is not charged against it:

All modes retain original Z/M-bearing vertices in their stored order. A
zero-length X/Y segment is not subdivided, so consecutive same-X/Y vertices
with different active ordinates remain present.

```python exec="on" html="true"
from _figures import panels, with_vertices
import gometry as gm

line = gm.LineString([(0, 0), (10, 0), (10, 6)])
print(panels(
    [
        (f"original ({len(line.coords)} vertices)", {'source': with_vertices(line)}, "The original L-shaped line has three vertices and two long segments."),
        ("segmentize(3.0)", {'result': with_vertices(line.segmentize(3.0))}, "Segmentizing to a maximum length of 3 units adds vertices without changing the L shape."),
        ("segmentize(fraction=0.25)", {'result': with_vertices(line.segmentize(fraction=0.25))}, "Segmentizing by one-quarter splits each original segment into four equal pieces without changing the L shape."),
    ],
    caption="Both constraints raise vertex density without moving the line.",
    alt="All three panels draw the same L-shaped path; only the number and spacing of its vertices change.",
))

```

## Subdivide

[`Geometry.subdivide`][gometry.Geometry.subdivide] recursively clips a geometry
until each part fits the requested coordinate bound. On geographic input, new
cut vertices remain tied to their original input-edge trace through later
recursive cuts, rather than being interpolated from an already-cut edge. This
also holds where a source edge crosses the CRS-native longitude seam (±180° in
EPSG:4326); the geographic Z/M interpolation rule is described in
[Z/M under operations](geometry.md#zm-under-operations).

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
| [`maximum_inscribed_circle`][gometry.Geometry.maximum_inscribed_circle] | retained largest-inscribed-circle solution with direct center/radius access and polygon realization |
| [`minimum_bounding_circle`][gometry.Geometry.minimum_bounding_circle] | retained smallest-enclosing-circle solution |
| [`minimum_width`][gometry.Geometry.minimum_width] | width of the narrowest enclosing parallel strip |

```python exec="on" source="block" result="text"
import gometry as gm
cloud = gm.MultiPoint([(0, 0), (2, 0), (3, 2), (1, 3), (0, 2), (1, 1)])
print('convex hull:', cloud.convex_hull())
print('centroid:', (cloud.centroid()).to_wkt())

```

`maximum_inscribed_circle` returns a [`Circle`][gometry.Circle] result.
Its `.center` is a `Point` in the input CRS and coordinate epoch, and `.radius`
is numerical. `unit=None` uses native coordinate units for planar/projected
input and meters in a local projection for geographic input. The result's
`.unit` and `.working_crs` identify that numerical frame.

`tolerance=None` selects a scale-aware default from the working geometry's extent.
An explicit positive tolerance bounds the planar global-inradius gap; arrays
also accept one tolerance per row. `polylabel` remains a separate approximate
label-placement search whose tolerance is a stopping target.

Call `.to_polygon(quadrant_segments=16)` when you need geometry. It uses the
retained disk without repeating the optimization; changing resolution does not
change the center or radius. Inscribed polygons are certified inside that disk.
The default has 64 vertices; a zero or subnormal-radius disk yields a `Point`,
and an empty disk yields `POLYGON EMPTY`. Polygonization can raise `OverflowError`
when a finite numerical solution has no representable polygonal realization.
The numerical result remains usable.

Geographic inscribed-circle results retain one local-projection disk. Their center and
polygon are transformed back to the source CRS, while the radius stays in meters.
The planar certificate applies in the working projection, not to an ellipsoidal
optimum or containment of the restored source-coordinate polygon.

```python exec="on" source="block" result="text"
import gometry as gm
room = gm.box(0, 0, 8, 4)
circle = room.maximum_inscribed_circle(tolerance=0.01)
print('center:', circle.center.to_wkt(), 'radius:', round(circle.radius, 3))
print('disk:', circle.to_polygon().geometry_type)

```

`concavity` sets the greedy peel's distance guard: a candidate triangle's
opposite vertex must lie within `edge_length / concavity` of its boundary edge
(`0` disables the guard). Higher values typically keep more area, but the
greedy peel does not guarantee area monotonicity in `concavity`. For fixed
input and `concavity`, increasing `length_threshold` never shrinks the hull.

```python exec="on" source="block" result="text" interactive="off"
import gometry as gm

cloud = gm.MultiPoint([(0, 0), (4, 0), (4, 4), (0, 4), (2, 1.2), (2, 0.8)])
print("convex:", cloud.convex_hull())
print("concave:", cloud.concave_hull(concavity=0.5))

```

```python exec="on" html="true"
from _figures import panels
import gometry as gm

cloud = gm.MultiPoint([(0, 0), (4, 0), (4, 4), (0, 4), (2, 1.2), (2, 0.8)])
print(panels(
    [
        ("convex hull (encloses all points)", {'source': cloud, 'result': cloud.convex_hull()}, "The convex hull spans the outer points and bridges across the inward pair."),
        ("concave hull (follows inward points)", {'source': cloud, 'result': cloud.concave_hull(concavity=0.5)}, "The concave hull bends inward to follow the interior pair while still enclosing every point."),
    ],
))

```

!!! tip "`centroid` can land outside the shape"
    For a C-shaped or ring polygon the center of mass may fall in a hole or
    outside the geometry entirely. [`point_on_surface`][gometry.Geometry.point_on_surface]
    returns a representative point guaranteed to lie in the interior.

## Tessellation: triangulate, Voronoi, polygonize

[`triangulate`][gometry.Geometry.triangulate] makes the triangulation choice
explicit: `method='delaunay'` triangulates input vertices,
`method='earcut'` triangulates polygon interiors, and `method='constrained'`
keeps input edges as constraints. [`voronoi`][gometry.Geometry.voronoi] returns
clipped cells by default; pass `output='edges'` for the internal Voronoi edges,
which exclude the clip polygon's exterior boundary. Geographic Voronoi charts
both its sites and supplied clip polygon in the CRS-native longitude frame;
Delaunay charts its input sites. Seam-crossing inputs do not require a
preliminary [`split_antimeridian`][gometry.Geometry.split_antimeridian].
Voronoi results preserve chart cuts and polar-cap boundaries. Delaunay is
a planar point-site triangulation, not a spherical mesh; use constrained
triangulation when a polygon boundary or selected polar cap must remain part of
the result. Geographic constrained triangulation carries seam constraints and
closes only the pole selected by the polygon. All three remain planar
longitude/latitude constructions, not geodesic operations.
`method='earcut'` follows the polygon's stored XY ring, so split a geographic
polygon first when it crosses the seam or encloses a pole. [`Geometry.polygonize()`][gometry.Geometry.polygonize]
polygonizes that geometry's own linework, automatically splitting crossings
and overlaps. [`polygonize_all`][gometry.polygonize_all] pools all supplied
geometries' linework into one graph before polygonization, so a ring can close
across inputs; its default form returns one [`GeometryArray`][gometry.GeometryArray]
of polygons. The row-wise [`GeometryArray.polygonize()`][gometry.GeometryArray.polygonize]
returns [`Groups`][gometry.Groups], one polygon array per input row; missing
rows yield `None` groups and present empty rows yield empty groups.
[`GeometryArray.build_area()`][gometry.GeometryArray.build_area] likewise
builds one areal result per input row without pooling linework between rows;
missing rows remain missing.
For BuildArea fill across several geometry values, combine the linework into
one [`GeometryCollection`][gometry.GeometryCollection] before calling
`build_area()`; `polygonize_all` is the pooled polygonizer and returns polygon
faces instead of the single even-odd fill result.

```python exec="on" source="block" result="text"
import gometry as gm

edges = [
    gm.LineString([(0, 0), (1, 0)]),
    gm.LineString([(1, 0), (1, 1)]),
    gm.LineString([(1, 1), (0, 1)]),
    gm.LineString([(0, 1), (0, 0)]),
]
pooled_area = gm.GeometryCollection(edges).build_area()
row_areas = gm.GeometryArray(edges).build_area()
print(pooled_area.geometry_type, "area:", pooled_area.area)
print("independent empty rows:", [area.is_empty for area in row_areas])
```

```python exec="on" source="block" result="text"
import gometry as gm

corners = gm.MultiPoint([(0, 0), (1, 0), (1, 1), (0, 1)])

triangles = corners.triangulate(method='delaunay')
cells = corners.voronoi()
edges = corners.voronoi(output='edges')
print("triangles:", len(triangles))
print("voronoi cells:", len(cells))
print("voronoi edges:", len(edges))

edges = [
    gm.LineString([(0, 0), (1, 0)]),
    gm.LineString([(1, 0), (1, 1)]),
    gm.LineString([(1, 1), (0, 0)]),
]
print("polygonized:", len(gm.polygonize_all(edges)))

```

```python exec="on" source="block" result="text"
import gometry as gm

sites = gm.MultiPoint([(170, 0), (-170, 0), (-170, 10), (170, 8)], crs=4326)
clip = gm.from_wkt(
    "POLYGON ((160 -10, 200 -10, 200 20, 160 20, 160 -10))",
    crs=4326,
)
print("Delaunay triangles:", len(sites.triangulate(method="delaunay")))
print("Voronoi cells:", len(sites.voronoi(clip=clip)))

seam_polygon = gm.Polygon(
    [(170, 0), (-170, 0), (-170, 10), (170, 8)],
    crs=4326,
)
print(
    "constrained seam triangles:",
    len(seam_polygon.triangulate(method="constrained")),
)

cap = gm.Polygon([(-45, 40), (45, 40), (135, 40), (-135, 40)], crs=4326)
split_cap = cap.split_antimeridian()
print(
    "Earcut triangles after splitting the polar cap:",
    len(split_cap.triangulate(method="earcut")),
)
```

[`minimum_bounding_circle`][gometry.Geometry.minimum_bounding_circle] returns
the same `Circle` model. For planar/projected input, `.radius` is the correctly
rounded mathematical minimum radius; `unit='meters'` scales the exact squared
radius before rounding. `.to_polygon()` creates a certified circumscribed
polygon whose edges enclose the exact disk and whose vertices lie outside it.
The exact disk is retained: reconstructing it from the independently rounded
`.center` and `.radius` can lose that guarantee.

For geographic point sets with two distinct sites, the result retains a
shortest-geodesic midpoint and half the ellipsoidal distance. These numerical
results remain available even for globally separated points. Their
`.to_polygon()` uses the existing geographic point-buffer approximation and can
raise `CRSError` when no sufficiently accurate local projection fits the
realization. A single site has radius zero and realizes as its center `Point`.
For these ellipsoidal disks, `.unit` is `"meters"` and `.working_crs` is `None`
because no Cartesian frame defines the solution. Other geographic bounding
circles use a retained local-projection support disk.

Array circle operations return [`CircleArray`][gometry.CircleArray].
`.center` is a point `GeometryArray`, `.radius` is a read-only `float64` NumPy
column, and `.is_missing` distinguishes missing rows from empty results.
Both have `NaN` radii. Indexing returns a `Circle` or `None`; slicing and
`.to_polygon()` preserve row order and missingness.

[`minimum_width`][gometry.Geometry.minimum_width] measures the narrowest strip
enclosing the complete convex hull. It is not minimum clearance, and it is not
the shorter side of the minimum-area rotated rectangle. Holes do not reduce
width; points and collinear support have width zero. Empty and missing rows
have width `NaN`. Its scalar/array and unit conventions match other metrics.

!!! note "Choose the triangulation"
    `method='constrained'` accepts `min_angle` and `max_area` for
    constrained refinement; `method='earcut'` is the direct polygon-interior
    triangulation; `method='delaunay'` needs only the input vertices.
    Constrained `min_angle`/`max_area` refinement is bounded at 16,000,000 generated
    vertices. A request whose estimated work exceeds that limit, or whose
    quality cannot be satisfied within it, raises a
    [`ResourceLimitError`][gometry.ResourceLimitError] naming the limit rather than
    returning an unconstrained result; relax the refinement targets when that
    happens.

### Polygonize diagnostics

[`polygonize_all(..., diagnostics=True)`][gometry.polygonize_all] returns the
pooled polygons plus source-row groups for cut edges, dangles, and invalid
rings. Geographic faces and diagnostics use one topology classification, so
an edge of a returned face is not also reported as an invalid ring.

```python exec="on" source="block" result="text"
import gometry as gm

crs = "+proj=longlat +a=7000000 +rf=150 +type=crs"
lines = gm.GeometryArray([
    gm.LineString([(0, 80), (90, 80)], crs=crs),
    gm.LineString([(90, 80), (180, 80)], crs=crs),
    gm.LineString([(180, 80), (270, 80)], crs=crs),
    gm.LineString([(270, 80), (0, 80)], crs=crs),
])
result = gm.polygonize_all(lines, diagnostics=True)
assert len(result.polygons) == 1
for group in (result.cut_edges, result.dangles, result.invalid_rings):
    assert all(len(row) == 0 for row in group)
print("polygons:", len(result.polygons))
print("cut edges:", [len(row) for row in result.cut_edges])
print("dangles:", [len(row) for row in result.dangles])
print("invalid rings:", [len(row) for row in result.invalid_rings])
```

## Z and M through constructive ops

Constructive operation Z/M behavior follows the [geometry guide's rules](geometry.md#zm-under-operations).

## See also

- [CRS](crs.md) — same CRS metric rule for `buffer` / `offset_curve`.
- [Predicates](predicates.md) — relationships without constructing geometry.
- [Geometry & dimensions](geometry.md) — types, inspection, and Z/M behavior.
- [API: Geometry.buffer][gometry.Geometry.buffer] · [intersection][gometry.intersection] ·
  [union_all][gometry.union_all]
