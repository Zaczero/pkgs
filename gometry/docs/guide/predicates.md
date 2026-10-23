---
description: gometry spatial predicates — contains, intersects, touches, crosses, overlaps, equals, boundary semantics, DE-9IM, and vectorized forms.
---

# Spatial predicates

Predicates answer **yes/no** questions about how two geometries relate:
*does this polygon contain that point? do these roads cross?* gometry's
predicates evaluate topology in X/Y without transforming coordinates or
measuring distance. Both geometries must carry matching CRS and coordinate-epoch
metadata; otherwise the operation raises [`CRSMismatchError`][gometry.CRSMismatchError].

## The predicate vocabulary

| Predicate | True when… | Symmetric? |
|-----------|------------|------------|
| [`contains`][gometry.contains] | no point of `right` lies outside `left`, and their interiors share a point | no |
| [`contains_properly`][gometry.contains_properly] | `contains` with **no boundary contact** at all | no |
| [`within`][gometry.within] | `left` is contained by `right` (the inverse of `contains`) | no |
| [`covers`][gometry.covers] | every point of `right` is in `left` (boundary-inclusive) | no |
| [`covered_by`][gometry.covered_by] | the inverse of `covers` | no |
| [`intersects`][gometry.intersects] | they share **any** point | yes |
| [`disjoint`][gometry.disjoint] | they share **no** point (`not intersects`) | yes |
| [`touches`][gometry.touches] | they meet only on boundaries, interiors stay apart | yes |
| [`crosses`][gometry.crosses] | a dimension-dependent interior crossing; for example, a line has interior parts inside and outside a polygon, or two curves meet at an interior point | yes |
| [`overlaps`][gometry.overlaps] | same-dimension overlap, neither contains the other | yes |
| [`equals`][gometry.equals] | topologically identical point sets | yes |

```python exec="on" source="block" result="text" interactive="off"
import gometry as gm

region = gm.box(0, 0, 10, 10)
inside = gm.Point(5, 5)
outside = gm.Point(20, 20)

print("contains inside:", gm.contains(region, inside))
print("contains outside:", gm.contains(region, outside))
print("disjoint outside:", gm.disjoint(region, outside))

```

```python exec="on" html="true"
from _figures import panels
import gometry as gm

a = gm.box(0, 0, 4, 4)
cases = [
    ("contains(A, B)", gm.box(1, 1, 3, 3), "A surrounds B, and their interiors overlap."),
    ("overlaps(A, B)", gm.box(2, 2, 6, 6), "A and B share an interior region, but neither contains the other."),
    ("touches(A, B)", gm.box(4, 0, 7, 4), "A and B share one boundary edge while their interiors remain separate."),
    ("disjoint(A, B)", gm.box(6, 6, 9, 9), "A and B share no interior or boundary points."),
]
print(panels(
    [(caption, {'source': a.exterior, 'query': b}, alt) for caption, b, alt in cases],
    labels={'source': 'A', 'query': 'B'},
))

```

Binary predicates are free functions: [`gm.contains(left, right)`][gometry.contains]. The same
spelling handles scalar and array combinations. Repeated probing needs no
preparation step: a geometry builds its point index after its first few probes
and keeps it.

## Vectorization

The module-level predicates are **vectorized**: when one argument is scalar and
the other is a [`GeometryArray`][gometry.GeometryArray] or another gometry geometry
sequence, they return a read-only `bool_` `numpy.ndarray` mask evaluated in Rust
without the Python per-pair overhead.

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(0, 0, 10, 10)
probes = gm.GeometryArray([gm.Point(5, 5), gm.Point(20, 20), gm.Point(4, 4)])

hits = gm.contains(region, probes)
print(repr(hits))
print("matching indices:", [i for i, h in enumerate(hits) if h])

```

A sequence argument returns one typed mask from a native batch. Lists, object
arrays, and other iterables of `Geometry | None` are normalized to geometry
rows; missing rows remain at their original positions. Broadcasting is strict:
**scalar-vs-sequence** broadcasts, and **equal-length sequences** are compared
pairwise; mismatched non-scalar lengths raise. Many-to-many work uses a
[`gm.SpatialIndex`][gometry.SpatialIndex] or [`gm.join`][gometry.join].

### Coordinate predicates

Use the same predicate with keyword-only `x` and `y` when testing raw
coordinates. This keeps the point-probe kernel without constructing
[`Point`][gometry.Point] objects:

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(0, 0, 10, 10)
print(gm.contains(region, x=5, y=5))
print(repr(gm.intersects(region, x=[5, 30], y=[5, 5])))

```

Provide either a right-hand geometry or both coordinate keywords. The
coordinates are interpreted in the left geometry's CRS. Geographic inputs use
the same antimeridian and pole topology as predicates against explicit points,
including strict interior versus boundary contact at the frame's periodic seam
and poles (±180°/±90° for EPSG:4326).

## Robust predicates

Orientation and intersection tests use **adaptive-precision arithmetic** to
resolve collinear, coincident, and nearly degenerate input. Scalar, array,
prepared, and index queries use the same exact topology.

### Geographic coordinates

In a geographic CRS, topology resolves the CRS-native longitude period, seams,
poles, and prime meridian before planar tests. Edges are straight in the source
coordinate chart, not geodesics; projection and densification can therefore
change the represented shape.

An unmarked once-winding ring is rejected when its latitude range spans both
hemispheres or lies entirely on the equator, because neither identifies the
enclosed pole. Equator-touching rings in one hemisphere remain admissible.
See [polar rings and full-turn edges](crs.md#polar-rings-and-full-turn-edges).

## Boundary points are not contained

The [OGC Simple Features](https://www.ogc.org/standard/sfa/) `contains` relation
requires that no point of the candidate lies outside the container and that
their interiors share a point. A point exactly on a polygon boundary does not
share the polygon's interior, so it fails `contains` and can satisfy `covers`.
A contained polygon can share part of the container boundary as long as their
interiors also overlap.

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(0, 0, 10, 10)
on_edge = gm.Point(0, 5)

print("contains:", gm.contains(region, on_edge))
print("covers:", gm.covers(region, on_edge))
print("touches:", gm.touches(region, on_edge))

```

The strictest member of the family is
[`contains_properly`][gometry.contains_properly]: the candidate must lie
entirely in the interior, never touching the container's boundary
(DE-9IM `T**FF*FF*`). It excludes shared borders.

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(0, 0, 10, 10)
inner = gm.box(2, 2, 8, 8)
flush = gm.box(0, 0, 5, 5)

print("contains:", gm.contains(region, flush))
print("contains_properly:", gm.contains_properly(region, flush))
print("properly inside:", gm.contains_properly(region, inner))

```

!!! tip "`contains` vs `covers`"
    `covers` includes the whole boundary case: a boundary point can be covered
    without being contained. Use `covers` when points on tile edges, snapped
    grids, or shared borders belong to the result.

## Empty geometry semantics

Operations can yield an **empty** geometry (e.g. the intersection of disjoint
shapes). Empties have **total** DE-9IM semantics — they are not "undefined":

| Pairing | Typical result |
|---|---|
| [`equals(empty, empty)`][gometry.equals] | **True** (compatible empty kinds) |
| [`disjoint(empty, anything)`][gometry.disjoint] incl. empty–empty | **True** |
| [`contains`][gometry.contains] / [`within`][gometry.within] / [`intersects`][gometry.intersects] / [`touches`][gometry.touches] / … | **False** |

```python exec="on" source="block" result="text"
import gometry as gm

empty = gm.intersection(gm.box(0, 0, 1, 1), gm.box(5, 5, 6, 6))
other = gm.from_wkt("POINT EMPTY")
print("is_empty:", empty.is_empty)
print("equals two empties:", gm.equals(empty, other))
print("disjoint two empties:", gm.disjoint(empty, other))
print("contains empty:", gm.contains(gm.box(0, 0, 1, 1), empty))

```

Applications that treat emptiness specially can branch on [`geom.is_empty`][gometry.Geometry.is_empty] (for
example, to skip rows or substitute a sentinel). The empty-input predicate table
defines the behavior.

## Predicates ignore Z/M; topology is XY

For truth values, planar predicates evaluate **X/Y topology** and ignore Z/M.
If you need ordinate-level identity, ask for it explicitly:

```python exec="on" source="block" result="text"
import gometry as gm
a = gm.Point(1.0, 2.0, z=10.0)
b = gm.Point(1.0, 2.0, z=99.0)
print('equals (XY topology):', gm.equals(a, b))
print('equals_exact (with Z):', gm.equals_exact(a, b, include_z=True))
print('== (value identity):', a == b)

```

See [geometry](geometry.md) for the ordinate model.

## Equality semantics

[`equals`][gometry.equals] is a **topological** test: two geometries are equal
if they cover the same point set, regardless of vertex order or starting
point. [`equals_exact`][gometry.equals_exact] is a **coordinate** test: same
structure with every paired ordinate within `tolerance` (default `0.0`, i.e.
exact), in the **same order** — pass `tolerance=` for floating-point
comparison and `include_z`/`include_m` to select ordinates.
[`equals_identical`][gometry.equals_identical] (and scalar `==`) is full
**value identity** — kind, vertex order, active Z/M, **and** CRS/epoch — so
geometries are hashable; a frame difference returns `False` rather than raising.

| Question | API | Vertex order | Z/M | CRS/epoch mismatch |
|---|---|---:|---:|---|
| Same spatial point set? | `gm.equals(a, b)` | Ignored | Ignored by 2D topology | Raises |
| Same coordinate sequence within a tolerance? | `gm.equals_exact(a, b, tolerance=...)` | Compared | Selected by `include_z`/`include_m` | Raises |
| Same complete Python value? | `gm.equals_identical(a, b)` / scalar `a == b` | Compared bit-for-bit | Compared | Returns `False` |

```python exec="on" source="block" result="text"
import gometry as gm
forward = gm.LineString([(0, 0), (1, 1), (2, 2)])
backward = gm.LineString([(2, 2), (1, 1), (0, 0)])
print('equals:', gm.equals(forward, backward))
print('equals_exact:', gm.equals_exact(forward, backward))
print('equals_identical:', gm.equals_identical(forward, backward))
print('==:', forward == backward)

```

## DE-9IM: the model under the hood

Each named predicate is a shorthand for a pattern over the
**Dimensionally Extended 9-Intersection Model** (DE-9IM). It describes how the
*interior*, *boundary*, and *exterior* of two geometries intersect, as a 3×3
matrix flattened to a 9-character string. A result cell is `F` (no intersection)
or `0`/`1`/`2` (the intersection's dimension). Pattern matching also accepts
`T` (any intersection) and `*` (don't care).

`relate` returns the matrix; [`relate_pattern`][gometry.relate_pattern]
tests it against a mask.

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(0, 0, 10, 10)
probe = gm.Point(5, 5)

matrix = gm.relate(region, probe)
print("DE-9IM:", matrix)

print("matches contains pattern:", gm.relate_pattern(region, probe, "T*****FF*"))

```

`relate`/`relate_pattern` represent relationships not covered by a named
predicate, such as boundaries that touch while interiors are disjoint and both
geometries are one-dimensional. One matrix can be tested against several
patterns without recomputing the relationship for each named predicate.

## MultiLineString boundary semantics

`MultiLineString` uses the **mod-2 boundary rule** from
[OGC Simple Feature Access](https://docs.ogc.org/is/06-103r4/06-103r4.pdf):
an endpoint belongs to the collection boundary only when it occurs in an odd
number of member `LineString` boundaries.

For the chain represented in the table below, `(0, 0)` and `(4, 4)` have odd
incidence and are boundary points. The shared node `(4, 0)` has even incidence,
so it is in the `MultiLineString` interior.

| Named case | `relate(point, lines)` | `relate(lines, point)` |
|---|---|---|
| Odd-incidence boundary `(0, 0)` | `F0FFFF102` | `FF10F0FF2` |
| Even-incidence shared node `(4, 0)` | `0FFFFF102` | `0F1FF0FF2` |
| Off-line point `(7, 9)` | `FF0FFF102` | `FF1FF00F2` |

Consequently, `contains(lines, point)`
and `contains_properly(lines, point)` are true only for the shared interior
node; `covers(lines, point)` is true for both on-line points. The inverse
`within` and `covered_by` results follow the same distinction. `touches` is
true only for the odd-incidence boundary point, while `intersects` is true for
both on-line points and `disjoint` is true only for the off-line point.

!!! note "Planar computation, strict metadata"
    Predicates read ordinates as a flat plane and require matching CRS and
    coordinate-epoch metadata. Reprojection can change vertices, seams, and the
    realized topology. On a geographic CRS, topology resolves longitude aliases
    and seam and pole equivalence in the CRS-native frame; seam-crossing input is
    split into chart pieces before construction. Metric behavior is documented in
    [CRS, units & measurement](crs.md).

## See also

- [Geometry](geometry.md) — types, the ordinate (Z/M) model, and inspection.
- [Validation & repair](validation.md) — detecting and fixing invalid geometry.
- [Constructive operations](constructive.md) — overlays that produce new geometry.
- [Arrays & performance](arrays.md) — vectorized calls and broadcasting.
- [Spatial indexing & joins](indexing.md) — `query(..., predicate=...)` refine.
- Coming from Shapely? See [Migrating](../migrating/index.md#coming-from-shapely).
- [API: contains][gometry.contains] · [intersects][gometry.intersects] ·
  [relate][gometry.relate] · [dwithin][gometry.dwithin]
