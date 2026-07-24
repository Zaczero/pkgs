---
description: Spatial predicates in gometry — contains, contains_properly, intersects, touches, crosses, overlaps, equals — their boundary semantics, the robust arithmetic and DE-9IM model behind them, vectorization, and the classic gotchas.
---

# Spatial predicates

Predicates answer **yes/no** questions about how two geometries relate:
*does this polygon contain that point? do these roads cross?* gometry's
predicates are **planar and topological**: they evaluate [DE-9IM](https://en.wikipedia.org/wiki/DE-9IM) relations on
the stored coordinate plane without transforming coordinates or measuring
distance, so they do **not** incur the square-degree metric error that bare
[measurement](crs.md) on lon/lat would. They are still model-sensitive: the
planar edge model must match the intended geometry (projection, densification,
seams, and domains can change realized topology). That planar computation does
**not** relax metadata rules — operand CRS and coordinate-epoch tags must still
match, or you get `CRSMismatchError` like every other binary op. It is also
**robust**: the orientation and intersection tests underneath are
adaptive-precision, so boundary-touching and near-degenerate input answer
consistently rather than flickering with floating-point noise.

## The predicate vocabulary

| Predicate | True when… | Symmetric? |
|-----------|------------|------------|
| [`contains`][gometry.contains] | `right` lies in `left`'s interior and they share interior | no |
| [`contains_properly`][gometry.contains_properly] | `contains` with **no boundary contact** at all | no |
| [`within`][gometry.within] | `left` is contained by `right` (the inverse of `contains`) | no |
| [`covers`][gometry.covers] | every point of `right` is in `left` (boundary-inclusive) | no |
| [`covered_by`][gometry.covered_by] | the inverse of `covers` | no |
| [`intersects`][gometry.intersects] | they share **any** point | yes |
| [`disjoint`][gometry.disjoint] | they share **no** point (`not intersects`) | yes |
| [`touches`][gometry.touches] | they meet only on boundaries, interiors stay apart | yes |
| [`crosses`][gometry.crosses] | interiors cross with lower dimension than either | yes |
| [`overlaps`][gometry.overlaps] | same-dimension overlap, neither contains the other | yes |
| [`equals`][gometry.equals] | topologically identical point sets | yes |

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(0, 0, 10, 10)
inside = gm.Point(5, 5)
outside = gm.Point(20, 20)

print("contains inside: ", gm.contains(region, inside))
print("contains outside:", gm.contains(region, outside))
print("disjoint outside:", gm.disjoint(region, outside))

```

A predicate has no geometric *result* — only a truth value — so an honest
figure has to encode the **relationship** itself. In each panel operand `A` is
drawn as an outline and operand `B` as a filled box, and the configuration is
chosen so the answer is unmistakable: `B` nested inside `A` (contains),
straddling a corner (overlaps), sharing a single edge (touches), and set well
apart (disjoint):

```python exec="on" html="true"
from _figures import panels
import gometry as gm

a = gm.box(0, 0, 4, 4)
cases = [
    ("contains(A, B)", gm.box(1, 1, 3, 3)),
    ("overlaps(A, B)", gm.box(2, 2, 6, 6)),
    ("touches(A, B)", gm.box(4, 0, 7, 4)),
    ("disjoint(A, B)", gm.box(6, 6, 9, 9)),
]
print(panels([(caption, [a.exterior, b]) for caption, b in cases]))

```

Binary predicates are free functions by design — `gm.contains(left, right)`,
never `left.contains(right)`. One spelling covers every scalar/array
combination, and it cannot be misread as mutating or asymmetric. (The
exceptions are the surfaces built for repeated probing: `PreparedGeometry`
and the grid coverages carry predicate *methods* because the receiver is the
accelerated object itself.)

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(0, 0, 10, 10)
print(gm.contains(region, gm.Point(5, 5)))   # True — the point lies inside the box

```

## Vectorization

The module-level predicates are **vectorized**: keep one argument scalar and
pass a [`GeometryArray`][gometry.GeometryArray] or another gometry geometry sequence
for the other, and you get back a read-only `bool_` `numpy.ndarray`
mask evaluated in Rust without the Python per-pair overhead. This is the idiomatic
way to test one geometry against many.

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(0, 0, 10, 10)
probes = gm.GeometryArray([gm.Point(5, 5), gm.Point(20, 20), gm.Point(4, 4)])

hits = gm.contains(region, probes)
print(hits)
print("matching indices:", [i for i, h in enumerate(hits) if h])

```

!!! tip "Reach for the array form before writing a Python loop"
    A list comprehension calling a predicate per pair pays Python's call
    overhead on every iteration. Passing the whole sequence once keeps the
    loop in Rust and is dramatically faster for large batches. Broadcasting is
    deliberately strict to avoid accidental Cartesian products: **scalar-vs-
    sequence** broadcasts, and **equal-length sequences** are compared
    pairwise, but mismatched non-scalar lengths raise. For genuine many-to-many
    work, build a [`gm.SpatialIndex`][gometry.SpatialIndex] or use [`gm.join`][gometry.join].

### The `_xy` fast path

When you are testing against raw coordinates rather than `Point` objects,
[`contains_xy`][gometry.contains_xy] and [`intersects_xy`][gometry.intersects_xy]
skip constructing the point entirely:

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(0, 0, 10, 10)
print(gm.contains_xy(region, 5, 5))
print(gm.intersects_xy(region, [5, 30], [5, 5]))   # vectorized over coordinates

```

The coordinates are interpreted in the geometry's CRS. Geographic inputs use
the same antimeridian and pole topology as predicates against explicit
[`Point`][gometry.Point] objects, including strict interior versus boundary
contact at ±180° and ±90°.

## Robust predicates

The foundational tests underneath every predicate — point orientation and
segment intersection — are evaluated with **adaptive-precision arithmetic**,
not raw `f64` cross products. A naive orientation test can report that three
nearly-collinear points turn left, turn right, or not at all depending on
evaluation order; a naive intersection can place a crossing on the wrong side
of an endpoint. Degenerate and near-degenerate input is the *normal* case in
real data, so the topological building blocks gometry relies on
(point-in-polygon, segment crossing, [DE-9IM](https://en.wikipedia.org/wiki/DE-9IM) `relate`) give the geometrically
correct answer even when points are collinear, coincident, or vanishingly
close.

```python exec="on" source="block" result="text"
import gometry as gm
square = gm.box(0.0, 0.0, 1.0, 1.0)
on_edge = gm.Point(0.5, 0.0)
print('contains (open):', gm.contains(square, on_edge))
print('covers (closed):', gm.covers(square, on_edge))
print('intersects:     ', gm.intersects(square, on_edge))

```

The `contains` vs. `covers` distinction (next section) is exactly the kind of
boundary subtlety robust predicates make trustworthy: both are decided
consistently, so the kernels built on them — overlay, `relate`, validation —
stay sound instead of emitting self-intersections or dropped slivers far
downstream.

!!! note "Predictable behavior under degeneracy"
    gometry guarantees *predictable* results on degenerate input rather than
    undefined ones. Empty geometries are representable — typed `POINT EMPTY` /
    `POLYGON EMPTY`, empty lines and multi-geometries, an empty
    `GeometryCollection` — and flow through predicates, metrics, overlay, and
    I/O rather than crashing. Boundary-touching cases resolve consistently
    through the robust predicates above. Constructive operations that cannot
    honestly produce a result raise a clear error instead of returning corrupt
    geometry.

## Gotcha 1: boundary points are *not* contained

This is the single most common predicate surprise, inherited from the
[OGC Simple Features](https://www.ogc.org/standard/sfa/) model. `contains`
requires the contained geometry to share the container's
**interior** — a point sitting exactly on an edge does not qualify. Use
[`covers`][gometry.covers] when you want the boundary-inclusive answer.

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(0, 0, 10, 10)
on_edge = gm.Point(0, 5)        # exactly on the left edge

print("contains:", gm.contains(region, on_edge))   # False!
print("covers:  ", gm.covers(region, on_edge))     # True
print("touches: ", gm.touches(region, on_edge))    # True

```

The strictest member of the family is
[`contains_properly`][gometry.contains_properly]: the candidate must lie
entirely in the interior, never touching the container's boundary
(DE-9IM `T**FF*FF*`). It is the right test when shared borders must *not*
count — and the fastest containment check on prepared geometries, because a
boundary-free answer needs no boundary noding.

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(0, 0, 10, 10)
inner = gm.box(2, 2, 8, 8)
flush = gm.box(0, 0, 5, 5)     # shares two edges with the region

print("contains:         ", gm.contains(region, flush))
print("contains_properly:", gm.contains_properly(region, flush))
print("properly inside:  ", gm.contains_properly(region, inner))

```

!!! warning "`contains` vs `covers`"
    Mnemonic: **`covers` is `contains` plus the boundary.** If your data sits
    on tile edges, snapped grids, or shared borders, `contains` will silently
    drop the boundary cases. Decide deliberately which you want.

## Gotcha 2: empty geometries

Operations can yield an **empty** geometry (e.g. the intersection of disjoint
shapes). Empties have **total** DE-9IM semantics — they are not "undefined":

| Pairing | Typical result |
|---|---|
| `equals(empty, empty)` | **True** (compatible empty kinds) |
| `disjoint(empty, anything)` incl. empty–empty | **True** |
| `contains` / `within` / `intersects` / `touches` / … | **False** |

```python exec="on" source="block" result="text"
import gometry as gm

empty = gm.intersection(gm.box(0, 0, 1, 1), gm.box(5, 5, 6, 6))
other = gm.from_wkt("POINT EMPTY")
print("is_empty:", empty.is_empty)
print("equals two empties:", gm.equals(empty, other))
print("disjoint two empties:", gm.disjoint(empty, other))
print("contains empty:", gm.contains(gm.box(0, 0, 1, 1), empty))

```

Branch on `geom.is_empty` only when *your domain* treats emptiness specially
(e.g. skip rows, substitute a sentinel). You do not need a pre-guard for the
predicate table itself — the totals above already hold.

## Predicates ignore Z/M; topology is XY

For truth values, planar predicates evaluate **X/Y topology** and ignore Z/M.
If you need ordinate-level identity, ask for it explicitly:

```python exec="on" source="block" result="text"
import gometry as gm
a = gm.Point(1.0, 2.0, z=10.0)
b = gm.Point(1.0, 2.0, z=99.0)
print('equals (XY topology):  ', gm.equals(a, b))
print('equals_exact (with Z): ', gm.equals_exact(a, b, include_z=True))
print('== (value identity):    ', a == b)

```

This keeps 2D topology predictable while still letting you compare full
coordinates when that is what you mean. See [geometry](geometry.md) for the
ordinate model.

## Gotcha 3: `equals` vs `equals_exact` vs `equals_identical` / `==`

[`equals`][gometry.equals] is a **topological** test: two geometries are equal
if they cover the same point set, regardless of vertex order or starting
point. [`equals_exact`][gometry.equals_exact] is a **coordinate** test: same
structure with every paired ordinate within `tolerance` (default `0.0`, i.e.
exact), in the **same order** — pass `tolerance=` for floating-point
comparison and `include_z`/`include_m` to select ordinates.
[`equals_identical`][gometry.equals_identical] (and scalar `==`) is full
**value identity** — kind, vertex order, active Z/M, **and** CRS/epoch — so
geometries are hashable; a frame difference is simply `False`, never a raise.

| Question | API | Vertex order | Z/M | CRS/epoch mismatch |
|---|---|---:|---:|---|
| Same spatial point set? | `gm.equals(a, b)` | Ignored | Ignored by 2D topology | Raises |
| Same coordinate sequence within a tolerance? | `gm.equals_exact(a, b, tolerance=...)` | Compared | Selected by `include_z`/`include_m` | Raises |
| Same complete Python value? | `gm.equals_identical(a, b)` / scalar `a == b` | Compared bit-for-bit | Compared | Returns `False` |

```python exec="on" source="block" result="text"
import gometry as gm
forward = gm.LineString([(0, 0), (1, 1), (2, 2)])
backward = gm.LineString([(2, 2), (1, 1), (0, 0)])
print('equals:           ', gm.equals(forward, backward))
print('equals_exact:     ', gm.equals_exact(forward, backward))
print('equals_identical: ', gm.equals_identical(forward, backward))
print('==:               ', forward == backward)

```

!!! note "Which one do you actually want?"
    Use `equals` for *"are these the same shape on the map?"* — the most
    forgiving and most expensive. Use `equals_exact` for round-trip tests where
    vertex order and Z/M values must match (or `tolerance=` for fuzzy
    floating-point comparison). Use `equals_identical` / `==` / `hash()` for
    dict keys, sets, and CRS-aware value identity (including dimensional
    empties). For a representation-independent vertex comparison, normalize
    first: `a.normalize().equals_exact(b.normalize())`.

## DE-9IM: the model under the hood

Every predicate above is a named shorthand for a pattern over the
**Dimensionally Extended 9-Intersection Model** (DE-9IM). It describes how the
*interior*, *boundary*, and *exterior* of two geometries intersect, as a 3×3
matrix flattened to a 9-character string. Each cell is `F` (no intersection),
`0`/`1`/`2` (intersection of that dimension), `T` (any intersection), or `*`
(don't care).

[`relate`][gometry.relate] returns the matrix; [`relate_pattern`][gometry.relate_pattern]
tests it against a mask.

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(0, 0, 10, 10)
probe = gm.Point(5, 5)

matrix = gm.relate(region, probe)
print("DE-9IM:", matrix)

# "T*****FF*" is exactly the definition of `contains`
print("matches contains pattern:", gm.relate_pattern(region, probe, "T*****FF*"))

```

Reach for `relate`/`relate_pattern` when the named predicates do not capture
the relationship you need — e.g. "boundaries touch but interiors are
disjoint, *and* both are 1-dimensional". You can read the matrix once and test
several patterns against it, which is cheaper than calling multiple named
predicates that each recompute the relationship.

!!! tip "Planar computation, strict metadata"
    Predicates never project or measure — they read ordinates as a flat plane.
    Do not infer that topology is invariant under arbitrary reprojection: a
    transform moves vertices and reconnects them with straight segments, and
    projection seams or invalid domains can change the realized shape. [`area`][gometry.Geometry.area] /
    [`length`][gometry.Geometry.length] are separate unary metrics: on a geographic CRS
    they are still geodesic m² / m. **Predicates** still require **matching CRS and
    coordinate-epoch metadata** on both operands; mixed tags are rejected before any
    DE-9IM work
    runs. On a geographic CRS, antimeridian-crossing input is **auto
    split-normalized** at the predicate chokepoint, so results are correct with
    no manual [`split_antimeridian`][gometry.Geometry.split_antimeridian] or local
    projection. Pole-enclosing rings are handled too.

## See also

- [Geometry](geometry.md) — types, the ordinate (Z/M) model, and inspection.
- [Validation & repair](validation.md) — detecting and fixing invalid geometry.
- [Constructive operations](constructive.md) — overlays that produce new geometry.
- [Spatial indexing & joins](indexing.md) — `query(..., predicate=...)` refine.
- Coming from Shapely? See [Migrating](../migrating/index.md#coming-from-shapely).
- [API: contains][gometry.contains] · [intersects][gometry.intersects] ·
  [relate][gometry.relate] · [dwithin][gometry.dwithin]
