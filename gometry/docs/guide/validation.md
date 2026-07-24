---
description: Validating, repairing, and contract-checking geometries in gometry — construction-time validity reports, require() fail-fast guards, repair() methods, precision/quantize, and remove_repeated_points.
---

# Validation & repair

Real-world geometry is messy: self-intersecting polygons, disconnected
interiors, non-finite coordinates, missing CRS, and coordinates carrying more
decimal digits than the data deserves. gometry treats reliability as a product
feature, and this page is the ingestion playbook: where the fail-fast line is
drawn, how to *detect* invalidity with `validate()`, how to *fix* it with
`repair()`, how to hold a boundary contract with the `require_*` guards, and how
to tame coordinate noise with `quantize` and `remove_repeated_points` — read it
top to bottom as one defensive-import pipeline.

## Construction validates structure, not topology

The first guardrail is the constructors themselves. They reject *structurally*
malformed input immediately — coordinates that are not finite, or rings with too
few vertices, have no valid interpretation, so they raise at the door:

```python exec="on" source="block" result="text"
import gometry as gm

for ring in ([(0, 0), (1, 0)], [(0, 0), (1, 0), (float("nan"), 1)]):
    try:
        gm.Polygon(ring)
    except gm.InvalidGeometryError as e:
        print("rejected:", e)

```

What constructors do **not** do is enforce [OGC](https://www.ogc.org/standard/sfa/) *topological* validity. A ring
that closes cleanly but self-intersects — a "bowtie" — is a well-formed
*structure* with an ill-formed *shape*, so it is accepted as an **invalid
geometry** rather than rejected:

```python exec="on" source="block" result="text"
import gometry as gm

bowtie = gm.Polygon([(0, 0), (2, 2), (2, 0), (0, 2)])
print("built:", bowtie.geometry_type, "| valid:", bowtie.validate().valid)   # False

```

That split is deliberate: topological validation performs nontrivial spatial
work whose cost depends on geometry structure, and you rarely
want to pay it on every constructor call. The same invalid geometry also arrives
from the outside — parsing foreign data with [`gm.from_wkt`][gometry.from_wkt],
[`gm.from_wkb`][gometry.from_wkb], or [`gm.from_geojson`][gometry.from_geojson],
which accept what they are given. Either way, you **detect** invalidity with
`validate()` and **fix** it with `repair()` — the rest of this page.

Cosmetic noise that does *not* change the shape is, by contrast, **valid**: a
repeated consecutive vertex or a clockwise shell describes a perfectly good
region — ring orientation does not affect validity — and `remove_repeated_points`
and `orient_polygons` clean it when you want a canonical form.

## Diagnosing: `is_valid` and `validate`

The quick check is the [`Geometry.is_valid`][gometry.Geometry.is_valid]
property (a plain `bool`); vectorized code uses
[`is_valid`][gometry.Geometry.is_valid] or the `GeometryArray.is_valid` mask. When
you need to know *why*, [`validate`][gometry.Geometry.validate]
returns a [`ValidationReport`][gometry.ValidationReport] without raising —
use it to branch on validity, log diagnostics, or repair only bad records
(`GeometryArray.validate()` yields one report per element).

```python exec="on" source="block" result="text"
import gometry as gm

bowtie = gm.Polygon([(0, 0), (2, 2), (2, 0), (0, 2)])

try:
    gm.require(bowtie)
except gm.InvalidGeometryError as e:
    print("invalid:", e)        # 'exterior ring has a self-intersection'

report = (gm.box(0, 0, 1, 1)).validate()
print("valid box ->", report.valid)

```

The returned [`ValidationReport`][gometry.ValidationReport] exposes `.valid`,
`.reason` (a human-readable message that names the actual fault — e.g. a
self-intersection in the exterior ring), `.location` (the `(x, y)` coordinate of
the first problem), and `.path` (a structural path to it, e.g. `'$.shell'`). For
an OGC-valid geometry `.reason`, `.location`, and `.path` are all `None`; they
carry detail when topology validation finds a problem — turning a "geometry is
invalid" mystery into a fixable one.

Geographic validity follows the same frame-aware topology as predicates. When
a geometry has a geographic CRS and crosses the antimeridian, gometry validates
its seam-normalized shape; this includes pole-enclosing shells and polar holes.
The same coordinates without a CRS, or under a projected CRS, remain ordinary
planar coordinates. This distinction is intentional: attaching a geographic
frame changes what ±180° means, while a planar frame has no identified seam.
`is_valid`, `is_simple`, `is_ring`, `validate`, `require`, and
`self_intersections` all share that rule. `repair` uses the same normalized
validity verdict before rebuilding, and `snap_to_grid(..., repair=True)` checks
each snapped result in that same frame.

!!! note "Use the check that matches the invariant"
    Returning `valid=False` invites callers to forget the check and march on
    with corrupt geometry. Raising forces the decision at the point of entry:
    catch it, repair, and continue — or let it abort the bad batch. If you only
    need a report without raising, use `validate()`. `is_simple` and `is_ring`
    answer narrower topological questions; neither is a general structural
    sanity check or a replacement for polygon validity.

## Fixing: `repair`

[`repair`][gometry.Geometry.repair] returns a valid copy. It
offers two strategies via `method`:

| `method` | Strategy |
|----------|----------|
| `'linework'` (default) | re-node the boundary **linework** and reassemble — boundary-faithful |
| `'structure'` | rebuild valid polygonal **areas** from the input — area-faithful |

The bowtie repairs into a valid geometry:

```python exec="on" source="block" result="text"
import gometry as gm

bowtie = gm.Polygon([(0, 0), (2, 2), (2, 0), (0, 2)])

fixed = bowtie.repair()                       # method='linework'
print(fixed.geometry_type, "valid:", fixed.validate().valid)

fixed_struct = bowtie.repair(method="structure")
print(fixed_struct.geometry_type, "valid:", fixed_struct.validate().valid)

```

Drawing each vertex as a dot makes the fix concrete: the bow-tie's four corners
gain a fifth vertex at the self-intersection, where `repair` splits the single
twisted ring into two clean triangles. The invalid input renders **red**, the
repaired result **green** (left → right):

```python exec="on" html="true"
from _figures import before_after, with_vertices
import gometry as gm

bowtie = gm.Polygon([(0, 0), (2, 2), (2, 0), (0, 2)])
print(before_after(with_vertices(bowtie), with_vertices(bowtie.repair()),
                   before_caption="invalid input", after_caption="repair(bowtie)"))

```

The two strategies genuinely differ once regions overlap: `linework` folds
by even-odd parity (a doubly-covered overlap cancels), `structure` unions the
enclosed areas:

```python exec="on" source="block" result="text"
import gometry as gm

overlap = gm.from_wkt(
    "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0)), ((2 2, 6 2, 6 6, 2 6, 2 2)))"
)
print("linework: ", overlap.repair().area)                      # XOR: 24
print("structure:", overlap.repair(method="structure").area)  # union: 28

```

Repair is cheap to call defensively: already-valid input is returned as-is at
validation cost, on every surface — `geom.repair()`,
`array.repair()` (valid rows are reused), and `report.repair()`. Z/M
ordinates are carried through the rebuild, and the output is deterministic
byte-for-byte run to run.

For a geographic antimeridian crossing, the validity check runs before any
rebuild. A valid input therefore keeps its original coordinates exactly; an
actually invalid crossing is repaired from the seam-split topology. The same
rule powers `snap_to_grid(..., repair=True)`.

!!! warning "Repair changes geometry — validate the result, don't trust blindly"
    `repair` can split, merge, or drop parts to reach validity. Re-`validate`
    the output if correctness matters, and prefer fixing data at its source.
    The two methods can yield different results on the same input; pick the one
    whose invariant (boundary vs area) matches your domain.

## Enforcing contracts at the boundary

Beyond OGC validity, you often need to assert that a geometry has a CRS, is
valid, or has a particular dimensionality before trusting it.
[`gm.require`][gometry.require] is the single boundary API: it accepts an
existing geometry or an external geometry-like object, returns the geometry
unchanged on success, and raises the matching specific exception otherwise —
[`CRSMismatchError`][gometry.CRSMismatchError] for a frame contract,
[`InvalidGeometryError`][gometry.InvalidGeometryError] for validity and axes — ideal at
the top of a function (design-by-contract).

```python exec="on" source="block" result="text"
import gometry as gm

def geodesic_area_km2(poly):
    poly = gm.require(poly, crs=4326)       # parse + valid + CRS contract
    return poly.area / 1e6                  # geographic CRS -> geodesic m^2

ok = gm.box(2, 48, 3, 49, crs=4326)
print(f"{geodesic_area_km2(ok):.1f} km^2")

try:
    geodesic_area_km2(gm.box(2, 48, 3, 49))   # no CRS
except gm.CRSMismatchError as e:
    print("rejected:", e)

```

The optional `crs=` and `axes=` keywords add frame and storage contracts to
that same validation call:

```python exec="on" source="block" result="text"
import gometry as gm

# Untrusted WKT at the boundary: parse + CRS + axes in one call
raw = "POLYGON ((2 48, 3 48, 3 49, 2 49, 2 48))"
area = gm.require(raw, crs=4326, axes="XY")
print("meets contract:", area.crs == 4326, area.coordinate_axes)

```

This fixes the classic pitfall where a validator checks only X/Y topology but
accidentally lets Z/M through into a nominally 2D storage path. Validate
untrusted input once at the edge, then trust the invariant internally — no need
to re-check the same geometry deep in your call stack.

!!! tip "Fail fast on programmer error, repair on data error"
    Treat *invalid input data* as something to validate and repair at the
    boundary. Treat *internal misuse* — the wrong type, an impossible
    argument — as a bug that should raise immediately. gometry's APIs validate
    untrusted input and then trust internal invariants, so you get clear errors
    at the edge and fast paths in the core.

## Cleaning: `quantize` and `remove_repeated_points`

Floating-point coordinates carry more digits than most data deserves, and those
extra digits are where snapping and overlay instability hide. Two targeted
cleaners handle the most common coordinate noise without a full repair.

`geom.quantize(precision)` rounds every coordinate to `precision` decimal
places, giving a controlled precision model — invaluable for shrinking precision
so coordinates from different sources align, for stabilizing robust overlays,
and for making equality checks, deduplication, and interchange reproducible.

```python exec="on" source="block" result="text"
import gometry as gm
noisy = gm.LineString([(1.11119, 2.22229), (3.33339, 4.44449)])
print((noisy.quantize(2)).to_wkt())

```

Snapped coordinates are easier to see when each vertex is drawn as a dot:

```python exec="on" html="true"
from _figures import before_after, with_vertices
import gometry as gm
noisy = gm.LineString([(0, 0), (1.111, 2.222), (3.333, 4.444)])
cleaned = (noisy.remove_repeated_points()).quantize(2)
print(before_after(with_vertices(noisy), with_vertices(cleaned), before_caption='noisy input', after_caption='quantize(2)'))

```

[`remove_repeated_points`][gometry.Geometry.remove_repeated_points] drops consecutive
duplicate vertices. The optional `tolerance` also collapses vertices closer than
that distance — handy for thinning oversampled tracks.

```python exec="on" source="block" result="text"
import gometry as gm
dup = gm.LineString([(0, 0), (0, 0), (1, 1), (1, 1), (2, 2)])
print(dup.remove_repeated_points())

```

Dropped vertices are easier to see when each coordinate is drawn as a dot:

```python exec="on" html="true"
from _figures import before_after, with_vertices
import gometry as gm

dup = gm.LineString([(0, 0), (0, 0), (1, 1), (1, 1), (2, 2)])
cleaned = dup.remove_repeated_points()
print(before_after(with_vertices(dup), with_vertices(cleaned),
                   before_caption="input", after_caption="remove_repeated_points"))

```

The related tools `snap` and `normalize` handle the other common sources of
geometric noise (near-misses that should coincide, and non-canonical vertex
order / ring orientation).

!!! tip "Precision workflow before overlay"
    Overlay operations are sensitive to near-coincident vertices. An explicit
    `quantize` / `snap_to_grid` pass can help sources align — but it can also
    *create* invalidity on tight geometries (see [choosing](choosing.md)). Prefer
    a deliberate precision workflow followed by `validate` / `repair` rather than
    treating quantize as a universal sliver cure — see
    [constructive operations](constructive.md#dissolving-many-geometries-union_all).

## A typical ingest pipeline

Putting it together, a defensive import that turns the fail-fast raise into a
repair-and-continue combines a dimension contract, validity checking, and
repair:

```python exec="on" source="block" result="text"
import gometry as gm

def ingest(payload):
    # 1) Parse foreign text first — ParseError is distinct from validity repair
    try:
        geom = gm.from_wkt(payload, crs=4326)
    except gm.ParseError:
        raise
    geom = geom.remove_repeated_points()
    if not geom.is_valid:
        geom = geom.repair()
    return gm.require(geom, crs=4326, axes="XY")  # contract after repair

result = ingest("POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))")  # self-intersecting
print(result.geometry_type, "valid:", result.validate().valid)

```

Parse failures (`ParseError`) stay separate from geometric invalidity
(`InvalidGeometryError` / repair). The detect → repair → assert flow keeps invalid
geometry from ever reaching your [predicates](predicates.md),
[overlays](constructive.md), or [measurements](crs.md), where it would otherwise
produce subtly wrong answers. See [Security](../about/security.md) for the
no-panic posture on malformed bytes.

Geometry that **crosses the ±180° antimeridian** is a separate kind of "broken":
individually valid coordinates that stored naively draw a band the wrong way
around the globe. gometry's topology ops auto-split crossing geographic input;
see [Working across the antimeridian](crs.md#across-the-antimeridian) for when
you must call [`split_antimeridian`][gometry.Geometry.split_antimeridian] yourself.

## See also

- [Predicates](predicates.md) — robust boundary semantics on validated input.
- [Geometry](geometry.md) — the Z/M ordinate model and storage contracts.
- [CRS, units & measurement](crs.md#across-the-antimeridian) — reprojection and the antimeridian.
- [Security](../about/security.md) — untrusted formats and no-panic posture.
- [API: Geometry.validate][gometry.Geometry.validate] ·
  [Geometry.repair][gometry.Geometry.repair] · [require][gometry.require]
