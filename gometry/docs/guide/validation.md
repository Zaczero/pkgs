---
description: Validate and repair gometry geometries, enforce CRS and axes contracts, and remove repeated points or excess coordinate precision.
---

# Validation & repair

Real-world geometry includes self-intersections, disconnected interiors,
non-finite coordinates, missing CRS metadata, and excess coordinate precision.

## Construction validates structure, not topology

Constructors reject *structurally* malformed input immediately. Non-finite
coordinates and rings with too few vertices have no valid interpretation and
raise [`InvalidGeometryError`][gometry.InvalidGeometryError]:

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
print("built:", bowtie.geometry_type, "| valid:", bowtie.validate().valid)

```

Constructors and parsers can admit structurally valid but topologically invalid
geometry. Foreign data can arrive through [`gm.from_wkt`][gometry.from_wkt],
[`gm.from_wkb`][gometry.from_wkb], or [`gm.from_geojson`][gometry.from_geojson].
Either way, you **detect** invalidity with
[`validate()`][gometry.Geometry.validate] and **fix** it with [`repair()`][gometry.Geometry.repair].

Cosmetic noise that does *not* change the shape is, by contrast, **valid**: a
repeated consecutive vertex does not itself invalidate a line with at least two
distinct XY positions or a ring with at least three; a clockwise shell is also
valid because ring orientation does not affect validity. A nonempty line whose
coordinates all share one XY position is invalid;
[`repair()`][gometry.Geometry.repair] converts it to a Point.
[`remove_repeated_points`][gometry.Geometry.remove_repeated_points] and
[`orient_polygons`][gometry.Geometry.orient_polygons] can canonicalize valid
redundancy when required.

## Diagnosing: `is_valid` and `validate`

The [`Geometry.is_valid`][gometry.Geometry.is_valid] property returns a plain
`bool`; vectorized code uses the [`GeometryArray.is_valid`][gometry.GeometryArray.is_valid] mask.
`validate` returns a
[`ValidationReport`][gometry.ValidationReport] without raising. The report
supports branching on validity, logging diagnostics, and repairing only bad records
([`GeometryArray.validate()`][gometry.GeometryArray.validate] yields one report per element).

```python exec="on" source="block" result="text"
import gometry as gm

bowtie = gm.Polygon([(0, 0), (2, 2), (2, 0), (0, 2)])

try:
    gm.require(bowtie)
except gm.InvalidGeometryError as e:
    print("invalid:", e)

report = (gm.box(0, 0, 1, 1)).validate()
print("valid box →", report.valid)

```

The returned `ValidationReport` exposes [`.valid`][gometry.ValidationReport.valid],
[`.reason`][gometry.ValidationReport.reason] (a human-readable message that names the actual fault — e.g. a
self-intersection in the exterior ring), [`.location`][gometry.ValidationReport.location] (the `(x, y)` coordinate of
the first problem), [`.path`][gometry.ValidationReport.path] (a structural path to it, e.g. `'$.shell'`),
and the stable [`.code`][gometry.ValidationReport.code] and human-readable
[`.detail`][gometry.ValidationReport.detail] fields. For
an OGC-valid geometry `.reason`, `.location`, `.path`, `.code`, and `.detail`
are all `None`; they carry detail when topology validation finds a problem.

Geographic validity follows the same frame-aware topology as predicates. Stored
coordinates in one certified open longitude chart keep planar OGC validity;
seam or pole cases and other alias spellings read exact quotient incidence
rather than a shifted coordinate copy. Pole-enclosing
shells and polar holes are included. Opposite-interior seam spans are a chart
cut regardless of whether the geometry uses east or west seam spelling;
same-side overlaps remain invalid. Geographic coordinate admission also
participates in validity:
an out-of-range latitude reports `latitude_out_of_range`, and a once-winding
unmarked ring whose latitude range does not identify one pole reports
`ambiguous_full_turn_ring`. `is_valid` returns `False` for either issue without
raising, and `GeometryArray.is_valid` writes `False` for the affected row.
`validate` reports the issue without raising. Geographic operations still raise
`InvalidGeometryError` when their input cannot be admitted, and `repair` raises
`RepairFailed` because an out-of-domain coordinate cannot be reconstructed.
For quotient-topology issues, the witness is a canonical coordinate of the
issue's contact set, chosen deterministically by minimum latitude then longitude.
The same coordinates without a CRS, or under a projected CRS, remain ordinary
planar coordinates. Attaching a geographic frame supplies its periodic longitude
seam and pole, while a planar frame has no identified seam.
`is_valid`, [`is_simple`][gometry.Geometry.is_simple], [`is_ring`][gometry.Geometry.is_ring], `validate`, [`require`][gometry.require], and
[`self_intersections`][gometry.Geometry.self_intersections] all use the same
geographic frame. `repair` validates the quotient result before returning it,
and [`snap_to_grid(..., repair=True)`][gometry.Geometry.snap_to_grid] checks
each snapped result in that same frame.

The latitude domain issue retains its source ring path and coordinate witness:

```python exec="on" source="block" result="text"
import gometry as gm

polygon = gm.Polygon([(0, 0), (10, 0), (10, 100), (0, 0)], crs=4326)
report = polygon.validate()
print(report.code, report.path, report.location)
print("valid:", polygon.is_valid)

```

!!! note "Validation reports and boundary errors"
    `validate()` returns a report without raising. `require()` raises the
    matching domain exception when its validity, CRS, or axes contract fails.
    `is_simple` and `is_ring` answer narrower topological questions; neither is
    a general structural check or a replacement for polygon validity.

## Fixing: `repair`

When repair succeeds, it returns a valid geometry; it can refuse input it
cannot reconstruct. It offers two strategies via `method`:

| `method` | Strategy |
|----------|----------|
| `'linework'` (default) | re-node the boundary **linework** and reassemble — boundary-faithful |
| `'structure'` | rebuild valid polygonal **areas** from the input — area-faithful |

The bowtie repairs into a valid geometry:

```python exec="on" source="block" result="text"
import gometry as gm

bowtie = gm.Polygon([(0, 0), (2, 2), (2, 0), (0, 2)])

fixed = bowtie.repair()
print(fixed.geometry_type, "valid:", fixed.validate().valid)

fixed_struct = bowtie.repair(method="structure")
print(fixed_struct.geometry_type, "valid:", fixed_struct.validate().valid)

```

```python exec="on" html="true"
from _figures import before_after, with_vertices
import gometry as gm

bowtie = gm.Polygon([(0, 0), (2, 2), (2, 0), (0, 2)])
print(before_after(with_vertices(bowtie), with_vertices(bowtie.repair()),
                   before_caption="invalid parcel", after_caption="repaired parcel pieces",
                   before_alt="The parcel boundary crosses itself at the center, making the bowtie polygon invalid.",
                   after_alt="Repair splits the self-crossing bowtie into valid polygonal pieces."))

```

The two strategies genuinely differ once regions overlap: `linework` folds
by even-odd parity (a doubly-covered overlap cancels), `structure` unions the
enclosed areas:

```python exec="on" source="block" result="text"
import gometry as gm

overlap = gm.from_wkt(
    "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0)), ((2 2, 6 2, 6 6, 2 6, 2 2)))"
)
print("linework:", overlap.repair().area)
print("structure:", overlap.repair(method="structure").area)

```

A collapsed line repairs to its first coordinate as a Point. Collapsed
MultiLineString members become Point or MultiPoint values; surviving linework
is retained alongside them in a GeometryCollection:

```python exec="on" source="block" result="text"
import gometry as gm

collapsed = gm.from_wkt("LINESTRING (2 3, 2 3)")
print(collapsed.is_valid, collapsed.validate().code, collapsed.repair().to_wkt())

mixed = gm.from_wkt("MULTILINESTRING ((0 0, 0 0), (1 1, 2 2))")
print(mixed.repair().to_wkt())

```

Already-valid input is returned as-is after the validation check on every surface:
`geom.repair()` and `array.repair()` reuse valid rows. Z/M ordinates are carried
through the rebuild.

For a geographic periodic-seam crossing, the validity check runs before any
rebuild. A valid input therefore keeps its original coordinates exactly; an
invalid crossing is repaired from the seam-split topology. The same
rule powers `snap_to_grid(..., repair=True)`.

!!! note "Repair output and revalidation"
    `repair` can split, merge, or drop parts to reach validity. Re-run
    `validate()` on the output when downstream correctness depends on it. The
    two methods can yield different results on the same input; choose the one
    whose invariant (boundary versus area) matches the domain.

## Enforcing contracts at the boundary

Beyond OGC validity, a boundary can require a CRS, valid topology, or a
particular dimensionality.
`gm.require` is the single boundary API: it accepts an
existing geometry or an external geometry-like object, returns the geometry
with its coordinate values unchanged on success, and raises the matching
specific exception otherwise. With `crs=`, it stamps CRS-free coordinates,
relabels an operationally equivalent existing CRS to the requested spelling,
and rejects a conflicting frame with [`CRSMismatchError`][gometry.CRSMismatchError].
When `crs` is omitted or `None`, an existing CRS is preserved. Coordinate
epochs are preserved and revalidated against the resulting CRS.

The `crs=` parameter uses the `CrsInput` contract documented by
[`CRS`][gometry.CRS]. Accepted forms include an EPSG integer, an authority,
WKT, or PROJ string, an `(authority, code)` pair, a PROJJSON or CF mapping,
and an object exposing `to_wkt()`, such as `gometry.CRS` or `pyproj.CRS`.
Conflicting legacy CRS metadata in GeoJSON raises [`ParseError`][gometry.ParseError];
native geometries and EWKT/EWKB embedded SRIDs raise
[`CRSMismatchError`][gometry.CRSMismatchError].

`InvalidGeometryError` remains the exception for invalid geometry or mismatched
axes.

```python exec="on" source="block" result="text"
import gometry as gm

def ellipsoidal_area_km2(poly):
    poly = gm.require(poly, crs=4326)
    return poly.area / 1e6

ok = gm.box(2, 48, 3, 49, crs=4326)
print(f"{ellipsoidal_area_km2(ok):.1f} km²")

try:
    ellipsoidal_area_km2(gm.box(2, 48, 3, 49))
except gm.CRSMismatchError as e:
    print("rejected:", e)

```

The optional `crs=` and `axes=` keywords add frame and storage contracts to
that same validation call:

```python exec="on" source="block" result="text"
import gometry as gm

raw = "POLYGON ((2 48, 3 48, 3 49, 2 49, 2 48))"
area = gm.require(raw, crs=4326, axes="XY")
print("meets contract:", area.crs == gm.CRS(4326), area.coordinate_axes)

```

The `axes=` argument prevents a validator from accepting Z/M into a nominally 2D
storage path. Validate untrusted input at the boundary, then consume that
invariant internally without repeating the boundary check.

!!! note "Ingress and programmer-error behavior"
    `gm.require` parses or validates external geometry-like input at the
    boundary. Invalid geometry data follows the validation or explicit repair
    path. Wrong types and impossible argument values raise immediately. After
    the boundary, native operations rely on the established type and validity
    invariants.

## Cleaning: `quantize` and `remove_repeated_points`

Floating-point coordinates can carry more digits than the data requires, affecting
snapping and overlay results. Two targeted cleaners handle common coordinate noise
without a full repair.

[`geom.quantize(precision)`][gometry.Geometry.quantize] rounds every coordinate to `precision` decimal
places. It gives coordinates from different sources a common precision for
overlay, equality, deduplication, and interchange.

```python exec="on" source="block" result="text"
import gometry as gm
noisy = gm.LineString([(1.11119, 2.22229), (3.33339, 4.44449)])
print((noisy.quantize(2)).to_wkt())

```

```python exec="on" html="true"
from _figures import before_after, with_vertices
import gometry as gm
noisy = gm.LineString([(0, 0), (1.111, 2.222), (3.333, 4.444)])
cleaned = (noisy.remove_repeated_points()).quantize(2)
print(before_after(
    with_vertices(noisy), with_vertices(cleaned),
    before_caption="survey track coordinates", after_caption="coordinates rounded to 2 decimals",
    before_alt="The CRS-free survey track contains vertices with three decimal places.",
    after_alt="Quantization moves each track vertex to two-decimal coordinates without removing vertices.",
))

```

`remove_repeated_points` drops consecutive
duplicate vertices. The optional `tolerance` also collapses vertices closer than
that distance, allowing oversampled tracks to be thinned.

```python exec="on" source="block" result="text"
import gometry as gm
dup = gm.LineString([(0, 0), (0, 0), (1, 1), (1, 1), (2, 2)])
print(dup.remove_repeated_points().to_wkt())

```

```python exec="on" html="true"
from _figures import before_after, with_vertices
import gometry as gm

dup = gm.LineString([(0, 0), (0, 0), (1, 1), (1, 1), (2, 2)])
cleaned = dup.remove_repeated_points()
print(before_after(with_vertices(dup), with_vertices(cleaned),
                   before_caption="track with repeated vertices", after_caption="consecutive repeats removed",
                   before_alt="The five-coordinate track repeats its first and middle positions consecutively.",
                   after_alt="Removing consecutive repeats leaves the same line path represented by three distinct vertices."))

```

The related tools [`snap`][gometry.snap] and [`normalize`][gometry.Geometry.normalize] handle the other common sources of
geometric noise (near-misses that should coincide, and non-canonical vertex
order / ring orientation).

!!! tip "Precision workflow before overlay"
    Overlay operations are sensitive to near-coincident vertices. An explicit
    `quantize` / `snap_to_grid` pass can help sources align — but it can also
    *create* invalidity on tight geometries (see [choosing](choosing.md)). Prefer
    a precision workflow followed by `validate` / `repair` rather than
    treating quantize as a universal sliver cure — see
    [constructive operations](constructive.md#dissolving-many-geometries-union_all).

## A typical ingest pipeline

An ingest pipeline can combine a dimension contract, validity checking, and
repair:

```python exec="on" source="block" result="text"
import gometry as gm

def ingest(payload):
    try:
        geom = gm.from_wkt(payload, crs=4326)
    except gm.ParseError:
        raise
    geom = geom.remove_repeated_points()
    if not geom.is_valid:
        geom = geom.repair()
    return gm.require(geom, crs=4326, axes="XY")

result = ingest("POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))")
print(result.geometry_type, "valid:", result.validate().valid)

```

Parse failures ([`ParseError`][gometry.ParseError]) stay separate from geometric invalidity
(`InvalidGeometryError` / repair). The detect → repair → assert flow keeps invalid
geometry from ever reaching your [predicates](predicates.md),
[overlays](constructive.md), or [measurements](crs.md), where it would otherwise
produce subtly wrong answers. See [Security](../about/security.md) for the
no-panic posture on malformed bytes.

Geometry that crosses a geographic frame's periodic seam (±180° for EPSG:4326)
is a separate kind of "broken":
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
