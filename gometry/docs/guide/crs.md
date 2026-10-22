---
description: CRS declaration and transformation in gometry — units for area, length, distance, and buffers, projected CRS choice, epochs, and antimeridian behavior.
---

# CRS, units & measurement

A coordinate reference system (CRS) says what your numbers
*mean*: whether `(21.0, 52.0)` is "21 degrees east, 52 degrees north on the
[WGS 84](https://epsg.org/crs_4326/WGS-84.html) ellipsoid" or "21 meters east of
some local origin." The CRS is also gometry's metric selector: by default,
geographic shape area, length, and linear referencing use the ellipsoid along
straight longitude/latitude chart edges, while point-to-point distances follow
geodesics. `unit='planar'` explicitly selects Cartesian measurement in stored
coordinates. Projected CRSs use native linear units and their squared area
units, and CRS-free geometries use raw coordinate units.
Geographic coordinates themselves remain in the CRS-native angular units, which
may be degrees, radians, grads, or another admitted angular unit.

CRS assignment and transformation have different effects:

- **[`with_crs`][gometry.Geometry.with_crs] *declares*** what existing coordinates
  already mean. It never moves a coordinate.
- **[`to_crs`][gometry.Geometry.to_crs] *transforms*** coordinates from one CRS
  into another through the selected operation. See [Z and M](#z-and-m-under-transformation)
  for how extra ordinates are handled.

<a id="set-crs-vs-to-crs"></a>

## `with_crs` vs `to_crs`

For a point supplied as longitude and latitude without CRS metadata,
`with_crs(4326)` assigns WGS 84 without changing the numbers.
`to_crs(3857)` then transforms those coordinates to Web Mercator:

```python exec="on" source="block" result="text" session="declare-transform"
from math import isclose

import gometry as gm

raw = gm.Point(21.0, 52.0)
print('crs before:', raw.crs)

declared = raw.with_crs(4326)
print('crs after with_crs:', declared.crs)
print('coords after with_crs:', list(declared.coords))
assert declared.coords.to_nested() == raw.coords.to_nested() == [21.0, 52.0]

projected = declared.to_crs(3857)
print('crs after to_crs:', projected.crs)
print('coords after to_crs:', list(projected.coords))
assert isclose(projected.x, 2_337_709.307, rel_tol=0.0, abs_tol=0.001)
assert isclose(projected.y, 6_800_125.454, rel_tol=0.0, abs_tol=0.001)

```

Both geometries describe the same location. The first stores degrees; the
second stores metres. Assign a CRS only when it describes the coordinates
already present.

## Operation families

A geometry's *frame* is its CRS plus an optional coordinate epoch. Geographic
measurements use ellipsoidal arclength of retained chart-straight source edges
and ellipsoidal area bounded by those edges, while point-to-point distances use
geodesics. Topology remains a coordinate-plane X/Y operation: in a geographic
CRS it resolves longitude aliases, seam spellings, and pole longitudes in the
CRS-native frame before construction. A seam split uses straight edges in an
unwrapped longitude/latitude chart; it does not construct geodesic edges. Metric
constructive operations use CRS-native distance inputs but can construct in a
local projection. Coordinate edits operate on stored coordinates; planar
constructive operations use stored coordinates or a method-specific geographic
chart, as described below.

| Family | Examples | Frame / unit / seam behavior |
|---|---|---|
| **CRS-aware measures** | [`area`][gometry.area], [`length`][gometry.length], [`distance`][gometry.distance], [`dwithin`][gometry.dwithin], [`hausdorff_distance`][gometry.hausdorff_distance], LRS | Geographic area, length, and LRS measure straight chart edges on the ellipsoid. Point-to-point distance uses a geodesic; shape distance, `dwithin`, `nearest_points`, and `shortest_line` minimize ellipsoidal geodesic separation over closed chart-edge supports, including filled polygons. Geographic Hausdorff ranges continuously over points and linework only. `unit='planar'` selects Cartesian measurement in stored coordinates. |
| **Metric constructive** | [`buffer`][gometry.Geometry.buffer], [`offset_curve`][gometry.Geometry.offset_curve] | Distance *inputs* use CRS-natural units (and may use a local-projection construction on a geographic CRS); shapes are **coordinate-planar** — **not** antimeridian auto-split |
| **Topology** | predicates, overlay, [`relate`][gometry.relate] | strict frame match; geographic topology resolves longitude aliases and seam and pole equivalence in the CRS-native frame; seam-crossing inputs are split into straight chart pieces in an unwrapped longitude/latitude plane. Geographic rectangle parallels match [`box`][gometry.box]. |
| **Coordinate refinement** | [`segmentize(max_length)`][gometry.Geometry.segmentize], [`line_interpolate`][gometry.Geometry.line_interpolate] | `segmentize` follows stored geographic edges by default; `path=` selects geodesic or rhumb routing. With CRS-aware units, geographic LRS inverts ellipsoidal arclength of the retained chart-straight source edge and retains original vertices. |
| **Coordinate edit** | [`quantize`][gometry.Geometry.quantize], [`snap_to_grid`][gometry.Geometry.snap_to_grid], affine | raw coordinate space; no metric conversion |
| **Planar constructive** | [`convex_hull`][gometry.Geometry.convex_hull], [`concave_hull`][gometry.Geometry.concave_hull], [`simplify`][gometry.Geometry.simplify], [`triangulate`][gometry.Geometry.triangulate], [`voronoi`][gometry.Geometry.voronoi] | Planar or local-projection construction; see [constructive operations](constructive.md#tessellation-triangulate-voronoi-polygonize) for geographic tessellation behavior. |

`buffer`/`offset_curve` are *not* exact geodesic buffers: on a geographic CRS the
distance is metres, but the produced outline is a local-projection approximation
(see [Across the antimeridian](#across-the-antimeridian)).

## Frame rules

- **Everything requires one frame — but a frame, not a spelling.** Mixing CRSs —
  or mixing epochs on the same CRS — raises
  [`CRSMismatchError`][gometry.CRSMismatchError] up front; nothing is silently
  reinterpreted. Reproject ([`to_crs`][gometry.Geometry.to_crs]) or relabel ([`with_crs`][gometry.Geometry.with_crs], if the metadata is
  what's wrong) first. Two labels that name the *same* frame and differ only in
  axis order — `EPSG:4326` and `OGC:CRS84`, or a projected CRS in N/E versus E/N
  — are accepted everywhere: predicates, measures, overlays, and equally when
  building a [`GeometryArray`][gometry.GeometryArray], concatenating, filling missing rows, or inserting
  into a [`SpatialIndex`][gometry.SpatialIndex]. The result keeps the left operand's (or the receiver's,
  or the first item's) label. What is *not* accepted is a genuine difference:
  `EPSG:2180` and `EPSG:2177` share datum, ellipsoid and units yet place the
  same coordinate about 3000 km apart, so they still raise.
- **Derived geometries inherit the input frame** — every constructive result
  ([`buffer`][gometry.Geometry.buffer], [`centroid`][gometry.Geometry.centroid], [`simplify`][gometry.Geometry.simplify], overlays, …) carries the CRS *and* epoch of
  its inputs, so metadata never silently drops out of a pipeline.
- **`unit=None` means the CRS-natural unit**, everywhere a `unit=` parameter
  exists: geographic shape area and length use ellipsoidal chart-edge measures,
  geographic LRS uses the ellipsoidal arclength of the retained
  chart-straight source edge, geographic point distances use
  geodesics, projected measurements use native linear units, and CRS-free
  measurements use raw coordinate units (area results square the corresponding
  unit). `unit='planar'` uses Cartesian coordinate space, with geographic
  coordinates in their native angular units.
  `unit='meters'` uses the CRS-aware metric and requires a CRS.
- **`to_crs` transforms X/Y; Z and M follow the [ordinate rules](#z-and-m-under-transformation)** — Z is
  carried unchanged through horizontal projections and transformed when the
  pipeline consumes it; M is always carried unchanged. Serialization
  constraints are separate: [`to_geojson`][gometry.Geometry.to_geojson] always raises on M because GeoJSON
  has no M slot.
- **Epoch requires a CRS** (`epoch ⟹ crs`): CRS-free constructors reject
  `epoch=`, and clearing the CRS clears the epoch. See
  [Coordinate epochs](#coordinate-epochs).

### Native geographic units and periodic aliases

The geographic frame is read from the CRS, not assumed to be WGS 84 degrees. It
owns the longitude and latitude angular units, the longitude period and pole, and
the prime meridian. For example, EPSG:4326 uses degrees, EPSG:4807 uses grads,
and a CRS expressed with radian axes uses radians. A full native longitude turn
therefore has a different numeric spelling in each frame, but the aliases denote
the same location in topology, metrics, arrays, prepared geometries, indexes, and
grid-source transforms.

```python exec="on" source="block" result="text"
import math

import gometry as gm

radian_crs = (
    'GEOGCS["WGS 84 radians",DATUM["WGS_1984",'
    'SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],UNIT["radian",1],'
    'AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]'
)
cases = (
    ("degree", 4326, 360.0),
    ("grad", 4807, 400.0),
    ("radian", radian_crs, math.tau),
)

for name, crs, period in cases:
    origin = gm.Point(0.0, 0.0, crs=crs)
    alias = gm.Point(period, 0.0, crs=crs)
    units = [axis["unit_name"] for axis in gm.CRS(crs).axes[:2]]
    print(f"{name}: {units}, one turn distance: {gm.distance(origin, alias)} m")
    assert gm.distance(origin, alias) == 0.0
    assert gm.distance(origin, gm.GeometryArray([alias])).tolist() == [0.0]

```

The native values are admitted and interpreted in their own frame. The
geodesic kernels receive a normalized degree/Greenwich value only at the metric
boundary; callers do not convert geometry coordinates before using `area`,
`length`, `distance`, `dwithin`, or exact geographic topology. A CRS object keeps
the normalized input text as [`CRS.definition`][gometry.CRS.definition]; an
authority ID discovered inside WKT or PROJJSON is metadata, not a replacement
for the authored definition.

The prime meridian is part of the same frame. EPSG:4807's 400-grad alias reaches
the same physical location as its zero-grad spelling when transformed to WGS 84:

```python exec="on" source="block" result="text"
import math

import gometry as gm

origin = gm.Point(0.0, 0.0, crs=4807).to_crs(4326)
alias = gm.Point(400.0, 0.0, crs=4807).to_crs(4326)
same_location = math.isclose(origin.x, alias.x, abs_tol=1e-12) and math.isclose(
    origin.y, alias.y, abs_tol=1e-12
)
print("same WGS 84 location:", same_location)
assert same_location

```

The raw-coordinate [`CRS.factors`][gometry.CRS.factors] retains its documented
degree/radian argument switch; the native-frame rule above describes geometry
metrics and topology.

### CRS inputs

Constructors, metadata assignment, transforms, and CRS exporters accept the same
CRS-like inputs: authority, WKT, or PROJ strings; non-boolean integer-like EPSG
codes; two-item authority/code pairs; PROJJSON or CF mappings; and objects that
provide CRS conversion methods such as `to_authority()`, `to_epsg()`, `to_wkt()`,
or `to_json()`. Objects may also expose a nested `.crs` or `.srs` value in one of
those forms. Invalid codes and definitions raise `CRSError`.

### Polar rings and full-turn edges

Geographic topology distinguishes an explicit one-period edge between ordinary
longitude spellings from the two spellings of the seam. For example, `0 → 360`
degrees is a full circle, while `-180 → 180` is the same seam point. A ring's
signed chart turn determines whether its unmarked boundary winds once around a
pole. A once-winding ring is unambiguous when its latitude range stays in one
closed hemisphere and extends beyond the equator: touching the equator is
allowed. A ring that crosses the equator, or lies entirely on it, does not name
a unique pole and is rejected at geographic admission. Use an explicit pole
closure or split the region into unambiguous rings.

The edge direction is part of the ring's total: a closing full-period edge can
cancel earlier full-circle traversals. A full-period edge alone therefore does
not declare that a ring encloses a pole.

```python exec="on" source="block" result="text"
import gometry as gm

ambiguous = gm.Polygon(
    [(0, -10), (360, -10), (300, 10), (0, -10)],
    crs=4326,
)
explicit_closure = gm.Polygon(
    [(0, 60), (360, 60), (360, 90), (0, 90), (0, 60)],
    crs=4326,
)

print("unmarked ring:", ambiguous.validate().code)
print("explicit pole closure:", explicit_closure.validate().valid)

```

### Changing a CRS label

!!! danger "A wrong CRS label changes the data's meaning"
    Calling `with_crs(3857)` on lon/lat data would *claim* `(21, 52)` is "21 meters
    east, 52 meters north" — a point in the Atlantic off the coast of Africa
    instead of Poland. Replacing one declared CRS with a *different* one raises
    unless you pass `overwrite=True` (re-tagging is
    almost always a `to_crs` mistake). Use `with_crs` only to attach the CRS the
    coordinates were authored in.

You can pass any CRS spelling PROJ can resolve to either call — an EPSG integer,
an `"EPSG:4326"` string, a CRS name, WKT, or PROJJSON. CRS metadata supplied at a
constructor or via `with_crs` is validated through PROJ *before* it is stored, so a
typo fails fast at the boundary rather than deep inside a later transform. WKT,
PROJJSON, and PROJ definitions retain their normalized input text and numeric
meaning; use `crs.definition` when the stored label matters:

```python exec="on" source="block" result="text"
import gometry as gm

p = gm.Point(21.0, 52.0)
for crs in (4326, "EPSG:4326", "WGS 84"):
    print(repr(crs), "→", p.with_crs(crs).crs)

try:
    p.with_crs(999999)
except Exception as e:
    print("rejected:", type(e).__name__)

```

A `crs=` argument on any constructor is a `with_crs`, not a `to_crs`. It attaches
metadata to the coordinates you supplied — the bounds stay the native coordinates
you passed:

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
print(area.crs, area.bounds)

```

!!! tip "Troubleshooting a `CRSMismatchError`"
    Binary operations require both operands to share the same CRS and coordinate
    epoch; gometry does not coerce a mismatch. Repair the metadata (`with_crs`,
    when only the tag is missing) or transform the coordinates (`to_crs`, when the
    numbers must move), then retry. The structured
    [`CRSMismatchError`][gometry.CRSMismatchError] carries `.left` / `.right`
    so a handler can branch without parsing the message.

    ```python exec="on" source="block" result="text"
    import gometry as gm

    wgs84 = gm.Point(2.35, 48.86, crs=4326)
    web_mercator = gm.Point(261600, 6249447, crs=3857)

    try:
        gm.distance(wgs84, web_mercator)
    except gm.CRSMismatchError as err:
        print("mismatch:", err.left, "vs", err.right)

    fixed = web_mercator.to_crs(wgs84.crs)
    print("distance after to_crs:", round(gm.distance(wgs84, fixed)), "m")
    ```

    For ingestion pipelines, repair once at the boundary and keep arrays in one
    frame before indexing, joining, or measuring.

## Measurement: the CRS decides

There is exactly one [`geom.area`][gometry.Geometry.area], one
[`geom.length`][gometry.Geometry.length], one [`distance()`][gometry.distance].
The geometry's CRS selects the default metric frame (see
the [operation-family table](#operation-families)). Override the *unit system* with
`unit=`, or reproject with [`to_crs`][gometry.Geometry.to_crs] to change the
*coordinate frame*. The same rule governs
[`buffer`][gometry.Geometry.buffer], [`offset_curve`][gometry.Geometry.offset_curve]
and [`dwithin`][gometry.dwithin] — distances in, distances out, native by default.

| `unit=` | Effect |
|---------|--------|
| omitted / `None` | CRS-natural default |
| `'meters'` | force SI meters / m² (raises without a CRS) |
| `'planar'` | force raw coordinate Cartesian math |

Bare measures are **properties** — [`geom.area`][gometry.Geometry.area] /
[`geom.length`][gometry.Geometry.length] on a scalar, [`arr.area`][gometry.GeometryArray.area] / [`arr.length`][gometry.GeometryArray.length] on
a [`GeometryArray`][gometry.GeometryArray]. The free functions
[`area`][gometry.area] and [`length`][gometry.length] exist only as **override
paths** that take `unit=`; they are not a second vectorized API. Binary
[`gm.distance`][gometry.distance] is free because it relates two operands.

### Geographic CRS → ellipsoidal measures, automatically

Attach a geographic CRS and shape [`area`][gometry.Geometry.area] and
[`length`][gometry.Geometry.length] measure straight longitude/latitude edges
on that CRS's ellipsoid. Point-to-point [`distance()`][gometry.distance]
follows the shortest ellipsoidal geodesic. For shape pairs, distance minimizes
geodesic separation between points in the closed supports: straight chart-edge
linework and filled polygon interiors. The same support model applies to
`dwithin`, `nearest_points`, and `shortest_line`. These metrics respect whether
angular coordinates use degrees, radians, or grads; geodesic calculations use
[Karney's algorithm](https://geographiclib.sourceforge.io/).

```python exec="on" source="block" result="text"
import gometry as gm

paris  = gm.Point(2.3522, 48.8566, crs=4326)
london = gm.Point(-0.1276, 51.5072, crs=4326)

d = gm.distance(paris, london)
print(f"distance: {d/1000:.1f} km")
print(f"bearing: {gm.bearing(paris, london):.1f}°")

```

`length` measures the chart-edge path in metres; `area` measures the ellipsoidal
area bounded by those edges in square metres:

```python exec="on" source="block" result="text"
import gometry as gm

route = gm.LineString(
    [(2.3522, 48.8566), (-0.1276, 51.5072)], crs=4326
)
print(f"route length: {route.length/1000:.1f} km")

city = gm.box(2, 48, 3, 49, crs=4326)
print(f"city area: {city.area/1e6:.0f} km²")

```

For geographic shape area and length, each stored edge is straight in the
longitude/latitude chart. A constant-latitude edge therefore follows its
parallel, rather than the shortest geodesic between its endpoints. WKT records
vertices without specifying an edge interpolation rule. GeoJSON follows RFC
7946's straight Cartesian interpolation in its WGS 84 longitude/latitude
coordinate system; it has no geodesic or rhumb-line edge option. RFC 7946
[§3.1.1](https://www.rfc-editor.org/rfc/rfc7946.html#section-3.1.1) defines
linearly interpolated positions and applies the same rule to the optional third
ordinate (height). Gometry instead inserts Z using the ellipsoidal
arclength fraction of the retained chart-straight source edge. Therefore a Z
inserted before GeoJSON export can differ from one inserted later by a consumer
interpolating the exported segment, even when both insertions use the same
longitude/latitude position. To represent a geodesic, rhumb line, or path
defined in another projected frame, **sample that path into intermediate
vertices before writing GeoJSON**. The serialized segments remain straight
between those samples. Ordinary densify interpolates the stored chart edge and
does not synthesize a rhumb route. Split or otherwise disambiguate half-period
edges that have no unique chart direction.

For geographic shape pairs, [`distance`][gometry.distance],
[`dwithin`][gometry.dwithin],
[`nearest_points`][gometry.nearest_points], and
[`shortest_line`][gometry.shortest_line] measure support along straight paths in
the longitude/latitude chart. `Polygon` and `MultiPolygon` operands use their
filled interiors: an interior point or overlapping pair has zero distance,
while disjoint shapes are measured boundary-to-boundary. `shortest_line`
connects the nearest witnesses as a chart-edge line. With the geographic
ellipsoidal metric, its chart-edge length is no shorter than their geodesic
separation, subject to binary64 rounding; the result may include extra vertices
to preserve the chart edge. Use the same `unit=` when comparing its length with
`distance`. Continuous geographic
`hausdorff_distance` ranges over point and linework supports only; filled
polygon operands raise [`CRSError`][gometry.CRSError]. Use
[`polygon.boundary()`][gometry.Geometry.boundary] when the question is
about the boundary itself, `unit='planar'` for coordinate-space areal distance,
or reproject to a suitable projected CRS.

Hausdorff's planar path is a certified computation rather than an unchecked
floating-point approximation. By default its relative error is at most
`1e-9`. Pass a positive, finite scalar `max_error` to replace that default
with an absolute bound in the final result units. A projected result uses
native linear units by default or metres with `unit='meters'`, so an absolute bound
follows the same choice. The geographic path uses a fixed radius-scaled
algorithmic gap rather than a per-call certificate; supplying `max_error` to it
raises instead of implying one.
Neither error contract blurs set equality:
planar Hausdorff returns zero exactly for equal closed supports, while a
positive distance smaller than the effective error bound remains positive. If
that bound cannot be certified within representable output or available
generated work and memory, the call raises instead of returning an unchecked
value.

#### Geographic box boundary fidelity

For nondegenerate geographic bounds whose longitude and latitude values lie
within the CRS frame's native ranges, `box` constructs edges along parallels
and meridians.
It inserts only enough intermediate longitudes to keep each step strictly
below half the longitude period. A span below half a period has five ring
positions including closure; a span from half to less than a full period has
seven, and a full-world band has nine. Geographic area and length measure these
stored chart edges. When `west > east`, `box` splits the interval at the seam
automatically; its bounds retain that west/east ordering.

Degenerate bounds and ordered bounds outside the native ranges retain the
corner-only, unwrapped construction. The native-range directed-span guarantee
does not apply on that path. A seam-crossing box must use valid endpoints in
the frame's native longitude and latitude ranges.

#### Geographic geodesic numerical domain

With CRS-aware units, geographic point-to-point inverse/direct calculations and
operations that explicitly follow geodesic routes use an order-six
binary64 series backend. In this model, geographic chart-edge length uses
ellipsoidal quadrature, while point-to-chart-edge minimization uses geodesic
inverse samples inside its own bounded search. Continuous geographic Hausdorff
ranges over those same straight chart edges. `unit='planar'` selects
Cartesian measurement in stored coordinates instead. These composite
operations are distinct from exact geographic topology. For a CRS that PROJ
resolves to an ellipsoid, its semi-major axis and `b = a (1 - f)` must be
positive and finite, and its flattening must be within the supported domain
`|f| <= 0.02`. Geodesic admission also checks that the squared axes, half the
squared semi-major axis, authalic radius squared, and full ellipsoid area are
representable as normal binary64 values. A CRS outside that domain raises
`CRSError` before the geodesic object is constructed.

The [GeographicLib geodesic documentation](https://geographiclib.sourceforge.io/html/python/interface.html)
identifies `f ∈ [-1/50, 1/50]` as the full-accuracy series range and reports an
approximate maximum distance error of `30 nm` at `|f| = 0.02`. gometry's
admission is an ordinary backend domain, not a per-call certificate for
composite algorithms.

#### Geographic point-to-segment accuracy

With CRS-aware units, geographic linework distance, nearest-point witnesses,
and `line_locate` use a minimum over each straight chart edge's parameter `t`.
Each sample evaluates geodesic distance from the query to the chart position. A
global interval search combines a Lipschitz lower bound with analytic speed and
covariant-acceleration bounds. Its second-order Taylor bound applies only when
the full distance interval lies inside a certified normal neighborhood;
otherwise the search uses the Lipschitz bound. Degenerate edges and pole
queries use their direct families. A witness lies on the chart edge, and LRS
carries its ordinates by the fraction of edge arclength.

The attained binary64 minimum uses the radius-scaled gap `4e-11 × a` and a
maximum of 128 evaluations per point-edge query. If the search reaches that
budget, distance, nearest-point, and `line_locate` return their best attained
sample. These are numerical quality budgets, not universal real-arithmetic
error certificates. Search results are attained witnesses, not topological
contacts. Exact chart topology establishes contact; a small positive distance
does not. `line_locate` uses the attained parameter's ellipsoidal chart-edge
arclength and adds the prefix before that edge.

Point-to-chart-edge `dwithin` returns `True` only after an attained sample is at
or below the supplied radius, and returns `False` only when its search certifies
the threshold result. Accumulated evaluations are limited to a binary64
midpoint-depth factor times the current live frontier, with a 16,384-evaluation
ceiling. The search also uses safeguarded secant proposals; this allowance is a
work limit, not a guarantee of reaching binary64 parameter resolution. If it
cannot certify the radius, `dwithin` raises `CRSError` rather than reporting
`False`. It does not add a contact tolerance or widen the radius by pruning
slack. A positive infinite radius returns `True` for admitted non-empty
operands without forcing an unrepresentable distance result. Vectorized results
retain one boolean per input pair: missing rows and present empty geometries
remain in place as `False`; a nonconvergent present pair raises instead of
receiving a boolean.

Geographic Hausdorff measures the continuous linework supports of the stored
chart edges: the source ranges over each straight longitude/latitude edge, and
its distance to the target is the minimum geodesic distance to target edges and
isolated points. Its global frontier uses a moving correspondence between
source and target chart edges when that gives a tighter variation bound;
otherwise it retains the fixed-witness Lipschitz bound and applies the
second-order Taylor bound only inside a normal neighborhood. Each reported
value is an attained source/target pair, not an interval bound. The algorithmic
gap is `a * (0.01 / 6_378_137)`, about one centimetre for WGS 84. If source
parameter refinement reaches adjacent binary64 values before the frontier
closes that gap, Hausdorff raises `CRSError` rather than dropping the interval.
These binary64 budgets are numerical quality targets, not real-arithmetic
certificates.
 Geographic Hausdorff supports point and linework operands only; filled polygon
 interiors require `unit='planar'` or a projected CRS. Omit `max_error` on the
geographic path; its algorithmic gap does not promise a caller-requested bound.

!!! note "Why geodesic and projected answers differ"
    [Web Mercator](https://epsg.org/crs_3857/WGS-84-Pseudo-Mercator.html) is a
    display and tile CRS whose scale varies with latitude. A geographic CRS
    measures on its ellipsoid without first projecting; a projected CRS measures
    the coordinates in that chosen frame.

#### Bearing and destination — point navigation

gometry exposes binary point navigation as free functions under `gm.`. Unary
destination is a receiver method on [`Point`][gometry.Point] and [`GeometryArray[Point]`][gometry.GeometryArray]. On a
geographic CRS these operations are
geodesic; on a projected or CRS-free point they are planar.

- [`gm.bearing(pt, other)`][gometry.bearing] — initial heading (degrees) from one point to another.
- [`gm.distance(a, b, path='rhumb')`][gometry.distance] — constant-bearing distance
  in meters between points on a geographic CRS. This route option applies only
  to point operands and does not accept a `unit` override.
- [`pt.destination(bearing, distance)`][gometry.Point.destination] — walk `distance` in CRS-natural units
  (meters on a geographic CRS; native linear units when projected; coordinate
  units when CRS-free; override with `unit=`) along `bearing` degrees and return
  the arrival point.
- [`gm.point_between(a, b, distance, *, normalized=False)`][gometry.point_between] — a point partway
  between two points.

```python exec="on" source="block" result="text"
import gometry as gm

start = gm.Point(2.3522, 48.8566, crs=4326)
arrival = start.destination(45.0, 100_000.0)
print(arrival.to_wkt())
print(round(gm.distance(
    gm.Point(-73.8, 40.6, crs=4326),
    gm.Point(-0.5, 51.6, crs=4326),
    path='rhumb',
) / 1000))

```

!!! warning "Argument order: bearing first, then distance"
    `pt.destination(bearing, distance)` takes the **bearing in degrees first**
    and the **distance in meters second**. Swapping them produces a
    plausible-but-wrong point with no error.

#### Geographic polygon boundary

[`Polygon.boundary`][gometry.Geometry.boundary] and
[`MultiPolygon.boundary`][gometry.Geometry.boundary] return the
Boundary-location cells of occupied support. A MultiPolygon's members form one
areal union, so shared or overlapping internal edges are not part of its
boundary; a GeometryCollection is handled member by member without dissolving
between members. Invalid polygonal input follows its occupied support rather
than returning every stored ring, and collapsed support can produce a
`MultiPoint` or an empty `GeometryCollection`.

Geographic boundary uses the CRS's longitude period and pole identity. Edges
that exist only because a ring was cut at the antimeridian, or because a chart
closes a polar ring, are not boundaries. A genuine one-sided edge on the seam
remains. The quotient line emitter can include a seam vertex where it splits a
line run; that vertex does not add a seam edge. CRS and epoch are preserved,
and no coordinate transformation is performed.

```python exec="on" source="block" result="text"
import gometry as gm

crossing = gm.Polygon(
    [(170, 40), (190, 40), (190, 50), (170, 50)],
    crs=4326,
)
print(crossing.boundary().to_wkt())
```

```text
LINESTRING (170 50, 170 40, -180 40, -170 40, -170 50, -180 50, 170 50)
```

### Projected CRS → native linear units

The same operations on a *projected* geometry are planar arithmetic in the CRS's
**native linear units** — feet stay feet, meters stay meters. Reproject to an
**appropriate projected CRS first**, then measure. Pass `unit='meters'` when you
need SI meters or square meters from a non-meter projected CRS.

#### Projection comparison

The same Earth coordinates can produce different planar maps because each
projection chooses a different distortion trade-off. Use this comparison to
understand the chosen CRS, not as a measurement recommendation: a projection's
units and distortion determine what a planar result means.

```python exec="on" html="true"
import gometry as gm
from _figures import panels

meridians = [
    gm.LineString([(lon, lat) for lat in range(0, 91, 2)], crs=4326)
    for lon in range(-180, 181, 30)
]
parallels = [
    gm.LineString([(lon, lat) for lon in range(-180, 181, 2)], crs=4326)
    for lat in range(0, 90, 15)
]
graticule = gm.GeometryArray([*meridians, *parallels])

print(panels(
    [
        ('North-polar orthographic', graticule.to_crs('ESRI:102035')),
        ('Mollweide equal-area', graticule.to_crs('ESRI:54009')),
    ],
    caption='The same northern-hemisphere coordinates take different planar shapes in each projection.',
    alt='In the orthographic projection, meridians meet at the centered north pole and the equator forms an outer circle. In the Mollweide projection, the same graticule spans a curved half-ellipse that preserves area.',
    share_frame=False,
))
```

This is a comparison of 2D coordinate frames, not a 3D globe or spherical
topology. The orthographic frame places the north pole near `(0, 0)` and its
equator near radius `6,378,137`; Mollweide keeps local area scale near `1` at
both 0° and 60° latitude while changing shape. Choose a CRS for the operation:
use a local projected CRS for local planar work, and use geographic CRS metrics
when the ellipsoidal result is intended.

```python exec="on" source="block" result="text"
import gometry as gm

city = gm.box(2, 48, 3, 49, crs=4326)
projected = city.to_crs(32631)

print("crs:", projected.crs)
print(f"area: {projected.area/1e6:.0f} km²")

```

!!! warning "Why the projection choice matters"
    The same 1° tile over Paris, measured under three CRSs:

    ```python exec="on" source="block" result="text"
    import gometry as gm

    city = gm.box(2, 48, 3, 49, crs=4326)

    geo = city.area / 1e6
    web = city.to_crs(3857).area / 1e6
    utm = city.to_crs(32631).area / 1e6

    print(f"ellipsoidal chart-edge area: {geo:.0f} km²")
    print(f"Web Mercator: {web:.0f} km²")
    print(f"UTM 31N: {utm:.0f} km²")
    ```

    [Web Mercator](https://epsg.org/crs_3857/WGS-84-Pseudo-Mercator.html) has
    latitude-dependent scale. Reprojecting selects the coordinate frame used for
    the result.

Web Mercator stretches the box away from the equator, more than doubling its
area at this latitude, while the UTM zone stays within a tenth of a percent of
the geographic ellipsoidal chart-edge area.

!!! tip "Choose the projection for the job"
    Local area/length operations use a UTM zone or national grid covering the
    region. Web Mercator (`EPSG:3857`) is intended for tiles rather than area
    measurement. Global or trans-continental measurement can remain in a
    geographic CRS, where `geom.area` / `geom.length` measure ellipsoidal chart
    edges.

### CRS-free measurement

With no CRS attached, a geometry is a set of numbers on a flat plane, and measurement
is in whatever unit those coordinates are in.

```python exec="on" source="block" result="text"
import gometry as gm

plot = gm.box(0, 0, 30, 20)
print("area:", plot.area)
print("length:", plot.length)
print("distance:", gm.distance(gm.Point(0, 0), gm.Point(3, 4)))

```

`unit='planar'` forces this raw coordinate Cartesian math even on a CRS-bearing
geometry — comparable with an ellipsoidal chart-edge measure:

```python exec="on" source="block" result="text"
import gometry as gm

city = gm.box(2.3, 48.8, 2.4, 48.9, crs=4326)
print("ellipsoidal m²:", round(city.area))
print("planar deg²:", gm.area(city, unit='planar'))

```

For a supported metric pair, [`nearest_points`][gometry.nearest_points] returns
the closest witnesses and [`shortest_line`][gometry.shortest_line] connects
them. Compare a line's length with the reported separation using the same
`unit=` choice. These operations accept filled polygon supports and can place a
witness on an edge interior, not only at a vertex.

Geographic Hausdorff distance is a separate linework operation: it supports
points and linework, while filled polygon operands raise `CRSError`. Use
`unit='planar'` or reproject when filled-set Hausdorff distance is intended.

```python exec="on" html="true"
from _figures import figure
import gometry as gm

a = gm.from_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))")
b = gm.from_wkt("POLYGON ((4 1, 5 0, 6 1, 5 2, 4 1))")
pa, pb = gm.nearest_points(a, b)
bridge = gm.shortest_line(a, b)
print(figure(
    {'source': [a, b], 'result': bridge, 'anchor': [pa, pb]},
    f"shortest_line(a, b) = {bridge.to_wkt()}",
    alt="The shortest line runs between the two nearest boundary points, one on each polygon.",
    labels={'source': 'a and b', 'result': 'shortest_line', 'anchor': 'nearest_points'},
))

```

An empty operand is total, not an error: `distance`, `hausdorff_distance` and
`frechet_distance` return `inf`, while `nearest_points` and `shortest_line`
return the output-type empty (`(POINT EMPTY, POINT EMPTY)` and
`LINESTRING EMPTY`), so a single empty row never aborts a vectorized call.
Planar Hausdorff polygon operands are filled closed regions: interiors belong
to the set and holes do not. A mixed-dimensional collection uses the union of
its members; a zero-area polygon contributes only its closed ring boundary.
When operands differ in dimensionality (an `XYZM` line
and an `XY` point) the witnesses drop to the ordinates both carry.

!!! note "Coordinate-space tolerances stay in raw units"
    A few operations take a tolerance that is always interpreted in raw coordinate
    units, regardless of CRS — they *remove or move* vertices, and reprojecting
    would move the survivors:
    [`simplify`][gometry.Geometry.simplify], [`snap`][gometry.snap],
    [`snap_to_grid`][gometry.Geometry.snap_to_grid],
    [`quantize`][gometry.Geometry.quantize], and
    [`remove_repeated_points`][gometry.Geometry.remove_repeated_points]. On geographic
    data such a tolerance is in the CRS's native angular units; reproject with
    [`to_crs`][gometry.Geometry.to_crs] first if you need it in meters. Every
    distance- or area-returning metric — including
    [`hausdorff_distance`][gometry.hausdorff_distance],
    [`frechet_distance`][gometry.frechet_distance],
    [`minimum_bounding_radius`][gometry.Geometry.minimum_bounding_radius],
    [`minimum_clearance`][gometry.Geometry.minimum_clearance], and
    [`nearest_points`][gometry.nearest_points] — is CRS-aware with a
    `unit='planar'` escape (and `unit='meters'` for forced SI).

!!! note "`segmentize` inserts vertices without moving originals"
    [`segmentize`][gometry.Geometry.segmentize] only *inserts* vertices along
    existing segments, so every original survives untouched. By default,
    geographic edges stay straight in longitude/latitude, and `max_length` is
    the ellipsoidal arclength of the retained chart-straight source edge,
    measured like
    [`length`][gometry.length] — meters along the ellipsoid on a geographic CRS,
    native linear units on a projected one, raw coordinate units when CRS-free — with the
    same `unit='planar'` / `unit='meters'` escapes as every other metric.

    Use `path='geodesic'` to space vertices along the shortest ellipsoidal route;
    on projected or CRS-free input it is planar. `path='rhumb'` follows a
    constant-bearing route and requires a geographic CRS. Both still return
    straight longitude/latitude edges, so a routed result's `length` approaches
    the route length as `max_length` shrinks. `fraction=` spaces vertices by
    equal fractions of the selected path's arclength. A non-`None` `path` cannot
    be combined with `unit=`.

    ```python exec="on" source="block" result="text"
    import gometry as gm

    line = gm.LineString([(0, 60), (90, 60)], crs=4326)
    print(f"length: {gm.length(line):,.0f} m")
    print(f"stored edge: {len(list(line.segmentize(500_000).coords))} vertices")
    print(
        "geodesic route:",
        len(list(line.segmentize(500_000, path='geodesic').coords)),
        "vertices",
    )
    ```

## Picking a projected CRS automatically

When you want *planar* measurement — because a downstream tool wants
projected coordinates — reproject to a CRS whose units are meters. Call
[`geom.estimate_local_crs()`][gometry.Geometry.estimate_local_crs] on the geometry
or array whose full extent must fit. It prefers a datum-aware
[UTM](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system)
or UPS authority CRS, then a local conformal CRS, and accepts a candidate only
when estimated linear scale error stays within 0.1% over the extent.
Geographic constructive operations keep this frame-admission check and may
refine source-chart edges to control the projected input chord deviation before
their planar kernels run. That input-refinement budget is separate from the
0.1% frame-fit limit and from round-arc resolution; see
[geographic local-frame input refinement](constructive.md#geographic-local-frame-input-refinement).

```python exec="on" source="block" result="text"
import gometry as gm

warsaw = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
local = warsaw.estimate_local_crs()
print("local:", local)
print("ellipsoidal chart-edge area m²:", round(warsaw.area))
print("projected area m²:", round(warsaw.to_crs(local).area))

```

The projected planar area is an answer in the selected local frame. The estimator
normalizes projected inputs to lon/lat for extent planning while preserving
datum-specific authority choices where available.

### Reproject to a local UTM zone

A local projected metric workflow uses `estimate_local_crs()` followed by
`to_crs`; metrics then use planar meters in the selected frame.

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(2.30, 48.84, 2.39, 48.89, crs=4326)

local = area.estimate_local_crs()
projected = area.to_crs(local)

print('local CRS:', local.to_authority())
print('planar area m²:', round(projected.area))

```

```python exec="on" source="block" result="text" interactive="off"
import gometry as gm

area = gm.box(2.30, 48.84, 2.39, 48.89, crs=4326)
projected = area.to_crs(area.estimate_local_crs())

ring = projected.buffer(500)
print('500 m buffer area m²:', round(ring.area))

```

```python exec="on" html="true"
from _figures import before_after
import gometry as gm

area = gm.box(2.30, 48.84, 2.39, 48.89, crs=4326)
projected = area.to_crs(area.estimate_local_crs())
print(before_after(
    projected, projected.buffer(500),
    before_caption="projected input", after_caption="buffer(500 m)",
    before_alt="The area is a rectangle in the estimated local metric frame.",
    after_alt="The 500 metre buffer stands off the rectangle by the same distance on every side.",
))

```

The *same* metric distance draws differently depending on **where** it is measured.
A 250 km buffer at the equator and at 55°N — both `250_000` meters through the local
projection — become ellipses of different shapes once drawn back in lon/lat, because
a degree of longitude shortens toward the poles:

```python exec="on" html="true"
from _figures import panels
import gometry as gm
print(panels(
    [
        (
            f'250 km buffer at {lat}°N (drawn in lon/lat)',
            {'anchor': gm.Point(3, lat, crs=4326), 'result': gm.Point(3, lat, crs=4326).buffer(250000)},
            f'At {lat} degrees north the same 250 kilometre buffer spans '
            f'{"as many degrees of longitude as of latitude" if lat == 0 else "far more degrees of longitude than of latitude"}.',
        )
        for lat in (0, 55)
    ],
    caption='The same metric radius covers more longitude the further it sits from the equator.',
    alt='Drawn in degrees, the equatorial buffer is round while the buffer at 55 degrees north is a wide ellipse.',
    # Degrees as numbers: correcting the aspect here would redraw both as
    # circles and erase the distortion this figure is about.
    aspect='raw',
    share_frame=False,
))

```

## Across the antimeridian

On a geographic CRS, the physical seam is represented by the CRS-native periodic
longitude axis. It is ±180° for EPSG:4326, ±200 grads for EPSG:4807, and ±π for
a radian frame with Greenwich as its prime meridian. gometry's **topology** ops —
predicates, relate, overlay,
centroid, [`point_on_surface`][gometry.Geometry.point_on_surface], bounds,
distance/[`dwithin`][gometry.dwithin], the
spatial index, and validation/repair ([`is_valid`][gometry.Geometry.is_valid],
[`validate`][gometry.Geometry.validate], [`require`][gometry.require], [`self_intersections`][gometry.Geometry.self_intersections], [`repair`][gometry.Geometry.repair], and
[`snap_to_grid(..., repair=True)`][gometry.Geometry.snap_to_grid]) — interpret
coordinates using the CRS's periodic longitude and pole identities. Operations that
construct split output insert affine X/Y crossings into straight edges in the
unwrapped longitude/latitude chart; see
[Z/M under operations](geometry.md#zm-under-operations) for how inserted
ordinates follow their source edges. Authored full-period edges keep their path
through quarter witnesses. Geographic shape measures
([`length`][gometry.length] and [`area`][gometry.area]) integrate those chart
edges, so splitting preserves ellipsoidal length and area to rounding.
Point-to-point [`distance`][gometry.distance] continues to use the shortest
geodesic across the seam; shape distance measures chart-edge supports.
Geographic area measures the polygon's lon/lat chart interior, including
regions larger than half the ellipsoid, independent of ring order.
At the seam, a collinear shared span with polygon interiors on opposite sides
is a chart cut regardless of whether members use east/east, west/west, or
east/west seam spellings; it does not count for validity or simplicity, while
same-side overlaps remain invalid. A full-longitude band may also use zigzag
sides when the two boundary copies are antiparallel and exactly one period
apart.

Geographic overlay rounds constructed intersection nodes to nearest binary64
coordinates when that assignment preserves the exact quotient chart. If nearest
rounding would change that chart, the overlay searches adjacent representable
coordinates for constructed nodes only; stored input vertices and graph
segmentation remain fixed. This is a topology-preserving repair, not a
user-selected snap grid.
On computed geographic edge chains, vertices in a zero-longitude-change
meridian run use the stored longitude spelling when the run touches a preserved
input vertex.
If preserved vertices on one such run use conflicting spellings, the operation
raises [`GeometryError`][gometry.GeometryError] rather than choosing one.

Geographic tessellations have method-specific seam and pole rules; see
[constructive operations](constructive.md#tessellation-triangulate-voronoi-polygonize).

The other stay-planar constructive ops ([`convex_hull`][gometry.Geometry.convex_hull],
[`concave_hull`][gometry.Geometry.concave_hull],
[`buffer`][gometry.Geometry.buffer], [`offset_curve`][gometry.Geometry.offset_curve],
and [`simplify`][gometry.Geometry.simplify]) do **not** auto-split; call
[`split_antimeridian`][gometry.Geometry.split_antimeridian] first on crossing
input. Geographic simplification also rejects a vertex removal that creates
an edge geographic topology would read across the seam. The check uses the
CRS-native longitude half-period and preserves the stored crossing input; it
does not normalize or split it. A crossing geometry's
[`bounds`][gometry.Geometry.bounds] reports
`west > east` (minx > maxx) — the established geographic convention.

For WGS 84 boxes that cross longitude 180/−180, pass west > east; `box`
automatically splits the interval into valid pieces. A geographic geometry's
bounds can be passed back to `box` or `boxes` in the same frame. For arbitrary
crossing linework, call
`split_antimeridian()` before planar algorithms that document that requirement,
including Earcut:

```python exec="on" html="true"
from _figures import antimeridian_before_after
import gometry as gm
line = gm.LineString([(170, -10), (-170, 10)], crs=4326)
print(antimeridian_before_after(line, line.split_antimeridian()))

```

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(170, -10, -170, 10, crs=4326)
track = gm.LineString([(179, 10), (-179, 10)], crs=4326)
split_track = track.split_antimeridian()

probes = gm.points([179.5, -179.5, 0.0], [0.0, 0.0, 0.0], crs=4326)
matches = gm.SpatialIndex(gm.GeometryArray([region])).query(
    probes,
    predicate="within",
)

print("region:", region.geometry_type, region.bounds)
print("track crosses:", track.crosses_antimeridian)
print("split track:", split_track.geometry_type)
print("point matches:", matches.values.tolist())

```

Spatial indexes widen periodic seam-crossing envelopes before candidate search and
still run exact refinement, so the middle-of-map false positives are removed by the
predicate step. Topology and indexing normalize for you. Geographic point-to-point
distance uses geodesics; shape distance operations use the same straight chart
edges as topology and include filled polygon interiors. Geographic Hausdorff
distance ranges over those chart edges but remains linework-only.

## Z and M under transformation

[`to_crs`][gometry.Geometry.to_crs] transforms X and Y. It carries **Z** through unchanged when the target is
a horizontal projection (e.g. Web Mercator), and transforms Z through the pipeline
when the operation consumes it (geocentric, vertical-capable, and
compound workflows). It never invents or silently drops Z. **M** (the measure
ordinate — timestamps, route distances) is not a spatial axis, so a coordinate
transform has no defined action for it and `to_crs` carries M through
**unchanged**. The full Z/M behavior is explained in the
[geometry guide](geometry.md).

```python exec="on" source="block" result="text"
import gometry as gm

flat = gm.Point(21.0, 52.0, crs=4326).to_crs(3857)
print("XY →", flat.coordinate_axes, "| has_z:", flat.has_z)

elev = gm.Point(21.0, 52.0, z=100.0, crs=4979).to_crs(3857)
print("XYZ →", elev.coordinate_axes, "| has_z:", elev.has_z)

trace = gm.Point(21.0, 52.0, m=1_700_000_000.0, crs=4326).to_crs(3857)
print(trace.coordinate_axes, "| m unchanged:", trace.m)

```

!!! note "M is application data, not a coordinate"
    A timestamp or route measure has no datum, so no pipeline reprojects it;
    `to_crs` carries it through unchanged. Drop measures explicitly with
    [`with_coordinates(m=None)`][gometry.Geometry.with_coordinates] (or [`force_2d()`][gometry.Geometry.force_2d] to drop both Z and M) before or after the
    transform.

## Coordinate epochs

Dynamic reference frames (ITRF, the WGS 84 realizations, current national datums)
keep moving with the tectonic plates, so a coordinate is only fully defined
together with the **decimal year** it was observed — its *coordinate epoch*.
gometry carries that as first-class metadata, [`geom.epoch`][gometry.Geometry.epoch], distinct from Z and M.
A coordinate epoch dates a CRS realization, so it is **only meaningful with a CRS**:
gometry enforces `epoch ⟹ crs` at every entry point.
Geometry frames admit epochs for CRSs based on a dynamic datum or the WGS 84
ensemble (EPSG:6326), including derived CRSs such as EPSG:3857. Dynamic members
of another ensemble do not by themselves make that ensemble epoch-capable.

Epoch inputs accept finite values Python can convert with `float()`, including
`Decimal`, `Fraction`, objects implementing `SupportsFloat` or `SupportsIndex`,
NumPy numeric scalars, and zero-dimensional numeric arrays. Gometry converts
them to binary64; the stored and returned epoch is a `float`. Dates and
datetimes are rejected because this numeric interface does not define calendar
or time-zone conversion.

```python exec="on" source="block" result="text"
import gometry as gm

station = gm.Point(21.0, 52.0, z=100.0, crs=4979)
observed = station.with_epoch(2020.0)
print("epoch:", observed.epoch, "| coords unchanged:", list(observed.coords) == list(station.coords))

try:
    gm.Point(21.0, 52.0).with_epoch(2020.0)
except gm.CRSError as error:
    print("rejected:", error)

print("after with_crs(None):", observed.with_crs(None).epoch)

```

[`with_epoch`][gometry.Geometry.with_epoch] is the assign/clear setter (`with_epoch(None)` clears; changing a
present epoch needs `overwrite=True`, like `with_crs`). When you reproject, the
**source** epoch is the geometry's own `epoch`, and [`to_crs(..., epoch=...)`][gometry.Geometry.to_crs] labels
the **output** epoch — there is no separate `source_epoch`/`target_epoch` on the
geometry surface (those live on the raw [`gm.crs_transform(...)`][gometry.crs_transform] /
[`CRS.operation(...)`][gometry.CRS.operation] coordinate APIs, which have no geometry metadata to read).

```python exec="on" source="block" result="text"
import gometry as gm

observed = gm.Point(21.0, 52.0, z=100.0, crs=4979, epoch=2010.0)
geocentric = observed.to_crs(4978, epoch=2020.0)
print("output epoch:", geocentric.epoch)

```

When you omit `epoch=`, the source epoch is kept when the CRS is unchanged or
the target CRS admits geometry epochs; otherwise it is cleared. An explicit
`epoch=` labels the output frame but does not assert that the selected coordinate
operation is time-dependent or has a particular accuracy.

```python exec="on" source="block" result="text"
import gometry as gm

observed = gm.Point(21.0, 52.0, crs=4326, epoch=2010.0)
print("to ITRF2014 (dynamic):", observed.to_crs(9000).epoch)
print("to Web Mercator (WGS 84 ensemble):", observed.to_crs(3857).epoch)
print("to Poland CS92 (static):", observed.to_crs(2180).epoch)

```

## The `CRS` object and standards export

[`geom.crs`][gometry.Geometry.crs] returns a [`gm.CRS`][gometry.CRS] object rather than a string. Its
`definition` property is the exact stored CRS text; authority strings and EPSG
codes are equal only when that is the stored spelling. It exposes PROJ's
introspection surface as properties and methods.

```python exec="on" source="block" result="text"
import gometry as gm

c = gm.Point(21.0, 52.0, crs=4326).crs
print(repr(c))
print(c == "EPSG:4326", c == 4326)
print("name:", c.name, "| kind:", c.kind)
print("geographic?", c.is_geographic, "| projected?", gm.CRS(3857).is_projected)
print("normalize 'epsg:4326':", gm.CRS("epsg:4326").definition)
print("local EPSG:", gm.box(20, 51, 22, 53, crs=4326).estimate_local_crs().to_epsg())

```

`CRS.__eq__` is structural and compares the exact stored definition. Use
[`same_as(..., mode="ignore_axis_order")`][gometry.CRS.same_as] when asking whether two labels are
operationally interchangeable; authority and code are metadata queries, not
replacement definitions. `EPSG:4326` and `OGC:CRS84` remain unequal as stored
CRS values even though geometry operations accept their shared X/Y frame.

Per-CRS introspection lives on the object as properties — [`definition`][gometry.CRS.definition],
[`is_geographic`][gometry.CRS.is_geographic],
[`is_projected`][gometry.CRS.is_projected], [`kind`][gometry.CRS.kind], [`name`][gometry.CRS.name], [`authority`][gometry.CRS.authority], [`code`][gometry.CRS.code], [`axis_order`][gometry.CRS.axis_order], [`axes`][gometry.CRS.axes],
[`area_of_use`][gometry.CRS.area_of_use], [`ellipsoid`][gometry.CRS.ellipsoid], [`datum`][gometry.CRS.datum], and more — plus methods like
[`to_wkt`][gometry.CRS.to_wkt], [`to_proj`][gometry.CRS.to_proj],
[`identify`][gometry.CRS.identify], and [`same_as`][gometry.CRS.same_as]. EPSG:4326's
*native* axis order is latitude-first, but **gometry's geometry and transform APIs
are always X/Y (lon, lat)**.

```python exec="on" source="block" result="text"
import gometry as gm

c = gm.CRS(4326)
print("EPSG:4326 native axes:", [ax["abbreviation"] for ax in c.axes])
print("axis_order:", c.axis_order)

p = gm.Point(21.0, 52.0, crs=4326)
print("gometry reads as (lon, lat):", list(p.coords))

```

!!! warning "Axis order is always X/Y in gometry"
    gometry is always X/Y: [`Point(x, y)`][gometry.Point] is `(lon, lat)` for geographic CRSs and
    `(easting, northing)` for projected ones — there is no axis-order mode switch.
    If you are porting pyproj code, drop the lat/lon swaps.
    [`gm.CRS(x).info`][gometry.CRS.info] returns a plain dictionary of PROJ
    metadata for iterating or serializing raw fields; the typed
    [`CRS`][gometry.CRS] properties expose the same metadata.

Round-trip a CRS to the standard text formats with
[`CRS.to_wkt`][gometry.CRS.to_wkt] ([ISO 19162](https://www.iso.org/standard/76496.html)
/ OGC WKT for CRS — not geometry WKT), [`CRS.to_proj`][gometry.CRS.to_proj], and
[`CRS.to_projjson`][gometry.CRS.to_projjson]. The PROJJSON exporter returns text
by default; use `as_dict=True` for a fresh Python mapping.

```python exec="on" source="block" result="text"
import gometry as gm

print(gm.CRS(4326).to_proj())
print(gm.CRS(4326).to_wkt()[:60], "...")
print(gm.CRS(4326).to_projjson(as_dict=True)['type'])

```

Use [`gm.crs_catalog`][gometry.crs_catalog] to inspect the PROJ CRS database.
Its optional `name` filter composes with authority, CRS kind, area, deprecation,
and celestial-body filters. Results are complete `CrsCatalogInfo` records;
without `limit`, the catalogue returns every matching record.

## Raw coordinate transforms

When you have loose coordinate arrays rather than geometries, transform them
directly with [`gm.crs_transform`][gometry.crs_transform]. Like the geometry API it
is always X/Y. Scalar inputs return an `(x, y)` or `(x, y, z)` tuple; bulk inputs
return one frozen interleaved `float64` NumPy matrix with shape `(N, 2)` or
`(N, 3)`:

```python exec="on" source="block" result="text"
import gometry as gm

x, y = gm.crs_transform(4326, 3857, 21.0, 52.0)
print("3857:", round(x), round(y))

xy = gm.crs_transform(4326, 3857, [21.0, 22.0], [52.0, 53.0])
print("bulk x:", [round(v) for v in xy[:, 0]])

print(gm.crs_transform_bounds(4326, 3857, (20.0, 51.0, 22.0, 53.0)))

```

[`gm.crs_transform`][gometry.crs_transform] accepts optional `z=` and `t=` (time)
ordinates for 3D/4D pipelines; `t` selects the time-dependent operation but is not
returned. [`gm.crs_apply`][gometry.crs_apply] runs an explicit PROJ
operation/pipeline string. [`geom.to_crs`][gometry.Geometry.to_crs] transforms
geometry values; the raw functions transform bare coordinate numbers.

Bounds use periodic longitude by default in geographic CRSs, so west greater
than east describes an antimeridian-crossing interval. With
`force_over=True`, bounds instead use literal longitude numbers: geographic
source bounds sample the closed segment from `min(west, east)` to
`max(west, east)`, and geographic target bounds return numeric extrema rather
than a periodic cover. Identity bounds retain out-of-period longitudes in
literal mode.

## Transformation resources

Wheels include the PROJ database, datum pipelines, and bundled grid files.
gometry does not download grids at runtime. Add local grid directories with
[`gm.crs_configure(search_paths=...)`][gometry.crs_configure].
These are search locations, not an exclusive whitelist: if a named grid is
absent there, PROJ may also try its bare filename in the current working
directory. A matching file there can change transformation availability,
operation selection, and whether a missing-grid warning is emitted.

Transforms sometimes need regional grid files. If the highest-ranked operation's grid is
unavailable, every Python-visible transform surface — `geom.to_crs(...)`,
[`GeometryArray.to_crs(...)`][gometry.GeometryArray.to_crs], `gm.crs_transform(...)`, and
[`gm.crs_transform_bounds(...)`][gometry.crs_transform_bounds] — uses PROJ's valid lower-accuracy fallback and emits
[`AccuracyWarning`][gometry.AccuracyWarning] once for that call. The warning names
the missing grid and links to PROJ's resource-file guidance. Pass
`only_best=True` when fallback is unacceptable: the same condition then raises
[`TransformError`][gometry.TransformError].

[`CRS.operations`][gometry.CRS.operations] enumerates PROJ's
ranked candidates even when their grids are not installed. This keeps the
operation list complete; each grid's `available` field states whether that
candidate can run in the current configuration. [`gm.crs_grid`][gometry.crs_grid]
adds the database `url` and `direct_download` flag so callers can locate an
unavailable resource. Configure local grid directories with
[`gm.crs_configure`][gometry.crs_configure]; `gm.crs_engine()['paths']` reports
those effective per-context search paths.

For performance diagnostics, [`gm.crs_cache_info`][gometry.crs_cache_info]
reports the most recent transform engine and invocation count on the current
thread.

[`gm.crs_configure`][gometry.crs_configure] applies only the options supplied
and returns the effective configuration, so calling it without arguments reads
the configuration. Omit a keyword to preserve that setting, or pass ``None`` to
restore just that setting to its default. Save and restore the returned mapping
when temporarily changing configuration. Changes invalidate cached operations
for subsequent calls; transforms already in progress can finish.

[PROJ](https://proj.org/) is the bundled authority backend. [EPSG](https://epsg.org/)
codes, datum pipelines, grid-aware transforms, [WKT](https://www.ogc.org/standard/sfa/),
and PROJJSON are available without requiring a system PROJ shared library in the
wheel.

Coming from pyproj? See [Migrating](../migrating/index.md#coming-from-pyproj).

## See also

- [The mental model](../get-started/mental-model.md) — the three operation models.
- [Geometry](geometry.md) — construction, dimensions, and the full Z/M rules.
- [Arrays](arrays.md) — columnar [`.area`][gometry.GeometryArray.area] / [`.length`][gometry.GeometryArray.length] and packed batch results.
- [Constructive operations](constructive.md) — [`buffer`][gometry.Geometry.buffer], [`offset_curve`][gometry.Geometry.offset_curve] (same
  metric rule).
- [Validation & repair](validation.md) — [`crosses_antimeridian`][gometry.Geometry.crosses_antimeridian] and
  [`split_antimeridian`][gometry.Geometry.split_antimeridian] at ingest time.
- [Spatial indexing](indexing.md) — envelope widening for crossing rows.
- [API: CRS][gometry.CRS] · [Geometry.to_crs][gometry.Geometry.to_crs] ·
  [crs_transform][gometry.crs_transform] · [distance][gometry.distance] ·
  [area (unit=)][gometry.area] · [length (unit=)][gometry.length]
