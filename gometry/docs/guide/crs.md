---
description: The CRS is gometry's single metric knob — set_crs declares, to_crs transforms, and a geometry's frame decides how area, length, and distance are measured (geodesic on a geographic CRS, native linear units when projected) and what units buffer distances mean.
---

# CRS, units & measurement

A coordinate reference system (CRS) is the contract that says what your numbers
*mean*: whether `(21.0, 52.0)` is "21 degrees east, 52 degrees north on the
[WGS 84](https://epsg.org/crs_4326/WGS-84.html) ellipsoid" or "21 meters east of
some local origin." The CRS is also gometry's **single metric knob** — a
geometry's frame alone decides how it is measured, so `area`/`length`/`distance`
cannot silently return a meaningless "square-degrees" number. This page is the
canonical home for declaring and transforming frames (`set_crs` vs `to_crs`), for
how each frame is measured, for picking a projected CRS automatically, and for
working across the antimeridian. Read it top-to-bottom the first time; jump to a
section by frame kind after that.

Two ideas carry almost all of the weight:

- **[`set_crs`][gometry.Geometry.set_crs] *declares*** what existing coordinates
  already mean. It never moves a coordinate.
- **[`to_crs`][gometry.Geometry.to_crs] *transforms*** coordinates from one CRS
  into another through the selected operation (unlike `set_crs`, which never
  moves a number). Identity pipelines, fixed points, carried M, and sometimes
  unchanged Z mean not every ordinate always changes.

## The frame doctrine at a glance

A geometry's *frame* is its CRS plus (optionally) a coordinate epoch. Ops fall
into a few families rather than one exception-free slogan. Keep **exact geodesic
measures** separate from **planar constructive** work that only *sizes* inputs in
CRS-natural units:

| Family | Examples | Frame / unit / seam behavior |
|---|---|---|
| **Exact geodesic measures** | `area`, `length`, `distance`, `dwithin`, LRS | CRS-natural units (ellipsoidal metres on a geographic CRS); optional `unit=` override; short-way geodesic across the antimeridian |
| **Metric constructive** | `buffer`, `offset_curve` | Distance *inputs* use CRS-natural units (and may use a local-projection construction on a geographic CRS); shapes are **coordinate-planar** — **not** antimeridian auto-split |
| **Topology** | predicates, overlay, `relate`, `clip_by_rect` | strict frame match; geographic seams auto-split-normalize |
| **Coordinate edit** | `quantize`, `snap_to_grid`, affine | raw coordinate space; no metric conversion |
| **Planar constructive** | `convex_hull`, `concave_hull`, `simplify`, Voronoi, Delaunay | operate on stored XY; may need local projection or `split_antimeridian` |

`buffer`/`offset_curve` are *not* exact geodesic buffers: on a geographic CRS the
distance is metres, but the produced outline is a local-projection approximation
(see [Across the antimeridian](#across-the-antimeridian)).

The CRS-units table and the candidate-vs-exact doctrine live in
[the mental model](../get-started/mental-model.md). The shared frame rules:

- **Everything requires one frame — but a frame, not a spelling.** Mixing CRSs —
  or mixing epochs on the same CRS — raises
  [`CRSMismatchError`][gometry.CRSMismatchError] up front; nothing is silently
  reinterpreted. Reproject (`to_crs`) or relabel (`set_crs`, if the metadata is
  what's wrong) first. Two labels that name the *same* frame and differ only in
  axis order — `EPSG:4326` and `OGC:CRS84`, or a projected CRS in N/E versus E/N
  — are accepted everywhere: predicates, measures, overlays, and equally when
  building a `GeometryArray`, concatenating, filling missing rows, or inserting
  into a `SpatialIndex`. The result keeps the left operand's (or the receiver's,
  or the first item's) label. What is *not* accepted is a genuine difference:
  `EPSG:2180` and `EPSG:2177` share datum, ellipsoid and units yet place the
  same coordinate about 3000 km apart, so they still raise.
- **Derived geometries inherit the input frame** — every constructive result
  (`buffer`, `centroid`, `simplify`, overlays, …) carries the CRS *and* epoch of
  its inputs, so metadata never silently drops out of a pipeline.
- **`unit=None` means the CRS-natural unit**, everywhere a `unit=` parameter
  exists: ellipsoidal meters on a geographic CRS, native units on a projected
  CRS, raw coordinate units when CRS-free. Asking a CRS-free geometry for
  `unit='meters'` raises — there is no frame to give meters meaning.
- **`to_crs` transforms X/Y; Z and M follow the ordinate rules below** — Z is
  carried unchanged through horizontal projections and transformed when the
  pipeline consumes it; M is always carried unchanged. Serialization
  constraints are separate: `to_geojson` always raises on M because GeoJSON
  has no M slot. See
  [Z and M under transformation](#z-and-m-under-transformation).
- **Epoch requires a CRS** (`epoch ⟹ crs`): CRS-free constructors reject
  `epoch=`, and clearing the CRS clears the epoch. See
  [Coordinate epochs](#coordinate-epochs).

## `set_crs` vs `to_crs`

!!! warning "The single most important CRS rule"
    `set_crs` relabels; `to_crs` reprojects. If you call `set_crs` when you meant
    `to_crs`, your coordinates keep their old numeric values but claim a new
    meaning — a silent corruption that no exception will catch. When in doubt: did
    the points physically move? If yes, you want `to_crs`.

Consider a point digitized as lon/lat but handed to you with no CRS attached.

```python exec="on" source="block" result="text"
import gometry as gm

raw = gm.Point(21.0, 52.0)          # no CRS yet — just two numbers
print("crs before:", raw.crs)

# DECLARE: the numbers are already WGS84 lon/lat. Coordinates do not move.
declared = raw.set_crs(4326)
print("crs after set_crs:", declared.crs)
print("coords after set_crs:", list(declared.coords))

# TRANSFORM: reproject those lon/lat degrees into Web Mercator meters.
projected = declared.to_crs(3857)
print("crs after to_crs:", projected.crs)
print("coords after to_crs:", list(projected.coords))

```

`set_crs(4326)` left `(21, 52)` untouched and only stamped metadata.
`to_crs(3857)` ran a [PROJ](https://proj.org/) pipeline and produced meters. The
numbers are completely different even though both geometries describe the same
place on Earth.

!!! danger "What misuse looks like"
    Calling `set_crs(3857)` on lon/lat data would *claim* `(21, 52)` is "21 meters
    east, 52 meters north" — a point in the Atlantic off the coast of Africa
    instead of Poland. gometry guards the classic slip: replacing one declared CRS
    with a *different* one raises unless you pass `overwrite=True` (re-tagging is
    almost always a `to_crs` mistake). Use `set_crs` only to attach the CRS the
    coordinates were authored in.

You can pass any CRS spelling PROJ can resolve to either call — an EPSG integer,
an `"EPSG:4326"` string, a CRS name, WKT, or PROJJSON. CRS metadata supplied at a
constructor or via `set_crs` is validated through PROJ *before* it is stored, so a
typo fails fast at the boundary rather than deep inside a later transform:

```python exec="on" source="block" result="text"
import gometry as gm

p = gm.Point(21.0, 52.0)
for crs in (4326, "EPSG:4326", "WGS 84"):
    print(repr(crs), "->", p.set_crs(crs).crs)

try:
    p.set_crs(999999)
except Exception as e:
    print("rejected:", type(e).__name__)

```

A `crs=` argument on any constructor is a `set_crs`, not a `to_crs`. It attaches
metadata to the coordinates you supplied — the bounds stay the lon/lat degrees you
passed:

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
print(area.crs, area.bounds)   # bounds are still the lon/lat degrees you passed

```

!!! tip "Troubleshooting a `CRSMismatchError`"
    Binary operations require both operands to share the same CRS and coordinate
    epoch; a mismatch is never guessed or coerced. Repair the metadata (`set_crs`,
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
You do not pick a separate "planar vs geodesic" engine per call — a geometry's CRS
alone decides the default metric frame (see
[the frame table](../get-started/mental-model.md)). Override the *unit system* with
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
[`geom.length`][gometry.Geometry.length] on a scalar, `arr.area` / `arr.length` on
a [`GeometryArray`][gometry.GeometryArray]. The free functions
[`area`][gometry.area] and [`length`][gometry.length] exist only as **override
paths** that take `unit=`; they are not a second vectorized API. Binary
[`gm.distance`][gometry.distance] is free because it relates two operands.

## Geographic CRS → geodesic, automatically

Attach a geographic CRS and the *same* `geom.area` / `geom.length` / `distance()`
measure geodesically on that ellipsoid
([Karney's algorithm](https://geographiclib.sourceforge.io/)) and return meters.

```python exec="on" source="block" result="text"
import gometry as gm

paris  = gm.Point(2.3522, 48.8566, crs=4326)
london = gm.Point(-0.1276, 51.5072, crs=4326)

d = gm.distance(paris, london)                        # meters (SI), ellipsoidal
print(f"distance: {d/1000:.1f} km")             # ~344 km
print(f"bearing:  {gm.bearing(paris, london):.1f}°")  # initial heading, degrees

```

`length` measures a path; `area` measures a polygon — both on the ellipsoid when
the CRS is geographic:

```python exec="on" source="block" result="text"
import gometry as gm

route = gm.LineString(
    [(2.3522, 48.8566), (-0.1276, 51.5072)], crs=4326
)
print(f"route length: {route.length/1000:.1f} km")

city = gm.box(2, 48, 3, 49, crs=4326)
print(f"city area:    {city.area/1e6:.0f} km^2")

```

For geographic line and polygon metrics, each stored edge is interpreted as the
shortest ellipsoidal geodesic between its endpoints (the stored-edge model —
not necessarily a constant-latitude parallel). WKT and GeoJSON store only
vertices; they do not encode an edge model. When the source means a rhumb line
or a path in a particular projected frame, **generate intermediate vertices with
that intended algorithm** before construction — ordinary densify interpolates the
stored edge model and does not synthesize a rhumb route. Split or otherwise
disambiguate antipodal and antimeridian-sensitive paths.

!!! note "Why geodesic beats \"just use Web Mercator\""
    [Web Mercator](https://epsg.org/crs_3857/WGS-84-Pseudo-Mercator.html) badly
    distorts area and distance away from the equator — the same polygon over Poland
    measures about 2.3x larger in planar Web Mercator than its true ellipsoidal
    area. A geographic CRS measures geodesically with no such distortion because it
    never projects. Keep the geometry geographic whenever you want a truthful
    metric and do not specifically need a projected coordinate frame.

### Bearing and destination — point navigation free functions

gometry exposes the point-to-point geodesic toolkit as free functions under
`gm.`. On a geographic CRS these are geodesic; on a projected or CRS-free point
they are planar.

- `gm.bearing(pt, other)` — initial heading (degrees) from one point to another.
- `gm.destination(pt, bearing, distance)` — walk `distance` in CRS-natural units
  (meters on a geographic CRS; native linear units when projected; coordinate
  units when CRS-free; override with `unit=`) along `bearing` degrees and return
  the arrival point.
- `gm.point_between(a, b, distance, *, normalized=False)` — a point partway
  between two points.

```python exec="on" source="block" result="text"
import gometry as gm

start = gm.Point(2.3522, 48.8566, crs=4326)
arrival = gm.destination(start, 45.0, 100_000.0)   # bearing 45°, 100 km
print(arrival.to_wkt())

```

!!! warning "Argument order: bearing first, then distance"
    `gm.destination(pt, bearing, distance)` takes the **bearing in degrees first**
    and the **distance in meters second**. Swapping them produces a
    plausible-but-wrong point with no error, so keep the order straight.

## Projected CRS → native linear units

The same operations on a *projected* geometry are fast planar maths in the CRS's
**native linear units** — feet stay feet, meters stay meters. Reproject to an
**appropriate projected CRS first**, then measure. Pass `unit='meters'` when you
need SI meters or square meters from a non-meter projected CRS.

```python exec="on" source="block" result="text"
import gometry as gm

city = gm.box(2, 48, 3, 49, crs=4326)
projected = city.to_crs(32631)   # UTM zone 31N, meters

print("crs:", projected.crs)
print(f"area: {projected.area/1e6:.0f} km^2")

```

!!! warning "Why the projection choice matters — the size of the error"
    The same 1° tile over Paris, measured under three CRSs:

    ```python exec="on" source="block" result="text"
    import gometry as gm

    city = gm.box(2, 48, 3, 49, crs=4326)

    geo = city.area / 1e6                          # geographic CRS -> geodesic
    web = city.to_crs(3857).area / 1e6        # Web Mercator
    utm = city.to_crs(32631).area / 1e6       # UTM 31N

    print(f"geodesic (truth): {geo:8.0f} km^2")
    print(f"Web Mercator:     {web:8.0f} km^2")   # >2x too big!
    print(f"UTM 31N:          {utm:8.0f} km^2")   # accurate
    ```

    [Web Mercator](https://epsg.org/crs_3857/WGS-84-Pseudo-Mercator.html) overstates
    the area by more than a factor of two at this latitude — and that error grows
    toward the poles. Reprojecting is how you choose the answer; choose a projection
    suited to the job.

The same tile drawn in each frame shows where that error comes from — Web Mercator
stretches the box vertically away from the equator, inflating its area, while UTM
keeps true local proportions:

```python exec="on" html="true"
from _figures import panels
import gometry as gm

city = gm.box(2, 48, 3, 49, crs=4326)
print(panels([
    ("EPSG:4326 (lon/lat)", city),
    ("EPSG:3857 Web Mercator", city.to_crs(3857)),
    ("EPSG:32631 UTM 31N", city.to_crs(32631)),
]))

```

!!! tip "Choose the projection for the job"
    There is no single "correct" projection. For local area/length, pick a UTM zone
    or a national grid covering your region — measurement on those is accurate and
    fast. Avoid Web Mercator (`EPSG:3857`) for measurement: it is for tiles, not
    areas. For global or trans-continental measurement, keep the geometry
    geographic and let `geom.area` / `geom.length` measure geodesically.

## CRS-free measurement

With no CRS attached, a geometry is just numbers on a flat plane, and measurement
is in whatever unit those coordinates are in.

```python exec="on" source="block" result="text"
import gometry as gm

plot = gm.box(0, 0, 30, 20)        # e.g. meters on a local grid
print("area:     ", plot.area)                                    # 600
print("length:   ", plot.length)                                  # 100
print("distance: ", gm.distance(gm.Point(0, 0), gm.Point(3, 4)))      # 5

```

`unit='planar'` forces this raw coordinate Cartesian math even on a CRS-bearing
geometry — useful for comparing against a geodesic answer:

```python exec="on" source="block" result="text"
import gometry as gm

city = gm.box(2.3, 48.8, 2.4, 48.9, crs=4326)
print("geodesic m^2: ", round(city.area))
print("planar deg^2:", gm.area(city, unit='planar'))  # coordinate Cartesian

```

Every distance is *realized* by an actual pair of points —
[`nearest_points`][gometry.nearest_points] returns it, and
[`shortest_line`][gometry.shortest_line] returns the same answer as the connecting
`LineString` (its `length` is exactly `distance(a, b)`). All three work on **any**
geometry pair, not just points, and find the *true* closest approach: the witness
can land on an **edge interior**, not only a vertex. Here the square's nearest
point to the rotated diamond is mid-edge at `(2, 1)`, not a corner:

```python exec="on" html="true"
from _figures import figure
import gometry as gm

a = gm.from_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))")
b = gm.from_wkt("POLYGON ((4 1, 5 0, 6 1, 5 2, 4 1))")  # a 45°-rotated square
pa, pb = gm.nearest_points(a, b)
bridge = gm.shortest_line(a, b)
print(figure([a, b, bridge, pa, pb], f"shortest_line(a, b) = {bridge.to_wkt()}"))

```

An empty operand is total, not an error: `distance` returns `inf`, while
`nearest_points` and `shortest_line` return the output-type empty
(`(POINT EMPTY, POINT EMPTY)` and `LINESTRING EMPTY`), so a single empty row never
aborts a vectorized call. When operands differ in dimensionality (an `XYZM` line
and an `XY` point) the witnesses drop to the ordinates both carry.

!!! note "Coordinate-space tolerances stay in raw units"
    A few operations take a tolerance that is always interpreted in raw coordinate
    units, regardless of CRS — they *remove or move* vertices, and reprojecting
    would move the survivors:
    [`simplify`][gometry.Geometry.simplify], [`snap`][gometry.snap],
    [`snap_to_grid`][gometry.Geometry.snap_to_grid],
    [`quantize`][gometry.Geometry.quantize], and
    [`remove_repeated_points`][gometry.Geometry.remove_repeated_points]. On lon/lat
    data such a tolerance is in degrees; reproject with
    [`to_crs`][gometry.Geometry.to_crs] first if you need it in meters. Every
    distance- or area-returning metric — including
    [`hausdorff_distance`][gometry.hausdorff_distance],
    [`frechet_distance`][gometry.frechet_distance],
    [`minimum_bounding_radius`][gometry.Geometry.minimum_bounding_radius],
    [`minimum_clearance`][gometry.Geometry.minimum_clearance], and
    [`nearest_points`][gometry.nearest_points] — is CRS-aware with a
    `unit='planar'` escape (and `unit='meters'` for forced SI).

!!! note "`segmentize` is CRS-aware, because it only inserts"
    [`segmentize`][gometry.Geometry.segmentize] does **not** belong to the list
    above: it only *inserts* vertices along existing segments, so every original
    survives untouched and the reprojection argument never applied to it. Its
    `max_length` is therefore a real-world distance measured exactly like
    [`length`][gometry.length] — meters along the ellipsoid on a geographic CRS,
    native units on a projected one, coordinate units when CRS-free — with the
    same `unit='planar'` / `unit='meters'` escapes as every other metric.

    ```python exec="on" source="block" result="text"
    import gometry as gm

    line = gm.LineString([(0, 0), (1, 0)], crs=4326)
    print(f"length: {gm.length(line):,.0f} m")
    print(f"segmentize(20_000): {len(list(line.segmentize(20_000).coords))} vertices")
    print(f"unit='planar':      {len(list(line.segmentize(0.25, unit='planar').coords))} vertices")
    ```

## Picking a projected CRS automatically

When you want *planar* measurement — for speed, or because a downstream tool wants
projected coordinates — reproject to a CRS whose units are meters. Call
[`geom.estimate_local_crs()`][gometry.Geometry.estimate_local_crs] on the geometry
or array whose full extent must fit. It prefers a datum-aware
[UTM](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system)
or UPS authority CRS, then a local conformal CRS, and accepts a candidate only
when estimated linear scale error stays within 0.1% over the extent.

```python exec="on" source="block" result="text"
import gometry as gm

warsaw = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
local = warsaw.estimate_local_crs()
print("local:", local)                         # EPSG:32634 (UTM zone 34N)

# Geographic CRS measures geodesically; the UTM frame measures planar meters.
print("geodesic area m^2:", round(warsaw.area))
print("projected area m^2:", round(warsaw.to_crs(local).area))

```

The projected planar area and the ellipsoidal geodesic area agree closely — exactly
because the estimator chose an appropriate local projection. It normalizes
projected inputs to lon/lat for extent planning while preserving datum-specific
authority choices (for example NAD83 UTM) where available.

### Reproject to a local UTM zone

The end-to-end recipe: let gometry pick the zone, `to_crs` into it, then every
metric is plain planar meters in that fixed frame.

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(2.30, 48.84, 2.39, 48.89, crs=4326)  # central Paris, lon/lat

local = area.estimate_local_crs()        # picks the right UTM zone
projected = area.to_crs(local)

print('local CRS:', local.to_authority())
print('planar area m^2:', round(projected.area))

```

Once projected, a `buffer(500)` is a 500-meter buffer, `distance` is in meters,
`area` in square meters:

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(2.30, 48.84, 2.39, 48.89, crs=4326)
projected = area.to_crs(area.estimate_local_crs())

ring = projected.buffer(500)              # 500 meters in the local UTM frame
print('500 m buffer area m^2:', round(ring.area))

```

```python exec="on" html="true"
from _figures import result_over_input
import gometry as gm

area = gm.box(2.30, 48.84, 2.39, 48.89, crs=4326)
projected = area.to_crs(area.estimate_local_crs())
ring = projected.buffer(500)
print(result_over_input(projected, ring, after_caption="buffer(500 m)"))

```

!!! tip "You often don't need this — and buffering follows the CRS either way"
    On a geographic CRS, `area` / `length` / `distance` are already **geodesic
    meters**, and `geom.buffer(1000)` is a **1000-meter** local-projection buffer
    (an approximation — not an ellipsoidal offset). On a projected geometry the
    distance is in that CRS's **native linear units** (feet stay feet;
    `unit='meters'` for SI); on a CRS-free geometry it is bare coordinate units.
    Reproject only when you want repeated planar math in a fixed local frame, or to
    feed code that requires projected coordinates. See the
    [degrees-vs-meters trap in bulk code](arrays.md) and
    [Constructive operations](constructive.md).

The *same* metric distance draws differently depending on **where** it is measured.
A 250 km buffer at the equator and at 55°N — both `250_000` meters through the local
projection — become ellipses of different shapes once drawn back in lon/lat, because
a degree of longitude shortens toward the poles:

```python exec="on" html="true"
from _figures import panels
import gometry as gm
print(panels([(f'250 km buffer at {lat}°N (drawn in lon/lat)', gm.Point(3, lat, crs=4326).buffer(250000)) for lat in (0, 55)]))

```

This distortion is exactly why gometry measures through the CRS instead of in raw
degrees.

## Across the antimeridian

On a geographic CRS, gometry's **topology** ops — predicates, relate, overlay,
`clip_by_rect`, centroid, `point_on_surface`, bounds, distance/`dwithin`, the
spatial index, `PreparedGeometry`, and validation/repair (`is_valid`,
`validate`, `require`, `self_intersections`, `repair`, and
`snap_to_grid(..., repair=True)`) — **auto-split-normalize**
antimeridian-crossing input at their chokepoints. Geodesic measures (`distance`,
`length`, `area`, …) take the short way across the seam. The **stay-planar**
constructive ops (`convex_hull`, `concave_hull`, `buffer`, `offset_curve`,
`simplify`, Voronoi, Delaunay) do **not** auto-split; call
[`split_antimeridian`][gometry.Geometry.split_antimeridian] first on crossing
input. A crossing geometry's [`bounds`][gometry.Geometry.bounds] reports
`west > east` (minx > maxx) — the established geographic convention.

For WGS 84 boxes that wrap longitude 180/−180, use `wrap="split"` so the visible
geometry is split into valid pieces. For arbitrary crossing linework, call
`split_antimeridian()` before planar algorithms that document that requirement:

```python exec="on" html="true"
from _figures import antimeridian_before_after
import gometry as gm
line = gm.LineString([(170, -10), (-170, 10)], crs=4326)
print(antimeridian_before_after(line, line.split_antimeridian()))

```

```python exec="on" source="block" result="text"
import gometry as gm

region = gm.box(170, -10, -170, 10, crs=4326, wrap="split")
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

Spatial indexes widen antimeridian-crossing envelopes before candidate search and
still run exact refinement, so the middle-of-map false positives are removed by the
predicate step. The auto-split-vs-planar rule above is the complete per-op story:
topology and indexing normalize for you; the planar constructive ops listed there
are the only places manual splitting is still required.

## Z and M under transformation

`to_crs` transforms X and Y. It carries **Z** through unchanged when the target is
a horizontal projection (e.g. Web Mercator), and transforms Z through the pipeline
when the operation actually consumes it (geocentric, vertical-capable, and
compound workflows). It never invents or silently drops Z. **M** (the measure
ordinate — timestamps, route distances) is not a spatial axis, so a coordinate
transform has no defined action for it and `to_crs` carries M through
**unchanged**. The full Z/M behavior is explained in the
[geometry guide](geometry.md).

```python exec="on" source="block" result="text"
import gometry as gm

# 2D transform: Z absent, nothing to carry.
flat = gm.Point(21.0, 52.0, crs=4326).to_crs(3857)
print("XY  ->", flat.coordinate_axes, "| has_z:", flat.has_z)

# XYZ transform: Z survives the reprojection.
elev = gm.Point(21.0, 52.0, z=100.0, crs=4979).to_crs(3857)
print("XYZ ->", elev.coordinate_axes, "| has_z:", elev.has_z)

# M is application data: carried verbatim, never reprojected.
trace = gm.Point(21.0, 52.0, m=1_700_000_000.0, crs=4326).to_crs(3857)
print(trace.coordinate_axes, "| m unchanged:", trace.m)

```

!!! note "M is application data, not a coordinate"
    A timestamp or route measure has no datum, so no pipeline can "reproject" it —
    carrying it through verbatim is the only honest action. If the measures should
    not survive a reprojection in your model, drop them explicitly with
    `set_m(None)` (or `force_2d()` to drop both Z and M) before or after the
    transform.

## Coordinate epochs

Dynamic reference frames (ITRF, the WGS 84 realizations, modern national datums)
keep moving with the tectonic plates, so a coordinate is only fully defined
together with the **decimal year** it was observed — its *coordinate epoch*.
gometry carries that as first-class metadata, `geom.epoch`, distinct from Z and M.
A coordinate epoch dates a CRS realization, so it is **only meaningful with a CRS**:
gometry enforces `epoch ⟹ crs` at every entry point.

```python exec="on" source="block" result="text"
import gometry as gm

# Stamp / clear the epoch without moving coordinates (mirrors set_crs).
station = gm.Point(21.0, 52.0, z=100.0, crs=4979)
observed = station.set_epoch(2020.0)
print("epoch:", observed.epoch, "| coords unchanged:", list(observed.coords) == list(station.coords))

# A CRS-free coordinate has no frame to date — an epoch is rejected.
try:
    gm.Point(21.0, 52.0).set_epoch(2020.0)
except gm.CRSError as error:
    print("rejected:", error)

# Clearing the CRS clears the epoch (CRS-free + epoch is incoherent).
print("after set_crs(None):", observed.set_crs(None).epoch)

```

`set_epoch` is the assign/clear setter (`set_epoch(None)` clears; changing a
present epoch needs `overwrite=True`, like `set_crs`). When you reproject, the
**source** epoch is the geometry's own `epoch`, and `to_crs(..., epoch=...)` labels
the **output** epoch — there is no separate `source_epoch`/`target_epoch` on the
geometry surface (those live on the raw `gm.crs_transform(...)` /
`CRS.operation(...)` coordinate APIs, which have no geometry metadata to read).

```python exec="on" source="block" result="text"
import gometry as gm

# Source epoch comes from the geometry; epoch= sets the output epoch.
observed = gm.Point(21.0, 52.0, z=100.0, crs=4979, epoch=2010.0)
geocentric = observed.to_crs(4978, epoch=2020.0)
print("output epoch:", geocentric.epoch)

```

When you omit `epoch=`, the source epoch survives **exactly while it still
means something**: it is kept when the target CRS is *dynamic* (ITRF, the
WGS 84 ensemble — coordinates there stay time-dependent) and cleared
automatically when the target is *static* (plate-fixed national frames, where
an epoch adds nothing and would only block strict frame checks against
ordinary epoch-free data in the same CRS).

```python exec="on" source="block" result="text"
import gometry as gm

observed = gm.Point(21.0, 52.0, crs=4326, epoch=2010.0)
print("to ITRF2014 (dynamic): ", observed.to_crs(9000).epoch)
print("to Poland CS92 (static):", observed.to_crs(2180).epoch)

```

## The `CRS` object and standards export

`geom.crs` returns a first-class [`gm.CRS`][gometry.CRS] — not a bare string. It
compares equal to the spellings you expect and carries PROJ's whole introspection
surface as properties and methods, so you query a CRS without reaching for a
separate global utility.

```python exec="on" source="block" result="text"
import gometry as gm

c = gm.Point(21.0, 52.0, crs=4326).crs
print(repr(c))                 # CRS("EPSG:4326")
print(c == "EPSG:4326", c == 4326)   # True True
print("name:", c.name, "| kind:", c.kind)
print("geographic?", c.is_geographic, "| projected?", gm.CRS(3857).is_projected)
print("normalize 'epsg:4326':", gm.CRS("epsg:4326").canonical)
print("local EPSG:", gm.box(20, 51, 22, 53, crs=4326).estimate_local_crs().to_epsg())

```

`CRS.__eq__` is structural and compares the stored canonical label. Use
`same_as(..., mode="ignore_axis_order")` when asking whether two labels are
operationally interchangeable; `EPSG:4326` and `OGC:CRS84` remain unequal as
stored CRS values even though geometry operations accept their shared X/Y
frame.

Per-CRS introspection lives on the object as properties — `is_geographic`,
`is_projected`, `kind`, `name`, `authority`, `code`, `axis_order`, `axes`,
`area_of_use`, `ellipsoid`, `datum`, and more — plus methods like
[`to_wkt`][gometry.CRS.to_wkt], [`to_proj`][gometry.CRS.to_proj],
[`identify`][gometry.CRS.identify], and [`same_as`][gometry.CRS.same_as]. EPSG:4326's
*native* axis order is latitude-first (its declared axis abbreviations are
`Lat, Lon`), but **gometry's geometry and transform APIs ignore that and are always
X/Y (lon, lat)** — a deliberate safety choice, so you never have to ask "does this
function want lat first or lon first?"

```python exec="on" source="block" result="text"
import gometry as gm

c = gm.CRS(4326)
print("EPSG:4326 native axes:", [ax["abbreviation"] for ax in c.axes])  # ['Lat', 'Lon']
print("axis_order:", c.axis_order)                                      # ['lat', 'lon']

# gometry is X/Y regardless: Point(x=lon, y=lat).
p = gm.Point(21.0, 52.0, crs=4326)
print("gometry reads as (lon, lat):", list(p.coords))

```

!!! warning "Axis order is always X/Y in gometry"
    gometry is always X/Y: `Point(x, y)` is `(lon, lat)` for geographic CRSs and
    `(easting, northing)` for projected ones — there is no axis-order mode switch.
    If you are porting pyproj code, drop the lat/lon swaps.
    [`gm.crs_info(x)`][gometry.crs_info] returns a plain dictionary of PROJ
    metadata when you want raw fields to iterate or serialize, but for everyday work
    prefer the typed [`CRS`][gometry.CRS] properties above.

Round-trip a CRS to the standard text formats with
[`CRS.to_wkt`][gometry.CRS.to_wkt] ([ISO 19162](https://www.iso.org/standard/76496.html)
/ OGC WKT for CRS — not geometry WKT), [`CRS.to_proj`][gometry.CRS.to_proj], and
the PROJJSON helpers:

```python exec="on" source="block" result="text"
import gometry as gm

print(gm.CRS(4326).to_proj())
print(gm.CRS(4326).to_wkt()[:60], "...")

```

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

# Reproject a bounding box (with edge densification for accuracy):
print(gm.crs_transform_bounds(4326, 3857, (20.0, 51.0, 22.0, 53.0)))

```

[`gm.crs_transform`][gometry.crs_transform] accepts optional `z=` and `t=` (time)
ordinates for 3D/4D pipelines; `t` selects the time-dependent operation but is not
returned. [`gm.crs_apply`][gometry.crs_apply] runs an explicit PROJ
operation/pipeline string. For everyday geometry work prefer
[`geom.to_crs`][gometry.Geometry.to_crs]; reach for the raw functions only when you
are moving bare numbers.

Transforms sometimes need regional grid files. If the best operation's grid is
unavailable, every Python-visible transform surface — `geom.to_crs(...)`,
`GeometryArray.to_crs(...)`, `gm.crs_transform(...)`, and
`gm.crs_transform_bounds(...)` — uses PROJ's valid lower-accuracy fallback and emits
[`AccuracyWarning`][gometry.AccuracyWarning] once for that call. The warning names
the missing grid and links to PROJ's resource-file guidance. Pass
`only_best=True` when fallback is unacceptable: the same condition then raises
[`TransformError`][gometry.TransformError].

[`CRS.operations`][gometry.CRS.operations] deliberately enumerates PROJ's
ranked candidates even when their grids are not installed. This keeps the
operation list complete; each grid's `available` field states whether that
candidate can run in the current configuration. [`gm.crs_grid`][gometry.crs_grid]
adds the database `url` and `direct_download` flag so callers can locate an
unavailable resource. Configure local grid directories with
[`gm.crs_configure`][gometry.crs_configure]; `gm.crs_engine()['paths']` reports
those effective per-context search paths.

For performance diagnostics, [`gm.crs_cache_info`][gometry.crs_cache_info]
reports ``last_transform_engine`` as ``'in_core'`` or ``'proj'`` for the most
recent transform on the current thread, plus ``transform_invocations`` since
the last cache clear/reset. The count records actual in-core batches and PROJ
calls, so empty or validation-rejected input does not claim an execution. This
observes the engine selected by the transform itself; it does not infer
execution from whether a PROJ cache entry remains.

[PROJ](https://proj.org/) is the bundled authority backend, so [EPSG](https://epsg.org/)
codes, datum pipelines, grid-aware transforms, [WKT](https://www.ogc.org/standard/sfa/),
and PROJJSON behave as you expect — without requiring a system PROJ shared library
in the wheel.

Coming from pyproj? See [Migrating](../migrating/index.md#coming-from-pyproj).

## See also

- [The mental model](../get-started/mental-model.md) — the frame-doctrine table
  and the candidate-vs-exact rule.
- [Geometry](geometry.md) — construction, dimensions, and the full Z/M rules.
- [Arrays](arrays.md) — columnar `.area` / `.length` and the degrees-vs-meters
  trap in bulk code.
- [Constructive operations](constructive.md) — `buffer`, `offset_curve` (same
  metric rule).
- [Validation & repair](validation.md) — `crosses_antimeridian` and
  [`split_antimeridian`][gometry.Geometry.split_antimeridian] at ingest time.
- [Spatial indexing](indexing.md) — envelope widening for crossing rows.
- [API: CRS][gometry.CRS] · [Geometry.to_crs][gometry.Geometry.to_crs] ·
  [crs_transform][gometry.crs_transform] · [distance][gometry.distance] ·
  [area (unit=)][gometry.area] · [length (unit=)][gometry.length]
