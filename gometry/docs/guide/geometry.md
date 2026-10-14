---
description: Constructing, inspecting, and comparing gometry's seven geometry types, plus XY/XYZ/XYM/XYZM layouts and Z/M behavior through operations.
---

# Geometry & dimensions

Every value in gometry is a [`Geometry`][gometry.Geometry]: one of exactly **seven** immutable,
hashable [OGC Simple Features](https://www.ogc.org/standard/sfa/) types, each
optionally carrying a CRS and per-vertex Z (elevation) and M (measure)
ordinates.

| Type | Made of | Constructor |
|------|---------|-------------|
| [`Point`][gometry.Point] | one coordinate | [`gm.Point`][gometry.Point] |
| [`LineString`][gometry.LineString] | ordered vertices | [`gm.LineString`][gometry.LineString] |
| [`Polygon`][gometry.Polygon] | a shell + holes | [`gm.Polygon`][gometry.Polygon] / [`gm.box`][gometry.box] |
| [`MultiPoint`][gometry.MultiPoint] | many points | [`gm.MultiPoint`][gometry.MultiPoint] |
| [`MultiLineString`][gometry.MultiLineString] | many lines | [`gm.MultiLineString`][gometry.MultiLineString] |
| [`MultiPolygon`][gometry.MultiPolygon] | many polygons | [`gm.MultiPolygon`][gometry.MultiPolygon] |
| [`GeometryCollection`][gometry.GeometryCollection] | any mix | [`gm.GeometryCollection`][gometry.GeometryCollection] |

!!! tip "Always (x, y) — lon/lat only under a geographic CRS"
    Every constructor takes coordinates in **(x, y)** order. Under a
    **geographic** CRS those axes are **(longitude, latitude) in that CRS's
    native angular units** (degrees, radians, grads, or another admitted angular
    unit); under a
    projected CRS they are the projection's easting/northing (or other linear
    axes); CRS-free geometries stay in raw coordinate units. The lon/lat
    reading is not universal — it is the geographic case. Pass `crs=4326` (or
    another geographic CRS) on [`gm.Point`][gometry.Point] and the bulk builders
    when the input is geographic.

## Points

Construct a point with [`gm.Point(x, y)`][gometry.Point]; under a geographic CRS, `(x, y)` is
`(longitude, latitude)` in that CRS's native angular units.

```python exec="on" source="block" result="text"
import gometry as gm

paris = gm.Point(2.3522, 48.8566, crs=4326)
print(paris.to_wkt())
print("x:", paris.x, "y:", paris.y)
print("crs:", paris.crs, "coordinate_axes:", paris.coordinate_axes)

```

!!! note "`.x` / `.y` are for single points only"
    A `Point` exposes [`.x`][gometry.Point.x], [`.y`][gometry.Point.y] (and [`.z`][gometry.Point.z] / [`.m`][gometry.Point.m] when present) for scalar
    access. For any other geometry, pull every vertex out with
    [`geom.coords`][gometry.Geometry.coords] (see
    [Inspecting geometries](#inspecting-geometries)).

## LineStrings

A [`LineString`][gometry.LineString] is an ordered sequence of vertices. Pass any sequence of
`(x, y)` pairs or an `(N, 2)` NumPy-style array — gometry copies the coordinates
once into its Rust geometry representation, so large inputs avoid per-point
Python object construction.

```python exec="on" source="block" result="text"
import numpy as np
import gometry as gm

coords = np.array([(0, 0), (1, 2), (3, 1), (4, 3)], dtype=float)
line = gm.LineString(coords)
print(line.to_wkt())
print("length:", line.length)

```

```python exec="on" html="true"
from _figures import figure, with_vertices
import gometry as gm
print(figure(
    with_vertices(gm.LineString([(0, 0), (1, 2), (3, 1), (4, 3)])),
    "Four ordered LineString vertices",
    alt="The LineString follows all four vertices in order from left to right.",
))

```

## Polygons

A [`Polygon`][gometry.Polygon] is a **shell** (exterior ring) plus zero or more **holes**
(interior rings). Rings are closed automatically — you do not need to repeat
the first vertex at the end, though it is harmless if you do.

```python exec="on" source="block" result="text"
import gometry as gm

square_with_hole = gm.Polygon(
    [(0, 0), (4, 0), (4, 4), (0, 4)],
    holes=[[(1, 1), (2, 1), (2, 2), (1, 2)]],
)
print(square_with_hole.to_wkt())
print("holes:", len(square_with_hole.interiors))
print("area:", square_with_hole.area)

```

```python exec="on" html="true"
from _figures import figure
import gometry as gm
print(figure(gm.Polygon(
    [(0, 0), (4, 0), (4, 4), (0, 4)],
    holes=[[(1, 1), (2, 1), (2, 2), (1, 2)]],
), "Shell and interior ring", alt="The polygon shell encloses one square hole."))

```

!!! note "Constructors check structure, not topology"
    `gm.Polygon` rejects *structurally* malformed input eagerly — non-finite
    coordinates, or a ring with fewer than three vertices, raise a
    [`InvalidGeometryError`][gometry.InvalidGeometryError].
    It does **not** run full OGC validity, so a self-intersecting "bowtie" is
    built as an *invalid* geometry rather than rejected. Detect it with
    [`geom.validate()`][gometry.Geometry.validate] and fix it with [`repair`][gometry.Geometry.repair] — see
    [Validation](validation.md).

### Boxes

For the common axis-aligned rectangle, [`gm.box`][gometry.box] is a shortcut
taking `(minx, miny, maxx, maxy)`:

```python exec="on" source="block" result="text"
import gometry as gm

extent = gm.box(0, 0, 10, 5)
print(extent.to_wkt())
print("bounds:", extent.bounds)

```

## Multi-part geometries

Multi-geometries group several parts of the **same** type.
[`gm.MultiPoint`][gometry.MultiPoint] accepts a sequence of coordinate pairs:

```python exec="on" source="block" result="text"
import numpy as np
import gometry as gm

cloud = gm.MultiPoint(np.random.default_rng(0).random((5, 2)).tolist())
print(cloud.geometry_type, "with", len(cloud), "points")

```

[`MultiLineString`][gometry.MultiLineString] and [`MultiPolygon`][gometry.MultiPolygon] take sequences of part-coordinate
sequences and part geometries respectively:

```python exec="on" source="block" result="text"
import gometry as gm

mls = gm.MultiLineString([
    [(0, 0), (1, 1)],
    [(2, 0), (3, 1)],
])
mp = gm.MultiPolygon([
    gm.box(0, 0, 1, 1),
    gm.box(2, 2, 3, 3),
])
print(mls.to_wkt())
print(mp.to_wkt())

```

Explicit empty members are retained in their original order and count. For
example, `gm.MultiLineString([gm.LineString(), [(0, 0), (1, 1)]])` renders as
`MULTILINESTRING (EMPTY, (0 0, 1 1))`; use `.parts` to inspect those typed
empty members. Multipart members must share one coordinate-axis layout.

### GeometryCollections

When parts have **mixed** types, use a [`GeometryCollection`][gometry.GeometryCollection]:

```python exec="on" source="block" result="text"
import gometry as gm

gc = gm.GeometryCollection([
    gm.Point(0, 0),
    gm.LineString([(1, 1), (2, 2)]),
    gm.box(3, 3, 4, 4),
])
print(gc.to_wkt())

```

!!! note "Collection area counts overlaps once; length sums members"
    [`.area`][gometry.Geometry.area] measures the union of a collection's
    areal support, so overlapping members count once. [`.length`][gometry.Geometry.length]
    sums every member's length or perimeter, including overlapping members.
    Points and lines contribute zero area.
    Prefer homogeneous multi-types when you want a single-kind column; reserve
    collections for genuinely heterogeneous
    results (e.g. an overlay that returns both lines and points, or the
    [tessellation operations](constructive.md#tessellation-triangulate-voronoi-polygonize)).

## Inspecting geometries

These properties and functions apply to every geometry type:

| Call | Returns |
|------|---------|
| [`geom.geometry_type`][gometry.Geometry.geometry_type] | the type name, e.g. `"Polygon"` |
| [`geom.coordinate_axes`][gometry.Geometry.coordinate_axes] | ordinate layout: `"XY"`, `"XYZ"`, `"XYM"`, `"XYZM"` |
| [`geom.is_empty`][gometry.Geometry.is_empty] | whether the geometry holds no coordinates |
| [`bounds`][gometry.Geometry.bounds] | `(minx, miny, maxx, maxy)` tuple |
| [`geom.coords`][gometry.Geometry.coords] | every vertex as a flat, indexable [`Coordinates`][gometry.Coordinates] view |
| [`geom.parts`][gometry.Geometry.parts] | component geometries of a multi-part |
| [`poly.exterior`][gometry.Polygon.exterior] / [`poly.interiors`][gometry.Polygon.interiors] | the shell and hole rings of a [`Polygon`][gometry.Polygon] |

```python exec="on" source="block" result="text"
import gometry as gm

poly = gm.Polygon(
    [(0, 0), (4, 0), (4, 4), (0, 4)],
    holes=[[(1, 1), (2, 1), (2, 2), (1, 2)]],
)

print("type:", poly.geometry_type)
print("coordinate_axes:", poly.coordinate_axes)
print("bounds:", poly.bounds)
print("vertices:", len(poly.coords))
print("parts:", len(poly.parts))
print("holes:", len(poly.interiors))

```

`geom.coords` is a flat, indexable `Coordinates` view over **every** vertex
(each ring, each part, flattened depth-first). Index it for tuples, iterate it,
or pull whole axes as columns and hand
them straight to NumPy, pandas, or polars without a Python loop:

```python exec="on" source="block" result="text"
import numpy as np
import gometry as gm

coords = gm.LineString([(0, 0), (1, 2), (3, 1)], z=[10.0, 20.0, 30.0]).coords

print("len:", len(coords))
print("first tuple:", coords[0])
print("axes:", coords.coordinate_axes)
print("x column:", repr(np.asarray(coords.x)))  # zero-copy ndarray
print("dict keys:", list(coords.to_dict().keys()))

```

[`.x`][gometry.Coordinates.x]/[`.y`][gometry.Coordinates.y] (and [`.z`][gometry.Coordinates.z]/[`.m`][gometry.Coordinates.m] when present) are zero-copy columns; [`.to_dict()`][gometry.Coordinates.to_dict] returns
read-only NumPy columns such as `{'x': ndarray, 'y': ndarray}` for direct use
with `pd.DataFrame` or `pl.DataFrame` (forced Z/M use `NaN` where absent);
`np.asarray(coords)` materializes an `(N, dims)` array; and [`.index`][gometry.Coordinates.index] tags each vertex
with its source row when the geometry is a [`GeometryArray`](arrays.md).

## Geometry types

Every geometry is an instance of a concrete subclass of
[`Geometry`][gometry.Geometry] — [`Point`][gometry.Point],
[`LineString`][gometry.LineString], [`Polygon`][gometry.Polygon],
[`MultiPoint`][gometry.MultiPoint], [`MultiLineString`][gometry.MultiLineString],
[`MultiPolygon`][gometry.MultiPolygon], or
[`GeometryCollection`][gometry.GeometryCollection] — so `isinstance` works and
constructors/operations return the precise type:

```python exec="on" source="block" result="text"
import gometry as gm

print(type(gm.Point(1, 2)).__name__)
print(type(gm.box(0, 0, 1, 1)).__name__)
print(isinstance(gm.box(0, 0, 1, 1), gm.Polygon))

```

A `Point` exposes [`.x`][gometry.Point.x]/[`.y`][gometry.Point.y]/[`.z`][gometry.Point.z]/[`.m`][gometry.Point.m]; a `Polygon` exposes [`.exterior`][gometry.Polygon.exterior] (a closed
`LineString`) and
[`.interiors`][gometry.Polygon.interiors] (its holes); and the multi-part / collection types are real
sequences — iterate them, index them, and take `len(...)`, with each part typed:

The multi-part classes are also registered as `collections.abc.Sequence`, so
`isinstance` checks and ABC-based dispatch recognize the sequence interface.
Their static types are `Sequence[Point]`, `Sequence[LineString]`,
`Sequence[Polygon]`, or `Sequence[Geometry]`. Use class patterns such as
`MultiPoint(parts)` for matching; anonymous sequence patterns do not match
geometry objects. NumPy still treats each multipart geometry as one object:
`np.asarray(multi)` has shape `()`, and `np.array([multi, multi], dtype=object)`
has shape `(2,)`.

```python exec="on" source="block" result="text"
import gometry as gm

poly = gm.Polygon([(0, 0), (4, 0), (4, 4), (0, 4)], holes=[[(1, 1), (2, 1), (2, 2), (1, 2)]])
print("exterior:", poly.exterior.geometry_type, "| holes:", len(poly.interiors))

mp = gm.MultiPolygon([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)])
print("parts:", len(mp), "| first:", mp[0].geometry_type, "| areas:", [p.area for p in mp])

```

Accessing a member on the wrong type raises `AttributeError`: `line.x` fails
(only points have coordinates).

### Equality and hashing

Geometries are **value objects**: `==` and `hash()` compare the CRS, coordinate
epoch, and exact geometry (type, coordinates including Z/M, and vertex order), so
geometries work in `set`s and as `dict` keys:

```python exec="on" source="block" result="text"
import gometry as gm

a = gm.LineString([(0, 0), (1, 1)])
b = gm.LineString([(0, 0), (1, 1)])
reversed_ = gm.LineString([(1, 1), (0, 0)])
print("a == b:", a == b, "| a == reversed:", a == reversed_)
print("distinct:", len({a, b, reversed_}))

```

This `==` is **structural** (vertex-order sensitive). For the topological
"same shape on the map" test that ignores vertex order, use
[`gm.equals`][gometry.equals]; for a representation-independent vertex check,
apply [`normalize`][gometry.Geometry.normalize] to both inputs before calling
[`gm.equals_exact`][gometry.equals_exact].

## Dimensions: XY, XYZ, XYM, XYZM

gometry supports four coordinate layouts — **XY, XYZ, XYM, and XYZM** — from the
constructors through IO.

### Three meanings of "dimension"

"Dimension" has three distinct meanings in GIS, and gometry keeps them separate:

| Concept | Property | Example value |
|---|---|---|
| **Topological dimension** | [`geom.topological_dimension`][gometry.Geometry.topological_dimension] | `0` (Point), `1` (LineString), `2` (Polygon) |
| **Coordinate axes / ordinate layout** | [`geom.coordinate_axes`][gometry.Geometry.coordinate_axes] | `"XY"`, `"XYZ"`, `"XYM"`, `"XYZM"` |
| **Has-ordinate flags** | [`geom.has_z`][gometry.Geometry.has_z], [`geom.has_m`][gometry.Geometry.has_m] | `True` / `False` |

```python exec="on" source="block" result="text"
import gometry as gm

trace = gm.LineString(
    [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)],
    z=[10.0, 20.0, 30.0],
    m=[0.0, 1.0, 2.0],
    crs=4326,
)
print("geometry_type:", trace.geometry_type)
print("topological_dimension:", trace.topological_dimension)
print("coordinate_axes:", trace.coordinate_axes)
print("has_z / has_m:", trace.has_z, "/", trace.has_m)

```

### Z is a spatial axis; M is a measure

- **Z** is an additional *spatial* ordinate, usually elevation or height. gometry does **not**
  assume Z is in meters unless a vertical CRS says so — Z is a third spatial number
  until a CRS gives it units.
- **M** is a *measure* ordinate. It commonly carries linear-referencing values, route
  measures, distance-along-line, per-vertex timestamps, or application-specific event
  alignment. **M is not a spatial axis** and must never be silently treated as Z. M is also
  distinct from the [coordinate epoch](crs.md#coordinate-epochs) ([`geom.epoch`][gometry.Geometry.epoch]): the
  epoch is a single decimal year that dates the whole geometry's CRS realization, whereas M is
  a free per-vertex value with no datum.

A GPX track or vehicle trace has both elevation (Z) and a
capture timestamp or route measure (M) aligned to each vertex. gometry lets you store both on
the geometry instead of the common workaround of a 2D line plus side arrays:

```python exec="on" source="block" result="text"
import gometry as gm

track = gm.LineString(
    [(21.00, 52.00), (21.01, 52.01), (21.02, 52.00)],
    z=[105.0, 118.0, 110.0],
    m=[1_700_000_000, 1_700_000_030, 1_700_000_060],
    crs=4326,
)
coords = list(track.coords.select('XYZM'))
print("coordinate_axes:", track.coordinate_axes)
for x, y, z, m in coords:
    print(f"  lon={x} lat={y} elev={z} t={int(m)}")

```

[`coords.select(...)`][gometry.Coordinates.select] chooses a fixed coordinate-view
layout for nested output, iteration, ndarray/dict exports, and equality without
changing the geometry.

### Explicit constructors identify ambiguous tuples

A bare 3-tuple `(x, y, v)` is ambiguous — it could be XYZ or XYM. Use the
`z=` and `m=` keywords to identify the layout:

```python exec="on" source="block" result="text"
import gometry as gm

p_xy   = gm.Point(21.0, 52.0, crs=4326)
p_xyz  = gm.Point(21.0, 52.0, z=105.0, crs=4326)
p_xym  = gm.Point(21.0, 52.0, m=1_700_000_000.0, crs=4326)
p_xyzm = gm.Point(21.0, 52.0, z=105.0, m=1_700_000_000.0, crs=4326)

for p in (p_xy, p_xyz, p_xym, p_xyzm):
    print(p.coordinate_axes, "→", p.to_wkt())

```

For arrays and lines, pass `z=` and `m=` keywords to [gm.points][gometry.points] and [gm.LineString][gometry.LineString]:

```python title="partial: requires coordinate arrays from the surrounding application"
points_xy = gm.points(lons, lats, crs=4326)
points_zm = gm.points(lons, lats, z=elevations, m=timestamps, crs=4326)
line      = gm.LineString(xy, z=elevations, m=timestamps, crs=4326)

```

!!! note "Consistent layout per scalar geometry"
    A non-collection geometry must use one coordinate-axis layout for every vertex — a
    [`LineString`][gometry.LineString] cannot mix XY and XYZM vertices. A [`GeometryCollection`][gometry.GeometryCollection] may hold children
    with different layouts, but the API makes that explicit rather than silent.

WKT carries the `Z`, `M`, and `ZM` dimensional tags, which round-trip through gometry:

```python exec="on" source="block" result="text"
import gometry as gm

print(gm.from_wkt("POINT Z (1 2 3)").coordinate_axes)
print(gm.from_wkt("POINT M (1 2 7.5)").coordinate_axes)
print(gm.from_wkt("POINT ZM (1 2 3 7.5)").coordinate_axes)

```

## Measuring with Z and M

**3D spatial measurement** uses the Z axis. [`length_3d`][gometry.Geometry.length_3d] and point-to-point
[`distance_3d(other)`][gometry.distance_3d] include the vertical component, and [`min_z`][gometry.Geometry.min_z] / [`max_z`][gometry.Geometry.max_z]
(properties) summarize the Z extent that complements the XY [`bounds`][gometry.Geometry.bounds]. Because a 3D length only makes
sense when every axis shares one linear unit, these require a **projected** CRS (or no
CRS); a geographic CRS mixes degrees and metre heights and raises under every `unit=` —
reproject first.

They report the CRS's own linear unit, exactly like their 2D siblings: a US-survey-foot
CRS gives feet from `length_3d`, as it does from `length`. The free functions
[`gm.length_3d`][gometry.length_3d] and [`gm.distance_3d`][gometry.distance_3d] take the
same `unit=` override as [`gm.length`][gometry.length] and
[`gm.distance`][gometry.distance] when you want SI metres, projected native linear
units, or CRS-free raw coordinate units.

On a geometry without Z, accessors and metrics have different results. The
**accessors** (`min_z` / `max_z`) describe what is
there, returning `None` for a scalar or `nan` for an array element. The **metrics**
(`length_3d` / `distance_3d`) require Z to compute: a scalar **raises**, while an array
**degrades per element to `nan`** for the rows that lack Z.

```python exec="on" source="block" result="text"
import gometry as gm
climb = gm.LineString([(0.0, 0.0), (3.0, 4.0), (3.0, 4.0)], z=[0.0, 0.0, 10.0], crs=32634)
print('length_3d:', climb.length_3d)
print('z extent:', climb.min_z, climb.max_z)
a = gm.Point(0.0, 0.0, z=0.0, crs=32634)
b = gm.Point(3.0, 4.0, z=12.0, crs=32634)
print('distance_3d:', gm.distance_3d(a, b))

```

## Linear referencing

Linear referencing uses the geometry's CRS metric by default. Values are absolute
distances unless `normalized=True`, when `0.0` and `1.0` mean the start and end.
Pass `basis='m'` when the line's M ordinate is the route measure instead.

With CRS-aware units (the default on a geographic CRS), linework edges are
straight in the CRS's longitude/latitude chart and LRS measures their
ellipsoidal arclength, rather than the shortest geodesic between each pair of
vertices. `line_locate` minimizes against those same chart edges and returns the
interior projection before a point or endpoint when evaluated backend distances
compare equal, then uses route order. This ranks available measurements, not an
exact geodesic continuum minimum. Repeated support and self-crossings
can have several stations at one location, so locate/interpolate is not a unique
inverse there.
An independently interpolated point has canonical longitude, while a
`line_substring` cut stays in the retained source edge's lifted chart.
`unit='planar'` instead uses Cartesian distance in the stored coordinate space.
Planar distance-basis locate uses the exactly nearest segment; exact ties prefer
an interior projection over an endpoint, then the first segment in route order,
matching planar proximity.

Zero-length X/Y segments consume no distance, but their stored vertices remain
on the route. Interpolation at an interior prefix shared by a zero-length run
selects its first stored vertex; normalized `0.0` and `1.0` select the first
and last stored vertices, including when the whole route has zero length.
`line_substring` includes active-distinct stored vertices in its inclusive
range, even when consecutive vertices share X/Y and differ only in Z/M.
At a multipart prefix boundary, interpolation selects the preceding part's
endpoint, so interpolating a locate station at that boundary need not recover
the next part's nearest point.

| Task | Distance basis (default) | M basis |
| --- | --- | --- |
| point at a location | [`line_interpolate(at)`][gometry.Geometry.line_interpolate] | `line_interpolate(at, basis='m')` |
| location nearest a point | [`line_locate(point)`][gometry.Geometry.line_locate] | `line_locate(point, basis='m')` |
| portion between two locations | [`line_substring(start, end)`][gometry.Geometry.line_substring] | `line_substring(start, end, basis='m')` |

On the distance basis, `line_interpolate` accepts a sequence of locations or
`count=` for evenly spaced samples. With `basis='m'`, a scalar line accepts one
stored measure for a `Point` or a measure sequence for a `GeometryArray[Point]`;
an array receiver accepts one measure per line row. An array receiver returns
row-aligned points for one location per row or [`Groups`][gometry.Groups] when
each row has its own sample count. `normalized` and `unit` belong only to the
distance basis, so a measured route never silently mixes two coordinate
systems. On the distance basis, `unit` selects the route metric even when
`normalized=True`:
fractions are measured along that selected route, so changing `unit` can change
the resulting position. `count=` also uses fractions of the selected metric and
accepts `unit=`, but rejects `normalized=True` because the count already defines
fractional sample positions.

```python exec="on" source="block" result="text"
import gometry as gm

line = gm.LineString([(0.0, 0.0), (10.0, 0.0)], crs=32634)
midpoint = line.line_interpolate(0.5, normalized=True)
samples = line.line_interpolate(count=3)
print('midpoint:', midpoint.to_wkt())
print('samples:', [point.to_wkt() for point in samples])

```

**M linear referencing** locates positions by the M ordinate rather than by
fractional or absolute length. `line_interpolate(m, basis='m')` returns the
point at a given measure, `line_locate(point, basis='m')` returns the measure
nearest a point, and `line_substring(start_m, end_m, basis='m')` cuts the
section between two measures. They require every vertex to carry a monotonic,
non-decreasing M. The extent accessors mirror the Z family:
[`min_m`][gometry.Geometry.min_m] / [`max_m`][gometry.Geometry.max_m] summarize the measure span (`None` for a scalar, `nan` for an
array element, when no vertex carries M).

Within each edge, M varies linearly with arclength. For increasing endpoint
measures, a requested `m` selects the arclength fraction
`(m - m0) / (m1 - m0)`; equal endpoint measures remain constant.
Z is carried at that same fraction, and `line_locate(..., basis='m')`
converts the nearest point's arclength fraction back to M. On a geographic
CRS this is ellipsoidal arclength along the straight source-chart edge;
the fraction is generally different from its longitude/latitude parameter.
For example, a WGS 84 meridian from latitude −10° to 10° with endpoint
measures 100 and 300 places M=150 at latitude approximately
−5.00037949475414°, not −5°. Distance-basis interpolation and substring use
the same Z/M carry rule; `interpolate_m` assigns vertex measures by cumulative
edge arclength without moving X/Y/Z.

**Empty input and bad measures.** Empty linework has no positions, so every
form answers it instead of raising: `line_locate` returns `nan`,
`line_interpolate` returns `POINT EMPTY`, `line_substring` returns
`LINESTRING EMPTY`, and `interpolate_m` returns the empty geometry with an M
axis. A `POINT EMPTY` query locates to `nan`. Missing or non-monotonic M is
different: a scalar call raises [`InvalidGeometryError`][gometry.InvalidGeometryError], because it asked for
measures the geometry does not have, while a `GeometryArray` degrades that row
the same way as an empty one rather than aborting the batch. Wrong-kind and
parameter errors raise on both.

```python exec="on" source="block" result="text"
import gometry as gm

route = gm.LineString([(0.0, 0.0), (10.0, 0.0)], m=[0.0, 100.0], crs=32634)
print("at m=50:", route.line_interpolate(50.0, basis='m'))
print("locate:", route.line_locate(gm.Point(5.0, 0.0, crs=32634), basis='m'))
print("m 20..80:", route.line_substring(20.0, 80.0, basis='m'))

```

## Z/M under operations

Planar Simple Features predicates are **XY topology** predicates. Whether `a` contains `b` is
decided in X/Y; Z and M are ignored for the truth value but preserved on the geometry.

Constructive operations resolve `Z` and `M` independently wherever they are
derivable and drop only the axis that cannot be sourced:

- Operations whose output vertices are **copied from input vertices** preserve Z/M:
  [`simplify`][gometry.Geometry.simplify], [`concave_hull`][gometry.Geometry.concave_hull],
  [`reverse`][gometry.Geometry.reverse], the triangulations, and source vertices
  retained by [`convex_hull`][gometry.Geometry.convex_hull] keep their ordinates.
  The convex hull's XY shape is independent of input point order (±0.0 compares
  as the same ordinate). When several input vertices share an XY position, a
  copied hull vertex comes from the first vertex in storage order, including its
  Z/M and signed-zero spelling.
- Operations whose new vertices are **resolvable against the input** — overlay,
  [`clip_by_rect`][gometry.Geometry.clip_by_rect], [`split`][gometry.split],
  [`snap`][gometry.snap], and linear referencing — interpolate Z/M along source
  segments. They retain active-distinct source vertices even when consecutive
  X/Y positions match; distance-based linear referencing gives zero-length X/Y
  segments no length.
  If an axis has a genuinely unsourceable output vertex (such as a clip-rectangle
  corner entering the output), or conflicting source values, that axis is
  dropped while the other axis may remain.
- On geographic input, [`split`][gometry.split], [`snap`][gometry.snap],
  [`convex_hull`][gometry.Geometry.convex_hull],
  [`nearest_points`][gometry.nearest_points],
  [`shortest_line`][gometry.shortest_line],
  [`minimum_clearance_line`][gometry.Geometry.minimum_clearance_line],
  [`subdivide`][gometry.Geometry.subdivide],
  [`line_merge`][gometry.Geometry.line_merge],
  [`coverage_union_all`][gometry.coverage_union_all], and
  [`coverage_simplify`][gometry.coverage_simplify] carry eligible Z/M at an
  inserted vertex when its producer traces it to a source edge. They interpolate
  from that edge's original endpoints by ellipsoidal arclength on the CRS
  ellipsoid, not by its longitude/latitude parameter or a later cut fragment.
  Source vertices retain their own ordinates; when sources disagree, Z and M
  are resolved independently and only a conflicting or unavailable axis is
  dropped. A planar `unit=` choice for a nearest witness does not change how
  its geographic source-edge ordinates are carried. A computed point with no
  sourceable edge has no source Z/M. Recursive subdivision retains original
  input-edge traces through later cuts, including seam vertices (±180° in
  EPSG:4326).
- A degenerate [`minimum_rotated_rectangle`][gometry.Geometry.minimum_rotated_rectangle]
  that follows geographic source-edge support carries resolvable Z/M by the
  same original-edge arclength rule. A computed areal rectangle is a new
  XY construction and is returned in XY.
- Empty overlay results retain the axes declared by the operation: `difference`
  follows its first operand, while the other set operations use the union of
  their operands' axes.
- Operations that **compute brand-new points** — [`centroid`][gometry.Geometry.centroid], [`point_on_surface`][gometry.Geometry.point_on_surface], [`envelope`][gometry.Geometry.envelope],
  [`buffer`][gometry.Geometry.buffer], [`maximum_inscribed_circle`][gometry.Geometry.maximum_inscribed_circle], `voronoi_*`,
  [`polylabel`][gometry.Geometry.polylabel] — cannot source Z/M for invented vertices, so they return 2D.

```python exec="on" source="block" result="text"
import gometry as gm

track = gm.LineString(
    [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)],
    z=[10.0, 20.0, 30.0], m=[0.0, 1.0, 2.0], crs=4326,
)

centroid = track.centroid()
print("centroid:", centroid.geometry_type, centroid.coordinate_axes)

flat = track.force_2d()
print("force_2d:", flat.geometry_type, flat.coordinate_axes)

```

```python exec="on" html="true"
from _figures import before_after
import gometry as gm

track = gm.LineString(
    [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)],
    z=[10.0, 20.0, 30.0], m=[0.0, 1.0, 2.0], crs=4326,
)
print(before_after(track, track.centroid(),
                   before_caption="XYZM survey track", after_caption="2D centroid",
                   before_alt="The survey track has three XY vertices carrying Z and M ordinates.",
                   after_alt="The computed centroid is a new 2D point, so Z and M are absent."))

```

For a permanent 2D conversion, use [`force_2d`][gometry.Geometry.force_2d] to
drop Z and M. [`force_3d`][gometry.Geometry.force_3d] fills only vertices that
lack Z. Use [`with_coordinates`][gometry.Geometry.with_coordinates] to replace
one or more ordinates without changing topology. Each supplied value can be a
scalar broadcast over vertices or a column with one value per vertex in the
flattened coordinate sequence; omitted ordinates are preserved, and
`z=None` / `m=None` removes that optional axis. X and Y cannot be removed.
Matrix mode replaces the coordinate matrix using its existing axis layout and
cannot be mixed with ordinate columns:

```python exec="on" source="block" result="text"
import gometry as gm

track = gm.LineString([(0, 0), (1, 1), (2, 0)], z=[10, 20, 30], crs=4326)
flat = track.force_2d()
print("force_2d axes:", flat.coordinate_axes)
print("flat centroid:", (flat.centroid()).to_wkt())
print("force_3d axes:", flat.force_3d().coordinate_axes)
print("with_coordinates(z=...):", flat.with_coordinates(z=10.0))
print("with_coordinates(m=...):", flat.with_coordinates(m=0.0).coordinate_axes)

```

## IO and format limits

WKT, WKB/EWKB, and GeoArrow preserve both ordinates. [`to_geojson()`][gometry.Geometry.to_geojson] raises on M, and `__geo_interface__` raises on M or a
coordinate epoch because GeoJSON has no slots for them. WKB/EWKB preserve M but
not epoch; GeoArrow preserves epoch metadata. WKT/WKB preserve dimensional-empty
axes, while GeoJSON does not. Format details are in [Text & binary
formats](../ecosystem/text-formats.md); GeoArrow's columnar metadata is described
in [Arrow & storage](../ecosystem/arrow.md).

## Enforcing a dimension contract

Validity and storage-dimension are separate contracts. Use `axes=` with
[gm.require][gometry.require] on an existing geometry or while parsing untrusted
input to assert the layout at a boundary, including a strict XY contract for data
that must not carry Z or M:

```python title="partial: requires an input object from the surrounding application"
area = gm.require(obj, crs=4326, axes="XY")

trace = gm.require(obj, crs=4326, axes="XYZM")

```

See [Validation](validation.md) for the validity side of the contract and end-to-end
ingestion patterns.

## See also

- [Predicates](predicates.md) — spatial relationships ([`contains`][gometry.contains], [`intersects`][gometry.intersects], [DE-9IM](https://en.wikipedia.org/wiki/DE-9IM)).
- [Constructive operations](constructive.md) — buffers, overlays, hulls, and how they carry Z/M.
- [CRS](crs.md) — the metric frame for lon/lat area, length, and distance.
- [Arrays](arrays.md) — vectorized [`GeometryArray`][gometry.GeometryArray] columns and the missing-row rule.
- [Validation](validation.md) — the OGC validity side of the storage contract.
