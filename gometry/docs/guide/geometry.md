---
description: Constructing, inspecting, and comparing gometry's seven geometry types, plus XY/XYZ/XYM/XYZM layouts and Z/M behavior through operations.
---

# Geometry & dimensions

Every value in gometry is a `Geometry`: one of exactly **seven** immutable,
hashable [OGC Simple Features](https://www.ogc.org/standard/sfa/) types, each
optionally carrying a CRS and per-vertex Z (elevation) and M (measure)
ordinates.

| Type | Made of | Constructor |
|------|---------|-------------|
| `Point` | one coordinate | [`gm.Point`][gometry.Point] |
| `LineString` | ordered vertices | [`gm.LineString`][gometry.LineString] |
| `Polygon` | a shell + holes | [`gm.Polygon`][gometry.Polygon] / [`gm.box`][gometry.box] |
| `MultiPoint` | many points | [`gm.MultiPoint`][gometry.MultiPoint] |
| `MultiLineString` | many lines | [`gm.MultiLineString`][gometry.MultiLineString] |
| `MultiPolygon` | many polygons | [`gm.MultiPolygon`][gometry.MultiPolygon] |
| `GeometryCollection` | any mix | [`gm.GeometryCollection`][gometry.GeometryCollection] |

!!! tip "Always (x, y) — lon/lat only under a geographic CRS"
    Every constructor takes coordinates in **(x, y)** order. Under a
    **geographic** CRS those axes are **(longitude, latitude)**; under a
    projected CRS they are the projection's easting/northing (or other linear
    axes); CRS-free geometries stay in raw coordinate units. The lon/lat
    reading is not universal — it is the geographic case. Pass `crs=4326` (or
    another geographic CRS) on [`gm.Point`][gometry.Point] and the bulk builders
    when the input is geographic.

## Points

Construct a point with `gm.Point(x, y)`; under a geographic CRS, `(x, y)` is
`(longitude, latitude)`.

```python exec="on" source="block" result="text"
import gometry as gm

paris = gm.Point(2.3522, 48.8566, crs=4326)
print(paris.to_wkt())
print("x =", paris.x, " y =", paris.y)
print("crs =", paris.crs, " coordinate_axes =", paris.coordinate_axes)

```

```python exec="on" html="true"
from _figures import figure
import gometry as gm
print(figure(gm.Point(2.3522, 48.8566), "Point(lon, lat)"))

```

!!! note "`.x` / `.y` are for single points only"
    A `Point` exposes `.x`, `.y` (and `.z` / `.m` when present) for scalar
    scalar access. For any other geometry, pull every vertex out with
    [`geom.coords`][gometry.Geometry.coords] (see
    [Inspecting geometries](#inspecting-geometries)).

## LineStrings

A `LineString` is an ordered sequence of vertices. Pass any sequence of
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
from _figures import figure
import gometry as gm
print(figure(gm.LineString([(0, 0), (1, 2), (3, 1), (4, 3)]), "LineString(...)"))

```

## Polygons

A `Polygon` is a **shell** (exterior ring) plus zero or more **holes**
(interior rings). Rings are closed automatically — you do not need to repeat
the first vertex at the end, though it is harmless if you do.

```python exec="on" source="block" result="text"
import gometry as gm

square_with_hole = gm.Polygon(
    [(0, 0), (4, 0), (4, 4), (0, 4)],          # shell (auto-closed)
    holes=[[(1, 1), (2, 1), (2, 2), (1, 2)]],   # one square hole
)
print(square_with_hole.to_wkt())
print("rings:", len(gm.rings(square_with_hole)))  # exterior + 1 hole
print("area:", square_with_hole.area)                 # 16 - 1 = 15

```

```python exec="on" html="true"
from _figures import figure
import gometry as gm
print(figure(gm.Polygon(
    [(0, 0), (4, 0), (4, 4), (0, 4)],
    holes=[[(1, 1), (2, 1), (2, 2), (1, 2)]],
), "Polygon(shell, holes)"))

```

!!! note "Constructors check structure, not topology"
    `gm.Polygon` rejects *structurally* malformed input eagerly — non-finite
    coordinates, or a ring with fewer than three vertices, raise a
    [`InvalidGeometryError`][gometry.InvalidGeometryError].
    It does **not** run full OGC validity, so a self-intersecting "bowtie" is
    built as an *invalid* geometry rather than rejected. Detect it with
    `geom.validate()` and fix it with [`repair`][gometry.Geometry.repair] — see
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

`MultiLineString` and `MultiPolygon` take sequences of part-coordinate
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

### GeometryCollections

When parts have **mixed** types, use a `GeometryCollection`:

```python exec="on" source="block" result="text"
import gometry as gm

gc = gm.GeometryCollection([
    gm.Point(0, 0),
    gm.LineString([(1, 1), (2, 2)]),
    gm.box(3, 3, 4, 4),
])
print(gc.to_wkt())

```

!!! note "Mixed-dimension collections sum area and length"
    `.area` and `.length` on a `GeometryCollection` **sum** each part's
    contribution (points and lines add zero area; polygon rings add their area).
    Prefer homogeneous multi-types when you want a single-kind column; reserve
    collections for genuinely heterogeneous
    results (e.g. an overlay that returns both lines and points, or the
    [tessellation operations](constructive.md#tessellation-triangulate-voronoi-polygonize)).

## Inspecting geometries

These properties and functions apply to every geometry type:

| Call | Returns |
|------|---------|
| `geom.geometry_type` | the type name, e.g. `"Polygon"` |
| `geom.coordinate_axes` | ordinate layout: `"XY"`, `"XYZ"`, `"XYM"`, `"XYZM"` |
| `geom.is_empty` | whether the geometry holds no coordinates |
| [`bounds`][gometry.Geometry.bounds] | `(minx, miny, maxx, maxy)` tuple |
| `geom.coords` | every vertex as a flat, indexable `Coordinates` view |
| [`gm.parts`][gometry.parts] | component geometries of a multi-part |
| [`gm.rings`][gometry.rings] | rings of a `Polygon` (exterior first) |

```python exec="on" source="block" result="text"
import gometry as gm

poly = gm.Polygon(
    [(0, 0), (4, 0), (4, 4), (0, 4)],
    holes=[[(1, 1), (2, 1), (2, 2), (1, 2)]],
)

print("type:     ", poly.geometry_type)
print("coordinate_axes:", poly.coordinate_axes)
print("bounds:   ", poly.bounds)
print("vertices: ", len(poly.coords))   # all rings, flattened
print("rings:    ", len(gm.rings(poly)))

```

`geom.coords` is a flat, indexable `Coordinates` view over **every** vertex
(each ring, each part, flattened depth-first). Index it for tuples, iterate it,
or pull whole axes as columns and hand
them straight to NumPy, pandas, or polars without a Python loop:

```python exec="on" source="block" result="text"
import numpy as np
import gometry as gm

coords = gm.LineString([(0, 0), (1, 2), (3, 1)], z=[10.0, 20.0, 30.0]).coords

print("len:        ", len(coords))
print("first tuple:", coords[0])
print("axes:       ", coords.coordinate_axes)    # "XYZ"
print("x column:   ", np.asarray(coords.x))      # zero-copy ndarray
print("dict keys:  ", list(coords.to_dict().keys()))   # ready for pd.DataFrame

```

`.x`/`.y` (and `.z`/`.m` when present) are zero-copy columns; `.to_dict()` returns
read-only NumPy columns such as `{'x': ndarray, 'y': ndarray}` for direct use
with `pd.DataFrame` or `pl.DataFrame` (forced Z/M use `NaN` where absent);
`np.asarray(coords)` materializes an `(N, dims)` array; and `.index` tags each vertex
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

print(type(gm.Point(1, 2)).__name__)            # Point
print(type(gm.box(0, 0, 1, 1)).__name__)  # Polygon
print(isinstance(gm.box(0, 0, 1, 1), gm.Polygon))   # True

```

A `Point` exposes `.x`/`.y`/`.z`/`.m`; a `Polygon` exposes `.exterior` (a closed
`LineString`) and
`.interiors` (its holes); and the multi-part / collection types are real
sequences — iterate them, index them, and take `len(...)`, with each part typed:

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
print("a == b:", a == b, "| a == reversed:", a == reversed_)   # True False
print("distinct:", len({a, b, reversed_}))                     # 2

```

This `==` is **structural** (vertex-order sensitive). For the topological
"same shape on the map" test that ignores vertex order, use
[`gm.equals`][gometry.equals]; for a representation-independent vertex check, normalize first:
`a.normalize().equals_exact(b.normalize())`.

## Dimensions: XY, XYZ, XYM, XYZM

gometry supports four coordinate layouts — **XY, XYZ, XYM, and XYZM** — from the
constructors through IO.

### Three meanings of "dimension"

"Dimension" has three distinct meanings in GIS, and gometry keeps them separate:

| Concept | Property | Example value |
|---|---|---|
| **Topological dimension** | `geom.topological_dimension` | `0` (Point), `1` (LineString), `2` (Polygon) |
| **Coordinate axes / ordinate layout** | `geom.coordinate_axes` | `"XY"`, `"XYZ"`, `"XYM"`, `"XYZM"` |
| **Has-ordinate flags** | `geom.has_z`, `geom.has_m` | `True` / `False` |

```python exec="on" source="block" result="text"
import gometry as gm

trace = gm.LineString(
    [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)],
    z=[10.0, 20.0, 30.0],
    m=[0.0, 1.0, 2.0],
    crs=4326,
)
print("geometry_type:        ", trace.geometry_type)
print("topological_dimension:", trace.topological_dimension)
print("coordinate_axes:      ", trace.coordinate_axes)
print("has_z / has_m:        ", trace.has_z, "/", trace.has_m)

```

### Z is a spatial axis; M is a measure

- **Z** is an additional *spatial* ordinate, usually elevation or height. gometry does **not**
  assume Z is in meters unless a vertical CRS says so — Z is a third spatial number
  until a CRS gives it units.
- **M** is a *measure* ordinate. It commonly carries linear-referencing values, route
  measures, distance-along-line, per-vertex timestamps, or application-specific event
  alignment. **M is not a spatial axis** and must never be silently treated as Z. M is also
  distinct from the [coordinate epoch](crs.md#coordinate-epochs) (`geom.epoch`): the
  epoch is a single decimal year that dates the whole geometry's CRS realization, whereas M is
  a free per-vertex value with no datum.

A GPX track or vehicle trace has both elevation (Z) and a
capture timestamp or route measure (M) aligned to each vertex. gometry lets you store both on
the geometry instead of the common workaround of a 2D line plus side arrays:

```python exec="on" source="block" result="text"
import gometry as gm

track = gm.LineString(
    [(21.00, 52.00), (21.01, 52.01), (21.02, 52.00)],
    z=[105.0, 118.0, 110.0],                            # elevation (Z)
    m=[1_700_000_000, 1_700_000_030, 1_700_000_060],    # unix capture time (M)
    crs=4326,
)
coords = list(track.coords.select('XYZM'))
print("coordinate_axes:", track.coordinate_axes)
for x, y, z, m in coords:
    print(f"  lon={x} lat={y} elev={z} t={int(m)}")

```

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
    print(p.coordinate_axes, "->", p.to_wkt())

```

For arrays and lines, pass `z=` and `m=` keywords to [gm.points][gometry.points] and [gm.LineString][gometry.LineString]:

```python title="partial: requires coordinate arrays from the surrounding application"
points_xy = gm.points(lons, lats, crs=4326)
points_zm = gm.points(lons, lats, z=elevations, m=timestamps, crs=4326)
line      = gm.LineString(xy, z=elevations, m=timestamps, crs=4326)

```

!!! note "Consistent layout per scalar geometry"
    A non-collection geometry must use one coordinate-axis layout for every vertex — a
    `LineString` cannot mix XY and XYZM vertices. A `GeometryCollection` may hold children
    with different layouts, but the API makes that explicit rather than silent.

WKT carries the `Z`, `M`, and `ZM` dimensional tags, which round-trip through gometry:

```python exec="on" source="block" result="text"
import gometry as gm

print(gm.from_wkt("POINT Z (1 2 3)").coordinate_axes)
print(gm.from_wkt("POINT M (1 2 7.5)").coordinate_axes)
print(gm.from_wkt("POINT ZM (1 2 3 7.5)").coordinate_axes)

```

## Measuring with Z and M

**3D spatial measurement** uses the Z axis. `length_3d` and point-to-point
`distance_3d(other)` include the vertical component, and `min_z` / `max_z` /
`z_range` (properties) plus `bounds_3d` (a 6-tuple `(minx, miny, minz, maxx, maxy,
maxz)`, or `None` when empty) summarize the Z extent. Because a 3D length only makes
sense when every axis shares one linear unit, these require a **projected** CRS (or no
CRS); a geographic CRS mixes degrees and metre heights and raises under every `unit=` —
reproject first.

They report the CRS's own linear unit, exactly like their 2D siblings: a US-survey-foot
CRS gives feet from `length_3d`, as it does from `length`. The free functions
[`gm.length_3d`][gometry.length_3d] and [`gm.distance_3d`][gometry.distance_3d] take the
same `unit=` override as [`gm.length`][gometry.length] and
[`gm.distance`][gometry.distance] when you want SI metres or raw coordinate units.

On a geometry without Z, accessors and metrics have different results. The
**accessors** (`min_z` / `max_z` / `z_range` / `bounds_3d`) describe what is
there, returning `None` for a scalar or `nan` for an array element. The **metrics**
(`length_3d` / `distance_3d`) require Z to compute: a scalar **raises**, while an array
**degrades per element to `nan`** for the rows that lack Z.

```python exec="on" source="block" result="text"
import gometry as gm
climb = gm.LineString([(0.0, 0.0), (3.0, 4.0), (3.0, 4.0)], z=[0.0, 0.0, 10.0], crs=32634)
print('length_3d:', climb.length_3d)
print('z_range: ', climb.z_range, '| bounds_3d:', climb.bounds_3d)
a = gm.Point(0.0, 0.0, z=0.0, crs=32634)
b = gm.Point(3.0, 4.0, z=12.0, crs=32634)
print('distance_3d:', gm.distance_3d(a, b))

```

## Linear referencing

Linear referencing uses the geometry's CRS metric by default. Values are absolute
distances unless `normalized=True`, when `0.0` and `1.0` mean the start and end.
Pass `basis='m'` when the line's M ordinate is the route measure instead.

| Task | Distance basis (default) | M basis |
| --- | --- | --- |
| point at a location | `line_interpolate(at)` | `line_interpolate(at, basis='m')` |
| location nearest a point | `line_locate(point)` | `line_locate(point, basis='m')` |
| portion between two locations | `line_substring(start, end)` | `line_substring(start, end, basis='m')` |

`line_interpolate` also accepts a sequence of locations or `count=` for evenly
spaced samples. A scalar line produces a `GeometryArray` for those plural
forms; an array receiver returns row-aligned points or `Groups` when each row
has its own sample count. `normalized` and `unit` belong only to the distance
basis, so a measured route never silently mixes two coordinate systems.

```python exec="on" source="block" result="text"
import gometry as gm

line = gm.LineString([(0.0, 0.0), (10.0, 0.0)], crs=32634)
midpoint = line.line_interpolate(0.5, normalized=True)
samples = line.line_interpolate(count=3)
print('midpoint:', midpoint.to_wkt())
print('samples: ', [point.to_wkt() for point in samples])

```

**M linear referencing** locates positions by the M ordinate rather than by
fractional or absolute length. `line_interpolate(m, basis='m')` returns the
point at a given measure, `line_locate(geom, basis='m')` returns the measure
nearest a point, and `line_substring(start_m, end_m, basis='m')` cuts the
section between two measures. They require every vertex to carry a monotonic,
non-decreasing M. The extent accessors mirror the Z family:
`min_m` / `max_m` / `m_range` summarize the measure span (`None` for a scalar, `nan` for an
array element, when no vertex carries M).

**Array degrade vs scalar raise.** On a `GeometryArray`, a per-row geometry-data
failure (`EmptyLinework`, `MissingMeasure`, or `NonMonotonicMeasure`) degrades
to `nan` (for `line_locate`) or a typed EMPTY geometry (for
`line_interpolate` / `line_substring`) rather than aborting the batch. The
    scalar forms of the same verbs still raise `InvalidGeometryError`. Wrong-kind
    and parameter errors raise on both paths — only those three data conditions
    degrade. `POINT EMPTY` / `LINESTRING EMPTY` rows in array LRS output represent
    degraded failures, not input data.

```python exec="on" source="block" result="text"
import gometry as gm

route = gm.LineString([(0.0, 0.0), (10.0, 0.0)], m=[0.0, 100.0], crs=32634)
print("at m=50: ", route.line_interpolate(50.0, basis='m'))
print("locate:  ", route.line_locate(gm.Point(5.0, 0.0, crs=32634), basis='m'))
print("m 20..80:", route.line_substring(20.0, 80.0, basis='m'))

```

## Z/M under operations

Planar Simple Features predicates are **XY topology** predicates. Whether `a` contains `b` is
decided in X/Y; Z and M are ignored for the truth value but preserved on the geometry.

Constructive operations preserve ordinates wherever they are derivable and
return 2D results when they cannot source them:

- Operations whose output vertices are **copied from input vertices** preserve Z/M:
  `simplify`, `convex_hull`, `concave_hull`, and
  the triangulations keep every surviving vertex's ordinates.
- Operations whose new vertices are **resolvable against the input** — overlay,
  `clip_by_rect`, linear referencing — interpolate Z/M along source segments. If
  any output vertex genuinely cannot be sourced (such as a clip-rectangle corner
  entering the output), the result is 2D.
- Operations that **compute brand-new points** — `centroid`, `point_on_surface`, `envelope`,
  `buffer`, `minimum_rotated_rectangle`, `maximum_inscribed_circle`, `voronoi_*`,
  `polylabel` — cannot source Z/M for invented vertices, so they return 2D.

```python exec="on" source="block" result="text"
import gometry as gm

track = gm.LineString(
    [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)],
    z=[10.0, 20.0, 30.0], m=[0.0, 1.0, 2.0], crs=4326,
)

# centroid returns 2D because it invents a new point
print("centroid:", track.centroid().coordinate_axes)

# force_2d drops both optional ordinates
print("force_2d:", track.force_2d().coordinate_axes)

```

```python exec="on" html="true"
from _figures import before_after
import gometry as gm

track = gm.LineString(
    [(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)],
    z=[10.0, 20.0, 30.0], m=[0.0, 1.0, 2.0], crs=4326,
)
print(before_after(track, track.centroid(),
                   before_caption="XYZM input", after_caption="centroid (2D)"))

```

For a permanent 2D conversion, use [`force_2d`][gometry.Geometry.force_2d] to
drop Z and M. [`force_3d`][gometry.Geometry.force_3d] fills only vertices that
lack Z, while [`set_z`][gometry.Geometry.set_z] and [`set_m`][gometry.Geometry.set_m]
assign an ordinate at every vertex or clear it with `None`:

```python exec="on" source="block" result="text"
import gometry as gm

track = gm.LineString([(0, 0), (1, 1), (2, 0)], z=[10, 20, 30], crs=4326)
flat = track.force_2d()
print("force_2d axes:", flat.coordinate_axes)
print("flat centroid:", (flat.centroid()).to_wkt())
print("force_3d axes:", flat.force_3d().coordinate_axes)
print("set_z:", flat.set_z(10.0))
print("set_m: ", flat.set_m(0.0).coordinate_axes)

```

## IO and format limits

WKT, WKB/EWKB, and GeoArrow preserve both ordinates. `to_geojson()` raises on M, and `__geo_interface__` raises on M or a
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
# Web bbox: strictly XY lon/lat
area = gm.require(obj, crs=4326, axes="XY")

# Trace ingestion: require full XYZM
trace = gm.require(obj, crs=4326, axes="XYZM")

```

See [Validation](validation.md) for the validity side of the contract and end-to-end
ingestion patterns.

## See also

- [Predicates](predicates.md) — spatial relationships (`contains`, `intersects`, [DE-9IM](https://en.wikipedia.org/wiki/DE-9IM)).
- [Constructive operations](constructive.md) — buffers, overlays, hulls, and how they carry Z/M.
- [CRS](crs.md) — the metric frame for lon/lat area, length, and distance.
- [Arrays](arrays.md) — vectorized `GeometryArray` columns and the missing-row rule.
- [Validation](validation.md) — the OGC validity side of the storage contract.
