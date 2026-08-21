---
description: The three models behind gometry — CRS-decided measurement, S2 spherical reasoning, and discrete grid cells, plus set_crs versus to_crs and the API shape.
---

# The mental model

A geometry carries its own frame, so an operation's metric follows from the data
rather than from the operation name.

## Three models of the Earth

gometry's operations use one of three:

- **Measure through the CRS** — planar or geodesic, decided by the geometry's
  [CRS](../guide/crs.md).
- **Reason globally on the sphere** — S2 gives seam-free global reasoning with no
  projection ([the spherical model](../guide/grids.md#the-spherical-model)).
- **Discretize into grid cells** — H3/S2/geohash/tile [cell covers](../guide/grids.md)
  bucket geometry for aggregation and joins.

## `set_crs` declares, `to_crs` transforms

[`set_crs`][gometry.Geometry.set_crs] attaches metadata to existing coordinates. [`to_crs`][gometry.Geometry.to_crs] transforms those
coordinates through a PROJ operation:

```python title="partial: API placement example"
geom.set_crs(4326)   # "these existing numbers are lon/lat" — no coordinates change
geom.to_crs(3857)    # "reproject these coordinates" — every coordinate changes

```

Both validate CRS metadata through PROJ. Frame matching, projection choice, and
metric behavior follow the [CRS, units & measurement](../guide/crs.md) contract.

## Geometries are Python values

Geometries and arrays implement Python value protocols for formatting, truth
testing, structural matching, sequence operations, and bitwise overlays:

```python exec="on" source="block" result="text"
import gometry as gm

geom = gm.from_wkt("POINT (1.20517 5)")
print(str(geom))                  # str() is bare WKT (repr keeps the frame)
print(f"{geom:.2f}")              # format specs round for display
print(bool(gm.from_wkt("POINT EMPTY")))  # empty geometries are falsy

match geom:                       # structural pattern matching destructures
    case gm.Point(x, y):
        print("matched point at", x, y)

arr = gm.GeometryArray([gm.Point(0, 0), gm.Point(3, 4)])
match arr:                        # arrays are sequences
    case [gm.Point(), gm.Point() as far]:
        print("two points; the far one is", far.to_wkt())
print(gm.Point(3, 4) in arr, arr.index(gm.Point(3, 4)))

a, b = gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)
print((a & b).area, (a | b).area)   # & is intersection, | is union

```

Equality is **value** equality (same CRS, epoch, and exact coordinates), and hash
agrees with it. Geometries and arrays can key dictionaries and live in sets.
`pickle`/`copy`/`deepcopy` apply to durable value and container types; pickle is
for trusted data only ([security](../about/security.md#never-unpickle-untrusted-data)).
[`Coordinates`][gometry.Coordinates] and iterator types are not picklable. Geometries,
arrays, and CRS support `weakref`; cell wrappers do not. Cells sort by their id
(`sorted(cells)`);
for the numeric-id systems (H3, S2, tiles) `int(cell)` bridges to h3-py /
s2sphere raw ids, while a geohash cell is keyed by its text token instead.

## The shape of the API

Facts are properties, unary operations are methods, and binary relationships are
free functions. Measurement overrides use the free [`area`][gometry.area] and [`length`][gometry.length] functions
with `unit=`. Naming and placement rules are in [Design principles](../about/design.md).

## Where to go next

- [Arrays & performance](../guide/arrays.md) and [Geometry](../guide/geometry.md) —
  vectorized methods and constructors.
- [NumPy interop](../ecosystem/numpy.md) and [DataFrames](../ecosystem/dataframes.md) —
  ndarray handoffs and explicit typed DataFrame conversions.
- [Spatial indexing & joins](../guide/indexing.md) — candidate/refine queries and joins.
- [CRS, units & measurement](../guide/crs.md) — CRS-based metric and projection rules.
- [Discrete grids](../guide/grids.md) — S2 spherical cells and grid covers.
