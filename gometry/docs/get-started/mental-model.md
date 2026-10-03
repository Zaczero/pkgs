---
description: "Gometry's operation-led model: declare, transform, refine; choose a metric frame; use planar topology; and partition with spatial keys."
---

# The mental model

A geometry carries its frame, so an operation's metric follows from the data
rather than from the operation name. Start every workflow by declaring what the
coordinates mean, transforming them only when their numbers must change, and
refining coarse spatial candidates before treating them as geometry answers.

## Declare, transform, refine

[`set_crs`][gometry.Geometry.set_crs] **declares** or relabels CRS metadata; its
numeric coordinates do not move. [`to_crs`][gometry.Geometry.to_crs] **transforms**
X/Y, so the same locations are represented by different numbers in another CRS.
A cover or index candidate **narrows the search**; it is not an exact geometry
answer. Retain the source geometry and use an exact predicate to refine those
candidates into exact matches.

```python exec="on" source="block" result="text"
import gometry as gm

raw = gm.Point(21.0, 52.0)
declared = raw.set_crs(4326)
projected = declared.to_crs(3857)

print("set_crs coordinates:", list(declared.coords))
print("set_crs CRS:", declared.crs)
print("to_crs coordinates:", [round(value) for value in projected.coords[0]])
print("to_crs CRS:", projected.crs)

```

Use [`set_crs`][gometry.Geometry.set_crs] only when the existing coordinates were
authored in that CRS. Use [`to_crs`][gometry.Geometry.to_crs] when the coordinates
must move. The full choice, including Z/M and epochs, is [set_crs vs to_crs](../guide/crs.md#set-crs-vs-to-crs).
For a bbox prefilter, use [index candidates versus query](../guide/indexing.md#candidates-vs-query);
for a cell selection, use [exact source membership after a grid cover](../guide/grids.md#exact-membership-with-the-source-geometry).

## Three operation families

gometry does not use one Earth model for every operation:

- **Measure in the geometry's frame** — by default, geographic area, length, and
  LRS use ellipsoidal arclength or area on straight longitude/latitude chart
  edges; point-to-point distance follows geodesics. `unit='planar'` explicitly
  selects Cartesian measurement in stored coordinates. Projected and CRS-free
  geometries use planar measurement in their respective units. See
  [CRS, units & measurement](../guide/crs.md).
- **Reason with coordinate-plane X/Y topology** — predicates, overlays, and
  constructive topology use planar coordinates. In a geographic CRS, topology
  first resolves longitude aliases and seam and pole equivalence in the
  CRS-native frame; seam-crossing inputs are split into chart pieces. This is
  not a geodesic operation. See [spatial predicates](../guide/predicates.md)
  and [constructive operations](../guide/constructive.md).
- **Partition with spatial keys** — H3, S2, geohash, and tiles select cells for
  grouping, joining, and candidate filtering. S2 cell selection and hierarchy are
  seam-free, but S2 does not provide spherical boolean topology here. Exact source
  membership remains predicate-owned. See [grids & geocodes](../guide/grids.md#the-spherical-model).

## Geometries are Python values

Geometries and arrays implement Python value protocols for formatting, truth
testing, structural matching, sequence operations, and bitwise overlays:

```python exec="on" source="block" result="text"
import gometry as gm

geom = gm.from_wkt("POINT (1.20517 5)")
print(str(geom))
print(f"{geom:.2f}")
print(bool(gm.from_wkt("POINT EMPTY")))

match geom:
    case gm.Point(x, y):
        print("matched point at", x, y)

arr = gm.GeometryArray([gm.Point(0, 0), gm.Point(3, 4)])
match arr:
    case [gm.Point(), gm.Point() as far]:
        print("two points; the far one is", far.to_wkt())
print(gm.Point(3, 4) in arr, arr.index(gm.Point(3, 4)))

a, b = gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)
print((a & b).area, (a | b).area)

```

Equality is **value** equality (same CRS, epoch, and exact coordinates), and hash
agrees with it. Geometries and arrays can key dictionaries and live in sets. For
protocol and interchange detail, see [Geometry & dimensions](../guide/geometry.md),
[Arrays & performance](../guide/arrays.md), and [Text & binary formats](../ecosystem/text-formats.md).

## The shape of the API

Facts are properties, unary operations are methods, and binary relationships are
free functions. Measurement overrides use the free [`area`][gometry.area] and [`length`][gometry.length] functions
with `unit=`. Naming and placement rules are in [Design principles](../about/design.md).

## Where to go next

- [User guide](../guide/index.md) — choose a task-oriented route.
- [Arrays & performance](../guide/arrays.md) and [Geometry](../guide/geometry.md) —
  vectorized methods and constructors.
- [NumPy interop](../ecosystem/numpy.md) and [DataFrames](../ecosystem/dataframes.md) —
  ndarray handoffs and explicit typed DataFrame conversions.
- [Spatial indexing & joins](../guide/indexing.md) — candidate/refine queries and joins.
- [CRS, units & measurement](../guide/crs.md) — CRS-based metric and projection rules.
- [Discrete grids](../guide/grids.md) — S2 spherical cells and grid covers.
