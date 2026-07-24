---
description: The one idea behind gometry — the CRS is the single knob and metrics are native by default. One geom.area property, the CRS decides geodesic vs planar, plus the spherical and grid models.
---

# The mental model

Most geospatial bugs are not arithmetic mistakes. They are **model mistakes**: planar area
computed on longitude/latitude, a 100-unit buffer that meant degrees instead of meters, a
spatial index treated as if it returned exact matches. The usual stack lets these
through silently because the model lives in the developer's head, not in the API.

gometry's central design decision is the opposite:

> **The CRS is the single knob, and metrics are native by default.** A geometry's CRS alone decides
> how it is measured. There is exactly one `area`, one `length`, one `distance` — you do not
> pick a separate "planar vs geodesic" engine per call. Override units with `unit=`, or change
> the frame with `to_crs(...)`.

Internalize this one page and the rest of gometry follows.

## Three lenses on the Earth

gometry looks at the Earth three ways, and every feature falls under one of them:

- **Measure through the CRS** — planar or geodesic, decided by the geometry's
  [CRS](../guide/crs.md). This is the single knob below.
- **Reason globally on the sphere** — S2 gives seam-free global reasoning with no
  projection ([the spherical model](../guide/grids.md#the-spherical-model)).
- **Discretize into grid cells** — H3/S2/geohash/tile [coverages](../guide/grids.md)
  bucket geometry for aggregation and joins.

The rest of this page is the first lens — the one that governs every metric.

## The CRS decides; the result is native

`area`, `length`, `distance`, `buffer`, `offset_curve` and `dwithin` all read the geometry's
CRS and do the right thing for it. The result is native by default:

| Geometry's CRS | How it is measured | Units |
|---|---|---|
| **geographic** (e.g. `EPSG:4326`) | geodesic, on that CRS's own ellipsoid ([Karney's algorithm](https://geographiclib.sourceforge.io/)) | m, m² |
| **projected** (e.g. [UTM](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system), `EPSG:3857`, State Plane feet) | planar, in the CRS's **native linear units** | feet stay feet; meters stay meters |
| **none** | planar, in bare coordinate units | coordinate units |

Pass `unit='meters'` to force SI (raises without a CRS) or `unit='planar'` for raw
coordinate Cartesian math.

The same polygon answers a *correct* question whether it is geographic or projected — you
just attach the CRS the data is really in:

```python exec="on" source="block" result="text"
import gometry as gm

poly = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)   # a 2x2 degree box over Poland

print("area (m^2):", f"{poly.area:,.0f}")        # geographic CRS -> geodesic
print("length (m):", f"{poly.length:,.0f}")

cover = gm.h3_cover(poly, resolution=5)
print("h3 cells covering it:", len(cover))

```

Because `poly`'s CRS is geographic, `poly.area` is the real ellipsoidal ~30 billion m² (about
30,000 km²), not a meaningless "square degrees" number. There is nothing to choose and
nothing to get wrong.

## Reproject to change the answer

To measure planar in a fixed local frame — for speed, or because a tool downstream wants
projected coordinates — reproject first. The same `poly.area` then reports the projected
CRS's **native linear units** (meters for UTM; feet for a foot-based State Plane CRS):

```python exec="on" source="block" result="text"
import gometry as gm

poly = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
utm = poly.to_crs(poly.estimate_local_crs())   # pick a sensible local UTM
print("projected area (m^2):", f"{utm.area:,.0f}")

```

A CRS-free geometry measures in bare coordinate units — that is the only case where `geom.area`
is "just numbers", and it is unambiguous precisely because no CRS claims otherwise.

## A decision table: what to reach for

| You want to… | Use | Why |
|---|---|---|
| Area/length/distance **on Earth in meters** | attach a geographic CRS, then `geom.area` / `geom.length` / `gm.distance(geom, other)` | Geodesic, ellipsoidal truth without picking a projection. |
| Area/length on **already-projected** data | `geom.area` / `geom.length` | Native linear units of the projected CRS, planar and fast; `unit='meters'` for SI. |
| A buffer in **meters** around lon/lat data | `geom.buffer(meters)` with a geographic CRS | Local-projection meter buffer; no "100 degrees?" trap. |
| A buffer in **coordinate units** | `geom.buffer(distance)` on CRS-free geometry | Explicit unit buffer. |
| Point bearing / walk a distance / interpolate | `gm.bearing(pt, o)` / `gm.destination(pt, az, m)` / `gm.point_between(pt, o, d)` | Geodesic on a geographic CRS, planar otherwise. |
| Topology: contains / intersects / within | `gm.contains(a, b)`, `gm.intersects(a, b)` | XY topology in the coordinates you hold; projection and seam handling can change the realized edges. |
| **Aggregate** points/polygons into cells | `gm.h3_cover(geom, resolution=...)` | Discrete grid for group-by, heatmaps, rollups. |
| **Global** geometry without antimeridian pain | `gm.s2_cover(geom, ...)` | Spherical cells, no projection seam. |
| **Prefilter** before expensive exact predicates | `gm.SpatialIndex(...)`, `gm.h3_cover(geom, ...)` | Candidate keys; exact answers built in. |
| Change what coordinates **mean** vs. **transform** them | `geom.set_crs(...)` vs. `geom.to_crs(...)` | Declaration is not transformation. |

## `set_crs` declares, `to_crs` transforms

This pair is the second model decision gometry makes unavoidable. They are *not*
interchangeable:

```python
geom.set_crs(4326)   # "these existing numbers are lon/lat" — no coordinates change
geom.to_crs(3857)    # "reproject these coordinates" — every coordinate changes

```

`set_crs` attaches metadata (and validates it through PROJ). `to_crs` runs an actual
coordinate transformation through the bundled PROJ pipeline. Mixing them up is a classic
source of data that is silently in the wrong place. See the [CRS, units & measurement](../guide/crs.md).

## Candidate vs. exact is also explicit

Cell keys are **candidates** — `cell_rule` names the covering rule the coverage was
built with — while membership questions are answered **exactly** against the source
geometry. An index likewise separates `candidates(...)` from exact `query(...)`, and
explains its plan:

```python exec="on" source="block" result="text"
import gometry as gm
poly = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cover = gm.h3_cover(poly, resolution=6)
print('cell_rule:', cover.cell_rule, '| cells:', len(cover.cells))
print('exact membership:', cover.covers(gm.Point(21.0, 52.0, crs=4326)))
pts = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
idx = gm.SpatialIndex(pts)
for step in idx.explain(poly, predicate='contains'):
    print('-', step)

```

You never have to guess whether a result is a coarse prefilter or a refined truth —
candidate spellings are named (`cells`, `candidates`), exact spellings are predicates
(`covers`, `query`), and `explain()` shows the plan. See the
[indexing guide](../guide/indexing.md) and [grids guide](../guide/grids.md).

## Geometries are Python values

gometry's data types behave like first-class Python values, so the language's
own idioms apply — no special API needed:

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
match arr:                        # arrays are real sequences
    case [gm.Point(), gm.Point() as far]:
        print("two points; the far one is", far.to_wkt())
print(gm.Point(3, 4) in arr, arr.index(gm.Point(3, 4)))

a, b = gm.box(0, 0, 2, 2), gm.box(1, 1, 3, 3)
print((a & b).area, (a | b).area)   # & is intersection, | is union

```

Equality is **value** equality (same CRS, epoch, and exact coordinates), hash
agrees with it — geometries and arrays key dicts and live in sets — and
`pickle`/`copy`/`deepcopy` work on every data type (copies of immutable
values are free), so multiprocessing and caching just work. Geometries,
arrays, and CRS support `weakref` for cache workflows; the tiny high-volume
cell wrappers intentionally do not. Cells sort by their id (`sorted(cells)`);
for the numeric-id systems (H3, S2, tiles) `int(cell)` bridges to h3-py /
s2sphere raw ids, while a geohash cell is keyed by its text token instead.

## The shape of the API

One spelling per concept (NumPy/pandas habits map cleanly):

| Kind | Spelling | Example |
|---|---|---|
| **Fact** | property | `geom.area`, `arr.is_empty` |
| **Unary op** | method on `Geometry` / `GeometryArray` | `geom.buffer(d)`, `arr.to_crs(crs)` |
| **Binary relation** | free function | `gm.contains(a, b)`, `gm.distance(a, b)` |
| **Measure override** | free `area` / `length` **with** `unit=` | `gm.area(geom, unit='planar')` |

## Where to go next

- [Arrays & performance](../guide/arrays.md) and [Geometry](../guide/geometry.md) —
  the day-to-day API shape and constructors (array/DataFrame users start here).
- [NumPy interop](../ecosystem/numpy.md) and [DataFrames](../ecosystem/dataframes.md) —
  ndarray handoffs and explicit typed DataFrame conversions.
- [Spatial indexing & joins](../guide/indexing.md) — operational payoff of the index idea.
- [CRS, units & measurement](../guide/crs.md) — metric and projection detail.
- [Discrete grids](../guide/grids.md) — deeper spherical and grid theory.
