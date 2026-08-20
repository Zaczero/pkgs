---
description: A hands-on, ten-minute tutorial — build a real spatial query with gometry by finding neighborhoods within a 500-meter straight-line radius of a transit stop.
---

# Quickstart

In this tutorial you'll build something real: **find the neighborhoods within a
500-meter straight-line radius of a transit stop.** Along the way you'll construct geometry,
measure correctly on the Earth, index, query, and bucket into a grid — the whole
gometry loop — in about ten minutes. Every example here runs during the docs
build, so the output is real, not transcribed.

By the end you'll have built this:

```python exec="on" html="true"
from _figures import figure
import gometry as gm

_stop = gm.Point(2.3479, 48.8589, crs=4326)
_catchment = _stop.buffer(500)
_hoods = gm.GeometryArray([
    gm.box(2.344, 48.857, 2.349, 48.861, crs=4326),
    gm.box(2.350, 48.858, 2.355, 48.862, crs=4326),
    gm.box(2.360, 48.865, 2.366, 48.870, crs=4326),
])
_matched = [_hoods[i] for i in gm.SpatialIndex(_hoods).query(_catchment, predicate='intersects')]
print(figure([_catchment, *_matched, _stop], 'neighborhoods intersecting a 500 m radius'))

```

!!! note "Before you start"
    You'll need **Python 3.11+** and gometry installed (next step). This tutorial
    assumes you know basic Python; no GIS background required. Budget about ten
    minutes.

## Install

```bash
pip install gometry
```

## 1. A transit stop

Everything starts with geometry. Create a `Point` for a transit stop from its
longitude and latitude, and tag it with a CRS so gometry knows the coordinates are
degrees on the WGS 84 ellipsoid (EPSG:4326):

```python exec="on" source="block" result="text" session="catchment"
import gometry as gm

stop = gm.Point(2.3479, 48.8589, crs=4326)  # Châtelet, Paris — (lon, lat)
print(stop)

```

In a notebook a geometry draws itself. Here is our stop:

```python exec="on" html="true" session="catchment"
from _figures import figure
print(figure(stop, 'the transit stop'))

```

## 2. A proximity radius

A buffer measures straight-line proximity, not travel time or route distance.
Build a 500-meter radius around the stop. Because the stop carries a *geographic* CRS, gometry reads the distance
through that CRS and buffers in **meters** — `buffer(500)` is 500 meters on the
ground, automatically:

```python exec="on" source="block" result="text" session="catchment"
catchment = stop.buffer(500)                          # 500 meters, the CRS decides
print('catchment area m^2:', round(catchment.area))  # ~ pi * 500^2

```

!!! note "Why this is 500 meters, not 500 degrees"
    Because the CRS is geographic, gometry reads the unit from the CRS and
    `buffer(500)` is 500 meters on the ground — not 500 degrees. (It is a
    local-projection radial buffer, not a walking isochrone.) The
    [mental model](mental-model.md#the-crs-decides-the-result-is-native) has the
    full rule; `unit='planar'` opts out into raw coordinate units.

```python exec="on" html="true" session="catchment"
print(figure([catchment, stop], 'the 500 m catchment'))

```

## 3. The neighborhoods

Real neighborhood polygons come from a file or database; here you'll build three
with [`gm.box`][gometry.box] and hold them in a [`GeometryArray`][gometry.GeometryArray] —
gometry's Rust-owned vectorized container. (WKT/WKB/`from_geojson` are the usual
file/DB ingress paths when you already have text or bytes.)

```python exec="on" source="block" result="text" session="catchment"
hoods = gm.GeometryArray([
    gm.box(2.344, 48.857, 2.349, 48.861, crs=4326),
    gm.box(2.350, 48.858, 2.355, 48.862, crs=4326),
    gm.box(2.360, 48.865, 2.366, 48.870, crs=4326),
])
print(len(hoods), 'neighborhoods loaded')

```

```python exec="on" html="true" session="catchment"
print(figure([catchment, *hoods, stop], 'the catchment and three neighborhoods'))

```

One neighborhood sits well outside the catchment — filter it out.

## 4. Which neighborhoods are in reach?

Build a spatial index over the neighborhoods, then ask which ones `intersects`
the catchment. The index runs a fast bounding-box prefilter and then an exact
geometry test, so the answer is precise — not merely "nearby":

```python exec="on" source="block" result="text" session="catchment"
idx = gm.SpatialIndex(hoods)
reachable = idx.query(catchment, predicate='intersects')
print('rows intersecting the radius:', reachable.tolist())
# row ids → geometries (or use gm.join for two-array attribute joins)
print('matched count:', len(reachable))

```

??? note "What the index is doing"
    `query(..., predicate='intersects')` runs a bounding-box prefilter to a small
    candidate set, then the exact `intersects` predicate on each candidate — the
    [candidate vs. exact](mental-model.md#candidate-vs-exact-is-also-explicit)
    split. `candidates(...)` exposes just the prefilter and `explain(...)` prints
    the plan — see [Spatial indexing & joins](../guide/indexing.md).

The two near neighborhoods match; the far one is dropped:

```python exec="on" html="true" session="catchment"
matched = [hoods[i] for i in reachable]
print(figure([catchment, *matched, stop], 'neighborhoods intersecting a 500 m radius'))

```

## 5. Bucket the catchment into grid cells

Finally, cover the catchment with H3 cells — useful for joining against tiled
datasets or binning at global scale. Keep the source geometry for exact
membership checks:

```python exec="on" source="block" result="text" session="catchment"
import gometry as gm
cells = gm.h3_cover(catchment, resolution=10)
print(len(cells), 'H3 cells cover the catchment')
print('covers the stop:', gm.covers(catchment, stop))

```

```python exec="on" html="true" session="catchment"
print(figure([*cells.polygon, catchment], 'H3 resolution-10 cells'))

```

## Recap

In one page you built a real spatial query using the whole gometry loop:

- **Geometry** — `Point`, `buffer`, and a `GeometryArray`.
- **CRS** — the catchment buffered in meters because the CRS is geographic; you
  never picked "planar vs geodesic" at the call site.
- **Indexing** — `gm.SpatialIndex` with a bounding-box prefilter and exact refinement.
- **Predicates** — `intersects` kept only the reachable neighborhoods.
- **Grids** — an H3 cell cover; use free predicates for exact membership.
- **Vectorization** — one `GeometryArray` carried every neighborhood at once.

## Where to go next

| If you want to... | Read |
| --- | --- |
| understand the planar / geodesic / grid models | [The gometry mental model](mental-model.md) |
| get a specific job done fast | [The user guide](../guide/geometry.md) |
| see why metrics follow the CRS | [CRS, units & measurement](../guide/crs.md) |
| migrate existing Shapely code | [Coming from Shapely](../migrating/index.md#coming-from-shapely) |
| replace pyproj `Transformer` / `Geod` workflows | [Coming from pyproj](../migrating/index.md#coming-from-pyproj) |
| tune joins and candidate/refine pipelines | [Spatial indexing & joins](../guide/indexing.md) |
| look up any callable | [API reference](../api/index.md) |
