---
description: A hands-on tutorial that builds a spatial query with gometry by finding neighborhoods within a 500-meter straight-line radius of a transit stop.
---

# Quickstart

This tutorial builds one spatial query: given a transit stop, find the
neighborhoods within a 500-meter straight-line radius, then bucket that radius
into H3 cells. It needs **Python 3.11+**.

## Install

=== "uv"

    ```bash
    uv add gometry
    ```

=== "pip"

    ```bash
    pip install gometry
    ```

## 1. A transit stop

Create a [`Point`][gometry.Point] for a transit stop from its
longitude and latitude, and tag it with a CRS so gometry knows the coordinates are
degrees on the WGS 84 ellipsoid (EPSG:4326):

```python exec="on" source="block" result="text" session="catchment"
import gometry as gm

stop = gm.Point(2.3479, 48.8589, crs=4326)  # Châtelet, Paris — (lon, lat)
print(stop)

```

```python exec="on" html="true" session="catchment"
from _figures import figure
print(figure(stop, 'the transit stop'))

```

## 2. A proximity radius

A buffer models straight-line proximity, not travel time or route distance. On a
[geographic CRS](../guide/crs.md), [`buffer(500)`][gometry.Geometry.buffer] uses a local-projection radial
distance of 500 meters rather than 500 degrees; `unit='planar'` selects raw
coordinate units:

```python exec="on" source="block" result="text" session="catchment"
catchment = stop.buffer(500)                          # 500 meters, the CRS decides
print('catchment area m^2:', round(catchment.area))  # ~ pi * 500^2

```

```python exec="on" html="true" session="catchment"
print(figure([catchment, stop], 'the 500 m catchment'))

```

## 3. Ingest named neighborhoods

[`gm.from_features`][gometry.from_features] parses a GeoJSON feature collection into a [`Features`][gometry.Features]
result with geometries in a [`GeometryArray`][gometry.GeometryArray] and parallel
properties as ordinary Python mappings.

```python exec="on" source="block" result="text" session="catchment"
payload = '''
{"type":"FeatureCollection","features":[
  {"type":"Feature","id":"north","properties":{"name":"North"},
   "geometry":{"type":"Polygon","coordinates":[[[2.344,48.857],[2.349,48.857],[2.349,48.861],[2.344,48.861],[2.344,48.857]]]}},
  {"type":"Feature","id":"east","properties":{"name":"East"},
   "geometry":{"type":"Polygon","coordinates":[[[2.350,48.858],[2.355,48.858],[2.355,48.862],[2.350,48.862],[2.350,48.858]]]}},
  {"type":"Feature","id":"far","properties":{"name":"Far"},
   "geometry":{"type":"Polygon","coordinates":[[[2.360,48.865],[2.366,48.865],[2.366,48.870],[2.360,48.870],[2.360,48.865]]]}}
]}
'''
features = gm.from_features(payload, crs=4326)
hoods, names, ids = features.geometries, features.properties, features.ids
print(len(hoods), 'neighborhoods loaded:', [row['name'] for row in names])

```

```python exec="on" html="true" session="catchment"
print(figure([catchment, *hoods, stop], 'the catchment and three neighborhoods'))

```

## 4. Which neighborhoods are in reach?

Build a spatial index over the neighborhoods, then ask which ones [`intersects`][gometry.intersects]
the catchment. The index runs a bounding-box prefilter and then an exact
geometry test; [`query`][gometry.SpatialIndex.query] returns row indices for intersecting geometries rather
than candidate envelopes, while [`SpatialIndex.candidates`][gometry.SpatialIndex.candidates]
returns prefilter row indices:

```python exec="on" source="block" result="text" session="catchment"
idx = gm.SpatialIndex(hoods)
reachable = idx.query(catchment, predicate='intersects')
print('rows intersecting the radius:', reachable.tolist())
print('matched names:', [names[i]['name'] for i in reachable])
# row ids → geometries (or use gm.join for two-array attribute joins)
print('matched count:', len(reachable))

```

```python exec="on" html="true" session="catchment"
matched = [hoods[i] for i in reachable]
print(figure([catchment, *matched, stop], 'neighborhoods intersecting a 500 m radius'))

```

## 5. Bucket the catchment into grid cells

[`gm.h3_cover`][gometry.h3_cover] returns H3 cells for joining tiled datasets or binning at global
scale; exact membership uses the source geometry with [`gm.covers`][gometry.covers]:

```python exec="on" source="block" result="text" session="catchment"
import gometry as gm
cells = gm.h3_cover(catchment, resolution=10)
print(len(cells), 'H3 cells cover the catchment')
print('covers the stop:', gm.covers(catchment, stop))

```

```python exec="on" html="true" session="catchment"
print(figure([*cells.polygon, catchment], 'H3 resolution-10 cells'))

```

## Where to go next

| If you want to... | Read |
| --- | --- |
| understand the planar / geodesic / grid models | [The gometry mental model](mental-model.md) |
| get a specific job done | [The user guide](../guide/geometry.md) |
| see why metrics follow the CRS | [CRS, units & measurement](../guide/crs.md) |
| migrate existing Shapely code | [Coming from Shapely](../migrating/index.md#coming-from-shapely) |
| replace pyproj `Transformer` / `Geod` workflows | [Coming from pyproj](../migrating/index.md#coming-from-pyproj) |
| tune joins and candidate/refine pipelines | [Spatial indexing & joins](../guide/indexing.md) |
| look up any callable | [API reference](../api/index.md) |
