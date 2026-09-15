---
description: A hands-on tutorial that builds a spatial query with gometry by finding authored tutorial blocks within a 500-meter straight-line radius of a station.
---

# Quickstart

This tutorial builds one spatial query: given a station, find three fictional
tutorial blocks within a 500-meter straight-line service area, then bucket that
area into H3 cells. The fixture is illustrative. It sits near the
Eiffel Tower only to make the coordinates recognizable; its rectangles are not
administrative boundaries or a real boundary dataset. It needs **CPython 3.11–3.14**
(or free-threaded CPython 3.14t).

## Install

=== "uv"

    ```bash
    uv add gometry
    ```

=== "pip"

    ```bash
    pip install gometry
    ```

## 1. A station

Create a [`Point`][gometry.Point] for a station from its
longitude and latitude, and tag it with a CRS so gometry knows the coordinates are
degrees on the WGS 84 ellipsoid (EPSG:4326):

```python exec="on" source="block" result="text" session="catchment"
import gometry as gm

station = gm.Point(
    2.2945,
    48.8584,
    crs=4326,
)
print(station)

```

## 2. A proximity radius

A buffer models straight-line proximity, not travel time or route distance. On a
[geographic CRS](../guide/crs.md), [`buffer(500)`][gometry.Geometry.buffer] uses a local-projection radial
distance of 500 meters rather than 500 degrees; `unit='planar'` selects raw
coordinate units:

```python exec="on" source="block" result="text" session="catchment"
service_area = station.buffer(500)
print('service area m²:', round(service_area.area))

```

## 3. Ingest named tutorial blocks

[`gm.from_features`][gometry.from_features] parses a GeoJSON feature collection into a [`Features`][gometry.Features]
result with geometries in a [`GeometryArray`][gometry.GeometryArray] and parallel
properties as ordinary Python mappings.

```python exec="on" source="block" result="text" session="catchment"
payload = '''
{"type":"FeatureCollection","features":[
  {"type":"Feature","id":"champ-edge","properties":{"name":"Champ-de-Mars edge"},
   "geometry":{"type":"Polygon","coordinates":[[[2.290,48.855],[2.296,48.855],[2.296,48.858],[2.290,48.858],[2.290,48.855]]]}},
  {"type":"Feature","id":"trocadero-side","properties":{"name":"Trocadero side"},
   "geometry":{"type":"Polygon","coordinates":[[[2.287,48.860],[2.293,48.860],[2.293,48.864],[2.287,48.864],[2.287,48.860]]]}},
  {"type":"Feature","id":"out-of-range","properties":{"name":"Out-of-range block"},
   "geometry":{"type":"Polygon","coordinates":[[[2.305,48.865],[2.311,48.865],[2.311,48.870],[2.305,48.870],[2.305,48.865]]]}}
]}
'''
features = gm.from_features(payload, crs=4326)
blocks, properties, ids = features.geometries, features.properties, features.ids
names = [row['name'] for row in properties]
print(len(blocks), 'tutorial blocks loaded:', names)

```

```python exec="on" html="true" session="catchment"
from _figures import figure
print(figure(
    {'source': blocks, 'query': service_area, 'anchor': station},
    'The station, its 500 m service area, and the three tutorial blocks.',
    alt='A station anchors a 500 m service area drawn over the three tutorial blocks it will be tested against.',
))

```

## 4. Which blocks are in reach?

Build a spatial index over the blocks, then ask which ones [`intersects`][gometry.intersects]
the service area. The index runs a bounding-box prefilter and then an exact
geometry test; [`query`][gometry.SpatialIndex.query] returns row indices for intersecting geometries rather
than candidate envelopes, while [`SpatialIndex.candidates`][gometry.SpatialIndex.candidates]
returns prefilter row indices:

```python exec="on" source="block" result="text" session="catchment"
idx = gm.SpatialIndex(blocks)
reachable = idx.query(service_area, predicate='intersects')
print('rows intersecting the radius:', reachable.tolist())
matched_names = [names[i] for i in reachable]
print('matched names:', matched_names)
print('matched count:', len(reachable))
assert reachable.tolist() == [0, 1]
assert matched_names == ['Champ-de-Mars edge', 'Trocadero side']

```

```python exec="on" html="true" session="catchment"
matched_rows = set(reachable.tolist())
print(figure(
    {
        'query': service_area,
        'result': [blocks[i] for i in reachable],
        'excluded': [b for i, b in enumerate(blocks) if i not in matched_rows],
        'anchor': station,
    },
    'Two blocks intersect the service area; the third is out of range.',
    alt='The query returns the two blocks that intersect the service area and excludes the out-of-range block.',
))

```

## 5. Bucket the service area into grid cells

[`gm.h3_cover`][gometry.h3_cover] returns H3 cells for joining tiled datasets or binning at global
scale; exact membership uses the source geometry with [`gm.covers`][gometry.covers]:

```python exec="on" source="block" result="text" session="catchment"
import gometry as gm
cells = gm.h3_cover(service_area, resolution=10)
print('H3 cover created:', len(cells) > 0)
print('source covers the station:', gm.covers(service_area, station))

```

```python exec="on" html="true" session="catchment"
print(figure(
    {'candidate': cells.polygon, 'source': service_area, 'anchor': station},
    'H3 cells cover the service area; the geometry still decides membership.',
    alt='H3 cells are join keys that can extend beyond the source service area, so the source geometry decides exact membership.',
))

```

## Where to go next

| If you want to... | Read |
| --- | --- |
| understand the planar / geodesic / grid models | [The gometry mental model](mental-model.md) |
| get a specific job done | [The user guide](../guide/index.md) |
| see why metrics follow the CRS | [CRS, units & measurement](../guide/crs.md) |
| migrate existing Shapely code | [Coming from Shapely](../migrating/index.md#coming-from-shapely) |
| replace pyproj `Transformer` / `Geod` workflows | [Coming from pyproj](../migrating/index.md#coming-from-pyproj) |
| tune joins and candidate/refine pipelines | [Spatial indexing & joins](../guide/indexing.md) |
| look up any callable | [API reference](../api/index.md) |
