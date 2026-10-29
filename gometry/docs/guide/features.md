---
description: Work with geometries, properties, and IDs together using immutable feature snapshots.
---

# Features & attributes

Use [`Features`][gometry.Features] when each geometry has properties or an ID.
It keeps those values aligned as you select and transform rows.

```python exec="on" source="block" result="text"
import gometry as gm

places = gm.Features(
    gm.points([1, 2, 3], [4, 5, 6], crs=4326),
    properties=[
        {"name": "A", "rank": 2},
        {"name": "B", "rank": 1},
        {"name": "C", "rank": 3},
    ],
    ids=[10, 11, 12],
)

selected = places[[2, 0]]
print(list(selected.column("name")))
print(list(selected.ids))
```

Integer indexing returns a [`Feature`][gometry.Feature] row. Slices, integer
selections, and boolean masks return `Features`, carrying the corresponding
properties and IDs with the geometries.

## Filter and update

Compute a mask from the geometry array, then apply it to the feature snapshot:

```python exec="on" source="block" result="text"
import gometry as gm

places = gm.Features(
    gm.points([1, 2, 3], [4, 5, 6], crs=4326),
    properties=[{"name": "A"}, {"name": "B"}, {"name": "C"}],
)
area = gm.box(0, 0, 2, 5, crs=4326)
nearby = places[gm.intersects(places.geometries, area)]
print(list(nearby.column("name")))
```

Snapshots are immutable. Use `with_geometries`, `with_columns`, or `with_ids`
to create a replacement, and `select_columns` to choose properties. Unchanged
data can be shared with the original.

```python exec="on" source="block" result="text"
import gometry as gm

places = gm.Features(
    gm.points([1, 2], [4, 5]),
    properties=[{"name": "A"}, {"name": "B"}],
)
ranked = places.with_columns({"rank": [2, 1]})
print(list(ranked.column("rank")))
```

Selections can keep their parent storage alive. Call `materialize()` when a
small selection should own compact storage instead.

## Exchange features

- [GeoJSON](../ecosystem/text-formats.md#__geo_interface__) reads and writes
  features with their properties and IDs.
- [Arrow & GeoParquet](../ecosystem/arrow.md#feature-tables) exchange typed
  columns and preserve the declared ID role.
- [GeoPandas](../ecosystem/dataframes.md#geopandas-interchange) converts a
  GeoDataFrame to and from `Features`.
- [Mapbox Vector Tiles](../ecosystem/vector-tiles.md) prepare features for
  rendering as tiles.
