---
description: Typed, explicit conversion between GeometryArray and pandas, Polars, or GeoPandas without global registration or import-order behavior.
---

# DataFrames

DataFrame integrations are explicit storage boundaries. Geometry computation
stays on [`GeometryArray`][gometry.GeometryArray]; converting a column never
installs accessors, mutates a framework class, or changes a process-wide dtype
registry.

| Extra | Storage | Into gometry | Out of gometry |
| --- | --- | --- | --- |
| `pandas` | `GeometryExtensionArray` or object Series of geometry-like rows | [`gm.from_pandas(series)`][gometry.from_pandas] | [`array.to_pandas()`][gometry.GeometryArray.to_pandas] |
| `polars` | WKB/EWKB `Binary` Series | [`gm.from_polars(series)`][gometry.from_polars] | [`array.to_polars()`][gometry.GeometryArray.to_polars] |
| `geopandas` | Shapely geometry column | [`gm.from_geopandas(series_or_frame)`][gometry.from_geopandas] | [`array.to_geopandas()`][gometry.GeometryArray.to_geopandas] |

Install only the boundary you use:

```bash
uv add "gometry[pandas]"
uv add "gometry[polars]"
uv add "gometry[geopandas]"
```

## pandas geometry storage

`GeometryArray.to_pandas()` constructs a Series backed by gometry's concrete
extension array. The Series and gometry array share immutable geometry storage,
so `gm.from_pandas()` returns that native column without a WKB round trip.
`from_pandas()` also imports ordinary object Series whose rows are Geometry
objects, WKB buffers, GeoJSON mappings, or objects exposing
`__geo_interface__`. That path constructs geometry rows rather than sharing
the Series' storage. `None`, `pd.NA`, and pandas-recognized missing values
become missing geometry rows; empty geometries remain values. Use
`from_geopandas()` for a GeoSeries so its CRS metadata is retained.

```python exec="on" source="block" result="text"
import gometry as gm

points = gm.points([21.0, 22.0], [52.0, 52.5], crs=4326)
buffered = points.buffer(500)
series = buffered.to_pandas(name="geometry")
restored = gm.from_pandas(series)

print("dtype:", series.dtype)
print("rows:", len(series))
print("frame:", restored.crs.to_authority())
```

```python exec="on" source="block" result="text"
import pandas as pd
import gometry as gm

series = pd.Series(
    [gm.Point(1, 2), pd.NA, {"type": "Point", "coordinates": [3, 4]}],
    dtype=object,
)
geometries = gm.from_pandas(series)
print(geometries.to_wkt())
```

The extension dtype reports a descriptive name, but gometry does not
register it with pandas or expose a second construction API. Construct geometry
columns through `array.to_pandas()`. This keeps imports and conversions free of
global side effects.

Missing rows are native `GeometryArray` state. `None` and
`pd.NA` are missing; an empty geometry is a value. pandas operations such as
`dropna`, `factorize`, `groupby`, and `value_counts` use the extension-array
protocol directly.

To calculate a new column, convert once, compute in gometry, then attach the
result:

```python title="partial: continues the preceding pandas example"
points = gm.points([21.0, 22.0], [52.0, 52.5], crs=4326)
series = points.to_pandas(name="geometry")
frame = series.to_frame()
geometry = gm.from_pandas(series)
frame["area"] = geometry.area
frame["buffered"] = geometry.buffer(500).to_pandas(index=frame.index)
```

## Polars binary storage

Polars stores the boundary as a `Binary` Series. `to_polars()` encodes the
whole column through native batched WKB; `from_polars()` decodes it in one
native call. Neither direction requires PyArrow.

```python exec="on" source="block" result="text"
import gometry as gm

points = gm.points([21.0, 22.0], [52.0, 52.5], crs=4326)
geometries = points.buffer(500)
series = geometries.to_polars(name="geometry")
geometry = gm.from_polars(series)
areas = geometry.area

print("storage:", series.dtype)
print("rows:", series.len())
print("areas:", areas.tolist())
```

An EPSG CRS rides as an EWKB SRID. Other CRS definitions and coordinate epochs
are not representable in WKB. `drop_crs=True` removes non-EPSG CRS metadata and
`drop_epoch=True` removes the coordinate epoch; restore them explicitly through
`gm.from_polars(..., crs=..., epoch=...)`. Missing rows encode as Polars nulls
and round-trip as missing geometry rows.

gometry registers no Polars Series or Expr namespace. `to_polars()` eagerly
encodes a `GeometryArray` as a binary Series, and `gm.from_polars()` eagerly
decodes the supplied Series into a `GeometryArray` in one native batch. These
conversions run when called; they do not become deferred `LazyFrame`
expressions.

## GeoPandas interchange

`gm.from_geopandas()` decodes a GeoSeries or GeoDataFrame geometry column
through vectorized WKB. `GeometryArray.to_geopandas()` returns a GeoSeries with
the array CRS.

```python exec="on" source="block" result="text"
import geopandas as gpd
import gometry as gm

frame = gpd.GeoDataFrame(
    {"name": ["A", "B"]},
    geometry=gpd.points_from_xy([21.0, 22.0], [52.0, 52.5], crs=4326),
)
geometry = gm.from_geopandas(frame)
back = geometry.to_geopandas()
print(type(back).__name__, len(back), back.crs)
```

This boundary copies. Use the pandas extension path when native shared storage
is the goal.

## Import behavior

`import gometry` imports none of pandas, Polars, GeoPandas, PyArrow, or
lonboard. Accessing a converter imports only its requested optional framework.
Import order has no behavioral meaning because there is no import hook or
registration step.

## See also

- [Arrow & storage](arrow.md) — GeoArrow capsules, PyArrow objects, and GeoParquet.
- [Text & binary formats](text-formats.md) — the WKB/EWKB encoding used by Polars.
- [Ecosystem & interoperability](index.md) — the complete boundary matrix.
