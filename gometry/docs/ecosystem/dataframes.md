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
| `pandas` | `GeometryExtensionArray` | `gm.from_pandas(series)` | `array.to_pandas()` |
| `polars` | WKB/EWKB `Binary` Series | `gm.from_polars(series)` | `array.to_polars()` |
| `geopandas` | Shapely geometry column | `gm.from_geopandas(series_or_frame)` | `array.to_geopandas()` |

Install only the boundary you use:

```bash
pip install 'gometry[pandas]'
pip install 'gometry[polars]'
pip install 'gometry[geopandas]'
```

## pandas extension storage

`GeometryArray.to_pandas()` constructs a Series backed by gometry's concrete
extension array. The Series and gometry array share immutable geometry storage;
`gm.from_pandas()` returns the native column without a WKB round trip.

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

The internal dtype object reports a descriptive name, but gometry does not
register it with pandas or expose a second construction API. Construct geometry
columns through `array.to_pandas()`. This keeps imports and conversions free of
global side effects.

Missing rows remain first-class native `GeometryArray` state. `None` and
`pd.NA` are missing; an empty geometry is a value. pandas operations such as
`dropna`, `factorize`, `groupby`, and `value_counts` use the extension-array
protocol directly.

To calculate a new column, convert once, compute in gometry, then attach the
result:

```python
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
series = points.to_polars(name="geometry")
geometry = gm.from_polars(series)
areas = geometry.area

print("storage:", series.dtype)
print("rows:", series.len())
print("areas:", areas.tolist())
```

An EPSG CRS rides as an EWKB SRID. Other CRS definitions and coordinate epochs
do not fit WKB: acknowledge their loss with `drop_crs=True` or
`drop_epoch=True`, then restore them explicitly through `gm.from_polars(...,
crs=..., epoch=...)`. Missing rows encode as Polars nulls and round-trip as
missing geometry rows.

There is deliberately no Polars Series or Expr namespace. A single statically
typed gometry API is easier to discover and keeps lazy-plan behavior free of
Python UDF magic.

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

This boundary copies because Shapely and gometry own different geometry
representations. Use the pandas extension path when native shared storage is
the goal.

## Import behavior

`import gometry` imports none of pandas, Polars, GeoPandas, PyArrow, or
lonboard. Accessing a converter imports only its requested optional framework.
Import order has no behavioral meaning because there is no import hook or
registration step.

## See also

- [Arrow & storage](arrow.md) — GeoArrow capsules, PyArrow objects, and GeoParquet.
- [Text & binary formats](text-formats.md) — the WKB/EWKB encoding used by Polars.
- [Ecosystem & interoperability](index.md) — the complete boundary matrix.
