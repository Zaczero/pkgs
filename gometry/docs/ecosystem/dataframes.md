---
description: Typed conversion between GeometryArray and pandas, Polars, or GeoPandas, with lazy optional integrations and pandas dtype discovery.
---

# DataFrames

Convert dataframe columns to [`GeometryArray`][gometry.GeometryArray] for
geometry computation, then attach the results to your dataframe.

| Extra | Storage | Into gometry | Out of gometry |
| --- | --- | --- | --- |
| `pandas` (`gometry[pandas]`, `pandas>=3.0.3`; add `gometry[arrow]` for GeoArrow-backed `ArrowDtype`) | `GeometryExtensionArray`, GeoArrow-backed `ArrowDtype`, or object Series | [`gm.from_pandas(series)`][gometry.from_pandas] | [`array.to_pandas()`][gometry.GeometryArray.to_pandas] |
| `polars` | WKB/EWKB `Binary` Series | [`gm.from_polars(series)`][gometry.from_polars] | [`array.to_polars()`][gometry.GeometryArray.to_polars] |
| `geopandas` | Shapely geometry column | [`gm.from_geopandas(series_or_frame)`][gometry.from_geopandas] | [`gm.to_geopandas(geometry_or_features)`][gometry.to_geopandas] |

Install the adapters you use:

```bash
uv add "gometry[pandas]"
uv add "gometry[arrow]"
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

A pandas Series backed by `pd.ArrowDtype` can also carry a GeoArrow extension
column. `from_pandas()` passes that Arrow column directly to
[`from_arrow`][gometry.from_arrow], preserving chunks, nulls, dimensions, CRS,
and coordinate epoch without converting rows to Python dictionaries. This path
requires `pyarrow` (`gometry[arrow]`); unrelated Arrow dtypes are rejected as
non-geometry input.

```python exec="on" source="block" result="text"
import pandas as pd

import gometry as gm

points = gm.points([21.0, 22.0], [52.0, 52.5], crs=4326)
buffered = points.buffer(500)
series = buffered.to_pandas(name="geometry")
restored = gm.from_pandas(series)
typed = pd.Series([gm.Point(23.0, 53.0, crs=4326)], dtype='gometry.geometry')

print("dtype:", series.dtype)
print("registered dtype:", typed.dtype)
print("rows:", len(series))
print("frame:", restored.crs.to_authority())
```

```python exec="on" source="block" result="text"
import pandas as pd
import pyarrow as pa

import gometry as gm

geometry = gm.points([1.0, 2.0], [3.0, 4.0], crs=4326, epoch=2020.0)
arrow = geometry.to_arrow()
chunked = pa.chunked_array([arrow.slice(0, 1), arrow.slice(1)])
series = pd.Series(chunked, dtype=pd.ArrowDtype(chunked.type))
restored = gm.from_pandas(series)

print("dtype:", series.dtype)
print(
    "stored CRS definition preserved:",
    restored.crs.definition == geometry.crs.definition,
)
print("epoch:", restored.epoch)
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

`GeometryDtype` registers as `gometry.geometry` when the pandas adapter is
imported. Use that name with `pandas_dtype`, `Series(..., dtype=...)`, and
`astype(...)`; `array.to_pandas()` remains the direct path from gometry.

Missing rows are native `GeometryArray` state. `None` and
`pd.NA` are missing; an empty geometry is a value. pandas operations such as
`dropna`, `factorize`, `groupby`, and `value_counts` use the extension-array
protocol directly.

To calculate a new column, convert once, compute in gometry, then attach the
result:

```python exec="on" source="block" result="text"
import gometry as gm

points = gm.points([21.0, 22.0], [52.0, 52.5], crs=4326)
series = points.to_pandas(name="geometry")
frame = series.to_frame()
geometry = gm.from_pandas(series)
frame["area"] = geometry.area
frame["buffered"] = geometry.buffer(500).to_pandas(index=frame.index)
print(frame.columns.tolist())
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

`gm.from_geopandas()` decodes a GeoSeries to a `GeometryArray`, or a
GeoDataFrame to aligned `Features`. Geometry conversion uses vectorized WKB.
`gm.to_geopandas()` returns a GeoSeries for geometries and a GeoDataFrame for
features.

```python exec="on" source="block" result="text"
import geopandas as gpd
import gometry as gm

frame = gpd.GeoDataFrame(
    {"fid": [0, 7], "name": ["A", "B"]},
    geometry=gpd.points_from_xy([21.0, 22.0], [52.0, 52.5], crs=4326),
)
features = gm.from_geopandas(frame, id_column="fid")
back = gm.to_geopandas(features)
print(type(back).__name__, len(back), back.crs)
print(list(gm.from_geopandas(back).ids))
```

The ID role is recorded in `GeoDataFrame.attrs["gometry:feature_id"]`; a
dataframe operation that drops `attrs` also drops that hint. Supply `id_column`
again to restore it, or pass `id_column=None` to treat the column as an ordinary
property. Neither the dataframe index nor a column named `id` is inferred as
the feature ID. Sparse properties become nullable dataframe columns.

This boundary copies. Use the pandas extension path when native shared geometry
storage is the goal. Epoch-bearing geometries require `drop_epoch=True` when
exporting because GeoPandas cannot represent the coordinate epoch.

For interactive maps, pass the aligned snapshot directly to `gm.explore(features)`.
Its selected properties and IDs travel with the geometry into lonboard; no
separate attribute table is needed.

## Import behavior

`import gometry` imports none of pandas, Polars, GeoPandas, PyArrow, or
lonboard. Accessing a converter imports only its requested optional framework.
No manual setup is needed for the converters.

## See also

- [Arrow & storage](arrow.md) — GeoArrow capsules, PyArrow objects, and GeoParquet.
- [Text & binary formats](text-formats.md) — the WKB/EWKB encoding used by Polars.
- [Ecosystem & interoperability](index.md) — the complete boundary matrix.
