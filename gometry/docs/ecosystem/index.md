---
description: How gometry fits the Python geospatial stack — NumPy, Arrow/GeoArrow, GeoParquet, pandas, polars, GeoPandas, and lonboard, with explicit copy behavior.
---

# Ecosystem & interoperability

gometry is a geometry **engine**. It speaks the formats and protocols used by the
Python geospatial stack through direct, typed adapters. Native Arrow capsules
and NumPy views can be zero-copy; encoded and foreign-object boundaries copy.
One Python-facing package (plus mandatory NumPy) covers
workflows commonly split across Shapely, pyproj, h3-py, s2sphere, and rtree;
everything else
interoperates through standards: NumPy arrays, the Arrow C Data Interface,
GeoArrow, GeoParquet, WKB/WKT/GeoJSON, and pickle. See the
[license inventory](../about/license.md) for the supply-chain detail.

## Choose your boundary

- **Coordinates and bulk numeric results** → [NumPy arrays & coordinates](numpy.md).
  NumPy is a required dependency; every dense result is a typed ndarray.
- **Columnar interchange / lakehouse / storage & maps** → [Arrow & storage](arrow.md)
  (zero-copy via Arrow PyCapsules, pyarrow optional; also GeoParquet files and lonboard maps).
- **DataFrames** → [DataFrames](dataframes.md) (explicit pandas extension
  storage, Polars WKB Series, and GeoPandas round-trip).
- **Files, wire formats, databases** → [Text & binary formats](text-formats.md)
  (WKB/EWKB, WKT/EWKT, GeoJSON, `__geo_interface__`, pickle).

## Partner matrix

"Native" integrations need no optional install.

| Partner | Install | → gometry | gometry → | CRS carried | Zero-copy path |
| --- | --- | --- | --- | --- | --- |
| **NumPy** | *(required)* | coordinate buffers → constructors / factories | ndarray returns, [`geom.coords.x`][gometry.Coordinates.x]/[`.y`][gometry.Coordinates.y], [`get_coordinates`][gometry.get_coordinates] | n/a | read-only views |
| **Arrow / GeoArrow** | *(native capsules)* | consume `__arrow_c_array__`/`__arrow_c_stream__` | export `__arrow_c_*__` capsules | PROJJSON | native packed layouts |
| **pyarrow** | `gometry[arrow]` | [`from_arrow`][gometry.from_arrow] | [`to_arrow`][gometry.Geometry.to_arrow] | PROJJSON | native layouts |
| **GeoParquet** | `gometry[arrow]` | [`from_geoparquet`][gometry.from_geoparquet] | [`arr.to_geoparquet()`][gometry.GeometryArray.to_geoparquet] | PROJJSON header | via pyarrow |
| **pandas** | `gometry[pandas]` | [`from_pandas`][gometry.from_pandas] | [`arr.to_pandas()`][gometry.GeometryArray.to_pandas] | dtype object | native extension storage |
| **polars** | `gometry[polars]` | [`gm.from_polars(series)`][gometry.from_polars] | [`arr.to_polars()`][gometry.GeometryArray.to_polars] | EWKB EPSG SRID only | encode/decode copy |
| **GeoPandas** | `gometry[geopandas]` | [`from_geopandas`][gometry.from_geopandas] | [`arr.to_geopandas()`][gometry.GeometryArray.to_geopandas] | from frame | vectorized WKB |
| **Arrow C consumers** | *(none)* | — | consume gometry `__arrow_c_*__` | metadata | capsule path |
| **lonboard** | `gometry[viz]` | — | [`explore`][gometry.explore], notebook `_repr_html_` | reprojects toward WGS84 | GeoArrow capsule |
| **Shapely** | *(none)* | `__geo_interface__`, [`GeometryArray([...])`][gometry.GeometryArray] | `geom.__geo_interface__` | no (GeoJSON) | — |
| **pyproj** | *(none)* | [`CRS(pyproj_crs)`][gometry.CRS] (duck-typed) | [`crs.to_wkt()`][gometry.CRS.to_wkt] / [`to_authority()`][gometry.CRS.to_authority] | — | — |
| **scipy / scikit-learn** | *(none)* | — | [`get_coordinates`][gometry.get_coordinates] → ndarray | project first | NumPy view |
| **pickle / multiprocessing** | *(none)* | `pickle.loads` | `pickle.dumps` | preserved (+ epoch) | — |

## Optional extras

| Extra | Install | Unlocks |
| --- | --- | --- |
| `arrow` | `uv add "gometry[arrow]"` | [`to_arrow`][gometry.Geometry.to_arrow] / [`from_arrow`][gometry.from_arrow] materialize pyarrow objects |
| `pandas` | `uv add "gometry[pandas]"` | concrete pandas extension storage + converters |
| `polars` | `uv add "gometry[polars]"` | WKB/EWKB Binary Series converters |
| `geopandas` | `uv add "gometry[geopandas]"` | vectorized GeoSeries/GeoDataFrame conversion |
| `viz` | `uv add "gometry[viz]"` | [`gm.explore`][gometry.explore] + notebook `_repr_html_` maps |

### Zero import cost when unused

`import gometry` never pulls pandas, Polars, PyArrow, GeoPandas, or lonboard. A
script that only calls [`gm.Point`][gometry.Point], [`gm.distance`][gometry.distance], and [`gm.h3_cover`][gometry.h3_cover] imports none
of them. Converter calls import only the requested framework and never mutate
framework classes or registries.

## Python Array API compatibility

Gometry is not a Python Array API provider; see [NumPy arrays & coordinates](numpy.md#python-array-api-compatibility)
for the supported NumPy protocols and coordinate handoff.
