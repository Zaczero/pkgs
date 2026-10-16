---
description: How gometry fits the Python geospatial stack — NumPy, Arrow/GeoArrow, GeoParquet, pandas, polars, GeoPandas, and lonboard, with explicit copy behavior.
---

# Ecosystem & interoperability

Use gometry with NumPy, dataframes, Arrow, and common geometry formats.
The adapters make copying and metadata loss explicit.

## Choose your boundary

- **Coordinates and bulk numeric results** → [NumPy arrays & coordinates](numpy.md).
  NumPy is a required dependency; every dense result is a typed ndarray.
- **Columnar interchange / lakehouse / storage & maps** → [Arrow & storage](arrow.md)
  (Arrow PyCapsules, pyarrow optional; also GeoParquet files and lonboard maps).
- **DataFrames** → [DataFrames](dataframes.md) (explicit pandas extension
  storage, Polars WKB Series, and GeoPandas round-trip).
- **Files, wire formats, databases** → [Text & binary formats](text-formats.md)
  (WKB/EWKB, WKT/EWKT, GeoJSON, `__geo_interface__`, pickle).

## Partner matrix

"Native" integrations need no optional install.

| Partner | Install | → gometry | gometry → | CRS carried | Copy/ownership note |
| --- | --- | --- | --- | --- | --- |
| **NumPy** | *(required)* | coordinate buffers → constructors / factories | ndarray returns, [`geom.coords`][gometry.Geometry.coords] → matrix / row-index views | n/a | matrix export copies; per-axis columns may borrow |
| **Arrow / GeoArrow** | *(native capsules)* | consume `__arrow_c_array__`/`__arrow_c_stream__` | export GeoArrow geometry and cell ID/token columns | PROJJSON | packed geometry or primitive cell values |
| **pyarrow** | `gometry[arrow]` | [`from_arrow`][gometry.from_arrow] | [`to_arrow`][gometry.Geometry.to_arrow] | PROJJSON | native layouts |
| **GeoParquet** | `gometry[arrow]` | [`from_geoparquet`][gometry.from_geoparquet] | [`arr.to_geoparquet()`][gometry.GeometryArray.to_geoparquet] | PROJJSON header | via pyarrow |
| **pandas** | `gometry[pandas]` | [`from_pandas`][gometry.from_pandas] | [`arr.to_pandas()`][gometry.GeometryArray.to_pandas] | registered `gometry.geometry` dtype | native extension storage |
| **polars** | `gometry[polars]` | [`gm.from_polars(series)`][gometry.from_polars] | [`arr.to_polars()`][gometry.GeometryArray.to_polars] | EWKB EPSG SRID only | encode/decode copy |
| **GeoPandas** | `gometry[geopandas]` | [`from_geopandas`][gometry.from_geopandas] | [`arr.to_geopandas()`][gometry.GeometryArray.to_geopandas] | from frame | vectorized WKB |
| **Arrow C consumers** | *(none)* | — | consume gometry `__arrow_c_*__` | metadata | capsule path |
| **lonboard** | `gometry[viz]` | — | [`explore`][gometry.explore], notebook `_repr_html_` | reprojects toward WGS84 | Arrow capsule route; preprocessing and widget construction may allocate |
| **Shapely** | *(none)* | `__geo_interface__`, [`GeometryArray([...])`][gometry.GeometryArray] | [`geom.__geo_interface__`](text-formats.md#__geo_interface__), [`Features.__geo_interface__`](text-formats.md#features__geo_interface__) | no (GeoJSON) | — |
| **pyproj** | *(none)* | [`CRS(pyproj_crs)`][gometry.CRS] (duck-typed) | [`crs.to_wkt()`][gometry.CRS.to_wkt] / [`to_authority()`][gometry.CRS.to_authority] | — | — |
| **scipy / scikit-learn** | *(none)* | — | `np.asarray(geom.coords, dtype=np.float64)` → ndarray | project first | matrix copy |
| **pickle / [multiprocessing](https://docs.python.org/3/library/multiprocessing.html)** | *(none)* | [`pickle.loads`](https://docs.python.org/3/library/pickle.html#pickle.loads) | [`pickle.dumps`](https://docs.python.org/3/library/pickle.html#pickle.dumps) | preserved (+ epoch) | — |

## Optional extras

Install an extra for the adapter you use, such as `pip install "gometry[pandas]"`.
See [Installation](../get-started/installation.md#optional-extras) for all extras.

### Zero import cost when unused

`import gometry` never pulls pandas, Polars, PyArrow, arro3, GeoPandas, or lonboard. A
script that only calls [`gm.Point`][gometry.Point], [`gm.distance`][gometry.distance], and [`gm.h3_cover`][gometry.h3_cover] imports none
of them. Converter calls import the requested framework as needed.

## Python Array API compatibility

Gometry is not a Python Array API provider; see [NumPy arrays & coordinates](numpy.md#python-array-api-compatibility)
for the supported NumPy protocols and coordinate handoff.
