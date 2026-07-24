---
description: How gometry fits into the Python geospatial stack — NumPy, Arrow/GeoArrow, GeoParquet, pandas, polars, GeoPandas, lonboard, databases, and the scientific ecosystem, with explicit ownership and copy behavior.
---

# Ecosystem & interoperability

gometry is a geometry **engine**, not a walled garden. It speaks the formats and
protocols the rest of the Python geospatial stack already uses, so you can drop it
into an existing pipeline through direct, typed adapters. Native Arrow capsules
and NumPy views can be zero-copy; encoded and foreign-object boundaries copy as
the matrix states. One Python-facing package (plus mandatory NumPy, with many
statically linked Rust/C components inside the wheel) covers the practical
Shapely + pyproj + h3-py + s2sphere + rtree workflows; everything else
interoperates through standards: NumPy arrays, the Arrow C Data Interface,
GeoArrow, GeoParquet, WKB/WKT/GeoJSON, and pickle. See the
[license inventory](../about/license.md) for the supply-chain detail.

This section is the map. Each page goes deep on one boundary; this page routes you
to the right one and shows, at a glance, what every integration preserves.

## Choose your boundary

- **Coordinates and bulk numeric results** → [NumPy arrays & coordinates](numpy.md).
  NumPy is mandatory and first-class — every dense result is a real typed ndarray.
- **Columnar interchange / lakehouse / storage & maps** → [Arrow & storage](arrow.md)
  (zero-copy via Arrow PyCapsules, pyarrow optional; also GeoParquet files and lonboard maps).
- **DataFrames** → [DataFrames](dataframes.md) (explicit pandas extension
  storage, Polars WKB Series, and GeoPandas round-trip).
- **Files, wire formats, databases** → [Text & binary formats](text-formats.md)
  (WKB/EWKB, WKT/EWKT, GeoJSON, `__geo_interface__`, pickle).

## Partner matrix

What each partner needs, the entry points in and out, and whether geometry moves
without a copy. "Native" integrations need no optional install.

| Partner | Install | → gometry | gometry → | CRS carried | Zero-copy path |
| --- | --- | --- | --- | --- | --- |
| **NumPy** | *(required)* | coordinate buffers → constructors / factories | ndarray returns, `geom.coords.x`/`.y`, [`get_coordinates`][gometry.get_coordinates] | n/a | read-only views |
| **Arrow / GeoArrow** | *(native capsules)* | consume `__arrow_c_array__`/`_stream__` | export `__arrow_c_*__` capsules | PROJJSON | native packed layouts |
| **pyarrow** | `gometry[arrow]` | [`from_arrow`][gometry.from_arrow] | [`to_arrow`][gometry.Geometry.to_arrow] | PROJJSON | native layouts |
| **GeoParquet** | `gometry[arrow]` | `from_geoparquet` | `arr.to_geoparquet()` | PROJJSON header | via pyarrow |
| **pandas** | `gometry[pandas]` | `from_pandas` | `arr.to_pandas()` | dtype object | native extension storage |
| **polars** | `gometry[polars]` | `gm.from_polars(series)` | `arr.to_polars()` | EWKB EPSG SRID only | encode/decode copy |
| **GeoPandas** | `gometry[geopandas]` | `from_geopandas` | `arr.to_geopandas()` | from frame | vectorized WKB |
| **DuckDB / capsule consumers** | *(none)* | — | consume gometry `__arrow_c_*__` | metadata | capsule path |
| **lonboard** | `gometry[viz]` | — | `explore`, `_repr_html_` | reprojects toward WGS84 | GeoArrow capsule |
| **Shapely** | *(none)* | `__geo_interface__`, `GeometryArray([...])` | `geom.__geo_interface__` | no (GeoJSON) | — |
| **pyproj** | *(none)* | `CRS(pyproj_crs)` (duck-typed) | `crs.to_wkt()` / `to_authority()` | — | — |
| **scipy / scikit-learn** | *(none)* | — | [`get_coordinates`][gometry.get_coordinates] → ndarray | project first | NumPy view |
| **pickle / multiprocessing** | *(none)* | `pickle.loads` | `pickle.dumps` | preserved (+ epoch) | — |

Everything tagged `gometry[...]` below is opt-in.

## Optional extras

| Extra | Install | Unlocks |
| --- | --- | --- |
| `arrow` | `pip install gometry[arrow]` | [`to_arrow`][gometry.Geometry.to_arrow] / [`from_arrow`][gometry.from_arrow] materialize pyarrow objects |
| `pandas` | `pip install gometry[pandas]` | concrete pandas extension storage + converters |
| `polars` | `pip install gometry[polars]` | WKB/EWKB Binary Series converters |
| `geopandas` | `pip install gometry[geopandas]` | vectorized GeoSeries/GeoDataFrame conversion |
| `viz` | `pip install gometry[viz]` | `gm.explore` + notebook `_repr_html_` maps |

### Zero import cost when unused

`import gometry` never pulls pandas, Polars, PyArrow, GeoPandas, or lonboard. A
script that only calls `gm.Point`, `gm.distance`, and `gm.h3_cover` imports none
of them. Converter calls import only the requested framework and never mutate
framework classes or registries.

## A note on the NumPy Array API standard

gometry implements the NumPy interchange protocols (`__array__`,
`__array_ufunc__ = None`, the buffer protocol) and returns real typed ndarrays —
but it is **not** a [Python Array API](https://data-apis.org/array-api/) provider
and does not expose `__array_namespace__`. That standard describes *numeric* array
libraries (elementwise math, linear algebra over numbers); a gometry array holds
geometries or grid cells, not numbers. Use gometry to *produce* coordinate arrays,
then hand them to NumPy/SciPy/scikit-learn. See [NumPy arrays &
coordinates](numpy.md) for the full native-NumPy story.
