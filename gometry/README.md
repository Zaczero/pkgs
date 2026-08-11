# gometry

**gometry** is a Rust-backed Python geospatial package for day-to-day geometry,
CRS, geodesy, H3/S2 grids, spatial indexing, and [GeoArrow](https://geoarrow.org/) interchange. It
replaces the practical stack of Shapely, pyproj, h3-py, s2sphere, and rtree with
one coherent, fast, typed API.

- **Apache-2.0 OR MIT** · **Python ≥ 3.11** · **Docs:** <https://gometry.monicz.dev/>

It is designed around one rule: **the CRS decides the measure.** Distance returns
geodesic metres and area returns geodesic **square metres** on a geographic CRS,
native units on a projected one, and raw coordinate units on a CRS-free geometry —
with a per-call `unit='planar'` escape. Grid coverage uses `gm.h3_cover` /
`gm.s2_cover` / `gm.geohash_cover` / `gm.tile_cover`.

```python
import gometry as gm
city = gm.box(2.0, 48.0, 3.0, 49.0, crs=4326)
station = gm.Point(2.35, 48.85, crs=4326)
print(gm.contains(city, station))                         # True
print(f"{city.area / 1_000_000.0:.1f} km^2")              # 8217.6 km^2
print(f"{gm.area(city, unit='planar'):.4f} degree^2")     # 1.0000 degree^2

```

Arrays return NumPy ndarrays instead of Python-object lists:

```python
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
points = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
mask = gm.contains(area, points)
print(type(mask).__name__, mask.tolist())                  # ndarray [True, False]
bounds = points.bounds
print(type(bounds).__name__, bounds.shape, bounds.dtype)   # ndarray (2, 4) float64
cover = gm.s2_cover(area, level=10, max_cells=None)
print(cover.cell_rule, cover.covers(points).tolist())      # overlap [True, False]

```

## Install

Install the core package from PyPI:

```bash
python -m pip install gometry
# or
uv add gometry
```

Optional integrations stay explicit; install only what the application uses:

```bash
python -m pip install "gometry[arrow]"       # PyArrow / GeoArrow objects
python -m pip install "gometry[pandas]"      # pandas extension storage
python -m pip install "gometry[polars]"      # Polars binary columns
python -m pip install "gometry[geopandas]"   # GeoPandas conversion
python -m pip install "gometry[viz]"         # lonboard exploration
```

With the `arrow` extra installed, `to_arrow` materializes pyarrow objects; without
it, geometries still expose Arrow PyCapsules for dependency-free consumers.

## The footgun it removes

Bare planar metrics on lon/lat data silently return square degrees. gometry makes
the CRS decide, so the default is the real-world answer:

```python
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
print(f"{area.area:.0f} m^2")                          # 30562197104 m^2
print(f"{gm.area(area, unit='planar'):.1f} degree^2")  # 4.0 degree^2

```

The default measures for the CRS — geodesic metres (and square metres for area) on
a geographic CRS, native linear units on a projected one, coordinate units when
CRS-free. Pass `unit='meters'` for SI on a non-meter projected CRS, `unit='planar'`
for raw coordinate math, or `to_crs(geom.estimate_local_crs())` for a local metric
frame.

## What it replaces

One coherent API for workflows that otherwise require a stack of separate
libraries:

| Instead of | use |
|---|---|
| Shapely geometry, predicates, overlays, constructive ops | `gm.Point/box/...`, `gm.contains(a, b)`, `geom.buffer(...)` |
| pyproj transforms and CRS introspection | `geom.to_crs(...)`, `gm.CRS(...)`, `gm.crs_*` |
| geographiclib / haversine geodesy | CRS-aware `area`/`length`/`distance`, `gm.bearing(..., path='rhumb')` / `gm.destination(..., path='rhumb')` |
| h3-py, s2sphere | `gm.h3_cover(geom, ...)` / `gm.s2_cover(geom, ...)`, typed cells and coverage |
| pygeohash, mercantile | `gm.geohash_cover(geom, ...)` / `gm.tile_cover(geom, ...)` |
| rtree | `gm.SpatialIndex(values)` with exact-predicate refinement |
| polyline, openlocationcode | `gm.from_polyline(...)`, `gm.pluscode_*` |

See the [migration guide](https://gometry.monicz.dev/migrating/) for symbol-level
mappings.

## Highlights

- **Scalar and vectorized** construction, predicates, measures, overlays, and
  constructive operations — the same names work on a `Geometry` or a
  Rust-owned `GeometryArray`.
- **CRS-aware metrics**, native by default, with `unit='planar'` / `unit='meters'`
  overrides and first-class coordinate-epoch support.
- **Exact grid coverage** — H3, S2, geohash, and XYZ tiles share one cell and
  coverage shape, and answer membership against the source geometry, not a
  bounding-box superset.
- **Spatial index** with explicit candidate vs exact-predicate refinement and
  CRS-aware distance queries.
- **GeoArrow** packed arrays for homogeneous XY/XYZ/XYM/XYZM geometries, with
  WKB fallback for mixed types, and WKT/WKB/EWKB/GeoJSON codecs.
- **Typed end to end** — a complete type stub, precisely narrowed signatures,
  public cross-grid protocols, and a structured exception hierarchy.

## Boundaries

gometry measures and indexes geometry; it is not a dataframe or a data-loading
layer. `GeometryArray` requires a coherent CRS, S2 support is cell/coverage
oriented rather than a full spherical boolean engine, and GeoParquet/dataframe
engines are integration boundaries rather than bundled dependencies.

## Documentation

The full guide, migration notes, and API reference are available at
<https://gometry.monicz.dev/>. Their canonical sources live under
`docs/` and build with `properdocs build --strict`.

## License

Distributed under the **Apache-2.0 OR MIT** dual license; use it under the terms
of either license, at your option. gometry bundles libPROJ — see
[the license page](https://gometry.monicz.dev/about/license/) for third-party
notices.
