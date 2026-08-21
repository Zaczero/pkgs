<p align="center">
  <img src="docs/assets/logo.svg" alt="gometry" width="180">
</p>

<h1 align="center">gometry</h1>

<p align="center">
  <strong>Blazing-fast geospatial engine for Python</strong>, written in Rust.<br>
  One package for geometry, CRS, geodesy, grids, and spatial indexing.
</p>

<p align="center">
  <a href="https://pypi.org/p/gometry"><img src="https://shields.monicz.dev/pypi/pyversions/gometry" alt="PyPI - Python Version"></a>
  <a href="https://liberapay.com/Zaczero/"><img src="https://shields.monicz.dev/liberapay/patrons/Zaczero?logo=liberapay&amp;label=Patrons" alt="Liberapay Patrons"></a>
  <a href="https://github.com/sponsors/Zaczero"><img src="https://shields.monicz.dev/github/sponsors/Zaczero?logo=github&amp;label=Sponsors&amp;color=%23db61a2" alt="GitHub Sponsors"></a>
</p>

<p align="center">
  <a href="https://gometry.monicz.dev/">Documentation</a> ·
  <a href="https://gometry.monicz.dev/get-started/quickstart/">Quickstart</a> ·
  <a href="https://gometry.monicz.dev/api/">API reference</a> ·
  <a href="https://gometry.monicz.dev/migrating/">Migrating</a>
</p>

---

- **One package instead of six** — Shapely, pyproj, h3-py, s2sphere, rtree,
  mercantile, and pygeohash, with no GEOS, no GDAL, and no system PROJ
- **The CRS decides the measure** — geodesic metres on a geographic CRS, native
  linear units on a projected one, raw coordinate units without one. See the
  [CRS, units & measurement guide](https://gometry.monicz.dev/guide/crs/)
- **Vectorized in Rust** — predicates, overlays, buffering, geodesics, and
  coverage run as batched kernels over packed buffers and return NumPy arrays.
  The same names work on a `Geometry` or on a Rust-owned `GeometryArray`
- **Interchange without conversion** — [GeoArrow](https://geoarrow.org/) packed
  arrays for homogeneous XY/XYZ/XYM/XYZM geometries, WKB fallback for mixed
  types, and WKT/WKB/EWKB/GeoJSON codecs
- **Grids and indexes on one geometry type** — H3, S2, geohash, and XYZ tiles
  share a typed `CellArray`; spatial indexes keep bounding-box `candidates`
  separate from exact `query`
- **Millimetre-accurate geodesics** on the ellipsoid, with coordinate-epoch
  support and `unit='planar'` / `unit='meters'` overrides
- **Typed** — stubs, narrowed signatures, public cross-grid protocols, and a
  structured exception hierarchy
- **Apache-2.0 OR MIT** · **Python ≥ 3.11**

## gometry in action

One `area` call, two answers: the geometry carries `crs=4326`, so it measures in
geodesic **square metres**, and `unit='planar'` drops to raw coordinate units.

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
points = gm.points([2.35, 30.0], [48.85, 52.0], crs=4326)
mask = gm.contains(area, points)
print(type(mask).__name__, mask.tolist())                  # ndarray [True, False]
bounds = points.bounds
print(type(bounds).__name__, bounds.shape, bounds.dtype)   # ndarray (2, 4) float64
cover = gm.s2_cover(area, level=10, max_cells=None)
print(len(cover), gm.covers(area, points).tolist())         # 160 [True, False]

```

## Install

Install the core package from PyPI:

```bash
uv add gometry # or: pip install gometry
```

Optional integrations are available as extras:

```bash
uv add "gometry[arrow]"         # PyArrow / GeoArrow objects
uv add "gometry[pandas]"        # pandas extension storage
uv add "gometry[polars]"        # Polars binary columns
uv add "gometry[geopandas]"     # GeoPandas conversion
uv add "gometry[viz]"           # lonboard exploration
```

With the `arrow` extra installed, `to_arrow` materializes pyarrow objects; without
it, geometries still expose Arrow PyCapsules for dependency-free consumers.

## What it replaces

| Instead of | use |
|---|---|
| Shapely geometry, predicates, overlays, constructive ops | `gm.Point/box/...`, `gm.contains(a, b)`, `geom.buffer(...)` |
| pyproj transforms and CRS introspection | `geom.to_crs(...)`, `gm.CRS(...)`, `gm.crs_*` |
| geographiclib / haversine geodesy | CRS-aware `area`/`length`/`distance`, `gm.bearing(..., path='rhumb')` / `point.destination(..., path='rhumb')` |
| h3-py, s2sphere | `gm.h3_cover(geom, ...)` / `gm.s2_cover(geom, ...)`, typed cell arrays |
| pygeohash, mercantile | `gm.geohash_cover(geom, ...)` / `gm.tile_cover(geom, ...)` |
| rtree | `gm.SpatialIndex(values)` with exact-predicate refinement |
| polyline, openlocationcode | `gm.from_polyline(...)`, `gm.pluscode_*` |

See the [migration guide](https://gometry.monicz.dev/migrating/) for symbol-level
mappings.

## Is gometry right for you?

gometry is a good choice when:

- you work with CRS-aware geometry and want the metric to follow the data
- you want one dependency rather than a stack of six, with no GEOS, GDAL, or
  system PROJ to install
- you process geometry in bulk and want packed arrays instead of Python-object
  loops
- you need H3, S2, geohash, or tile covers alongside ordinary geometry, and
  CRS-aware distance queries from one spatial index

gometry is not the right choice when:

- you need a dataframe or a data-loading layer. GeoParquet and dataframe
  engines are integration boundaries rather than bundled dependencies
- you need spherical boolean operations. S2 support is oriented to cells and
  covers
- you need mixed-CRS collections. A `GeometryArray` requires a shared CRS

## Support

Bug reports, feature requests, and questions go in the
[GitHub issue tracker](https://github.com/Zaczero/pkgs/issues). Include the
gometry version, Python version, platform, and a minimal reproduction.

The [installation guide](https://gometry.monicz.dev/get-started/installation/)
covers PyPI and source installation, and the
[API reference](https://gometry.monicz.dev/api/) is the canonical lookup surface.
