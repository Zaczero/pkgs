<p align="center">
  <img src="docs/assets/logo.svg" alt="gometry" width="180">
</p>

<h1 align="center">gometry</h1>

<p align="center">
  <strong>Geospatial engine for Python</strong>, written in Rust.<br>
  One package for geometry, CRS, geodesy, grids, and spatial indexing.
</p>

<p align="center">
  <a href="https://pypi.org/project/gometry/"><img src="https://shields.monicz.dev/pypi/pyversions/gometry" alt="PyPI - Python Version"></a>
  <a href="https://liberapay.com/Zaczero/"><img src="https://shields.monicz.dev/liberapay/patrons/Zaczero?logo=liberapay&amp;label=Patrons" alt="Liberapay Patrons"></a>
  <a href="https://github.com/sponsors/Zaczero"><img src="https://shields.monicz.dev/github/sponsors/Zaczero?logo=github&amp;label=Sponsors&amp;color=%23db61a2" alt="GitHub Sponsors"></a>
</p>

<p align="center">
  <a href="https://gometry.monicz.dev/">Documentation</a> ·
  <a href="https://gometry.monicz.dev/get-started/quickstart/">Quickstart</a> ·
  <a href="https://gometry.monicz.dev/guide/">User guide</a> ·
  <a href="https://gometry.monicz.dev/api/">API reference</a> ·
  <a href="https://gometry.monicz.dev/migrating/">Migrating</a>
</p>

---

- **One package for a specialist stack** — geometry, CRS transforms, H3/S2,
  geohash/tile keys, and spatial indexing, with no GEOS, no GDAL, and no system
  PROJ
- **The CRS decides the measure** — ellipsoidal chart-edge measures and
  point-to-point geodesics on a geographic CRS, native linear units on a
  projected one, raw coordinate units without one. See the
  [CRS, units & measurement guide](https://gometry.monicz.dev/guide/crs/)
- **Vectorized in Rust** — predicates, overlays, buffering, geodesics, and
  coverage run as batched kernels over packed buffers and return NumPy arrays.
  The same names work on a `Geometry` or on a Rust-owned `GeometryArray`
- **Exchange data** — [GeoArrow](https://geoarrow.org/)
  packed arrays for homogeneous XY/XYZ/XYM/XYZM geometries, WKB fallback for
  mixed types, WKT/WKB/EWKB/GeoJSON codecs, and
  [Mapbox Vector Tiles](https://gometry.monicz.dev/ecosystem/vector-tiles/)
- **Keep features together** — immutable
  [feature snapshots](https://gometry.monicz.dev/guide/features/) keep
  geometries, properties, and IDs aligned
- **Grids and indexes on one geometry type** — H3, S2, geohash, and XYZ tiles
  share a typed `CellArray`; spatial indexes keep bounding-box `candidates`
  separate from exact `query`
- **Typed** — stubs, narrowed signatures, public cross-grid protocols, and a
  structured exception hierarchy
- **Apache-2.0 OR MIT** · [Python compatibility](https://gometry.monicz.dev/about/compatibility/)

## Install

```bash
uv add gometry # or: pip install gometry
```

On x86_64, wheels require an x86-64-v2 CPU (roughly 2009 or newer); arm64
wheels target native arm64. The [installation guide](https://gometry.monicz.dev/get-started/installation/)
covers requirements and extras for Arrow, pandas, Polars, GeoPandas, and notebook maps.

## gometry in action

One `area` call, two answers: the geometry carries `crs=4326`, so it measures
ellipsoidal **square metres** along its straight chart edges, and
`unit='planar'` drops to raw coordinate units.

```python
import gometry as gm

city = gm.box(2.0, 48.0, 3.0, 49.0, crs=4326)
station = gm.Point(2.35, 48.85, crs=4326)
contains_station = gm.contains(city, station)
area_km2 = f'{city.area / 1_000_000.0:.1f} km²'
planar_area = f'{gm.area(city, unit="planar"):.4f} degree²'
print(contains_station)  # True
print(area_km2)  # 8217.7 km²
print(planar_area)  # 1.0000 degree²
```

Arrays return NumPy ndarrays instead of Python-object lists:

```python
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
points = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
mask = gm.contains(area, points)
print(type(mask).__name__, mask.tolist())  # ndarray [True, False]
```

## Benchmarks

For 1,000 bounding-box candidate queries, gometry is slower than Shapely with
1,000 indexed polygons and faster with 10,000; the comparison at 5,000 is
inconclusive.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/benchmark-spatial-index-cardinality-dark.svg">
  <img src="docs/assets/benchmark-spatial-index-cardinality-light.svg" alt="Spatial-index query times for 1,000 bounding-box candidate queries: Gometry is slower than Shapely at 1,000 indexed polygons, inconclusive at 5,000, and faster at 10,000.">
</picture>

These are candidate queries, before exact predicate refinement.
[See all workloads, measurements, and methodology](https://gometry.monicz.dev/about/benchmarks/).

## What it replaces

| Instead of | use |
|---|---|
| Shapely geometry, predicates, overlays, constructive ops | `gm.Point/box/...`, `gm.contains(a, b)`, `geom.buffer(...)` |
| pyproj transforms and CRS introspection | `geom.to_crs(...)`, `gm.CRS(...)`, `gm.crs_*` |
| geographiclib / haversine geodesy | CRS-aware ellipsoidal chart-edge `area`/`length`, geodesic point-to-point `distance`, `gm.bearing(..., path='rhumb')` / `point.destination(..., path='rhumb')` |
| h3-py, s2sphere | `gm.h3_cover(geom, ...)`, fixed `gm.s2_cover(geom, level=...)` or adaptive `gm.s2_cover(geom, target_cells=...)`, typed cell arrays |
| pygeohash, mercantile | `gm.geohash_cover(geom, ...)` / `gm.tile_cover(geom, ...)` |
| rtree | `gm.SpatialIndex(values)` with exact-predicate refinement |
| polyline, openlocationcode | `gm.from_polyline(...)`, `gm.pluscode_*` |

See the [migration guide](https://gometry.monicz.dev/migrating/) for symbol-level
mappings.

## Is gometry right for you?

Use gometry for geometry computation; keep your dataframe or database for
storage and queries. A `GeometryArray` shares one CRS. S2 provides cells and
covers, not spherical boolean operations.

## Support

Bug reports, feature requests, and questions go in the
[GitHub issue tracker](https://github.com/Zaczero/pkgs/issues).

For correctness review, migration help, performance audits, or prioritized
work, commercial support is available through
[monicz.dev](https://monicz.dev). See the
[Support page](https://gometry.monicz.dev/about/support/) for details.

Security disclosures use GitHub's
[private vulnerability reporting](https://github.com/Zaczero/pkgs/security/advisories/new).
