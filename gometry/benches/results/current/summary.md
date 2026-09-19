## Run environment

Measured on AMD Ryzen 9 5950X 16-Core Processor with Linux 7.2.6 and CPython 3.14.7, at commit `4f2f9c5e`.
Versions: GeoPandas 1.1.4, h3-py 4.5.0, Mercantile 1.2.1, NumPy 2.5.2, pyarrow 25.0.1, pygeohash 3.3.1, pyproj 3.7.2, Shapely 2.1.2, GEOS 3.13.1, PROJ 9.5.1.

### Array construction & I/O

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Array construction & I/O benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| from_wkb mixed EWKB — 10k mixed EWKB | 2.19 ms | Shapely · 2.84 ms | <span class="benchmark-status benchmark-faster">1.31× faster</span> |
| from_arrow mixed GeoArrow WKB — 100k mixed GeoArrow WKB / 10% missing | 13.69 ms | GeoPandas · 29.66 ms | <span class="benchmark-status benchmark-faster">2.16× faster</span> |
| to_wkb mixed EWKB — 10k mixed EWKB | 812.71 us | Shapely · 3.47 ms | <span class="benchmark-status benchmark-faster">4.30× faster</span> |
| get_coordinates with index — 100k vertices with index | 155.00 us | Shapely · 513.24 us | <span class="benchmark-status benchmark-faster">3.43× faster</span> |
| points from NumPy XY — 10k NumPy XY | 8.96 us | Shapely · 1.15 ms | <span class="benchmark-status benchmark-faster">128.69× faster</span> |
| from WKT mixed geometries — 10k realistic mixed WKT | 6.31 ms | Shapely · 22.13 ms | <span class="benchmark-status benchmark-faster">3.44× faster</span> |
| from GeoJSON countries — 217 countries / 2.26 MB | 17.92 ms | Shapely · 49.77 ms | <span class="benchmark-status benchmark-faster">2.74× faster</span> |
</div>

### Geometry

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Geometry benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| intersects irregular polygon points — 12k interior, boundary, and exterior probes | 3.43 ms | Shapely · 3.01 ms | <span class="benchmark-status benchmark-slower">1.14× slower</span> |
| prepared polygon contains XY — 100k probes / 1,316-coordinate holed polygon | 4.91 ms | Shapely · 9.63 ms | <span class="benchmark-status benchmark-faster">1.93× faster</span> |
| dwithin pairwise — 10k pairs / 50% matches | 7.12 us | Shapely · 293.65 us | <span class="benchmark-status benchmark-faster">42.61× faster</span> |
| area projected buildings — 10k projected buildings | 256.55 us | Shapely · 220.03 us | <span class="benchmark-status benchmark-slower">1.16× slower</span> |
| length roads — 10k roads / 100k vertices | 157.05 us | Shapely · 223.55 us | <span class="benchmark-status benchmark-faster">1.42× faster</span> |
| simplify Douglas-Peucker — 10k roads / 100k vertices | 851.69 us | Shapely · 5.25 ms | <span class="benchmark-status benchmark-faster">5.80× faster</span> |
| buffer projected points — 10k projected points | 945.35 us | Shapely · 62.05 ms | <span class="benchmark-status benchmark-faster">65.58× faster</span> |
| intersection pairwise — 1k irregular polygon pairs | 12.85 ms | Shapely · 8.41 ms | <span class="benchmark-status benchmark-slower">1.54× slower</span> |
| coverage_union parcels — 10k edge-matched parcels | 7.59 ms | Shapely · 11.75 ms | <span class="benchmark-status benchmark-faster">1.56× faster</span> |
| is_valid mixed polygons — 10k mixed polygons / 20% invalid | 7.55 ms | Shapely · 11.80 ms | <span class="benchmark-status benchmark-faster">1.46× faster</span> |
| repair linework — 1k invalid polygons | 31.47 ms | Shapely · 173.15 ms | <span class="benchmark-status benchmark-faster">4.94× faster</span> |
| bounds projected buildings — 10k projected buildings | 156.27 us | Shapely · 184.83 us | <span class="benchmark-status benchmark-faster">1.18× faster</span> |
| clip_by_rect realistic linework — 10k linework / crossing, inside, outside categories | 4.32 ms | Shapely · 3.49 ms | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| point_on_surface buildings — 10k concave and holed buildings | 732.12 us | Shapely · 2.64 ms | <span class="benchmark-status benchmark-faster">3.57× faster</span> |
</div>

### CRS & geodesy

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="CRS & geodesy benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| to_crs masked points — 200k points / 10% missing ‡ | 7.50 ms | GeoPandas · 104.95 ms | <span class="benchmark-status benchmark-faster">12.87× faster</span> |
| crs_transform WGS84 to Web Mercator — 10k EPSG:4326 to 3857 | 360.75 us | pyproj · 844.68 us | <span class="benchmark-status benchmark-faster">2.32× faster</span> |
| geodesic distance — 10k WGS84 pairs † | 13.97 ms | pyproj · 13.52 ms | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| geodesic destination — 10k WGS84 destinations † | 5.83 ms | pyproj + Shapely + GeoPandas · 8.17 ms | <span class="benchmark-status benchmark-faster">1.40× faster</span> |
| geodesic country area — 217 countries † | 14.63 ms | pyproj · 80.65 ms | <span class="benchmark-status benchmark-faster">5.53× faster</span> |
| geodesic exterior length — 1,034 exteriors / 16,135 vertices † | 12.43 ms | pyproj · 48.91 ms | <span class="benchmark-status benchmark-faster">3.97× faster</span> |
</div>

### Discrete global grids

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Discrete global grids benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| geohash encode tokens — 10k WGS84 points / precision 6 | 1.21 ms | pygeohash · 8.25 ms | <span class="benchmark-status benchmark-faster">6.79× faster</span> |
| h3 cover center Brazil — Brazil res5 / 32,260 cells | 50.01 ms | h3-py · 70.05 ms | <span class="benchmark-status benchmark-faster">1.41× faster</span> |
| h3_compact — 32,260 to 1,732 cells | 1.19 ms | h3-py · 1.29 ms | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
</div>

### Spatial index

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Spatial index benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| one-shot join within — 10k POIs x 217 countries | 69.39 ms | Shapely · 68.41 ms | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| index join within — 10k POIs x 217 countries | 33.67 ms | Shapely · 66.11 ms | <span class="benchmark-status benchmark-faster">1.94× faster</span> |
| index candidates — 1k queries x 10k polygons | 2.49 ms | Shapely · 3.74 ms | <span class="benchmark-status benchmark-faster">1.49× faster</span> |
| index nearest k=1 — 1k non-intersecting queries x 10k polygons | 4.85 ms | Shapely · 14.49 ms | <span class="benchmark-status benchmark-faster">3.03× faster</span> |
| index build — 10k polygons | 1.18 ms | Shapely · 2.67 ms | <span class="benchmark-status benchmark-faster">2.24× faster</span> |
</div>

† geodesic/ellipsoidal metric.
‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.

### Scaling Curves

#### union_all: n-ary overlay input count

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `64` | `64 input-geometries` | 2.38 ms | Shapely · 3.73 ms | 1.468 | 37.20 us | 58.21 us | <span class="benchmark-status benchmark-faster">1.47× faster</span> |
| `256` | `256 input-geometries` | 9.13 ms | Shapely · 15.11 ms | 1.705 | 35.67 us | 59.01 us | <span class="benchmark-status benchmark-faster">1.70× faster</span> |
| `1024` | `1024 input-geometries` | 23.04 ms | Shapely · 31.97 ms | 1.412 | 22.50 us | 31.22 us | <span class="benchmark-status benchmark-faster">1.41× faster</span> |

Endpoint growth (64 → 1024): gometry median 9.675x, comparator median 8.580x, gometry/work 0.605x, comparator/work 0.536x.

#### tile_cover: output count

Dimension `grid-output-count` in `tiles`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `990` | `990 output-tiles` | 22.38 us | Mercantile · 238.86 us | 10.926 | 22.6 ns | 241.3 ns | <span class="benchmark-status benchmark-faster">10.93× faster</span> |
| `3960` | `3960 output-tiles` | 90.24 us | Mercantile · 1.56 ms | 18.834 | 22.8 ns | 394.2 ns | <span class="benchmark-status benchmark-faster">18.83× faster</span> |
| `15340` | `15340 output-tiles` | 465.47 us | Mercantile · 11.28 ms | 25.110 | 30.3 ns | 735.3 ns | <span class="benchmark-status benchmark-faster">25.11× faster</span> |

Endpoint growth (990 → 15340): gometry median 20.803x, comparator median 47.225x, gometry/work 1.343x, comparator/work 3.048x.

#### index candidates: indexed cardinality

Dimension `indexed-polygons` in `polygons`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1k` | `1000 candidate-pairs` | 131.22 us | Shapely · 168.21 us | 1.287 | 131.2 ns | 168.2 ns | <span class="benchmark-status benchmark-faster">1.29× faster</span> |
| `5k` | `1000 candidate-pairs` | 155.49 us | Shapely · 229.89 us | 1.519 | 155.5 ns | 229.9 ns | <span class="benchmark-status benchmark-faster">1.52× faster</span> |
| `10k` | `1000 candidate-pairs` | 149.23 us | Shapely · 273.15 us | 1.721 | 149.2 ns | 273.2 ns | <span class="benchmark-status benchmark-faster">1.72× faster</span> |

Endpoint growth (1k → 10k): gometry median 1.137x, comparator median 1.624x, gometry/work 1.137x, comparator/work 1.624x.

#### index candidates: query selectivity

Dimension `query-selectivity` in `fraction`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1pct` | `10 candidate-pairs` | 43.06 us | Shapely · 35.09 us | 0.817 | 4.31 us | 3.51 us | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| `10pct` | `100 candidate-pairs` | 54.54 us | Shapely · 59.50 us | 1.136 | 545.4 ns | 595.0 ns | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| `50pct` | `500 candidate-pairs` | 96.21 us | Shapely · 149.25 us | 1.570 | 192.4 ns | 298.5 ns | <span class="benchmark-status benchmark-faster">1.57× faster</span> |

Endpoint growth (1pct → 50pct): gometry median 2.234x, comparator median 4.254x, gometry/work 0.045x, comparator/work 0.085x.

#### index candidates: result amplification

Dimension `candidate-pairs-per-query` in `pairs/query`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1` | `1 candidate-pairs` | 4.50 us | Shapely · 2.81 us | 0.605 | 4.50 us | 2.81 us | <span class="benchmark-status benchmark-slower">1.65× slower</span> |
| `10` | `10 candidate-pairs` | 5.94 us | Shapely · 5.45 us | 0.919 | 594.5 ns | 545.0 ns | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| `100` | `100 candidate-pairs` | 5.93 us | Shapely · 8.48 us | 1.296 | 59.3 ns | 84.8 ns | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |

Endpoint growth (1 → 100): gometry median 1.318x, comparator median 3.018x, gometry/work 0.013x, comparator/work 0.030x.

#### union_all: disjoint input count

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1k` | `1000 input-geometries` | 7.84 ms | Shapely · 43.37 ms | 5.252 | 7.84 us | 43.37 us | <span class="benchmark-status benchmark-faster">5.25× faster</span> |
| `4k` | `4000 input-geometries` | 22.58 ms | Shapely · 147.78 ms | 6.587 | 5.64 us | 36.95 us | <span class="benchmark-status benchmark-faster">6.59× faster</span> |
| `16k` | `16000 input-geometries` | 91.04 ms | Shapely · 777.54 ms | 8.272 | 5.69 us | 48.60 us | <span class="benchmark-status benchmark-faster">8.27× faster</span> |

Endpoint growth (1k → 16k): gometry median 11.617x, comparator median 17.926x, gometry/work 0.726x, comparator/work 1.120x.

#### intersection: polygon hole count

Dimension `holes` in `holes`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `250` | `250 holes` | 686.61 us | Shapely · 772.57 us | 1.126 | 2.75 us | 3.09 us | <span class="benchmark-status benchmark-faster">1.13× faster</span> |
| `1k` | `1000 holes` | 2.75 ms | Shapely · 3.15 ms | 1.147 | 2.75 us | 3.15 us | <span class="benchmark-status benchmark-faster">1.15× faster</span> |
| `4k` | `4000 holes` | 11.32 ms | Shapely · 13.01 ms | 1.145 | 2.83 us | 3.25 us | <span class="benchmark-status benchmark-faster">1.15× faster</span> |

Endpoint growth (250 → 4k): gometry median 16.494x, comparator median 16.843x, gometry/work 1.031x, comparator/work 1.053x.

#### contains_xy: certified batch boundary

Dimension `probe-count` in `probes`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `below-gate` | `9999 predicate-results` | 481.21 us | Shapely · 985.81 us | 2.067 | 48.1 ns | 98.6 ns | <span class="benchmark-status benchmark-faster">2.07× faster</span> |
| `gate` | `10000 predicate-results` | 728.01 us | Shapely · 1.64 ms | 2.269 | 72.8 ns | 164.5 ns | <span class="benchmark-status benchmark-faster">2.27× faster</span> |
| `above-gate` | `10001 predicate-results` | 432.19 us | Shapely · 892.51 us | 2.065 | 43.2 ns | 89.2 ns | <span class="benchmark-status benchmark-faster">2.07× faster</span> |

Endpoint growth (below-gate → above-gate): gometry median 0.898x, comparator median 0.905x, gometry/work 0.898x, comparator/work 0.905x.
