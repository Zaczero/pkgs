## Run environment

Measured on AMD Ryzen 9 5950X 16-Core Processor with Linux 7.2.6 and CPython 3.14.7, at commit `ba304ea2`.
Versions: GeoPandas 1.1.4, h3-py 4.5.0, Mercantile 1.2.1, NumPy 2.5.2, pyarrow 25.0.1, pygeohash 3.3.1, pyproj 3.7.2, Shapely 2.1.2, GEOS 3.13.1, PROJ 9.5.1.

### Array construction & I/O

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Array construction & I/O benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| from_wkb mixed EWKB — 10k mixed EWKB | 2.18 ms | Shapely · 2.56 ms | <span class="benchmark-status benchmark-faster">1.17× faster</span> |
| from_arrow mixed GeoArrow WKB — 100k mixed GeoArrow WKB / 10% missing | 14.54 ms | GeoPandas · 24.30 ms | <span class="benchmark-status benchmark-faster">1.68× faster</span> |
| to_wkb mixed EWKB — 10k mixed EWKB | 654.31 us | Shapely · 2.96 ms | <span class="benchmark-status benchmark-faster">4.52× faster</span> |
| get_coordinates with index — 100k vertices with index | 112.92 us | Shapely · 435.15 us | <span class="benchmark-status benchmark-faster">3.86× faster</span> |
| points from NumPy XY — 10k NumPy XY | 6.69 us | Shapely · 1.07 ms | <span class="benchmark-status benchmark-faster">161.29× faster</span> |
| from WKT mixed geometries — 10k realistic mixed WKT | 5.49 ms | Shapely · 16.67 ms | <span class="benchmark-status benchmark-faster">3.04× faster</span> |
| from GeoJSON countries — 217 countries / 2.26 MB | 15.67 ms | Shapely · 35.45 ms | <span class="benchmark-status benchmark-faster">2.27× faster</span> |
</div>

### Geometry

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Geometry benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| intersects irregular polygon points — 12k interior, boundary, and exterior probes | 4.50 ms | Shapely · 2.70 ms | <span class="benchmark-status benchmark-slower">1.67× slower</span> |
| prepared polygon contains XY — 100k probes / 1,316-coordinate holed polygon | 6.79 ms | Shapely · 8.69 ms | <span class="benchmark-status benchmark-faster">1.28× faster</span> |
| dwithin pairwise — 10k pairs / 50% matches | 5.87 us | Shapely · 256.25 us | <span class="benchmark-status benchmark-faster">44.60× faster</span> |
| area projected buildings — 10k projected buildings | 218.04 us | Shapely · 181.92 us | <span class="benchmark-status benchmark-slower">1.21× slower</span> |
| length roads — 10k roads / 100k vertices | 137.75 us | Shapely · 198.85 us | <span class="benchmark-status benchmark-faster">1.45× faster</span> |
| simplify Douglas-Peucker — 10k roads / 100k vertices | 720.95 us | Shapely · 3.98 ms | <span class="benchmark-status benchmark-faster">5.53× faster</span> |
| buffer projected points — 10k projected points | 771.64 us | Shapely · 49.66 ms | <span class="benchmark-status benchmark-faster">64.19× faster</span> |
| intersection pairwise — 1k irregular polygon pairs | 39.09 ms | Shapely · 7.38 ms | <span class="benchmark-status benchmark-slower">5.32× slower</span> |
| coverage_union parcels — 10k edge-matched parcels | 7.02 ms | Shapely · 11.45 ms | <span class="benchmark-status benchmark-faster">1.63× faster</span> |
| is_valid mixed polygons — 10k mixed polygons / 20% invalid | 5.92 ms | Shapely · 9.39 ms | <span class="benchmark-status benchmark-faster">1.59× faster</span> |
| repair linework — 1k invalid polygons | 36.05 ms | Shapely · 91.51 ms | <span class="benchmark-status benchmark-faster">2.54× faster</span> |
| bounds projected buildings — 10k projected buildings | 138.42 us | Shapely · 163.35 us | <span class="benchmark-status benchmark-faster">1.18× faster</span> |
| clip_by_rect realistic linework — 10k linework / crossing, inside, outside categories | 3.44 ms | Shapely · 2.89 ms | <span class="benchmark-status benchmark-slower">1.19× slower</span> |
| point_on_surface buildings — 10k concave and holed buildings | 766.32 us | Shapely · 2.10 ms | <span class="benchmark-status benchmark-faster">2.74× faster</span> |
</div>

### CRS & geodesy

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="CRS & geodesy benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| to_crs masked points — 200k points / 10% missing ‡ | 6.11 ms | GeoPandas · 112.47 ms | <span class="benchmark-status benchmark-faster">18.28× faster</span> |
| crs_transform WGS84 to Web Mercator — 10k EPSG:4326 to 3857 | 350.24 us | pyproj · 746.20 us | <span class="benchmark-status benchmark-faster">2.13× faster</span> |
| geodesic distance — 10k WGS84 pairs † | 8.06 ms | pyproj · 8.02 ms | <span class="benchmark-status benchmark-similar">Similar</span> |
| geodesic destination — 10k WGS84 destinations † | 2.97 ms | pyproj + Shapely + GeoPandas · 4.16 ms | <span class="benchmark-status benchmark-faster">1.40× faster</span> |
| geodesic country area — 217 countries † | 7.70 ms | pyproj · 34.92 ms | <span class="benchmark-status benchmark-faster">4.53× faster</span> |
| geodesic exterior length — 1,034 exteriors / 16,135 vertices † | 6.81 ms | pyproj · 20.92 ms | <span class="benchmark-status benchmark-faster">3.07× faster</span> |
</div>

### Discrete global grids

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Discrete global grids benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| geohash encode tokens — 10k WGS84 points / precision 6 | 670.99 us | pygeohash · 4.00 ms | <span class="benchmark-status benchmark-faster">5.95× faster</span> |
| h3 cover center Brazil — Brazil res5 / 32,260 cells | 21.08 ms | h3-py · 30.18 ms | <span class="benchmark-status benchmark-faster">1.43× faster</span> |
| h3_compact — 32,260 to 1,732 cells | 825.28 us | h3-py · 540.12 us | <span class="benchmark-status benchmark-slower">1.53× slower</span> |
</div>

### Spatial index

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Spatial index benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| one-shot join within — 10k POIs x 217 countries | 34.81 ms | Shapely · 29.44 ms | <span class="benchmark-status benchmark-slower">1.18× slower</span> |
| index join within — 10k POIs x 217 countries | 17.53 ms | Shapely · 29.15 ms | <span class="benchmark-status benchmark-faster">1.65× faster</span> |
| index candidates — 1k queries x 10k polygons | 3.53 ms | Shapely · 1.77 ms | <span class="benchmark-status benchmark-slower">2.00× slower</span> |
| index nearest k=1 — 1k non-intersecting queries x 10k polygons | 2.45 ms | Shapely · 5.47 ms | <span class="benchmark-status benchmark-faster">2.23× faster</span> |
| index build — 10k polygons | 631.75 us | Shapely · 1.17 ms | <span class="benchmark-status benchmark-faster">1.86× faster</span> |
</div>

† geodesic/ellipsoidal metric.
‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.

### Scaling Curves

#### union_all: n-ary overlay input count

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `64` | `64 input-geometries` | 3.46 ms | Shapely · 1.79 ms | 0.517 | 54.02 us | 27.98 us | <span class="benchmark-status benchmark-slower">1.93× slower</span> |
| `256` | `256 input-geometries` | 14.61 ms | Shapely · 7.20 ms | 0.493 | 57.06 us | 28.14 us | <span class="benchmark-status benchmark-slower">2.03× slower</span> |
| `1024` | `1024 input-geometries` | 69.41 ms | Shapely · 28.03 ms | 0.404 | 67.78 us | 27.37 us | <span class="benchmark-status benchmark-slower">2.47× slower</span> |

Endpoint growth (64 → 1024): gometry median 20.078x, comparator median 15.651x, gometry/work 1.255x, comparator/work 0.978x.

#### tile_cover: output count

Dimension `grid-output-count` in `tiles`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `990` | `990 output-tiles` | 351.38 us | Mercantile · 236.51 us | 0.672 | 354.9 ns | 238.9 ns | <span class="benchmark-status benchmark-slower">1.49× slower</span> |
| `3960` | `3960 output-tiles` | 529.54 us | Mercantile · 1.14 ms | 2.148 | 133.7 ns | 287.5 ns | <span class="benchmark-status benchmark-faster">2.15× faster</span> |
| `15340` | `15340 output-tiles` | 871.34 us | Mercantile · 4.81 ms | 5.533 | 56.8 ns | 313.7 ns | <span class="benchmark-status benchmark-faster">5.53× faster</span> |

Endpoint growth (990 → 15340): gometry median 2.480x, comparator median 20.345x, gometry/work 0.160x, comparator/work 1.313x.

#### index candidates: indexed cardinality

Dimension `indexed-polygons` in `polygons`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1k` | `1000 candidate-pairs` | 107.53 us | Shapely · 73.50 us | 0.682 | 107.5 ns | 73.5 ns | <span class="benchmark-status benchmark-slower">1.47× slower</span> |
| `5k` | `1000 candidate-pairs` | 121.15 us | Shapely · 98.85 us | 0.811 | 121.2 ns | 98.9 ns | <span class="benchmark-status benchmark-slower">1.23× slower</span> |
| `10k` | `1000 candidate-pairs` | 120.80 us | Shapely · 113.79 us | 0.934 | 120.8 ns | 113.8 ns | <span class="benchmark-status benchmark-slower">1.07× slower</span> |

Endpoint growth (1k → 10k): gometry median 1.123x, comparator median 1.548x, gometry/work 1.123x, comparator/work 1.548x.

#### index candidates: query selectivity

Dimension `query-selectivity` in `fraction`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1pct` | `1 candidate-pairs` | 50.20 us | Shapely · 14.60 us | 0.291 | 50.20 us | 14.60 us | <span class="benchmark-status benchmark-slower">3.44× slower</span> |
| `10pct` | `1 candidate-pairs` | 50.26 us | Shapely · 14.52 us | 0.287 | 50.26 us | 14.52 us | <span class="benchmark-status benchmark-slower">3.49× slower</span> |
| `50pct` | `1 candidate-pairs` | 50.28 us | Shapely · 14.52 us | 0.290 | 50.28 us | 14.52 us | <span class="benchmark-status benchmark-slower">3.45× slower</span> |

Endpoint growth (1pct → 50pct): gometry median 1.001x, comparator median 0.995x, gometry/work 1.001x, comparator/work 0.995x.

#### index candidates: result amplification

Dimension `candidate-pairs-per-query` in `pairs/query`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1` | `1 candidate-pairs` | 1.36 us | Shapely · 1.60 us | 1.154 | 1.36 us | 1.60 us | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| `10` | `10 candidate-pairs` | 1.64 us | Shapely · 2.12 us | 1.315 | 163.5 ns | 212.5 ns | <span class="benchmark-status benchmark-faster">1.31× faster</span> |
| `100` | `100 candidate-pairs` | 3.19 us | Shapely · 3.18 us | 0.968 | 31.8 ns | 31.8 ns | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |

Endpoint growth (1 → 100): gometry median 2.342x, comparator median 1.984x, gometry/work 0.023x, comparator/work 0.020x.

#### contains_xy: certified batch boundary

Dimension `probe-count` in `probes`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `below-gate` | `9999 predicate-results` | 678.84 us | Shapely · 881.87 us | 1.299 | 67.9 ns | 88.2 ns | <span class="benchmark-status benchmark-faster">1.30× faster</span> |
| `gate` | `10000 predicate-results` | 679.42 us | Shapely · 814.34 us | 1.198 | 67.9 ns | 81.4 ns | <span class="benchmark-status benchmark-faster">1.20× faster</span> |
| `above-gate` | `10001 predicate-results` | 678.98 us | Shapely · 884.35 us | 1.302 | 67.9 ns | 88.4 ns | <span class="benchmark-status benchmark-faster">1.30× faster</span> |

Endpoint growth (below-gate → above-gate): gometry median 1.000x, comparator median 1.003x, gometry/work 1.000x, comparator/work 1.003x.
