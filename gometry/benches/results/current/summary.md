## Run environment

Measured on AMD Ryzen 9 5950X 16-Core Processor with Linux 7.2.6 and CPython 3.14.7, at commit `322173ff`.
Versions: GeoPandas 1.1.4, h3-py 4.5.0, Mercantile 1.2.1, NumPy 2.5.2, pyarrow 25.0.1, pygeohash 3.3.1, pyproj 3.7.2, Shapely 2.1.2, GEOS 3.13.1, PROJ 9.5.1.

### Array construction & I/O

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Array construction & I/O benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| from_wkb mixed EWKB — 10k mixed EWKB | 2.60 ms | Shapely · 3.27 ms | <span class="benchmark-status benchmark-faster">1.41× faster</span> |
| from_arrow mixed GeoArrow WKB — 100k mixed GeoArrow WKB / 10% missing | 19.40 ms | GeoPandas · 46.18 ms | <span class="benchmark-status benchmark-faster">2.37× faster</span> |
| to_wkb mixed EWKB — 10k mixed EWKB | 1.55 ms | Shapely · 5.29 ms | <span class="benchmark-status benchmark-faster">3.80× faster</span> |
| get_coordinates with index — 100k vertices with index | 233.44 us | Shapely · 983.78 us | <span class="benchmark-status benchmark-faster">3.52× faster</span> |
| points from NumPy XY — 10k NumPy XY | 13.06 us | Shapely · 2.41 ms | <span class="benchmark-status benchmark-faster">156.35× faster</span> |
| from WKT mixed geometries — 10k realistic mixed WKT | 7.81 ms | Shapely · 38.02 ms | <span class="benchmark-status benchmark-faster">3.73× faster</span> |
| from GeoJSON countries — 217 countries / 2.26 MB | 17.41 ms | Shapely · 47.80 ms | <span class="benchmark-status benchmark-faster">2.72× faster</span> |
</div>

### Geometry

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Geometry benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| intersects irregular polygon points — 12k interior, boundary, and exterior probes | 3.62 ms | Shapely · 3.10 ms | <span class="benchmark-status benchmark-slower">1.18× slower</span> |
| prepared polygon contains XY — 100k probes / 1,316-coordinate holed polygon | 6.64 ms | Shapely · 11.89 ms | <span class="benchmark-status benchmark-faster">1.98× faster</span> |
| dwithin pairwise — 10k pairs / 50% matches | 7.67 us | Shapely · 343.58 us | <span class="benchmark-status benchmark-faster">44.44× faster</span> |
| area projected buildings — 10k projected buildings | 264.71 us | Shapely · 231.99 us | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| length roads — 10k roads / 100k vertices | 156.31 us | Shapely · 222.08 us | <span class="benchmark-status benchmark-faster">1.42× faster</span> |
| simplify Douglas-Peucker — 10k roads / 100k vertices | 869.90 us | Shapely · 5.90 ms | <span class="benchmark-status benchmark-faster">6.33× faster</span> |
| buffer projected points — 10k projected points | 960.77 us | Shapely · 61.76 ms | <span class="benchmark-status benchmark-faster">65.89× faster</span> |
| intersection pairwise — 1k irregular polygon pairs | 25.83 ms | Shapely · 8.51 ms | <span class="benchmark-status benchmark-slower">3.01× slower</span> |
| coverage_union parcels — 10k edge-matched parcels | 8.27 ms | Shapely · 12.25 ms | <span class="benchmark-status benchmark-faster">1.51× faster</span> |
| is_valid mixed polygons — 10k mixed polygons / 20% invalid | 6.91 ms | Shapely · 10.96 ms | <span class="benchmark-status benchmark-faster">1.59× faster</span> |
| repair linework — 1k invalid polygons | 27.04 ms | Shapely · 109.62 ms | <span class="benchmark-status benchmark-faster">4.00× faster</span> |
| bounds projected buildings — 10k projected buildings | 154.40 us | Shapely · 191.11 us | <span class="benchmark-status benchmark-faster">1.25× faster</span> |
| clip_by_rect realistic linework — 10k linework / crossing, inside, outside categories | 3.51 ms | Shapely · 2.90 ms | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| point_on_surface buildings — 10k concave and holed buildings | 910.44 us | Shapely · 2.72 ms | <span class="benchmark-status benchmark-faster">3.01× faster</span> |
</div>

### CRS & geodesy

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="CRS & geodesy benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| to_crs masked points — 200k points / 10% missing ‡ | 7.50 ms | GeoPandas · 80.93 ms | <span class="benchmark-status benchmark-faster">10.87× faster</span> |
| crs_transform WGS84 to Web Mercator — 10k EPSG:4326 to 3857 | 588.02 us | pyproj · 1.04 ms | <span class="benchmark-status benchmark-faster">1.90× faster</span> |
| geodesic distance — 10k WGS84 pairs † | 12.86 ms | pyproj · 12.05 ms | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| geodesic destination — 10k WGS84 destinations † | 3.46 ms | pyproj + Shapely + GeoPandas · 4.84 ms | <span class="benchmark-status benchmark-faster">1.40× faster</span> |
| geodesic country area — 217 countries † | 14.69 ms | pyproj · 86.73 ms | <span class="benchmark-status benchmark-faster">5.91× faster</span> |
| geodesic exterior length — 1,034 exteriors / 16,135 vertices † | 12.30 ms | pyproj · 48.36 ms | <span class="benchmark-status benchmark-faster">3.89× faster</span> |
</div>

### Discrete global grids

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Discrete global grids benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| geohash encode tokens — 10k WGS84 points / precision 6 | 1.16 ms | pygeohash · 8.22 ms | <span class="benchmark-status benchmark-faster">6.90× faster</span> |
| h3 cover center Brazil — Brazil res5 / 32,260 cells | 49.98 ms | h3-py · 69.26 ms | <span class="benchmark-status benchmark-faster">1.40× faster</span> |
| h3_compact — 32,260 to 1,732 cells | 1.18 ms | h3-py · 1.27 ms | <span class="benchmark-status benchmark-faster">1.07× faster</span> |
</div>

### Spatial index

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Spatial index benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| one-shot join within — 10k POIs x 217 countries | 66.86 ms | Shapely · 65.74 ms | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| index join within — 10k POIs x 217 countries | 33.38 ms | Shapely · 64.99 ms | <span class="benchmark-status benchmark-faster">1.96× faster</span> |
| index candidates — 1k queries x 10k polygons | 2.71 ms | Shapely · 3.73 ms | <span class="benchmark-status benchmark-faster">1.37× faster</span> |
| index nearest k=1 — 1k non-intersecting queries x 10k polygons | 5.17 ms | Shapely · 13.34 ms | <span class="benchmark-status benchmark-faster">2.61× faster</span> |
| index build — 10k polygons | 1.18 ms | Shapely · 2.82 ms | <span class="benchmark-status benchmark-faster">2.47× faster</span> |
</div>

† geodesic/ellipsoidal metric.
‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.

### Scaling Curves

#### union_all: n-ary overlay input count

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `64` | `64 input-geometries` | 4.41 ms | Shapely · 3.41 ms | 0.761 | 68.87 us | 53.25 us | <span class="benchmark-status benchmark-slower">1.31× slower</span> |
| `256` | `256 input-geometries` | 22.11 ms | Shapely · 14.42 ms | 0.651 | 86.38 us | 56.32 us | <span class="benchmark-status benchmark-slower">1.54× slower</span> |
| `1024` | `1024 input-geometries` | 51.40 ms | Shapely · 32.52 ms | 0.632 | 50.19 us | 31.76 us | <span class="benchmark-status benchmark-slower">1.58× slower</span> |

Endpoint growth (64 → 1024): gometry median 11.661x, comparator median 9.542x, gometry/work 0.729x, comparator/work 0.596x.

#### tile_cover: output count

Dimension `grid-output-count` in `tiles`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `990` | `990 output-tiles` | 42.57 us | Mercantile · 526.74 us | 12.280 | 43.0 ns | 532.1 ns | <span class="benchmark-status benchmark-faster">12.28× faster</span> |
| `3960` | `3960 output-tiles` | 131.59 us | Mercantile · 2.64 ms | 20.306 | 33.2 ns | 666.1 ns | <span class="benchmark-status benchmark-faster">20.31× faster</span> |
| `15340` | `15340 output-tiles` | 486.75 us | Mercantile · 11.30 ms | 23.216 | 31.7 ns | 736.6 ns | <span class="benchmark-status benchmark-faster">23.22× faster</span> |

Endpoint growth (990 → 15340): gometry median 11.434x, comparator median 21.450x, gometry/work 0.738x, comparator/work 1.384x.

#### index candidates: indexed cardinality

Dimension `indexed-polygons` in `polygons`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1k` | `1000 candidate-pairs` | 138.08 us | Shapely · 165.96 us | 1.190 | 138.1 ns | 166.0 ns | <span class="benchmark-status benchmark-faster">1.19× faster</span> |
| `5k` | `1000 candidate-pairs` | 156.22 us | Shapely · 234.08 us | 1.485 | 156.2 ns | 234.1 ns | <span class="benchmark-status benchmark-faster">1.49× faster</span> |
| `10k` | `1000 candidate-pairs` | 161.72 us | Shapely · 263.47 us | 1.646 | 161.7 ns | 263.5 ns | <span class="benchmark-status benchmark-faster">1.65× faster</span> |

Endpoint growth (1k → 10k): gometry median 1.171x, comparator median 1.588x, gometry/work 1.171x, comparator/work 1.588x.

#### index candidates: query selectivity

Dimension `query-selectivity` in `fraction`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1pct` | `10 candidate-pairs` | 50.13 us | Shapely · 34.60 us | 0.683 | 5.01 us | 3.46 us | <span class="benchmark-status benchmark-slower">1.46× slower</span> |
| `10pct` | `100 candidate-pairs` | 56.99 us | Shapely · 58.34 us | 1.013 | 569.9 ns | 583.4 ns | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| `50pct` | `500 candidate-pairs` | 106.44 us | Shapely · 152.92 us | 1.478 | 212.9 ns | 305.8 ns | <span class="benchmark-status benchmark-faster">1.48× faster</span> |

Endpoint growth (1pct → 50pct): gometry median 2.123x, comparator median 4.419x, gometry/work 0.042x, comparator/work 0.088x.

#### index candidates: result amplification

Dimension `candidate-pairs-per-query` in `pairs/query`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1` | `1 candidate-pairs` | 3.69 us | Shapely · 2.84 us | 0.787 | 3.69 us | 2.84 us | <span class="benchmark-status benchmark-slower">1.27× slower</span> |
| `10` | `10 candidate-pairs` | 3.60 us | Shapely · 4.16 us | 1.193 | 360.0 ns | 416.1 ns | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| `100` | `100 candidate-pairs` | 5.02 us | Shapely · 6.41 us | 1.178 | 50.2 ns | 64.1 ns | <span class="benchmark-status benchmark-faster">1.18× faster</span> |

Endpoint growth (1 → 100): gometry median 1.361x, comparator median 2.251x, gometry/work 0.014x, comparator/work 0.023x.

#### contains_xy: certified batch boundary

Dimension `probe-count` in `probes`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `below-gate` | `9999 predicate-results` | 817.27 us | Shapely · 1.67 ms | 2.039 | 81.7 ns | 167.1 ns | <span class="benchmark-status benchmark-faster">2.04× faster</span> |
| `gate` | `10000 predicate-results` | 783.70 us | Shapely · 1.66 ms | 2.109 | 78.4 ns | 165.7 ns | <span class="benchmark-status benchmark-faster">2.11× faster</span> |
| `above-gate` | `10001 predicate-results` | 780.91 us | Shapely · 1.56 ms | 2.015 | 78.1 ns | 156.0 ns | <span class="benchmark-status benchmark-faster">2.01× faster</span> |

Endpoint growth (below-gate → above-gate): gometry median 0.956x, comparator median 0.934x, gometry/work 0.955x, comparator/work 0.934x.
