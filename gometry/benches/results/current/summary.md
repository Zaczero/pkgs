> Historical measurement: the coordinate row below measured the former
> `get_coordinates(..., return_index=True)` entry point at commit `b8e785c8`.
> It is retained as evidence for that release and does not measure the current
> `.coords` plus NumPy matrix/row-index path. No replacement timing is claimed.

## Run environment

Measured on AMD Ryzen 9 5950X 16-Core Processor with Linux 7.2.6 and CPython 3.14.7, at commit `b8e785c8`.
Versions: GeoPandas 1.1.4, h3-py 4.5.0, Mercantile 1.2.1, NumPy 2.5.2, pyarrow 25.0.1, pygeohash 3.3.1, pyproj 3.7.2, Shapely 2.1.2, GEOS 3.13.1, PROJ 9.5.1.

### Array construction & I/O

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Array construction & I/O benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| from_wkb mixed EWKB — 10k mixed EWKB | 1.69 ms | Shapely · 2.33 ms | <span class="benchmark-status benchmark-faster">1.38× faster</span> |
| from_arrow mixed GeoArrow WKB — 100k mixed GeoArrow WKB / 10% missing | 11.91 ms | GeoPandas · 24.91 ms | <span class="benchmark-status benchmark-faster">2.07× faster</span> |
| to_wkb mixed EWKB — 10k mixed EWKB | 660.94 us | Shapely · 2.99 ms | <span class="benchmark-status benchmark-faster">4.55× faster</span> |
| Historical `get_coordinates` with index — 100k vertices with index | 117.05 us | Shapely · 448.71 us | <span class="benchmark-status benchmark-faster">3.79× faster</span> |
| points from NumPy XY — 10k NumPy XY | 7.28 us | Shapely · 1.01 ms | <span class="benchmark-status benchmark-faster">140.45× faster</span> |
| from WKT mixed geometries — 10k realistic mixed WKT | 5.03 ms | Shapely · 16.68 ms | <span class="benchmark-status benchmark-faster">3.32× faster</span> |
| from GeoJSON countries — 217 countries / 2.26 MB | 13.41 ms | Shapely · 36.14 ms | <span class="benchmark-status benchmark-faster">2.70× faster</span> |
</div>

### Geometry

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Geometry benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| intersects irregular polygon points — 12k interior, boundary, and exterior probes | 3.04 ms | Shapely · 2.77 ms | <span class="benchmark-status benchmark-slower">1.10× slower</span> |
| prepared polygon contains XY — 100k probes / 1,316-coordinate holed polygon | 4.52 ms | Shapely · 8.23 ms | <span class="benchmark-status benchmark-faster">1.82× faster</span> |
| dwithin pairwise — 10k pairs / 50% matches | 6.09 us | Shapely · 256.02 us | <span class="benchmark-status benchmark-faster">42.35× faster</span> |
| area projected buildings — 10k projected buildings | 221.53 us | Shapely · 190.00 us | <span class="benchmark-status benchmark-slower">1.17× slower</span> |
| length roads — 10k roads / 100k vertices | 142.37 us | Shapely · 202.06 us | <span class="benchmark-status benchmark-faster">1.42× faster</span> |
| simplify Douglas-Peucker — 10k roads / 100k vertices | 820.64 us | Shapely · 4.04 ms | <span class="benchmark-status benchmark-faster">4.91× faster</span> |
| buffer projected points — 10k projected points | 791.92 us | Shapely · 51.00 ms | <span class="benchmark-status benchmark-faster">64.40× faster</span> |
| intersection pairwise — 1k irregular polygon pairs | 10.73 ms | Shapely · 7.28 ms | <span class="benchmark-status benchmark-slower">1.47× slower</span> |
| intersects pairwise polygons — 1k irregular polygon pairs | 580.37 us | Shapely · 570.96 us | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| coverage_union_all parcels — 10k edge-matched parcels | 6.04 ms | Shapely · 10.13 ms | <span class="benchmark-status benchmark-faster">1.68× faster</span> |
| is_valid mixed polygons — 10k mixed polygons / 20% invalid | 6.01 ms | Shapely · 9.45 ms | <span class="benchmark-status benchmark-faster">1.58× faster</span> |
| repair linework — 1k invalid polygons | 17.88 ms | Shapely · 92.93 ms | <span class="benchmark-status benchmark-faster">5.21× faster</span> |
| bounds projected buildings — 10k projected buildings | 142.18 us | Shapely · 164.38 us | <span class="benchmark-status benchmark-faster">1.15× faster</span> |
| clip_by_rect realistic linework — 10k linework / crossing, inside, outside categories | 2.95 ms | Shapely · 2.46 ms | <span class="benchmark-status benchmark-slower">1.20× slower</span> |
| point_on_surface buildings — 10k concave and holed buildings | 760.99 us | Shapely · 2.14 ms | <span class="benchmark-status benchmark-faster">2.80× faster</span> |
</div>

### CRS & geodesy

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="CRS & geodesy benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| to_crs masked points — 200k points / 10% missing ‡ | 6.51 ms | GeoPandas · 63.19 ms | <span class="benchmark-status benchmark-faster">9.75× faster</span> |
| crs_transform WGS84 to Web Mercator — 10k EPSG:4326 to 3857 | 319.69 us | pyproj · 747.75 us | <span class="benchmark-status benchmark-faster">2.34× faster</span> |
| geodesic distance — 10k WGS84 pairs † | 13.33 ms | pyproj · 12.12 ms | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| geodesic destination — 10k WGS84 destinations † | 5.61 ms | pyproj + Shapely + GeoPandas · 8.47 ms | <span class="benchmark-status benchmark-faster">1.51× faster</span> |
| geodesic country area — 217 countries † | 14.16 ms | pyproj · 81.05 ms | <span class="benchmark-status benchmark-faster">5.73× faster</span> |
| geodesic exterior length — 1,034 exteriors / 16,135 vertices † | 6.89 ms | pyproj · 21.00 ms | <span class="benchmark-status benchmark-faster">3.05× faster</span> |
</div>

### Discrete global grids

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Discrete global grids benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| geohash encode tokens — 10k WGS84 points / precision 6 | 654.64 us | pygeohash · 3.98 ms | <span class="benchmark-status benchmark-faster">6.04× faster</span> |
| h3 cover center Brazil — Brazil res5 / 32,260 cells | 21.28 ms | h3-py · 30.34 ms | <span class="benchmark-status benchmark-faster">1.43× faster</span> |
| h3_compact — 32,260 to 1,732 cells | 528.38 us | h3-py · 535.62 us | <span class="benchmark-status benchmark-similar">Similar</span> |
</div>

### Spatial index

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Spatial index benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| one-shot join within — 10k POIs x 217 countries | 32.02 ms | Shapely · 29.07 ms | <span class="benchmark-status benchmark-slower">1.10× slower</span> |
| index join within — 10k POIs x 217 countries | 16.17 ms | Shapely · 29.15 ms | <span class="benchmark-status benchmark-faster">1.81× faster</span> |
| index candidates — 1k queries x 10k polygons | 1.12 ms | Shapely · 1.75 ms | <span class="benchmark-status benchmark-faster">1.55× faster</span> |
| index nearest k=1 — 1k non-intersecting queries x 10k polygons | 2.35 ms | Shapely · 5.55 ms | <span class="benchmark-status benchmark-faster">2.37× faster</span> |
| index build — 10k polygons | 707.42 us | Shapely · 1.18 ms | <span class="benchmark-status benchmark-faster">1.66× faster</span> |
</div>

† geodesic/ellipsoidal metric.
‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.

### Scaling Curves

#### union_all: n-ary overlay input count

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `64` | `64 input-geometries` | 1.12 ms | Shapely · 1.77 ms | 1.575 | 17.55 us | 27.62 us | <span class="benchmark-status benchmark-faster">1.57× faster</span> |
| `256` | `256 input-geometries` | 5.91 ms | Shapely · 8.63 ms | 1.633 | 23.10 us | 33.71 us | <span class="benchmark-status benchmark-faster">1.63× faster</span> |
| `1024` | `1024 input-geometries` | 18.88 ms | Shapely · 28.25 ms | 1.497 | 18.44 us | 27.59 us | <span class="benchmark-status benchmark-faster">1.50× faster</span> |

Endpoint growth (64 → 1024): gometry median 16.813x, comparator median 15.982x, gometry/work 1.051x, comparator/work 0.999x.

#### tile_cover: output count

Dimension `grid-output-count` in `tiles`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `990` | `990 output-tiles` | 21.53 us | Mercantile · 237.82 us | 11.047 | 21.7 ns | 240.2 ns | <span class="benchmark-status benchmark-faster">11.05× faster</span> |
| `3960` | `3960 output-tiles` | 65.29 us | Mercantile · 1.13 ms | 17.504 | 16.5 ns | 286.6 ns | <span class="benchmark-status benchmark-faster">17.50× faster</span> |
| `15340` | `15340 output-tiles` | 233.14 us | Mercantile · 5.02 ms | 21.352 | 15.2 ns | 327.0 ns | <span class="benchmark-status benchmark-faster">21.35× faster</span> |

Endpoint growth (990 → 15340): gometry median 10.831x, comparator median 21.089x, gometry/work 0.699x, comparator/work 1.361x.

#### index candidates: indexed cardinality

Dimension `indexed-polygons` in `polygons`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1k` | `1000 candidate-pairs` | 66.02 us | Shapely · 73.43 us | 1.109 | 66.0 ns | 73.4 ns | <span class="benchmark-status benchmark-faster">1.11× faster</span> |
| `5k` | `1000 candidate-pairs` | 75.66 us | Shapely · 104.20 us | 1.404 | 75.7 ns | 104.2 ns | <span class="benchmark-status benchmark-faster">1.40× faster</span> |
| `10k` | `1000 candidate-pairs` | 84.77 us | Shapely · 144.89 us | 1.663 | 84.8 ns | 144.9 ns | <span class="benchmark-status benchmark-faster">1.66× faster</span> |

Endpoint growth (1k → 10k): gometry median 1.284x, comparator median 1.973x, gometry/work 1.284x, comparator/work 1.973x.

#### index candidates: query selectivity

Dimension `query-selectivity` in `fraction`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1pct` | `10 candidate-pairs` | 20.82 us | Shapely · 15.78 us | 0.756 | 2.08 us | 1.58 us | <span class="benchmark-status benchmark-slower">1.32× slower</span> |
| `10pct` | `100 candidate-pairs` | 25.26 us | Shapely · 24.78 us | 0.981 | 252.6 ns | 247.8 ns | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| `50pct` | `500 candidate-pairs` | 45.94 us | Shapely · 62.31 us | 1.351 | 91.9 ns | 124.6 ns | <span class="benchmark-status benchmark-faster">1.35× faster</span> |

Endpoint growth (1pct → 50pct): gometry median 2.207x, comparator median 3.947x, gometry/work 0.044x, comparator/work 0.079x.

#### index candidates: result amplification

Dimension `candidate-pairs-per-query` in `pairs/query`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1` | `1 candidate-pairs` | 1.95 us | Shapely · 1.54 us | 0.820 | 1.95 us | 1.54 us | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| `10` | `10 candidate-pairs` | 1.71 us | Shapely · 2.23 us | 1.297 | 170.5 ns | 223.0 ns | <span class="benchmark-status benchmark-faster">1.30× faster</span> |
| `100` | `100 candidate-pairs` | 2.25 us | Shapely · 3.46 us | 1.540 | 22.6 ns | 34.6 ns | <span class="benchmark-status benchmark-faster">1.54× faster</span> |

Endpoint growth (1 → 100): gometry median 1.159x, comparator median 2.236x, gometry/work 0.012x, comparator/work 0.022x.

#### union_all: disjoint input count

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1k` | `1000 input-geometries` | 5.38 ms | Shapely · 28.53 ms | 5.306 | 5.38 us | 28.53 us | <span class="benchmark-status benchmark-faster">5.31× faster</span> |
| `4k` | `4000 input-geometries` | 37.46 ms | Shapely · 246.63 ms | 6.775 | 9.36 us | 61.66 us | <span class="benchmark-status benchmark-faster">6.77× faster</span> |
| `16k` | `16000 input-geometries` | 149.95 ms | Shapely · 1361.01 ms | 9.181 | 9.37 us | 85.06 us | <span class="benchmark-status benchmark-faster">9.18× faster</span> |

Endpoint growth (1k → 16k): gometry median 27.889x, comparator median 47.702x, gometry/work 1.743x, comparator/work 2.981x.

#### intersection: polygon hole count

Dimension `holes` in `holes`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `250` | `250 holes` | 684.35 us | Shapely · 771.39 us | 1.127 | 2.74 us | 3.09 us | <span class="benchmark-status benchmark-faster">1.13× faster</span> |
| `1k` | `1000 holes` | 2.74 ms | Shapely · 3.17 ms | 1.152 | 2.74 us | 3.17 us | <span class="benchmark-status benchmark-faster">1.15× faster</span> |
| `4k` | `4000 holes` | 11.36 ms | Shapely · 12.99 ms | 1.142 | 2.84 us | 3.25 us | <span class="benchmark-status benchmark-faster">1.14× faster</span> |

Endpoint growth (250 → 4k): gometry median 16.598x, comparator median 16.836x, gometry/work 1.037x, comparator/work 1.052x.

#### contains coordinate probes: certified batch boundary

Dimension `probe-count` in `probes`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `below-gate` | `9999 predicate-results` | 444.43 us | Shapely · 814.11 us | 1.833 | 44.4 ns | 81.4 ns | <span class="benchmark-status benchmark-faster">1.83× faster</span> |
| `gate` | `10000 predicate-results` | 443.61 us | Shapely · 816.44 us | 1.852 | 44.4 ns | 81.6 ns | <span class="benchmark-status benchmark-faster">1.85× faster</span> |
| `above-gate` | `10001 predicate-results` | 441.56 us | Shapely · 820.98 us | 1.857 | 44.2 ns | 82.1 ns | <span class="benchmark-status benchmark-faster">1.86× faster</span> |

Endpoint growth (below-gate → above-gate): gometry median 0.994x, comparator median 1.008x, gometry/work 0.993x, comparator/work 1.008x.
