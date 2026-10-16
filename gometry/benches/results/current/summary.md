## Run environment

Measured on AMD Ryzen 9 5950X 16-Core Processor with Linux 7.2.9 and CPython 3.14.8. Benchmark harness commit: `e589bb57`.
Gometry producer source commit: `46e953cb` (bound to the loaded wheel by its receipt).

Versions: GeoPandas 1.1.4, h3-py 4.5.0, Mercantile 1.2.1, NumPy 2.5.2, pyarrow 25.0.1, pygeohash 3.3.1, pyproj 3.7.2, Shapely 2.1.2, GEOS 3.13.1, PROJ 9.5.1.

### Array construction & I/O

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Array construction & I/O benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| from_wkb mixed EWKB — 10k mixed EWKB | 1.47 ms | Shapely · 2.36 ms | <span class="benchmark-status benchmark-faster">1.63× faster</span> |
| from_arrow mixed GeoArrow WKB — 100k mixed GeoArrow WKB / 10% missing | 10.59 ms | GeoPandas · 25.94 ms | <span class="benchmark-status benchmark-faster">2.45× faster</span> |
| to_wkb mixed EWKB — 10k mixed EWKB | 710.67 us | Shapely · 2.94 ms | <span class="benchmark-status benchmark-faster">4.14× faster</span> |
| coords matrix and row_index — 100k vertices / float64 matrix + int64 row index | 214.70 us | Shapely · 429.71 us | <span class="benchmark-status benchmark-faster">2.00× faster</span> |
| points from NumPy XY — 10k NumPy XY | 7.90 us | Shapely · 1.05 ms | <span class="benchmark-status benchmark-faster">134.51× faster</span> |
| from WKT mixed geometries — 10k realistic mixed WKT | 4.16 ms | Shapely · 16.86 ms | <span class="benchmark-status benchmark-faster">4.04× faster</span> |
| from GeoJSON countries — 217 countries / 2.26 MB | 16.44 ms | Shapely · 36.28 ms | <span class="benchmark-status benchmark-faster">2.19× faster</span> |
</div>

### Geometry

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Geometry benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| intersects irregular polygon points — 12k interior, boundary, and exterior probes | 1.36 ms | Shapely · 2.74 ms | <span class="benchmark-status benchmark-faster">2.02× faster</span> |
| prepared polygon contains coordinate probes — 100k probes / 1,316-coordinate holed polygon | 4.10 ms | Shapely · 8.45 ms | <span class="benchmark-status benchmark-faster">2.06× faster</span> |
| dwithin pairwise — 10k pairs / 50% matches | 5.79 us | Shapely · 265.72 us | <span class="benchmark-status benchmark-faster">46.19× faster</span> |
| area projected buildings — 10k projected buildings | 167.55 us | Shapely · 185.45 us | <span class="benchmark-status benchmark-faster">1.11× faster</span> |
| length roads — 10k roads / 100k vertices | 132.97 us | Shapely · 200.70 us | <span class="benchmark-status benchmark-faster">1.51× faster</span> |
| simplify Douglas-Peucker — 10k roads / 100k vertices | 772.00 us | Shapely · 4.10 ms | <span class="benchmark-status benchmark-faster">5.32× faster</span> |
| buffer projected points — 10k projected points | 765.95 us | Shapely · 51.88 ms | <span class="benchmark-status benchmark-faster">67.73× faster</span> |
| intersection pairwise — 1k irregular polygon pairs | 6.92 ms | Shapely · 7.25 ms | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| intersects pairwise polygons — 1k irregular polygon pairs | 396.87 us | Shapely · 589.20 us | <span class="benchmark-status benchmark-faster">1.47× faster</span> |
| coverage_union_all parcels — 10k edge-matched parcels | 4.33 ms | Shapely · 10.51 ms | <span class="benchmark-status benchmark-faster">2.43× faster</span> |
| is_valid mixed polygons — 10k mixed polygons / 20% invalid | 5.63 ms | Shapely · 9.60 ms | <span class="benchmark-status benchmark-faster">1.70× faster</span> |
| repair linework — 1k invalid polygons | 12.36 ms | Shapely · 93.28 ms | <span class="benchmark-status benchmark-faster">7.53× faster</span> |
| bounds projected buildings — 10k projected buildings | 140.44 us | Shapely · 165.05 us | <span class="benchmark-status benchmark-faster">1.18× faster</span> |
| clip_by_rect realistic linework — 10k linework / crossing, inside, outside categories | 31.27 ms | Shapely · 2.55 ms | <span class="benchmark-status benchmark-slower">12.21× slower</span> |
| point_on_surface buildings — 10k concave and holed buildings | 990.40 us | Shapely · 2.17 ms | <span class="benchmark-status benchmark-faster">2.20× faster</span> |
</div>

### CRS & geodesy

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="CRS & geodesy benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| to_crs masked points — 200k points / 10% missing ‡ | 6.55 ms | GeoPandas · 64.82 ms | <span class="benchmark-status benchmark-faster">10.00× faster</span> |
| crs_transform WGS84 to Web Mercator — 10k EPSG:4326 to 3857 | 311.32 us | pyproj · 755.35 us | <span class="benchmark-status benchmark-faster">2.43× faster</span> |
| geodesic distance — 10k WGS84 pairs † | 8.45 ms | pyproj · 8.03 ms | <span class="benchmark-status benchmark-slower">1.05× slower</span> |
| geodesic destination — 10k WGS84 destinations † | 3.70 ms | pyproj + Shapely + GeoPandas · 4.26 ms | <span class="benchmark-status benchmark-faster">1.15× faster</span> |
| ellipsoidal chart-edge country area — 217 countries | 161.83 ms | PyProj + NumPy quadrature reference · 143.00 ms | <span class="benchmark-status benchmark-slower">1.14× slower</span> |
| ellipsoidal chart-edge exterior length — 1,034 exteriors / 16,135 vertices | 2.43 ms | PyProj + NumPy quadrature reference · 169.82 ms | <span class="benchmark-status benchmark-faster">70.03× faster</span> |
</div>

### Discrete global grids

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Discrete global grids benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| geohash encode tokens — 10k WGS84 points / precision 6 | 617.35 us | pygeohash · 3.96 ms | <span class="benchmark-status benchmark-faster">6.44× faster</span> |
| h3 cover center Brazil — Brazil res5 / 32,260 cells | 137.92 ms | h3-py · 30.63 ms | <span class="benchmark-status benchmark-slower">4.50× slower</span> |
| h3_compact — 32,260 to 1,732 cells | 484.69 us | h3-py · 537.49 us | <span class="benchmark-status benchmark-faster">1.11× faster</span> |
</div>

### Spatial index

<div markdown="1" class="benchmark-table-scroll" role="region" tabindex="0" aria-label="Spatial index benchmark results">
| API / input | gometry | Comparator | Relative |
|---|---:|---:|---:|
| one-shot join within — 10k POIs x 217 countries | 42.75 ms | Shapely · 30.88 ms | <span class="benchmark-status benchmark-slower">1.38× slower</span> |
| index join within — 10k POIs x 217 countries | 14.95 ms | Shapely · 31.14 ms | <span class="benchmark-status benchmark-faster">2.08× faster</span> |
| index candidates — 1k queries x 10k polygons | 1.39 ms | Shapely · 1.72 ms | <span class="benchmark-status benchmark-faster">1.24× faster</span> |
| index nearest k=1 — 1k non-intersecting queries x 10k polygons | 2.13 ms | Shapely · 5.45 ms | <span class="benchmark-status benchmark-faster">2.56× faster</span> |
| index build — 10k polygons | 603.69 us | Shapely · 1.19 ms | <span class="benchmark-status benchmark-faster">1.97× faster</span> |
</div>

† geodesic/ellipsoidal metric.
‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.

### Scaling Curves

#### union_all: n-ary overlay input count

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `64` | `64 input-geometries` | 645.62 us | Shapely · 1.77 ms | 2.731 | 10.09 us | 27.73 us | <span class="benchmark-status benchmark-faster">2.73× faster</span> |
| `256` | `256 input-geometries` | 3.02 ms | Shapely · 7.22 ms | 2.376 | 11.80 us | 28.19 us | <span class="benchmark-status benchmark-faster">2.38× faster</span> |
| `1024` | `1024 input-geometries` | 13.55 ms | Shapely · 28.78 ms | 2.136 | 13.23 us | 28.11 us | <span class="benchmark-status benchmark-faster">2.14× faster</span> |

Endpoint growth (64 → 1024): gometry median 20.983x, comparator median 16.215x, gometry/work 1.311x, comparator/work 1.013x.

#### tile_cover: output count

Dimension `grid-output-count` in `tiles`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `990` | `990 output-tiles` | 21.08 us | Mercantile · 252.54 us | 12.036 | 21.3 ns | 255.1 ns | <span class="benchmark-status benchmark-faster">12.04× faster</span> |
| `3960` | `3960 output-tiles` | 62.72 us | Mercantile · 1.19 ms | 19.069 | 15.8 ns | 301.3 ns | <span class="benchmark-status benchmark-faster">19.07× faster</span> |
| `15340` | `15340 output-tiles` | 205.20 us | Mercantile · 5.34 ms | 26.165 | 13.4 ns | 348.0 ns | <span class="benchmark-status benchmark-faster">26.17× faster</span> |

Endpoint growth (990 → 15340): gometry median 9.734x, comparator median 21.138x, gometry/work 0.628x, comparator/work 1.364x.

#### index candidates: indexed cardinality

Dimension `indexed-polygons` in `polygons`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1k` | `1000 candidate-pairs` | 84.50 us | Shapely · 73.73 us | 0.849 | 84.5 ns | 73.7 ns | <span class="benchmark-status benchmark-slower">1.18× slower</span> |
| `5k` | `1000 candidate-pairs` | 92.55 us | Shapely · 101.20 us | 1.093 | 92.6 ns | 101.2 ns | <span class="benchmark-status benchmark-faster">1.09× faster</span> |
| `10k` | `1000 candidate-pairs` | 93.37 us | Shapely · 114.67 us | 1.240 | 93.4 ns | 114.7 ns | <span class="benchmark-status benchmark-faster">1.24× faster</span> |

Endpoint growth (1k → 10k): gometry median 1.105x, comparator median 1.555x, gometry/work 1.105x, comparator/work 1.555x.

#### index candidates: query selectivity

Dimension `query-selectivity` in `fraction`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1pct` | `10 candidate-pairs` | 23.80 us | Shapely · 16.00 us | 0.677 | 2.38 us | 1.60 us | <span class="benchmark-status benchmark-slower">1.48× slower</span> |
| `10pct` | `100 candidate-pairs` | 29.43 us | Shapely · 25.15 us | 0.859 | 294.3 ns | 251.5 ns | <span class="benchmark-status benchmark-slower">1.16× slower</span> |
| `50pct` | `500 candidate-pairs` | 57.88 us | Shapely · 63.55 us | 1.098 | 115.8 ns | 127.1 ns | <span class="benchmark-status benchmark-faster">1.10× faster</span> |

Endpoint growth (1pct → 50pct): gometry median 2.431x, comparator median 3.972x, gometry/work 0.049x, comparator/work 0.079x.

#### index candidates: result amplification

Dimension `candidate-pairs-per-query` in `pairs/query`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1` | `1 candidate-pairs` | 1.50 us | Shapely · 1.49 us | 0.973 | 1.50 us | 1.49 us | <span class="benchmark-status benchmark-inconclusive">Inconclusive</span> |
| `10` | `10 candidate-pairs` | 1.63 us | Shapely · 2.38 us | 1.388 | 162.5 ns | 238.0 ns | <span class="benchmark-status benchmark-faster">1.39× faster</span> |
| `100` | `100 candidate-pairs` | 2.40 us | Shapely · 3.63 us | 1.550 | 23.9 ns | 36.4 ns | <span class="benchmark-status benchmark-faster">1.55× faster</span> |

Endpoint growth (1 → 100): gometry median 1.602x, comparator median 2.447x, gometry/work 0.016x, comparator/work 0.024x.

#### union_all: disjoint input count

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `1k` | `1000 input-geometries` | 2.41 ms | Shapely · 28.42 ms | 11.771 | 2.41 us | 28.42 us | <span class="benchmark-status benchmark-faster">11.77× faster</span> |
| `4k` | `4000 input-geometries` | 10.64 ms | Shapely · 145.24 ms | 13.789 | 2.66 us | 36.31 us | <span class="benchmark-status benchmark-faster">13.79× faster</span> |
| `16k` | `16000 input-geometries` | 47.60 ms | Shapely · 763.05 ms | 15.985 | 2.97 us | 47.69 us | <span class="benchmark-status benchmark-faster">15.99× faster</span> |

Endpoint growth (1k → 16k): gometry median 19.712x, comparator median 26.846x, gometry/work 1.232x, comparator/work 1.678x.

#### intersection: polygon hole count

Dimension `holes` in `holes`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `250` | `250 holes` | 532.07 us | Shapely · 763.27 us | 1.434 | 2.13 us | 3.05 us | <span class="benchmark-status benchmark-faster">1.43× faster</span> |
| `1k` | `1000 holes` | 2.35 ms | Shapely · 3.16 ms | 1.346 | 2.35 us | 3.16 us | <span class="benchmark-status benchmark-faster">1.35× faster</span> |
| `4k` | `4000 holes` | 9.84 ms | Shapely · 13.07 ms | 1.340 | 2.46 us | 3.27 us | <span class="benchmark-status benchmark-faster">1.34× faster</span> |

Endpoint growth (250 → 4k): gometry median 18.496x, comparator median 17.123x, gometry/work 1.156x, comparator/work 1.070x.

#### contains coordinate probes: certified batch boundary

Dimension `probe-count` in `probes`; each point is an independent paired workload.

| Point | Useful work | gometry | Comparator | Ratio C/G | gometry/work | Comparator/work | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|
| `below-gate` | `9999 predicate-results` | 402.24 us | Shapely · 839.31 us | 2.096 | 40.2 ns | 83.9 ns | <span class="benchmark-status benchmark-faster">2.10× faster</span> |
| `gate` | `10000 predicate-results` | 402.78 us | Shapely · 832.15 us | 2.070 | 40.3 ns | 83.2 ns | <span class="benchmark-status benchmark-faster">2.07× faster</span> |
| `above-gate` | `10001 predicate-results` | 404.11 us | Shapely · 878.05 us | 2.158 | 40.4 ns | 87.8 ns | <span class="benchmark-status benchmark-faster">2.16× faster</span> |

Endpoint growth (below-gate → above-gate): gometry median 1.005x, comparator median 1.046x, gometry/work 1.004x, comparator/work 1.046x.

[Run manifest, raw measurements, oracle, and producer receipts](../assets/benchmark-runs/a29776bf4721aac9bed51f1a21377cc0dc9c8077e59f98d66504ccb9118131a1/run.json) are retained in the content-addressed evidence bundle; see its [producer receipt index](../assets/benchmark-runs/a29776bf4721aac9bed51f1a21377cc0dc9c8077e59f98d66504ccb9118131a1/receipts/index.json).
