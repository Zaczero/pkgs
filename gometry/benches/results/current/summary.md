<!-- --8<-- [start:environment] -->

## Run environment

Measured on AMD Ryzen 9 5950X 16-Core Processor with Linux 7.2.9 and CPython 3.15.0. Benchmark harness commit: `6a538156`.
Gometry producer source commit: `23423935` (bound to the loaded wheel by its receipt).

Versions: GeoPandas 1.2.0, h3-py 4.5.0, Mercantile 1.2.1, NumPy 2.5.4, pyarrow 26.0.0, pygeohash 3.5.1, pyproj 3.8.0, Shapely 2.2.0, GEOS 3.14.1, PROJ 9.8.1.

<!-- --8<-- [end:environment] -->

<!-- --8<-- [start:comparisons] -->

### Array construction & I/O

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from_wkb mixed EWKB</span> <small class="benchmark-detail">10k mixed EWKB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.44 ms</span> <span class="benchmark-status">1.73× faster</span></td><td><span class="benchmark-time">2.51 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from_arrow mixed GeoArrow WKB</span> <small class="benchmark-detail">100k mixed GeoArrow WKB / 10% missing </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">9.98 ms</span> <span class="benchmark-status">2.43× faster</span></td><td><span class="benchmark-time">24.26 ms</span> <span class="benchmark-runner">GeoPandas</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">to_wkb mixed EWKB</span> <small class="benchmark-detail">10k mixed EWKB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">699.56 us</span> <span class="benchmark-status">4.26× faster</span></td><td><span class="benchmark-time">2.98 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">coords matrix and row_index</span> <small class="benchmark-detail">100k vertices / float64 matrix + int64 row index </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">211.27 us</span> <span class="benchmark-status">1.97× faster</span></td><td><span class="benchmark-time">415.19 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">points from NumPy XY</span> <small class="benchmark-detail">10k NumPy XY </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.27 us</span> <span class="benchmark-status">219.42× faster</span></td><td><span class="benchmark-time">1.16 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from WKT mixed geometries</span> <small class="benchmark-detail">10k realistic mixed WKT </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.18 ms</span> <span class="benchmark-status">3.14× faster</span></td><td><span class="benchmark-time">13.13 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from GeoJSON countries</span> <small class="benchmark-detail">217 countries / 2.26 MB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">15.06 ms</span> <span class="benchmark-status">1.55× faster</span></td><td><span class="benchmark-time">23.52 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

### Geometry

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersects irregular polygon points</span> <small class="benchmark-detail">12k interior, boundary, and exterior probes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.33 ms</span> <span class="benchmark-status">1.95× faster</span></td><td><span class="benchmark-time">2.59 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">prepared polygon contains coordinate probes</span> <small class="benchmark-detail">100k probes / 1,316-coordinate holed polygon </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.07 ms</span> <span class="benchmark-status">1.97× faster</span></td><td><span class="benchmark-time">8.00 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">dwithin pairwise</span> <small class="benchmark-detail">10k pairs / 50% matches </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.63 us</span> <span class="benchmark-status">44.89× faster</span></td><td><span class="benchmark-time">210.15 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">area projected buildings</span> <small class="benchmark-detail">10k projected buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">172.57 us</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">174.76 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">length roads</span> <small class="benchmark-detail">10k roads / 100k vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">130.69 us</span> <span class="benchmark-status">1.48× faster</span></td><td><span class="benchmark-time">193.11 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">simplify Douglas-Peucker</span> <small class="benchmark-detail">10k roads / 100k vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">757.47 us</span> <span class="benchmark-status">5.29× faster</span></td><td><span class="benchmark-time">4.01 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">buffer projected points</span> <small class="benchmark-detail">10k projected points </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">778.31 us</span> <span class="benchmark-status">53.04× faster</span></td><td><span class="benchmark-time">41.21 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">intersection pairwise</span> <small class="benchmark-detail">1k irregular polygon pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">6.63 ms</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">6.82 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersects pairwise polygons</span> <small class="benchmark-detail">1k irregular polygon pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">376.34 us</span> <span class="benchmark-status">1.45× faster</span></td><td><span class="benchmark-time">547.89 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">coverage_union_all parcels</span> <small class="benchmark-detail">10k edge-matched parcels </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.25 ms</span> <span class="benchmark-status">1.26× faster</span></td><td><span class="benchmark-time">5.37 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">is_valid mixed polygons</span> <small class="benchmark-detail">10k mixed polygons / 20% invalid </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.40 ms</span> <span class="benchmark-status">1.69× faster</span></td><td><span class="benchmark-time">9.13 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">repair linework</span> <small class="benchmark-detail">1k invalid polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.65 ms</span> <span class="benchmark-status">7.18× faster</span></td><td><span class="benchmark-time">90.75 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">bounds projected buildings</span> <small class="benchmark-detail">10k projected buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">142.13 us</span> <span class="benchmark-status">1.15× faster</span></td><td><span class="benchmark-time">162.89 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">Clip short lines to a rectangle (clip_by_rect)</span> <small class="benchmark-detail">10,000 three-vertex lines; 40% crossing, 40% inside, 20% outside </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">11.89 ms</span> <span class="benchmark-status">4.54× slower</span></td><td><span class="benchmark-time">2.62 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">point_on_surface buildings</span> <small class="benchmark-detail">10k concave and holed buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">987.08 us</span> <span class="benchmark-status">2.12× faster</span></td><td><span class="benchmark-time">2.10 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

### CRS & geodesy

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">to_crs masked points</span> <small class="benchmark-detail">200k points / 10% missing ‡</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">6.62 ms</span> <span class="benchmark-status">9.93× faster</span></td><td><span class="benchmark-time">65.92 ms</span> <span class="benchmark-runner">GeoPandas</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">crs_transform WGS84 to Web Mercator</span> <small class="benchmark-detail">10k EPSG:4326 to 3857 </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">337.90 us</span> <span class="benchmark-status">2.32× faster</span></td><td><span class="benchmark-time">786.76 us</span> <span class="benchmark-runner">pyproj</span> </td></tr>
<tr class="benchmark-similar"><th scope="row"><span class="benchmark-api">geodesic distance</span> <small class="benchmark-detail">10k WGS84 pairs †</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">8.19 ms</span> <span class="benchmark-status">Similar</span></td><td><span class="benchmark-time">8.17 ms</span> <span class="benchmark-runner">pyproj</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">geodesic destination</span> <small class="benchmark-detail">10k WGS84 destinations †</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.47 ms</span> <span class="benchmark-status">1.24× faster</span></td><td><span class="benchmark-time">4.29 ms</span> <span class="benchmark-runner">pyproj + Shapely + GeoPandas</span> </td></tr>
</tbody>
</table>

### Discrete global grids

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">geohash encode tokens</span> <small class="benchmark-detail">10k WGS84 points / precision 6 </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">591.13 us</span> <span class="benchmark-status">6.93× faster</span></td><td><span class="benchmark-time">4.09 ms</span> <span class="benchmark-runner">pygeohash</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">h3 cover center Brazil</span> <small class="benchmark-detail">Brazil res5 / 32,260 cells </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.74 ms</span> <span class="benchmark-status">2.29× faster</span></td><td><span class="benchmark-time">29.04 ms</span> <span class="benchmark-runner">h3-py</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">h3_compact</span> <small class="benchmark-detail">32,260 to 1,732 cells </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">457.51 us</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">468.83 us</span> <span class="benchmark-runner">h3-py</span> </td></tr>
</tbody>
</table>

### Spatial index

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">one-shot join within</span> <small class="benchmark-detail">10k POIs x 217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">41.95 ms</span> <span class="benchmark-status">1.41× slower</span></td><td><span class="benchmark-time">29.42 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index join within</span> <small class="benchmark-detail">10k POIs x 217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">15.85 ms</span> <span class="benchmark-status">2.12× faster</span></td><td><span class="benchmark-time">37.31 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index candidates</span> <small class="benchmark-detail">1k queries x 10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.00 ms</span> <span class="benchmark-status">1.27× faster</span></td><td><span class="benchmark-time">3.81 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index nearest k=1</span> <small class="benchmark-detail">1k non-intersecting queries x 10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.15 ms</span> <span class="benchmark-status">2.37× faster</span></td><td><span class="benchmark-time">5.13 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: packed points</span> <small class="benchmark-detail">256 viewports / 10k projected points / varied hit counts </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.53 ms</span> <span class="benchmark-status">2.22× slower</span></td><td><span class="benchmark-time">1.57 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: mixed geometries</span> <small class="benchmark-detail">256 viewports / 10k projected points, lines, polygons, and holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.00 ms</span> <span class="benchmark-status">2.83× slower</span></td><td><span class="benchmark-time">1.78 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: geographic geometries</span> <small class="benchmark-detail">256 viewports / 10k mixed EPSG:4326 geometries away from seams and poles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">58.59 ms</span> <span class="benchmark-status">29.53× slower</span></td><td><span class="benchmark-time">1.91 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index build</span> <small class="benchmark-detail">10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.09 ms</span> <span class="benchmark-status">2.40× faster</span></td><td><span class="benchmark-time">2.61 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

† geodesic/ellipsoidal metric.
‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.

<!-- --8<-- [end:comparisons] -->

<!-- --8<-- [start:references] -->

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">ellipsoidal chart-edge country area</span> <small class="benchmark-detail">217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">169.41 ms</span> <span class="benchmark-status">1.36× slower</span></td><td><span class="benchmark-time">125.05 ms</span> <span class="benchmark-runner">Python/NumPy quadrature reference</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">ellipsoidal chart-edge exterior length</span> <small class="benchmark-detail">1,034 exteriors / 16,135 vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.34 ms</span> <span class="benchmark-status">67.48× faster</span></td><td><span class="benchmark-time">158.66 ms</span> <span class="benchmark-runner">Python/NumPy quadrature reference</span> </td></tr>
</tbody>
</table>

<!-- --8<-- [end:references] -->

<!-- --8<-- [start:scaling] -->

### Scaling Curves

#### union_all: n-ary overlay input count

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">64 geometries</span> <small class="benchmark-detail">64 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">645.24 us</span> <small class="benchmark-detail">10.08 us / input geometry</small> <span class="benchmark-status">2.63× faster</span></td><td><span class="benchmark-time">1.69 ms</span> <small class="benchmark-detail">26.39 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">256 geometries</span> <small class="benchmark-detail">256 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.67 ms</span> <small class="benchmark-detail">14.35 us / input geometry</small> <span class="benchmark-status">2.28× faster</span></td><td><span class="benchmark-time">8.12 ms</span> <small class="benchmark-detail">31.71 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1024 geometries</span> <small class="benchmark-detail">1024 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.82 ms</span> <small class="benchmark-detail">12.52 us / input geometry</small> <span class="benchmark-status">2.08× faster</span></td><td><span class="benchmark-time">26.80 ms</span> <small class="benchmark-detail">26.17 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (64 → 1024): gometry median 19.866x, comparator median 15.865x, gometry/work 1.242x, comparator/work 0.992x.

#### tile_cover: output count

Dimension `grid-output-count` in `tiles`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">990 tiles</span> <small class="benchmark-detail">990 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">37.58 us</span> <small class="benchmark-detail">38.0 ns / tile</small> <span class="benchmark-status">13.66× faster</span></td><td><span class="benchmark-time">514.15 us</span> <small class="benchmark-detail">519.3 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">3960 tiles</span> <small class="benchmark-detail">3960 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">75.90 us</span> <small class="benchmark-detail">19.2 ns / tile</small> <span class="benchmark-status">16.69× faster</span></td><td><span class="benchmark-time">1.30 ms</span> <small class="benchmark-detail">328.7 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">15340 tiles</span> <small class="benchmark-detail">15340 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">199.26 us</span> <small class="benchmark-detail">13.0 ns / tile</small> <span class="benchmark-status">19.77× faster</span></td><td><span class="benchmark-time">3.96 ms</span> <small class="benchmark-detail">258.1 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
</tbody>
</table>

Endpoint growth (990 → 15340): gometry median 5.303x, comparator median 7.702x, gometry/work 0.342x, comparator/work 0.497x.

#### index candidates: indexed cardinality

Dimension `indexed-polygons` in `polygons`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">97.68 us</span> <small class="benchmark-detail">97.7 ns / candidate pair</small> <span class="benchmark-status">1.28× slower</span></td><td><span class="benchmark-time">75.12 us</span> <small class="benchmark-detail">75.1 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">5k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">111.14 us</span> <small class="benchmark-detail">111.1 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">138.71 us</span> <small class="benchmark-detail">138.7 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">10k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">94.16 us</span> <small class="benchmark-detail">94.2 ns / candidate pair</small> <span class="benchmark-status">1.15× faster</span></td><td><span class="benchmark-time">108.22 us</span> <small class="benchmark-detail">108.2 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 10k): gometry median 0.964x, comparator median 1.441x, gometry/work 0.964x, comparator/work 1.441x.

#### Spatial index: queries with a match

Dimension `queries-with-a-match` in `fraction`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1pct fraction</span> <small class="benchmark-detail">10 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">23.12 us</span> <small class="benchmark-detail">2.31 us / candidate pair</small> <span class="benchmark-status">1.78× slower</span></td><td><span class="benchmark-time">13.13 us</span> <small class="benchmark-detail">1.31 us / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">10pct fraction</span> <small class="benchmark-detail">100 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">38.46 us</span> <small class="benchmark-detail">384.6 ns / candidate pair</small> <span class="benchmark-status">1.19× slower</span></td><td><span class="benchmark-time">31.25 us</span> <small class="benchmark-detail">312.5 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">50pct fraction</span> <small class="benchmark-detail">500 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">63.25 us</span> <small class="benchmark-detail">126.5 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">65.29 us</span> <small class="benchmark-detail">130.6 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1pct → 50pct): gometry median 2.736x, comparator median 4.972x, gometry/work 0.055x, comparator/work 0.099x.

#### index candidates: result amplification

Dimension `candidate-pairs-per-query` in `pairs/query`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1 pairs/query</span> <small class="benchmark-detail">1 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.05 us</span> <small class="benchmark-detail">1.05 us / candidate pair</small> <span class="benchmark-status">1.49× slower</span></td><td><span class="benchmark-time">711.5 ns</span> <small class="benchmark-detail">711.5 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">10 pairs/query</span> <small class="benchmark-detail">10 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">965.6 ns</span> <small class="benchmark-detail">96.6 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">991.0 ns</span> <small class="benchmark-detail">99.1 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">100 pairs/query</span> <small class="benchmark-detail">100 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.53 us</span> <small class="benchmark-detail">15.3 ns / candidate pair</small> <span class="benchmark-status">1.25× faster</span></td><td><span class="benchmark-time">1.91 us</span> <small class="benchmark-detail">19.1 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1 → 100): gometry median 1.457x, comparator median 2.690x, gometry/work 0.015x, comparator/work 0.027x.

#### union_all: disjoint input count

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k geometries</span> <small class="benchmark-detail">1000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.09 ms</span> <small class="benchmark-detail">5.09 us / input geometry</small> <span class="benchmark-status">13.37× faster</span></td><td><span class="benchmark-time">67.98 ms</span> <small class="benchmark-detail">67.98 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">4k geometries</span> <small class="benchmark-detail">4000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">11.01 ms</span> <small class="benchmark-detail">2.75 us / input geometry</small> <span class="benchmark-status">12.66× faster</span></td><td><span class="benchmark-time">139.22 ms</span> <small class="benchmark-detail">34.81 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">16k geometries</span> <small class="benchmark-detail">16000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">49.82 ms</span> <small class="benchmark-detail">3.11 us / input geometry</small> <span class="benchmark-status">14.58× faster</span></td><td><span class="benchmark-time">752.98 ms</span> <small class="benchmark-detail">47.06 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 16k): gometry median 9.795x, comparator median 11.077x, gometry/work 0.612x, comparator/work 0.692x.

#### intersection: polygon hole count

Dimension `holes` in `holes`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">250 holes</span> <small class="benchmark-detail">250 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">545.49 us</span> <small class="benchmark-detail">2.18 us / hole</small> <span class="benchmark-status">1.39× faster</span></td><td><span class="benchmark-time">753.33 us</span> <small class="benchmark-detail">3.01 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k holes</span> <small class="benchmark-detail">1000 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.35 ms</span> <small class="benchmark-detail">2.35 us / hole</small> <span class="benchmark-status">1.30× faster</span></td><td><span class="benchmark-time">3.02 ms</span> <small class="benchmark-detail">3.02 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">4k holes</span> <small class="benchmark-detail">4000 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">14.14 ms</span> <small class="benchmark-detail">3.54 us / hole</small> <span class="benchmark-status">1.34× faster</span></td><td><span class="benchmark-time">18.95 ms</span> <small class="benchmark-detail">4.74 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (250 → 4k): gometry median 25.922x, comparator median 25.152x, gometry/work 1.620x, comparator/work 1.572x.

#### Point-in-polygon queries

Dimension `probe-count` in `probes`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k probes</span> <small class="benchmark-detail">1000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">31.86 us</span> <small class="benchmark-detail">31.9 ns / query</small> <span class="benchmark-status">2.04× faster</span></td><td><span class="benchmark-time">65.52 us</span> <small class="benchmark-detail">65.5 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">10k probes</span> <small class="benchmark-detail">10000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">401.25 us</span> <small class="benchmark-detail">40.1 ns / query</small> <span class="benchmark-status">2.02× faster</span></td><td><span class="benchmark-time">808.68 us</span> <small class="benchmark-detail">80.9 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">100k probes</span> <small class="benchmark-detail">100000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.07 ms</span> <small class="benchmark-detail">40.7 ns / query</small> <span class="benchmark-status">1.97× faster</span></td><td><span class="benchmark-time">8.03 ms</span> <small class="benchmark-detail">80.3 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 100k): gometry median 127.753x, comparator median 122.582x, gometry/work 1.278x, comparator/work 1.226x.

<!-- --8<-- [end:scaling] -->

<!-- --8<-- [start:evidence] -->

[Raw measurements](../assets/benchmark-runs/6f544771a9ead759017ff978425174c3900d0d24836d7459956d027a18aa530d/run.json) · [Build provenance](../assets/benchmark-runs/6f544771a9ead759017ff978425174c3900d0d24836d7459956d027a18aa530d/receipts/index.json).

<!-- --8<-- [end:evidence] -->
