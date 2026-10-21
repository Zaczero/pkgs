<!-- --8<-- [start:environment] -->

## Run environment

Measured on AMD Ryzen 9 5950X 16-Core Processor with Linux 7.2.9 and CPython 3.15.0. Benchmark harness commit: `21a508b7`.
Gometry producer source commit: `21a508b7` (bound to the loaded wheel by its receipt).

Versions: GeoPandas 1.2.0, h3-py 4.5.0, Mercantile 1.2.1, NumPy 2.5.4, pyarrow 26.0.0, pygeohash 3.5.1, pyproj 3.8.0, Shapely 2.2.0, GEOS 3.14.1, PROJ 9.8.1.

<!-- --8<-- [end:environment] -->

<!-- --8<-- [start:comparisons] -->

### Array construction & I/O

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from_wkb mixed EWKB</span> <small class="benchmark-detail">10k mixed EWKB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.47 ms</span> <span class="benchmark-status">1.83× faster</span></td><td><span class="benchmark-time">2.67 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from_arrow mixed GeoArrow WKB</span> <small class="benchmark-detail">100k mixed GeoArrow WKB / 10% missing </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">10.59 ms</span> <span class="benchmark-status">2.45× faster</span></td><td><span class="benchmark-time">24.86 ms</span> <span class="benchmark-runner">GeoPandas</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">to_wkb mixed EWKB</span> <small class="benchmark-detail">10k mixed EWKB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">783.17 us</span> <span class="benchmark-status">4.35× faster</span></td><td><span class="benchmark-time">3.43 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">coords matrix and row_index</span> <small class="benchmark-detail">100k vertices / float64 matrix + int64 row index </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">222.16 us</span> <span class="benchmark-status">1.97× faster</span></td><td><span class="benchmark-time">434.13 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">points from NumPy XY</span> <small class="benchmark-detail">10k NumPy XY </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.38 us</span> <span class="benchmark-status">225.95× faster</span></td><td><span class="benchmark-time">1.22 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from WKT mixed geometries</span> <small class="benchmark-detail">10k realistic mixed WKT </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.53 ms</span> <span class="benchmark-status">3.38× faster</span></td><td><span class="benchmark-time">16.33 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from GeoJSON countries</span> <small class="benchmark-detail">217 countries / 2.26 MB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">15.74 ms</span> <span class="benchmark-status">1.56× faster</span></td><td><span class="benchmark-time">24.82 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

### Geometry

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersects irregular polygon points</span> <small class="benchmark-detail">12k interior, boundary, and exterior probes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.49 ms</span> <span class="benchmark-status">1.96× faster</span></td><td><span class="benchmark-time">2.90 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">prepared polygon contains coordinate probes</span> <small class="benchmark-detail">100k probes / 1,316-coordinate holed polygon </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.20 ms</span> <span class="benchmark-status">1.96× faster</span></td><td><span class="benchmark-time">8.18 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">dwithin pairwise</span> <small class="benchmark-detail">10k pairs / 50% matches </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.74 us</span> <span class="benchmark-status">44.66× faster</span></td><td><span class="benchmark-time">210.59 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">area projected buildings</span> <small class="benchmark-detail">10k projected buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">178.47 us</span> <span class="benchmark-status">1.08× faster</span></td><td><span class="benchmark-time">194.05 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">length roads</span> <small class="benchmark-detail">10k roads / 100k vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">141.35 us</span> <span class="benchmark-status">1.46× faster</span></td><td><span class="benchmark-time">202.71 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">simplify Douglas-Peucker</span> <small class="benchmark-detail">10k roads / 100k vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">923.24 us</span> <span class="benchmark-status">5.27× faster</span></td><td><span class="benchmark-time">4.92 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">buffer projected points</span> <small class="benchmark-detail">10k projected points </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">808.55 us</span> <span class="benchmark-status">57.34× faster</span></td><td><span class="benchmark-time">46.03 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersection pairwise</span> <small class="benchmark-detail">1k irregular polygon pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">8.25 ms</span> <span class="benchmark-status">1.08× faster</span></td><td><span class="benchmark-time">9.30 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersects pairwise polygons</span> <small class="benchmark-detail">1k irregular polygon pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">384.29 us</span> <span class="benchmark-status">1.51× faster</span></td><td><span class="benchmark-time">585.54 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">coverage_union_all parcels</span> <small class="benchmark-detail">10k edge-matched parcels </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.35 ms</span> <span class="benchmark-status">1.25× faster</span></td><td><span class="benchmark-time">5.49 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">is_valid mixed polygons</span> <small class="benchmark-detail">10k mixed polygons / 20% invalid </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.37 ms</span> <span class="benchmark-status">1.79× faster</span></td><td><span class="benchmark-time">9.55 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">repair linework</span> <small class="benchmark-detail">1k invalid polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.43 ms</span> <span class="benchmark-status">7.31× faster</span></td><td><span class="benchmark-time">91.11 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">bounds projected buildings</span> <small class="benchmark-detail">10k projected buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">144.87 us</span> <span class="benchmark-status">1.18× faster</span></td><td><span class="benchmark-time">171.23 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">Clip short lines to a rectangle (clip_by_rect)</span> <small class="benchmark-detail">10,000 three-vertex lines; 40% crossing, 40% inside, 20% outside </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">11.59 ms</span> <span class="benchmark-status">4.40× slower</span></td><td><span class="benchmark-time">2.65 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">point_on_surface buildings</span> <small class="benchmark-detail">10k concave and holed buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">978.99 us</span> <span class="benchmark-status">2.19× faster</span></td><td><span class="benchmark-time">2.14 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

### CRS & geodesy

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">to_crs masked points</span> <small class="benchmark-detail">200k points / 10% missing ‡</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">7.27 ms</span> <span class="benchmark-status">9.74× faster</span></td><td><span class="benchmark-time">73.58 ms</span> <span class="benchmark-runner">GeoPandas</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">crs_transform WGS84 to Web Mercator</span> <small class="benchmark-detail">10k EPSG:4326 to 3857 </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">339.40 us</span> <span class="benchmark-status">2.31× faster</span></td><td><span class="benchmark-time">790.13 us</span> <span class="benchmark-runner">pyproj</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">geodesic distance</span> <small class="benchmark-detail">10k WGS84 pairs †</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">8.21 ms</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">8.09 ms</span> <span class="benchmark-runner">pyproj</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">geodesic destination</span> <small class="benchmark-detail">10k WGS84 destinations †</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.46 ms</span> <span class="benchmark-status">1.24× faster</span></td><td><span class="benchmark-time">4.36 ms</span> <span class="benchmark-runner">pyproj + Shapely + GeoPandas</span> </td></tr>
</tbody>
</table>

### Discrete global grids

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">geohash encode tokens</span> <small class="benchmark-detail">10k WGS84 points / precision 6 </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">601.11 us</span> <span class="benchmark-status">6.96× faster</span></td><td><span class="benchmark-time">4.17 ms</span> <span class="benchmark-runner">pygeohash</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">h3 cover center Brazil</span> <small class="benchmark-detail">Brazil res5 / 32,260 cells </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">18.25 ms</span> <span class="benchmark-status">2.38× faster</span></td><td><span class="benchmark-time">43.37 ms</span> <span class="benchmark-runner">h3-py</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">h3_compact</span> <small class="benchmark-detail">32,260 to 1,732 cells </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">468.63 us</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">483.41 us</span> <span class="benchmark-runner">h3-py</span> </td></tr>
</tbody>
</table>

### Spatial index

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">one-shot join within</span> <small class="benchmark-detail">10k POIs x 217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">48.20 ms</span> <span class="benchmark-status">1.34× slower</span></td><td><span class="benchmark-time">38.15 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index join within</span> <small class="benchmark-detail">10k POIs x 217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">20.10 ms</span> <span class="benchmark-status">2.16× faster</span></td><td><span class="benchmark-time">34.38 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">index candidates</span> <small class="benchmark-detail">1k queries x 10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.60 ms</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">1.98 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index nearest k=1</span> <small class="benchmark-detail">1k non-intersecting queries x 10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.10 ms</span> <span class="benchmark-status">2.47× faster</span></td><td><span class="benchmark-time">8.50 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: packed points</span> <small class="benchmark-detail">256 viewports / 10k projected points / varied hit counts </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.82 ms</span> <span class="benchmark-status">2.07× slower</span></td><td><span class="benchmark-time">2.43 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: mixed geometries</span> <small class="benchmark-detail">256 viewports / 10k projected points, lines, polygons, and holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.92 ms</span> <span class="benchmark-status">2.73× slower</span></td><td><span class="benchmark-time">1.79 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: geographic geometries</span> <small class="benchmark-detail">256 viewports / 10k mixed EPSG:4326 geometries away from seams and poles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">48.47 ms</span> <span class="benchmark-status">28.54× slower</span></td><td><span class="benchmark-time">1.70 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index build</span> <small class="benchmark-detail">10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">610.57 us</span> <span class="benchmark-status">2.02× faster</span></td><td><span class="benchmark-time">1.23 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

† geodesic/ellipsoidal metric.
‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.

<!-- --8<-- [end:comparisons] -->

<!-- --8<-- [start:references] -->

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">ellipsoidal chart-edge country area</span> <small class="benchmark-detail">217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">161.86 ms</span> <span class="benchmark-status">1.26× slower</span></td><td><span class="benchmark-time">127.69 ms</span> <span class="benchmark-runner">Python/NumPy quadrature reference</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">ellipsoidal chart-edge exterior length</span> <small class="benchmark-detail">1,034 exteriors / 16,135 vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.35 ms</span> <span class="benchmark-status">68.36× faster</span></td><td><span class="benchmark-time">166.04 ms</span> <span class="benchmark-runner">Python/NumPy quadrature reference</span> </td></tr>
</tbody>
</table>

<!-- --8<-- [end:references] -->

<!-- --8<-- [start:scaling] -->

### Scaling Curves

#### Union of edge-matched areas

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">64 geometries</span> <small class="benchmark-detail">64 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">690.09 us</span> <small class="benchmark-detail">10.78 us / input geometry</small> <span class="benchmark-status">2.66× faster</span></td><td><span class="benchmark-time">1.81 ms</span> <small class="benchmark-detail">28.35 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">256 geometries</span> <small class="benchmark-detail">256 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.00 ms</span> <small class="benchmark-detail">11.73 us / input geometry</small> <span class="benchmark-status">2.31× faster</span></td><td><span class="benchmark-time">6.89 ms</span> <small class="benchmark-detail">26.93 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1024 geometries</span> <small class="benchmark-detail">1024 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">16.14 ms</span> <small class="benchmark-detail">15.76 us / input geometry</small> <span class="benchmark-status">2.08× faster</span></td><td><span class="benchmark-time">32.53 ms</span> <small class="benchmark-detail">31.76 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (64 → 1024): gometry median 23.387x, comparator median 17.923x, gometry/work 1.462x, comparator/work 1.120x.

#### Tile cover by output size

Dimension `grid-output-count` in `tiles`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">990 tiles</span> <small class="benchmark-detail">990 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">19.89 us</span> <small class="benchmark-detail">20.1 ns / tile</small> <span class="benchmark-status">11.33× faster</span></td><td><span class="benchmark-time">226.09 us</span> <small class="benchmark-detail">228.4 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">3960 tiles</span> <small class="benchmark-detail">3960 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">61.15 us</span> <small class="benchmark-detail">15.4 ns / tile</small> <span class="benchmark-status">15.94× faster</span></td><td><span class="benchmark-time">971.29 us</span> <small class="benchmark-detail">245.3 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">15340 tiles</span> <small class="benchmark-detail">15340 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">209.16 us</span> <small class="benchmark-detail">13.6 ns / tile</small> <span class="benchmark-status">19.39× faster</span></td><td><span class="benchmark-time">4.03 ms</span> <small class="benchmark-detail">263.0 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
</tbody>
</table>

Endpoint growth (990 → 15340): gometry median 10.518x, comparator median 17.846x, gometry/work 0.679x, comparator/work 1.152x.

#### Index queries as the index grows

Dimension `indexed-polygons` in `polygons`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">88.03 us</span> <small class="benchmark-detail">88.0 ns / candidate pair</small> <span class="benchmark-status">1.32× slower</span></td><td><span class="benchmark-time">67.03 us</span> <small class="benchmark-detail">67.0 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">5k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">99.62 us</span> <small class="benchmark-detail">99.6 ns / candidate pair</small> <span class="benchmark-status">1.08× slower</span></td><td><span class="benchmark-time">92.60 us</span> <small class="benchmark-detail">92.6 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">10k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">100.44 us</span> <small class="benchmark-detail">100.4 ns / candidate pair</small> <span class="benchmark-status">1.07× faster</span></td><td><span class="benchmark-time">107.46 us</span> <small class="benchmark-detail">107.5 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 10k): gometry median 1.141x, comparator median 1.603x, gometry/work 1.141x, comparator/work 1.603x.

#### Index queries by hit rate

Dimension `queries-with-a-match` in `fraction`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1pct fraction</span> <small class="benchmark-detail">10 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">24.75 us</span> <small class="benchmark-detail">2.48 us / candidate pair</small> <span class="benchmark-status">1.88× slower</span></td><td><span class="benchmark-time">13.12 us</span> <small class="benchmark-detail">1.31 us / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">10pct fraction</span> <small class="benchmark-detail">100 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">30.86 us</span> <small class="benchmark-detail">308.6 ns / candidate pair</small> <span class="benchmark-status">1.40× slower</span></td><td><span class="benchmark-time">21.96 us</span> <small class="benchmark-detail">219.6 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">50pct fraction</span> <small class="benchmark-detail">500 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">61.43 us</span> <small class="benchmark-detail">122.9 ns / candidate pair</small> <span class="benchmark-status">1.04× slower</span></td><td><span class="benchmark-time">58.86 us</span> <small class="benchmark-detail">117.7 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1pct → 50pct): gometry median 2.482x, comparator median 4.488x, gometry/work 0.050x, comparator/work 0.090x.

#### Index queries returning more candidates

Dimension `candidate-pairs-per-query` in `pairs/query`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1 pairs/query</span> <small class="benchmark-detail">1 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">928.0 ns</span> <small class="benchmark-detail">928.0 ns / candidate pair</small> <span class="benchmark-status">1.50× slower</span></td><td><span class="benchmark-time">625.0 ns</span> <small class="benchmark-detail">625.0 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-similar"><th scope="row"><span class="benchmark-api">10 pairs/query</span> <small class="benchmark-detail">10 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">949.2 ns</span> <small class="benchmark-detail">94.9 ns / candidate pair</small> <span class="benchmark-status">Similar</span></td><td><span class="benchmark-time">929.2 ns</span> <small class="benchmark-detail">92.9 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">100 pairs/query</span> <small class="benchmark-detail">100 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.52 us</span> <small class="benchmark-detail">15.2 ns / candidate pair</small> <span class="benchmark-status">1.15× faster</span></td><td><span class="benchmark-time">1.73 us</span> <small class="benchmark-detail">17.3 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1 → 100): gometry median 1.637x, comparator median 2.774x, gometry/work 0.016x, comparator/work 0.028x.

#### Union of disjoint polygons

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k geometries</span> <small class="benchmark-detail">1000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.44 ms</span> <small class="benchmark-detail">2.44 us / input geometry</small> <span class="benchmark-status">11.28× faster</span></td><td><span class="benchmark-time">27.15 ms</span> <small class="benchmark-detail">27.15 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">4k geometries</span> <small class="benchmark-detail">4000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">10.86 ms</span> <small class="benchmark-detail">2.71 us / input geometry</small> <span class="benchmark-status">12.77× faster</span></td><td><span class="benchmark-time">139.50 ms</span> <small class="benchmark-detail">34.87 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">16k geometries</span> <small class="benchmark-detail">16000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">49.59 ms</span> <small class="benchmark-detail">3.10 us / input geometry</small> <span class="benchmark-status">14.79× faster</span></td><td><span class="benchmark-time">732.74 ms</span> <small class="benchmark-detail">45.80 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 16k): gometry median 20.363x, comparator median 26.984x, gometry/work 1.273x, comparator/work 1.686x.

#### Intersection with many holes

Dimension `holes` in `holes`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">250 holes</span> <small class="benchmark-detail">250 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">526.26 us</span> <small class="benchmark-detail">2.11 us / hole</small> <span class="benchmark-status">1.40× faster</span></td><td><span class="benchmark-time">736.13 us</span> <small class="benchmark-detail">2.94 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k holes</span> <small class="benchmark-detail">1000 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.26 ms</span> <small class="benchmark-detail">2.26 us / hole</small> <span class="benchmark-status">1.32× faster</span></td><td><span class="benchmark-time">2.99 ms</span> <small class="benchmark-detail">2.99 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">4k holes</span> <small class="benchmark-detail">4000 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">10.01 ms</span> <small class="benchmark-detail">2.50 us / hole</small> <span class="benchmark-status">1.29× faster</span></td><td><span class="benchmark-time">12.73 ms</span> <small class="benchmark-detail">3.18 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (250 → 4k): gometry median 19.013x, comparator median 17.296x, gometry/work 1.188x, comparator/work 1.081x.

#### Point-in-polygon queries

Dimension `probe-count` in `probes`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k probes</span> <small class="benchmark-detail">1000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">31.05 us</span> <small class="benchmark-detail">31.1 ns / query</small> <span class="benchmark-status">2.09× faster</span></td><td><span class="benchmark-time">64.76 us</span> <small class="benchmark-detail">64.8 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">10k probes</span> <small class="benchmark-detail">10000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">395.84 us</span> <small class="benchmark-detail">39.6 ns / query</small> <span class="benchmark-status">2.03× faster</span></td><td><span class="benchmark-time">807.59 us</span> <small class="benchmark-detail">80.8 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">100k probes</span> <small class="benchmark-detail">100000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.06 ms</span> <small class="benchmark-detail">40.6 ns / query</small> <span class="benchmark-status">2.00× faster</span></td><td><span class="benchmark-time">8.13 ms</span> <small class="benchmark-detail">81.3 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 100k): gometry median 130.810x, comparator median 125.617x, gometry/work 1.308x, comparator/work 1.256x.

<!-- --8<-- [end:scaling] -->

<!-- --8<-- [start:evidence] -->

[Raw measurements](../assets/benchmark-runs/03a235654288a2d96ff6044dac1143d19f3fbb75b3f91c059451be3accebabba/run.json) · [Build provenance](../assets/benchmark-runs/03a235654288a2d96ff6044dac1143d19f3fbb75b3f91c059451be3accebabba/receipts/index.json).

<!-- --8<-- [end:evidence] -->
