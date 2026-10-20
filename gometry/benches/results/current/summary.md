<!-- --8<-- [start:environment] -->

## Run environment

Measured on AMD Ryzen 9 5950X 16-Core Processor with Linux 7.2.9 and CPython 3.15.0. Benchmark harness commit: `b919f069`.
Gometry producer source commit: `b919f069` (bound to the loaded wheel by its receipt).

Versions: GeoPandas 1.2.0, h3-py 4.5.0, Mercantile 1.2.1, NumPy 2.5.4, pyarrow 26.0.0, pygeohash 3.5.1, pyproj 3.8.0, Shapely 2.2.0, GEOS 3.14.1, PROJ 9.8.1.

<!-- --8<-- [end:environment] -->

<!-- --8<-- [start:comparisons] -->

### Array construction & I/O

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from_wkb mixed EWKB</span> <small class="benchmark-detail">10k mixed EWKB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.44 ms</span> <span class="benchmark-status">1.74× faster</span></td><td><span class="benchmark-time">2.51 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from_arrow mixed GeoArrow WKB</span> <small class="benchmark-detail">100k mixed GeoArrow WKB / 10% missing </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">10.13 ms</span> <span class="benchmark-status">2.39× faster</span></td><td><span class="benchmark-time">24.33 ms</span> <span class="benchmark-runner">GeoPandas</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">to_wkb mixed EWKB</span> <small class="benchmark-detail">10k mixed EWKB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">694.05 us</span> <span class="benchmark-status">4.30× faster</span></td><td><span class="benchmark-time">2.98 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">coords matrix and row_index</span> <small class="benchmark-detail">100k vertices / float64 matrix + int64 row index </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">211.87 us</span> <span class="benchmark-status">1.96× faster</span></td><td><span class="benchmark-time">416.16 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">points from NumPy XY</span> <small class="benchmark-detail">10k NumPy XY </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.26 us</span> <span class="benchmark-status">219.05× faster</span></td><td><span class="benchmark-time">1.15 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from WKT mixed geometries</span> <small class="benchmark-detail">10k realistic mixed WKT </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.20 ms</span> <span class="benchmark-status">3.19× faster</span></td><td><span class="benchmark-time">13.49 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from GeoJSON countries</span> <small class="benchmark-detail">217 countries / 2.26 MB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">15.29 ms</span> <span class="benchmark-status">1.57× faster</span></td><td><span class="benchmark-time">24.03 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

### Geometry

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersects irregular polygon points</span> <small class="benchmark-detail">12k interior, boundary, and exterior probes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.33 ms</span> <span class="benchmark-status">1.97× faster</span></td><td><span class="benchmark-time">2.63 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">prepared polygon contains coordinate probes</span> <small class="benchmark-detail">100k probes / 1,316-coordinate holed polygon </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.11 ms</span> <span class="benchmark-status">1.96× faster</span></td><td><span class="benchmark-time">8.05 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">dwithin pairwise</span> <small class="benchmark-detail">10k pairs / 50% matches </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.39 us</span> <span class="benchmark-status">51.83× faster</span></td><td><span class="benchmark-time">278.96 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">area projected buildings</span> <small class="benchmark-detail">10k projected buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">177.08 us</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">188.37 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">length roads</span> <small class="benchmark-detail">10k roads / 100k vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">130.86 us</span> <span class="benchmark-status">1.49× faster</span></td><td><span class="benchmark-time">194.26 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">simplify Douglas-Peucker</span> <small class="benchmark-detail">10k roads / 100k vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">755.41 us</span> <span class="benchmark-status">5.30× faster</span></td><td><span class="benchmark-time">4.01 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">buffer projected points</span> <small class="benchmark-detail">10k projected points </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">778.54 us</span> <span class="benchmark-status">54.01× faster</span></td><td><span class="benchmark-time">41.81 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">intersection pairwise</span> <small class="benchmark-detail">1k irregular polygon pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">6.66 ms</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">6.89 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersects pairwise polygons</span> <small class="benchmark-detail">1k irregular polygon pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">372.68 us</span> <span class="benchmark-status">1.46× faster</span></td><td><span class="benchmark-time">544.44 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">coverage_union_all parcels</span> <small class="benchmark-detail">10k edge-matched parcels </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.22 ms</span> <span class="benchmark-status">1.27× faster</span></td><td><span class="benchmark-time">5.39 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">is_valid mixed polygons</span> <small class="benchmark-detail">10k mixed polygons / 20% invalid </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.41 ms</span> <span class="benchmark-status">1.68× faster</span></td><td><span class="benchmark-time">9.10 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">repair linework</span> <small class="benchmark-detail">1k invalid polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.43 ms</span> <span class="benchmark-status">7.19× faster</span></td><td><span class="benchmark-time">89.37 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">bounds projected buildings</span> <small class="benchmark-detail">10k projected buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">141.34 us</span> <span class="benchmark-status">1.15× faster</span></td><td><span class="benchmark-time">162.29 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">Clip short lines to a rectangle (clip_by_rect)</span> <small class="benchmark-detail">10,000 three-vertex lines; 40% crossing, 40% inside, 20% outside </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">11.43 ms</span> <span class="benchmark-status">4.45× slower</span></td><td><span class="benchmark-time">2.57 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">point_on_surface buildings</span> <small class="benchmark-detail">10k concave and holed buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">974.85 us</span> <span class="benchmark-status">2.13× faster</span></td><td><span class="benchmark-time">2.08 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

### CRS & geodesy

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">to_crs masked points</span> <small class="benchmark-detail">200k points / 10% missing ‡</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">6.57 ms</span> <span class="benchmark-status">9.67× faster</span></td><td><span class="benchmark-time">63.42 ms</span> <span class="benchmark-runner">GeoPandas</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">crs_transform WGS84 to Web Mercator</span> <small class="benchmark-detail">10k EPSG:4326 to 3857 </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">333.37 us</span> <span class="benchmark-status">2.33× faster</span></td><td><span class="benchmark-time">777.00 us</span> <span class="benchmark-runner">pyproj</span> </td></tr>
<tr class="benchmark-similar"><th scope="row"><span class="benchmark-api">geodesic distance</span> <small class="benchmark-detail">10k WGS84 pairs †</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">7.99 ms</span> <span class="benchmark-status">Similar</span></td><td><span class="benchmark-time">7.97 ms</span> <span class="benchmark-runner">pyproj</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">geodesic destination</span> <small class="benchmark-detail">10k WGS84 destinations †</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.44 ms</span> <span class="benchmark-status">1.25× faster</span></td><td><span class="benchmark-time">4.38 ms</span> <span class="benchmark-runner">pyproj + Shapely + GeoPandas</span> </td></tr>
</tbody>
</table>

### Discrete global grids

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">geohash encode tokens</span> <small class="benchmark-detail">10k WGS84 points / precision 6 </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">599.79 us</span> <span class="benchmark-status">6.75× faster</span></td><td><span class="benchmark-time">4.07 ms</span> <span class="benchmark-runner">pygeohash</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">h3 cover center Brazil</span> <small class="benchmark-detail">Brazil res5 / 32,260 cells </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.68 ms</span> <span class="benchmark-status">2.27× faster</span></td><td><span class="benchmark-time">28.73 ms</span> <span class="benchmark-runner">h3-py</span> </td></tr>
<tr class="benchmark-similar"><th scope="row"><span class="benchmark-api">h3_compact</span> <small class="benchmark-detail">32,260 to 1,732 cells </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">454.91 us</span> <span class="benchmark-status">Similar</span></td><td><span class="benchmark-time">463.32 us</span> <span class="benchmark-runner">h3-py</span> </td></tr>
</tbody>
</table>

### Spatial index

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">one-shot join within</span> <small class="benchmark-detail">10k POIs x 217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">41.11 ms</span> <span class="benchmark-status">1.40× slower</span></td><td><span class="benchmark-time">29.34 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index join within</span> <small class="benchmark-detail">10k POIs x 217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">13.78 ms</span> <span class="benchmark-status">2.11× faster</span></td><td><span class="benchmark-time">29.05 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index candidates</span> <small class="benchmark-detail">1k queries x 10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.44 ms</span> <span class="benchmark-status">1.20× faster</span></td><td><span class="benchmark-time">1.72 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index nearest k=1</span> <small class="benchmark-detail">1k non-intersecting queries x 10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.10 ms</span> <span class="benchmark-status">2.38× faster</span></td><td><span class="benchmark-time">4.99 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: packed points</span> <small class="benchmark-detail">256 viewports / 10k projected points / varied hit counts </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.38 ms</span> <span class="benchmark-status">2.16× slower</span></td><td><span class="benchmark-time">1.56 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: mixed geometries</span> <small class="benchmark-detail">256 viewports / 10k projected points, lines, polygons, and holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.82 ms</span> <span class="benchmark-status">2.81× slower</span></td><td><span class="benchmark-time">1.73 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: geographic geometries</span> <small class="benchmark-detail">256 viewports / 10k mixed EPSG:4326 geometries away from seams and poles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">48.24 ms</span> <span class="benchmark-status">28.79× slower</span></td><td><span class="benchmark-time">1.67 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index build</span> <small class="benchmark-detail">10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">608.14 us</span> <span class="benchmark-status">2.01× faster</span></td><td><span class="benchmark-time">1.22 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

† geodesic/ellipsoidal metric.
‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.

<!-- --8<-- [end:comparisons] -->

<!-- --8<-- [start:references] -->

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">ellipsoidal chart-edge country area</span> <small class="benchmark-detail">217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">161.64 ms</span> <span class="benchmark-status">1.31× slower</span></td><td><span class="benchmark-time">123.69 ms</span> <span class="benchmark-runner">Python/NumPy quadrature reference</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">ellipsoidal chart-edge exterior length</span> <small class="benchmark-detail">1,034 exteriors / 16,135 vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.33 ms</span> <span class="benchmark-status">67.75× faster</span></td><td><span class="benchmark-time">157.59 ms</span> <span class="benchmark-runner">Python/NumPy quadrature reference</span> </td></tr>
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
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">64 geometries</span> <small class="benchmark-detail">64 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">636.24 us</span> <small class="benchmark-detail">9.94 us / input geometry</small> <span class="benchmark-status">2.64× faster</span></td><td><span class="benchmark-time">1.69 ms</span> <small class="benchmark-detail">26.38 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">256 geometries</span> <small class="benchmark-detail">256 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.94 ms</span> <small class="benchmark-detail">11.48 us / input geometry</small> <span class="benchmark-status">2.33× faster</span></td><td><span class="benchmark-time">6.90 ms</span> <small class="benchmark-detail">26.94 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1024 geometries</span> <small class="benchmark-detail">1024 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.96 ms</span> <small class="benchmark-detail">12.65 us / input geometry</small> <span class="benchmark-status">2.10× faster</span></td><td><span class="benchmark-time">27.25 ms</span> <small class="benchmark-detail">26.61 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (64 → 1024): gometry median 20.363x, comparator median 16.140x, gometry/work 1.273x, comparator/work 1.009x.

#### Tile cover by output size

Dimension `grid-output-count` in `tiles`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">990 tiles</span> <small class="benchmark-detail">990 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">19.82 us</span> <small class="benchmark-detail">20.0 ns / tile</small> <span class="benchmark-status">11.43× faster</span></td><td><span class="benchmark-time">226.41 us</span> <small class="benchmark-detail">228.7 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">3960 tiles</span> <small class="benchmark-detail">3960 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">60.30 us</span> <small class="benchmark-detail">15.2 ns / tile</small> <span class="benchmark-status">15.93× faster</span></td><td><span class="benchmark-time">961.78 us</span> <small class="benchmark-detail">242.9 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">15340 tiles</span> <small class="benchmark-detail">15340 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">199.80 us</span> <small class="benchmark-detail">13.0 ns / tile</small> <span class="benchmark-status">19.73× faster</span></td><td><span class="benchmark-time">3.94 ms</span> <small class="benchmark-detail">257.1 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
</tbody>
</table>

Endpoint growth (990 → 15340): gometry median 10.081x, comparator median 17.417x, gometry/work 0.651x, comparator/work 1.124x.

#### Index queries as the index grows

Dimension `indexed-polygons` in `polygons`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">83.27 us</span> <small class="benchmark-detail">83.3 ns / candidate pair</small> <span class="benchmark-status">1.28× slower</span></td><td><span class="benchmark-time">65.59 us</span> <small class="benchmark-detail">65.6 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">5k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">93.79 us</span> <small class="benchmark-detail">93.8 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">91.52 us</span> <small class="benchmark-detail">91.5 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">10k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">93.70 us</span> <small class="benchmark-detail">93.7 ns / candidate pair</small> <span class="benchmark-status">1.13× faster</span></td><td><span class="benchmark-time">106.21 us</span> <small class="benchmark-detail">106.2 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 10k): gometry median 1.125x, comparator median 1.619x, gometry/work 1.125x, comparator/work 1.619x.

#### Index queries by hit rate

Dimension `queries-with-a-match` in `fraction`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1pct fraction</span> <small class="benchmark-detail">10 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">23.03 us</span> <small class="benchmark-detail">2.30 us / candidate pair</small> <span class="benchmark-status">1.74× slower</span></td><td><span class="benchmark-time">13.26 us</span> <small class="benchmark-detail">1.33 us / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">10pct fraction</span> <small class="benchmark-detail">100 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">29.16 us</span> <small class="benchmark-detail">291.6 ns / candidate pair</small> <span class="benchmark-status">1.35× slower</span></td><td><span class="benchmark-time">21.63 us</span> <small class="benchmark-detail">216.3 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">50pct fraction</span> <small class="benchmark-detail">500 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">56.16 us</span> <small class="benchmark-detail">112.3 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">58.10 us</span> <small class="benchmark-detail">116.2 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1pct → 50pct): gometry median 2.438x, comparator median 4.381x, gometry/work 0.049x, comparator/work 0.088x.

#### Index queries returning more candidates

Dimension `candidate-pairs-per-query` in `pairs/query`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1 pairs/query</span> <small class="benchmark-detail">1 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">911.5 ns</span> <small class="benchmark-detail">911.5 ns / candidate pair</small> <span class="benchmark-status">1.48× slower</span></td><td><span class="benchmark-time">618.3 ns</span> <small class="benchmark-detail">618.3 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">10 pairs/query</span> <small class="benchmark-detail">10 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">954.0 ns</span> <small class="benchmark-detail">95.4 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">995.8 ns</span> <small class="benchmark-detail">99.6 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">100 pairs/query</span> <small class="benchmark-detail">100 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.52 us</span> <small class="benchmark-detail">15.2 ns / candidate pair</small> <span class="benchmark-status">1.28× faster</span></td><td><span class="benchmark-time">1.94 us</span> <small class="benchmark-detail">19.4 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1 → 100): gometry median 1.667x, comparator median 3.135x, gometry/work 0.017x, comparator/work 0.031x.

#### Union of disjoint polygons

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k geometries</span> <small class="benchmark-detail">1000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.40 ms</span> <small class="benchmark-detail">2.40 us / input geometry</small> <span class="benchmark-status">11.21× faster</span></td><td><span class="benchmark-time">26.91 ms</span> <small class="benchmark-detail">26.91 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">4k geometries</span> <small class="benchmark-detail">4000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">10.79 ms</span> <small class="benchmark-detail">2.70 us / input geometry</small> <span class="benchmark-status">12.70× faster</span></td><td><span class="benchmark-time">136.98 ms</span> <small class="benchmark-detail">34.24 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">16k geometries</span> <small class="benchmark-detail">16000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">48.52 ms</span> <small class="benchmark-detail">3.03 us / input geometry</small> <span class="benchmark-status">14.53× faster</span></td><td><span class="benchmark-time">699.75 ms</span> <small class="benchmark-detail">43.73 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 16k): gometry median 20.206x, comparator median 25.999x, gometry/work 1.263x, comparator/work 1.625x.

#### Intersection with many holes

Dimension `holes` in `holes`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">250 holes</span> <small class="benchmark-detail">250 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">528.31 us</span> <small class="benchmark-detail">2.11 us / hole</small> <span class="benchmark-status">1.38× faster</span></td><td><span class="benchmark-time">733.42 us</span> <small class="benchmark-detail">2.93 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k holes</span> <small class="benchmark-detail">1000 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.26 ms</span> <small class="benchmark-detail">2.26 us / hole</small> <span class="benchmark-status">1.31× faster</span></td><td><span class="benchmark-time">2.95 ms</span> <small class="benchmark-detail">2.95 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">4k holes</span> <small class="benchmark-detail">4000 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">9.73 ms</span> <small class="benchmark-detail">2.43 us / hole</small> <span class="benchmark-status">1.28× faster</span></td><td><span class="benchmark-time">12.46 ms</span> <small class="benchmark-detail">3.12 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (250 → 4k): gometry median 18.418x, comparator median 16.996x, gometry/work 1.151x, comparator/work 1.062x.

#### Point-in-polygon queries

Dimension `probe-count` in `probes`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k probes</span> <small class="benchmark-detail">1000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">31.36 us</span> <small class="benchmark-detail">31.4 ns / query</small> <span class="benchmark-status">2.03× faster</span></td><td><span class="benchmark-time">63.72 us</span> <small class="benchmark-detail">63.7 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">10k probes</span> <small class="benchmark-detail">10000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">398.34 us</span> <small class="benchmark-detail">39.8 ns / query</small> <span class="benchmark-status">2.00× faster</span></td><td><span class="benchmark-time">800.66 us</span> <small class="benchmark-detail">80.1 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">100k probes</span> <small class="benchmark-detail">100000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.05 ms</span> <small class="benchmark-detail">40.5 ns / query</small> <span class="benchmark-status">1.97× faster</span></td><td><span class="benchmark-time">7.97 ms</span> <small class="benchmark-detail">79.7 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 100k): gometry median 129.097x, comparator median 125.127x, gometry/work 1.291x, comparator/work 1.251x.

<!-- --8<-- [end:scaling] -->

<!-- --8<-- [start:evidence] -->

[Raw measurements](../assets/benchmark-runs/af279ac2d60c1678e6eb29defedf40443a94f381b36df9a1819c6be4442e4b03/run.json) · [Build provenance](../assets/benchmark-runs/af279ac2d60c1678e6eb29defedf40443a94f381b36df9a1819c6be4442e4b03/receipts/index.json).

<!-- --8<-- [end:evidence] -->
