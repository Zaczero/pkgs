## Run environment

Measured on AMD Ryzen 9 5950X 16-Core Processor with Linux 7.2.9 and CPython 3.14.8. Benchmark harness commit: `e589bb57`.
Gometry producer source commit: `46e953cb` (bound to the loaded wheel by its receipt).

Versions: GeoPandas 1.1.4, h3-py 4.5.0, Mercantile 1.2.1, NumPy 2.5.2, pyarrow 25.0.1, pygeohash 3.3.1, pyproj 3.7.2, Shapely 2.1.2, GEOS 3.13.1, PROJ 9.5.1.

### Array construction & I/O

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from_wkb mixed EWKB</span> <small class="benchmark-detail">10k mixed EWKB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.47 ms</span> <span class="benchmark-status">1.63× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">2.36 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from_arrow mixed GeoArrow WKB</span> <small class="benchmark-detail">100k mixed GeoArrow WKB / 10% missing </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">10.59 ms</span> <span class="benchmark-status">2.45× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">25.94 ms</span> <span class="benchmark-runner">GeoPandas</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">to_wkb mixed EWKB</span> <small class="benchmark-detail">10k mixed EWKB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">710.67 us</span> <span class="benchmark-status">4.14× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">2.94 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">coords matrix and row_index</span> <small class="benchmark-detail">100k vertices / float64 matrix + int64 row index </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">214.70 us</span> <span class="benchmark-status">2.00× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">429.71 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">points from NumPy XY</span> <small class="benchmark-detail">10k NumPy XY </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">7.90 us</span> <span class="benchmark-status">134.51× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">1.05 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from WKT mixed geometries</span> <small class="benchmark-detail">10k realistic mixed WKT </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.16 ms</span> <span class="benchmark-status">4.04× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">16.86 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from GeoJSON countries</span> <small class="benchmark-detail">217 countries / 2.26 MB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">16.44 ms</span> <span class="benchmark-status">2.19× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">36.28 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

### Geometry

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersects irregular polygon points</span> <small class="benchmark-detail">12k interior, boundary, and exterior probes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.36 ms</span> <span class="benchmark-status">2.02× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">2.74 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">prepared polygon contains coordinate probes</span> <small class="benchmark-detail">100k probes / 1,316-coordinate holed polygon </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.10 ms</span> <span class="benchmark-status">2.06× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">8.45 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">dwithin pairwise</span> <small class="benchmark-detail">10k pairs / 50% matches </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.79 us</span> <span class="benchmark-status">46.19× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">265.72 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">area projected buildings</span> <small class="benchmark-detail">10k projected buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">167.55 us</span> <span class="benchmark-status">1.11× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">185.45 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">length roads</span> <small class="benchmark-detail">10k roads / 100k vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">132.97 us</span> <span class="benchmark-status">1.51× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">200.70 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">simplify Douglas-Peucker</span> <small class="benchmark-detail">10k roads / 100k vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">772.00 us</span> <span class="benchmark-status">5.32× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">4.10 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">buffer projected points</span> <small class="benchmark-detail">10k projected points </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">765.95 us</span> <span class="benchmark-status">67.73× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">51.88 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">intersection pairwise</span> <small class="benchmark-detail">1k irregular polygon pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">6.92 ms</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">7.25 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersects pairwise polygons</span> <small class="benchmark-detail">1k irregular polygon pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">396.87 us</span> <span class="benchmark-status">1.47× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">589.20 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">coverage_union_all parcels</span> <small class="benchmark-detail">10k edge-matched parcels </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.33 ms</span> <span class="benchmark-status">2.43× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">10.51 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">is_valid mixed polygons</span> <small class="benchmark-detail">10k mixed polygons / 20% invalid </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.63 ms</span> <span class="benchmark-status">1.70× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">9.60 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">repair linework</span> <small class="benchmark-detail">1k invalid polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.36 ms</span> <span class="benchmark-status">7.53× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">93.28 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">bounds projected buildings</span> <small class="benchmark-detail">10k projected buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">140.44 us</span> <span class="benchmark-status">1.18× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">165.05 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">clip_by_rect realistic linework</span> <small class="benchmark-detail">10k linework / crossing, inside, outside categories </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">31.27 ms</span> <span class="benchmark-status">12.21× slower</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">2.55 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">point_on_surface buildings</span> <small class="benchmark-detail">10k concave and holed buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">990.40 us</span> <span class="benchmark-status">2.20× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">2.17 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

### CRS & geodesy

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">to_crs masked points</span> <small class="benchmark-detail">200k points / 10% missing ‡</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">6.55 ms</span> <span class="benchmark-status">10.00× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">64.82 ms</span> <span class="benchmark-runner">GeoPandas</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">crs_transform WGS84 to Web Mercator</span> <small class="benchmark-detail">10k EPSG:4326 to 3857 </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">311.32 us</span> <span class="benchmark-status">2.43× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">755.35 us</span> <span class="benchmark-runner">pyproj</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">geodesic distance</span> <small class="benchmark-detail">10k WGS84 pairs †</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">8.45 ms</span> <span class="benchmark-status">1.05× slower</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">8.03 ms</span> <span class="benchmark-runner">pyproj</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">geodesic destination</span> <small class="benchmark-detail">10k WGS84 destinations †</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.70 ms</span> <span class="benchmark-status">1.15× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">4.26 ms</span> <span class="benchmark-runner">pyproj + Shapely + GeoPandas</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">ellipsoidal chart-edge country area</span> <small class="benchmark-detail">217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">161.83 ms</span> <span class="benchmark-status">1.14× slower</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">143.00 ms</span> <span class="benchmark-runner">PyProj + NumPy quadrature reference</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">ellipsoidal chart-edge exterior length</span> <small class="benchmark-detail">1,034 exteriors / 16,135 vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.43 ms</span> <span class="benchmark-status">70.03× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">169.82 ms</span> <span class="benchmark-runner">PyProj + NumPy quadrature reference</span> </td></tr>
</tbody>
</table>

### Discrete global grids

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">geohash encode tokens</span> <small class="benchmark-detail">10k WGS84 points / precision 6 </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">617.35 us</span> <span class="benchmark-status">6.44× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">3.96 ms</span> <span class="benchmark-runner">pygeohash</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">h3 cover center Brazil</span> <small class="benchmark-detail">Brazil res5 / 32,260 cells </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">137.92 ms</span> <span class="benchmark-status">4.50× slower</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">30.63 ms</span> <span class="benchmark-runner">h3-py</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">h3_compact</span> <small class="benchmark-detail">32,260 to 1,732 cells </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">484.69 us</span> <span class="benchmark-status">1.11× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">537.49 us</span> <span class="benchmark-runner">h3-py</span> </td></tr>
</tbody>
</table>

### Spatial index

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">one-shot join within</span> <small class="benchmark-detail">10k POIs x 217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">42.75 ms</span> <span class="benchmark-status">1.38× slower</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">30.88 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index join within</span> <small class="benchmark-detail">10k POIs x 217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">14.95 ms</span> <span class="benchmark-status">2.08× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">31.14 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index candidates</span> <small class="benchmark-detail">1k queries x 10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.39 ms</span> <span class="benchmark-status">1.24× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">1.72 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index nearest k=1</span> <small class="benchmark-detail">1k non-intersecting queries x 10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.13 ms</span> <span class="benchmark-status">2.56× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">5.45 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index build</span> <small class="benchmark-detail">10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">603.69 us</span> <span class="benchmark-status">1.97× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">1.19 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

† geodesic/ellipsoidal metric.
‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.

### Scaling Curves

#### union_all: n-ary overlay input count

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">64 geometries</span> <small class="benchmark-detail">64 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">645.62 us</span> <small class="benchmark-detail">10.09 us / work</small> <span class="benchmark-status">2.73× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">1.77 ms</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">27.73 us / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">256 geometries</span> <small class="benchmark-detail">256 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.02 ms</span> <small class="benchmark-detail">11.80 us / work</small> <span class="benchmark-status">2.38× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">7.22 ms</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">28.19 us / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1024 geometries</span> <small class="benchmark-detail">1024 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">13.55 ms</span> <small class="benchmark-detail">13.23 us / work</small> <span class="benchmark-status">2.14× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">28.78 ms</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">28.11 us / work</small> </td></tr>
</tbody>
</table>

Endpoint growth (64 → 1024): gometry median 20.983x, comparator median 16.215x, gometry/work 1.311x, comparator/work 1.013x.

#### tile_cover: output count

Dimension `grid-output-count` in `tiles`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">990 tiles</span> <small class="benchmark-detail">990 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">21.08 us</span> <small class="benchmark-detail">21.3 ns / work</small> <span class="benchmark-status">12.04× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">252.54 us</span> <span class="benchmark-runner">Mercantile</span> <small class="benchmark-detail">255.1 ns / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">3960 tiles</span> <small class="benchmark-detail">3960 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">62.72 us</span> <small class="benchmark-detail">15.8 ns / work</small> <span class="benchmark-status">19.07× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">1.19 ms</span> <span class="benchmark-runner">Mercantile</span> <small class="benchmark-detail">301.3 ns / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">15340 tiles</span> <small class="benchmark-detail">15340 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">205.20 us</span> <small class="benchmark-detail">13.4 ns / work</small> <span class="benchmark-status">26.17× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">5.34 ms</span> <span class="benchmark-runner">Mercantile</span> <small class="benchmark-detail">348.0 ns / work</small> </td></tr>
</tbody>
</table>

Endpoint growth (990 → 15340): gometry median 9.734x, comparator median 21.138x, gometry/work 0.628x, comparator/work 1.364x.

#### index candidates: indexed cardinality

Dimension `indexed-polygons` in `polygons`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">84.50 us</span> <small class="benchmark-detail">84.5 ns / work</small> <span class="benchmark-status">1.18× slower</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">73.73 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">73.7 ns / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">5k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">92.55 us</span> <small class="benchmark-detail">92.6 ns / work</small> <span class="benchmark-status">1.09× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">101.20 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">101.2 ns / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">10k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">93.37 us</span> <small class="benchmark-detail">93.4 ns / work</small> <span class="benchmark-status">1.24× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">114.67 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">114.7 ns / work</small> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 10k): gometry median 1.105x, comparator median 1.555x, gometry/work 1.105x, comparator/work 1.555x.

#### index candidates: query selectivity

Dimension `query-selectivity` in `fraction`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1pct fraction</span> <small class="benchmark-detail">10 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">23.80 us</span> <small class="benchmark-detail">2.38 us / work</small> <span class="benchmark-status">1.48× slower</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">16.00 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">1.60 us / work</small> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">10pct fraction</span> <small class="benchmark-detail">100 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">29.43 us</span> <small class="benchmark-detail">294.3 ns / work</small> <span class="benchmark-status">1.16× slower</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">25.15 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">251.5 ns / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">50pct fraction</span> <small class="benchmark-detail">500 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">57.88 us</span> <small class="benchmark-detail">115.8 ns / work</small> <span class="benchmark-status">1.10× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">63.55 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">127.1 ns / work</small> </td></tr>
</tbody>
</table>

Endpoint growth (1pct → 50pct): gometry median 2.431x, comparator median 3.972x, gometry/work 0.049x, comparator/work 0.079x.

#### index candidates: result amplification

Dimension `candidate-pairs-per-query` in `pairs/query`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">1 pairs/query</span> <small class="benchmark-detail">1 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.50 us</span> <small class="benchmark-detail">1.50 us / work</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">1.49 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">1.49 us / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">10 pairs/query</span> <small class="benchmark-detail">10 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.63 us</span> <small class="benchmark-detail">162.5 ns / work</small> <span class="benchmark-status">1.39× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">2.38 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">238.0 ns / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">100 pairs/query</span> <small class="benchmark-detail">100 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.40 us</span> <small class="benchmark-detail">23.9 ns / work</small> <span class="benchmark-status">1.55× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">3.63 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">36.4 ns / work</small> </td></tr>
</tbody>
</table>

Endpoint growth (1 → 100): gometry median 1.602x, comparator median 2.447x, gometry/work 0.016x, comparator/work 0.024x.

#### union_all: disjoint input count

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k geometries</span> <small class="benchmark-detail">1000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.41 ms</span> <small class="benchmark-detail">2.41 us / work</small> <span class="benchmark-status">11.77× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">28.42 ms</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">28.42 us / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">4k geometries</span> <small class="benchmark-detail">4000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">10.64 ms</span> <small class="benchmark-detail">2.66 us / work</small> <span class="benchmark-status">13.79× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">145.24 ms</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">36.31 us / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">16k geometries</span> <small class="benchmark-detail">16000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">47.60 ms</span> <small class="benchmark-detail">2.97 us / work</small> <span class="benchmark-status">15.99× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">763.05 ms</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">47.69 us / work</small> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 16k): gometry median 19.712x, comparator median 26.846x, gometry/work 1.232x, comparator/work 1.678x.

#### intersection: polygon hole count

Dimension `holes` in `holes`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">250 holes</span> <small class="benchmark-detail">250 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">532.07 us</span> <small class="benchmark-detail">2.13 us / work</small> <span class="benchmark-status">1.43× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">763.27 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">3.05 us / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k holes</span> <small class="benchmark-detail">1000 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.35 ms</span> <small class="benchmark-detail">2.35 us / work</small> <span class="benchmark-status">1.35× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">3.16 ms</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">3.16 us / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">4k holes</span> <small class="benchmark-detail">4000 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">9.84 ms</span> <small class="benchmark-detail">2.46 us / work</small> <span class="benchmark-status">1.34× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">13.07 ms</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">3.27 us / work</small> </td></tr>
</tbody>
</table>

Endpoint growth (250 → 4k): gometry median 18.496x, comparator median 17.123x, gometry/work 1.156x, comparator/work 1.070x.

#### contains coordinate probes: certified batch boundary

Dimension `probe-count` in `probes`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">below-gate probes</span> <small class="benchmark-detail">9999 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">402.24 us</span> <small class="benchmark-detail">40.2 ns / work</small> <span class="benchmark-status">2.10× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">839.31 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">83.9 ns / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">gate probes</span> <small class="benchmark-detail">10000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">402.78 us</span> <small class="benchmark-detail">40.3 ns / work</small> <span class="benchmark-status">2.07× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">832.15 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">83.2 ns / work</small> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">above-gate probes</span> <small class="benchmark-detail">10001 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">404.11 us</span> <small class="benchmark-detail">40.4 ns / work</small> <span class="benchmark-status">2.16× faster</span></td><td><span class="benchmark-mobile-label">Comparator</span> <span class="benchmark-time">878.05 us</span> <span class="benchmark-runner">Shapely</span> <small class="benchmark-detail">87.8 ns / work</small> </td></tr>
</tbody>
</table>

Endpoint growth (below-gate → above-gate): gometry median 1.005x, comparator median 1.046x, gometry/work 1.004x, comparator/work 1.046x.

[Run manifest, raw measurements, oracle, and producer receipts](../assets/benchmark-runs/a29776bf4721aac9bed51f1a21377cc0dc9c8077e59f98d66504ccb9118131a1/run.json) are retained in the content-addressed evidence bundle; see its [producer receipt index](../assets/benchmark-runs/a29776bf4721aac9bed51f1a21377cc0dc9c8077e59f98d66504ccb9118131a1/receipts/index.json).
