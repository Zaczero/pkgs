<!-- --8<-- [start:environment] -->

## Run environment

Measured on AMD Ryzen 9 5950X 16-Core Processor with Linux 7.2.9 and CPython 3.15.0. Benchmark harness commit: `73ba6194`.
Gometry producer source commit: `5121b198` (bound to the loaded wheel by its receipt).

Versions: GeoPandas 1.2.0, h3-py 4.5.0, Mercantile 1.2.1, NumPy 2.5.4, pyarrow 26.0.0, pygeohash 3.5.1, pyproj 3.8.0, Shapely 2.2.0, GEOS 3.14.1, PROJ 9.8.1.

<!-- --8<-- [end:environment] -->

Ratios are comparator duration divided by gometry duration. Each comparison uses exact median order intervals with at least 95% coverage across its permitted looks. “Similar” requires the entire interval within the 3% practical-equivalence margin; a narrow interval crossing 1 is not by itself evidence of equality.

<!-- --8<-- [start:comparisons] -->

### Array construction & I/O

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from_wkb mixed EWKB</span> <small class="benchmark-detail">10k mixed EWKB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.51 ms</span> <span class="benchmark-status">1.67× faster</span></td><td><span class="benchmark-time">2.54 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from_arrow mixed GeoArrow WKB</span> <small class="benchmark-detail">100k mixed GeoArrow WKB / 10% missing </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">10.74 ms</span> <span class="benchmark-status">2.45× faster</span></td><td><span class="benchmark-time">26.34 ms</span> <span class="benchmark-runner">GeoPandas</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">to_wkb mixed EWKB</span> <small class="benchmark-detail">10k mixed EWKB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">706.08 us</span> <span class="benchmark-status">4.42× faster</span></td><td><span class="benchmark-time">3.15 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">coords matrix and row_index</span> <small class="benchmark-detail">100k vertices / float64 matrix + int64 row index </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">213.17 us</span> <span class="benchmark-status">1.96× faster</span></td><td><span class="benchmark-time">418.16 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">points from NumPy XY</span> <small class="benchmark-detail">10k NumPy XY </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.39 us</span> <span class="benchmark-status">206.85× faster</span></td><td><span class="benchmark-time">1.11 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from WKT mixed geometries</span> <small class="benchmark-detail">10k realistic mixed WKT </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.34 ms</span> <span class="benchmark-status">3.14× faster</span></td><td><span class="benchmark-time">13.61 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from GeoJSON countries</span> <small class="benchmark-detail">217 countries / 2.26 MB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">15.95 ms</span> <span class="benchmark-status">1.55× faster</span></td><td><span class="benchmark-time">24.84 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

### Geometry

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersects irregular polygon points</span> <small class="benchmark-detail">12k interior, boundary, and exterior probes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.34 ms</span> <span class="benchmark-status">2.06× faster</span></td><td><span class="benchmark-time">2.77 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">prepared polygon contains coordinate probes</span> <small class="benchmark-detail">100k probes / 1,316-coordinate holed polygon </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.14 ms</span> <span class="benchmark-status">1.95× faster</span></td><td><span class="benchmark-time">8.05 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">dwithin pairwise</span> <small class="benchmark-detail">10k pairs / 50% matches </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.78 us</span> <span class="benchmark-status">50.96× faster</span></td><td><span class="benchmark-time">246.24 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">area projected buildings</span> <small class="benchmark-detail">10k projected buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">173.64 us</span> <span class="benchmark-status">1.05× faster</span></td><td><span class="benchmark-time">183.08 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">length roads</span> <small class="benchmark-detail">10k roads / 100k vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">137.99 us</span> <span class="benchmark-status">1.42× faster</span></td><td><span class="benchmark-time">196.97 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">simplify Douglas-Peucker</span> <small class="benchmark-detail">10k roads / 100k vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">790.90 us</span> <span class="benchmark-status">5.03× faster</span></td><td><span class="benchmark-time">3.98 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">buffer projected points</span> <small class="benchmark-detail">10k projected points </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">798.17 us</span> <span class="benchmark-status">61.94× faster</span></td><td><span class="benchmark-time">49.69 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersection pairwise</span> <small class="benchmark-detail">1k irregular polygon pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">6.53 ms</span> <span class="benchmark-status">1.08× faster</span></td><td><span class="benchmark-time">7.06 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersects pairwise polygons</span> <small class="benchmark-detail">1k irregular polygon pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">373.53 us</span> <span class="benchmark-status">1.52× faster</span></td><td><span class="benchmark-time">565.42 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">coverage_union_all parcels</span> <small class="benchmark-detail">10k edge-matched parcels </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.45 ms</span> <span class="benchmark-status">1.27× faster</span></td><td><span class="benchmark-time">5.60 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">is_valid mixed polygons</span> <small class="benchmark-detail">10k mixed polygons / 20% invalid </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.27 ms</span> <span class="benchmark-status">1.87× faster</span></td><td><span class="benchmark-time">9.85 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">repair linework</span> <small class="benchmark-detail">1k invalid polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.72 ms</span> <span class="benchmark-status">7.24× faster</span></td><td><span class="benchmark-time">90.66 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">bounds projected buildings</span> <small class="benchmark-detail">10k projected buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">142.37 us</span> <span class="benchmark-status">1.16× faster</span></td><td><span class="benchmark-time">164.23 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">Intersect short lines with a rectangle</span> <small class="benchmark-detail">10,000 three-vertex lines; 40% crossing, 40% inside, 20% outside </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.32 ms</span> <span class="benchmark-status">3.70× faster</span></td><td><span class="benchmark-time">45.52 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">point_on_surface buildings</span> <small class="benchmark-detail">10k concave and holed buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.00 ms</span> <span class="benchmark-status">2.08× faster</span></td><td><span class="benchmark-time">2.10 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

### CRS & geodesy

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">to_crs masked points</span> <small class="benchmark-detail">200k points / 10% missing ‡</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">6.87 ms</span> <span class="benchmark-status">11.16× faster</span></td><td><span class="benchmark-time">76.64 ms</span> <span class="benchmark-runner">GeoPandas</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">crs_transform WGS84 to Web Mercator</span> <small class="benchmark-detail">10k EPSG:4326 to 3857 </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">337.47 us</span> <span class="benchmark-status">2.32× faster</span></td><td><span class="benchmark-time">783.83 us</span> <span class="benchmark-runner">pyproj</span> </td></tr>
<tr class="benchmark-similar"><th scope="row"><span class="benchmark-api">geodesic distance</span> <small class="benchmark-detail">10k WGS84 pairs †</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">8.20 ms</span> <span class="benchmark-status">Similar</span></td><td><span class="benchmark-time">8.07 ms</span> <span class="benchmark-runner">pyproj</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">geodesic destination</span> <small class="benchmark-detail">10k WGS84 destinations †</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.54 ms</span> <span class="benchmark-status">1.23× faster</span></td><td><span class="benchmark-time">4.33 ms</span> <span class="benchmark-runner">pyproj + Shapely + GeoPandas</span> </td></tr>
</tbody>
</table>

### Discrete global grids

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">geohash encode tokens</span> <small class="benchmark-detail">10k WGS84 points / precision 6 </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">588.68 us</span> <span class="benchmark-status">7.08× faster</span></td><td><span class="benchmark-time">4.17 ms</span> <span class="benchmark-runner">pygeohash</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">h3 cover center Brazil</span> <small class="benchmark-detail">Brazil res5 / 32,260 cells </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.87 ms</span> <span class="benchmark-status">2.32× faster</span></td><td><span class="benchmark-time">30.16 ms</span> <span class="benchmark-runner">h3-py</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">h3_compact</span> <small class="benchmark-detail">32,260 to 1,732 cells </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">478.26 us</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">477.20 us</span> <span class="benchmark-runner">h3-py</span> </td></tr>
</tbody>
</table>

### Spatial index

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">one-shot join within</span> <small class="benchmark-detail">10k POIs x 217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">42.09 ms</span> <span class="benchmark-status">1.41× slower</span></td><td><span class="benchmark-time">29.74 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index join within</span> <small class="benchmark-detail">10k POIs x 217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">14.88 ms</span> <span class="benchmark-status">2.10× faster</span></td><td><span class="benchmark-time">30.62 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index candidates</span> <small class="benchmark-detail">1k queries x 10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.50 ms</span> <span class="benchmark-status">3.52× faster</span></td><td><span class="benchmark-time">5.29 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index nearest k=1</span> <small class="benchmark-detail">1k non-intersecting queries x 10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.26 ms</span> <span class="benchmark-status">2.67× faster</span></td><td><span class="benchmark-time">7.03 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: packed points</span> <small class="benchmark-detail">256 viewports / 10k projected points / varied hit counts </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.63 ms</span> <span class="benchmark-status">2.10× slower</span></td><td><span class="benchmark-time">1.73 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: mixed geometries</span> <small class="benchmark-detail">256 viewports / 10k projected points, lines, polygons, and holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.29 ms</span> <span class="benchmark-status">2.58× slower</span></td><td><span class="benchmark-time">2.03 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: geographic geometries</span> <small class="benchmark-detail">256 viewports / 10k mixed EPSG:4326 geometries away from seams and poles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">53.92 ms</span> <span class="benchmark-status">28.24× slower</span></td><td><span class="benchmark-time">1.93 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index build</span> <small class="benchmark-detail">10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">638.68 us</span> <span class="benchmark-status">2.06× faster</span></td><td><span class="benchmark-time">1.33 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

† geodesic/ellipsoidal metric.
‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.

<!-- --8<-- [end:comparisons] -->

<!-- --8<-- [start:references] -->

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">ellipsoidal chart-edge country area</span> <small class="benchmark-detail">217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">162.25 ms</span> <span class="benchmark-status">1.29× slower</span></td><td><span class="benchmark-time">126.39 ms</span> <span class="benchmark-runner">Python/NumPy quadrature reference</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">ellipsoidal chart-edge exterior length</span> <small class="benchmark-detail">1,034 exteriors / 16,135 vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.42 ms</span> <span class="benchmark-status">67.32× faster</span></td><td><span class="benchmark-time">163.10 ms</span> <span class="benchmark-runner">Python/NumPy quadrature reference</span> </td></tr>
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
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">64 geometries</span> <small class="benchmark-detail">64 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">686.18 us</span> <small class="benchmark-detail">10.72 us / input geometry</small> <span class="benchmark-status">2.58× faster</span></td><td><span class="benchmark-time">1.79 ms</span> <small class="benchmark-detail">27.99 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">256 geometries</span> <small class="benchmark-detail">256 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.23 ms</span> <small class="benchmark-detail">12.62 us / input geometry</small> <span class="benchmark-status">2.30× faster</span></td><td><span class="benchmark-time">7.52 ms</span> <small class="benchmark-detail">29.37 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1024 geometries</span> <small class="benchmark-detail">1024 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">13.75 ms</span> <small class="benchmark-detail">13.43 us / input geometry</small> <span class="benchmark-status">2.03× faster</span></td><td><span class="benchmark-time">27.80 ms</span> <small class="benchmark-detail">27.15 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (64 → 1024): gometry median 20.044x, comparator median 15.524x, gometry/work 1.253x, comparator/work 0.970x.

#### Tile cover by output size

Dimension `grid-output-count` in `tiles`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">990 tiles</span> <small class="benchmark-detail">990 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">20.04 us</span> <small class="benchmark-detail">20.2 ns / tile</small> <span class="benchmark-status">11.44× faster</span></td><td><span class="benchmark-time">228.93 us</span> <small class="benchmark-detail">231.2 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">3960 tiles</span> <small class="benchmark-detail">3960 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">61.52 us</span> <small class="benchmark-detail">15.5 ns / tile</small> <span class="benchmark-status">15.72× faster</span></td><td><span class="benchmark-time">967.24 us</span> <small class="benchmark-detail">244.3 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">15340 tiles</span> <small class="benchmark-detail">15340 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">205.64 us</span> <small class="benchmark-detail">13.4 ns / tile</small> <span class="benchmark-status">19.95× faster</span></td><td><span class="benchmark-time">4.19 ms</span> <small class="benchmark-detail">273.1 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
</tbody>
</table>

Endpoint growth (990 → 15340): gometry median 10.261x, comparator median 18.302x, gometry/work 0.662x, comparator/work 1.181x.

#### Index queries as the index grows

Dimension `indexed-polygons` in `polygons`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">85.19 us</span> <small class="benchmark-detail">85.2 ns / candidate pair</small> <span class="benchmark-status">1.23× slower</span></td><td><span class="benchmark-time">69.18 us</span> <small class="benchmark-detail">69.2 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">5k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">95.47 us</span> <small class="benchmark-detail">95.5 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">98.05 us</span> <small class="benchmark-detail">98.0 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">10k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">95.22 us</span> <small class="benchmark-detail">95.2 ns / candidate pair</small> <span class="benchmark-status">1.16× faster</span></td><td><span class="benchmark-time">111.39 us</span> <small class="benchmark-detail">111.4 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 10k): gometry median 1.118x, comparator median 1.610x, gometry/work 1.118x, comparator/work 1.610x.

#### Index queries by hit rate

Dimension `queries-with-a-match` in `fraction`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1pct fraction</span> <small class="benchmark-detail">10 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">23.69 us</span> <small class="benchmark-detail">2.37 us / candidate pair</small> <span class="benchmark-status">1.82× slower</span></td><td><span class="benchmark-time">13.08 us</span> <small class="benchmark-detail">1.31 us / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">10pct fraction</span> <small class="benchmark-detail">100 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">29.78 us</span> <small class="benchmark-detail">297.8 ns / candidate pair</small> <span class="benchmark-status">1.35× slower</span></td><td><span class="benchmark-time">22.01 us</span> <small class="benchmark-detail">220.1 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">50pct fraction</span> <small class="benchmark-detail">500 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">57.44 us</span> <small class="benchmark-detail">114.9 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">59.79 us</span> <small class="benchmark-detail">119.6 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1pct → 50pct): gometry median 2.424x, comparator median 4.571x, gometry/work 0.048x, comparator/work 0.091x.

#### Index queries returning more candidates

Dimension `candidate-pairs-per-query` in `pairs/query`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1 pairs/query</span> <small class="benchmark-detail">1 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">936.3 ns</span> <small class="benchmark-detail">936.3 ns / candidate pair</small> <span class="benchmark-status">1.51× slower</span></td><td><span class="benchmark-time">622.8 ns</span> <small class="benchmark-detail">622.8 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">10 pairs/query</span> <small class="benchmark-detail">10 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.01 us</span> <small class="benchmark-detail">100.7 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">1.01 us</span> <small class="benchmark-detail">101.4 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">100 pairs/query</span> <small class="benchmark-detail">100 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.60 us</span> <small class="benchmark-detail">16.0 ns / candidate pair</small> <span class="benchmark-status">1.19× faster</span></td><td><span class="benchmark-time">1.91 us</span> <small class="benchmark-detail">19.1 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1 → 100): gometry median 1.707x, comparator median 3.059x, gometry/work 0.017x, comparator/work 0.031x.

#### Union of disjoint polygons

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k geometries</span> <small class="benchmark-detail">1000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.47 ms</span> <small class="benchmark-detail">2.47 us / input geometry</small> <span class="benchmark-status">11.33× faster</span></td><td><span class="benchmark-time">27.83 ms</span> <small class="benchmark-detail">27.83 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">4k geometries</span> <small class="benchmark-detail">4000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.17 ms</span> <small class="benchmark-detail">3.04 us / input geometry</small> <span class="benchmark-status">11.81× faster</span></td><td><span class="benchmark-time">145.17 ms</span> <small class="benchmark-detail">36.29 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">16k geometries</span> <small class="benchmark-detail">16000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">49.95 ms</span> <small class="benchmark-detail">3.12 us / input geometry</small> <span class="benchmark-status">14.56× faster</span></td><td><span class="benchmark-time">725.21 ms</span> <small class="benchmark-detail">45.33 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 16k): gometry median 20.241x, comparator median 26.056x, gometry/work 1.265x, comparator/work 1.628x.

#### Intersection with many holes

Dimension `holes` in `holes`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">250 holes</span> <small class="benchmark-detail">250 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">530.62 us</span> <small class="benchmark-detail">2.12 us / hole</small> <span class="benchmark-status">1.40× faster</span></td><td><span class="benchmark-time">741.80 us</span> <small class="benchmark-detail">2.97 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k holes</span> <small class="benchmark-detail">1000 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.27 ms</span> <small class="benchmark-detail">2.27 us / hole</small> <span class="benchmark-status">1.33× faster</span></td><td><span class="benchmark-time">3.01 ms</span> <small class="benchmark-detail">3.01 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">4k holes</span> <small class="benchmark-detail">4000 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">9.74 ms</span> <small class="benchmark-detail">2.43 us / hole</small> <span class="benchmark-status">1.30× faster</span></td><td><span class="benchmark-time">12.72 ms</span> <small class="benchmark-detail">3.18 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (250 → 4k): gometry median 18.355x, comparator median 17.143x, gometry/work 1.147x, comparator/work 1.071x.

#### Point-in-polygon queries

Dimension `probe-count` in `probes`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k probes</span> <small class="benchmark-detail">1000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">31.86 us</span> <small class="benchmark-detail">31.9 ns / query</small> <span class="benchmark-status">2.04× faster</span></td><td><span class="benchmark-time">64.99 us</span> <small class="benchmark-detail">65.0 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">10k probes</span> <small class="benchmark-detail">10000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">402.65 us</span> <small class="benchmark-detail">40.3 ns / query</small> <span class="benchmark-status">1.99× faster</span></td><td><span class="benchmark-time">807.09 us</span> <small class="benchmark-detail">80.7 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">100k probes</span> <small class="benchmark-detail">100000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.11 ms</span> <small class="benchmark-detail">41.1 ns / query</small> <span class="benchmark-status">1.96× faster</span></td><td><span class="benchmark-time">8.06 ms</span> <small class="benchmark-detail">80.6 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 100k): gometry median 128.892x, comparator median 124.022x, gometry/work 1.289x, comparator/work 1.240x.

<!-- --8<-- [end:scaling] -->

<!-- --8<-- [start:evidence] -->

[Raw measurements](../assets/benchmark-runs/59db822ebc73926a1688c48db0569ce31bcedd7a32689dec329f60c14395922a/run.json) · [Build provenance](../assets/benchmark-runs/59db822ebc73926a1688c48db0569ce31bcedd7a32689dec329f60c14395922a/receipts/index.json).

<!-- --8<-- [end:evidence] -->
