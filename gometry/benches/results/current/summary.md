<!-- --8<-- [start:environment] -->

## Run environment

Measured on AMD Ryzen 9 5950X 16-Core Processor with Linux 7.2.9 and CPython 3.15.0. Benchmark harness commit: `d801b232`.
Gometry producer source commit: `d801b232` (bound to the loaded wheel by its receipt).

Versions: GeoPandas 1.2.0, h3-py 4.5.0, Mercantile 1.2.1, NumPy 2.5.4, pyarrow 26.0.0, pygeohash 3.5.1, pyproj 3.8.0, Shapely 2.2.0, GEOS 3.14.1, PROJ 9.8.1.

<!-- --8<-- [end:environment] -->

Ratios are comparator duration divided by gometry duration. Each comparison uses exact median order intervals with at least 95% coverage across its permitted looks. “Similar” requires the entire interval within the 3% practical-equivalence margin; a narrow interval crossing 1 is not by itself evidence of equality.

<!-- --8<-- [start:comparisons] -->

### Array construction & I/O

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from_wkb mixed EWKB</span> <small class="benchmark-detail">10k mixed EWKB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.47 ms</span> <span class="benchmark-status">1.67× faster</span></td><td><span class="benchmark-time">2.47 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from_arrow mixed GeoArrow WKB</span> <small class="benchmark-detail">100k mixed GeoArrow WKB / 10% missing </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">11.47 ms</span> <span class="benchmark-status">2.26× faster</span></td><td><span class="benchmark-time">26.24 ms</span> <span class="benchmark-runner">GeoPandas</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">to_wkb mixed EWKB</span> <small class="benchmark-detail">10k mixed EWKB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">732.39 us</span> <span class="benchmark-status">4.35× faster</span></td><td><span class="benchmark-time">3.19 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">coords matrix and row_index</span> <small class="benchmark-detail">100k vertices / float64 matrix + int64 row index </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">239.41 us</span> <span class="benchmark-status">2.01× faster</span></td><td><span class="benchmark-time">480.23 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">points from NumPy XY</span> <small class="benchmark-detail">10k NumPy XY </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.50 us</span> <span class="benchmark-status">203.25× faster</span></td><td><span class="benchmark-time">1.13 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from WKT mixed geometries</span> <small class="benchmark-detail">10k realistic mixed WKT </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.36 ms</span> <span class="benchmark-status">3.28× faster</span></td><td><span class="benchmark-time">14.32 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">from GeoJSON countries</span> <small class="benchmark-detail">217 countries / 2.26 MB </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">16.48 ms</span> <span class="benchmark-status">1.59× faster</span></td><td><span class="benchmark-time">26.35 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

### Geometry

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">intersects irregular polygon points</span> <small class="benchmark-detail">12k interior, boundary, and exterior probes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.44 ms</span> <span class="benchmark-status">1.92× faster</span></td><td><span class="benchmark-time">2.74 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">prepared polygon contains coordinate probes</span> <small class="benchmark-detail">100k probes / 1,316-coordinate holed polygon </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.58 ms</span> <span class="benchmark-status">1.91× faster</span></td><td><span class="benchmark-time">8.56 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">dwithin pairwise</span> <small class="benchmark-detail">10k pairs / 50% matches </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.73 us</span> <span class="benchmark-status">55.22× faster</span></td><td><span class="benchmark-time">263.41 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">area projected buildings</span> <small class="benchmark-detail">10k projected buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">170.94 us</span> <span class="benchmark-status">1.08× faster</span></td><td><span class="benchmark-time">186.09 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">length roads</span> <small class="benchmark-detail">10k roads / 100k vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">137.33 us</span> <span class="benchmark-status">1.51× faster</span></td><td><span class="benchmark-time">203.93 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">simplify Douglas-Peucker</span> <small class="benchmark-detail">10k roads / 100k vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">772.93 us</span> <span class="benchmark-status">5.35× faster</span></td><td><span class="benchmark-time">4.13 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">buffer projected points</span> <small class="benchmark-detail">10k projected points </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">828.19 us</span> <span class="benchmark-status">62.35× faster</span></td><td><span class="benchmark-time">51.45 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">intersection pairwise</span> <small class="benchmark-detail">1k irregular polygon pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">7.14 ms</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">7.51 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">intersects pairwise polygons</span> <small class="benchmark-detail">1k irregular polygon pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">620.77 us</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">594.60 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">coverage_union_all parcels</span> <small class="benchmark-detail">10k edge-matched parcels </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.30 ms</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">6.12 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">is_valid mixed polygons</span> <small class="benchmark-detail">10k mixed polygons / 20% invalid </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">5.50 ms</span> <span class="benchmark-status">1.78× faster</span></td><td><span class="benchmark-time">9.90 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">repair linework</span> <small class="benchmark-detail">1k invalid polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">12.72 ms</span> <span class="benchmark-status">7.32× faster</span></td><td><span class="benchmark-time">93.74 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">bounds projected buildings</span> <small class="benchmark-detail">10k projected buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">147.84 us</span> <span class="benchmark-status">1.21× faster</span></td><td><span class="benchmark-time">180.83 us</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">Intersect short lines with a rectangle</span> <small class="benchmark-detail">10,000 three-vertex lines; 40% crossing, 40% inside, 20% outside </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">13.15 ms</span> <span class="benchmark-status">3.45× faster</span></td><td><span class="benchmark-time">45.60 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">point_on_surface buildings</span> <small class="benchmark-detail">10k concave and holed buildings </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.01 ms</span> <span class="benchmark-status">2.14× faster</span></td><td><span class="benchmark-time">2.15 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

### CRS & geodesy

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">to_crs masked points</span> <small class="benchmark-detail">200k points / 10% missing ‡</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">7.11 ms</span> <span class="benchmark-status">11.13× faster</span></td><td><span class="benchmark-time">78.67 ms</span> <span class="benchmark-runner">GeoPandas</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">crs_transform WGS84 to Web Mercator</span> <small class="benchmark-detail">10k EPSG:4326 to 3857 </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">344.88 us</span> <span class="benchmark-status">2.34× faster</span></td><td><span class="benchmark-time">807.41 us</span> <span class="benchmark-runner">pyproj</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">geodesic distance</span> <small class="benchmark-detail">10k WGS84 pairs †</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">8.26 ms</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">8.32 ms</span> <span class="benchmark-runner">pyproj</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">geodesic destination</span> <small class="benchmark-detail">10k WGS84 destinations †</small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.53 ms</span> <span class="benchmark-status">1.22× faster</span></td><td><span class="benchmark-time">4.43 ms</span> <span class="benchmark-runner">pyproj + Shapely + GeoPandas</span> </td></tr>
</tbody>
</table>

### Discrete global grids

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">geohash encode tokens</span> <small class="benchmark-detail">10k WGS84 points / precision 6 </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">605.88 us</span> <span class="benchmark-status">6.84× faster</span></td><td><span class="benchmark-time">4.20 ms</span> <span class="benchmark-runner">pygeohash</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">h3 cover center Brazil</span> <small class="benchmark-detail">Brazil res5 / 32,260 cells </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">13.51 ms</span> <span class="benchmark-status">2.35× faster</span></td><td><span class="benchmark-time">32.13 ms</span> <span class="benchmark-runner">h3-py</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">h3_compact</span> <small class="benchmark-detail">32,260 to 1,732 cells </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">477.88 us</span> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">487.70 us</span> <span class="benchmark-runner">h3-py</span> </td></tr>
</tbody>
</table>

### Spatial index

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">one-shot join within</span> <small class="benchmark-detail">10k POIs x 217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">44.90 ms</span> <span class="benchmark-status">1.41× slower</span></td><td><span class="benchmark-time">31.63 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index join within</span> <small class="benchmark-detail">10k POIs x 217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">14.75 ms</span> <span class="benchmark-status">2.10× faster</span></td><td><span class="benchmark-time">31.27 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index candidates</span> <small class="benchmark-detail">1k queries x 10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.46 ms</span> <span class="benchmark-status">3.76× faster</span></td><td><span class="benchmark-time">5.48 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index nearest k=1</span> <small class="benchmark-detail">1k non-intersecting queries x 10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.20 ms</span> <span class="benchmark-status">2.42× faster</span></td><td><span class="benchmark-time">5.34 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">exact rectangle query: packed points</span> <small class="benchmark-detail">256 viewports / 10k projected points / varied hit counts </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.38 ms</span> <span class="benchmark-status">1.21× faster</span></td><td><span class="benchmark-time">1.70 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: mixed geometries</span> <small class="benchmark-detail">256 viewports / 10k projected points, lines, polygons, and holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">6.96 ms</span> <span class="benchmark-status">3.69× slower</span></td><td><span class="benchmark-time">1.89 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">exact rectangle query: geographic geometries</span> <small class="benchmark-detail">256 viewports / 10k mixed EPSG:4326 geometries away from seams and poles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">7.78 ms</span> <span class="benchmark-status">4.30× slower</span></td><td><span class="benchmark-time">1.83 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">index build</span> <small class="benchmark-detail">10k polygons </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">627.46 us</span> <span class="benchmark-status">2.10× faster</span></td><td><span class="benchmark-time">1.32 ms</span> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

† geodesic/ellipsoidal metric.
‡ gometry uses its admitted in-core projection path; this is not a claim about arbitrary CRS transforms.

<!-- --8<-- [end:comparisons] -->

<!-- --8<-- [start:references] -->

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">ellipsoidal chart-edge country area</span> <small class="benchmark-detail">217 countries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">166.90 ms</span> <span class="benchmark-status">1.27× slower</span></td><td><span class="benchmark-time">131.97 ms</span> <span class="benchmark-runner">Python/NumPy quadrature reference</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">ellipsoidal chart-edge exterior length</span> <small class="benchmark-detail">1,034 exteriors / 16,135 vertices </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.50 ms</span> <span class="benchmark-status">68.55× faster</span></td><td><span class="benchmark-time">171.40 ms</span> <span class="benchmark-runner">Python/NumPy quadrature reference</span> </td></tr>
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
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">64 geometries</span> <small class="benchmark-detail">64 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">690.81 us</span> <small class="benchmark-detail">10.79 us / input geometry</small> <span class="benchmark-status">2.54× faster</span></td><td><span class="benchmark-time">1.78 ms</span> <small class="benchmark-detail">27.84 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">256 geometries</span> <small class="benchmark-detail">256 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">3.18 ms</span> <small class="benchmark-detail">12.43 us / input geometry</small> <span class="benchmark-status">2.28× faster</span></td><td><span class="benchmark-time">7.27 ms</span> <small class="benchmark-detail">28.39 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1024 geometries</span> <small class="benchmark-detail">1024 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">13.85 ms</span> <small class="benchmark-detail">13.53 us / input geometry</small> <span class="benchmark-status">2.03× faster</span></td><td><span class="benchmark-time">28.34 ms</span> <small class="benchmark-detail">27.67 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (64 → 1024): gometry median 20.048x, comparator median 15.902x, gometry/work 1.253x, comparator/work 0.994x.

#### Tile cover by output size

Dimension `grid-output-count` in `tiles`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">990 tiles</span> <small class="benchmark-detail">990 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">20.47 us</span> <small class="benchmark-detail">20.7 ns / tile</small> <span class="benchmark-status">11.41× faster</span></td><td><span class="benchmark-time">235.22 us</span> <small class="benchmark-detail">237.6 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">3960 tiles</span> <small class="benchmark-detail">3960 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">62.69 us</span> <small class="benchmark-detail">15.8 ns / tile</small> <span class="benchmark-status">15.75× faster</span></td><td><span class="benchmark-time">987.73 us</span> <small class="benchmark-detail">249.4 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">15340 tiles</span> <small class="benchmark-detail">15340 output-tiles </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">212.62 us</span> <small class="benchmark-detail">13.9 ns / tile</small> <span class="benchmark-status">19.91× faster</span></td><td><span class="benchmark-time">4.21 ms</span> <small class="benchmark-detail">274.3 ns / tile</small> <span class="benchmark-runner">Mercantile</span> </td></tr>
</tbody>
</table>

Endpoint growth (990 → 15340): gometry median 10.386x, comparator median 17.887x, gometry/work 0.670x, comparator/work 1.154x.

#### Index queries as the index grows

Dimension `indexed-polygons` in `polygons`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">86.53 us</span> <small class="benchmark-detail">86.5 ns / candidate pair</small> <span class="benchmark-status">1.25× slower</span></td><td><span class="benchmark-time">69.12 us</span> <small class="benchmark-detail">69.1 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">5k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">96.63 us</span> <small class="benchmark-detail">96.6 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">97.45 us</span> <small class="benchmark-detail">97.4 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">10k polygons</span> <small class="benchmark-detail">1000 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">97.49 us</span> <small class="benchmark-detail">97.5 ns / candidate pair</small> <span class="benchmark-status">1.14× faster</span></td><td><span class="benchmark-time">111.25 us</span> <small class="benchmark-detail">111.3 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 10k): gometry median 1.127x, comparator median 1.610x, gometry/work 1.127x, comparator/work 1.610x.

#### Index queries by hit rate

Dimension `queries-with-a-match` in `fraction`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1pct fraction</span> <small class="benchmark-detail">10 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">23.93 us</span> <small class="benchmark-detail">2.39 us / candidate pair</small> <span class="benchmark-status">1.78× slower</span></td><td><span class="benchmark-time">13.86 us</span> <small class="benchmark-detail">1.39 us / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">10pct fraction</span> <small class="benchmark-detail">100 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">29.51 us</span> <small class="benchmark-detail">295.1 ns / candidate pair</small> <span class="benchmark-status">1.32× slower</span></td><td><span class="benchmark-time">22.32 us</span> <small class="benchmark-detail">223.2 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">50pct fraction</span> <small class="benchmark-detail">500 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">58.55 us</span> <small class="benchmark-detail">117.1 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">59.98 us</span> <small class="benchmark-detail">120.0 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1pct → 50pct): gometry median 2.447x, comparator median 4.326x, gometry/work 0.049x, comparator/work 0.087x.

#### Index queries returning more candidates

Dimension `candidate-pairs-per-query` in `pairs/query`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-slower"><th scope="row"><span class="benchmark-api">1 pairs/query</span> <small class="benchmark-detail">1 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">972.7 ns</span> <small class="benchmark-detail">972.7 ns / candidate pair</small> <span class="benchmark-status">1.49× slower</span></td><td><span class="benchmark-time">647.4 ns</span> <small class="benchmark-detail">647.4 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">10 pairs/query</span> <small class="benchmark-detail">10 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">992.0 ns</span> <small class="benchmark-detail">99.2 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">967.2 ns</span> <small class="benchmark-detail">96.7 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-inconclusive"><th scope="row"><span class="benchmark-api">100 pairs/query</span> <small class="benchmark-detail">100 candidate-pairs </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">1.59 us</span> <small class="benchmark-detail">15.9 ns / candidate pair</small> <span class="benchmark-status">Inconclusive</span></td><td><span class="benchmark-time">1.82 us</span> <small class="benchmark-detail">18.2 ns / candidate pair</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1 → 100): gometry median 1.631x, comparator median 2.819x, gometry/work 0.016x, comparator/work 0.028x.

#### Union of disjoint polygons

Dimension `input-geometries` in `geometries`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k geometries</span> <small class="benchmark-detail">1000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.61 ms</span> <small class="benchmark-detail">2.61 us / input geometry</small> <span class="benchmark-status">10.98× faster</span></td><td><span class="benchmark-time">28.68 ms</span> <small class="benchmark-detail">28.68 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">4k geometries</span> <small class="benchmark-detail">4000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">11.75 ms</span> <small class="benchmark-detail">2.94 us / input geometry</small> <span class="benchmark-status">12.65× faster</span></td><td><span class="benchmark-time">146.94 ms</span> <small class="benchmark-detail">36.73 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">16k geometries</span> <small class="benchmark-detail">16000 input-geometries </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">49.94 ms</span> <small class="benchmark-detail">3.12 us / input geometry</small> <span class="benchmark-status">14.44× faster</span></td><td><span class="benchmark-time">728.01 ms</span> <small class="benchmark-detail">45.50 us / input geometry</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 16k): gometry median 19.105x, comparator median 25.382x, gometry/work 1.194x, comparator/work 1.586x.

#### Intersection with many holes

Dimension `holes` in `holes`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">250 holes</span> <small class="benchmark-detail">250 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">541.12 us</span> <small class="benchmark-detail">2.16 us / hole</small> <span class="benchmark-status">1.41× faster</span></td><td><span class="benchmark-time">768.20 us</span> <small class="benchmark-detail">3.07 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k holes</span> <small class="benchmark-detail">1000 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">2.43 ms</span> <small class="benchmark-detail">2.43 us / hole</small> <span class="benchmark-status">1.25× faster</span></td><td><span class="benchmark-time">3.01 ms</span> <small class="benchmark-detail">3.01 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">4k holes</span> <small class="benchmark-detail">4000 holes </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">9.79 ms</span> <small class="benchmark-detail">2.45 us / hole</small> <span class="benchmark-status">1.29× faster</span></td><td><span class="benchmark-time">12.65 ms</span> <small class="benchmark-detail">3.16 us / hole</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (250 → 4k): gometry median 18.096x, comparator median 16.470x, gometry/work 1.131x, comparator/work 1.029x.

#### Point-in-polygon queries

Dimension `probe-count` in `probes`; each point is an independent paired workload.

<table class="benchmark-table">
<thead><tr><th scope="col">API / input</th><th scope="col">gometry</th><th scope="col">Comparator</th></tr></thead>
<tbody>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">1k probes</span> <small class="benchmark-detail">1000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">32.56 us</span> <small class="benchmark-detail">32.6 ns / query</small> <span class="benchmark-status">2.05× faster</span></td><td><span class="benchmark-time">67.67 us</span> <small class="benchmark-detail">67.7 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">10k probes</span> <small class="benchmark-detail">10000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">773.04 us</span> <small class="benchmark-detail">77.3 ns / query</small> <span class="benchmark-status">2.01× faster</span></td><td><span class="benchmark-time">1.55 ms</span> <small class="benchmark-detail">155.0 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
<tr class="benchmark-faster"><th scope="row"><span class="benchmark-api">100k probes</span> <small class="benchmark-detail">100000 predicate-results </small></th><td><span class="benchmark-mobile-label">gometry</span> <span class="benchmark-time">4.72 ms</span> <small class="benchmark-detail">47.2 ns / query</small> <span class="benchmark-status">1.86× faster</span></td><td><span class="benchmark-time">9.27 ms</span> <small class="benchmark-detail">92.7 ns / query</small> <span class="benchmark-runner">Shapely</span> </td></tr>
</tbody>
</table>

Endpoint growth (1k → 100k): gometry median 144.932x, comparator median 137.028x, gometry/work 1.449x, comparator/work 1.370x.

<!-- --8<-- [end:scaling] -->

<!-- --8<-- [start:evidence] -->

[Raw measurements](../assets/benchmark-runs/b050e93edccfcd081372dec5a59d383e9ef4da16ccecfc1890be6188b7623813/run.json) · [Build provenance](../assets/benchmark-runs/b050e93edccfcd081372dec5a59d383e9ef4da16ccecfc1890be6188b7623813/receipts/index.json).

<!-- --8<-- [end:evidence] -->
