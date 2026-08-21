---
description: H3, S2, geohash, and XYZ-tile grids in gometry plus the point geocodes (plus codes, OSM shortlinks), the spherical model, and the space-filling-curve keys that order cells — typed cells and explicit cell-selection rules.
---

# Grids & geocodes

Discrete global grids turn geometry into compact cell IDs for grouping and
candidate filtering before exact geometry.

gometry covers four grids in one API — [H3](https://h3geo.org/),
[S2](https://s2geometry.io/), [Geohash](https://en.wikipedia.org/wiki/Geohash),
and XYZ tiles — that other ecosystems split across separate packages:

- **H3** — Uber's hexagonal grid, addressed by *resolution* (0 coarse … 15 fine).
- **S2** — Google's quadrilateral grid on the sphere, addressed by *level*
  (0 coarse … 30 fine).
- **Geohash** — the base-32 lon/lat bisection code, addressed by
  *precision* (1 coarse … 12 fine).
- **XYZ tiles** — the slippy-map [Web Mercator](https://epsg.org/crs_3857/WGS-84-Pseudo-Mercator.html) grid, addressed by *zoom*
  (0 coarse … 29 fine).

All four share **one cell-array shape** and the same `cell_rule` semantics.
[`gm.Cell`][gometry.Cell] is the structural protocol every cell type satisfies.

Cover factories materialize cell keys. Keep the source geometry separately and
use the free `gm.*` predicates when an exact geometry question is needed.

## Cells from points

Point cells are constructed with [`gm.H3Cell(...)`][gometry.H3Cell] and
[`gm.h3_cells(...)`][gometry.h3_cells] (likewise
[`gm.S2Cell(...)`][gometry.S2Cell] / [`gm.s2_cells(...)`][gometry.s2_cells]). The scalar
constructor returns a typed cell object; the plural `cells(...)` function returns a
typed [`CellArray`][gometry.CellArray] (one cell per coordinate).

```python exec="on" source="block" result="text"
import gometry as gm

p = gm.Point(21.0, 52.0, crs=4326)

h = gm.H3Cell(p, resolution=8)
print("H3 cell:", h.id, "| resolution:", h.resolution)
print("parent (resolution 7):", h.parent(7).id, "| #children at resolution 9:", len(h.children(9)))

s = gm.S2Cell(p, level=14)
print("S2 cell token:", s.token, "| level:", s.level, "| id:", s.id)
print("parent (level 10) token:", s.parent(10).token)

```

Cell arrays include grouping helpers for “count by cell” analytics:

```python exec="on" source="block" result="text"
import gometry as gm

cells = gm.h3_cells([21.0, 21.0, 22.0], [52.0, 52.0, 52.5], resolution=7)
unique, counts = cells.value_counts()
print(list(zip(unique.token, counts.tolist())))

codes, uniques = cells.factorize()
print("codes:", codes.tolist(), "| uniques:", uniques.token)

```

Every cell class shares one uniform surface — `.token`, `.center`, `.polygon`,
`.neighbors`, `.area`, `.parent(...)`, `.children(...)`, and
cell-hierarchy `.contains(...)` / `.intersects(...)` — captured by the
[`gm.Cell`][gometry.Cell] protocol, so grid-system-agnostic
code type-checks against any of them. Each cell class is constructed directly —
`H3Cell(value)` / `S2Cell(value)` / `Tile(value)` — accepting a cell object,
a numeric id (where the system has one), or a token string. H3 topological
vertices and directed edges are separate scalar identities (`H3Vertex(value)`,
`H3Edge(value)`) with fixed arrays (`H3VertexArray`, `H3EdgeArray`) because
they do not have cell hierarchy or area/polygon operations. Each system adds
its own vocabulary on top: `H3Cell.resolution`,
`.grid_disk(k)` / `.grid_ring(k)` / `.grid_path(...)`; `S2Cell.level`.
Arbitrary [`CellArray`][gometry.CellArray] sets roll up and down with
`cells.compact()` / `cells.uncompact(depth)`.

For a tile coordinate, name its frame: use `Tile(lon=..., lat=..., zoom=...)`
for WGS84 coordinates or `Tile(x=..., y=..., zoom=...)` for XYZ indices.
`Tile(Point(...), zoom=...)` is the geometry form. A two-number positional call
is rejected because it cannot say which frame those numbers occupy.

!!! note "Scalar cells keep native depth names; CellArray uses generic depth"
    H3 says `resolution`, S2 says `level`, geohash says `precision`, and XYZ
    tiles say `zoom` — scalar cells keep those native names on constructors,
    `parent`, and `children`. A `CellArray` may be generic over the cell kind,
    so its hierarchy methods use one positional `depth` argument:
    `cells.parent(5)`, `cells.compact(5)`, and `cells.uncompact(7)`.

!!! note "`.center` and `.polygon` are properties; `parent`/`children` step by one by default"
    On both `H3Cell` and `S2Cell`, `.center` and `.polygon` are *properties* (no
    parentheses) returning a WGS84 [`Geometry`][gometry.Geometry]. For H3 and
    S2 it is a planar lon/lat chord proxy for display and interoperability, not
    the spherical cell boundary. `parent()` defaults to one
    step coarser and `children()` to one step finer; pass a target
    (`cell.parent(resolution)`, `cell.children(level)`) to jump straight to it. A
    maximum-depth cell yields an empty `CellArray` from `children()`; a depth-0 cell
    raises on `parent()`.

### The geometry behind a cell ID

A cell ID is shorthand for an area on the ground. Scalar cells expose `.polygon`,
and `CellArray.polygon` returns a whole batch:

```python exec="on" source="block" result="text"
import gometry as gm
cell = gm.H3Cell(gm.Point(21.0, 52.0, crs=4326), resolution=6)
poly = cell.polygon
print('cell', cell.id, '->', poly.geometry_type)
print('WKT:', poly.to_wkt()[:48], '...')
print('batch:', type(cell.grid_disk(1).polygon).__name__)

```

For H3 and S2 that polygon is a planar lon/lat chord proxy rather than the
spherical cell boundary. Adjacency and
local indexing are cell methods: `neighbors` lists the edge-adjacent ring,
`is_neighbor` tests one candidate, and `local_ij`/`cell_from_local_ij` move through
the local grid-algebra space around an anchor. Resolution metadata lives in the
H3-prefixed global family — [`gm.h3_pentagons`][gometry.h3_pentagons] and
[`gm.h3_base_cells`][gometry.h3_base_cells]:

```python exec="on" html="true"
from _figures import cell_polys, figure
import gometry as gm

cell = gm.H3Cell(gm.Point(21.0, 52.0, crs=4326), resolution=6)
print(figure([cell.polygon, *cell_polys(cell.children())], "cell.children() nested inside one H3 cell"))

```

```python exec="on" source="block" result="text"
import gometry as gm

cell = gm.H3Cell(gm.Point(21.0, 52.0, crs=4326), resolution=6)
neighbor = cell.neighbors[0]
print("adjacent:", cell.is_neighbor(neighbor), "| base cell:", cell.base_cell)
i, j = neighbor.local_ij(cell)
print("local ij:", (i, j), "->", cell.cell_from_local_ij(i, j) == neighbor)
print("pentagons at res 4:", len(gm.h3_pentagons(4)))

```

```python exec="on" html="true"
from _figures import cell_polys, figure
import gometry as gm

cell = gm.H3Cell(gm.Point(21.0, 52.0, crs=4326), resolution=6)
print(figure([cell.polygon, *cell_polys(cell.neighbors), cell.center], "neighbors around one H3 cell"))

```

## The spherical model

S2 uses a spherical model for cells and coverings, so the antimeridian and poles
do not create a longitude seam in cell selection. gometry exposes typed cells and
cell sets rather than a general spherical boolean-topology engine.

```python exec="on" html="true"
from _figures import cell_polys, figure
import gometry as gm
seam = gm.box(175.0, -5.0, -175.0, 5.0, crs=4326, wrap='split')
covering = gm.s2_cover(seam, level=None, max_cells=24)
print(figure([seam, *cell_polys(covering)], "s2.cover across the antimeridian — no seam"))

```

For planar *geometry* crossing ±180 (as opposed to cells), see
[Across the antimeridian](crs.md#across-the-antimeridian).

```python exec="on" source="block" result="text"
import gometry as gm

p = gm.Point(179.9, 0.5, crs=4326)
cell = gm.S2Cell(p, level=12)
print("S2 cell level:", cell.level, "| token:", cell.token)
print("parent level 8:", cell.parent(8).token)

poly = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
covering = gm.s2_cover(poly, level=10, max_cells=None)
print("covering cells:", len(covering))

```

!!! note "S2 covers cells and coverings, not boolean topology"
    gometry's S2 surface provides cells, cell hierarchies, and deterministic
    coverings — not a spherical intersection/union engine. Metric calculations use
    the geodesic model; planar topology uses a local projected CRS and the planar
    model.

## Covering an area

`gm.h3_cover(geom, resolution=...)` and `gm.s2_cover(geom, level=...)` return a
typed `CellArray` directly:

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cov = gm.h3_cover(area, resolution=5)

print("# cells:", len(cov))
print("first cell id:", cov[0].id)

```

### `cell_rule` is explicit semantics

The `cell_rule` argument selects which cells `cells` materializes. The same four
modes apply to H3, S2, geohash, and tiles. It defaults to `"overlap"`, the
complete-coverage superset used for candidate keys.

| `cell_rule` | A cell is included when… | Typical use |
| --- | --- | --- |
| `"center"` | the cell's **center** falls inside the geometry | h3-py-style polygon-to-cell selection; boundary cells may be omitted |
| `"within"` | the cell lies **entirely inside** the geometry | a *subset* — cells the area fully owns |
| `"overlap"` (default) | the cell **intersects** the geometry at any point (interior or boundary) | a *superset* — safe join/prefilter keys, never misses a true hit |
| `"bbox"` | the cell's **bounding box** overlaps the geometry | loosest candidate rule (for geohash & tiles a cell *is* its bbox, so same as `"overlap"`) |

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
for rule in ("within", "center", "overlap", "bbox"):
    cov = gm.h3_cover(area, resolution=5, cell_rule=rule)
    print(f"{rule:>8}: {len(cov):>3} cells")

```

The counts differ because the rules mean different things: `"within"` ⊆
`"center"` ⊆ `"overlap"` ⊆ `"bbox"` for the same geometry and resolution. The
selected cell sets form these nested sets:

```python exec="on" html="true"
from _figures import cell_polys, panels
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
print(panels([
    (rule, [area.exterior, *cell_polys(gm.h3_cover(area, resolution=5, cell_rule=rule))])
    for rule in ("within", "center", "overlap", "bbox")
]))

```

The returned cell set depends on `cell_rule`; exact geometry predicates require
the source geometry.

### Projected input is normalized to lon/lat

All four grid factories accept a **WGS 84 lon/lat interchange domain** (and
transform supported projected input into that domain first). The grids themselves
are not one model: H3 is an icosahedral discrete global grid, S2 a spherical
cube, geohash a lon/lat rectangle hierarchy, and tiles the Web Mercator XYZ
scheme. If you hand `gm.h3_cover` / `gm.s2_cover` a geometry in a supported
projected CRS, gometry transforms it to WGS 84 lon/lat first. WGS 84 and CRS-free
lon/lat pass through; other supported frames are reprojected before cover keys
are generated.

```python exec="on" source="block" result="text"
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
projected = area.to_crs(3857)
cov = gm.h3_cover(projected, resolution=5)
print('# cells:', len(cov))
print('projected candidate:', gm.covers(projected, gm.Point(21.0, 52.0, crs=4326).to_crs(3857)))

```

## Exact membership with the source geometry

The returned `CellArray` contains the cell keys selected by `cell_rule`; it does
not retain the source geometry or provide exact source-membership results. Keep
the source geometry and apply `gm.covers`, `gm.contains`, or `gm.intersects` to
refine cell candidates into exact predicate results.

```python exec="on" source="block" result="text"
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cells = gm.h3_cover(area, resolution=5)
points = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
print('covers:', gm.covers(area, points))
print('edge point:', gm.covers(area, gm.Point(20.0, 52.0, crs=4326)), '| strictly inside:', gm.contains(area, gm.Point(20.0, 52.0, crs=4326)))
print('raw lon/lat stream:', gm.intersects_xy(area, [21.0, 30.0], [52.0, 52.0]))

```

```python exec="on" html="true"
from _figures import figure
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cells = gm.h3_cover(area, resolution=5)
print(figure([*cells.polygon, area], 'H3 cells'))

```

!!! tip "Joins at scale"
    Use the default `"overlap"` cells as join keys (a superset never loses a
    candidate), join on cell IDs in your database or stream, then finish with
    `gm.covers(area, matched)` for the exact answer. For in-memory many-to-many joins,
    [`gm.join`][gometry.join] does both stages for you.

## From cells back to geometry

A cell ID is shorthand for a polygon, and a covering is a mesh of them. A
single-cell bound or a dissolved covering recovers geometry.

### The single cell that bounds a geometry

A covering returns *many* cells. A bounding-cell operation returns the **one** cell
that wholly contains a geometry, which serves as a coarse partition key or spatial
bucket.
Each grid spells it by return type: `gm.h3_bounding_cell` / `gm.s2_bounding_cell`
/ `gm.geohash_bounding_cell` return their cell type, and `gm.tile_bounding_cell`
returns a [`Tile`][gometry.Tile].

```python exec="on" source="block" result="text"
import gometry as gm

berlin = gm.box(13.0, 52.3, 13.7, 52.7, crs=4326)

print("h3     :", gm.h3_bounding_cell(berlin), "res", gm.h3_bounding_cell(berlin).resolution)
print("s2     :", gm.s2_bounding_cell(berlin), "level", gm.s2_bounding_cell(berlin).level)
print("geohash:", gm.geohash_bounding_cell(berlin).token)
print("tile   :", gm.tile_bounding_cell(berlin))

```

Each returns the *deepest* single cell whose extent still covers the whole input
(for H3, the highest-resolution cell that still contains every corner — res 15 for
a point-sized input, coarser only when no finer single cell fits). Pass a geometry,
an array, or a raw `(minx, miny, maxx, maxy)` bounds tuple.

### Dissolving a covering back to one outline

To recover one geometry from a covering, `to_polygon()` dissolves it into one
outline; shared cell edges cancel, so the interior costs no geometry:

```python exec="on" source="block" result="text"
import gometry as gm

cov = gm.h3_cover(gm.box(20.0, 51.0, 22.0, 53.0, crs=4326), resolution=5)
print("dissolved:", cov.to_polygon().geometry_type)

```

When you have a *detached* list of cell IDs rather than a coverage — rows from a
database, the output of `compact`/`union`, a hand-built set — build a
`CellArray` and call `cells.to_polygon()` (mixed resolutions allowed, so
compacted sets work as-is):

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cells = gm.h3_cover(area, resolution=6).compact()
print(cells.to_polygon().geometry_type, "from", len(cells), "compacted cells")

```

Floor-bounded compaction keeps detail where you ask it to stop:
`cells.compact(4)` for H3 resolution 4, `cells.compact(9)` for S2 level 9,
`cells.compact(5)` for geohash precision 5, and `cells.compact(8)` for tile
zoom 8.

!!! warning "`cov.to_polygon()` is not `coverage_*`"
    `to_polygon()` dissolves *grid cells* — an approximate outline of the
    source under the grid. The polygonal-coverage operators
    ([`coverage_union`][gometry.coverage_union], `coverage_simplify`,
    `coverage_clean`) validate and repair an *arbitrary source polygon fabric*
    (parcels, admin boundaries) with edge-matched interfaces. A DGGS
    Cell arrays are a different domain: same-resolution H3/S2 tessellations do
    share boundaries, but hierarchy, compact/uncompact, and cell-set algebra
    already own that problem — use those, not `coverage_*`, to merge cells.

## Cell-set algebra (all four grids)

Every grid family exposes the same **factories + set-algebra** shape: prefixed
`union`, `intersection`, and `difference` functions on cell sets (H3, S2,
geohash, and tiles). Hierarchy behavior still differs per system (S2 range nesting,
H3 resolution parents, geohash/tile prefix parents). H3 cell sets require
hierarchy-aware free functions rather than plain Python `set` arithmetic.

**H3 resolution normalization** (`gm.h3_union` / `gm.h3_intersection` /
`gm.h3_difference`): inputs may mix resolutions. The result is cell-ID algebra
under the compact contract — sorted, with descendants absorbed by ancestors and
complete sibling groups merged into parents — so mixed-resolution operands are
normalized rather than rejected. This is identity algebra (an H3 child's *geometry*
does not nest exactly inside its parent, but its id does). S2 algebra is
range-nesting exact; geohash/tile algebra is prefix-based.

```python exec="on" source="block" result="text"
import gometry as gm

a = gm.h3_cover(gm.box(20.0, 51.0, 22.0, 53.0, crs=4326), resolution=5)
b = gm.h3_cover(gm.box(21.0, 52.0, 23.0, 54.0, crs=4326), resolution=5)
print("h3 intersection:", len(gm.h3_intersection(a, b)))
mixed = gm.h3_union(a, gm.h3_cover(gm.box(21.0, 52.0, 23.0, 54.0, crs=4326), resolution=6))
print("h3 mixed-res union resolutions:", sorted({c.resolution for c in mixed}))
print("s2 union sample:", len(gm.s2_union(
    gm.s2_cover(gm.box(20.0, 51.0, 21.0, 52.0, crs=4326), level=8),
    gm.s2_cover(gm.box(21.0, 52.0, 22.0, 53.0, crs=4326), level=8),
)))

```

## S2 coverings: budget and multi-resolution

S2 coverings are multi-resolution, and S2 cell IDs nest as ranges. These
properties provide a `cover` budget and whole-set algebra without touching
geometry.

### A budget and a rule

`gm.s2_cover(geom, ...)` materializes cells at the requested `level`, with the
same `cell_rule` and `max_cells` controls as the other grid factories:

```python exec="on" source="block" result="text"
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cov = gm.s2_cover(area, level=8, max_cells=64)
print('# cells:', len(cov))
print('levels present:', sorted({c.level for c in cov}))

```

`max_cells` is the hard output and allocation cap, defaulting to `1_000_000`;
pass `max_cells=None` for an unlimited cover (bounded only by memory). A fixed
`level=` cover cannot coarsen, so it raises when the cover exceeds the cap. An
adaptive cover (using `min_level`/`max_level`, or omitting `level`) stops
refinement and retains coarser cells when its children would exceed the cap, so
the result has at most `max_cells` cells. It raises only when even the
admissible starting cover cannot fit or another coverage error occurs.
`target_cells` remains the adaptive refinement target; it is distinct from the
hard cap. The cells are a spatial key materialized according to `cell_rule`;
exact geometry predicates remain free functions such as `gm.covers` and
`gm.contains_xy`.

### Cell-set algebra

S2 cell ids nest as ranges, so whole-set operations stay exact without touching
geometry: [`gm.s2_union`][gometry.s2_union] absorbs descendants and merges complete
sibling groups, [`gm.s2_intersection`][gometry.s2_intersection] keeps the finer cell
of an ancestor/descendant overlap, and [`gm.s2_difference`][gometry.s2_difference]
splits partially-covered cells exactly (`gm.S2Cell` takes a `Point` or a
bare ``lon, lat`` pair):

```python exec="on" source="block" result="text"
import gometry as gm
cell = gm.S2Cell(13.4, 52.5, level=10)
children = cell.children()
print('union:       ', list(gm.s2_union(children[:2], children[2:])) == [cell])
print('intersection:', [c.level for c in gm.s2_intersection([cell], children[:2])])
print('difference:  ', len(gm.s2_difference([cell], children[:1])), 'cells remain')

```

[`gm.h3_union`][gometry.h3_union] / [`gm.h3_intersection`][gometry.h3_intersection]
/ [`gm.h3_difference`][gometry.h3_difference] normalize mixed resolutions under
the compact contract. A fixed-resolution `set` of raw IDs performs identity
equality without parent/child absorption.

## Geohash and XYZ tiles

Geohash and tiles are *rectangular* grids — every cell is an exact lon/lat
rectangle, and their cover keys use those rectangles rather than a separate
bounding-box approximation. They wear the same API as H3 and S2.

```python exec="on" source="block" result="text"
import gometry as gm
point = gm.Point(13.4, 52.5, crs=4326)
print('geohash:', gm.GeohashCell(point, precision=6).token)
print('tile:   ', gm.Tile(point, zoom=12).token)
area = gm.box(13.2, 52.4, 13.6, 52.6, crs=4326)
cov = gm.geohash_cover(area, precision=5, cell_rule='overlap')
print('# geohash cells:', len(cov))
print('covers a point:', gm.covers(area, gm.Point(13.4, 52.5, crs=4326)))
tiles = gm.tile_cover(area, zoom=10)
print('# tiles:', len(tiles), 'first quadkey:', tiles[0].token)

```

A geohash or tile cover is built as a **uniform single-depth tiling** —
    every cell is the same precision/zoom, which makes the returned `CellArray` a
    uniform spatial key. Exact predicates test the source geometry supplied by the caller;
token lookup alone is not the answer. Cell-set methods operate on the returned
`CellArray` directly.

`GeohashCell` and `Tile` carry the full cell surface — `center`,
`polygon`, `area`, `parent`/`children`/`neighbors`, ordering, and pickle —
and `CellArray` adds the set operations (`cells.compact()` /
`cells.uncompact(depth)`) while H3 vertex/edge arrays expose only their valid
topological surfaces. `Tile` keeps the quadkey constructor
(`gm.Tile('0313102310')`). Tiles reject latitudes outside the Web Mercator
domain (±85.0511°), the limit where the projection stays finite; they do not
clamp coordinates.

## Aggregate by cell, then refine

Cell arrays provide global bucketing, while a spatial index provides exact
polygon refinement. The grid step groups rows before the geometry predicate.

```python exec="on" source="block" result="text"
import gometry as gm

lons = [2.3479, 2.3490, 2.3600, 2.3470]
lats = [48.8589, 48.8590, 48.8650, 48.8580]
points = gm.points(lons, lats, crs=4326)

h3_cells = gm.h3_cells(lons, lats, resolution=8)
unique_h3, counts = h3_cells.value_counts()
codes, unique_for_codes = h3_cells.factorize()

s2_cells = gm.s2_cells(lons, lats, level=12)
unique_s2, _ = s2_cells.value_counts()

districts = gm.GeometryArray([
    gm.box(2.34, 48.85, 2.35, 48.86, crs=4326),
    gm.box(2.35, 48.86, 2.37, 48.87, crs=4326),
])
point_to_district = gm.SpatialIndex(districts).query(points, predicate="within")

print("H3 counts:", list(zip(unique_h3.token, counts.tolist())))
print("H3 codes:", codes.tolist(), "unique rows:", len(unique_for_codes))
print("unique S2 cells:", len(unique_s2))
print("refined district rows:", point_to_district.values.tolist())

```

`value_counts()` is the direct aggregation primitive. `factorize()` is the
handoff shape for pandas/NumPy-style grouping: `codes[i]` points at a row in the
returned unique `CellArray`.

Cell equality is a candidate join, not polygon truth. After exporting cells
to a DataFrame or lakehouse, refine matched rows with `SpatialIndex.query`,
`gm.join`, or the corresponding exact predicate against the original geometry.

## Ordering & locality

A [space-filling curve](https://en.wikipedia.org/wiki/Hilbert_curve) threads a
single line through a 2-D grid so that points close on the line are close in
space. Encoding each geometry as its position along that curve produces an
**integer key whose sort order preserves locality** for [GeoParquet](https://geoparquet.org/)
row-group packing, spatial shuffles, and index builds.

gometry exposes one keying and sorting API with an explicit `curve=` choice.
S2 and tile cell identifiers also encode locality-preserving orderings:

- **Hilbert** — the default `curve='hilbert'`, a recursive locality-preserving
  order.
- **Morton (Z-order)** — `curve='morton'`. Cheaper to compute (bit
  interleave), with one diagonal jump per quadrant — the [Z-order curve](https://en.wikipedia.org/wiki/Z-order_curve).

### Keys from geometries

[`spatial_key`][gometry.Geometry.spatial_key] discretizes a geometry's
bounding-box center onto a `2**level × 2**level` grid over `bounds` and returns
its distance along the selected curve. Keys that are numerically close imply
locations that are close.

```python exec="on" source="block" result="text"
import gometry as gm
cities = gm.GeometryArray([gm.Point(-0.13, 51.51), gm.Point(2.35, 48.86), gm.Point(-73.94, 40.72), gm.Point(-74.01, 40.71)])
keys = cities.spatial_key(level=16)
print(keys)

```

The default `level=16` is **gometry's default only**. GeoParquet does not define a
Hilbert level or a canonical key algorithm. Bit-identical ordering across tools
requires matching bounds, level, quantization, coordinate convention, and
algorithm. Another writer may use different defaults.

### Sorting an array

[`sort_by_spatial_key`][gometry.GeometryArray.sort_by_spatial_key] returns the
array reordered by the selected key, so nearby geometries land in nearby rows
— the packing that makes row-group min/max filters and block reads effective.

```python exec="on" source="block" result="text"
import gometry as gm

grid = gm.GeometryArray([gm.Point(x, y) for y in range(4) for x in range(4)])
ordered = grid.sort_by_spatial_key(level=8)
print([(round(p.x), round(p.y)) for p in ordered])

```

```python exec="on" html="true"
from _figures import curve_through, panels
import gometry as gm

grid = gm.GeometryArray([gm.Point(x, y) for y in range(8) for x in range(8)])
print(panels([
    ("sort_by_spatial_key()", curve_through(grid, curve="hilbert", level=3)),
    ("sort_by_spatial_key(curve='morton')", curve_through(grid, curve="morton", level=3)),
]))

```

The sorted order can be written to GeoParquet or passed to `gm.SpatialIndex`; each
block then contains a spatially compact run of geometries.

### The grids are curves too

S2, tile, and geohash identifiers encode these orderings, so a separate sort is unnecessary
when data already uses those cells:

- **[S2](https://s2geometry.io/) cell ids are Hilbert order.** `sorted(s2_cells)` groups spatial
  neighbors, because an S2 id is the cell's position along the face Hilbert
  curve. The cell-set algebra relies on it.
- **Tile ids and [quadkeys](https://learn.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system) are Morton order.** A `Tile`'s packed id interleaves
  `x`/`y` so it sorts in quadkey order; `tile.morton` is that index directly.
- **[Geohash](https://en.wikipedia.org/wiki/Geohash) token order is a Z-ish curve.** Base-32 tokens sort
  lexicographically into a locality-preserving order — which is exactly why a
  `GeohashCell`'s token carries that ordering directly.

Geometry rows use `spatial_key()` for GeoParquet or index packing, with
`curve='morton'` selecting Morton order. Cell identities carry their grid's
ordering directly.

## Point geocodes

[Open Location Code](https://github.com/google/open-location-code) (plus codes)
and OSM shortlinks are compact point codes rather than cell systems. gometry
exposes them as direct, codec-named functions:

```python exec="on" source="block" result="text"
import gometry as gm

# Plus codes: the offline-friendly address replacement.
code = gm.pluscode_encode(8.628, 47.366)
print("plus code:", code)
print("cell:     ", gm.pluscode_polygon(code).bounds)

# OSM shortlink — the code in https://osm.org/go/... URLs.
link = gm.osm_shortlink_encode(13.365, 52.5077, zoom=17)
print("shortlink:", link)
print("decoded:  ", gm.osm_shortlink_location(link))

```

`pluscode` also accepts a `Point` or array (CRS-aware), and
`pluscode_shorten` / `pluscode_recover` encode and recover short codes against a
reference location. `osm_shortlink_location` accepts the legacy `@` spelling.

Coming from h3-py or s2sphere? See [Migrating](../migrating/index.md#coming-from-h3-py-s2sphere).

## See also

- [Spatial indexing](indexing.md) — candidate vs exact refine on geometry, and
  rebuilding an index after a spatial sort.
- [The mental model](../get-started/mental-model.md) — the three operation models.
- [API: h3_cover][gometry.h3_cover] · [s2_cover][gometry.s2_cover] ·
  [H3Cell][gometry.H3Cell] · [S2Cell][gometry.S2Cell] ·
  [SpatialIndex][gometry.SpatialIndex] ·
  [Geometry.spatial_key][gometry.Geometry.spatial_key] ·
  [GeometryArray.sort_by_spatial_key][gometry.GeometryArray.sort_by_spatial_key]
