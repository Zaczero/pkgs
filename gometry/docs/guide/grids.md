---
description: H3, S2, geohash, and XYZ-tile grids in gometry plus the point geocodes (plus codes, OSM shortlinks), the spherical model, and the space-filling-curve keys that order cells — typed cells, rule-exact tilings, and coverages that answer exact membership without a separate refine step.
---

# Grids & geocodes

Discrete global grids turn geometry into compact cell IDs — the backbone of
geospatial analytics: group-by-cell aggregation, cheap candidate filtering
before exact geometry, heatmaps, and lakehouse partitioning. This page covers
the four grids gometry unifies under one API (H3, S2, geohash, XYZ tiles), the
point geocodes (plus codes, OSM shortlinks), the spherical model that makes
global covering seam-safe, the space-filling-curve keys that give cells their
locality-preserving order, and the aggregate-then-refine workflow. Read the cell
and coverage sections first — learn the H3 surface and the other three follow —
then reach for ordering and geocodes as needed.

gometry covers four grids in one API — [H3](https://h3geo.org/),
[S2](https://s2geometry.io/), [Geohash](https://en.wikipedia.org/wiki/Geohash),
and XYZ tiles — that other ecosystems split across separate packages:

- **H3** — Uber's hexagonal grid, addressed by *resolution* (0 coarse … 15 fine).
- **S2** — Google's quadrilateral grid on the sphere, addressed by *level*
  (0 coarse … 30 fine).
- **Geohash** — the classic base-32 lon/lat bisection code, addressed by
  *precision* (1 coarse … 12 fine).
- **XYZ tiles** — the slippy-map [Web Mercator](https://epsg.org/crs_3857/WGS-84-Pseudo-Mercator.html) grid, addressed by *zoom*
  (0 coarse … 29 fine).

All four share **one cell and coverage shape**: the same `cell_rule` semantics,
the same hierarchy moves, the same exact membership predicates. Learn the H3
surface and you already know the other three — [`gm.Cell`][gometry.Cell] is the
structural protocol every cell type satisfies.

**A coverage is an exact region predicate plus the cell keys that accelerate and
shard it.** The cells answer the *cell*
question exactly (which cells satisfy your `cell_rule`); the membership methods
answer the *geometry* question exactly (is this candidate really in the area) —
and you never have to remember a second call to get the exact answer.

## Cells from points

The simplest grid operation: which cell contains a point? Use
[`gm.H3Cell(...)`][gometry.H3Cell] and [`gm.h3_cells(...)`][gometry.h3_cells] (likewise
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

Cell arrays include first-class grouping helpers for “count by cell” analytics:

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
its own vocabulary on top: `H3Cell.resolution`, `.children_count(resolution)`,
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

A cell ID is shorthand for an area on the ground. Convert one back to geometry
to sanity-check it before partitioning a dataset — scalar cells expose `.polygon`,
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
local indexing are first-class on the cell: `neighbors` lists the edge-adjacent ring,
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

Rendering a cell together with its ring of neighbors (`grid_disk`) shows the
hexagonal tiling:

```python exec="on" html="true"
from _figures import cell_polys, figure
import gometry as gm

cell = gm.H3Cell(gm.Point(21.0, 52.0, crs=4326), resolution=6)
print(figure([cell.polygon, *cell_polys(cell.neighbors), cell.center], "neighbors around one H3 cell"))

```

## The spherical model

The spherical model is geometry on a **sphere**, where there are no projection
discontinuities and the antimeridian is not special. gometry exposes this through **S2** —
typed cells and exact-classified coverings — rather than as a general spherical boolean-topology
engine. The value is global correctness: an S2 cell or covering behaves the same near the
antimeridian or the poles as it does over the equator, which is exactly where planar
reasoning and projected frames break down.

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
    gometry's S2 surface provides cells, cell hierarchies, and exact
    coverings — not a spherical intersection/union engine. For exact metric truth
    use the geodesic model; for exact planar topology, project into a local CRS
    and use the planar model.

## Covering an area

`gm.h3_cover(geom, resolution=...)` and `gm.s2_cover(geom, level=...)` return the coverage
directly. It is typed and self-describing:

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cov = gm.h3_cover(area, resolution=5)

print("cell_rule:", cov.cell_rule)   # which cells were materialized
print("resolution:", cov.resolution)
print("# cells:", len(cov.cells))
print("first cell id:", cov.cells[0].id)

```

### `cell_rule` is explicit semantics

The `cell_rule` argument decides which cells `cells` materializes — the same four
modes across H3, S2, geohash, and tiles, from strictest (fewest cells) to loosest.
It defaults to `"overlap"` — the complete-coverage superset, the safe choice for
candidate keys — but pick it deliberately: different rules answer different questions.

| `cell_rule` | A cell is included when… | Typical use |
| --- | --- | --- |
| `"center"` | the cell's **center** falls inside the geometry | h3-py-style polygon-to-cell selection; boundary cells may be omitted |
| `"within"` | the cell lies **entirely inside** the geometry | a *subset* — cells the area fully owns |
| `"overlap"` (default) | the cell **intersects** the geometry at any point (interior or boundary) | a *superset* — safe join/prefilter keys, never misses a true hit |
| `"bbox"` | the cell's **bounding box** overlaps the geometry | loosest/fastest (for geohash & tiles a cell *is* its bbox, so same as `"overlap"`) |

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
for rule in ("within", "center", "overlap", "bbox"):
    cov = gm.h3_cover(area, resolution=5, cell_rule=rule)
    print(f"{rule:>8}: {len(cov.cells):>3} cells")

```

The counts differ because the rules mean different things: `"within"` ⊆
`"center"` ⊆ `"overlap"` ⊆ `"bbox"` for the same geometry and resolution. The cells
(grey) over the source box (outline) make the difference visible:

```python exec="on" html="true"
from _figures import cell_polys, panels
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
print(panels([
    (rule, [area.exterior, *cell_polys(gm.h3_cover(area, resolution=5, cell_rule=rule))])
    for rule in ("within", "center", "overlap", "bbox")
]))

```

The rule shapes **only the visible cells** — never the membership answers below.

### Projected input is normalized to lon/lat

All four grid factories accept a **WGS 84 lon/lat interchange domain** (and
transform supported projected input into that domain first). The grids themselves
are not one model: H3 is an icosahedral discrete global grid, S2 a spherical
cube, geohash a lon/lat rectangle hierarchy, and tiles the Web Mercator XYZ
scheme. If you hand `gm.h3_cover` / `gm.s2_cover` a geometry in a supported
projected CRS, gometry transforms it to WGS 84 lon/lat first, so you do
not have to remember to `to_crs(4326)` yourself — and membership candidates follow
the same policy (WGS 84 and CRS-free lon/lat pass through, anything else is
reprojected):

```python exec="on" source="block" result="text"
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
projected = area.to_crs(3857)
cov = gm.h3_cover(projected, resolution=5)
print('# cells:', len(cov.cells))
print('projected candidate:', cov.covers(gm.Point(21.0, 52.0, crs=4326).to_crs(3857)))

```

## Exact membership on a coverage object

Cells never align with a geometry's boundary, so cell membership alone can never
answer "is this point really in my area". gometry's coverages answer that question
themselves, exactly, with the same predicate verbs the rest of the library uses:

The coverage can do this because it retains its source geometry. If you export
`.cells` as join keys, the source geometry is no longer part of the key table:
refine candidate matches against the original geometries before treating them as
exact.

```python exec="on" source="block" result="text"
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cov = gm.h3_cover(area, resolution=5)
points = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
print('covers:', cov.covers(points))
print('edge point:', cov.covers(gm.Point(20.0, 52.0, crs=4326)), '| strictly inside:', cov.contains(gm.Point(20.0, 52.0, crs=4326)))
print('raw lon/lat stream:', cov.intersects_xy([21.0, 30.0], [52.0, 52.0]))

```

Membership is not a cell-index query. It delegates to the prepared
source-geometry predicate kernel — the same kernel as [`gm.contains_xy`][gometry.contains_xy]
and friends — so the answer is exact even when a visible cell only approximates
the boundary. `explain()` narrates that contract:

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
for line in gm.h3_cover(area, resolution=6).explain():
    print(line)

```

The classification itself is public inspection data: `interior_cells` are
certified fully inside, and `boundary_cells` are the fringe where cells and
geometry disagree. They are retained for rendering, debugging, and derived
coverage inputs — not as a query index:

```python exec="on" html="true"
from _figures import figure
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cov = gm.h3_cover(area, resolution=5)
interior = cov.interior_cells.polygon
fringe = cov.boundary_cells.polygon
print(figure([*list(interior), *(cell.exterior for cell in fringe)], 'interior_cells (filled) vs boundary_cells (outline)'))

```

!!! tip "Joins at scale"
    Use the default `"overlap"` cells as join keys (a superset never loses a
    candidate), join on cell IDs in your database or stream, then finish with
    `cov.covers(matched)` for the exact answer. For in-memory many-to-many joins,
    [`gm.join`][gometry.join] does both stages for you.

## From cells back to geometry

A cell ID is shorthand for a polygon, and a covering is a mesh of them. Two moves
recover geometry: the single cell that *bounds* an input, and dissolving a whole
covering back to one outline.

### The single cell that bounds a geometry

A covering returns *many* cells. The complement is the **one** cell that wholly
contains a geometry — useful as a coarse partition key or a quick spatial bucket.
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

Sometimes you want the cells *as a single geometry* — to render the covered region
or hand a polygon to another tool. `to_polygon()` dissolves the covering into one
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
cells = gm.h3_cover(area, resolution=6).cells.compact()
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
    `Coverage` is a different domain: same-resolution H3/S2 tessellations do
    share boundaries, but hierarchy, compact/uncompact, and cell-set algebra
    already own that problem — use those, not `coverage_*`, to merge cells.

## Cell-set algebra (all four grids)

Every grid family exposes the same **factories + set-algebra** shape: prefixed
`union`, `intersection`, and `difference` functions on cell sets (H3, S2,
geohash, and tiles). Hierarchy behavior still differs per system (S2 range nesting,
H3 resolution parents, geohash/tile prefix parents), but you do **not** drop
to plain Python `set` for H3 — use the hierarchy-aware free functions.

**H3 resolution normalization** (`gm.h3_union` / `gm.h3_intersection` /
`gm.h3_difference`): inputs may mix resolutions. The result is cell-ID algebra
under the compact contract — sorted, with descendants absorbed by ancestors and
complete sibling groups merged into parents — so mixed-resolution operands are
normalized rather than rejected. This is identity algebra (an H3 child's *geometry*
does not nest exactly inside its parent, but its id does). S2 algebra is
range-nesting exact (see below); geohash/tile algebra is prefix-based.

```python exec="on" source="block" result="text"
import gometry as gm

a = gm.h3_cover(gm.box(20.0, 51.0, 22.0, 53.0, crs=4326), resolution=5).cells
b = gm.h3_cover(gm.box(21.0, 52.0, 23.0, 54.0, crs=4326), resolution=5).cells
print("h3 intersection:", len(gm.h3_intersection(a, b)))
mixed = gm.h3_union(a, gm.h3_cover(gm.box(21.0, 52.0, 23.0, 54.0, crs=4326), resolution=6).cells)
print("h3 mixed-res union resolutions:", sorted({c.resolution for c in mixed}))
print("s2 union sample:", len(gm.s2_union(
    gm.s2_cover(gm.box(20.0, 51.0, 21.0, 52.0, crs=4326), level=8).cells,
    gm.s2_cover(gm.box(21.0, 52.0, 22.0, 53.0, crs=4326), level=8).cells,
)))

```

## S2 coverings: budget and multi-resolution

S2's covering algorithm is multi-resolution, and S2 cell ids nest as ranges — two
properties that give it a budget knob on `cover` and efficient whole-set algebra
without touching geometry.

### A budget and a rule

S2's covering algorithm is multi-resolution: candidate cells classify exactly
against the geometry itself within a cell budget, mixing large and small cells.
`gm.s2_cover(geom, ...)` takes the same `cell_rule` as H3 plus the budget knobs
`level`, `max_cells`, `target_cells`, `min_level`, `max_level`, and `level_mod`:

```python exec="on" source="block" result="text"
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cov = gm.s2_cover(area, min_level=6, max_level=12, max_cells=64)
print('# cells:', len(cov.cells), '| budget:', cov.max_cells)
print('levels present:', sorted({c.level for c in cov.cells}))
print('exact membership anyway:', cov.covers(gm.Point(21.0, 52.0, crs=4326)))

```

`max_cells` is the hard cap on fixed-depth cover factories (H3, geohash, tiles,
and fixed-level S2), defaulting to `1_000_000`; pass `max_cells=None` for an
unlimited fixed-depth cover (bounded only by memory). On S2, omit `level` for an
adaptive multi-level covering guided by `target_cells` (default `8`, the
S2-idiomatic approximation target). A fixed `level` never coarsens: if its
exact covering exceeds `max_cells`, the factory raises.
The cells are a true spatial key — exactly the cells satisfying `cell_rule` — and
`interior_cells`/`boundary_cells` expose the same certified core-vs-fringe
split as H3. The membership methods (`covers`/`contains`/`intersects` and the
`_xy` spellings) answer exactly against the source geometry regardless, using
the prepared predicate kernel.

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

Prefer [`gm.h3_union`][gometry.h3_union] / [`gm.h3_intersection`][gometry.h3_intersection]
/ [`gm.h3_difference`][gometry.h3_difference] over plain Python `set` arithmetic:
those free functions already normalize mixed resolutions under the compact
contract. A fixed-resolution `set` of raw ids is fine only when you intentionally
want identity equality with no parent/child absorption.

## Geohash and XYZ tiles

Geohash and tiles are *rectangular* grids — every cell is an exact lon/lat
rectangle — so their coverages are true spatial keys, not bounding-box
supersets. They wear the same API as H3 and S2.

```python exec="on" source="block" result="text"
import gometry as gm
point = gm.Point(13.4, 52.5, crs=4326)
print('geohash:', gm.GeohashCell(point, precision=6).token)
print('tile:   ', gm.Tile(point, zoom=12).token)
area = gm.box(13.2, 52.4, 13.6, 52.6, crs=4326)
cov = gm.geohash_cover(area, precision=5, cell_rule='overlap')
print('# geohash cells:', len(cov.cells), 'interior:', len(cov.interior_cells))
print('covers a point:', cov.contains_xy(13.4, 52.5))
tiles = gm.tile_cover(area, zoom=10)
print('# tiles:', len(tiles.cells), 'first quadkey:', tiles.cells[0].token)

```

A geohash or tile coverage is built as a **uniform single-depth tiling** —
every cell is the same precision/zoom, which is what makes `.cells` a clean
spatial key. Exact membership still tests the retained source geometry and its
cost therefore depends on that geometry; token lookup alone is not the answer.
All four coverages
share one cell-set surface: `compact()` / `uncompact(depth)` /
`with_parents()` re-represent the visible cells (the exact predicates still
answer against the source geometry, unchanged), and `CellArray` methods do the
same algebra over a raw cell list.

`GeohashCell` and `Tile` carry the full cell surface — `center`,
`boundary`, `area`, `parent`/`children`/`neighbors`, ordering, and pickle —
and `CellArray` adds the set operations (`cells.compact()` /
`cells.uncompact(depth)`) while H3 vertex/edge arrays expose only their valid
topological surfaces. `Tile` keeps the quadkey constructor
(`gm.Tile('0313102310')`). Tiles reject latitudes outside the Web Mercator
domain (±85.0511°), the limit where the projection stays finite; they do not
clamp coordinates.

## Aggregate by cell, then refine

Use cell arrays for fast global bucketing, then use a spatial index when the
answer must be exact against polygons. The grid step groups cheaply; the refine
step keeps topology honest.

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

Cell equality is a candidate join, not polygon truth. A coverage can answer exact
membership only while it retains its source geometry; after exporting `.cells`
to a DataFrame or lakehouse, refine matched rows with `SpatialIndex.query`,
`gm.join`, or the corresponding exact predicate against the original geometry.

## Ordering & locality

A [space-filling curve](https://en.wikipedia.org/wiki/Hilbert_curve) threads a single line through a 2-D grid so that points
close on the line are close in space. Encode each geometry as its position
along that curve and you get an **integer key whose sort order preserves
locality** — the one trick behind [GeoParquet](https://geoparquet.org/) row-group packing, spatial
shuffles, cache-friendly index builds, and "give me a deterministic but
spatially-coherent order" requests.

gometry exposes one keying and sorting API with an explicit `curve=` choice,
plus the observation that two of the grid systems already *are* these
orderings:

- **Hilbert** — the default `curve='hilbert'`. The best locality of any
  practical curve (no long jumps), and the order GeoParquet writers use.
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
requires matching bounds, level, quantization, coordinate convention, and algorithm
explicitly — do not assume another writer uses the same defaults.

### Sorting an array

[`sort_by_spatial_key`][gometry.GeometryArray.sort_by_spatial_key] returns the
array reordered by the selected key, so nearby geometries land in nearby rows
— the packing that makes row-group min/max filters and block reads effective.

```python exec="on" source="block" result="text"
import gometry as gm

grid = gm.GeometryArray([gm.Point(x, y) for y in range(4) for x in range(4)])
ordered = grid.sort_by_spatial_key(level=8)
print([(round(p.x), round(p.y)) for p in ordered])
# The walk snakes through the grid: consecutive rows are spatial neighbors,
# never a jump across the whole extent.

```

Tracing the sorted order as a path makes the locality concrete. Hilbert and
Morton both preserve neighborhoods, but the path shape shows why Hilbert is the
default when row groups should stay as compact as possible:

```python exec="on" html="true"
from _figures import curve_through, panels
import gometry as gm

grid = gm.GeometryArray([gm.Point(x, y) for y in range(8) for x in range(8)])
print(panels([
    ("sort_by_spatial_key()", curve_through(grid, curve="hilbert", level=3)),
    ("sort_by_spatial_key(curve='morton')", curve_through(grid, curve="morton", level=3)),
]))

```

Write that order to GeoParquet (or feed it to `gm.SpatialIndex`) and every
block holds a spatially compact run of geometries.

### The grids are curves too

Two of the discrete grids above already encode these orderings, so you
rarely need a separate sort once you are working in cells:

- **[S2](https://s2geometry.io/) cell ids are Hilbert order.** `sorted(s2_cells)` groups spatial
  neighbors, because an S2 id is the cell's position along the face Hilbert
  curve. The cell-set algebra and coverage membership rely on it.
- **Tile ids and [quadkeys](https://learn.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system) are Morton order.** A `Tile`'s packed id interleaves
  `x`/`y` so it sorts in quadkey order; `tile.morton` is that index directly.
- **[Geohash](https://en.wikipedia.org/wiki/Geohash) token order is a Z-ish curve.** Base-32 tokens sort
  lexicographically into a locality-preserving order — which is exactly why a
  `GeohashCell`'s integer key equals its token order.

So the choice is: key your geometries explicitly with `spatial_key()` for
GeoParquet/index packing (pass `curve='morton'` when its cheaper ordering is
the fit), or address them as cells and let the cell id carry the ordering for
free.

## Point geocodes

[Open Location Code](https://github.com/google/open-location-code) (plus codes)
and OSM shortlinks are compact point codes rather than cell systems — small enough
to expose as direct, codec-named functions:

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
`pluscode_shorten` / `pluscode_recover` handle the short-code dance against a
reference location. `osm_shortlink_location` accepts the legacy `@` spelling.

Coming from h3-py or s2sphere? See [Migrating](../migrating/index.md#coming-from-h3-py-s2sphere).

## See also

- [Spatial indexing](indexing.md) — candidate vs exact refine on geometry, and
  rebuilding an index after a spatial sort.
- [The mental model](../get-started/mental-model.md) — the candidates-vs-exact
  doctrine and the decision table for which model to reach for.
- [API: h3_cover][gometry.h3_cover] · [s2_cover][gometry.s2_cover] ·
  [H3Cell][gometry.H3Cell] · [S2Cell][gometry.S2Cell] ·
  [SpatialIndex][gometry.SpatialIndex] ·
  [Geometry.spatial_key][gometry.Geometry.spatial_key] ·
  [GeometryArray.sort_by_spatial_key][gometry.GeometryArray.sort_by_spatial_key]
