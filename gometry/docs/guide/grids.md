---
description: H3, S2, geohash, and XYZ tile grids in gometry, plus point geocodes, spherical cells, space-filling keys, and cell-selection rules.
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

Cover factories materialize cell keys. A grid cover narrows the search; it is not
an exact geometry answer. Retain the source geometry and use an exact predicate to
refine those candidates into exact matches. The chosen `cell_rule`, rather than a
bbox prefilter, determines which cells are candidates.

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

The plural factories also accept an iterable of `Point` geometries or an
N×2-or-wider coordinate matrix. Point rows share one CRS and epoch just like a
`GeometryArray`; `None` rows remain missing cells. Bare coordinate matrices
are WGS84 longitude/latitude and use only their first two columns.

```python exec="on" source="block" result="text"
import gometry as gm

points = [gm.Point(21.0, 52.0, crs=4326), None]
cells = gm.h3_cells(points, resolution=7)
print("missing rows:", cells.is_missing.tolist())

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

Every cell class shares one uniform surface — [`.token`][gometry.Cell.token], [`.center`][gometry.Cell.center], [`.polygon`][gometry.Cell.polygon],
[`.neighbors`][gometry.Cell.neighbors], [`.area`][gometry.Cell.area], [`.parent(...)`][gometry.Cell.parent], [`.children(...)`][gometry.Cell.children], and
cell-hierarchy [`.contains(...)`][gometry.Cell.contains] / [`.intersects(...)`][gometry.Cell.intersects] — captured by the
`gm.Cell` protocol, so grid-system-agnostic
code type-checks against any of them. Each cell class is constructed directly —
`H3Cell(value)` / `S2Cell(value)` / [`Tile(value)`][gometry.Tile] — accepting a cell object,
a numeric id (where the system has one), or a token string. H3 topological
vertices and directed edges are separate scalar identities ([`H3Vertex(value)`][gometry.H3Vertex],
[`H3Edge(value)`][gometry.H3Edge]) with fixed arrays ([`H3VertexArray`][gometry.H3VertexArray], [`H3EdgeArray`][gometry.H3EdgeArray]) because
they do not have cell hierarchy or area/polygon operations. Each system adds
its own vocabulary on top: [`H3Cell.resolution`][gometry.H3Cell.resolution],
[`.grid_disk(k)`][gometry.H3Cell.grid_disk] / [`.grid_ring(k)`][gometry.H3Cell.grid_ring] / [`.grid_path(...)`][gometry.H3Cell.grid_path]; [`S2Cell.level`][gometry.S2Cell.level].
Arbitrary [`CellArray`][gometry.CellArray] sets roll up and down with
[`cells.compact()`][gometry.CellArray.compact] / [`cells.uncompact(depth)`][gometry.CellArray.uncompact].

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

Cell `.area` is reported in square meters using each grid's area model. H3 and
S2 use an authalic sphere; geohash and tile use the WGS 84 ellipsoidal area of
their longitude/latitude rectangle. H3/S2 `.polygon` values approximate curved
spherical boundaries with straight longitude/latitude edges, so their
`polygon.area` need not equal `cell.area`. For geohash and tile, `.polygon` and
`.area` share the rectangle definition but use separate binary64 area paths, so
their results are not guaranteed to be bit-identical.

### The geometry behind a cell ID

A cell ID is shorthand for an area on the ground. Scalar cells expose `.polygon`,
and [`CellArray.polygon`][gometry.CellArray.polygon] returns a whole batch:

```python exec="on" source="block" result="text"
import gometry as gm
cell = gm.H3Cell(gm.Point(21.0, 52.0, crs=4326), resolution=6)
poly = cell.polygon
print('cell', cell.id, '→', poly.geometry_type)
print('WKT:', poly.to_wkt()[:48], '...')
print('batch:', type(cell.grid_disk(1).polygon).__name__)

```

For H3 and S2 that polygon is a planar lon/lat chord proxy rather than the
spherical cell boundary. Adjacency and
local indexing are cell methods: `neighbors` lists the edge-adjacent ring,
[`is_neighbor`][gometry.H3Cell.is_neighbor] tests one candidate, and [`local_ij`][gometry.H3Cell.local_ij]/[`cell_from_local_ij`][gometry.H3Cell.cell_from_local_ij] move through
the local grid-algebra space around an anchor. Resolution metadata lives in the
H3-prefixed global family — [`gm.h3_pentagons`][gometry.h3_pentagons] and
[`gm.h3_base_cells`][gometry.h3_base_cells]:

```python exec="on" html="true"
from _figures import cell_polys, figure
import gometry as gm

cell = gm.H3Cell(gm.Point(21.0, 52.0, crs=4326), resolution=6)
print(figure(
    {'source': cell.polygon, 'result': cell_polys(cell.children())},
    "cell.children() nested inside one H3 cell",
    alt="The seven child cells tile the interior of the single parent cell.",
))

```

```python exec="on" source="block" result="text"
import gometry as gm

cell = gm.H3Cell(gm.Point(21.0, 52.0, crs=4326), resolution=6)
neighbor = cell.neighbors[0]
print("adjacent:", cell.is_neighbor(neighbor), "| base cell:", cell.base_cell)
i, j = neighbor.local_ij(cell)
print("local ij:", (i, j), "→", cell.cell_from_local_ij(i, j) == neighbor)
print("pentagons at res 4:", len(gm.h3_pentagons(4)))

```

```python exec="on" html="true"
from _figures import cell_polys, figure
import gometry as gm

cell = gm.H3Cell(gm.Point(21.0, 52.0, crs=4326), resolution=6)
print(figure(
    {'source': cell.polygon, 'result': cell_polys(cell.neighbors), 'anchor': cell.center},
    "neighbors around one H3 cell",
    alt="Six neighbor cells ring the centered cell, sharing one edge with it each.",
))

```

## The spherical model

S2 uses a spherical model for cells and coverings, so the antimeridian and poles
do not create a longitude seam in cell selection. gometry exposes typed cells and
cell sets rather than a general spherical boolean-topology engine.

```python exec="on" source="block" result="text"
import gometry as gm

seam = gm.box(175.0, -5.0, -175.0, 5.0, crs=4326, wrap='split')
covering = gm.s2_cover(seam, max_cells=24)
positive_lon = sum(1 for cell in covering.polygon if cell.bounds[0] >= 0)

print("cells:", len(covering), "| levels:", sorted({cell.level for cell in covering}))
print("cells at 175°E..180°:", positive_lon, "| at 180°..175°W:", len(covering) - positive_lon)
print("covered / requested area:", round(covering.to_polygon().area / seam.area, 2))

```

The covering spans both sides of the meridian and stays a single cell set. It is
not drawn here: every cell of this covering meets ±180 exactly rather than
straddling it, so a longitude/latitude plot of it is indistinguishable from a
seam-aware planar grid and would illustrate nothing about the spherical model.

For WGS 84 planar *geometry* crossing ±180 (as opposed to cells), see
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
    coverings, and authalic-sphere cell areas — not spherical boolean topology.
    Geometry topology evaluates stored chart edges after geographic frames
    resolve seam aliases and pole equivalence in their periodic quotient; it is
    not raw-longitude plane topology. It does not run spherical S2 boolean
    operations. See [spatial predicates](predicates.md) for the
    chart/quotient model. CRS-aware measures use ellipsoidal chart edges for
    shapes and geodesics for point-to-point distance.

## Covering an area

[`gm.h3_cover(geom, resolution=...)`][gometry.h3_cover], fixed-level
[`gm.s2_cover(geom, level=...)`][gometry.s2_cover], and adaptive
[`gm.s2_cover(geom, target_cells=...)`][gometry.s2_cover] calls return a typed
`CellArray` directly:

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cov = gm.h3_cover(area, resolution=5)

print("# cells:", len(cov))
print("first cell id:", cov[0].id)

```

Cover functions also accept an iterable of geometries. They return one
`Groups` row per input item. A `None` input produces a missing row, while a
present empty geometry produces an empty `CellArray`.

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
groups = gm.h3_cover([area, None], resolution=5)
print("rows:", len(groups), "| missing:", groups.is_missing.tolist())
print("missing row:", groups[1])

```

### `cell_rule` is explicit semantics

The `cell_rule` argument selects which cells `cells` materializes. The same four
modes apply to H3, S2, geohash, and tiles. It defaults to `"overlap"`, the
complete-coverage superset used for candidate keys.

| `cell_rule` | A cell is included when… | Typical use |
| --- | --- | --- |
| `"within"` | the cell lies **entirely inside** the geometry | a *subset* — cells the area fully owns |
| `"center"` | the cell's **center** falls inside the geometry | h3-py-style polygon-to-cell selection; boundary cells may be omitted |
| `"overlap"` (default) | the cell **intersects** the geometry at any point (interior or boundary) | a *superset* — safe join/prefilter keys, never misses a true hit |
| `"bbox"` | the cell meets the geometry's **envelope** (one window per part, split at the antimeridian) | loosest and fastest candidate rule: no shape test decides it |

```python exec="on" source="block" result="text" session="grid-rules"
import gometry as gm

area = gm.Polygon(
    [(20.0, 51.25), (20.25, 51.0), (22.0, 52.75), (21.75, 53.0), (20.0, 51.25)],
    crs=4326,
)
for rule in ("within", "center", "overlap", "bbox"):
    cov = gm.h3_cover(area, resolution=5, cell_rule=rule)
    if rule == 'within':
        assert all(gm.covers(area, cell.polygon) for cell in cov)
    elif rule == 'center':
        assert all(gm.covers(area, cell.center) for cell in cov)
    elif rule == 'overlap':
        assert all(gm.intersects(area, cell.polygon) for cell in cov)
    elif rule == 'bbox':
        assert all(gm.intersects(area.envelope(), cell.polygon) for cell in cov)
    print(f"{rule:>8}: {len(cov):>3} cells")

```

For this rotated rectangle and resolution, the selected cell sets form these
nested sets: `"within"` ⊆ `"center"` ⊆ `"overlap"` ⊆ `"bbox"`.

```python exec="on" html="true" session="grid-rules"
from _figures import panels
print(panels(
    [
        (
            f'cell_rule={rule!r}',
            {'candidate': gm.h3_cover(area, resolution=5, cell_rule=rule).polygon,
             'source': area},
            conclusion,
        )
        for rule, conclusion in (
            ('within', 'Every selected cell lies wholly inside the polygon, so the cells stop short of its edges.'),
            ('center', 'Every selected cell has its center inside the polygon, so the cells reach the edges and overhang them slightly.'),
            ('overlap', 'Every cell touching the polygon is selected, so the cells cover it completely and spill past every edge.'),
            ('bbox', 'Every cell meeting the polygon envelope is selected, filling the whole bounding rectangle around the diagonal shape.'),
        )
    ],
    caption='Each rule keeps the one before it and adds the cells its looser test admits.',
    alt='The within cells sit inside the polygon, center adds cells on the boundary, overlap adds every touching cell, and bbox fills the surrounding rectangle.',
))

```

The rotated rectangle has a much wider envelope than its area, so each panel
adds cells for a distinct reason: the cell is inside the shape, its center is
inside, the cell intersects the shape, or the cell reaches the envelope.

The returned cell set depends on `cell_rule`; exact geometry predicates still use
the source geometry.

### Input is normalized to WGS 84 lon/lat

All four grid factories accept a **WGS 84 lon/lat interchange domain** (and
transform supported projected input into that domain first). The grids themselves
are not one model: H3 is an icosahedral discrete global grid, S2 a spherical
cube, geohash a lon/lat rectangle hierarchy, and tiles the Web Mercator XYZ
scheme. If you hand `gm.h3_cover` / `gm.s2_cover` a geometry in a supported
projected CRS, gometry transforms it to WGS 84 lon/lat first. WGS 84 and CRS-free
lon/lat pass through; other supported frames are reprojected before cover keys
are generated. This normalization is a grid-factory boundary: do not pass native
radian or grad values as bare WGS 84 coordinates. Attach the source CRS to a
geometry and let the factory transform it first.

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
the source geometry and apply [`gm.covers`][gometry.covers], [`gm.contains`][gometry.contains], or [`gm.intersects`][gometry.intersects] to
determine exact membership for candidate rows.

```python exec="on" source="block" result="text"
import gometry as gm

area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
points = gm.points([21.0, 22.1, 30.0], [52.0, 52.0, 52.0], crs=4326)
cover_tokens = {cell.token for cell in gm.h3_cover(area, resolution=5)}
point_cells = gm.h3_cells(points.coords.x, points.coords.y, resolution=5)
candidate_rows = [i for i, cell in enumerate(point_cells) if cell.token in cover_tokens]
exact_rows = [i for i, point in enumerate(points) if gm.covers(area, point)]

print("candidate rows:", candidate_rows)
print("exact covered rows:", exact_rows)
assert candidate_rows == [0, 1]
assert exact_rows == [0]

```

The H3 key join admits row 1 because its cell overlaps the source cover; the
source-geometry predicate removes that candidate. The cell key is a candidate,
not an exact point-in-polygon result.

```python exec="on" source="block" result="text"
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cells = gm.h3_cover(area, resolution=5)
points = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
print('covers:', repr(gm.covers(area, points)))
print('edge point:', gm.covers(area, gm.Point(20.0, 52.0, crs=4326)), '| strictly inside:', gm.contains(area, gm.Point(20.0, 52.0, crs=4326)))
print('raw lon/lat stream:', repr(gm.intersects(area, x=[21.0, 30.0], y=[52.0, 52.0])))

```

```python exec="on" html="true"
from _figures import figure
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cells = gm.h3_cover(area, resolution=5)
print(figure(
    {'candidate': cells.polygon, 'source': area},
    'H3 overlap cells are candidate join keys, not membership answers.',
    alt='H3 overlap cells can extend beyond the source area, so exact membership still uses the source geometry.',
))

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
Each grid spells it by return type: [`gm.h3_bounding_cell`][gometry.h3_bounding_cell] / [`gm.s2_bounding_cell`][gometry.s2_bounding_cell]
/ [`gm.geohash_bounding_cell`][gometry.geohash_bounding_cell] return their cell type, and [`gm.tile_bounding_cell`][gometry.tile_bounding_cell]
returns a `Tile`.

```python exec="on" source="block" result="text"
import gometry as gm

berlin = gm.box(13.0, 52.3, 13.7, 52.7, crs=4326)

h3_cell = gm.h3_bounding_cell(berlin)
s2_cell = gm.s2_bounding_cell(berlin)
print("h3:", h3_cell, "res", h3_cell.resolution)
print("s2:", s2_cell, "level", s2_cell.level)
print("geohash:", gm.geohash_bounding_cell(berlin).token)
print("tile:", gm.tile_bounding_cell(berlin))

```

Each returns the *deepest* single cell whose certified extent covers the complete
lon/lat bounding region, not just the input vertices. The proof includes region
interiors and curved cell-edge bulges; seam and pole extents are normalized
before checking, and polygon holes do not shrink the bounding region. A
candidate that cannot be proved is replaced by a coarser proven ancestor. Pass
a geometry, an array, or a raw `(minx, miny, maxx, maxy)` bounds tuple.

### Dissolving a covering back to one outline

To recover one geometry from a covering, [`to_polygon()`][gometry.CellArray.to_polygon] dissolves it into one
outline; shared cell edges cancel, so the interior costs no geometry:

```python exec="on" source="block" result="text"
import gometry as gm

cov = gm.h3_cover(gm.box(20.0, 51.0, 22.0, 53.0, crs=4326), resolution=5)
print("dissolved:", cov.to_polygon().geometry_type)

```

The outline uses WGS 84 longitude/latitude coordinates. When the selected cells cross the
antimeridian, it preserves the occupied regions on both sides of ±180° rather
than filling the longitude span through the middle of the map; the chart-cut
edge is not part of the represented cell boundary.

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
    ([`coverage_union_all`][gometry.coverage_union_all], [`coverage_simplify`][gometry.coverage_simplify],
    [`coverage_clean`][gometry.coverage_clean]) validate and repair an *arbitrary source polygon fabric*
    (parcels, admin boundaries) with edge-matched interfaces. DGGS cell arrays
    are a different domain: same-resolution H3/S2 tessellations do
    share boundaries, but hierarchy, compact/uncompact, and cell-set algebra
    already own that problem — use those, not `coverage_*`, to merge cells.

## Cell-set algebra (all four grids)

Every grid family exposes the same **factories + set-algebra** shape: prefixed
`union`, `intersection`, and `difference` functions on cell sets (H3, S2,
geohash, and tiles). Hierarchy behavior still differs per system (S2 range nesting,
H3 resolution parents, geohash/tile prefix parents). H3 cell sets require
hierarchy-aware free functions rather than plain Python `set` arithmetic.

**H3 resolution normalization** ([`gm.h3_union`][gometry.h3_union] / [`gm.h3_intersection`][gometry.h3_intersection] /
[`gm.h3_difference`][gometry.h3_difference]): inputs may mix resolutions. The result is cell-ID algebra
under the compact contract — sorted, with descendants absorbed by ancestors and
complete sibling groups merged into parents — so mixed-resolution operands are
normalized rather than rejected. This is identity algebra (an H3 child's *geometry*
does not nest exactly inside its parent, but its id does). S2 algebra is
range-nesting exact; geohash/tile algebra is prefix-based.

Every set-algebra, `compact()`, and `uncompact()` result uses the grid family's
public identity order: H3 and S2 numeric id order, tile packed Morton-id order,
and geohash token order. This presentation order is separate from the internal
descendant-range order used to prove containment and merge sibling groups.
Set-algebra operands skip `None` entries inside an iterable; use an empty
iterable to represent an empty set.

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

## S2 coverings: fixed levels and adaptive budgets

S2 coverings are multi-resolution, and S2 cell IDs nest as ranges. These
properties provide a `cover` budget and whole-set algebra without touching
geometry.

### A budget and a rule

`gm.s2_cover(geom, level=...)` materializes cells only at the requested level.
It never coarsens to fit `max_cells`. Fixed and adaptive modes use the same
four `cell_rule` values:

```python exec="on" source="block" result="text"
import gometry as gm
area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
cov = gm.s2_cover(area, level=8, max_cells=64)
print('# cells:', len(cov))
print('levels present:', sorted({c.level for c in cov}))

```

Omit `level` to use adaptive refinement through the same
[`gm.s2_cover`][gometry.s2_cover] operation. `target_cells`, `min_level`,
`max_level`, and `level_mod` control that mode. The target guides refinement
but is not a hard limit; the legal minimum cover can exceed it. Refinement may
retain coarser cells within the requested level range when children would
exceed `max_cells`. A fixed `level` cannot be combined with non-default
adaptive controls.

Both S2 modes default `max_cells` to `1_000_000`; grouped calls apply it to the
total returned cells. Discarded candidates do not consume the cap. Pass
`max_cells=None` for an unlimited cover (bounded only by memory). The cap does
not bound classifier or traversal CPU work. The cells are spatial keys
materialized according to `cell_rule`; exact geometry predicates remain free
functions such as `gm.covers` and [`gm.contains`][gometry.contains] with its
coordinate-keyword form.

### Cell-set algebra

S2 cell ids nest as ranges, so whole-set operations stay exact without touching
geometry: [`gm.s2_union`][gometry.s2_union] absorbs descendants and merges complete
sibling groups, [`gm.s2_intersection`][gometry.s2_intersection] keeps the finer cell
of an ancestor/descendant overlap, and [`gm.s2_difference`][gometry.s2_difference]
splits partially-covered cells exactly (`gm.S2Cell` takes a [`Point`][gometry.Point] or a
bare ``lon, lat`` pair):

```python exec="on" source="block" result="text"
import gometry as gm
cell = gm.S2Cell(13.4, 52.5, level=10)
children = cell.children()
print('union:', list(gm.s2_union(children[:2], children[2:])) == [cell])
print('intersection:', [c.level for c in gm.s2_intersection([cell], children[:2])])
print('difference:', len(gm.s2_difference([cell], children[:1])), 'cells remain')

```

`gm.h3_union` / `gm.h3_intersection`
/ `gm.h3_difference` normalize mixed resolutions under
the compact contract. A fixed-resolution `set` of raw IDs performs identity
equality without parent/child absorption.

## Geohash and XYZ tiles

Geohash and tiles are *rectangular* grids — every cell is an exact lon/lat
rectangle, and their cover keys use those rectangles rather than a separate
bounding-box approximation. They wear the same API as H3 and S2.

Tile point ownership is representational: geographic construction uses the
same emitted binary64 edges as the tile footprint. Longitude is
west-inclusive/east-exclusive, and latitude is north-inclusive/south-exclusive
at shared edges. This defines stable cell IDs for stored floating-point
coordinates; it is not a claim that the ideal Web Mercator transcendental
function is exactly rounded in binary64.

```python exec="on" source="block" result="text"
import gometry as gm
point = gm.Point(13.4, 52.5, crs=4326)
print('geohash:', gm.GeohashCell(point, precision=6).token)
print('tile:', gm.Tile(point, zoom=12).token)
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

[`GeohashCell`][gometry.GeohashCell] and `Tile` carry the full cell surface — `center`,
`polygon`, `area`, `parent`/`children`/`neighbors`, ordering, and pickle —
and `CellArray` adds the set operations (`cells.compact()` /
`cells.uncompact(depth)`) while H3 vertex/edge arrays expose only their valid
topological surfaces. `Tile` keeps the quadkey constructor
(`gm.Tile('0313102310')`). Tiles reject latitudes outside the square Web Mercator
tile grid (±85.0511°), a representational extent distinct from the finite
projection domain; they do not clamp coordinates.

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

[`value_counts()`][gometry.CellArray.value_counts] is the direct aggregation primitive. [`factorize()`][gometry.CellArray.factorize] is the
handoff shape for pandas/NumPy-style grouping: `codes[i]` points at a row in the
returned unique `CellArray`.

Cell equality is a candidate join, not polygon truth. After exporting cells
to a DataFrame or lakehouse, refine matched rows with [`SpatialIndex.query`][gometry.SpatialIndex.query],
`gm.join`, or the corresponding exact predicate against the original geometry.

## Ordering & locality

A [space-filling curve](https://en.wikipedia.org/wiki/Hilbert_curve) threads a
single line through a 2-D grid. Encoding each geometry as its position along that
curve produces an **integer key whose sorted blocks tend to cluster nearby
locations** for [GeoParquet](https://geoparquet.org/) row-group packing, spatial
shuffles, and index builds. Numeric key distance is not geographic distance:
every curve has discontinuities, so use bounds or exact predicates for spatial
decisions.

gometry exposes one keying and sorting API with an explicit `curve=` choice.
S2 and tile cell identifiers also encode locality-preserving orderings:

- **Hilbert** — the default `curve='hilbert'`, a recursive locality-preserving
  order.
- **Morton (Z-order)** — `curve='morton'`. Cheaper to compute (bit
  interleave), with one diagonal jump per quadrant — the [Z-order curve](https://en.wikipedia.org/wiki/Z-order_curve).

### Keys from geometries

[`spatial_key`][gometry.GeometryArray.spatial_key] discretizes each row's
bounding-box center onto a `2**level × 2**level` grid over one shared frame, the
array's [`total_bounds`][gometry.GeometryArray.total_bounds] unless `bounds=` is
given, and returns its position along the selected curve. Keys compare only
within one frame, so pass the same `bounds=` when keying separate arrays.
Sorting keys tends to group nearby locations into blocks, but numerically close
keys can be geographically distant at curve discontinuities.

```python exec="on" source="block" result="text"
import gometry as gm
cities = gm.GeometryArray([gm.Point(-0.13, 51.51), gm.Point(2.35, 48.86), gm.Point(-73.94, 40.72), gm.Point(-74.01, 40.71)])
keys = cities.spatial_key(level=16)
print(repr(keys))

```

The default `level=16` is **gometry's default only**. GeoParquet does not define a
Hilbert level or a canonical key algorithm. Bit-identical ordering across tools
requires matching bounds, level, quantization, coordinate convention, and
algorithm. Another writer may use different defaults.

### Sorting an array

[`sort_by_spatial_key`][gometry.GeometryArray.sort_by_spatial_key] returns the
array reordered by the selected key, so sorted blocks tend to hold nearby
geometries — useful packing for row-group min/max filters and block reads. It is
not a spatial predicate or a distance order.

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

The sorted order can be written to GeoParquet or passed to [`gm.SpatialIndex`][gometry.SpatialIndex]; each
block then contains a spatially compact run of geometries.

### The grids are curves too

S2, tile, and geohash identifiers encode these orderings, so sorting their keys
can provide the same block-locality benefit. It does not make numeric differences
geographic distances or remove discontinuities:

- **[S2](https://s2geometry.io/) cell ids are Hilbert order.** `sorted(s2_cells)`
  tends to group spatial neighbors because an S2 id is the cell's position along
  a face Hilbert curve. The cell-set algebra relies on its range hierarchy.
- **Tile ids and [quadkeys](https://learn.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system) are Morton order.** A `Tile`'s packed id interleaves
  [`x`][gometry.Tile.x]/[`y`][gometry.Tile.y] so it sorts in quadkey order; [`tile.morton`][gometry.Tile.morton] is that index directly.
- **[Geohash](https://en.wikipedia.org/wiki/Geohash) token order is a Z-ish curve.**
  Base-32 tokens sort lexicographically into an order that tends to preserve
  locality, which is why a `GeohashCell` token carries that ordering directly.

Geometry arrays use `spatial_key()` for GeoParquet or index packing, with
`curve='morton'` selecting Morton order. Cell identities carry their grid's
ordering directly.

## Point geocodes

[Open Location Code](https://github.com/google/open-location-code) (plus codes)
and OSM shortlinks are compact point codes rather than cell systems. gometry
exposes them as direct, codec-named functions:

```python exec="on" source="block" result="text"
import gometry as gm

code = gm.pluscode_encode(8.628, 47.366)
print("plus code:", code)
print("cell:", gm.pluscode_polygon(code).bounds)

link = gm.osm_shortlink_encode(13.365, 52.5077, zoom=17)
print("shortlink:", link)
print("decoded:", gm.osm_shortlink_location(link))

```

`pluscode_encode` accepts a `Point` or `GeometryArray` (CRS-aware), as well as
coordinate columns. [`pluscode_shorten`][gometry.pluscode_shorten] and
[`pluscode_recover`][gometry.pluscode_recover] accept a `Point` reference or
broadcast longitude and latitude columns; one scalar code is applied to each
coordinate row. [`osm_shortlink_location`][gometry.osm_shortlink_location]
accepts the legacy `@` spelling.

Coming from h3-py or s2sphere? See [Migrating](../migrating/index.md#coming-from-h3-py-s2sphere).

## See also

- [Spatial indexing](indexing.md) — candidate vs exact refine on geometry, and
  rebuilding an index after a spatial sort.
- [Arrow & storage](../ecosystem/arrow.md) — exporting cell IDs and geohash tokens
  as Arrow columns without retaining grid-kind metadata.
- [The mental model](../get-started/mental-model.md) — the three operation models.
- [API: h3_cover][gometry.h3_cover] · [s2_cover][gometry.s2_cover] ·
  [H3Cell][gometry.H3Cell] · [S2Cell][gometry.S2Cell] ·
  [SpatialIndex][gometry.SpatialIndex] ·
  [GeometryArray.spatial_key][gometry.GeometryArray.spatial_key] ·
  [GeometryArray.sort_by_spatial_key][gometry.GeometryArray.sort_by_spatial_key]
