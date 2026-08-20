use crate::collections::{HashSet, HashSetExt as _};
use crate::geometry::{
    Point, PointBatchTester, PolePosition, Polygon, Ring, Shape, ShapeData, pole_position,
    shape_has_polar_ring, shape_spans_full_longitude,
};
use crate::grid::affine_source::visit_grid_cover_components;
use crate::grid::{CoverBudgetExceeded, ensure_cover_budget};
use crate::predicates::engine::{Predicate, topology_scalar_pair};
use crate::py::cells::h3::{CellIndex, LatLng, Resolution, TiledCell, h3_latlng};

/// Whether the unsplit source is inherently spherical (poles / full longitude /
/// antimeridian) so center probes use the geographic topology gate.
fn source_needs_checked_flood(source: &Shape) -> bool {
    pole_position(source, true) != PolePosition::Exterior
        || pole_position(source, false) != PolePosition::Exterior
        || shape_has_polar_ring(source)
        || source.crosses_antimeridian()
        || shape_spans_full_longitude(source)
}

/// Boundary-inclusive center probe against a prepared areal tester (planar path).
fn center_covers_prepared(tester: &PointBatchTester, cell: CellIndex) -> bool {
    let center = LatLng::from(cell);
    tester.covers_point(Point::new_unchecked_xy(center.lng(), center.lat()))
}

/// Geographic classifier against the unsplit whole source (H3 covers are always
/// WGS84). Used when `checked` is true so seam/polar cell probes cannot take
/// the false-middle planar path.
fn checked_predicate(source: &ShapeData, cell: CellIndex) -> bool {
    let center = LatLng::from(cell);
    let probe = ShapeData::from(Shape::Point(Point::new_unchecked_xy(
        center.lng(),
        center.lat(),
    )));
    topology_scalar_pair(&Predicate::Covers.spec(), source, &probe, true)
}

/// A native H3 polyfill: each polygon is flood-filled independently from the
/// cells containing its vertices and an interior point, then the per-polygon
/// coverages are unioned. Ordinary mid-latitude sources use planar center
/// containment on the split working shape; polar / full-longitude / seam
/// sources classify centers against the unsplit source with the geographic
/// topology gate.
pub(super) fn h3_tile(
    shape: &Shape,
    resolution: Resolution,
    max_cells: Option<usize>,
) -> Result<Vec<TiledCell>, CoverBudgetExceeded> {
    // Union every part's coverage. One pass; the set is bounded by
    // `max_cells` (when set), checked at every insertion, so a
    // world-scale polygon at a fine resolution fails before flooding memory.
    let mut coverage: HashSet<CellIndex> = HashSet::new();
    // Visible H3 rows are the exact union of the same canonical atomic
    // components used by the certified overlap/bbox coverer.  Do not let the
    // center flood invent a container-specific path: each
    // component is tiled against its own source, then the shared map performs
    // the sole deduplicated global budget check.
    visit_grid_cover_components(shape, &mut |component| {
        let component_source = ShapeData::new(component.clone());
        h3_tile_shape(
            component,
            &component_source,
            resolution,
            max_cells,
            &mut coverage,
        )
    })?;
    let mut cells: Vec<_> = coverage
        .into_iter()
        .map(|cell| TiledCell { cell })
        .collect();
    cells.sort_unstable_by_key(|cell| u64::from(cell.cell));
    Ok(cells)
}

/// Accumulate one atomic center-rule H3 source into `coverage`. Aggregate shapes were
/// decomposed by `visit_grid_cover_components` before reaching this owner.
pub(super) fn h3_tile_shape(
    shape: &Shape,
    unsplit_source: &ShapeData,
    resolution: Resolution,
    max_cells: Option<usize>,
    coverage: &mut HashSet<CellIndex>,
) -> Result<(), CoverBudgetExceeded> {
    match shape {
        Shape::Polygon(polygon) => {
            h3_tile_polygon(polygon, unsplit_source, resolution, max_cells, coverage)?;
        },
        Shape::Point(point) => h3_tile_point(*point, resolution, max_cells, coverage)?,
        Shape::LineString(coords) => {
            h3_tile_line(unsplit_source, coords, resolution, max_cells, coverage)?;
        },
        // `visit_grid_cover_components` admits only these three atomic kinds
        // and skips empties. Keeping the invariant explicit prevents a second
        // aggregate traversal from quietly reappearing beside that owner.
        Shape::MultiPoint(..)
        | Shape::MultiLineString(..)
        | Shape::MultiPolygon(..)
        | Shape::GeometryCollection(..)
        | Shape::Empty(..) => unreachable!("H3 tile source must be atomic"),
    }
    Ok(())
}

/// Rule-specific point coverage. `center` emits the cell only when the point
/// is that cell's exact center (bit-identical lon/lat after H3 decode);
/// Non-area `within` is owned by the certified traversal, not this tiler.
pub(super) fn h3_tile_point(
    point: Point,
    resolution: Resolution,
    max_cells: Option<usize>,
    coverage: &mut HashSet<CellIndex>,
) -> Result<(), CoverBudgetExceeded> {
    let Ok(latlng) = h3_latlng(point) else {
        return Ok(());
    };
    let cell = latlng.to_cell(resolution);
    // Exact center membership: the point must be the cell's center.
    let center = LatLng::from(cell);
    let center_pt = Point::new_unchecked_xy(center.lng(), center.lat());
    if points_equal_signed_zero(point, center_pt) {
        coverage.insert(cell);
        ensure_cover_budget(coverage.len(), max_cells)?;
    }
    Ok(())
}

/// IEEE equality that treats −0.0 and +0.0 as the same coordinate.
fn points_equal_signed_zero(a: Point, b: Point) -> bool {
    #[expect(clippy::float_cmp, reason = "signed-zero-aware center identity")]
    {
        a.x == b.x && a.y == b.y
    }
}

/// Center-rule cells for a line.  Exact overlap/bbox line contact belongs to
/// the affine carrier and ordered H3 arcs, never this sample-ring helper.
pub(super) fn h3_tile_line(
    source: &ShapeData,
    coords: &crate::geometry::CoordSeq,
    resolution: Resolution,
    max_cells: Option<usize>,
    coverage: &mut HashSet<CellIndex>,
) -> Result<(), CoverBudgetExceeded> {
    let mut seen: HashSet<CellIndex> = HashSet::new();
    let mut scratch = [0_u64; 7];
    visit_edge_cells(coords, resolution, |edge_cell| {
        for &raw in h3_one_ring(edge_cell, &mut scratch) {
            let cell = CellIndex::try_from(raw).expect("h3o grid disk returned a valid cell");
            if !seen.insert(cell) {
                continue;
            }
            let center = LatLng::from(cell);
            let center_pt = Point::new_unchecked_xy(center.lng(), center.lat());
            let keep = source.covers_point(center_pt);
            if keep {
                coverage.insert(cell);
                ensure_cover_budget(coverage.len(), max_cells)?;
            }
        }
        Ok(())
    })
}

/// Checked inward flood: every first-seen neighbor is tested against the
/// unsplit source; only matched cells are recorded and enqueued.
fn flood_checked(
    unsplit_source: &ShapeData,
    max_cells: Option<usize>,
    coverage: &mut HashSet<CellIndex>,
    seen: &mut HashSet<CellIndex>,
    mut frontier: Vec<CellIndex>,
    scratch: &mut [u64; 7],
) -> Result<(), CoverBudgetExceeded> {
    while !frontier.is_empty() {
        let mut next: Vec<CellIndex> = Vec::new();
        for &cell in &frontier {
            for &raw in h3_one_ring(cell, scratch) {
                let neighbor =
                    CellIndex::try_from(raw).expect("h3o grid disk returned a valid cell");
                if seen.insert(neighbor) && checked_predicate(unsplit_source, neighbor) {
                    coverage.insert(neighbor);
                    ensure_cover_budget(coverage.len(), max_cells)?;
                    next.push(neighbor);
                }
            }
        }
        frontier = next;
    }
    Ok(())
}

/// Unchecked inward flood: first-seen neighbors need no geometry test because
/// the center-tested outline band is a closed barrier in `seen`.
fn flood_unchecked(
    max_cells: Option<usize>,
    coverage: &mut HashSet<CellIndex>,
    seen: &mut HashSet<CellIndex>,
    mut frontier: Vec<CellIndex>,
    scratch: &mut [u64; 7],
) -> Result<(), CoverBudgetExceeded> {
    while !frontier.is_empty() {
        let mut next: Vec<CellIndex> = Vec::new();
        for &cell in &frontier {
            for &raw in h3_one_ring(cell, scratch) {
                let neighbor =
                    CellIndex::try_from(raw).expect("h3o grid disk returned a valid cell");
                if seen.insert(neighbor) {
                    coverage.insert(neighbor);
                    ensure_cover_budget(coverage.len(), max_cells)?;
                    next.push(neighbor);
                }
            }
        }
        frontier = next;
    }
    Ok(())
}

/// Flood-fill one polygon's center-rule H3 coverage into `coverage`.
pub(super) fn h3_tile_polygon(
    polygon: &Polygon,
    unsplit_source: &ShapeData,
    resolution: Resolution,
    max_cells: Option<usize>,
    coverage: &mut HashSet<CellIndex>,
) -> Result<(), CoverBudgetExceeded> {
    // Outline / first-inner never allocate hexagonal `Shape`s. Cell centers
    // are classified via `LatLng` against one prepared areal tester (planar)
    // or the geographic topology gate (checked). The gate is source-level:
    // center classification is a single point probe and the flood is pure
    // H3-graph traversal.
    let part_source = Shape::Polygon(polygon.clone());
    let checked = source_needs_checked_flood(unsplit_source.shape());
    let mut seen: HashSet<CellIndex> = HashSet::new();
    let mut scratch = [0_u64; 7];

    // One prepared tester for the whole part's planar center probes.
    let planar_tester = (!checked).then(|| PointBatchTester::new(&part_source));

    // 1. Stream the outline straight into the exact classifier. A public
    // budget therefore stops at the first excess *emitted* cell instead of
    // first retaining an unbounded trace of discarded edge owners.
    let mut outline: Vec<CellIndex> = Vec::new();
    for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
        visit_edge_cells(ring.coords(), resolution, |edge_cell| {
            for &raw in h3_one_ring(edge_cell, &mut scratch) {
                let cell = CellIndex::try_from(raw).expect("h3o grid disk returned a valid cell");
                if seen.insert(cell) {
                    let matched = if checked {
                        checked_predicate(unsplit_source, cell)
                    } else {
                        center_covers_prepared(
                            planar_tester
                                .as_ref()
                                .expect("planar tester built when !checked"),
                            cell,
                        )
                    };
                    if matched {
                        coverage.insert(cell);
                        ensure_cover_budget(coverage.len(), max_cells)?;
                        outline.push(cell);
                    }
                }
            }
            Ok(())
        })?;
    }

    if seen.is_empty() {
        return h3_tile_polygon_seeds_center(
            polygon,
            &part_source,
            unsplit_source,
            resolution,
            max_cells,
            coverage,
            &mut seen,
            checked,
        );
    }

    // 2. OUTERMOST INNER RING seeds the flood.
    let mut frontier: Vec<CellIndex> = Vec::new();
    for &cell in &outline {
        for &raw in h3_one_ring(cell, &mut scratch) {
            let neighbor = CellIndex::try_from(raw).expect("h3o grid disk returned a valid cell");
            if seen.insert(neighbor) {
                let matched = if checked {
                    checked_predicate(unsplit_source, neighbor)
                } else {
                    center_covers_prepared(
                        planar_tester
                            .as_ref()
                            .expect("planar tester built when !checked"),
                        neighbor,
                    )
                };
                if matched {
                    coverage.insert(neighbor);
                    ensure_cover_budget(coverage.len(), max_cells)?;
                    frontier.push(neighbor);
                }
            }
        }
    }

    // 4. INWARD FLOOD — checked center still uses LatLng + topology gate;
    // unchecked interior stays geometry-free.
    if checked {
        flood_checked(
            unsplit_source,
            max_cells,
            coverage,
            &mut seen,
            frontier,
            &mut scratch,
        )
    } else {
        flood_unchecked(max_cells, coverage, &mut seen, frontier, &mut scratch)
    }
}

/// Sub-cell seed path for Center mode (no hex Shape materialization).
fn h3_tile_polygon_seeds_center(
    polygon: &Polygon,
    part_source: &Shape,
    unsplit_source: &ShapeData,
    resolution: Resolution,
    max_cells: Option<usize>,
    coverage: &mut HashSet<CellIndex>,
    seen: &mut HashSet<CellIndex>,
    checked: bool,
) -> Result<(), CoverBudgetExceeded> {
    let mut seed_band: Vec<CellIndex> = Vec::new();
    let mut push_seed = |seed: CellIndex| {
        if seen.insert(seed) {
            seed_band.push(seed);
        }
    };
    for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
        for point in ring.coords().points() {
            if let Ok(latlng) = h3_latlng(point) {
                push_seed(latlng.to_cell(resolution));
            }
        }
    }
    if let Ok(Shape::Point(inside)) = part_source.point_on_surface()
        && let Ok(latlng) = h3_latlng(inside)
    {
        push_seed(latlng.to_cell(resolution));
    }
    let planar_tester = (!checked).then(|| PointBatchTester::new(part_source));
    for &seed in &seed_band {
        let matched = if checked {
            checked_predicate(unsplit_source, seed)
        } else {
            center_covers_prepared(
                planar_tester
                    .as_ref()
                    .expect("planar tester built when !checked"),
                seed,
            )
        };
        if matched {
            coverage.insert(seed);
            ensure_cover_budget(coverage.len(), max_cells)?;
        }
    }
    Ok(())
}

fn h3_one_ring(cell: CellIndex, scratch: &mut [u64; 7]) -> &[u64] {
    let mut count = 0;
    for candidate in cell.grid_disk_fast(1) {
        let Some(neighbor) = candidate else {
            count = 0;
            break;
        };
        scratch[count] = neighbor.into();
        count += 1;
    }
    if count == 0 {
        for neighbor in cell.grid_disk_safe(1) {
            scratch[count] = neighbor.into();
            count += 1;
        }
    }
    &scratch[..count]
}

/// Stream the conservative edge-owner trace to a caller-owned sink.
///
/// The public output budget belongs at the sink: a caller may discard every
/// outline owner, while a center-line caller can fail immediately after the
/// first excess emitted cell.  Keeping this traversal streaming also removes
/// the old `count + 1` temporary allocation for long high-resolution lines.
fn visit_edge_cells(
    coords: &crate::geometry::CoordSeq,
    resolution: Resolution,
    mut visit: impl FnMut(CellIndex) -> Result<(), CoverBudgetExceeded>,
) -> Result<(), CoverBudgetExceeded> {
    let points: Vec<Point> = coords.points().collect();
    for &point in &points {
        if let Ok(latlng) = h3_latlng(point) {
            visit(latlng.to_cell(resolution))?;
        }
    }
    for &[start, end] in points.array_windows::<2>() {
        let count = line_hex_estimate(start, end, resolution);
        let mut previous = None;
        for index in 0..=count {
            let fraction = index as f64 / count as f64;
            let point = Point::new_unchecked_xy(
                start.x + (end.x - start.x) * fraction,
                start.y + (end.y - start.y) * fraction,
            );
            let Ok(latlng) = h3_latlng(point) else {
                continue;
            };
            let cell = latlng.to_cell(resolution);
            visit(cell)?;
            if let Some((previous_point, previous_cell)) = previous {
                densify_edge_samples(
                    previous_point,
                    previous_cell,
                    point,
                    cell,
                    resolution,
                    &mut visit,
                )?;
            }
            previous = Some((point, cell));
        }
    }
    Ok(())
}

/// Midpoint-refine consecutive edge samples until each hop is at most one H3
/// cell (or grid distance cannot be computed — still split). Lon/lat-linear
/// midpoints only; no great-circle reinterpretation.
fn densify_edge_samples(
    start: Point,
    start_cell: CellIndex,
    end: Point,
    end_cell: CellIndex,
    resolution: Resolution,
    visit: &mut impl FnMut(CellIndex) -> Result<(), CoverBudgetExceeded>,
) -> Result<(), CoverBudgetExceeded> {
    // Explicit stack avoids deep recursion on long multi-hop edges.
    let mut stack: Vec<(Point, CellIndex, Point, CellIndex)> = Vec::new();
    stack.push((start, start_cell, end, end_cell));
    while let Some((a_pt, a_cell, b_pt, b_cell)) = stack.pop() {
        if a_cell == b_cell {
            continue;
        }
        // Neighbor (or same) → done. Unknown distance (pentagon distortion /
        // face issues) fails open to a midpoint split.
        let adjacent = matches!(a_cell.grid_distance(b_cell), Ok(0 | 1));
        if adjacent {
            continue;
        }
        let mid = Point::new_unchecked_xy(
            a_pt.x + (b_pt.x - a_pt.x) * 0.5,
            a_pt.y + (b_pt.y - a_pt.y) * 0.5,
        );
        let Ok(latlng) = h3_latlng(mid) else {
            continue;
        };
        let mid_cell = latlng.to_cell(resolution);
        visit(mid_cell)?;
        // Guard against exact float stasis (identical endpoints after split).
        if points_equal_signed_zero(mid, a_pt) || points_equal_signed_zero(mid, b_pt) {
            continue;
        }
        stack.push((a_pt, a_cell, mid, mid_cell));
        stack.push((mid, mid_cell, b_pt, b_cell));
    }
    Ok(())
}

/// Upper bound on the cells an edge crosses: its great-circle length over the
/// most-distorted (pentagon) cell diameter at this resolution. The constants
/// are h3's `PENT_DIAMETER_RADS` table (radians), so the trace density matches
/// the native tiler exactly.
pub(super) fn line_hex_estimate(start: Point, end: Point, resolution: Resolution) -> u64 {
    const PENT_DIAMETER_RADS: [f64; 16] = [
        0.325_493_555_083_826_27,
        0.110_620_004_316_979_26,
        0.043_153_124_637_549_6,
        0.015_280_278_825_461_551,
        0.006_095_981_694_441_515,
        0.002_172_375_862_483_39,
        0.000_869_453_299_939_708_2,
        0.000_310_125_153_780_977_2,
        0.000_124_179_024_309_106_14,
        0.000_044_299_222_206_151_81,
        0.000_017_739_277_167_968_58,
        0.000_006_328_371_112_691_009,
        0.000_002_534_170_547_271_686_5,
        0.000_000_904_051_197_380_709_7,
        0.000_000_362_024_123_008_734_75,
        0.000_000_129_150_135_232_098_86,
    ];
    let pentagon_diameter = PENT_DIAMETER_RADS[usize::from(resolution)];
    match (h3_latlng(start), h3_latlng(end)) {
        (Ok(origin), Ok(destination)) => {
            let distance = origin.distance_rads(destination);
            ((distance / pentagon_diameter).ceil() as u64).max(1)
        },
        _ => 1,
    }
}

/// One H3 cell's boundary as a closed planar polygon `Shape` in lon/lat, for
/// the native containment predicates.
///
/// Emitted edges are **planar chord proxies** for the true spherical hexagon
/// boundary (H3 edges are geodesic arcs). Prefer cell-algebra methods
/// (`contains`, `parent`, set ops, `grid_disk`) for exact hierarchical work;
/// do not treat this polygon as a densified spherical boundary.
pub(super) fn h3_cell_shape(cell: CellIndex) -> Shape {
    let mut shell: Vec<Point> = cell
        .boundary()
        .iter()
        .map(|latlng| Point::new_unchecked_xy(latlng.lng(), latlng.lat()))
        .collect();
    if let Some(first) = shell.first().copied() {
        shell.push(first);
    }
    Shape::Polygon(Polygon::new(Ring::from_trusted_closed(shell), Vec::new()))
}

#[cfg(test)]
mod edge_trace_tests {
    use super::*;
    use crate::geometry::CoordSeq;

    #[test]
    fn streaming_edge_sink_stops_before_a_long_trace_is_materialized() {
        let coordinates = CoordSeq::from(vec![
            Point::new_unchecked_xy(-170.0, 0.0),
            Point::new_unchecked_xy(170.0, 0.0),
        ]);
        assert!(
            line_hex_estimate(
                coordinates.point_at(0),
                coordinates.point_at(1),
                Resolution::Fifteen,
            ) > 1_000_000
        );
        let mut calls = 0;
        let error = visit_edge_cells(&coordinates, Resolution::Fifteen, |_| {
            calls += 1;
            Err(CoverBudgetExceeded::new(1))
        })
        .expect_err("the sink's emitted-cell budget must stop the trace immediately");
        assert_eq!(calls, 1);
        assert_eq!(error.limit, 1);
    }

    #[test]
    fn center_cover_is_sorted_and_unique() {
        let shape = Shape::Polygon(Polygon::new(
            Ring::from_trusted_closed(vec![
                Point::new_unchecked_xy(-1.0, -1.0),
                Point::new_unchecked_xy(1.0, -1.0),
                Point::new_unchecked_xy(1.0, 1.0),
                Point::new_unchecked_xy(-1.0, 1.0),
                Point::new_unchecked_xy(-1.0, -1.0),
            ]),
            Vec::new(),
        ));
        let ids = |cells: Vec<TiledCell>| {
            cells
                .into_iter()
                .map(|cell| u64::from(cell.cell))
                .collect::<Vec<_>>()
        };
        let first = ids(h3_tile(&shape, Resolution::Six, None).expect("center cover"));
        let second = ids(h3_tile(&shape, Resolution::Six, None).expect("center cover repeat"));
        assert!(first.len() >= 2);
        assert!(first.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(first, second);
    }
}
