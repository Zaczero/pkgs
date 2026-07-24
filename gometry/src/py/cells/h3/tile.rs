use super::*;
use crate::collections::{HashMap, HashMapExt, HashSet, HashSetExt};
use crate::geometry::{
    Point, PolePosition, Polygon, Ring, Shape, ShapeData, geographic_crossing_bounds,
    pole_position, shape_encloses_pole, shape_has_polar_ring, shape_spans_full_longitude,
};
use crate::grid::{CoverBudgetExceeded, ensure_cover_budget};
use crate::py::cells::*;
use crate::py::functions::predicate::{Predicate, topology_scalar_pair};

/// Record one cell's `(matched, fully_contained)` verdict into the coverage
/// map, OR-ing the fully-contained flag across parts. A free function (not a
/// closure) so the `&mut coverage` borrow releases between insertions and the
/// budget check can read `coverage.len()`.
fn record_cell(coverage: &mut HashMap<CellIndex, bool>, cell: CellIndex, fully: bool) {
    coverage
        .entry(cell)
        .and_modify(|flag| *flag = *flag || fully)
        .or_insert(fully);
}

/// Whether the unsplit source is inherently spherical (poles / full longitude /
/// antimeridian) so the planar outline barrier cannot be trusted.
fn source_needs_checked_flood(source: &Shape) -> bool {
    pole_position(source, true) != PolePosition::Exterior
        || pole_position(source, false) != PolePosition::Exterior
        || shape_has_polar_ring(source)
        || source.crosses_antimeridian()
        || shape_spans_full_longitude(source)
}

/// Whether a raw H3 cell polygon is a seam-wrapping or polar-winding cell —
/// part of the resolution-aware barrier certificate.
fn cell_is_spherical(cell_shape: &Shape) -> bool {
    cell_shape.crosses_antimeridian() || shape_has_polar_ring(cell_shape)
}

/// Geographic classifier against the unsplit whole source (H3 covers are always
/// WGS84). Used when `checked` is true so seam/polar cell probes cannot take
/// the false-middle planar path.
fn checked_predicate(mode: CellRule, source: &ShapeData, cell: CellIndex) -> (bool, bool) {
    match mode {
        CellRule::Center => {
            let center = LatLng::from(cell);
            let probe = ShapeData::from(Shape::Point(Point::new_unchecked_xy(
                center.lng(),
                center.lat(),
            )));
            let inside = topology_scalar_pair(&Predicate::Covers.spec(), source, &probe, true);
            (inside, inside)
        },
        CellRule::Within => {
            let probe = ShapeData::from(h3_cell_shape(cell));
            let inside = topology_scalar_pair(&Predicate::Covers.spec(), source, &probe, true);
            (inside, inside)
        },
        CellRule::Overlap => {
            let probe = ShapeData::from(h3_cell_shape(cell));
            let matched = topology_scalar_pair(&Predicate::Intersects.spec(), source, &probe, true);
            let fully =
                matched && topology_scalar_pair(&Predicate::Covers.spec(), source, &probe, true);
            (matched, fully)
        },
        CellRule::Bbox => {
            let bbox = ShapeData::from(h3_cell_bbox_shape(cell));
            let matched = topology_scalar_pair(&Predicate::Intersects.spec(), source, &bbox, true);
            let hex = ShapeData::from(h3_cell_shape(cell));
            let fully =
                matched && topology_scalar_pair(&Predicate::Covers.spec(), source, &hex, true);
            (matched, fully)
        },
    }
}

/// Geographic classifier reusing a prebuilt cell hexagon (outline/first-inner).
fn checked_predicate_with_shape(
    mode: CellRule,
    source: &ShapeData,
    cell: CellIndex,
    cell_shape: &Shape,
) -> (bool, bool) {
    match mode {
        CellRule::Center => checked_predicate(mode, source, cell),
        CellRule::Within => {
            let probe = ShapeData::from(cell_shape.clone());
            let inside = topology_scalar_pair(&Predicate::Covers.spec(), source, &probe, true);
            (inside, inside)
        },
        CellRule::Overlap => {
            let probe = ShapeData::from(cell_shape.clone());
            let matched = topology_scalar_pair(&Predicate::Intersects.spec(), source, &probe, true);
            let fully =
                matched && topology_scalar_pair(&Predicate::Covers.spec(), source, &probe, true);
            (matched, fully)
        },
        CellRule::Bbox => {
            // Bbox rule still needs the logical geographic box, not the hexagon.
            checked_predicate(mode, source, cell)
        },
    }
}

/// Planar (unchecked) per-cell classifier against one split polygon part —
/// byte-for-byte the historical fast path.
fn planar_predicate(mode: CellRule, source: &Shape, cell: CellIndex) -> (bool, bool) {
    match mode {
        CellRule::Center => {
            let center = LatLng::from(cell);
            let inside = source.covers_point(Point::new_unchecked_xy(center.lng(), center.lat()));
            (inside, inside)
        },
        CellRule::Within => {
            let inside = source.covers(&h3_cell_shape(cell));
            (inside, inside)
        },
        CellRule::Overlap => {
            let cell_shape = h3_cell_shape(cell);
            let overlaps = source.intersects(&cell_shape);
            (overlaps, overlaps && source.covers(&cell_shape))
        },
        CellRule::Bbox => {
            let overlaps = source.intersects(&h3_cell_bbox_shape(cell));
            (overlaps, overlaps && source.covers(&h3_cell_shape(cell)))
        },
    }
}

fn planar_predicate_with_shape(
    mode: CellRule,
    source: &Shape,
    cell: CellIndex,
    cell_shape: &Shape,
) -> (bool, bool) {
    match mode {
        CellRule::Center => planar_predicate(mode, source, cell),
        CellRule::Within => {
            let inside = source.covers(cell_shape);
            (inside, inside)
        },
        CellRule::Overlap => {
            let overlaps = source.intersects(cell_shape);
            (overlaps, overlaps && source.covers(cell_shape))
        },
        CellRule::Bbox => {
            let overlaps = source.intersects(&h3_cell_bbox_shape(cell));
            (overlaps, overlaps && source.covers(cell_shape))
        },
    }
}

/// A native H3 polyfill: each polygon is flood-filled independently from the
/// cells containing its vertices and an interior point, then the per-polygon
/// coverages are unioned (a cell is fully contained when it is fully inside
/// *any* part). Ordinary mid-latitude sources use planar containment on the
/// split working shape; polar / full-longitude / seam sources (and outlines
/// that enter a spherical H3 cell) classify every cell against the unsplit
/// source with the geographic topology gate.
pub(super) fn h3_tile(
    shape: &Shape,
    unsplit_source: &ShapeData,
    resolution: Resolution,
    mode: CellRule,
    max_cells: Option<usize>,
) -> Result<Vec<TiledCell>, CoverBudgetExceeded> {
    // Union every part's coverage, OR-ing the fully-contained flag (a cell can
    // straddle one part's edge yet sit fully inside another). One pass; the map
    // is bounded by `max_cells` (when set), checked at every insertion, so a
    // world-scale polygon at a fine resolution fails before flooding memory.
    let mut coverage: HashMap<CellIndex, bool> = HashMap::new();
    h3_tile_shape(
        shape,
        unsplit_source,
        resolution,
        mode,
        max_cells,
        &mut coverage,
    )?;
    Ok(coverage
        .into_iter()
        .map(|(cell, is_fully_contained)| TiledCell {
            cell,
            is_fully_contained,
        })
        .collect())
}

/// Accumulate `shape`'s H3 coverage into `coverage` (key = cell, value =
/// certified fully-inside the source). Areal parts flood; puntal/lineal parts
/// contribute only the cells they touch, and only under the touch-based rules
/// (`overlap`/`bbox`) — `center`/`within` are areal-containment rules, so a
/// point or line selects nothing under them, exactly like the s2/geohash/tile
/// coverers. This makes ``h3_cover(...)`` accept any geometry, not just
/// polygons, closing the one cross-grid input asymmetry.
pub(super) fn h3_tile_shape(
    shape: &Shape,
    unsplit_source: &ShapeData,
    resolution: Resolution,
    mode: CellRule,
    max_cells: Option<usize>,
    coverage: &mut HashMap<CellIndex, bool>,
) -> Result<(), CoverBudgetExceeded> {
    match shape {
        Shape::Polygon(polygon) => h3_tile_polygon(
            polygon,
            unsplit_source,
            resolution,
            mode,
            max_cells,
            coverage,
        )?,
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                h3_tile_polygon(
                    polygon,
                    unsplit_source,
                    resolution,
                    mode,
                    max_cells,
                    coverage,
                )?;
            }
        },
        Shape::GeometryCollection(parts) => {
            for part in parts {
                h3_tile_shape(part, unsplit_source, resolution, mode, max_cells, coverage)?;
            }
        },
        // Puntal/lineal sources qualify cells only under the touch-based rules.
        _ if !matches!(mode, CellRule::Overlap | CellRule::Bbox) => {},
        Shape::Point(point) => h3_tile_point(*point, resolution, max_cells, coverage)?,
        Shape::MultiPoint(points) => {
            for point in points {
                h3_tile_point(point, resolution, max_cells, coverage)?;
            }
        },
        Shape::LineString(coords) => {
            h3_tile_line(
                unsplit_source,
                coords,
                resolution,
                mode,
                max_cells,
                coverage,
            )?;
        },
        Shape::MultiLineString(lines) => {
            for coords in lines {
                h3_tile_line(
                    unsplit_source,
                    coords,
                    resolution,
                    mode,
                    max_cells,
                    coverage,
                )?;
            }
        },
        Shape::Empty(..) => {},
    }
    Ok(())
}

/// The single cell containing a point. A point never certifies a (larger) cell
/// as fully-inside, so the interior flag is always `false`.
pub(super) fn h3_tile_point(
    point: Point,
    resolution: Resolution,
    max_cells: Option<usize>,
    coverage: &mut HashMap<CellIndex, bool>,
) -> Result<(), CoverBudgetExceeded> {
    if let Ok(latlng) = h3_latlng(point) {
        coverage.entry(latlng.to_cell(resolution)).or_insert(false);
        ensure_cover_budget(coverage.len(), max_cells)?;
    }
    Ok(())
}

/// The cells a line passes through: trace the polyline (`get_edge_cells`),
/// buffer each by one ring so the approximate trace can't skip a crossed cell,
/// then keep the candidates that actually meet the source — the hexagon for
/// `overlap`, the cell bbox for `bbox`. A line never fully covers an areal
/// cell, so the interior flag stays `false`. Probes always use the geographic
/// topology gate against the unsplit source so seam/polar cell shapes cannot
/// take a false-middle planar path.
pub(super) fn h3_tile_line(
    source: &ShapeData,
    coords: &crate::geometry::CoordSeq,
    resolution: Resolution,
    mode: CellRule,
    max_cells: Option<usize>,
    coverage: &mut HashMap<CellIndex, bool>,
) -> Result<(), CoverBudgetExceeded> {
    let mut seen: HashSet<CellIndex> = HashSet::new();
    let mut scratch = [0_u64; 7];
    for edge_cell in get_edge_cells(coords, resolution, max_cells)? {
        for &raw in h3_one_ring(edge_cell, &mut scratch) {
            let cell = CellIndex::try_from(raw).expect("h3o grid disk returned a valid cell");
            if seen.insert(cell) {
                let probe = if mode == CellRule::Bbox {
                    ShapeData::from(h3_cell_bbox_shape(cell))
                } else {
                    ShapeData::from(h3_cell_shape(cell))
                };
                if topology_scalar_pair(&Predicate::Intersects.spec(), source, &probe, true) {
                    coverage.entry(cell).or_insert(false);
                    ensure_cover_budget(coverage.len(), max_cells)?;
                }
            }
        }
    }
    Ok(())
}

/// Classify one cell and, if matched, record it. Returns the `(matched, fully)`
/// verdict so callers can seed the flood from matched neighbors.
fn classify_cell(
    mode: CellRule,
    checked: bool,
    unsplit_source: &ShapeData,
    part_source: &Shape,
    cell: CellIndex,
    raw_cell: &Shape,
    coverage: &mut HashMap<CellIndex, bool>,
    max_cells: Option<usize>,
) -> Result<(bool, bool), CoverBudgetExceeded> {
    let (matched, fully) = if checked {
        checked_predicate_with_shape(mode, unsplit_source, cell, raw_cell)
    } else {
        planar_predicate_with_shape(mode, part_source, cell, raw_cell)
    };
    if matched {
        record_cell(coverage, cell, fully);
        ensure_cover_budget(coverage.len(), max_cells)?;
    }
    Ok((matched, fully))
}

/// Sub-cell seed path: vertices + interior point when the outline band is empty.
fn h3_tile_polygon_seeds(
    polygon: &Polygon,
    part_source: &Shape,
    unsplit_source: &ShapeData,
    resolution: Resolution,
    mode: CellRule,
    max_cells: Option<usize>,
    coverage: &mut HashMap<CellIndex, bool>,
    seen: &mut HashSet<CellIndex>,
    mut checked: bool,
) -> Result<(), CoverBudgetExceeded> {
    let mut seed_band: Vec<(CellIndex, Shape)> = Vec::new();
    let mut push_seed = |seed: CellIndex| {
        if seen.insert(seed) {
            let raw_cell = h3_cell_shape(seed);
            checked |= cell_is_spherical(&raw_cell);
            seed_band.push((seed, raw_cell));
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
    for (seed, raw_cell) in &seed_band {
        classify_cell(
            mode,
            checked,
            unsplit_source,
            part_source,
            *seed,
            raw_cell,
            coverage,
            max_cells,
        )?;
    }
    Ok(())
}

/// Checked inward flood: every first-seen neighbor is tested against the
/// unsplit source; only matched cells are recorded and enqueued.
fn flood_checked(
    mode: CellRule,
    unsplit_source: &ShapeData,
    max_cells: Option<usize>,
    coverage: &mut HashMap<CellIndex, bool>,
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
                    let (matched, fully) = checked_predicate(mode, unsplit_source, neighbor);
                    if matched {
                        record_cell(coverage, neighbor, fully);
                        ensure_cover_budget(coverage.len(), max_cells)?;
                        next.push(neighbor);
                    }
                }
            }
        }
        frontier = next;
    }
    Ok(())
}

/// Unchecked inward flood: first-seen neighbors are fully-contained interior
/// with no geometry test (outline band is a closed barrier in `seen`).
fn flood_unchecked(
    max_cells: Option<usize>,
    coverage: &mut HashMap<CellIndex, bool>,
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
                    record_cell(coverage, neighbor, true);
                    ensure_cover_budget(coverage.len(), max_cells)?;
                    next.push(neighbor);
                }
            }
        }
        frontier = next;
    }
    Ok(())
}

/// Flood-fill one polygon's H3 coverage into `coverage` (keyed by cell, value
/// = fully-contained-by-this-polygon, OR-ed into any existing entry).
pub(super) fn h3_tile_polygon(
    polygon: &Polygon,
    unsplit_source: &ShapeData,
    resolution: Resolution,
    mode: CellRule,
    max_cells: Option<usize>,
    coverage: &mut HashMap<CellIndex, bool>,
) -> Result<(), CoverBudgetExceeded> {
    // Per-part planar source (split working shape). Checked mode classifies
    // against the whole unsplit source instead.
    let part_source = Shape::Polygon(polygon.clone());

    // Source-level gate is resolution-independent and evaluated once per part
    // from the unsplit whole (holes/poles need the original container).
    let mut checked = source_needs_checked_flood(unsplit_source.shape());

    // `seen` doubles as the propagation barrier: every band cell — matched OR
    // rejected — lands here, so the inward flood cannot escape the outline.
    let mut seen: HashSet<CellIndex> = HashSet::new();
    let mut scratch = [0_u64; 7];

    // 1. OUTLINE BAND: materialize every first-seen ring-buffer cell and its
    // raw hexagon BEFORE any classification, so the barrier certificate can
    // inspect rejected cells too and `checked` is known before recording.
    let mut outline_band: Vec<(CellIndex, Shape)> = Vec::new();
    for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
        for edge_cell in get_edge_cells(ring.coords(), resolution, max_cells)? {
            for &raw in h3_one_ring(edge_cell, &mut scratch) {
                let cell = CellIndex::try_from(raw).expect("h3o grid disk returned a valid cell");
                if seen.insert(cell) {
                    let raw_cell = h3_cell_shape(cell);
                    checked |= cell_is_spherical(&raw_cell);
                    outline_band.push((cell, raw_cell));
                }
            }
        }
    }

    if outline_band.is_empty() {
        return h3_tile_polygon_seeds(
            polygon,
            &part_source,
            unsplit_source,
            resolution,
            mode,
            max_cells,
            coverage,
            &mut seen,
            checked,
        );
    }

    // 2. BARRIER GATE on first-inner candidates: inspect every first-seen
    // neighbor of the outline band (matched or rejected) for spherical cells,
    // WITHOUT inserting them into `seen` (that would expand the barrier and
    // could strand true interior cells reachable only through the flood).
    let mut shape_cache: HashMap<CellIndex, Shape> = HashMap::new();
    for &(cell, _) in &outline_band {
        for &raw in h3_one_ring(cell, &mut scratch) {
            let neighbor = CellIndex::try_from(raw).expect("h3o grid disk returned a valid cell");
            if !seen.contains(&neighbor) && !shape_cache.contains_key(&neighbor) {
                let raw_cell = h3_cell_shape(neighbor);
                checked |= cell_is_spherical(&raw_cell);
                shape_cache.insert(neighbor, raw_cell);
            }
        }
    }

    // 3. CLASSIFY outline band. Only matched cells are recorded; rejected
    // cells stay in `seen` as the barrier. `checked` is now final.
    let mut outline: Vec<CellIndex> = Vec::new();
    for (cell, raw_cell) in &outline_band {
        let (matched, _) = classify_cell(
            mode,
            checked,
            unsplit_source,
            &part_source,
            *cell,
            raw_cell,
            coverage,
            max_cells,
        )?;
        if matched {
            outline.push(*cell);
        }
    }

    // 4. OUTERMOST INNER RING: neighbors of the matched outline seed the flood.
    let mut frontier: Vec<CellIndex> = Vec::new();
    for &cell in &outline {
        for &raw in h3_one_ring(cell, &mut scratch) {
            let neighbor = CellIndex::try_from(raw).expect("h3o grid disk returned a valid cell");
            if seen.insert(neighbor) {
                let raw_cell = shape_cache
                    .remove(&neighbor)
                    .unwrap_or_else(|| h3_cell_shape(neighbor));
                let (matched, _) = classify_cell(
                    mode,
                    checked,
                    unsplit_source,
                    &part_source,
                    neighbor,
                    &raw_cell,
                    coverage,
                    max_cells,
                )?;
                if matched {
                    frontier.push(neighbor);
                }
            }
        }
    }
    drop(shape_cache);

    // 5. INWARD PROPAGATION — checked vs unchecked as separate loops so the
    // common path keeps no per-neighbor branch.
    if checked {
        flood_checked(
            mode,
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

/// The cells crossed by a ring's edges (an approximate trace): each edge is
/// sampled at roughly one point per cell diameter and resolved to its cell,
/// mirroring h3o's native `get_edge_cells` so the outline matches the tiler.
pub(super) fn get_edge_cells(
    coords: &crate::geometry::CoordSeq,
    resolution: Resolution,
    max_cells: Option<usize>,
) -> Result<Vec<CellIndex>, CoverBudgetExceeded> {
    let points: Vec<Point> = coords.points().collect();
    let mut cells = Vec::new();
    for &[start, end] in points.array_windows::<2>() {
        let count = line_hex_estimate(start, end, resolution);
        for index in 0..count {
            let fraction = index as f64 / count as f64;
            let lng = start.x + (end.x - start.x) * fraction;
            let lat = start.y + (end.y - start.y) * fraction;
            if let Ok(latlng) = LatLng::new(lat, lng) {
                cells.push(latlng.to_cell(resolution));
                // The edge trace itself is unbounded at a fine resolution over
                // a large span; a cover whose outline exceeds the budget can
                // never fit, so fail here before materializing every sample.
                ensure_cover_budget(cells.len(), max_cells)?;
            }
        }
    }
    Ok(cells)
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

fn rect_shape(west: f64, south: f64, east: f64, north: f64) -> Shape {
    Shape::Polygon(rect_polygon(west, south, east, north))
}

fn rect_polygon(west: f64, south: f64, east: f64, north: f64) -> Polygon {
    Polygon::new(
        Ring::from_trusted_closed(vec![
            Point::new_unchecked_xy(west, south),
            Point::new_unchecked_xy(east, south),
            Point::new_unchecked_xy(east, north),
            Point::new_unchecked_xy(west, north),
            Point::new_unchecked_xy(west, south),
        ]),
        Vec::new(),
    )
}

/// Logical geographic cell bbox as a closed rectangle `Shape` (or a two-part
/// MultiPolygon when the cell crosses ±180). Polar-winding cells span full
/// longitude and extend to the enclosed pole; seam-crossing cells use the
/// west>east circular interval rather than a false-middle raw min/max.
pub(super) fn h3_cell_bbox_shape(cell: CellIndex) -> Shape {
    let cell_shape = h3_cell_shape(cell);
    if shape_has_polar_ring(&cell_shape) {
        let mut south = f64::INFINITY;
        let mut north = f64::NEG_INFINITY;
        for latlng in cell.boundary().iter() {
            south = south.min(latlng.lat());
            north = north.max(latlng.lat());
        }
        if shape_encloses_pole(&cell_shape, true) {
            north = 90.0;
        }
        if shape_encloses_pole(&cell_shape, false) {
            south = -90.0;
        }
        return rect_shape(-180.0, south, 180.0, north);
    }
    if cell_shape.crosses_antimeridian()
        && let Some(bounds) = geographic_crossing_bounds(&cell_shape)
    {
        let west = bounds.minx();
        let south = bounds.miny();
        let east = bounds.maxx();
        let north = bounds.maxy();
        // west>east is the geographic crossing convention.
        if west > east {
            return Shape::MultiPolygon(vec![
                rect_polygon(west, south, 180.0, north),
                rect_polygon(-180.0, south, east, north),
            ]);
        }
        return rect_shape(west, south, east, north);
    }
    let mut west = f64::INFINITY;
    let mut south = f64::INFINITY;
    let mut east = f64::NEG_INFINITY;
    let mut north = f64::NEG_INFINITY;
    for latlng in cell.boundary().iter() {
        west = west.min(latlng.lng());
        south = south.min(latlng.lat());
        east = east.max(latlng.lng());
        north = north.max(latlng.lat());
    }
    rect_shape(west, south, east, north)
}
