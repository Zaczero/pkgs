use super::*;
use crate::grid::s2::cell::LatLngRect;
use crate::grid::s2::projection::lonlat_to_point;

/// Independent reference coverer: classifies each cell with gometry's
/// general native DE-9IM relate engine (a DIFFERENT code path from the
/// production `NativeRectClassifier`), so the differential test below
/// cross-checks the fast rect classifier against the relate engine.
pub(super) struct RelateCoverContext {
    pub(super) authority: GeographicCoverageSource,
    pub(super) tester: PointBatchTester,
    pub(super) source: Shape,
    pub(super) point_leaves: Option<Vec<CellId>>,
}

/// Sorted-vector membership, named for readable assertions.
pub(super) fn has(cells: &[CellId], cell: CellId) -> bool {
    cells.binary_search(&cell).is_ok()
}

pub(super) fn point_leaves(source: &Shape) -> Option<Vec<CellId>> {
    (!ShapeCountQuery::default().has_segments(source)).then(|| {
        let mut leaves = Vec::new();
        source.for_each_point(|point| {
            let xyz = lonlat_to_point(point.x, point.y);
            let leaf = CellId::from_point(xyz);
            leaves.push(leaf);
            for neighbor in leaf.edge_neighbors() {
                if Cell::from_id(neighbor).contains_point(xyz) {
                    leaves.push(neighbor);
                    for diagonal in neighbor.edge_neighbors() {
                        if diagonal != leaf && Cell::from_id(diagonal).contains_point(xyz) {
                            leaves.push(diagonal);
                        }
                    }
                }
            }
        });
        leaves.sort_unstable();
        leaves.dedup();
        leaves
    })
}

pub(super) fn cover_with_relate_oracle(coverer: Coverer, source: &Shape) -> Covering {
    let oracle_source;
    let source = if let Shape::GeometryCollection(parts) = source {
        oracle_source = Shape::union_all(parts, Strictness::Strict)
            .expect("non-empty collection union should not fail with ordinate dropping");
        &oracle_source
    } else {
        source
    };
    let ctx = RelateCoverContext {
        authority: GeographicCoverageSource::new(source, SphericalGridTarget::S2).unwrap(),
        tester: PointBatchTester::new(source).unwrap(),
        source: source.clone(),
        point_leaves: point_leaves(source),
    };
    // Mirror production's budget-aware DFS + stage-before-commit so
    // geometric differential tests share the same hard-budget threshold
    // semantics (tests use large/unlimited budgets for shape parity).
    let mut queue: BinaryHeap<(Reverse<u8>, CellId, CellClass)> = BinaryHeap::new();
    let mut cells = Vec::new();
    let mut dfs: Vec<(u8, CellId, CellClass)> = Vec::new();
    let mut staged: Vec<(CellId, CellClass)> = Vec::new();
    let limit = coverer.max_cells;
    for face in Face::ALL {
        let id = CellId::from_face(face);
        match classify_relate(&ctx, id) {
            CellClass::Outside => {},
            class @ (CellClass::Interior | CellClass::Boundary) => {
                if coverer.min_level == 0 {
                    queue.push((Reverse(0), id, class));
                } else {
                    dfs.push((0, id, class));
                }
            },
        }
    }
    while let Some((level, id, class)) = dfs.pop() {
        match class {
            CellClass::Outside => unreachable!(),
            CellClass::Interior => {
                let end = id.child_end_at(coverer.min_level);
                let mut cursor = id.child_begin_at(coverer.min_level);
                while cursor != end {
                    cells.push((cursor, true));
                    cursor = cursor.next();
                }
            },
            CellClass::Boundary => {
                staged.clear();
                let child_level = level
                    .checked_add(1)
                    .expect("DFS candidates are below the supported maximum level");
                let end = id.child_end_at(child_level);
                let mut cursor = id.child_begin_at(child_level);
                while cursor != end {
                    match classify_relate(&ctx, cursor) {
                        CellClass::Outside => {},
                        class @ (CellClass::Interior | CellClass::Boundary) => {
                            staged.push((cursor, class));
                        },
                    }
                    cursor = cursor.next();
                }
                if child_level >= coverer.min_level {
                    for &(child, c) in &staged {
                        queue.push((Reverse(child_level), child, c));
                    }
                } else {
                    for &(child, c) in &staged {
                        dfs.push((child_level, child, c));
                    }
                }
            },
        }
    }
    while let Some((Reverse(level), id, class)) = queue.pop() {
        if class == CellClass::Boundary
            && let Some(target) = level
                .checked_add(coverer.level_mod)
                .filter(|&target| target <= coverer.max_level && target <= MAX_LEVEL)
        {
            staged.clear();
            let end = id.child_end_at(target);
            let mut cursor = id.child_begin_at(target);
            while cursor != end {
                match classify_relate(&ctx, cursor) {
                    CellClass::Outside => {},
                    class @ (CellClass::Interior | CellClass::Boundary) => {
                        staged.push((cursor, class));
                    },
                }
                cursor = cursor.next();
            }
            let base = cells
                .len()
                .checked_add(queue.len())
                .expect("S2 reference frontier cardinality fits usize");
            let k = staged.len();
            let fits = limit.is_none_or(|m| base.saturating_add(k) <= m);
            if fits {
                for &(child, c) in &staged {
                    queue.push((Reverse(target), child, c));
                }
                continue;
            }
        }
        let interior = class == CellClass::Interior;
        cells.push((id, interior));
    }
    cells.sort_unstable_by_key(|(id, _)| *id);
    Covering {
        cells,
        stats: CoverStats::default(),
    }
}

pub(super) fn classify_relate(ctx: &RelateCoverContext, id: CellId) -> CellClass {
    if let Some(leaves) = &ctx.point_leaves {
        let from = leaves.partition_point(|&leaf| leaf < id.range_min());
        return if leaves.get(from).is_some_and(|&leaf| leaf <= id.range_max()) {
            CellClass::Boundary
        } else {
            CellClass::Outside
        };
    }
    let cell = Cell::from_id(id);
    let vertices = cell.vertices_lonlat();
    let rect = cell.rect_bound(&vertices);
    if ctx
        .authority
        .classify_rect(
            &ctx.authority
                .admit_rectangular_candidate(super::super::classify::rect_bounds(rect)),
        )
        .expect("exact rectangle classification")
        == AffineRectClass::Outside
    {
        return CellClass::Outside;
    }
    let covered = vertices
        .iter()
        .filter(|vertex| ctx.tester.covers_point(**vertex))
        .count();
    let center_hit = covered < 4 && ctx.tester.covers_point(cell.center_lonlat());
    if rect.crosses_seam() || rect.is_full_lng() {
        // Mirror production: fail-open Boundary for outer, but certify
        // Interior when every positive-width lon window is covered.
        // A polar full-lng bound stays fail-open: vertex longitudes away
        // from its closed pole are not an Outside certificate.
        if covered == 4 && relate_wrapped_rect_is_interior(ctx, rect) {
            return CellClass::Interior;
        }
        return CellClass::Boundary;
    }
    // Independent of the production `NativeRectClassifier`: classify the
    // rect against the source with gometry's general DE-9IM relate engine.
    let rect_shape = box_shape(rect.lng_lo, rect.lat_lo, rect.lng_hi, rect.lat_hi);
    if covered == 4 && ctx.source.covers(&rect_shape).unwrap() {
        return CellClass::Interior;
    }
    if covered > 0 || center_hit {
        return CellClass::Boundary;
    }
    if !ctx.source.intersects(&rect_shape).unwrap() {
        return CellClass::Outside;
    }
    // Match production: rect Outside is the only negative certificate;
    // Boundary/Interior without vertex/center hits fails open (the planar
    // chord proxy must not prune).
    CellClass::Boundary
}

pub(super) fn relate_wrapped_rect_is_interior(ctx: &RelateCoverContext, rect: LatLngRect) -> bool {
    if rect.is_full_lng() {
        let full = box_shape(-180.0, rect.lat_lo, 180.0, rect.lat_hi);
        return ctx.source.covers(&full).unwrap();
    }
    let ((lo0, hi0), second) = rect.lng_windows();
    if hi0 > lo0
        && !ctx
            .source
            .covers(&box_shape(lo0, rect.lat_lo, hi0, rect.lat_hi))
            .unwrap()
    {
        return false;
    }
    match second {
        Some((lo1, hi1)) if hi1 > lo1 => ctx
            .source
            .covers(&box_shape(lo1, rect.lat_lo, hi1, rect.lat_hi))
            .unwrap(),
        _ => true,
    }
}

pub(super) fn box_shape(west: f64, south: f64, east: f64, north: f64) -> Shape {
    let shell = vec![
        Point::new_raw_xy(west, south),
        Point::new_raw_xy(east, south),
        Point::new_raw_xy(east, north),
        Point::new_raw_xy(west, north),
        Point::new_raw_xy(west, south),
    ];
    Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&shell).unwrap()).unwrap(),
        Vec::new(),
    ))
}

pub(super) fn polygon_with_hole() -> Shape {
    let shell = vec![
        Point::new_raw_xy(10.0, 10.0),
        Point::new_raw_xy(20.0, 10.0),
        Point::new_raw_xy(20.0, 20.0),
        Point::new_raw_xy(10.0, 20.0),
        Point::new_raw_xy(10.0, 10.0),
    ];
    let hole = vec![
        Point::new_raw_xy(13.0, 13.0),
        Point::new_raw_xy(13.0, 17.0),
        Point::new_raw_xy(17.0, 17.0),
        Point::new_raw_xy(17.0, 13.0),
        Point::new_raw_xy(13.0, 13.0),
    ];
    Shape::Polygon(Polygon::new(
        Ring::from_closed(CoordSeq::try_from_points(&shell).unwrap()).unwrap(),
        vec![Ring::from_closed(CoordSeq::try_from_points(&hole).unwrap()).unwrap()],
    ))
}

pub(super) fn classify_with_native(source: &Shape, id: CellId) -> CellClass {
    let affine = GeographicCoverageSource::new(source, SphericalGridTarget::S2)
        .expect("test source carrier allocates");
    let ctx = CoverContext::prepare(source, &affine, WorkingShapeRelation::Identity).unwrap();
    classify(&ctx, id)
}

pub(super) fn fixed(level: u8) -> Coverer {
    Coverer {
        min_level: level,
        max_level: level,
        level_mod: 1,
        max_cells: Some(usize::try_from(crate::grid::GRID_MAX_CELLS).unwrap()),
        target_cells: 8,
    }
}

/// Differential parity between the native coverer and the independent
/// relate-based oracle, for one shape across a ladder of fixed levels.
///
/// The oracle re-derives the covering with a full `classify_relate` per
/// candidate cell, so its cost grows as 4^level while the native side stays
/// output-sensitive. Each shape is therefore its own test — the cases are
/// independent, so they run concurrently and a failure names its own shape —
/// and each level ladder stops where the parity it demonstrates stops
/// changing rather than at the deepest level the oracle can still reach.
pub(super) fn assert_relate_oracle_parity(name: &str, source: &Shape, levels: &[u8]) {
    for level in levels {
        let coverer = fixed(*level);
        let native = coverer
            .cover_identity(source)
            .expect("small fixed-level covering is within budget");
        let oracle = cover_with_relate_oracle(coverer, source);
        assert_eq!(native.outer(), oracle.outer(), "{name} level {level} outer");
        assert_eq!(
            native.interior(),
            oracle.interior(),
            "{name} level {level} interior"
        );
    }
}

pub(super) fn adjacent_areal_collection() -> Shape {
    Shape::geometry_collection(vec![
        box_shape(-1.0, -1.0, 0.03, 1.0),
        box_shape(0.03, -1.0, 1.0, 1.0),
    ])
    .expect("test collection is valid")
}

pub(super) fn overlapping_areal_collection() -> Shape {
    Shape::geometry_collection(vec![
        box_shape(-1.0, -1.0, 0.25, 1.0),
        box_shape(-0.25, -1.0, 1.0, 1.0),
    ])
    .expect("test collection is valid")
}

pub(super) fn nested_areal_collection() -> Shape {
    Shape::geometry_collection(vec![
        box_shape(2.0, 2.0, 3.0, 3.0),
        box_shape(2.51, 2.45, 2.55, 2.56),
    ])
    .expect("test collection is valid")
}

pub(super) fn mixed_dimension_collection() -> Shape {
    Shape::geometry_collection(vec![
        box_shape(-2.0, -2.0, 2.0, 2.0),
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(3.0, -1.0),
                    Point::new_raw_xy(6.0, 1.0),
                ])
                .unwrap(),
            )
            .expect("test line is valid"),
        ),
        Shape::MultiPoint(
            CoordSeq::try_from_points(&[Point::new_raw_xy(-4.0, 0.0), Point::new_raw_xy(0.0, 4.0)])
                .unwrap()
                .into(),
        ),
    ])
    .expect("test collection is valid")
}

pub(super) fn antimeridian_seam_source() -> Shape {
    match (
        box_shape(179.5, -1.0, 180.0, 1.0),
        box_shape(-180.0, -1.0, -179.5, 1.0),
    ) {
        (Shape::Polygon(a), Shape::Polygon(b)) => Shape::MultiPolygon(vec![a, b].into()),
        _ => unreachable!(),
    }
}
