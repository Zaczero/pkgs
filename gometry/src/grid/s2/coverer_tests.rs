use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::geometry::{
    CoordSeq, LineSeq, Point, PointBatchTester, Polygon, Ring, Shape, Strictness,
};
use crate::grid::affine_source::{GridAffineSource, SphericalGridTarget, WorkingShapeRelation};
use crate::grid::coverer::CellClass;
use crate::grid::s2::cell::Cell;
use crate::grid::s2::cellid::CellId;
use crate::grid::s2::coverer::{
    AffineRectClass, CoverContext, Coverer, Covering, certified_rect_windows, classify,
};
use crate::grid::s2::projection::{MAX_LEVEL, NUM_FACES};
use crate::grid::s2::seam::SourceWindows;
use crate::grid::spherical_arc::{
    CertifiedDegreeWindows, CertifiedLongitudeDegrees, DegreeWindowResult,
};

/// Independent reference coverer: classifies each cell with gometry's
/// general native DE-9IM relate engine (a DIFFERENT code path from the
/// production `NativeRectClassifier`), so the differential test below
/// cross-checks the fast rect classifier against the relate engine.
struct RelateCoverContext {
    windows: SourceWindows,
    tester: PointBatchTester,
    source: Shape,
    point_leaves: Option<Vec<CellId>>,
}

/// Sorted-vector membership, named for readable assertions.
fn has(cells: &[CellId], cell: CellId) -> bool {
    cells.binary_search(&cell).is_ok()
}

fn point_leaves(source: &Shape) -> Option<Vec<CellId>> {
    (source.segment_count() == 0).then(|| {
        let mut leaves = Vec::new();
        source.for_each_point(|point| {
            let xyz = super::super::projection::lonlat_to_point(point.x, point.y);
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

fn cover_with_relate_oracle(coverer: Coverer, source: &Shape) -> Covering {
    let oracle_source;
    let source = if let Shape::GeometryCollection(parts) = source {
        oracle_source = Shape::union_all(parts, Strictness::Strict)
            .expect("non-empty collection union should not fail with ordinate dropping");
        &oracle_source
    } else {
        source
    };
    let ctx = RelateCoverContext {
        windows: SourceWindows::new(source),
        tester: PointBatchTester::new(source),
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
    for face in 0..NUM_FACES {
        let id = CellId::from_face(face);
        match classify_relate(&ctx, id) {
            CellClass::Outside => {},
            class => {
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
                let end = id.child_end_at(level + 1);
                let mut cursor = id.child_begin_at(level + 1);
                while cursor != end {
                    match classify_relate(&ctx, cursor) {
                        CellClass::Outside => {},
                        c => staged.push((cursor, c)),
                    }
                    cursor = cursor.next();
                }
                let child_level = level + 1;
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
        let can_refine = class == CellClass::Boundary
            && level + coverer.level_mod <= coverer.max_level
            && level + coverer.level_mod <= MAX_LEVEL;
        if can_refine {
            let target = level + coverer.level_mod;
            staged.clear();
            let end = id.child_end_at(target);
            let mut cursor = id.child_begin_at(target);
            while cursor != end {
                match classify_relate(&ctx, cursor) {
                    CellClass::Outside => {},
                    c => staged.push((cursor, c)),
                }
                cursor = cursor.next();
            }
            let base = cells.len() + queue.len();
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
    Covering { cells }
}

fn classify_relate(ctx: &RelateCoverContext, id: CellId) -> CellClass {
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
    if !ctx.windows.may_overlap(rect) {
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
    if covered == 4 && ctx.source.covers(&rect_shape) {
        return CellClass::Interior;
    }
    if covered > 0 || center_hit {
        return CellClass::Boundary;
    }
    if !ctx.source.intersects(&rect_shape) {
        return CellClass::Outside;
    }
    // Match production: rect Outside is the only negative certificate;
    // Boundary/Interior without vertex/center hits fails open (the planar
    // chord proxy must not prune).
    CellClass::Boundary
}

fn relate_wrapped_rect_is_interior(
    ctx: &RelateCoverContext,
    rect: super::super::cell::LatLngRect,
) -> bool {
    if rect.is_full_lng() {
        let full = box_shape(-180.0, rect.lat_lo, 180.0, rect.lat_hi);
        return ctx.source.covers(&full);
    }
    let ((lo0, hi0), second) = rect.lng_windows();
    if hi0 > lo0
        && !ctx
            .source
            .covers(&box_shape(lo0, rect.lat_lo, hi0, rect.lat_hi))
    {
        return false;
    }
    match second {
        Some((lo1, hi1)) if hi1 > lo1 => {
            ctx.source
                .covers(&box_shape(lo1, rect.lat_lo, hi1, rect.lat_hi))
        },
        _ => true,
    }
}

fn box_shape(west: f64, south: f64, east: f64, north: f64) -> Shape {
    let shell = vec![
        Point::new_unchecked_xy(west, south),
        Point::new_unchecked_xy(east, south),
        Point::new_unchecked_xy(east, north),
        Point::new_unchecked_xy(west, north),
        Point::new_unchecked_xy(west, south),
    ];
    Shape::Polygon(Polygon::new(Ring::from_trusted_closed(shell), Vec::new()))
}

fn polygon_with_hole() -> Shape {
    let shell = vec![
        Point::new_unchecked_xy(10.0, 10.0),
        Point::new_unchecked_xy(20.0, 10.0),
        Point::new_unchecked_xy(20.0, 20.0),
        Point::new_unchecked_xy(10.0, 20.0),
        Point::new_unchecked_xy(10.0, 10.0),
    ];
    let hole = vec![
        Point::new_unchecked_xy(13.0, 13.0),
        Point::new_unchecked_xy(13.0, 17.0),
        Point::new_unchecked_xy(17.0, 17.0),
        Point::new_unchecked_xy(17.0, 13.0),
        Point::new_unchecked_xy(13.0, 13.0),
    ];
    Shape::Polygon(Polygon::new(Ring::from_trusted_closed(shell), vec![
        Ring::from_trusted_closed(hole),
    ]))
}

fn classify_with_native(source: &Shape, id: CellId) -> CellClass {
    let affine = GridAffineSource::new(source, SphericalGridTarget::S2)
        .expect("test source carrier allocates");
    let ctx = CoverContext::prepare(source, &affine, WorkingShapeRelation::Identity);
    classify(&ctx, id)
}

fn fixed(level: u8) -> Coverer {
    Coverer {
        min_level: level,
        max_level: level,
        level_mod: 1,
        max_cells: Some(crate::grid::GRID_MAX_CELLS),
        target_cells: 8,
    }
}

#[test]
fn productive_cache_is_isolated_between_sources() {
    let line = Shape::LineString(
        LineSeq::try_new(CoordSeq::from(vec![
            Point::new_unchecked_xy(-50.0, -40.0),
            Point::new_unchecked_xy(50.0, -40.0),
        ]))
        .expect("fixture line is valid"),
    );
    let adaptive = Coverer {
        min_level: 0,
        max_level: MAX_LEVEL,
        level_mod: 1,
        max_cells: None,
        target_cells: 8,
    };
    adaptive
        .cover_identity(&line)
        .expect("first source covers without a budget");

    let face = CellId::from_token("3").expect("canonical level-zero face token");
    let point = Shape::Point(Cell::from_id(face).center_lonlat());
    let covering = fixed(0)
        .cover_identity(&point)
        .expect("point has one level-zero owner");
    assert_eq!(covering.outer(), vec![face]);
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
fn assert_relate_oracle_parity(name: &str, source: &Shape, levels: &[u8]) {
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

fn adjacent_areal_collection() -> Shape {
    Shape::GeometryCollection(vec![
        box_shape(-1.0, -1.0, 0.03, 1.0),
        box_shape(0.03, -1.0, 1.0, 1.0),
    ])
}

fn overlapping_areal_collection() -> Shape {
    Shape::GeometryCollection(vec![
        box_shape(-1.0, -1.0, 0.25, 1.0),
        box_shape(-0.25, -1.0, 1.0, 1.0),
    ])
}

fn nested_areal_collection() -> Shape {
    Shape::GeometryCollection(vec![
        box_shape(2.0, 2.0, 3.0, 3.0),
        box_shape(2.51, 2.45, 2.55, 2.56),
    ])
}

fn mixed_dimension_collection() -> Shape {
    Shape::GeometryCollection(vec![
        box_shape(-2.0, -2.0, 2.0, 2.0),
        Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new_unchecked_xy(3.0, -1.0),
                Point::new_unchecked_xy(6.0, 1.0),
            ]))
            .expect("test line is valid"),
        ),
        Shape::MultiPoint(CoordSeq::from(vec![
            Point::new_unchecked_xy(-4.0, 0.0),
            Point::new_unchecked_xy(0.0, 4.0),
        ])),
    ])
}

fn antimeridian_seam_source() -> Shape {
    match (
        box_shape(179.5, -1.0, 180.0, 1.0),
        box_shape(-180.0, -1.0, -179.5, 1.0),
    ) {
        (Shape::Polygon(a), Shape::Polygon(b)) => Shape::MultiPolygon(vec![a, b]),
        _ => unreachable!(),
    }
}

#[test]
fn native_coverer_matches_relate_oracle_on_a_box() {
    assert_relate_oracle_parity("box", &box_shape(13.0, 52.0, 14.0, 53.0), &[6, 8, 10]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_a_holed_polygon() {
    assert_relate_oracle_parity("hole", &polygon_with_hole(), &[5, 7, 9]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_a_line() {
    let source = Shape::LineString(
        LineSeq::try_new(CoordSeq::from(vec![
            Point::new_unchecked_xy(-3.0, -2.0),
            Point::new_unchecked_xy(4.0, 3.0),
        ]))
        .expect("test line is valid"),
    );
    assert_relate_oracle_parity("line", &source, &[5, 7, 9]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_a_mixed_dimension_collection() {
    assert_relate_oracle_parity("mixed", &mixed_dimension_collection(), &[5, 7]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_an_adjacent_areal_collection() {
    assert_relate_oracle_parity("adjacent-areal", &adjacent_areal_collection(), &[8, 10]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_an_overlapping_areal_collection() {
    assert_relate_oracle_parity("overlapping-areal", &overlapping_areal_collection(), &[
        6, 8,
    ]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_a_nested_areal_collection() {
    assert_relate_oracle_parity("nested-areal", &nested_areal_collection(), &[8, 10]);
}

#[test]
fn native_coverer_matches_relate_oracle_on_a_thin_boundary() {
    assert_relate_oracle_parity("thin-boundary", &box_shape(-0.25, -0.05, 0.25, 0.05), &[
        6, 8, 10,
    ]);
}

#[test]
fn native_coverer_matches_relate_oracle_across_the_antimeridian() {
    assert_relate_oracle_parity("antimeridian", &antimeridian_seam_source(), &[4, 6, 8]);
}

#[test]
fn multi_areal_collection_internal_rings_do_not_block_interior_certificates() {
    let adjacent = Shape::GeometryCollection(vec![
        box_shape(-1.0, -1.0, 0.03, 1.0),
        box_shape(0.03, -1.0, 1.0, 1.0),
    ]);
    let seam_cell = CellId::from_lonlat(0.03, 0.0).parent(12).expect("level 12");
    assert_eq!(
        classify_with_native(&adjacent, seam_cell),
        CellClass::Interior,
        "adjacent collection seam cell {}",
        seam_cell.token()
    );

    let nested = Shape::GeometryCollection(vec![
        box_shape(2.0, 2.0, 3.0, 3.0),
        box_shape(2.51, 2.45, 2.55, 2.56),
    ]);
    let nested_cell = CellId::from_token("1010011").expect("reviewer repro token");
    assert_eq!(nested_cell.level(), 12);
    assert_eq!(
        classify_with_native(&nested, nested_cell),
        CellClass::Interior,
        "nested collection cell {}",
        nested_cell.token()
    );
}

#[test]
fn split_working_window_cannot_prune_the_retained_seam_owner() {
    // The exact lifted parameter for the owner probe is t=7/20. The
    // working split follows the short seam representation. Its window
    // alone rejects this S2 owner, while the retained affine edge still
    // meets the owner's certified rectangle. That disagreement is
    // precisely the hazard corroboration must keep open.
    let source = Shape::LineString(
        LineSeq::try_new(CoordSeq::from(vec![
            Point::new_unchecked_xy(170.0, 0.0),
            Point::new_unchecked_xy(-160.0, 40.0),
        ]))
        .expect("test line is valid"),
    );
    let working = source
        .split_antimeridian()
        .expect("test line has a split working shape");
    let affine = GridAffineSource::new(&source, SphericalGridTarget::S2)
        .expect("test source carrier allocates");
    let owner = CellId::from_token("7b32f3").expect("frozen S2 seam owner");
    let rect = Cell::from_id(owner).rect_bound(&Cell::from_id(owner).vertices_lonlat());

    assert!(affine.s2_needs_corroboration(WorkingShapeRelation::AntimeridianSplit));
    assert!(
        !SourceWindows::new(&working).may_overlap(rect),
        "the split working window supplies the unsafe Outside proposal"
    );
    assert_eq!(
        affine.classify_rect(certified_rect_windows(rect)),
        AffineRectClass::Boundary,
        "the retained raw affine edge meets the candidate enclosure"
    );
    let ctx = CoverContext::prepare(&working, &affine, WorkingShapeRelation::AntimeridianSplit);
    assert_eq!(classify(&ctx, owner), CellClass::Boundary);
    let covering = fixed(10)
        .cover(&working, &affine, WorkingShapeRelation::AntimeridianSplit)
        .expect("the regression line has a small fixed-level cover");
    assert!(covering.outer().contains(&owner));
}

#[test]
fn corroboration_is_relation_gated_before_candidate_traversal() {
    let source = box_shape(10.0, 10.0, 20.0, 20.0);
    let affine = GridAffineSource::new(&source, SphericalGridTarget::S2)
        .expect("test source carrier allocates");

    assert!(
        !affine.s2_needs_corroboration(WorkingShapeRelation::Identity),
        "an ordinary physical source stays on the fast path"
    );
    let ordinary = CoverContext::prepare(&source, &affine, WorkingShapeRelation::Identity);
    assert!(ordinary.affine.is_none());

    assert!(
        affine.s2_needs_corroboration(WorkingShapeRelation::AntimeridianSplit),
        "working-image identity is an explicit part of the authority proof"
    );
    let split = CoverContext::prepare(&source, &affine, WorkingShapeRelation::AntimeridianSplit);
    assert!(split.affine.is_some());
}

#[test]
fn split_seam_carrier_can_exclude_the_far_sheet() {
    let raw = box_shape(179.0, -1.0, -179.0, 1.0);
    let affine = GridAffineSource::new(&raw, SphericalGridTarget::S2).unwrap();
    assert!(
        !affine.is_unknown(),
        "the exact 179/-179 seam rectangle must retain a complete affine carrier"
    );
    let working = raw.split_antimeridian().unwrap();
    let id = CellId::from_lonlat(0.0, 0.0).parent(8).unwrap();
    let cell = Cell::from_id(id);
    let rect = cell.rect_bound(&cell.vertices_lonlat());
    assert_eq!(
        affine.classify_rect(certified_rect_windows(rect)),
        AffineRectClass::Outside,
        "the continuous seam lift excludes lon 0 from a 179/-179 strip"
    );

    let far_seam = CellId::from_lonlat(179.5, 45.0).parent(8).unwrap();
    let far_cell = Cell::from_id(far_seam);
    let far_rect = far_cell.rect_bound(&far_cell.vertices_lonlat());
    assert!(
        !SourceWindows::new(&working).may_overlap(far_rect),
        "normalized seam components keep their latitude bounds"
    );
    assert_eq!(
        affine.classify_rect(certified_rect_windows(far_rect)),
        AffineRectClass::Outside,
        "the exact seam carrier excludes a far-latitude seam cell"
    );
    let ctx = CoverContext::prepare(&working, &affine, WorkingShapeRelation::AntimeridianSplit);
    assert_eq!(classify(&ctx, far_seam), CellClass::Outside);
    assert_eq!(
        fixed(8)
            .cover(&working, &affine, WorkingShapeRelation::AntimeridianSplit)
            .unwrap()
            .outer()
            .len(),
        64,
        "the certified split seam rectangle keeps the s2sphere outer oracle"
    );
}

#[test]
fn sub_ulp_seam_sheet_can_certify_retained_authority_interior() {
    // `6554d` has one ordinary and one sub-ULP longitude sheet at the
    // seam. Its public S2 polygon is wholly inside the raw 179/-179 linear
    // source. The latter needs its own vertical closed-set proof; forcing
    // it to Boundary loses this exact `within` cell even though the split
    // working classifier is also Interior.
    let raw = box_shape(179.0, -1.0, -179.0, 1.0);
    let working = raw.split_antimeridian().unwrap();
    let affine = GridAffineSource::new(&raw, SphericalGridTarget::S2).unwrap();
    let id = CellId::from_token("6554d").expect("frozen seam interior");
    let cell = Cell::from_id(id);
    let rect = cell.rect_bound(&cell.vertices_lonlat());
    assert!(rect.crosses_seam());
    let windows = certified_rect_windows(rect);
    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        longitude: CertifiedLongitudeDegrees::Two([_, seam]),
        ..
    }) = windows
    else {
        panic!("the frozen seam cell has two certified longitude sheets");
    };
    assert_eq!(seam.lo.to_bits(), (-180.0_f64).to_bits());
    assert_eq!(seam.hi.to_bits(), (-180.0_f64).next_up().to_bits());
    assert!(
        seam.lo.next_up() >= seam.hi,
        "the frozen second sheet enters the adjacent-double closed witness"
    );
    assert_eq!(
        affine.classify_rect(windows),
        AffineRectClass::Interior,
        "candidate={windows:?} rect={rect:?}"
    );
    let ctx = CoverContext::prepare(&working, &affine, WorkingShapeRelation::AntimeridianSplit);
    assert_eq!(classify(&ctx, id), CellClass::Interior);
}

#[test]
fn polar_cap_affine_certificate_prunes_the_south() {
    let cap = box_shape(-180.0, 80.0, 180.0, 90.0);
    let affine = GridAffineSource::new(&cap, SphericalGridTarget::S2).unwrap();
    let id = CellId::from_lonlat(0.0, 0.0).parent(4).unwrap();
    let cell = Cell::from_id(id);
    let rect = cell.rect_bound(&cell.vertices_lonlat());
    assert_eq!(
        affine.classify_rect(certified_rect_windows(rect)),
        AffineRectClass::Outside,
        "the full-longitude cap's exact carrier excludes a south cell"
    );
    let ctx = CoverContext::prepare(&cap, &affine, WorkingShapeRelation::Identity);
    assert!(
        !ctx.windows.may_overlap(rect),
        "the cap's working window is disjoint from a south cell"
    );
    assert!(ctx.affine.is_some(), "the polar carrier is corroborated");
    assert!(
        !affine.s2_raw_owner_descends_from(id),
        "the south cell owns no retained cap endpoint"
    );
    assert_eq!(
        classify(&ctx, id),
        CellClass::Outside,
        "a corroborated polar cap still prunes its disjoint S2 subtree"
    );
}

#[test]
fn certified_rect_windows_preserve_circular_s2_enclosures() {
    let ordinary = super::super::cell::LatLngRect {
        lat_lo: 13.0,
        lat_hi: 14.0,
        lng_lo: -5.0,
        lng_hi: 6.0,
    };
    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        latitude,
        longitude: CertifiedLongitudeDegrees::One(longitude),
    }) = certified_rect_windows(ordinary)
    else {
        panic!("ordinary S2 rectangle has a certified degree window");
    };
    assert_eq!(latitude.lo.to_bits(), ordinary.lat_lo.to_bits());
    assert_eq!(latitude.hi.to_bits(), ordinary.lat_hi.to_bits());
    assert_eq!(longitude.lo.to_bits(), ordinary.lng_lo.to_bits());
    assert_eq!(longitude.hi.to_bits(), ordinary.lng_hi.to_bits());

    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        longitude: CertifiedLongitudeDegrees::Two([west, east]),
        ..
    }) = certified_rect_windows(super::super::cell::LatLngRect {
        lng_lo: 170.0,
        lng_hi: -170.0,
        ..ordinary
    })
    else {
        panic!("a seam enclosure retains both canonical longitude sheets");
    };
    assert_eq!(west.lo.to_bits(), 170.0_f64.to_bits());
    assert_eq!(west.hi.to_bits(), 180.0_f64.to_bits());
    assert_eq!(east.lo.to_bits(), (-180.0_f64).to_bits());
    assert_eq!(east.hi.to_bits(), (-170.0_f64).to_bits());

    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        longitude: CertifiedLongitudeDegrees::Full,
        ..
    }) = certified_rect_windows(super::super::cell::LatLngRect {
        lng_lo: -180.0,
        lng_hi: 180.0,
        ..ordinary
    })
    else {
        panic!("a polar closure retains its full-longitude enclosure");
    };
}

/// Fixed-level covering of a box: complete (every interior sample's
/// ancestor cell is in `outer`), exact (no stray cells), and
/// `interior ⊆ outer` with covered centers.
#[test]
fn fixed_level_box_covering_is_complete_and_exact() {
    let source = box_shape(13.0, 52.0, 14.0, 53.0);
    let covering = fixed(10)
        .cover_identity(&source)
        .expect("small fixed-level covering is within budget");
    let outer = covering.outer();
    let interior = covering.interior();
    assert!(!outer.is_empty());
    assert!(!interior.is_empty());
    assert!(outer.windows(2).all(|pair| pair[0] < pair[1]));
    // Completeness: sampled interior points classify into outer cells.
    for i in 0..=20 {
        for j in 0..=20 {
            let lon = 13.0 + f64::from(i) / 20.0;
            let lat = 52.0 + f64::from(j) / 20.0;
            let leaf = CellId::from_lonlat(lon, lat).parent(10).expect("coarser");
            assert!(has(&outer, leaf), "missing cell for lon={lon} lat={lat}");
        }
    }
    // Interior certificates: contained in outer, centers covered.
    for &id in &interior {
        assert!(has(&outer, id));
        let center = Cell::from_id(id).center_lonlat();
        assert!(source.covers_point(center));
    }
    // The exact covering is materially tighter than the bbox: cells
    // far outside the box never appear.
    for &id in &outer {
        let center = Cell::from_id(id).center_lonlat();
        assert!(
            (12.7..=14.3).contains(&center.x) && (51.7..=53.3).contains(&center.y),
            "stray cell at {center:?}"
        );
    }
}

/// An L-shaped (concave) source: the exact coverer excludes the
/// notch — the rectangle coverer's documented false positives.
#[test]
fn concave_source_excludes_notch() {
    let shell = vec![
        Point::new_unchecked_xy(0.0, 0.0),
        Point::new_unchecked_xy(4.0, 0.0),
        Point::new_unchecked_xy(4.0, 1.0),
        Point::new_unchecked_xy(1.0, 1.0),
        Point::new_unchecked_xy(1.0, 4.0),
        Point::new_unchecked_xy(0.0, 4.0),
        Point::new_unchecked_xy(0.0, 0.0),
    ];
    let source = Shape::Polygon(Polygon::new(Ring::from_trusted_closed(shell), Vec::new()));
    let covering = fixed(8)
        .cover_identity(&source)
        .expect("small fixed-level covering is within budget");
    let outer = covering.outer();
    // The notch interior (around 3, 3) is far from the L; its cell must
    // not appear, while the bbox covering would include it.
    let notch = CellId::from_lonlat(3.0, 3.0).parent(8).expect("coarser");
    assert!(!has(&outer, notch));
    let arm = CellId::from_lonlat(0.5, 3.5).parent(8).expect("coarser");
    assert!(has(&outer, arm));
}

/// Adaptive covers use `target_cells` as a target, not `max_cells` as a
/// hard rejection threshold. This must stay distinct from fixed-level
/// cover construction, which does enforce a hard cap.
#[test]
fn adaptive_max_cells_is_not_a_hard_cap() {
    let source = box_shape(10.0, 40.0, 20.0, 50.0);
    let coverer = Coverer {
        min_level: 4,
        max_level: 12,
        level_mod: 1,
        max_cells: Some(1),
        target_cells: 64,
    };
    let covering = coverer
        .cover_identity(&source)
        .expect("adaptive cover must not reject at max_cells");
    let outer = covering.outer();
    let interior = covering.interior();
    assert!(
        outer.len() > 1,
        "fixture must exceed max_cells=1 to exercise target semantics"
    );
    let levels: std::collections::BTreeSet<u8> = outer.iter().map(|id| id.level()).collect();
    assert!(levels.len() > 1, "expected mixed levels, got {levels:?}");
    assert!(levels.iter().all(|&level| (4..=12).contains(&level)));
    for &id in &interior {
        assert!(source.covers_point(Cell::from_id(id).center_lonlat()));
    }
}

/// Seam-straddling sources stay narrow (no degradation to a global
/// covering) and cover both sides of the seam.
#[test]
fn seam_source_stays_narrow() {
    let east = box_shape(179.5, -1.0, 180.0, 1.0);
    let west = box_shape(-180.0, -1.0, -179.5, 1.0);
    let source = match (east, west) {
        (Shape::Polygon(a), Shape::Polygon(b)) => Shape::MultiPolygon(vec![a, b]),
        _ => unreachable!(),
    };
    let covering = fixed(8)
        .cover_identity(&source)
        .expect("small fixed-level covering is within budget");
    let outer = covering.outer();
    assert!(!outer.is_empty());
    // Both spellings are represented.
    let east_cell = CellId::from_lonlat(179.9, 0.0).parent(8).expect("coarser");
    let west_cell = CellId::from_lonlat(-179.9, 0.0).parent(8).expect("coarser");
    assert!(has(&outer, east_cell));
    assert!(has(&outer, west_cell));
    // Narrow: nothing near lon 0, and far from global scale.
    let far = CellId::from_lonlat(0.0, 0.0).parent(8).expect("coarser");
    assert!(!has(&outer, far));
    assert!(outer.len() < 2000, "{} cells", outer.len());
}

/// Point and line sources produce boundary-only coverings (no false
/// interior certificates on measure-zero geometry).
#[test]
fn thin_sources_have_no_interior() {
    let point = Shape::Point(Point::new_unchecked_xy(13.4, 52.5));
    let covering = fixed(12)
        .cover_identity(&point)
        .expect("small fixed-level covering is within budget");
    let outer = covering.outer();
    assert_eq!(outer.len(), 1);
    assert!(covering.interior().is_empty());
    let leaf = CellId::from_lonlat(13.4, 52.5).parent(12).expect("coarser");
    assert_eq!(outer, vec![leaf]);
}

/// Polar-face kite cells (the refuted expanded-proxy counterexample
/// region): interior certificates near lat 85 stay sound — every
/// certified cell's vertices, center, AND edge midpoints lie inside
/// the source.
#[test]
fn polar_kite_interior_certificates_are_sound() {
    let source = box_shape(20.0, 80.0, 70.0, 88.0);
    let covering = fixed(6)
        .cover_identity(&source)
        .expect("small fixed-level covering is within budget");
    for id in covering.interior() {
        let cell = Cell::from_id(id);
        assert!(source.covers_point(cell.center_lonlat()), "{id:?} center");
        for vertex in cell.vertices_lonlat() {
            assert!(source.covers_point(vertex), "{id:?} vertex {vertex:?}");
        }
        // Edge midpoints are also exact cell points (children's shared
        // corners): sample via the four children's vertices.
        if let Some(children) = id.children() {
            for child in children {
                for vertex in Cell::from_id(child).vertices_lonlat() {
                    assert!(
                        source.covers_point(vertex),
                        "{id:?} child vertex {vertex:?}"
                    );
                }
            }
        }
    }
}

/// Fixed-level coverings never emit below `min_level`, even when an
/// explicit soft/hard `max_cells` is present but large enough to fit the
/// true fixed-level cover (F7: soft-budget must not substitute coarser
/// interior cells).
#[test]
fn fixed_level_soft_budget_never_emits_below_min_level() {
    let source = box_shape(0.0, 0.0, 1.0, 1.0);
    let unlimited = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: None,
        target_cells: 8,
    }
    .cover_identity(&source)
    .expect("unlimited fixed-level covering");
    let fit = unlimited.outer().len();
    assert!(fit > 0);
    // Finite budget strictly above the fit count must match unlimited
    // (no silent coarsening of interior cells below min_level).
    let capped = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: Some(fit + 32),
        target_cells: 8,
    }
    .cover_identity(&source)
    .expect("fitting fixed-level covering under soft budget");
    assert_eq!(capped.outer(), unlimited.outer());
    assert!(
        capped.outer().iter().all(|id| id.level() == 10),
        "fixed-level cover emitted non-level-10 cells"
    );
}

/// A large areal source forced down to a fine `min_level` expands past the
/// cell budget; the coverer fails deterministically (naming `max_cells`)
/// during the interior descendant emission, never flooding memory.
#[test]
fn cover_rejects_fine_min_level_before_flooding() {
    let source = box_shape(-60.0, -40.0, 60.0, 40.0);
    let coverer = Coverer {
        min_level: 16,
        max_level: 16,
        level_mod: 1,
        max_cells: Some(crate::grid::GRID_MAX_CELLS),
        target_cells: 8,
    };
    let err = coverer
        .cover_identity(&source)
        .expect_err("world-scale fixed min_level exceeds the cell budget");
    assert_eq!(err.limit, crate::grid::GRID_MAX_CELLS);
    assert!(err.to_string().contains("max_cells"));
}

/// Revert-sensitive: a short line whose unlimited cover is K cells must
/// succeed for every `max_cells >= K` with the same tokens, and raise only
/// for `max_cells < K`. Transient fail-open DFS frontier size must not be
/// charged against the emission budget (the max_cells=1 repro).
#[test]
fn fixed_level_budget_matches_unlimited_threshold() {
    let source = Shape::LineString(
        LineSeq::try_new(CoordSeq::from(vec![
            Point::new_unchecked_xy(-75.0, 40.0),
            Point::new_unchecked_xy(-74.99, 40.01),
        ]))
        .expect("test line is valid"),
    );
    let unlimited = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: None,
        target_cells: 8,
    }
    .cover_identity(&source)
    .expect("unlimited short-line covering");
    let k = unlimited.outer().len();
    assert_eq!(k, 1, "repro line must fit in one L10 cell");
    assert_eq!(unlimited.outer()[0].token(), "89c6b5");

    for m in 1..=k + 2 {
        let got = Coverer {
            min_level: 10,
            max_level: 10,
            level_mod: 1,
            max_cells: Some(m),
            target_cells: 8,
        }
        .cover_identity(&source)
        .unwrap_or_else(|_| panic!("budget {m} must fit K={k} cover"));
        assert_eq!(got.outer(), unlimited.outer(), "budget {m}");
    }
    // K-1 must raise when K > 0 (here K=1 ⇒ no smaller positive budget to
    // probe; use a multi-cell line for the raise side).
    let longer = Shape::LineString(
        LineSeq::try_new(CoordSeq::from(vec![
            Point::new_unchecked_xy(-75.0, 40.0),
            Point::new_unchecked_xy(-74.0, 41.0),
        ]))
        .expect("longer line"),
    );
    let long_u = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: None,
        target_cells: 8,
    }
    .cover_identity(&longer)
    .expect("unlimited longer line");
    let n = long_u.outer().len();
    assert!(n > 1, "longer line should need multiple L10 cells, got {n}");
    let fit = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: Some(n),
        target_cells: 8,
    }
    .cover_identity(&longer)
    .expect("budget N must fit");
    assert_eq!(fit.outer(), long_u.outer());
    let err = Coverer {
        min_level: 10,
        max_level: 10,
        level_mod: 1,
        max_cells: Some(n - 1),
        target_cells: 8,
    }
    .cover_identity(&longer)
    .expect_err("budget N-1 must raise");
    assert_eq!(err.limit, n - 1);
    assert!(err.to_string().contains("max_cells"));
}
