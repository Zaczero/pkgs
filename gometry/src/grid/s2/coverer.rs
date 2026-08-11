#![cfg_attr(
    test,
    allow(
        clippy::similar_names,
        reason = "S2 differential tests compare paired latitude and longitude bounds"
    )
)]
//! The exact-geometry S2 coverer: classifies candidate cells directly
//! against gometry's predicate engine, with certificates that are sound for
//! the true spherical cells.
//!
//! Soundness model: a cell's exact [`LatLngRect`] bound provably contains
//! the lon/lat of every point of the true cell, so `source.covers(rect)`
//! certifies `Interior` and `!source.intersects(rect)` certifies `Outside`
//! (`rect_bound` **may exclude** — it is a sound negative certificate).
//! Vertex/center hits are exact cell points, so they certify intersection
//! outright. The four-corner lon/lat proxy (`boundary_shape`) is a planar
//! chord of the true spherical cell and may only supply a **positive**
//! witness — a negative proxy result must not prune (it is not a sound
//! Outside certificate; face footprints reach past their chord edges, e.g.
//! face 0 to lat −45° while the chord south edge sits at ≈−35.26°). When the
//! rect is Boundary/Interior without vertex/center hits the coverer therefore
//! fails open to `Boundary`. Seam-crossing rects fail open to `Boundary` for
//! outer-cover inclusion (planar Outside is unsound there). Polar full-lng
//! rects (pole closure expands any pole-touching cell to all longitudes) fail
//! open to `Boundary`: non-pole vertex longitudes cannot prove that a source
//! misses the closed polar part of the cell.
//! `Interior` still certifies when every non-wrapped lon window of the cell
//! bound is interior to the source — so `within` keeps genuine full-longitude
//! polar interior cells.

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::collections::{HashMap, HashMapExt as _};
use crate::geometry::{Bounds, Point, PointBatchTester, Shape, same_point};
use crate::grid::affine_source::{
    GridAffineSource, RectClass as AffineRectClass, SphericalGridTarget, WorkingShapeRelation,
};
pub(crate) use crate::grid::coverer::CellClass;
use crate::grid::coverer::{NativeRectClassifier, RectClass};
use crate::grid::s2::cell::Cell;
use crate::grid::s2::cellid::CellId;
use crate::grid::s2::projection::{MAX_LEVEL, NUM_FACES};
use crate::grid::s2::seam::SourceWindows;
use crate::grid::spherical_arc::{
    Bound, CertifiedDegreeWindows, CertifiedLongitudeDegrees, DegreeWindowResult,
};
use crate::grid::{CoverBudgetExceeded, ensure_cover_budget};

/// Level budget for a covering. `min_level <= max_level`; emitted levels
/// satisfy `(level - min_level) % level_mod == 0`; `target_cells` guides
/// optional adaptive refinement. `max_cells` is a hard emission budget only
/// for fixed-level coverings (`None` = unlimited); adaptive construction uses
/// its aggregate `target_cells` guide instead.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Coverer {
    pub min_level: u8,
    pub max_level: u8,
    pub level_mod: u8,
    /// Fixed-level hard emission cap. `None` = unlimited.
    pub max_cells: Option<usize>,
    /// Adaptive refinement target, independent of the hard emission cap.
    pub target_cells: usize,
}

/// An exact classified covering: one sorted cell column plus an interior bit.
#[derive(Clone, Debug)]
pub(crate) struct Covering {
    cells: Vec<(CellId, bool)>,
}

impl Covering {
    pub(crate) fn into_cells(self) -> Vec<(CellId, bool)> {
        self.cells
    }

    /// Test oracle view of every emitted cell.  Production callers consume
    /// the tagged column directly, so keeping this support accessor test-only
    /// avoids reviving a second aggregate API.
    #[cfg(test)]
    pub(crate) fn outer(&self) -> Vec<CellId> {
        self.cells.iter().map(|(id, _)| *id).collect()
    }

    #[cfg(test)]
    fn interior(&self) -> Vec<CellId> {
        self.cells
            .iter()
            .filter_map(|(id, interior)| interior.then_some(*id))
            .collect()
    }
}

/// The prepared classification context: built once per covering, reused by
/// every candidate — the hierarchical [`PointBatchTester`] for vertex/center
/// probes and the native rectangle classifier for rect-polygon certificates.
/// Rect Outside is the only negative certificate; the planar chord proxy is
/// never used to prune (see module soundness model).
struct CoverContext<'a> {
    windows: SourceWindows,
    tester: Arc<PointBatchTester>,
    rects: NativeRectClassifier,
    /// For a point carrier (including zero-length linework), every leaf cell
    /// containing a source point under closed spherical-cell semantics.  This
    /// is the exact point fast path; unlike the historical version it uses the
    /// pole/seam-normalized owner representation rather than rounded trig XYZ.
    point_leaves: Option<Vec<CellId>>,
    /// The unsplit affine source is consulted only on seam/pole/periodic or
    /// otherwise uncertain inputs.  The ordinary fast path keeps the old
    /// source-window/native-rectangle cost per candidate.
    affine: Option<&'a GridAffineSource>,
    /// Candidate classification is a pure function of the source and cell.
    /// Productivity probes look deep into a subtree before adaptive descent
    /// later visits the same cells, so retain those expensive results for the
    /// lifetime of one cover instead of re-running the polygon classifiers.
    classes: RefCell<HashMap<CellId, CellClass>>,
    /// Successful descendant probes are followed again by adaptive descent.
    /// Retain the witness path so that proof is paid once.
    productive: RefCell<HashMap<CellId, bool>>,
    /// Adjacent S2 cells share vertices. Polygon membership at those exact
    /// coordinates is source-invariant, so classify each shared point once.
    point_membership: RefCell<HashMap<(u64, u64), bool>>,
}

impl Coverer {
    /// Fixed-level `max_cells` is a hard emission cap; adaptive construction
    /// uses it as its approximate target rather than a rejection threshold.
    const fn hard_budget(self) -> Option<usize> {
        if self.min_level == self.max_level {
            self.max_cells
        } else {
            None
        }
    }

    /// Adaptive refinement target.
    const fn target_budget(self) -> usize {
        self.target_cells
    }

    /// Cover `source` (canonical lon/lat, non-empty) exactly.
    ///
    /// Forced descent to `min_level` is **budget-aware and depth-first**.
    /// Fixed-level `max_cells` charges only **proven-productive** cells
    /// (emitted terminals and phase-2 queue entries that will emit ≥1 cell),
    /// never the transient fail-open DFS frontier — conservative Boundary
    /// candidates can vanish on deeper classification, so the frontier size
    /// is not a lower bound on final emissions. Interior subtrees preflight
    /// exact `4^Δ` multiplicity before enumerating. Adaptive splits above
    /// `min_level` stage actual 4/16/64 (`level_mod`) non-Outside children and
    /// commit only when the projected total still fits `target_cells`;
    /// otherwise the parent is emitted (optional coarsening — never below
    /// `min_level`).
    ///
    /// `max_cells = None` is unlimited (factory `max_cells=None`).
    pub(crate) fn cover(
        &self,
        working: &Shape,
        affine: &GridAffineSource,
        relation: WorkingShapeRelation,
    ) -> Result<Covering, CoverBudgetExceeded> {
        debug_assert_eq!(affine.target(), SphericalGridTarget::S2);
        let ctx = CoverContext::prepare(working, affine, relation);
        // E = emitted cells; Q = phase-2 queue (proven ≥1 emission each).
        // Transient DFS candidates are *not* charged against max_cells.
        let mut cells: Vec<(CellId, bool)> = Vec::new();
        // Phase-2 frontier (candidates already at/above min_level).
        let mut queue: BinaryHeap<(Reverse<u8>, CellId, CellClass)> = BinaryHeap::new();
        // Phase-1 DFS stack: unresolved candidates still below min_level.
        // Fail-open Boundary entries may later classify Outside — do not
        // charge them until proven productive.
        let mut dfs: Vec<(u8, CellId, CellClass)> = Vec::with_capacity(6 + 3 * MAX_LEVEL as usize);
        // Reusable staging buffer for 4/16/64 target descendants.
        let mut staged: Vec<(CellId, CellClass)> = Vec::with_capacity(64);
        // Scratch for productivity probes (must not alias `staged`).
        let mut scratch: Vec<(CellId, CellClass)> = Vec::with_capacity(64);

        self.seed_faces(&ctx, &cells, &mut dfs, &mut queue, &mut scratch)?;
        self.force_min_level(
            &ctx,
            &mut cells,
            &mut dfs,
            &mut queue,
            &mut staged,
            &mut scratch,
        )?;
        self.adaptive_emit(&ctx, &mut cells, &mut queue, &mut staged, &mut scratch)?;

        cells.sort_unstable_by_key(|(id, _)| *id);
        Ok(Covering { cells })
    }

    #[cfg(test)]
    fn cover_identity(&self, source: &Shape) -> Result<Covering, CoverBudgetExceeded> {
        let affine = GridAffineSource::new(source, SphericalGridTarget::S2)
            .expect("test source carrier allocates");
        self.cover(source, &affine, WorkingShapeRelation::Identity)
    }

    /// Classify the six face roots into the phase-1 DFS stack or phase-2 queue.
    fn seed_faces(
        self,
        ctx: &CoverContext<'_>,
        cells: &[(CellId, bool)],
        dfs: &mut Vec<(u8, CellId, CellClass)>,
        queue: &mut BinaryHeap<(Reverse<u8>, CellId, CellClass)>,
        scratch: &mut Vec<(CellId, CellClass)>,
    ) -> Result<(), CoverBudgetExceeded> {
        for face in 0..NUM_FACES {
            let id = CellId::from_face(face);
            match classify(ctx, id) {
                CellClass::Outside => {},
                class => {
                    if self.min_level == 0 {
                        // Only charge proven-productive faces (fail-open may
                        // prune entirely under adaptive max_level).
                        if !is_productive(ctx, id, 0, class, self, scratch) {
                            continue;
                        }
                        let base = cells.len() + queue.len();
                        ensure_cover_budget(base.saturating_add(1), self.hard_budget())?;
                        queue.push((Reverse(0), id, class));
                        debug_assert_budget(cells.len(), queue.len(), self.hard_budget());
                    } else {
                        // Below min_level: unresolved — resolve depth-first
                        // without charging the transient frontier.
                        dfs.push((0, id, class));
                    }
                },
            }
        }
        Ok(())
    }

    /// Phase 1: depth-first forced descent to `min_level`.
    ///
    /// Budget charges only proven-productive emissions: interior `4^Δ`
    /// terminals and phase-2 queue entries that will emit ≥1 cell. Fail-open
    /// Boundary that deeper adaptive refinement prunes to Outside is never
    /// charged (same class as R11 B2, applied at the min_level queue entry).
    fn force_min_level(
        self,
        ctx: &CoverContext<'_>,
        cells: &mut Vec<(CellId, bool)>,
        dfs: &mut Vec<(u8, CellId, CellClass)>,
        queue: &mut BinaryHeap<(Reverse<u8>, CellId, CellClass)>,
        staged: &mut Vec<(CellId, CellClass)>,
        scratch: &mut Vec<(CellId, CellClass)>,
    ) -> Result<(), CoverBudgetExceeded> {
        while let Some((level, id, class)) = dfs.pop() {
            debug_assert!(level < self.min_level);
            // Proven base: emitted + phase-2 queue (not the unresolved DFS).
            let proven = cells.len() + queue.len();
            match class {
                CellClass::Outside => unreachable!("Outside never enters the frontier"),
                CellClass::Interior => {
                    emit_interior_descendants(self, id, level, proven, cells)?;
                    debug_assert_budget(cells.len(), queue.len(), self.hard_budget());
                },
                CellClass::Boundary => {
                    stage_children_at(ctx, id, level + 1, staged);
                    let child_level = level + 1;
                    if child_level >= self.min_level {
                        // Drop fail-open children that resolve to Outside
                        // before charging the phase-2 queue.
                        retain_productive(ctx, child_level, staged, self, scratch);
                        for &(child, child_class) in &*staged {
                            let base = cells.len() + queue.len();
                            ensure_cover_budget(base.saturating_add(1), self.hard_budget())?;
                            queue.push((Reverse(child_level), child, child_class));
                        }
                        debug_assert_budget(cells.len(), queue.len(), self.hard_budget());
                    } else {
                        for &(child, child_class) in &*staged {
                            dfs.push((child_level, child, child_class));
                        }
                    }
                },
            }
        }
        Ok(())
    }

    /// Phase 2: adaptive refinement (actual K fanout) or emit conforming cells.
    ///
    /// Staged children are filtered to proven-productive before budget charge:
    /// fail-open Boundary that deeper classification prunes to Outside never
    /// enters the queue. Optional coarsen uses E+Q+staged against the
    /// adaptive `target_cells` guide.
    fn adaptive_emit(
        self,
        ctx: &CoverContext<'_>,
        cells: &mut Vec<(CellId, bool)>,
        queue: &mut BinaryHeap<(Reverse<u8>, CellId, CellClass)>,
        staged: &mut Vec<(CellId, CellClass)>,
        scratch: &mut Vec<(CellId, CellClass)>,
    ) -> Result<(), CoverBudgetExceeded> {
        while let Some((Reverse(level), id, class)) = queue.pop() {
            debug_assert!(level >= self.min_level);
            let can_refine = self.can_refine(level, class);
            if can_refine {
                let target = level + self.level_mod;
                stage_children_at(ctx, id, target, staged);
                retain_productive(ctx, target, staged, self, scratch);
                if staged.is_empty() {
                    // Fail-open parent pruned entirely — not an emission.
                    continue;
                }
                let base = cells.len() + queue.len();
                let projected = base.saturating_add(staged.len());
                let fits_target = projected <= self.target_budget();
                let fits_hard = self.hard_budget().is_none_or(|limit| projected <= limit);
                if fits_target && fits_hard {
                    for &(child, child_class) in &*staged {
                        queue.push((Reverse(target), child, child_class));
                    }
                    debug_assert_budget(cells.len(), queue.len(), self.hard_budget());
                    continue;
                }
                // Optional split does not fit — emit the conforming parent
                // (never below min_level; F7 purity preserved).
            }
            // Emit at this level (interior, max-level boundary, or unaffordable split).
            let base = cells.len() + queue.len();
            ensure_cover_budget(base.saturating_add(1), self.hard_budget())?;
            cells.push((id, class == CellClass::Interior));
            debug_assert_budget(cells.len(), queue.len(), self.hard_budget());
        }
        Ok(())
    }

    /// Whether a queue entry can still subdivide under this coverer.
    fn can_refine(self, level: u8, class: CellClass) -> bool {
        class == CellClass::Boundary
            && level + self.level_mod <= self.max_level
            && level + self.level_mod <= MAX_LEVEL
    }
}

/// Keep only children that will emit ≥1 terminal under this coverer.
fn retain_productive(
    ctx: &CoverContext<'_>,
    level: u8,
    staged: &mut Vec<(CellId, CellClass)>,
    coverer: Coverer,
    scratch: &mut Vec<(CellId, CellClass)>,
) {
    staged.retain(|&(id, class)| is_productive(ctx, id, level, class, coverer, scratch));
}

/// Whether `id` yields ≥1 final emission (Interior/terminal Boundary, or a
/// refinable Boundary with a productive descendant). Fail-open Boundary that
/// classifies Outside at every deeper level returns false.
fn is_productive(
    ctx: &CoverContext<'_>,
    id: CellId,
    level: u8,
    class: CellClass,
    coverer: Coverer,
    scratch: &mut Vec<(CellId, CellClass)>,
) -> bool {
    if let Some(productive) = ctx.productive.borrow().get(&id).copied() {
        return productive;
    }
    match class {
        CellClass::Outside => {
            ctx.productive.borrow_mut().insert(id, false);
            false
        },
        CellClass::Interior => {
            ctx.productive.borrow_mut().insert(id, true);
            true
        },
        CellClass::Boundary => {
            if !coverer.can_refine(level, class) {
                ctx.productive.borrow_mut().insert(id, true);
                return true;
            }
            // DFS until a terminal emission is found, or the whole branch is Outside.
            let mut stack = vec![(level, id)];
            while let Some((lvl, cell)) = stack.pop() {
                let target = lvl + coverer.level_mod;
                stage_children_at(ctx, cell, target, scratch);
                for &(child, child_class) in scratch.iter() {
                    match child_class {
                        CellClass::Outside => {},
                        CellClass::Interior => {
                            cache_productive_path(ctx, child, target, level, coverer.level_mod);
                            return true;
                        },
                        CellClass::Boundary => {
                            if !coverer.can_refine(target, child_class) {
                                cache_productive_path(ctx, child, target, level, coverer.level_mod);
                                return true;
                            }
                            stack.push((target, child));
                        },
                    }
                }
            }
            ctx.productive.borrow_mut().insert(id, false);
            false
        },
    }
}

fn cache_productive_path(
    ctx: &CoverContext<'_>,
    leaf: CellId,
    leaf_level: u8,
    root_level: u8,
    level_mod: u8,
) {
    let mut productive = ctx.productive.borrow_mut();
    let mut level = leaf_level;
    loop {
        productive.insert(leaf.parent(level).expect("ancestor level"), true);
        if level == root_level {
            break;
        }
        level -= level_mod;
    }
}

impl<'a> CoverContext<'a> {
    fn prepare(
        source: &Shape,
        affine: &'a GridAffineSource,
        relation: WorkingShapeRelation,
    ) -> Self {
        let tester = Arc::new(PointBatchTester::new(source));
        let shared_area_tester = matches!(source, Shape::Polygon(_) | Shape::MultiPolygon(_))
            .then(|| Arc::clone(&tester));
        Self {
            windows: SourceWindows::new(source),
            tester,
            rects: NativeRectClassifier::new_with_area_tester(source, shared_area_tester),
            point_leaves: closed_point_leaves(source),
            affine: affine.s2_needs_corroboration(relation).then_some(affine),
            classes: RefCell::new(HashMap::new()),
            productive: RefCell::new(HashMap::new()),
            point_membership: RefCell::new(HashMap::new()),
        }
    }

    fn covers_point(&self, point: Point) -> bool {
        let key = (point.x.to_bits(), point.y.to_bits());
        if let Some(covered) = self.point_membership.borrow().get(&key).copied() {
            return covered;
        }
        let covered = self.tester.covers_point(point);
        self.point_membership.borrow_mut().insert(key, covered);
        covered
    }
}

/// Leaf owners of a point-only source under S2's closed-cell relation.
///
/// The source factory has already performed the shared geographic pole
/// normalization.  A zero-length LineString represents the same point set as
/// `Point`, so it deliberately enters this exact owner path too; a nonzero
/// segment stays on the general certificate traversal.
fn closed_point_leaves(source: &Shape) -> Option<Vec<CellId>> {
    let mut has_nonzero_segment = false;
    source.for_each_segment(|segment| {
        has_nonzero_segment |= !same_point(segment.start, segment.end);
    });
    if has_nonzero_segment {
        return None;
    }

    let mut leaves = Vec::new();
    source.for_each_point(|point| {
        let xyz = super::projection::lonlat_to_closed_owner_point(point.x, point.y);
        let leaf = CellId::from_point(xyz);
        leaves.push(leaf);
        for neighbor in leaf.edge_neighbors() {
            if Cell::from_id(neighbor).contains_point(xyz) {
                leaves.push(neighbor);
                // A corner can meet a diagonal leaf as well.  Discover it
                // from an admitted edge neighbour; this is positive-only and
                // never invents an owner from a proxy-negative result.
                for diagonal in neighbor.edge_neighbors() {
                    if diagonal != leaf && Cell::from_id(diagonal).contains_point(xyz) {
                        leaves.push(diagonal);
                    }
                }
            }
        }
    });
    (!leaves.is_empty()).then(|| {
        leaves.sort_unstable();
        leaves.dedup();
        leaves
    })
}

/// Bulk-emit interior `min_level` descendants after preflighting `4^Δ`.
fn emit_interior_descendants(
    coverer: Coverer,
    id: CellId,
    level: u8,
    base: usize,
    cells: &mut Vec<(CellId, bool)>,
) -> Result<(), CoverBudgetExceeded> {
    // Required terminals: exactly 4^(min_level - level). Preflight the
    // projected count before enumerating so a tiny budget never walks a
    // multi-million child range.
    if let Some(n) = interior_multiplicity(level, coverer.min_level) {
        ensure_cover_budget(base.saturating_add(n), coverer.hard_budget())?;
        cells.reserve(n);
    } else if let Some(limit) = coverer.hard_budget() {
        // Shift overflow ⇒ larger than any finite usize budget.
        return Err(CoverBudgetExceeded::new(limit));
    }
    let end = id.child_end_at(coverer.min_level);
    let mut cursor = id.child_begin_at(coverer.min_level);
    while cursor != end {
        cells.push((cursor, true));
        cursor = cursor.next();
    }
    // Unlimited path: still honor a finite budget if set mid-enumeration
    // (defensive; preflight covers the finite case).
    ensure_cover_budget(cells.len(), coverer.hard_budget())
}

/// `4^(min_level - level)` interior terminals, or `None` on overflow.
fn interior_multiplicity(level: u8, min_level: u8) -> Option<usize> {
    debug_assert!(level <= min_level);
    let delta = u32::from(min_level - level);
    // 4^delta = 2^(2*delta). delta ≤ 30 ⇒ shift ≤ 60, fits in u64/usize on
    // 64-bit; checked_shl covers smaller pointer widths.
    let shift = 2_u32.checked_mul(delta)?;
    1_usize.checked_shl(shift)
}

/// Classify every non-Outside descendant of `id` at exactly `target` level
/// into `out` (cleared first). Used for 4-way immediate children and for
/// `level_mod` 16/64 fanouts.
fn stage_children_at(
    ctx: &CoverContext<'_>,
    id: CellId,
    target: u8,
    out: &mut Vec<(CellId, CellClass)>,
) {
    out.clear();
    let end = id.child_end_at(target);
    let mut cursor = id.child_begin_at(target);
    while cursor != end {
        match classify(ctx, cursor) {
            CellClass::Outside => {},
            class => out.push((cursor, class)),
        }
        cursor = cursor.next();
    }
}

fn debug_assert_budget(emitted: usize, frontier: usize, max_cells: Option<usize>) {
    if let Some(limit) = max_cells {
        debug_assert!(
            emitted.saturating_add(frontier) <= limit,
            "cover budget invariant broken: E={emitted} + Q={frontier} > M={limit}"
        );
    }
}

/// Classify one candidate cell against the source (decode once, stage the
/// gates cheap-to-expensive).
fn classify(ctx: &CoverContext<'_>, id: CellId) -> CellClass {
    if let Some(class) = ctx.classes.borrow().get(&id).copied() {
        return class;
    }
    let class = classify_uncached(ctx, id);
    ctx.classes.borrow_mut().insert(id, class);
    class
}

/// Run the candidate classifier once; [`classify`] owns per-cover reuse.
fn classify_uncached(ctx: &CoverContext<'_>, id: CellId) -> CellClass {
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
    let proposed = if ctx.windows.may_overlap(rect) {
        let covered = vertices
            .iter()
            .filter(|vertex| ctx.covers_point(**vertex))
            .count();
        let center_hit = covered < 4 && ctx.covers_point(cell.center_lonlat());
        // Seam-crossing: planar Outside is unsound → fail-open Boundary.
        // Polar full-lng (pole closure): vertices away from the pole cannot
        // establish an Outside result for a cell whose closed bound deliberately
        // contains every longitude at the pole. Keep the rect certificate; any
        // remaining ambiguity fails open to Boundary.
        // Still certify Interior when every positive-width bound window is
        // interior (full-cap / seam-crossing within).
        if rect.crosses_seam() || rect.is_full_lng() {
            if covered == 4 && wrapped_rect_is_interior(ctx, rect) {
                CellClass::Interior
            } else {
                // Seam-crossing rects: planar Outside is unsound → fail-open
                // Boundary. (True-cell planar refine is also unsound across
                // ±180.)
                CellClass::Boundary
            }
        } else {
            // Rect certificates only: rect ⊇ true cell, so covers(rect) ⇒
            // Interior and !intersects(rect) ⇒ Outside (`rect_bound` may
            // exclude). A Boundary or Interior rect without vertex/center
            // hits fails open to Boundary — `boundary_shape` is a planar
            // chord and may only witness positively, so a negative proxy
            // result must not prune whole subtrees.
            let rect_class = ctx.rects.classify_bounds(rect_bounds(rect));
            if covered == 4 && rect_class == RectClass::Interior {
                CellClass::Interior
            } else if covered > 0 || center_hit {
                CellClass::Boundary
            } else {
                match rect_class {
                    RectClass::Outside => CellClass::Outside,
                    RectClass::Interior | RectClass::Boundary => CellClass::Boundary,
                }
            }
        }
    } else {
        CellClass::Outside
    };

    // An exact affine certificate only corroborates a planar proposal on
    // hazards.  Boundary stays Boundary: no terminal spherical refiner is
    // smuggled onto S2's ordinary candidate path.
    let Some(affine) = ctx.affine else {
        return proposed;
    };
    if proposed == CellClass::Boundary {
        return CellClass::Boundary;
    }
    merge_certified_rect(
        proposed,
        affine.classify_rect(certified_rect_windows(rect)),
        affine.s2_raw_owner_descends_from(id),
    )
}

/// Convert the exact S2 cell enclosure into a degree certificate for the
/// unsplit affine source.  A wrapped bound becomes its two closed canonical
/// sheets; a polar closure becomes the closed full-longitude enclosure.  Each
/// still contains the true spherical cell, so agreement with the working
/// proposal can certify either result without treating a chord proxy as a
/// negative.
fn certified_rect_windows(rect: super::cell::LatLngRect) -> DegreeWindowResult {
    let Some(latitude) = Bound::new(rect.lat_lo, rect.lat_hi) else {
        return DegreeWindowResult::Boundary;
    };
    let longitude = if rect.is_full_lng() {
        CertifiedLongitudeDegrees::Full
    } else if rect.crosses_seam() {
        let (west, Some(east)) = rect.lng_windows() else {
            return DegreeWindowResult::Boundary;
        };
        let (Some(west), Some(east)) = (Bound::new(west.0, west.1), Bound::new(east.0, east.1))
        else {
            return DegreeWindowResult::Boundary;
        };
        CertifiedLongitudeDegrees::Two([west, east])
    } else {
        let Some(longitude) = Bound::new(rect.lng_lo, rect.lng_hi) else {
            return DegreeWindowResult::Boundary;
        };
        CertifiedLongitudeDegrees::One(longitude)
    };
    DegreeWindowResult::Windows(CertifiedDegreeWindows {
        latitude,
        longitude,
    })
}

/// Intersect the working-shape proposal with the retained affine authority.
/// Neither source may establish a negative alone on a hazard input; a native
/// raw endpoint owner also vetoes a proposed Outside result.
const fn merge_certified_rect(
    proposed: CellClass,
    certified: AffineRectClass,
    raw_owner: bool,
) -> CellClass {
    match (proposed, certified) {
        (CellClass::Outside, AffineRectClass::Outside) if !raw_owner => CellClass::Outside,
        (CellClass::Interior, AffineRectClass::Interior) => CellClass::Interior,
        _ => CellClass::Boundary,
    }
}

/// Sound Interior certificate for seam-crossing / full-longitude cell bounds.
///
/// `rect` contains the true spherical cell. Split into non-wrapped lon
/// windows (full-lng is one world-wide window); if every positive-width
/// window is `Interior` under the native rect classifier, the cell is
/// interior. Never used to certify Outside (that stays fail-open Boundary).
fn wrapped_rect_is_interior(ctx: &CoverContext<'_>, rect: super::cell::LatLngRect) -> bool {
    if rect.is_full_lng() {
        return ctx.rects.classify_bounds(Bounds::new_unchecked(
            -180.0,
            rect.lat_lo,
            180.0,
            rect.lat_hi,
        )) == RectClass::Interior;
    }
    let ((lo0, hi0), second) = rect.lng_windows();
    if !window_interior(ctx, lo0, rect.lat_lo, hi0, rect.lat_hi) {
        return false;
    }
    match second {
        Some((lo1, hi1)) => window_interior(ctx, lo1, rect.lat_lo, hi1, rect.lat_hi),
        None => true,
    }
}

fn window_interior(ctx: &CoverContext<'_>, west: f64, south: f64, east: f64, north: f64) -> bool {
    // Degenerate zero-width lon strip (seam edge only) cannot host area;
    // treat as vacuously interior so a single-side polar spelling still
    // certifies when its positive-width sibling does.
    if east <= west {
        return true;
    }
    ctx.rects
        .classify_bounds(Bounds::new_unchecked(west, south, east, north))
        == RectClass::Interior
}

const fn rect_bounds(rect: super::cell::LatLngRect) -> Bounds {
    Bounds::new_unchecked(rect.lng_lo, rect.lat_lo, rect.lng_hi, rect.lat_hi)
}

#[cfg(test)]
#[path = "coverer_tests.rs"]
mod tests;
