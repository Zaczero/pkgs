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
//! certifies `Interior` and `!source.intersects(rect)` certifies `Outside`.
//! Vertex/center hits are exact cell points, so they certify intersection
//! outright. Seam-crossing rects fail open to `Boundary` for outer-cover
//! inclusion (planar Outside is unsound there). Polar full-lng rects (pole
//! closure expands any pole-touching cell to all longitudes) fail open only
//! when the **source** is genuinely full-longitude — otherwise opposite-side
//! polar wedges are classified against the cell's true vertex lon span so a
//! partial-lon polar box does not force-include them. `Interior` still
//! certifies when every non-wrapped lon window of the cell bound is interior
//! to the source — so `within` keeps genuine full-longitude polar interior
//! cells. Other borderline cells fail open to `Boundary` and the rect-vs-cell
//! slack self-corrects through subdivision.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use super::cell::Cell;
use super::cellid::CellId;
use super::projection::{MAX_LEVEL, NUM_FACES};
use super::seam::SourceWindows;
use crate::geometry::{Bounds, Point, PointBatchTester, Shape};
pub(crate) use crate::grid::coverer::CellClass;
use crate::grid::coverer::{NativeRectClassifier, RectClass};
use crate::grid::{CoverBudgetExceeded, ensure_cover_budget};

/// Level budget for a covering. `min_level <= max_level`; emitted levels
/// satisfy `(level - min_level) % level_mod == 0`; `target_cells` guides
/// optional adaptive refinement while `max_cells` is the hard emission budget
/// (`None` = unlimited). Fixed-level coverings still subdivide to
/// `min_level`, but emission stops with [`CoverBudgetExceeded`] once
/// `max_cells` is crossed.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Coverer {
    pub min_level: u8,
    pub max_level: u8,
    pub level_mod: u8,
    /// Hard emission cap. `None` = unlimited.
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
/// every candidate — the banded point raycaster for vertex/center probes
/// and the native rectangle classifier for the rect-polygon certificates.
struct CoverContext {
    windows: SourceWindows,
    tester: Arc<PointBatchTester>,
    rects: NativeRectClassifier,
    /// For point-only sources: the sorted leaf ids of every point — cell
    /// membership is then an exact integer range search, no planar
    /// geometry at all.
    point_leaves: Option<Vec<CellId>>,
}

impl Coverer {
    /// Explicit `max_cells` hard **emission** cap (`None` = unlimited).
    const fn hard_budget(self) -> Option<usize> {
        self.max_cells
    }

    /// Adaptive refinement target.
    const fn target_budget(self) -> usize {
        self.target_cells
    }

    /// Cover `source` (canonical lon/lat, non-empty) exactly.
    ///
    /// Forced descent to `min_level` is **budget-aware and depth-first**.
    /// `max_cells` charges only **proven-productive** cells (emitted terminals
    /// and phase-2 queue entries that will emit ≥1 cell), never the transient
    /// fail-open DFS frontier — conservative Boundary candidates can vanish on
    /// deeper classification, so the frontier size is not a lower bound on
    /// final emissions. Interior subtrees preflight exact `4^Δ` multiplicity
    /// before enumerating. Adaptive splits above `min_level` stage actual
    /// 4/16/64 (`level_mod`) non-Outside children and commit only when the
    /// projected total still fits; otherwise the parent is emitted (optional
    /// coarsening — never below `min_level`).
    ///
    /// `max_cells = None` is unlimited (factory `max_cells=None`).
    pub(crate) fn cover(&self, source: &Shape) -> Result<Covering, CoverBudgetExceeded> {
        let ctx = CoverContext::prepare(source);
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

    /// Classify the six face roots into the phase-1 DFS stack or phase-2 queue.
    fn seed_faces(
        self,
        ctx: &CoverContext,
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
        ctx: &CoverContext,
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
    /// enters the queue. Optional coarsen still uses E+Q+staged against
    /// `target_cells`; the hard cap is emit-time `E+1` against `max_cells`.
    fn adaptive_emit(
        self,
        ctx: &CoverContext,
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
    ctx: &CoverContext,
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
    ctx: &CoverContext,
    id: CellId,
    level: u8,
    class: CellClass,
    coverer: Coverer,
    scratch: &mut Vec<(CellId, CellClass)>,
) -> bool {
    match class {
        CellClass::Outside => false,
        CellClass::Interior => true,
        CellClass::Boundary => {
            if !coverer.can_refine(level, class) {
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
                        CellClass::Interior => return true,
                        CellClass::Boundary => {
                            if !coverer.can_refine(target, child_class) {
                                return true;
                            }
                            stack.push((target, child));
                        },
                    }
                }
            }
            false
        },
    }
}

impl CoverContext {
    fn prepare(source: &Shape) -> Self {
        let point_leaves = (source.segment_count() == 0).then(|| {
            // Closed-cell semantics: a point on a cell edge belongs to every
            // touching cell, so each point contributes its canonical leaf
            // PLUS any edge-neighbor leaf that also contains it exactly
            // (the borderline cases users construct: lon ±180, lat 0, cell
            // corners).
            let mut leaves = Vec::new();
            source.for_each_point(|point| {
                let xyz = super::projection::lonlat_to_point(point.x, point.y);
                let leaf = CellId::from_point(xyz);
                leaves.push(leaf);
                for neighbor in leaf.edge_neighbors() {
                    if Cell::from_id(neighbor).contains_point(xyz) {
                        leaves.push(neighbor);
                        // A corner point also touches the diagonal cell —
                        // reachable as an edge neighbor of this neighbor.
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
        });
        let tester = Arc::new(PointBatchTester::new(source));
        let shared_area_tester = matches!(source, Shape::Polygon(_) | Shape::MultiPolygon(_))
            .then(|| Arc::clone(&tester));
        Self {
            windows: SourceWindows::new(source),
            tester,
            rects: NativeRectClassifier::new_with_area_tester(source, shared_area_tester),
            point_leaves,
        }
    }

    fn covers_point(&self, point: Point) -> bool {
        self.tester.covers_point(point)
    }
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
    ctx: &CoverContext,
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

#[inline]
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
fn classify(ctx: &CoverContext, id: CellId) -> CellClass {
    // Point-only sources classify exactly on the integer hierarchy: a cell
    // intersects the source iff it contains some point's leaf id.
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
        .filter(|vertex| ctx.covers_point(**vertex))
        .count();
    let center_hit = covered < 4 && ctx.covers_point(cell.center_lonlat());
    // Seam-crossing: planar Outside is unsound → fail-open Boundary.
    // Polar full-lng (pole closure): fail-open only for genuinely full-
    // longitude sources; partial-lon polar boxes classify via vertex lon span.
    // Still certify Interior when every positive-width bound window is
    // interior (full-cap / seam-crossing within).
    if rect.crosses_seam() || rect.is_full_lng() {
        if covered == 4 && wrapped_rect_is_interior(ctx, rect) {
            return CellClass::Interior;
        }
        if rect.is_full_lng() && !ctx.windows.is_full_longitude() {
            // Partial-lon source + polar-expanded cell: do not force-include
            // opposite wedges. Vertex/center hits certify Boundary; else the
            // cell's true vertex lon span vs source windows decides Outside.
            if covered > 0 || center_hit {
                return CellClass::Boundary;
            }
            let (first, second) = vertex_lng_windows(&vertices);
            if !ctx
                .windows
                .may_overlap_lng(first, second, rect.lat_lo, rect.lat_hi)
            {
                return CellClass::Outside;
            }
            return CellClass::Boundary;
        }
        return CellClass::Boundary;
    }
    // One native classification against the rect proxy answers both
    // certificates: rect ⊇ true cell, so covers(rect) ⇒ Interior and
    // !intersects(rect) ⇒ Outside.
    let rect_class = ctx.rects.classify_bounds(rect_bounds(rect));
    if covered == 4 && rect_class == RectClass::Interior {
        return CellClass::Interior;
    }
    if covered > 0 || center_hit {
        return CellClass::Boundary;
    }
    match rect_class {
        RectClass::Outside => CellClass::Outside,
        RectClass::Interior | RectClass::Boundary => CellClass::Boundary,
    }
}

/// Lon windows of a cell from its four lon/lat vertices (no polar expansion).
///
/// Pole-touching cells expand `rect_bound` to full-lng; the true wedge is the
/// shortest span covering the vertices (largest exterior gap on the circle).
fn vertex_lng_windows(vertices: &[Point; 4]) -> ((f64, f64), Option<(f64, f64)>) {
    let mut lons = [vertices[0].x, vertices[1].x, vertices[2].x, vertices[3].x];
    lons.sort_by(f64::total_cmp);
    // Largest gap on the circle is exterior; the complementary arc is the
    // cell's lon span (possibly wrapped across ±180).
    let gaps = [
        (lons[1] - lons[0], 0_usize),
        (lons[2] - lons[1], 1_usize),
        (lons[3] - lons[2], 2_usize),
        ((lons[0] + 360.0) - lons[3], 3_usize),
    ];
    let mut gap_i = 0_usize;
    let mut gap_w = gaps[0].0;
    for &(w, i) in &gaps[1..] {
        if w > gap_w {
            gap_w = w;
            gap_i = i;
        }
    }
    if gap_i == 3 {
        // Exterior crosses the antimeridian → vertices form one non-wrapped band.
        ((lons[0], lons[3]), None)
    } else {
        // Exterior is an interior gap → span wraps: [lons[gap_i+1], 180] ∪ [-180, lons[gap_i]].
        let start = lons[gap_i + 1];
        let end = lons[gap_i];
        ((start, 180.0), Some((-180.0, end)))
    }
}

/// Sound Interior certificate for seam-crossing / full-longitude cell bounds.
///
/// `rect` contains the true spherical cell. Split into non-wrapped lon
/// windows (full-lng is one world-wide window); if every positive-width
/// window is `Interior` under the native rect classifier, the cell is
/// interior. Never used to certify Outside (that stays fail-open Boundary).
fn wrapped_rect_is_interior(ctx: &CoverContext, rect: super::cell::LatLngRect) -> bool {
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

#[inline]
fn window_interior(ctx: &CoverContext, west: f64, south: f64, east: f64, north: f64) -> bool {
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
mod tests {
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq, Polygon, Ring, Strictness};

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
            // Full-lng fail-open only for genuinely full-longitude sources.
            if covered == 4 && relate_wrapped_rect_is_interior(ctx, rect) {
                return CellClass::Interior;
            }
            if rect.is_full_lng() && !ctx.windows.is_full_longitude() {
                if covered > 0 || center_hit {
                    return CellClass::Boundary;
                }
                let (first, second) = vertex_lng_windows(&vertices);
                if !ctx
                    .windows
                    .may_overlap_lng(first, second, rect.lat_lo, rect.lat_hi)
                {
                    return CellClass::Outside;
                }
                return CellClass::Boundary;
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
        let ctx = CoverContext {
            windows: SourceWindows::new(source),
            tester: Arc::new(PointBatchTester::new(source)),
            rects: NativeRectClassifier::new(source),
            point_leaves: point_leaves(source),
        };
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
    fn native_coverer_matches_relate_oracle_on_representative_shapes() {
        let seam_source = match (
            box_shape(179.5, -1.0, 180.0, 1.0),
            box_shape(-180.0, -1.0, -179.5, 1.0),
        ) {
            (Shape::Polygon(a), Shape::Polygon(b)) => Shape::MultiPolygon(vec![a, b]),
            _ => unreachable!(),
        };
        let adjacent_collection = Shape::GeometryCollection(vec![
            box_shape(-1.0, -1.0, 0.03, 1.0),
            box_shape(0.03, -1.0, 1.0, 1.0),
        ]);
        let overlapping_collection = Shape::GeometryCollection(vec![
            box_shape(-1.0, -1.0, 0.25, 1.0),
            box_shape(-0.25, -1.0, 1.0, 1.0),
        ]);
        let nested_collection = Shape::GeometryCollection(vec![
            box_shape(2.0, 2.0, 3.0, 3.0),
            box_shape(2.51, 2.45, 2.55, 2.56),
        ]);
        let cases = [
            (
                "box",
                box_shape(13.0, 52.0, 14.0, 53.0),
                [6_u8, 8, 10].as_slice(),
            ),
            ("hole", polygon_with_hole(), [5_u8, 7, 9].as_slice()),
            (
                "line",
                Shape::LineString(
                    LineSeq::try_new(CoordSeq::from(vec![
                        Point::new_unchecked_xy(-3.0, -2.0),
                        Point::new_unchecked_xy(4.0, 3.0),
                    ]))
                    .expect("test line is valid"),
                ),
                [5_u8, 7, 9].as_slice(),
            ),
            (
                "mixed",
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
                ]),
                [5_u8, 7, 8].as_slice(),
            ),
            (
                "adjacent-areal-collection",
                adjacent_collection,
                [8_u8, 10, 12].as_slice(),
            ),
            (
                "overlapping-areal-collection",
                overlapping_collection,
                [8_u8, 10, 12].as_slice(),
            ),
            (
                "nested-areal-collection",
                nested_collection,
                [8_u8, 10, 12].as_slice(),
            ),
            (
                "thin-boundary",
                box_shape(-0.25, -0.05, 0.25, 0.05),
                [6_u8, 8, 10].as_slice(),
            ),
            ("antimeridian", seam_source, [4_u8, 6, 8].as_slice()),
        ];
        for (name, source, levels) in cases {
            for level in levels {
                let coverer = fixed(*level);
                let native = coverer
                    .cover(&source)
                    .expect("small fixed-level covering is within budget");
                let oracle = cover_with_relate_oracle(coverer, &source);
                assert_eq!(native.outer(), oracle.outer(), "{name} level {level} outer");
                assert_eq!(
                    native.interior(),
                    oracle.interior(),
                    "{name} level {level} interior"
                );
            }
        }
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

    /// Fixed-level covering of a box: complete (every interior sample's
    /// ancestor cell is in `outer`), exact (no stray cells), and
    /// `interior ⊆ outer` with covered centers.
    #[test]
    fn fixed_level_box_covering_is_complete_and_exact() {
        let source = box_shape(13.0, 52.0, 14.0, 53.0);
        let covering = fixed(10)
            .cover(&source)
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
            .cover(&source)
            .expect("small fixed-level covering is within budget");
        let outer = covering.outer();
        // The notch interior (around 3, 3) is far from the L; its cell must
        // not appear, while the bbox covering would include it.
        let notch = CellId::from_lonlat(3.0, 3.0).parent(8).expect("coarser");
        assert!(!has(&outer, notch));
        let arm = CellId::from_lonlat(0.5, 3.5).parent(8).expect("coarser");
        assert!(has(&outer, arm));
    }

    /// Adaptive budgets produce mixed levels under max_cells with interior
    /// certification intact. Explicit `max_cells` is a hard emission cap
    /// (no hidden floor); the coverer must stay within that count.
    #[test]
    fn adaptive_budget_produces_mixed_levels() {
        let source = box_shape(10.0, 40.0, 20.0, 50.0);
        let max_cells = 64;
        let coverer = Coverer {
            min_level: 4,
            max_level: 12,
            level_mod: 1,
            max_cells: Some(max_cells),
            target_cells: max_cells,
        };
        let covering = coverer
            .cover(&source)
            .expect("adaptive covering under an explicit budget");
        let outer = covering.outer();
        let interior = covering.interior();
        assert!(
            outer.len() <= max_cells,
            "explicit max_cells is a hard cap, got {}",
            outer.len()
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
            .cover(&source)
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
            .cover(&point)
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
            .cover(&source)
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
        .cover(&source)
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
        .cover(&source)
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
            .cover(&source)
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
        .cover(&source)
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
            .cover(&source)
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
        .cover(&longer)
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
        .cover(&longer)
        .expect("budget N must fit");
        assert_eq!(fit.outer(), long_u.outer());
        let err = Coverer {
            min_level: 10,
            max_level: 10,
            level_mod: 1,
            max_cells: Some(n - 1),
            target_cells: 8,
        }
        .cover(&longer)
        .expect_err("budget N-1 must raise");
        assert_eq!(err.limit, n - 1);
        assert!(err.to_string().contains("max_cells"));
    }
}
