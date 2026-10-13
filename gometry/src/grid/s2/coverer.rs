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

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::geometry::Shape;
use crate::grid::affine_source::{
    GeographicCoverageSource, RectClass as AffineRectClass, SphericalGridTarget,
    WorkingShapeRelation,
};
pub(crate) use crate::grid::coverer::CellClass;
use crate::grid::s2::cellid::CellId;
use crate::grid::s2::projection::MAX_LEVEL;
use crate::grid::{CoverBudgetExceeded, CoverError, ensure_cover_budget};

fn cover_total(counts: &[usize]) -> usize {
    counts.iter().fold(0_usize, |total, &count| {
        total
            .checked_add(count)
            .expect("simultaneous S2 frontier lengths fit usize")
    })
}

#[cfg(test)]
const S2_LEVEL_COUNT: usize = MAX_LEVEL as usize + 1;
mod candidate;
mod classify;
mod refine;
mod stats;

use candidate::{
    Candidate, CandidateCertificate, CertificateBatch, PointMembership, Productivity,
    admit_visible_candidate, debug_assert_budget, is_visible, productive_state, retain_productive,
};
#[cfg(test)]
use classify::classify;
use classify::{CoverContext, classify_one, rect_bounds, stage_children_at};
pub(crate) use stats::CoverStats;

/// Level budget for a covering. `min_level <= max_level`; emitted levels
/// satisfy `(level - min_level) % level_mod == 0`; `target_cells` guides
/// optional adaptive refinement. `max_cells` is a coarsening ceiling and hard
/// emission/allocation budget (`None` = unlimited); if the ceiling cannot be
/// met without violating `min_level`, construction returns a typed rejection.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Coverer {
    pub min_level: u8,
    pub max_level: u8,
    pub level_mod: u8,
    /// Coarsening ceiling and finite emission/allocation cap; adaptive covering
    /// may return a typed rejection when it cannot meet this above `min_level`.
    /// `None` = unlimited.
    pub max_cells: Option<usize>,
    /// Adaptive refinement target, independent of the hard emission cap.
    pub target_cells: usize,
}

/// Public cell-selection rule applied while the cover tree emits results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CoverRule {
    Overlap,
    Within,
    Center,
}

/// An exact classified covering: one sorted cell column plus an interior bit.
#[derive(Clone, Debug)]
pub(crate) struct Covering {
    cells: Vec<(CellId, bool)>,
    #[cfg(test)]
    stats: CoverStats,
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

    #[cfg(test)]
    const fn stats(&self) -> CoverStats {
        self.stats
    }
}

impl Coverer {
    /// `max_cells` is a coarsening ceiling for adaptive construction and a hard
    /// emission/allocation cap for fixed-level construction. If adaptive
    /// coarsening cannot meet it above `min_level`, return a typed rejection.
    /// `target_cells` remains the adaptive quality target and does not replace
    /// or widen this ceiling.
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
    /// Fixed-level `max_cells` charges only cells visible under `rule`
    /// (emitted terminals and visible phase-2 queue entries),
    /// never the transient fail-open DFS frontier — conservative Boundary
    /// candidates can vanish on deeper classification, so the frontier size
    /// is not a lower bound on final emissions. Interior subtrees preflight
    /// exact `4^Δ` multiplicity before enumerating. Adaptive splits above
    /// `min_level` stage actual 4/16/64 (`level_mod`) non-Outside children and
    /// commit only when the projected total still fits `target_cells`;
    /// otherwise the parent is retained only when it is visible under `rule`
    /// (optional coarsening, never below `min_level`).
    ///
    /// `max_cells = None` is unlimited (factory `max_cells=None`).
    #[cfg(test)]
    pub(crate) fn cover(
        &self,
        working: &Shape,
        affine: &GeographicCoverageSource,
        relation: WorkingShapeRelation,
    ) -> Result<Covering, CoverError> {
        self.cover_for_rule(working, affine, relation, CoverRule::Overlap)
    }

    pub(crate) fn cover_for_rule(
        &self,
        working: &Shape,
        affine: &GeographicCoverageSource,
        relation: WorkingShapeRelation,
        rule: CoverRule,
    ) -> Result<Covering, CoverError> {
        debug_assert_eq!(affine.target(), SphericalGridTarget::S2);
        if rule == CoverRule::Within && !affine.has_polygon() {
            return Ok(Covering {
                cells: Vec::new(),
                #[cfg(test)]
                stats: CoverStats::default(),
            });
        }
        let ctx = CoverContext::prepare(working, affine, relation)?;
        self.cover_prepared(&ctx, rule)
    }

    fn cover_prepared(
        &self,
        ctx: &CoverContext<'_>,
        rule: CoverRule,
    ) -> Result<Covering, CoverError> {
        let mut stats = CoverStats::default();
        // E = emitted cells; Q = visible phase-2 emissions. Non-visible
        // candidates use depth-first scratch and never consume max_cells.
        let mut cells: Vec<(CellId, bool)> = Vec::with_capacity(1);
        let fixed_depth = self.min_level == self.max_level;
        // Visible phase-2 frontier (candidates already at/above min_level).
        let mut queue: BinaryHeap<(Reverse<u8>, CellId, Candidate)> = if fixed_depth {
            BinaryHeap::new()
        } else {
            BinaryHeap::with_capacity(6)
        };
        // Non-visible boundary candidates can still have visible descendants.
        // Keep them depth-first so scratch scales with depth/fanout, not the
        // number of discarded candidates.
        let stack_depth_capacity = usize::from(MAX_LEVEL)
            .checked_mul(3)
            .expect("S2 stack depth capacity fits usize");
        let mut invisible: Vec<Candidate> = if fixed_depth {
            Vec::new()
        } else {
            Vec::with_capacity(stack_depth_capacity)
        };
        // Phase-1 DFS stack: unresolved candidates still below min_level.
        // Fail-open Boundary entries may later classify Outside — do not
        // charge them until proven productive.
        let dfs_capacity = Face::ALL
            .len()
            .checked_add(stack_depth_capacity)
            .expect("S2 DFS capacity fits usize");
        let mut dfs: Vec<Candidate> = Vec::with_capacity(dfs_capacity);
        // Reusable staging buffer for 4/16/64 target descendants.
        let mut staged: Vec<Candidate> = Vec::with_capacity(64);

        self.seed_faces(
            ctx,
            rule,
            &mut cells,
            &mut dfs,
            &mut queue,
            &mut invisible,
            &mut stats,
        )?;
        self.drain_invisible(
            ctx,
            rule,
            &mut cells,
            &mut queue,
            &mut invisible,
            &mut staged,
            &mut stats,
        )?;
        self.force_min_level(
            ctx,
            rule,
            &mut cells,
            &mut dfs,
            &mut queue,
            &mut invisible,
            &mut staged,
            &mut stats,
        )?;
        self.adaptive_emit(
            ctx,
            rule,
            &mut cells,
            &mut queue,
            &mut invisible,
            &mut staged,
            &mut stats,
        )?;

        cells.sort_unstable_by_key(|(id, _)| *id);
        Ok(Covering {
            cells,
            #[cfg(test)]
            stats,
        })
    }

    #[cfg(test)]
    fn cover_identity(&self, source: &Shape) -> Result<Covering, CoverError> {
        let affine = GeographicCoverageSource::new(source, SphericalGridTarget::S2)
            .expect("test source carrier allocates");
        self.cover(source, &affine, WorkingShapeRelation::Identity)
    }

    #[cfg(test)]
    fn cover_identity_for_rule(
        &self,
        source: &Shape,
        rule: CoverRule,
    ) -> Result<Covering, CoverError> {
        let affine = GeographicCoverageSource::new(source, SphericalGridTarget::S2)
            .expect("test source carrier allocates");
        self.cover_for_rule(source, &affine, WorkingShapeRelation::Identity, rule)
    }
}

#[cfg(test)]
#[path = "coverer/tests.rs"]
mod tests;
