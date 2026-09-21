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
use std::sync::Arc;

use crate::geometry::{Bounds, Point, PointBatchTester, Shape, same_point};
use crate::grid::affine_source::{
    GeographicCoverageSource, RectClass as AffineRectClass, RectangularCandidate,
    SphericalGridTarget, WorkingShapeRelation,
};
pub(crate) use crate::grid::coverer::CellClass;
use crate::grid::coverer::{NativeRectClassifier, RectClass};
use crate::grid::s2::cell::Cell;
use crate::grid::s2::cellid::{CellId, Face};
use crate::grid::s2::projection::MAX_LEVEL;
use crate::grid::spherical_arc::{
    Bound, CertifiedDegreeWindows, CertifiedLongitudeDegrees, DegreeWindowResult,
};
use crate::grid::{CoverBudgetExceeded, CoverError, ensure_cover_budget};

#[cfg(test)]
const S2_LEVEL_COUNT: usize = MAX_LEVEL as usize + 1;

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

/// Per-cover observations used by the native traversal tests.
///
/// The counters are test-only fields so production covers retain no diagnostic
/// state.  The operation still passes one zero-sized counter value through the
/// traversal in non-test builds, keeping the instrumentation seam at the
/// operation boundary rather than in global state. `visited_unique_candidates`
/// is derived from raw classifications: the traversal's only repeated
/// candidate observations are explicit carried-certificate hits.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CoverStats {
    #[cfg(test)]
    raw_classify_calls: usize,
    #[cfg(test)]
    classifications_by_level: [usize; S2_LEVEL_COUNT],
    #[cfg(test)]
    visited_unique_candidates: usize,
    #[cfg(test)]
    unique_local_point_probes: usize,
    #[cfg(test)]
    local_duplicate_hits: usize,
    #[cfg(test)]
    carried_certificate_hits: usize,
    #[cfg(test)]
    carried_certificates_by_level: [usize; S2_LEVEL_COUNT],
    #[cfg(test)]
    productive_path_reuses: usize,
    #[cfg(test)]
    productive_path_reuses_by_level: [usize; S2_LEVEL_COUNT],
    #[cfg(test)]
    productive_path_hits: usize,
    #[cfg(test)]
    visible_queue_peak: usize,
    #[cfg(test)]
    productive_frontier_peak: usize,
    #[cfg(test)]
    committed_emissions: usize,
    #[cfg(test)]
    invisible_stack_peak: usize,
    #[cfg(test)]
    certificate_storage_peak: usize,
    #[cfg(test)]
    retained_map_entries: usize,
}

#[cfg_attr(
    not(test),
    expect(
        clippy::needless_pass_by_ref_mut,
        reason = "the counter mutates in test builds and is intentionally zero-sized in production"
    )
)]
impl CoverStats {
    const fn raw_classify(&mut self, level: u8) {
        #[cfg(not(test))]
        let _ = level;
        #[cfg(test)]
        {
            self.raw_classify_calls += 1;
            self.classifications_by_level[level as usize] += 1;
            self.visited_unique_candidates += 1;
        }
    }

    const fn unique_local_point_probe(&mut self) {
        #[cfg(test)]
        {
            self.unique_local_point_probes += 1;
        }
    }

    const fn local_duplicate_hit(&mut self) {
        #[cfg(test)]
        {
            self.local_duplicate_hits += 1;
        }
    }

    const fn carried_certificate_hit(&mut self, level: u8) {
        #[cfg(not(test))]
        let _ = level;
        #[cfg(test)]
        {
            self.carried_certificate_hits += 1;
            self.carried_certificates_by_level[level as usize] += 1;
        }
    }

    const fn productive_path_reuse(&mut self, level: u8) {
        #[cfg(not(test))]
        let _ = level;
        #[cfg(test)]
        {
            self.productive_path_reuses += 1;
            self.productive_path_reuses_by_level[level as usize] += 1;
        }
    }

    const fn productive_path_hit(&mut self) {
        #[cfg(test)]
        {
            self.productive_path_hits += 1;
        }
    }

    const fn visible_queue_peak(&mut self, len: usize) {
        #[cfg(not(test))]
        let _ = len;
        #[cfg(test)]
        {
            if len > self.visible_queue_peak {
                self.visible_queue_peak = len;
            }
        }
    }

    const fn productive_frontier(&mut self, emitted: usize, queued: usize) {
        #[cfg(not(test))]
        let _ = (emitted, queued);
        #[cfg(test)]
        {
            let len = emitted.saturating_add(queued);
            if len > self.productive_frontier_peak {
                self.productive_frontier_peak = len;
            }
        }
    }

    const fn committed_emission(&mut self) {
        #[cfg(test)]
        {
            self.committed_emissions += 1;
        }
    }

    const fn invisible_stack_peak(&mut self, len: usize) {
        #[cfg(not(test))]
        let _ = len;
        #[cfg(test)]
        {
            if len > self.invisible_stack_peak {
                self.invisible_stack_peak = len;
            }
        }
    }

    #[cfg(test)]
    const fn certificate_storage_peak(&mut self, len: usize) {
        if len > self.certificate_storage_peak {
            self.certificate_storage_peak = len;
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            clippy::missing_const_for_fn,
            reason = "certificate accounting walks runtime frontier storage"
        )
    )]
    fn observe_candidate(&mut self, candidate: &Candidate) {
        #[cfg(not(test))]
        let _ = candidate;
        #[cfg(test)]
        self.certificate_storage_peak(candidate.certificate_storage());
    }

    #[cfg_attr(
        not(test),
        expect(
            clippy::missing_const_for_fn,
            reason = "certificate accounting walks runtime batch storage"
        )
    )]
    fn observe_batch(&mut self, candidates: &[Candidate]) {
        #[cfg(not(test))]
        let _ = candidates;
        #[cfg(test)]
        self.certificate_storage_peak(candidates.iter().map(Candidate::certificate_storage).sum());
    }

    #[cfg_attr(
        not(test),
        expect(
            clippy::missing_const_for_fn,
            reason = "certificate accounting walks runtime frontier storage"
        )
    )]
    fn observe_frontier(
        &mut self,
        dfs: &[Candidate],
        queue: &BinaryHeap<(Reverse<u8>, CellId, Candidate)>,
        invisible: &[Candidate],
        staged: &[Candidate],
    ) {
        #[cfg(not(test))]
        let _ = (dfs, queue, invisible, staged);
        #[cfg(test)]
        self.certificate_storage_peak(
            dfs.iter()
                .chain(queue.iter().map(|(_, _, candidate)| candidate))
                .chain(invisible)
                .chain(staged)
                .map(Candidate::certificate_storage)
                .sum(),
        );
    }
}

/// One classified S2 candidate.  The membership facts are carried with the
/// candidate so visibility never has to ask the source tester a second time.
#[derive(Clone, Copy, Debug)]
struct CandidateCertificate {
    id: CellId,
    class: CellClass,
    membership: Option<PointMembership>,
}

#[derive(Clone, Copy, Debug)]
struct PointMembership {
    vertices: [bool; 4],
    center: bool,
}

/// Certificates for every child in one parent-local fan. The successful
/// branch carries its own nested batches; siblings retain only their already
/// classified candidate certificate until their own productivity is needed.
#[derive(Clone, Debug)]
struct CertificateBatch {
    candidates: Vec<Candidate>,
}

impl CertificateBatch {
    #[cfg(test)]
    fn certificate_storage(&self) -> usize {
        self.candidates
            .iter()
            .map(Candidate::certificate_storage)
            .sum()
    }
}

/// A candidate's productivity state. `Terminal` covers an interior candidate
/// or a boundary candidate at the last permitted level. `Children` carries a
/// finite fan whose certificates were already paid while proving that at
/// least one descendant is productive.
#[derive(Clone, Debug)]
enum Productivity {
    Terminal,
    Children(CertificateBatch),
}

impl Productivity {
    #[cfg(test)]
    fn certificate_storage(&self) -> usize {
        match self {
            Self::Terminal => 0,
            Self::Children(batch) => batch.certificate_storage(),
        }
    }
}

/// A frontier candidate may carry the finite sibling batch returned while
/// establishing that it has a terminal descendant. `Some` means productivity
/// is known; `None` means it is unknown.
#[derive(Clone, Debug)]
struct Candidate {
    certificate: CandidateCertificate,
    productivity: Option<Productivity>,
}

impl Candidate {
    const fn new(certificate: CandidateCertificate) -> Self {
        Self {
            certificate,
            productivity: None,
        }
    }

    #[cfg(test)]
    fn certificate_storage(&self) -> usize {
        1 + self
            .productivity
            .as_ref()
            .map_or(0, Productivity::certificate_storage)
    }
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.certificate.id == other.certificate.id
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.certificate.id.cmp(&other.certificate.id)
    }
}

/// The prepared classification context: built once per covering, reused by
/// every candidate — the hierarchical [`PointBatchTester`] for vertex/center
/// probes and the native rectangle classifier for rect-polygon certificates.
/// Rect Outside is the only negative certificate; the planar chord proxy is
/// never used to prune (see module soundness model).
struct CoverContext<'a> {
    tester: Arc<PointBatchTester>,
    rects: NativeRectClassifier,
    /// For a point carrier (including collapsed linework), every leaf cell
    /// containing a source point under closed spherical-cell semantics.  This
    /// is the exact point fast path; unlike the historical version it uses the
    /// pole/seam-normalized owner representation rather than rounded trig XYZ.
    point_leaves: Option<Vec<CellId>>,
    /// The unsplit periodic source owns candidate negatives. Its richer
    /// corroboration is needed only on seam/pole/periodic or otherwise
    /// uncertain inputs.
    affine: &'a GeographicCoverageSource,
    corroborate: bool,
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
        // Visible phase-2 frontier (candidates already at/above min_level).
        let mut queue: BinaryHeap<(Reverse<u8>, CellId, Candidate)> = BinaryHeap::with_capacity(6);
        // Non-visible boundary candidates can still have visible descendants.
        // Keep them depth-first so scratch scales with depth/fanout, not the
        // number of discarded candidates.
        let mut invisible: Vec<Candidate> = Vec::with_capacity(3 * MAX_LEVEL as usize);
        // Phase-1 DFS stack: unresolved candidates still below min_level.
        // Fail-open Boundary entries may later classify Outside — do not
        // charge them until proven productive.
        let mut dfs: Vec<Candidate> = Vec::with_capacity(6 + 3 * MAX_LEVEL as usize);
        // Reusable staging buffer for 4/16/64 target descendants.
        let mut staged: Vec<Candidate> = Vec::with_capacity(64);

        self.seed_faces(
            ctx,
            rule,
            &cells,
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

    /// Classify the six face roots into the phase-1 DFS stack or phase-2 queue.
    fn seed_faces(
        self,
        ctx: &CoverContext<'_>,
        rule: CoverRule,
        cells: &[(CellId, bool)],
        dfs: &mut Vec<Candidate>,
        queue: &mut BinaryHeap<(Reverse<u8>, CellId, Candidate)>,
        invisible: &mut Vec<Candidate>,
        stats: &mut CoverStats,
    ) -> Result<(), CoverError> {
        for face in Face::ALL {
            let id = CellId::from_face(face);
            let mut candidate = classify_one(ctx, id, stats)?;
            match candidate.certificate.class {
                CellClass::Outside => {},
                _class => {
                    if self.min_level == 0 {
                        // Only charge proven-productive faces (fail-open may
                        // prune entirely under adaptive max_level).
                        let Some(productivity) =
                            productive_state(ctx, candidate.certificate, self, stats)?
                        else {
                            continue;
                        };
                        candidate.productivity = Some(productivity);
                        let visible = is_visible(rule, candidate.certificate);
                        ensure_cover_budget(
                            cells
                                .len()
                                .saturating_add(queue.len())
                                .saturating_add(usize::from(visible)),
                            self.hard_budget(),
                        )?;
                        if visible {
                            queue.push((
                                Reverse(candidate.certificate.id.level()),
                                candidate.certificate.id,
                                candidate,
                            ));
                            stats.visible_queue_peak(queue.len());
                            stats.productive_frontier(cells.len(), queue.len());
                            stats.observe_frontier(dfs, queue, invisible, &[]);
                        } else {
                            invisible.push(candidate);
                            stats.invisible_stack_peak(invisible.len());
                            stats.observe_frontier(dfs, queue, invisible, &[]);
                        }
                    } else {
                        // Below min_level: unresolved — resolve depth-first
                        // without charging the transient frontier.
                        dfs.push(candidate);
                    }
                },
            }
        }
        Ok(())
    }

    /// Phase 1: depth-first forced descent to `min_level`.
    ///
    /// Budget charges only visible emissions: interior `4^Δ` terminals and
    /// visible phase-2 queue entries. Fail-open
    /// Boundary that deeper adaptive refinement prunes to Outside is never
    /// charged (same class as R11 B2, applied at the min_level queue entry).
    fn force_min_level(
        self,
        ctx: &CoverContext<'_>,
        rule: CoverRule,
        cells: &mut Vec<(CellId, bool)>,
        dfs: &mut Vec<Candidate>,
        queue: &mut BinaryHeap<(Reverse<u8>, CellId, Candidate)>,
        invisible: &mut Vec<Candidate>,
        staged: &mut Vec<Candidate>,
        stats: &mut CoverStats,
    ) -> Result<(), CoverError> {
        while let Some(candidate) = dfs.pop() {
            let certificate = candidate.certificate;
            debug_assert!(certificate.id.level() < self.min_level);
            // Proven base: emitted + phase-2 queue (not the unresolved DFS).
            let proven = cells.len() + queue.len();
            match certificate.class {
                CellClass::Outside => unreachable!("Outside never enters the frontier"),
                CellClass::Interior => {
                    emit_interior_descendants(
                        self,
                        certificate.id,
                        certificate.id.level(),
                        proven,
                        cells,
                        stats,
                    )?;
                    stats.productive_frontier(cells.len(), queue.len());
                    debug_assert_budget(cells.len(), queue.len(), self.hard_budget());
                },
                CellClass::Boundary => {
                    stage_children_at(
                        ctx,
                        certificate.id,
                        certificate.id.level() + 1,
                        None,
                        staged,
                        stats,
                    )?;
                    let child_level = certificate.id.level() + 1;
                    if child_level >= self.min_level {
                        // Drop fail-open children that resolve to Outside
                        // before charging the phase-2 queue.
                        retain_productive(ctx, staged, self, stats)?;
                        for child in staged.drain(..) {
                            let visible = is_visible(rule, child.certificate);
                            ensure_cover_budget(
                                cells
                                    .len()
                                    .saturating_add(queue.len())
                                    .saturating_add(usize::from(visible)),
                                self.hard_budget(),
                            )?;
                            if visible {
                                queue.push((
                                    Reverse(child.certificate.id.level()),
                                    child.certificate.id,
                                    child,
                                ));
                                stats.visible_queue_peak(queue.len());
                                stats.productive_frontier(cells.len(), queue.len());
                                stats.observe_frontier(dfs, queue, invisible, &[]);
                            } else {
                                invisible.push(child);
                                stats.invisible_stack_peak(invisible.len());
                                stats.observe_frontier(dfs, queue, invisible, &[]);
                            }
                        }
                        // Preserve the old hidden LIFO order, but only for
                        // this finite fan. No min-level candidate survives
                        // into a global phase-two hidden list.
                        self.drain_invisible(ctx, rule, cells, queue, invisible, staged, stats)?;
                    } else {
                        staged.retain(|child| child.certificate.class != CellClass::Outside);
                        dfs.reserve(staged.len());
                        dfs.append(staged);
                        stats.observe_frontier(dfs, queue, invisible, staged);
                    }
                },
            }
        }
        Ok(())
    }

    /// Phase 2: adaptive refinement (actual K fanout) or emit conforming cells.
    ///
    /// Staged children are filtered to proven-productive before visibility and budget charge:
    /// fail-open Boundary that deeper classification prunes to Outside never
    /// enters the queue. Optional coarsen uses E+Q+staged against the
    /// adaptive `target_cells` guide.
    fn adaptive_emit(
        self,
        ctx: &CoverContext<'_>,
        rule: CoverRule,
        cells: &mut Vec<(CellId, bool)>,
        queue: &mut BinaryHeap<(Reverse<u8>, CellId, Candidate)>,
        invisible: &mut Vec<Candidate>,
        staged: &mut Vec<Candidate>,
        stats: &mut CoverStats,
    ) -> Result<(), CoverError> {
        loop {
            let (candidate, visible) = if let Some(candidate) = invisible.pop() {
                (candidate, false)
            } else if let Some((Reverse(_), _, candidate)) = queue.pop() {
                (candidate, true)
            } else {
                break;
            };
            debug_assert!(candidate.certificate.id.level() >= self.min_level);
            self.process_adaptive_candidate(
                ctx, rule, cells, queue, invisible, staged, candidate, visible, stats,
            )?;
        }
        Ok(())
    }

    /// Process invisible candidates immediately in DFS order. The stack holds
    /// at most the current refinement fan plus its ancestor siblings; it never
    /// becomes a second result-sized frontier.
    fn drain_invisible(
        self,
        ctx: &CoverContext<'_>,
        rule: CoverRule,
        cells: &mut Vec<(CellId, bool)>,
        queue: &mut BinaryHeap<(Reverse<u8>, CellId, Candidate)>,
        invisible: &mut Vec<Candidate>,
        staged: &mut Vec<Candidate>,
        stats: &mut CoverStats,
    ) -> Result<(), CoverError> {
        while let Some(candidate) = invisible.pop() {
            self.process_adaptive_candidate(
                ctx, rule, cells, queue, invisible, staged, candidate, false, stats,
            )?;
        }
        Ok(())
    }

    /// Refine a visible or invisible candidate. Its parent-local certificate
    /// batch is consumed by `stage_children_at`, so the adaptive descent does
    /// not classify the finite fan a second time.
    fn process_adaptive_candidate(
        self,
        ctx: &CoverContext<'_>,
        rule: CoverRule,
        cells: &mut Vec<(CellId, bool)>,
        queue: &mut BinaryHeap<(Reverse<u8>, CellId, Candidate)>,
        invisible: &mut Vec<Candidate>,
        staged: &mut Vec<Candidate>,
        mut candidate: Candidate,
        visible: bool,
        stats: &mut CoverStats,
    ) -> Result<(), CoverError> {
        let certificate = candidate.certificate;
        if self.can_refine(certificate.id.level(), certificate.class) {
            let target = certificate.id.level() + self.level_mod;
            stage_children_at(
                ctx,
                certificate.id,
                target,
                candidate.productivity.take(),
                staged,
                stats,
            )?;
            retain_productive(ctx, staged, self, stats)?;
            if staged.is_empty() {
                // Fail-open parent pruned entirely — not an emission.
                return Ok(());
            }
            let staged_visible = staged
                .iter()
                .filter(|child| is_visible(rule, child.certificate))
                .count();
            let base = cells.len() + queue.len();
            let projected = base.saturating_add(staged_visible);
            let fits_target = projected <= self.target_budget();
            let fits_hard = self.hard_budget().is_none_or(|limit| projected <= limit);
            if fits_target && fits_hard {
                // Validate the projected visible frontier once before making
                // any push. Invisible descendants remain outside the budget.
                ensure_cover_budget(projected, self.hard_budget())?;
                for child in staged.drain(..) {
                    if is_visible(rule, child.certificate) {
                        queue.push((
                            Reverse(child.certificate.id.level()),
                            child.certificate.id,
                            child,
                        ));
                        stats.visible_queue_peak(queue.len());
                        stats.productive_frontier(cells.len(), queue.len());
                        stats.observe_frontier(&[], queue, invisible, &[]);
                    } else {
                        invisible.push(child);
                        stats.invisible_stack_peak(invisible.len());
                        stats.observe_frontier(&[], queue, invisible, &[]);
                    }
                }
                return Ok(());
            }
            // Optional split does not fit — emit the conforming parent
            // (never below min_level; F7 purity preserved).
        }
        // Emit at this level (interior, max-level boundary, or unaffordable
        // split). Invisible terminal candidates drop immediately.
        if visible {
            let projected = cells.len().saturating_add(queue.len()).saturating_add(1);
            ensure_cover_budget(projected, self.hard_budget())?;
            cells.push((certificate.id, certificate.class == CellClass::Interior));
            stats.committed_emission();
            stats.productive_frontier(cells.len(), queue.len());
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
    staged: &mut Vec<Candidate>,
    coverer: Coverer,
    stats: &mut CoverStats,
) -> Result<(), CoverError> {
    let mut index = 0;
    while index < staged.len() {
        if staged[index].certificate.class == CellClass::Outside {
            staged.remove(index);
            continue;
        }
        if staged[index].productivity.is_some() {
            stats.productive_path_hit();
            index += 1;
            continue;
        }
        let certificate = staged[index].certificate;
        let Some(productivity) = productive_state(ctx, certificate, coverer, stats)? else {
            staged.remove(index);
            continue;
        };
        staged[index].productivity = Some(productivity);
        stats.observe_candidate(&staged[index]);
        index += 1;
    }
    Ok(())
}

/// Find one already-classified branch that yields a final emission, retaining
/// the finite sibling batch needed by the later adaptive descent.
///
/// The returned state is deliberately a finite certificate batch, not a
/// global cell-result memo. Nested successful children carry their own batch.
fn productive_state(
    ctx: &CoverContext<'_>,
    root: CandidateCertificate,
    coverer: Coverer,
    stats: &mut CoverStats,
) -> Result<Option<Productivity>, CoverError> {
    match root.class {
        CellClass::Outside => Ok(None),
        CellClass::Interior => Ok(Some(Productivity::Terminal)),
        CellClass::Boundary => {
            if !coverer.can_refine(root.id.level(), root.class) {
                return Ok(Some(Productivity::Terminal));
            }
            let target = root.id.level() + coverer.level_mod;
            let mut children = Vec::with_capacity(fan_size(root.id.level(), target));
            stage_children_at(ctx, root.id, target, None, &mut children, stats)?;
            for index in 0..children.len() {
                let child_certificate = children[index].certificate;
                if child_certificate.class == CellClass::Outside {
                    continue;
                }
                if child_certificate.class == CellClass::Interior
                    || !coverer.can_refine(child_certificate.id.level(), child_certificate.class)
                {
                    children[index].productivity = Some(Productivity::Terminal);
                    stats.observe_batch(&children);
                    return Ok(Some(Productivity::Children(CertificateBatch {
                        candidates: children,
                    })));
                }
                let Some(productivity) = productive_state(ctx, child_certificate, coverer, stats)?
                else {
                    continue;
                };
                children[index].productivity = Some(productivity);
                stats.observe_batch(&children);
                return Ok(Some(Productivity::Children(CertificateBatch {
                    candidates: children,
                })));
            }
            Ok(None)
        },
    }
}

impl<'a> CoverContext<'a> {
    fn prepare(
        source: &Shape,
        affine: &'a GeographicCoverageSource,
        relation: WorkingShapeRelation,
    ) -> Result<Self, crate::error::Error> {
        let tester = Arc::new(PointBatchTester::new(source)?);
        let shared_area_tester = matches!(source, Shape::Polygon(_) | Shape::MultiPolygon(_))
            .then(|| Arc::clone(&tester));
        Ok(Self {
            tester,
            rects: NativeRectClassifier::new_with_area_tester(source, shared_area_tester)?,
            point_leaves: closed_point_leaves(source),
            affine,
            corroborate: affine.s2_needs_corroboration(relation),
        })
    }
}

/// Leaf owners of a point-only source under S2's closed-cell relation.
///
/// The source factory has already performed the shared geographic pole
/// normalization. Empty linework represents the same point set as
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
    stats: &mut CoverStats,
) -> Result<(), CoverError> {
    // Required terminals: exactly 4^(min_level - level). Preflight the
    // projected count before enumerating so a tiny budget never walks a
    // multi-million child range.
    if let Some(n) = interior_multiplicity(level, coverer.min_level) {
        ensure_cover_budget(base.saturating_add(n), coverer.hard_budget())?;
        cells.reserve(n);
    } else if let Some(limit) = coverer.hard_budget() {
        // Shift overflow ⇒ larger than any finite usize budget.
        return Err(CoverBudgetExceeded::new(limit).into());
    }
    let end = id.child_end_at(coverer.min_level);
    let mut cursor = id.child_begin_at(coverer.min_level);
    while cursor != end {
        cells.push((cursor, true));
        stats.committed_emission();
        cursor = cursor.next();
    }
    // Unlimited path: still honor a finite budget if set mid-enumeration
    // (defensive; preflight covers the finite case).
    ensure_cover_budget(cells.len(), coverer.hard_budget())?;
    Ok(())
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

/// Number of descendants in one supported 1/2/3-level fan.
fn fan_size(parent_level: u8, target: u8) -> usize {
    debug_assert!(target > parent_level);
    let shift = 2 * u32::from(target - parent_level);
    1_usize
        .checked_shl(shift)
        .expect("S2 fan depth is limited to three levels")
}

/// Classify every descendant of `id` at exactly `target` level into `out`.
/// The finite fan owns its probe set: adjacent fans may repeat a point, but no
/// point result survives this call.
fn stage_children_at(
    ctx: &CoverContext<'_>,
    id: CellId,
    target: u8,
    reuse: Option<Productivity>,
    out: &mut Vec<Candidate>,
    stats: &mut CoverStats,
) -> Result<(), CoverError> {
    let reuse = reuse.and_then(|productivity| match productivity {
        Productivity::Terminal => {
            debug_assert!(false, "terminal productivity cannot refine");
            None
        },
        Productivity::Children(batch) => {
            debug_assert!(batch.candidates.iter().all(|candidate| {
                let child = candidate.certificate.id;
                child.level() == target && id.range_min() <= child && child <= id.range_max()
            }));
            Some(batch.candidates)
        },
    });
    let end = id.child_end_at(target);
    let mut cursor = id.child_begin_at(target);
    let ids = std::iter::from_fn(|| {
        (cursor != end).then(|| {
            let current = cursor;
            cursor = cursor.next();
            current
        })
    });
    classify_ids(
        ctx,
        ids,
        target,
        fan_size(id.level(), target),
        reuse,
        out,
        stats,
    )?;
    stats.observe_batch(out);
    Ok(())
}

fn classify_one(
    ctx: &CoverContext<'_>,
    id: CellId,
    stats: &mut CoverStats,
) -> Result<Candidate, CoverError> {
    let mut out = Vec::with_capacity(1);
    classify_ids(
        ctx,
        std::iter::once(id),
        id.level(),
        1,
        None,
        &mut out,
        stats,
    )?;
    Ok(out.pop().expect("one candidate was classified"))
}

/// The native test helper retains its old scalar shape while the traversal
/// consumes the full certificate.
#[cfg(test)]
fn classify(ctx: &CoverContext<'_>, id: CellId) -> CellClass {
    let mut stats = CoverStats::default();
    classify_one(ctx, id, &mut stats)
        .expect("test candidate classification")
        .certificate
        .class
}

struct LocalProbe {
    point: Point,
    covered: Option<bool>,
}

struct PendingCandidate {
    id: CellId,
    cell: Cell,
    rectangular: Option<RectangularCandidate>,
    affine: Option<AffineRectClass>,
    probe_indices: [usize; 5],
    membership_known: bool,
    reused: Option<Candidate>,
}

fn local_probe_index(
    probes: &mut Vec<LocalProbe>,
    point: Point,
    known: Option<bool>,
    stats: &mut CoverStats,
) -> usize {
    if let Some(index) = probes.iter().position(|probe| {
        probe.point.x.to_bits() == point.x.to_bits() && probe.point.y.to_bits() == point.y.to_bits()
    }) {
        stats.local_duplicate_hit();
        if let Some(known) = known {
            if let Some(existing) = probes[index].covered {
                debug_assert_eq!(
                    existing, known,
                    "local probe {:?} has conflicting carried membership",
                    probes[index].point
                );
            } else {
                probes[index].covered = Some(known);
            }
        }
        return index;
    }
    probes.push(LocalProbe {
        point,
        covered: known,
    });
    probes.len() - 1
}

/// Classify a finite fan after its local point probes have been deduplicated.
/// `reuse` contains the parent-local fan found while proving productivity;
/// consuming it here reuses every certificate in that finite batch.
fn classify_ids(
    ctx: &CoverContext<'_>,
    ids: impl IntoIterator<Item = CellId>,
    level: u8,
    fan_capacity: usize,
    reuse: Option<Vec<Candidate>>,
    out: &mut Vec<Candidate>,
    stats: &mut CoverStats,
) -> Result<(), CoverError> {
    out.clear();
    if out.capacity() < fan_capacity {
        out.reserve(fan_capacity - out.capacity());
    }
    let mut reuse = reuse;
    let mut probes = Vec::with_capacity(fan_capacity.saturating_mul(5));
    let mut pending = Vec::with_capacity(fan_capacity);

    for id in ids {
        let cell = Cell::from_id(id);
        debug_assert_eq!(id.level(), level);
        let vertices = cell.vertices_lonlat();
        let rect = cell.rect_bound(&vertices);
        let points = [
            vertices[0],
            vertices[1],
            vertices[2],
            vertices[3],
            cell.center_lonlat(),
        ];
        let reused = reuse.as_mut().and_then(|candidates| {
            candidates
                .iter()
                .position(|candidate| candidate.certificate.id == id)
                .map(|index| candidates.swap_remove(index))
        });
        if reused.is_some() {
            let level = id.level();
            stats.carried_certificate_hit(level);
            if reused
                .as_ref()
                .is_some_and(|candidate| candidate.productivity.is_some())
            {
                stats.productive_path_reuse(level);
            }
        }
        let rectangular = (reused.is_none() && ctx.point_leaves.is_none())
            .then(|| ctx.affine.admit_rectangular_candidate(rect_bounds(rect)));
        let affine = rectangular
            .as_ref()
            .map(|candidate| ctx.affine.classify_rect(candidate))
            .transpose()?;
        let mut probe_indices = [0_usize; 5];
        let known_membership = reused
            .as_ref()
            .and_then(|candidate| candidate.certificate.membership);
        let probe_count = if known_membership.is_some()
            || (reused.is_none() && affine.is_none_or(|class| class != AffineRectClass::Outside))
        {
            if ctx.point_leaves.is_some() && reused.is_none() {
                1
            } else {
                5
            }
        } else {
            0
        };
        let probe_order = if probe_count == 1 && ctx.point_leaves.is_some() && reused.is_none() {
            [4, usize::MAX, usize::MAX, usize::MAX, usize::MAX]
        } else {
            [0, 1, 2, 3, 4]
        };
        for &index in &probe_order[..probe_count] {
            let known = known_membership.map(|membership| {
                if index < 4 {
                    membership.vertices[index]
                } else {
                    membership.center
                }
            });
            probe_indices[index] = local_probe_index(&mut probes, points[index], known, stats);
        }
        pending.push(PendingCandidate {
            id,
            cell,
            rectangular,
            affine,
            probe_indices,
            membership_known: probe_count != 0,
            reused,
        });
    }
    debug_assert!(
        reuse.is_none_or(|candidates| candidates.is_empty()),
        "certificate batch did not belong to the staged descendant fan"
    );

    for probe in &mut probes {
        if probe.covered.is_none() {
            probe.covered = Some(ctx.tester.covers_point(probe.point)?);
            stats.unique_local_point_probe();
        }
    }

    for pending in pending {
        if let Some(candidate) = pending.reused {
            out.push(candidate);
            continue;
        }
        let mut membership = PointMembership {
            vertices: [false; 4],
            center: false,
        };
        for index in 0..5 {
            let Some(covered) = probes
                .get(pending.probe_indices[index])
                .and_then(|probe| probe.covered)
            else {
                continue;
            };
            if index < 4 {
                membership.vertices[index] = covered;
            } else {
                membership.center = covered;
            }
        }
        let membership = pending.membership_known.then_some(membership);
        let class = classify_candidate(
            ctx,
            pending.id,
            pending.cell,
            pending.rectangular.as_ref(),
            pending.affine,
            membership,
            stats,
        )?;
        out.push(Candidate::new(CandidateCertificate {
            id: pending.id,
            class,
            membership,
        }));
    }
    Ok(())
}

fn debug_assert_budget(emitted: usize, frontier: usize, max_cells: Option<usize>) {
    if let Some(limit) = max_cells {
        debug_assert!(
            emitted.saturating_add(frontier) <= limit,
            "cover budget invariant broken: E={emitted} + Q={frontier} > M={limit}"
        );
    }
}

fn is_visible(rule: CoverRule, candidate: CandidateCertificate) -> bool {
    match rule {
        CoverRule::Overlap => true,
        CoverRule::Within => candidate.class == CellClass::Interior,
        CoverRule::Center => {
            candidate.class == CellClass::Interior
                || candidate
                    .membership
                    .is_some_and(|membership| membership.center)
        },
    }
}

/// Classify one candidate after the fan has paid its local membership probes.
fn classify_candidate(
    ctx: &CoverContext<'_>,
    id: CellId,
    cell: Cell,
    rectangular: Option<&RectangularCandidate>,
    affine: Option<AffineRectClass>,
    membership: Option<PointMembership>,
    stats: &mut CoverStats,
) -> crate::error::Result<CellClass> {
    stats.raw_classify(id.level());
    if let Some(leaves) = &ctx.point_leaves {
        let from = leaves.partition_point(|&leaf| leaf < id.range_min());
        return Ok(
            if leaves.get(from).is_some_and(|&leaf| leaf <= id.range_max()) {
                CellClass::Boundary
            } else {
                CellClass::Outside
            },
        );
    }
    // The unsplit periodic authority owns the only candidate negative. A
    // charted source window may refine a retained candidate below, but an
    // internal seam cut can never prune a subtree.
    let affine = affine.expect("non-point candidate carries its affine class");
    if affine == AffineRectClass::Outside {
        return Ok(CellClass::Outside);
    }
    let rectangular = rectangular.expect("non-point candidate carries its rectangle");
    let membership = membership.expect("non-outside candidate carries point facts");
    let proposed = {
        let covered = membership
            .vertices
            .iter()
            .filter(|&&covered| covered)
            .count();
        let center_hit = covered < 4 && membership.center;
        // Seam-crossing: planar Outside is unsound → fail-open Boundary.
        // Polar full-lng (pole closure): vertices away from the pole cannot
        // establish an Outside result for a cell whose closed bound deliberately
        // contains every longitude at the pole. Keep the rect certificate; any
        // remaining ambiguity fails open to Boundary.
        // Still certify Interior when every positive-width bound window is
        // interior (full-cap / seam-crossing within).
        let wrapped = matches!(
            rectangular.windows(),
            DegreeWindowResult::Windows(CertifiedDegreeWindows {
                longitude: CertifiedLongitudeDegrees::Full | CertifiedLongitudeDegrees::Two(_),
                ..
            })
        );
        if wrapped {
            if covered == 4 && wrapped_rect_is_interior(ctx, rectangular)? {
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
            let rect_class = ctx.rects.classify_bounds(rectangular.bounds())?;
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
    };

    // An exact affine certificate only corroborates a planar proposal on
    // hazards.  Boundary stays Boundary: no terminal spherical refiner is
    // smuggled onto S2's ordinary candidate path.
    if !ctx.corroborate {
        return Ok(proposed);
    }
    if proposed == CellClass::Boundary {
        return Ok(CellClass::Boundary);
    }
    Ok(merge_certified_rect(
        proposed,
        affine,
        ctx.affine.s2_raw_owner_descends_from(cell),
    ))
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
fn wrapped_rect_is_interior(
    ctx: &CoverContext<'_>,
    candidate: &RectangularCandidate,
) -> crate::error::Result<bool> {
    let DegreeWindowResult::Windows(CertifiedDegreeWindows {
        latitude,
        longitude,
    }) = candidate.windows()
    else {
        return Ok(false);
    };
    match longitude {
        CertifiedLongitudeDegrees::Full => {
            Ok(ctx.rects.classify_bounds(candidate.bounds())? == RectClass::Interior)
        },
        CertifiedLongitudeDegrees::Two([west, east]) => {
            if !window_interior(ctx, west, latitude)? {
                return Ok(false);
            }
            window_interior(ctx, east, latitude)
        },
        CertifiedLongitudeDegrees::One(_) => Ok(false),
    }
}

fn window_interior(
    ctx: &CoverContext<'_>,
    longitude: Bound,
    latitude: Bound,
) -> crate::error::Result<bool> {
    // Degenerate zero-width lon strip (seam edge only) cannot host area;
    // treat as vacuously interior so a single-side polar spelling still
    // certifies when its positive-width sibling does.
    if longitude.hi <= longitude.lo {
        return Ok(true);
    }
    Ok(ctx.rects.classify_bounds(Bounds::new_raw(
        longitude.lo,
        latitude.lo,
        longitude.hi,
        latitude.hi,
    ))? == RectClass::Interior)
}

const fn rect_bounds(rect: super::cell::LatLngRect) -> Bounds {
    Bounds::new_raw(rect.lng_lo, rect.lat_lo, rect.lng_hi, rect.lat_hi)
}

#[cfg(test)]
#[path = "coverer_tests.rs"]
mod tests;
