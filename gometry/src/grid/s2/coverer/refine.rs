use super::{
    BinaryHeap, Candidate, CellClass, CellId, CoverBudgetExceeded, CoverContext, CoverError,
    CoverRule, CoverStats, Coverer, Face, MAX_LEVEL, Reverse, admit_visible_candidate,
    classify_one, cover_total, debug_assert_budget, ensure_cover_budget, is_visible,
    productive_state, retain_productive, stage_children_at,
};

impl Coverer {
    /// Classify the six face roots into the phase-1 DFS stack or phase-2 queue.
    pub(super) fn seed_faces(
        self,
        ctx: &CoverContext<'_>,
        rule: CoverRule,
        cells: &mut Vec<(CellId, bool)>,
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
                CellClass::Interior | CellClass::Boundary => {
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
                        if self.min_level == self.max_level {
                            if visible {
                                admit_visible_candidate(
                                    candidate.certificate,
                                    cells,
                                    queue,
                                    self.hard_budget(),
                                    stats,
                                )?;
                            }
                        } else if visible {
                            let projected =
                                cover_total(&[cells.len(), queue.len(), usize::from(visible)]);
                            ensure_cover_budget(projected, self.hard_budget())?;
                            queue.push((
                                Reverse(candidate.certificate.id.level()),
                                candidate.certificate.id,
                                candidate,
                            ));
                            stats.visible_queue_peak(queue.len());
                            stats.productive_frontier(cells.len(), queue.len());
                            stats.observe_frontier(dfs, queue, invisible, &[]);
                        } else {
                            let projected =
                                cover_total(&[cells.len(), queue.len(), usize::from(visible)]);
                            ensure_cover_budget(projected, self.hard_budget())?;
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
    pub(super) fn force_min_level(
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
            let proven = cover_total(&[cells.len(), queue.len()]);
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
                    let child_level = certificate
                        .id
                        .level()
                        .checked_add(1)
                        .expect("unresolved candidate is below the admitted min_level");
                    stage_children_at(ctx, certificate.id, child_level, None, staged, stats)?;
                    if child_level >= self.min_level {
                        // Drop fail-open children that resolve to Outside
                        // before charging the phase-2 queue.
                        retain_productive(ctx, staged, self, stats)?;
                        for child in staged.drain(..) {
                            let visible = is_visible(rule, child.certificate);
                            if self.min_level == self.max_level {
                                if visible {
                                    admit_visible_candidate(
                                        child.certificate,
                                        cells,
                                        queue,
                                        self.hard_budget(),
                                        stats,
                                    )?;
                                }
                            } else if visible {
                                let projected =
                                    cover_total(&[cells.len(), queue.len(), usize::from(visible)]);
                                ensure_cover_budget(projected, self.hard_budget())?;
                                queue.push((
                                    Reverse(child.certificate.id.level()),
                                    child.certificate.id,
                                    child,
                                ));
                                stats.visible_queue_peak(queue.len());
                                stats.productive_frontier(cells.len(), queue.len());
                                stats.observe_frontier(dfs, queue, invisible, &[]);
                            } else {
                                let projected =
                                    cover_total(&[cells.len(), queue.len(), usize::from(visible)]);
                                ensure_cover_budget(projected, self.hard_budget())?;
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
    pub(super) fn adaptive_emit(
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
    pub(super) fn drain_invisible(
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
        if certificate.class == CellClass::Boundary
            && let Some(target) = self.refinement_target(certificate.id.level())
        {
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
            let base = cover_total(&[cells.len(), queue.len()]);
            let projected = cover_total(&[base, staged_visible]);
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
            admit_visible_candidate(certificate, cells, queue, self.hard_budget(), stats)?;
        }
        Ok(())
    }

    /// Whether a queue entry can still subdivide under this coverer.
    pub(super) fn can_refine(self, level: u8, class: CellClass) -> bool {
        class == CellClass::Boundary && self.refinement_target(level).is_some()
    }

    pub(super) fn refinement_target(self, level: u8) -> Option<u8> {
        let target = level.checked_add(self.level_mod)?;
        (target <= self.max_level && target <= MAX_LEVEL).then_some(target)
    }
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
    let delta = u32::from(min_level.checked_sub(level)?);
    // 4^delta = 2^(2*delta). delta ≤ 30 ⇒ shift ≤ 60, fits in u64/usize on
    // 64-bit; checked_shl covers smaller pointer widths.
    let shift = 2_u32.checked_mul(delta)?;
    1_usize.checked_shl(shift)
}

/// Number of descendants in one supported 1/2/3-level fan.
pub(super) fn fan_size(parent_level: u8, target: u8) -> usize {
    debug_assert!(target > parent_level);
    let delta = target
        .checked_sub(parent_level)
        .expect("fan target is finer than its parent");
    let shift = u32::from(delta)
        .checked_mul(2)
        .expect("S2 fan shift fits u32");
    1_usize
        .checked_shl(shift)
        .expect("S2 fan depth is limited to three levels")
}
