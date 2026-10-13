use super::*;

/// One classified S2 candidate.  The membership facts are carried with the
/// candidate so visibility never has to ask the source tester a second time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CandidateCertificate {
    pub(super) id: CellId,
    pub(super) class: CellClass,
    pub(super) membership: Option<PointMembership>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PointMembership {
    pub(super) vertices: [bool; 4],
    pub(super) center: bool,
}

/// Certificates for every child in one parent-local fan. The successful
/// branch carries its own nested batches; siblings retain only their already
/// classified candidate certificate until their own productivity is needed.
#[derive(Clone, Debug)]
pub(super) struct CertificateBatch {
    pub(super) candidates: Vec<Candidate>,
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
pub(super) enum Productivity {
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
pub(super) struct Candidate {
    pub(super) certificate: CandidateCertificate,
    pub(super) productivity: Option<Productivity>,
}

impl Candidate {
    pub(super) const fn new(certificate: CandidateCertificate) -> Self {
        Self {
            certificate,
            productivity: None,
        }
    }

    #[cfg(test)]
    pub(super) fn certificate_storage(&self) -> usize {
        self.productivity
            .as_ref()
            .map_or(0, Productivity::certificate_storage)
            .checked_add(1)
            .expect("certificate-storage estimate fits usize")
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

pub(super) fn admit_visible_candidate(
    certificate: CandidateCertificate,
    cells: &mut Vec<(CellId, bool)>,
    queue: &BinaryHeap<(Reverse<u8>, CellId, Candidate)>,
    hard_budget: Option<usize>,
    stats: &mut CoverStats,
) -> Result<(), CoverError> {
    let projected = cover_total(&[cells.len(), queue.len(), 1]);
    ensure_cover_budget(projected, hard_budget)?;
    cells.push((certificate.id, certificate.class == CellClass::Interior));
    stats.committed_emission();
    stats.productive_frontier(cells.len(), queue.len());
    Ok(())
}

/// Keep only children that will emit ≥1 terminal under this coverer.
pub(super) fn retain_productive(
    ctx: &CoverContext<'_>,
    staged: &mut Vec<Candidate>,
    coverer: Coverer,
    stats: &mut CoverStats,
) -> Result<(), CoverError> {
    let mut failure = None;
    staged.retain_mut(|candidate| {
        if failure.is_some() {
            return true;
        }
        if candidate.certificate.class == CellClass::Outside {
            return false;
        }
        if candidate.productivity.is_some() {
            stats.productive_path_hit();
            return true;
        }
        match productive_state(ctx, candidate.certificate, coverer, stats) {
            Err(error) => {
                failure = Some(error);
                true
            },
            Ok(None) => false,
            Ok(Some(productivity)) => {
                candidate.productivity = Some(productivity);
                stats.observe_candidate(candidate);
                true
            },
        }
    });
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}

/// Find one already-classified branch that yields a final emission, retaining
/// the finite sibling batch needed by the later adaptive descent.
///
/// The returned state is deliberately a finite certificate batch, not a
/// global cell-result memo. Nested successful children carry their own batch.
pub(super) fn productive_state(
    ctx: &CoverContext<'_>,
    root: CandidateCertificate,
    coverer: Coverer,
    stats: &mut CoverStats,
) -> Result<Option<Productivity>, CoverError> {
    match root.class {
        CellClass::Outside => Ok(None),
        CellClass::Interior => Ok(Some(Productivity::Terminal)),
        CellClass::Boundary => {
            let Some(target) = coverer.refinement_target(root.id.level()) else {
                return Ok(Some(Productivity::Terminal));
            };
            let mut children = Vec::with_capacity(fan_size(root.id.level(), target));
            stage_children_at(ctx, root.id, target, None, &mut children, stats)?;
            let mut productive = false;
            for child in &mut children {
                let child_certificate = child.certificate;
                if child_certificate.class == CellClass::Outside {
                    continue;
                }
                if child_certificate.class == CellClass::Interior
                    || !coverer.can_refine(child_certificate.id.level(), child_certificate.class)
                {
                    child.productivity = Some(Productivity::Terminal);
                    productive = true;
                    break;
                }
                let Some(productivity) = productive_state(ctx, child_certificate, coverer, stats)?
                else {
                    continue;
                };
                child.productivity = Some(productivity);
                productive = true;
                break;
            }
            if productive {
                stats.observe_batch(&children);
                return Ok(Some(Productivity::Children(CertificateBatch {
                    candidates: children,
                })));
            }
            Ok(None)
        },
    }
}

pub(super) fn debug_assert_budget(emitted: usize, frontier: usize, max_cells: Option<usize>) {
    if let Some(limit) = max_cells {
        debug_assert!(
            cover_total(&[emitted, frontier]) <= limit,
            "cover budget invariant broken: E={emitted} + Q={frontier} > M={limit}"
        );
    }
}

pub(super) fn is_visible(rule: CoverRule, candidate: CandidateCertificate) -> bool {
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
