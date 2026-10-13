use super::*;

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
    pub(super) raw_classify_calls: usize,
    #[cfg(test)]
    pub(super) candidate_preparations: usize,
    #[cfg(test)]
    pub(super) classifications_by_level: [usize; S2_LEVEL_COUNT],
    #[cfg(test)]
    pub(super) visited_unique_candidates: usize,
    #[cfg(test)]
    pub(super) unique_local_point_probes: usize,
    #[cfg(test)]
    pub(super) local_duplicate_hits: usize,
    #[cfg(test)]
    pub(super) carried_certificate_hits: usize,
    #[cfg(test)]
    pub(super) carried_certificates_by_level: [usize; S2_LEVEL_COUNT],
    #[cfg(test)]
    pub(super) productive_path_reuses: usize,
    #[cfg(test)]
    pub(super) productive_path_reuses_by_level: [usize; S2_LEVEL_COUNT],
    #[cfg(test)]
    pub(super) productive_path_hits: usize,
    #[cfg(test)]
    pub(super) visible_queue_peak: usize,
    #[cfg(test)]
    pub(super) productive_frontier_peak: usize,
    #[cfg(test)]
    pub(super) committed_emissions: usize,
    #[cfg(test)]
    pub(super) invisible_stack_peak: usize,
    #[cfg(test)]
    pub(super) certificate_storage_peak: usize,
    #[cfg(test)]
    pub(super) retained_map_entries: usize,
}

#[cfg_attr(
    not(test),
    expect(
        clippy::needless_pass_by_ref_mut,
        reason = "the counter mutates in test builds and is intentionally zero-sized in production"
    )
)]
impl CoverStats {
    #[cfg(test)]
    const fn increment(counter: &mut usize) {
        *counter = counter.checked_add(1).expect("S2 test counter fits usize");
    }

    #[cfg(test)]
    pub(super) fn raw_classify(&mut self, level: u8) {
        Self::increment(&mut self.raw_classify_calls);
        Self::increment(
            self.classifications_by_level
                .get_mut(usize::from(level))
                .expect("CellId level fits the classification counter"),
        );
        Self::increment(&mut self.visited_unique_candidates);
    }

    #[cfg(not(test))]
    pub(super) const fn raw_classify(&mut self, _level: u8) {}

    pub(super) const fn candidate_preparation(&mut self) {
        #[cfg(test)]
        {
            Self::increment(&mut self.candidate_preparations);
        }
    }

    pub(super) const fn unique_local_point_probe(&mut self) {
        #[cfg(test)]
        {
            Self::increment(&mut self.unique_local_point_probes);
        }
    }

    pub(super) const fn local_duplicate_hit(&mut self) {
        #[cfg(test)]
        {
            Self::increment(&mut self.local_duplicate_hits);
        }
    }

    #[cfg(test)]
    pub(super) fn carried_certificate_hit(&mut self, level: u8) {
        Self::increment(&mut self.carried_certificate_hits);
        Self::increment(
            self.carried_certificates_by_level
                .get_mut(usize::from(level))
                .expect("CellId level fits the carried-certificate counter"),
        );
    }

    #[cfg(not(test))]
    pub(super) const fn carried_certificate_hit(&mut self, _level: u8) {}

    #[cfg(test)]
    pub(super) fn productive_path_reuse(&mut self, level: u8) {
        Self::increment(&mut self.productive_path_reuses);
        Self::increment(
            self.productive_path_reuses_by_level
                .get_mut(usize::from(level))
                .expect("CellId level fits the productivity counter"),
        );
    }

    #[cfg(not(test))]
    pub(super) const fn productive_path_reuse(&mut self, _level: u8) {}

    pub(super) const fn productive_path_hit(&mut self) {
        #[cfg(test)]
        {
            Self::increment(&mut self.productive_path_hits);
        }
    }

    pub(super) const fn visible_queue_peak(&mut self, len: usize) {
        #[cfg(not(test))]
        let _ = len;
        #[cfg(test)]
        {
            if len > self.visible_queue_peak {
                self.visible_queue_peak = len;
            }
        }
    }

    pub(super) const fn productive_frontier(&mut self, emitted: usize, queued: usize) {
        #[cfg(not(test))]
        let _ = (emitted, queued);
        #[cfg(test)]
        {
            let len = emitted
                .checked_add(queued)
                .expect("S2 test frontier cardinality fits usize");
            if len > self.productive_frontier_peak {
                self.productive_frontier_peak = len;
            }
        }
    }

    pub(super) const fn committed_emission(&mut self) {
        #[cfg(test)]
        {
            Self::increment(&mut self.committed_emissions);
        }
    }

    pub(super) const fn invisible_stack_peak(&mut self, len: usize) {
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
    pub(super) fn observe_candidate(&mut self, candidate: &Candidate) {
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
    pub(super) fn observe_batch(&mut self, candidates: &[Candidate]) {
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
    pub(super) fn observe_frontier(
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
