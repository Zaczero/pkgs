//! Test-only counters and guards for the bounded Hausdorff search.
//!
//! The search is judged by work done -- exact span evaluations, refinement
//! steps, sqrt fallbacks -- so the counters that record it live apart from
//! the algorithm they measure.

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
use crate::geometry::Result;

#[cfg(test)]
thread_local! {
    static LINE_FILTER_ENABLED: Cell<bool> = const { Cell::new(true) };
    static EXACT_SPAN_EVALUATIONS: Cell<usize> = const { Cell::new(0) };
    static REFINEMENT_WORK: Cell<usize> = const { Cell::new(0) };
    static REFINEMENT_WORK_LIMIT: Cell<Option<usize>> = const { Cell::new(None) };
}

#[cfg(test)]
pub(super) fn line_filter_enabled() -> bool {
    LINE_FILTER_ENABLED.with(Cell::get)
}

#[cfg(not(test))]
pub(super) const fn line_filter_enabled() -> bool {
    true
}

#[cfg(test)]
pub(super) struct LineFilterGuard(bool);

#[cfg(test)]
impl Drop for LineFilterGuard {
    fn drop(&mut self) {
        LINE_FILTER_ENABLED.with(|enabled| enabled.set(self.0));
    }
}

#[cfg(test)]
pub(super) fn set_line_filter_enabled(enabled: bool) -> LineFilterGuard {
    LineFilterGuard(LINE_FILTER_ENABLED.with(|current| {
        let previous = current.get();
        current.set(enabled);
        previous
    }))
}

#[cfg(test)]
pub(super) fn reset_exact_span_evaluations() {
    EXACT_SPAN_EVALUATIONS.with(|count| count.set(0));
}

#[cfg(test)]
pub(super) fn exact_span_evaluations() -> usize {
    EXACT_SPAN_EVALUATIONS.with(Cell::get)
}

#[cfg(test)]
pub(super) fn count_exact_span_evaluation() {
    EXACT_SPAN_EVALUATIONS.with(|count| count.set(count.get().saturating_add(1)));
}

#[cfg(test)]
pub(super) struct RefinementWorkGuard {
    pub(super) previous_count: usize,
    pub(super) previous_limit: Option<usize>,
}

#[cfg(test)]
impl Drop for RefinementWorkGuard {
    fn drop(&mut self) {
        REFINEMENT_WORK.with(|count| count.set(self.previous_count));
        REFINEMENT_WORK_LIMIT.with(|limit| limit.set(self.previous_limit));
    }
}

#[cfg(test)]
pub(super) fn limit_refinement_work(limit: usize) -> RefinementWorkGuard {
    let previous_count = REFINEMENT_WORK.with(|count| {
        let previous = count.get();
        count.set(0);
        previous
    });
    let previous_limit = REFINEMENT_WORK_LIMIT.with(|current| {
        let previous = current.get();
        current.set(Some(limit));
        previous
    });
    RefinementWorkGuard {
        previous_count,
        previous_limit,
    }
}

#[cfg(test)]
pub(super) fn refinement_work() -> usize {
    REFINEMENT_WORK.with(Cell::get)
}

#[cfg(test)]
pub(super) fn count_refinement_work() -> Result<()> {
    let count = REFINEMENT_WORK.with(|current| {
        let next = current.get().saturating_add(1);
        current.set(next);
        next
    });
    if let Some(limit) = REFINEMENT_WORK_LIMIT.with(Cell::get)
        && count > limit
    {
        return Err(
            crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge {
                operation: "hausdorff_distance",
                parameter: "refinement_work",
                produced: count,
                limit,
            }
            .into(),
        );
    }
    Ok(())
}
