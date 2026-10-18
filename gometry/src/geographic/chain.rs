use super::PeriodicAxis;

/// A shortcut must retain the source interval's directed seam transits.
pub(crate) struct ChartSpanGuard<'a> {
    axis: PeriodicAxis,
    xs: &'a [f64],
    turns: Option<Vec<i64>>,
}

impl<'a> ChartSpanGuard<'a> {
    pub(crate) fn new(xs: &'a [f64], axis: PeriodicAxis) -> Self {
        let mut turns = None;
        let mut current = 0_i64;
        let mut previous = None;
        for (index, &longitude) in xs.iter().enumerate() {
            if let Some(previous) = previous {
                current = axis
                    .add_chart_transit(current, previous, longitude)
                    .expect("admitted coordinate chains have finite chart transits");
            }
            if current != 0 && turns.is_none() {
                let mut prefix = Vec::with_capacity(xs.len());
                prefix.resize(index, 0_i64);
                turns = Some(prefix);
            }
            if let Some(prefix) = turns.as_mut() {
                prefix.push(current);
            }
            previous = Some(longitude);
        }
        Self { axis, xs, turns }
    }

    pub(crate) fn rejects_span(&self, start: usize, end: usize) -> bool {
        let from = *self
            .xs
            .get(start)
            .expect("span start belongs to its source");
        let to = *self.xs.get(end).expect("span end belongs to its source");
        if self.axis.edge_exceeds_half_period(from, to) {
            return true;
        }
        let Some(turns) = &self.turns else {
            return false;
        };
        let source_turns = turns
            .get(end)
            .expect("span end belongs to its turn prefix")
            .checked_sub(
                *turns
                    .get(start)
                    .expect("span start belongs to its turn prefix"),
            )
            .expect("an admitted source span's turn count fits i64");
        let chord_turns = self
            .axis
            .chart_transit(from, to)
            .expect("source endpoints have an admitted chart step");
        source_turns != i64::from(chord_turns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::GeographicFrame;

    #[rstest::rstest]
    #[case::xs_180_0_60_0_60_0_180_0([-180.0, -60.0, 60.0, -180.0])]
    #[case::xs_180_0_60_0_60_0_180_0_32c40121([-180.0, 60.0, -60.0, -180.0])]
    fn shortcuts_cannot_erase_a_complete_parallel_turn(#[case] xs: [f64; 4]) {
        let axis = GeographicFrame::crs84().longitude();

        let guard = ChartSpanGuard::new(&xs, axis);
        assert!(guard.rejects_span(0, 3));
        assert!(!guard.rejects_span(1, 2));
    }

    #[test]
    fn opposite_seam_transits_can_cancel_in_a_shortcut() {
        let axis = GeographicFrame::crs84().longitude();
        let xs = [170.0, -170.0, 170.0];
        let guard = ChartSpanGuard::new(&xs, axis);
        assert!(!guard.rejects_span(0, 2));
    }
}
