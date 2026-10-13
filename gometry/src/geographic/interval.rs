use std::cmp::Ordering;

use super::{CanonicalLongitude, PeriodicAxis};
#[cfg(test)]
use super::{ChartConnection, ChartStep, EdgeDirection, TurnDirection};
use crate::geometry::numerics::product_sum::ExactSum;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum CircularInterval {
    Empty,
    Arc {
        start: CanonicalLongitude,
        end: CanonicalLongitude,
    },
    Full,
}

impl CircularInterval {
    pub(crate) const fn point(longitude: CanonicalLongitude) -> Self {
        Self::Arc {
            start: longitude,
            end: longitude,
        }
    }

    pub(crate) const fn from_endpoints(start: CanonicalLongitude, end: CanonicalLongitude) -> Self {
        Self::Arc { start, end }
    }

    #[cfg(test)]
    pub(crate) const fn from_chart_connection(connection: ChartConnection) -> Self {
        let reverse = match connection.step {
            ChartStep::Circle(_) => return Self::Full,
            ChartStep::Shortest(EdgeDirection::Minor(delta)) => delta.is_sign_negative(),
            ChartStep::Shortest(EdgeDirection::HalfTurn(TurnDirection::Negative)) => true,
            ChartStep::Shortest(EdgeDirection::HalfTurn(TurnDirection::Positive)) => false,
        };
        if reverse {
            Self::from_endpoints(connection.end, connection.start)
        } else {
            Self::from_endpoints(connection.start, connection.end)
        }
    }

    pub(crate) const fn west_east(self) -> Option<(f64, f64)> {
        match self {
            Self::Arc { start, end } => Some((start.get(), end.get())),
            Self::Empty | Self::Full => None,
        }
    }

    pub(crate) fn windows(self, axis: PeriodicAxis) -> Option<LongitudeWindows> {
        match self {
            Self::Empty => None,
            Self::Full => Some(LongitudeWindows::Full),
            Self::Arc { start, end } => {
                let (first, second) = native_windows_for_arc(axis, start, end);
                Some(second.map_or(LongitudeWindows::One(first), |second| {
                    LongitudeWindows::Two(first, second)
                }))
            },
        }
    }

    /// Cover sorted finite longitude samples that have already been canonicalized.
    pub(crate) fn cover_sorted_canonical_points(axis: PeriodicAxis, points: &[f64]) -> Self {
        smallest_covering_arc(axis, points.iter().copied().map(|value| [value, value]))
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum LongitudeWindows {
    One([f64; 2]),
    Two([f64; 2], [f64; 2]),
    Full,
}

pub(crate) fn union_circular_intervals(
    axis: PeriodicAxis,
    intervals: &[CircularInterval],
) -> CircularInterval {
    let mut first = None;
    let mut arc_count = 0_usize;
    for interval in intervals {
        match *interval {
            CircularInterval::Empty => {},
            CircularInterval::Full => return CircularInterval::Full,
            CircularInterval::Arc { start, end } => {
                first.get_or_insert((start, end));
                arc_count = arc_count.wrapping_add(1);
            },
        }
    }
    let Some(first) = first else {
        return CircularInterval::Empty;
    };
    if arc_count == 1 {
        return CircularInterval::from_endpoints(first.0, first.1);
    }

    let mut windows = Vec::with_capacity(arc_count.saturating_mul(2));
    for interval in intervals {
        if let CircularInterval::Arc { start, end } = *interval {
            push_arc_windows(axis, start, end, &mut windows);
        }
    }
    windows.sort_unstable_by(|[left_start, left_end], [right_start, right_end]| {
        left_start
            .total_cmp(right_start)
            .then_with(|| left_end.total_cmp(right_end))
    });
    windows.dedup_by(|window, previous| {
        let [window_start, window_end] = *window;
        let [previous_start, previous_end] = *previous;
        if window_start <= previous_end {
            *previous = [previous_start, previous_end.max(window_end)];
            true
        } else {
            false
        }
    });
    smallest_covering_arc(axis, windows.into_iter())
}

fn native_windows_for_arc(
    axis: PeriodicAxis,
    start: CanonicalLongitude,
    end: CanonicalLongitude,
) -> ([f64; 2], Option<[f64; 2]>) {
    let (start, end) = (start.get(), end.get());
    if start <= end {
        ([start, end], None)
    } else {
        (
            [start, axis.half_period()],
            Some([-axis.half_period(), end]),
        )
    }
}

fn push_arc_windows(
    axis: PeriodicAxis,
    start: CanonicalLongitude,
    end: CanonicalLongitude,
    windows: &mut Vec<[f64; 2]>,
) {
    let (first, second) = native_windows_for_arc(axis, start, end);
    windows.push(first);
    if let Some(second) = second {
        windows.push(second);
    }
}

#[derive(Clone, Copy)]
struct UncoveredGap {
    lower: f64,
    upper: f64,
    wraps: bool,
}

impl UncoveredGap {
    fn compare_width(self, other: Self, axis: PeriodicAxis) -> Ordering {
        let mut difference = ExactSum::zero();
        self.accumulate_width(&mut difference, axis, false);
        other.accumulate_width(&mut difference, axis, true);
        difference.ordering()
    }

    fn is_empty(self, axis: PeriodicAxis) -> bool {
        let mut width = ExactSum::zero();
        self.accumulate_width(&mut width, axis, false);
        width.ordering().is_eq()
    }

    fn accumulate_width(self, sum: &mut ExactSum, axis: PeriodicAxis, subtract: bool) {
        if subtract {
            sum.subtract_product(self.upper, 1.0);
            sum.add_product(self.lower, 1.0);
            if self.wraps {
                sum.subtract_product(axis.period(), 1.0);
            }
        } else {
            sum.add_product(self.upper, 1.0);
            sum.subtract_product(self.lower, 1.0);
            if self.wraps {
                sum.add_product(axis.period(), 1.0);
            }
        }
    }
}

fn consider_gap(axis: PeriodicAxis, candidate: UncoveredGap, best: &mut Option<UncoveredGap>) {
    let replace = best.is_none_or(|current| match candidate.compare_width(current, axis) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => candidate.upper.total_cmp(&current.upper).is_lt(),
    });
    if replace {
        *best = Some(candidate);
    }
}

fn smallest_covering_arc(
    axis: PeriodicAxis,
    mut windows: impl Iterator<Item = [f64; 2]>,
) -> CircularInterval {
    let Some([first_start, mut previous_end]) = windows.next() else {
        return CircularInterval::Empty;
    };
    let mut best = None;
    for [next_start, next_end] in windows {
        consider_gap(
            axis,
            UncoveredGap {
                lower: previous_end,
                upper: next_start,
                wraps: false,
            },
            &mut best,
        );
        previous_end = next_end;
    }
    consider_gap(
        axis,
        UncoveredGap {
            lower: previous_end,
            upper: first_start,
            wraps: true,
        },
        &mut best,
    );
    let best = best.expect("a nonempty sorted window set has a seam gap");
    if best.is_empty(axis) {
        return CircularInterval::Full;
    }
    let end = if best.lower.to_bits() == axis.half_period().to_bits() {
        -axis.half_period()
    } else {
        best.lower
    };
    CircularInterval::from_endpoints(
        axis.canonicalize(best.upper)
            .expect("native window starts are finite"),
        axis.canonicalize(end)
            .expect("native window ends are finite"),
    )
}
