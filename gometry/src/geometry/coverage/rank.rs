use std::cmp::Ordering;

use crate::geometry::numerics::product::{dd_add, floor_binary_exponent, two_sum};
use crate::geometry::{
    XY, axis_pow2_scale, point_distance, power_of_two_exponent, scale_by_power_of_two,
    scaled_residual,
};

/// A nonnegative binary64 significand times an extended binary exponent.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ScaledLength {
    pub(in crate::geometry) significand: f64,
    pub(in crate::geometry) exponent: i32,
}

impl ScaledLength {
    pub(crate) fn from_finite(value: f64) -> Self {
        debug_assert!(value.is_finite() && value >= 0.0);
        Self::from_scaled(value, 0)
    }

    pub(crate) fn from_scaled(value: f64, exponent: i32) -> Self {
        debug_assert!(value.is_finite() && value >= 0.0);
        if value.total_cmp(&0.0) == Ordering::Equal {
            return Self::default();
        }
        let adjustment = floor_binary_exponent(value);
        Self {
            significand: scale_by_power_of_two(value, adjustment.wrapping_neg()),
            exponent: exponent.wrapping_add(adjustment),
        }
    }

    pub(crate) fn is_positive(self) -> bool {
        self.significand > 0.0
    }

    pub(crate) fn compare(self, other: Self) -> Ordering {
        match (
            self.significand.total_cmp(&0.0) == Ordering::Equal,
            other.significand.total_cmp(&0.0) == Ordering::Equal,
        ) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => self
                .exponent
                .cmp(&other.exponent)
                .then_with(|| self.significand.total_cmp(&other.significand)),
        }
    }
}

/// A guarded binary64 reduction of positive lengths without an f64 range cap.
///
/// The rank orders binary64 length terms, not an exact sum of generally
/// non-rational lengths.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ScaledLengthSum {
    high: f64,
    low: f64,
    exponent: i32,
    nonzero: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum BorderTieBreak {
    LowestRow,
    HighestRow,
}

impl ScaledLengthSum {
    pub(crate) fn add(&mut self, length: ScaledLength) {
        if !length.is_positive() {
            return;
        }
        if !self.nonzero {
            self.high = length.significand;
            self.exponent = length.exponent;
            self.nonzero = true;
            return;
        }

        let term = if length.exponent > self.exponent {
            let shift = self.exponent.wrapping_sub(length.exponent);
            self.high = scale_by_power_of_two(self.high, shift);
            self.low = scale_by_power_of_two(self.low, shift);
            self.exponent = length.exponent;
            length.significand
        } else {
            scale_by_power_of_two(
                length.significand,
                length.exponent.wrapping_sub(self.exponent),
            )
        };
        (self.high, self.low) = dd_add((self.high, self.low), (term, 0.0));
        self.normalize();
    }

    fn normalize(&mut self) {
        let adjustment = floor_binary_exponent(self.high);
        self.high = scale_by_power_of_two(self.high, adjustment.wrapping_neg());
        self.low = scale_by_power_of_two(self.low, adjustment.wrapping_neg());
        self.exponent = self.exponent.wrapping_add(adjustment);
    }

    pub(crate) fn rank(self) -> ScaledLength {
        if !self.nonzero {
            return ScaledLength::default();
        }
        ScaledLength::from_scaled(two_sum(self.high, self.low).0, self.exponent)
    }
}

pub(crate) fn longest_border_owner(
    rows: impl IntoIterator<Item = usize>,
    overflowed_ranks: &[ScaledLength],
    tie_break: BorderTieBreak,
    include_zero: bool,
) -> Option<usize> {
    longest_border_ranked_owner(
        rows.into_iter().map(|row| {
            (
                row,
                *overflowed_ranks
                    .get(row)
                    .expect("row ordinal comes from the parallel border-rank column"),
            )
        }),
        tie_break,
        include_zero,
    )
}

pub(crate) fn longest_border_ranked_owner(
    rows: impl IntoIterator<Item = (usize, ScaledLength)>,
    tie_break: BorderTieBreak,
    include_zero: bool,
) -> Option<usize> {
    let mut best: Option<(usize, ScaledLength)> = None;
    for (row, rank) in rows {
        if !include_zero && !rank.is_positive() {
            continue;
        }
        let replace = if let Some((best_row, best_rank)) = best {
            let order = rank.compare(best_rank);
            order == Ordering::Greater
                || (order == Ordering::Equal
                    && match tie_break {
                        BorderTieBreak::LowestRow => row < best_row,
                        BorderTieBreak::HighestRow => row > best_row,
                    })
        } else {
            true
        };
        if replace {
            best = Some((row, rank));
        }
    }
    best.map(|(row, _)| row)
}

pub(crate) fn scaled_point_distance(start: XY, end: XY) -> ScaledLength {
    let distance = point_distance(start, end);
    if distance.is_finite() {
        return ScaledLength::from_finite(distance);
    }

    let (dx, dx_base_exponent) = scaled_difference(start.x, end.x);
    let (dy, dy_base_exponent) = scaled_difference(start.y, end.y);
    let dx_world_exponent = if dx.abs().total_cmp(&0.0) == Ordering::Equal {
        None
    } else {
        Some(dx_base_exponent.wrapping_add(floor_binary_exponent(dx.abs())))
    };
    let dy_world_exponent = if dy.abs().total_cmp(&0.0) == Ordering::Equal {
        None
    } else {
        Some(dy_base_exponent.wrapping_add(floor_binary_exponent(dy.abs())))
    };
    let common_exponent = match (dx_world_exponent, dy_world_exponent) {
        (Some(dx), Some(dy)) => dx.max(dy),
        (Some(dx), None) => dx,
        (None, Some(dy)) => dy,
        (None, None) => return ScaledLength::default(),
    };
    let dx = dx_world_exponent.map_or(0.0, |_| {
        scale_by_power_of_two(dx, dx_base_exponent.wrapping_sub(common_exponent))
    });
    let dy = dy_world_exponent.map_or(0.0, |_| {
        scale_by_power_of_two(dy, dy_base_exponent.wrapping_sub(common_exponent))
    });
    ScaledLength::from_scaled(dx.hypot(dy), common_exponent)
}

fn scaled_difference(start: f64, end: f64) -> (f64, i32) {
    let delta = end - start;
    if delta.is_finite() {
        return (delta, 0);
    }
    let scale = axis_pow2_scale(start.abs().max(end.abs()));
    (
        scaled_residual(end, start, scale),
        power_of_two_exponent(scale).wrapping_neg(),
    )
}
