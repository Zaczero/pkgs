use std::cmp::Ordering;

use super::{CanonicalLongitude, GeographicError, PeriodicAxis};
use crate::error::Result;
use crate::{try_push_vec, try_reserve_vec};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum CircularInterval {
    Empty,
    Arc {
        start: CanonicalLongitude,
        sweep: f64,
    },
    Full,
}

impl CircularInterval {
    pub(crate) fn try_arc(
        axis: PeriodicAxis,
        start: f64,
        sweep: f64,
    ) -> Result<Self, GeographicError> {
        if !sweep.is_finite() || sweep < 0.0 || sweep >= axis.period() {
            return Err(GeographicError::InvalidSweep {
                sweep,
                period: axis.period(),
            });
        }
        Ok(Self::Arc {
            start: axis.canonicalize(start)?,
            sweep: if sweep == 0.0 { 0.0 } else { sweep },
        })
    }

    pub(crate) fn west_east(self, axis: PeriodicAxis) -> Option<(f64, f64)> {
        match self {
            Self::Arc { start, sweep } => {
                Some((start.get(), arc_endpoint(axis, start, sweep).get()))
            },
            Self::Empty | Self::Full => None,
        }
    }

    pub(crate) fn windows(self, axis: PeriodicAxis) -> Option<LongitudeWindows> {
        let Self::Arc { start, sweep } = self else {
            return match self {
                Self::Full => Some(LongitudeWindows::Full),
                Self::Empty => None,
                Self::Arc { .. } => unreachable!(),
            };
        };
        let west = start.get();
        if sweep == 0.0 {
            return Some(LongitudeWindows::One([west, west]));
        }
        let distance_to_seam = axis.half_period() - west;
        if sweep <= distance_to_seam {
            Some(LongitudeWindows::One([
                west,
                axis.half_period() - (distance_to_seam - sweep),
            ]))
        } else {
            Some(LongitudeWindows::Two([west, axis.half_period()], [
                -axis.half_period(),
                -axis.half_period() + (sweep - distance_to_seam),
            ]))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum LongitudeWindows {
    One([f64; 2]),
    Two([f64; 2], [f64; 2]),
    Full,
}

pub(crate) fn union_circular_intervals<I>(
    axis: PeriodicAxis,
    intervals: I,
) -> Result<CircularInterval>
where
    I: IntoIterator<Item = CircularInterval>,
{
    let mut iter = intervals
        .into_iter()
        .filter(|value| !matches!(value, CircularInterval::Empty));
    let Some(first) = iter.next() else {
        return Ok(CircularInterval::Empty);
    };
    if matches!(first, CircularInterval::Full) {
        return Ok(CircularInterval::Full);
    }
    let Some(second) = iter.next() else {
        return Ok(first);
    };
    if matches!(second, CircularInterval::Full) {
        return Ok(CircularInterval::Full);
    }
    let mut segments = Vec::new();
    try_reserve_vec(&mut segments, 4)?;
    add_segments(axis, first, &mut segments)?;
    add_segments(axis, second, &mut segments)?;
    for interval in iter {
        if matches!(interval, CircularInterval::Full) {
            return Ok(CircularInterval::Full);
        }
        add_segments(axis, interval, &mut segments)?;
    }
    segments.sort_unstable_by(|a, b| a[0].total_cmp(&b[0]).then_with(|| a[1].total_cmp(&b[1])));
    let mut merged: Vec<[f64; 2]> = Vec::new();
    try_reserve_vec(&mut merged, segments.len())?;
    for segment in segments {
        if let Some(last) = merged.last_mut().filter(|last| segment[0] <= last[1]) {
            last[1] = last[1].max(segment[1]);
        } else {
            try_push_vec(&mut merged, segment)?;
        }
    }
    let mut best_gap = 0.0;
    let mut best_start = 0.0;
    for index in 0..merged.len() {
        let end = merged[index][1];
        let (gap, next) = if index + 1 == merged.len() {
            ((axis.period() - end) + merged[0][0], merged[0][0])
        } else {
            let next = merged[index + 1][0];
            (next - end, next)
        };
        let candidate_start = from_zero_based(axis, next).get();
        if gap > best_gap
            || (gap.total_cmp(&best_gap) == Ordering::Equal && candidate_start < best_start)
        {
            best_gap = gap;
            best_start = candidate_start;
        }
    }
    if best_gap == 0.0 {
        Ok(CircularInterval::Full)
    } else {
        Ok(CircularInterval::Arc {
            start: axis
                .canonicalize(best_start)
                .expect("best start is canonical"),
            sweep: axis.period() - best_gap,
        })
    }
}

fn add_segments(
    axis: PeriodicAxis,
    interval: CircularInterval,
    out: &mut Vec<[f64; 2]>,
) -> Result<()> {
    let CircularInterval::Arc { start, sweep } = interval else {
        return Ok(());
    };
    let zero_start = to_zero_based(axis, start);
    let remaining = axis.period() - zero_start;
    if sweep <= remaining {
        try_push_vec(out, [zero_start, axis.period() - (remaining - sweep)])?;
    } else {
        try_push_vec(out, [zero_start, axis.period()])?;
        try_push_vec(out, [0.0, sweep - remaining])?;
    }
    Ok(())
}

fn arc_endpoint(axis: PeriodicAxis, start: CanonicalLongitude, sweep: f64) -> CanonicalLongitude {
    let distance_to_seam = axis.half_period() - start.get();
    let endpoint = if sweep < distance_to_seam {
        axis.half_period() - (distance_to_seam - sweep)
    } else {
        -axis.half_period() + (sweep - distance_to_seam)
    };
    axis.canonicalize(endpoint)
        .expect("endpoint arithmetic stays finite")
}

fn to_zero_based(axis: PeriodicAxis, value: CanonicalLongitude) -> f64 {
    if value.get().is_sign_negative() {
        axis.half_period() - (-value.get())
    } else {
        axis.half_period() + value.get()
    }
}

fn from_zero_based(axis: PeriodicAxis, value: f64) -> CanonicalLongitude {
    let canonical = if value < axis.half_period() {
        -(axis.half_period() - value)
    } else {
        value - axis.half_period()
    };
    axis.canonicalize(canonical)
        .expect("zero-based coordinate is finite")
}
