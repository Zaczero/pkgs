//! Coordinate and ordinate interpolation along segment parameters.

use super::{ExactDyadic, dd_interpolate};
use crate::error::Result;
use crate::geometry::numerics::product::two_sum;
use crate::geometry::{
    MOrdinate, Point, XY, ZOrdinate, axis_pow2_scale, round_ratio, scaled_residual,
};

/// Affinely interpolate between two finite points. A convex `fraction` in
/// `[0, 1]` always yields a finite point; far extrapolation (e.g. a
/// near-parallel `line_intersection`) can overflow, in which case we fall back
/// to the nearer endpoint rather than panic across the FFI boundary.
pub(in crate::geometry) fn interpolate_segment_point(
    start: impl Into<XY>,
    end: impl Into<XY>,
    fraction: f64,
) -> XY {
    let (start, end) = (start.into(), end.into());
    let x = interpolate_f64(start.x, end.x, fraction);
    let y = interpolate_f64(start.y, end.y, fraction);
    if x.is_finite() && y.is_finite() {
        XY::new(x, y)
    } else if fraction <= 0.5 {
        start
    } else {
        end
    }
}

/// Ordinate-carrying interpolation for the LRS lanes: every present
/// ordinate (X/Y/Z/M) interpolates; the engine's planar form is
/// [`interpolate_segment_point`].
pub(crate) fn lerp_point(start: Point, end: Point, fraction: f64) -> Point {
    Point::new_axes(
        interpolate_f64(start.x, end.x, fraction),
        interpolate_f64(start.y, end.y, fraction),
        ZOrdinate(interpolate_optional(start.z(), end.z(), fraction)),
        MOrdinate(interpolate_optional(start.m(), end.m(), fraction)),
    )
    .unwrap_or(if fraction <= 0.5 { start } else { end })
}

/// Affine interpolation `start*(1 - fraction) + end*fraction`, valid for any
/// `fraction` (callers such as `line_intersection` extrapolate past `[0, 1]`).
/// Return exact endpoint and constant-ordinate identities before evaluating the
/// convex form, whose separate products can underflow an equal subnormal value.
/// Otherwise the convex form keeps the result bounded by
/// `max(|start|, |end|)` for `fraction` in `[0, 1]`, avoiding the
/// `end - start` overflow class.
#[expect(
    clippy::float_cmp,
    reason = "zero and one are exact affine parameter identities; equal finite ordinates are preserved"
)]
pub(crate) fn interpolate_f64(start: f64, end: f64, fraction: f64) -> f64 {
    if fraction == 0.0 {
        return start;
    }
    if fraction == 1.0 {
        return end;
    }
    if fraction.is_finite() && start.is_finite() && start == end {
        return start;
    }
    let value = start * (1.0 - fraction) + end * fraction;
    if value.is_finite() || !start.is_finite() || !end.is_finite() {
        return value;
    }
    // The same convex reconstruction in a power-of-two frame: `±1e308`
    // endpoints can have a perfectly finite interior point even though the
    // direct two products overflow with opposite signs.
    let scale = axis_pow2_scale(start.abs().max(end.abs()));
    (start * scale * (1.0 - fraction) + end * scale * fraction) / scale
}

/// [`interpolate_f64`] for an optional Z or M ordinate, present only when both
/// endpoints carry it.
pub(crate) fn interpolate_optional_ordinate(
    start: Option<f64>,
    end: Option<f64>,
    fraction: f64,
) -> Option<f64> {
    Some(interpolate_f64(start?, end?, fraction))
}

/// Interpolation for emitted subdivision ordinates. The error-free two-sum
/// difference identifies the common case where one FMA avoids an extra
/// product/add rounding; an existing double-double path handles an inexact
/// difference. Both retain the convex finite-range fallback for extremes.
#[expect(
    clippy::float_cmp,
    reason = "zero and one endpoints and an exact two-sum residual are deliberate"
)]
pub(crate) fn interpolate_subdivision_ordinate(start: f64, end: f64, fraction: f64) -> f64 {
    if fraction == 0.0 {
        return start;
    }
    if fraction == 1.0 {
        return end;
    }
    let (difference, residual) = two_sum(end, -start);
    if difference.is_finite() && residual == 0.0 {
        let value = fraction.mul_add(difference, start);
        if value.is_finite() {
            return value;
        }
    }
    dd_interpolate(start, end, (fraction, 0.0))
}

/// Fraction for a finite interpolation interval. Keep the established
/// arithmetic on the ordinary path; exact stored-binary64 ratio arithmetic is
/// only needed when the source interval or target residual overflows.
pub(crate) fn interpolate_fraction(start: f64, end: f64, target: f64) -> Result<f64> {
    let numerator = target - start;
    let denominator = end - start;
    if denominator == 0.0 {
        return Ok(0.0);
    }
    if numerator.is_finite() && denominator.is_finite() {
        return Ok(numerator / denominator);
    }
    let numerator = ExactDyadic::from_f64(target).try_subtract(ExactDyadic::from_f64(start))?;
    let denominator = ExactDyadic::from_f64(end).try_subtract(ExactDyadic::from_f64(start))?;
    round_ratio(&numerator, &denominator)
}

/// Reconstruct the free axis at a known held-axis crossing without forming
/// `(at - start) / (end - start)` in world scale. `None` is deliberate: an
/// unresolved crossing must make a caller fall back, never manufacture a
/// non-finite vertex or an empty clipped result.
#[expect(
    clippy::float_cmp,
    reason = "a held axis exactly selects an already-stored endpoint before any interpolation"
)]
pub(crate) fn held_axis_interpolate(
    start_held: f64,
    start_free: f64,
    end_held: f64,
    end_free: f64,
    held: f64,
) -> Option<f64> {
    if held == start_held {
        return Some(start_free);
    }
    if held == end_held {
        return Some(end_free);
    }
    // Preserve the established ordinary finite arithmetic exactly.  The
    // normalized reconstruction below exists for an overflowed delta or a
    // non-finite raw interpolation, not to perturb unrelated stored-double
    // crossings by changing their rounding path.
    let numerator = held - start_held;
    let denominator = end_held - start_held;
    if numerator.is_finite() && denominator.is_finite() {
        let fraction = numerator / denominator;
        let value = start_free + fraction * (end_free - start_free);
        if fraction.is_finite() && (0.0..=1.0).contains(&fraction) && value.is_finite() {
            return Some(value);
        }
    }
    let scale = axis_pow2_scale(start_held.abs().max(end_held.abs()).max(held.abs()));
    let numerator = scaled_residual(held, start_held, scale);
    let denominator = scaled_residual(end_held, start_held, scale);
    let fraction = numerator / denominator;
    if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
        return None;
    }
    let value = interpolate_f64(start_free, end_free, fraction);
    value.is_finite().then_some(value)
}

pub(in crate::geometry) fn interpolate_optional(
    start: Option<f64>,
    end: Option<f64>,
    fraction: f64,
) -> Option<f64> {
    match (start, end) {
        (Some(start), Some(end)) => Some(interpolate_f64(start, end, fraction)),
        _ => None,
    }
}
