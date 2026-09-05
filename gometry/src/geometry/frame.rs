//! Shared anisotropic power-of-two affine frames.
//!
//! A single rescue scale selected from the largest coordinate destroys an
//! independent reciprocal axis (`1e300 × 1e-300` is the canonical case).
//! This owner keeps each residual axis normal without changing stored-double
//! topology.  Consumers that need an area undo the two exponents together;
//! consumers that only need a robust predicate retain the framed values.

use crate::geometry::exact_arith::{exact_power_of_two, floor_binary_exponent};
use crate::geometry::{Point, XY};

/// Positive power-of-two scale that maps a finite nonzero magnitude close to
/// one.  Tiny axes scale up and huge axes scale down.
pub(crate) fn axis_pow2_scale(max_abs: f64) -> f64 {
    if max_abs <= 0.0 || !max_abs.is_finite() {
        return 1.0;
    }
    let scale_exp = (-floor_binary_exponent(max_abs)).clamp(-1022, 1023);
    exact_power_of_two(i16::try_from(scale_exp).expect("axis scale exponent fits i16"))
}

/// Residual in a power-of-two frame.
///
/// A finite world-space delta is the most accurate value and must be formed
/// before scaling.  Only an overflowing subtraction uses the scaled fallback;
/// scaling first would turn equal or adjacent huge origins into a less exact
/// residual and can change the topology of the framed computation.
pub(crate) fn scaled_residual(value: f64, origin: f64, scale: f64) -> f64 {
    let delta = value - origin;
    if delta.is_finite() {
        delta * scale
    } else {
        value * scale - origin * scale
    }
}

pub(crate) fn scale_by_power_of_two(mut value: f64, mut exponent: i32) -> f64 {
    while exponent != 0 {
        let step = exponent.clamp(-1022, 1023);
        let factor = exact_power_of_two(i16::try_from(step).expect("scale step fits i16"));
        value *= factor;
        exponent -= step;
    }
    value
}

pub(crate) fn power_of_two_exponent(scale: f64) -> i32 {
    debug_assert!(scale.is_normal() && scale.is_sign_positive());
    floor_binary_exponent(scale)
}

/// Undo an area in one exponent operation.  Separately unscaling the axes can
/// round an otherwise-normal final area through a subnormal intermediate.
pub(crate) fn unscale_area(value: f64, scale_x: f64, scale_y: f64) -> f64 {
    scale_by_power_of_two(
        value,
        -power_of_two_exponent(scale_x) - power_of_two_exponent(scale_y),
    )
}

/// Maximum finite residual magnitude from `origin`.
///
/// When a finite subtraction overflows, return the maximum finite operand
/// magnitude instead so [`scaled_residual`] can scale both operands first.
pub(crate) fn axis_extent(values: impl IntoIterator<Item = f64>, origin: f64) -> Option<f64> {
    let mut extent = 0.0_f64;
    let mut absolute = origin.abs();
    let mut overflowed = false;
    for value in values {
        if !value.is_finite() {
            return None;
        }
        absolute = absolute.max(value.abs());
        let delta = (value - origin).abs();
        if delta.is_finite() {
            extent = extent.max(delta);
        } else {
            overflowed = true;
        }
    }
    Some(if overflowed { absolute } else { extent })
}

/// A translation plus independent exact binary scales for the two axes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AxisFrame {
    origin: Point,
    scale_x: f64,
    scale_y: f64,
}

/// A translation plus one exact binary scale shared by both axes.
///
/// This is deliberately a different type from [`AxisFrame`]: a Euclidean
/// similarity preserves metric structure, while an anisotropic frame is only
/// an arithmetic coordinate system. In particular, reciprocal-axis admission
/// is not a meaningful question for this type and no such method exists.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SimilarityFrame {
    origin: Point,
    scale: f64,
}

impl SimilarityFrame {
    pub(crate) fn from_points(points: &[Point]) -> Option<Self> {
        let origin = *points.first()?;
        let extent_x = axis_extent(points.iter().map(|point| point.x), origin.x)?;
        let extent_y = axis_extent(points.iter().map(|point| point.y), origin.y)?;
        ((extent_x > 0.0) || (extent_y > 0.0)).then_some(Self {
            origin,
            scale: axis_pow2_scale(extent_x.max(extent_y)),
        })
    }

    pub(crate) const fn scale(self) -> f64 {
        self.scale
    }

    pub(crate) fn frame_xy(self, x: f64, y: f64) -> XY {
        XY::new(
            scaled_residual(x, self.origin.x, self.scale),
            scaled_residual(y, self.origin.y, self.scale),
        )
    }
}

impl AxisFrame {
    /// Frame a non-empty point set at its first stored point.  `None` means
    /// every point is coincident (there is no meaningful rescue frame).
    pub(crate) fn from_points(points: &[Point]) -> Option<Self> {
        let origin = *points.first()?;
        let extent_x = axis_extent(points.iter().map(|point| point.x), origin.x)?;
        let extent_y = axis_extent(points.iter().map(|point| point.y), origin.y)?;
        ((extent_x > 0.0) || (extent_y > 0.0)).then_some(Self {
            origin,
            scale_x: axis_pow2_scale(extent_x),
            scale_y: axis_pow2_scale(extent_y),
        })
    }

    /// Construct directly from a caller-selected stable origin and axis
    /// drivers.  A zero driver deliberately keeps that axis unscaled.
    pub(crate) fn from_origin_extents(origin: Point, extent_x: f64, extent_y: f64) -> Option<Self> {
        (origin.x.is_finite()
            && origin.y.is_finite()
            && extent_x.is_finite()
            && extent_y.is_finite()
            && (extent_x > 0.0 || extent_y > 0.0))
            .then_some(Self {
                origin,
                scale_x: axis_pow2_scale(extent_x),
                scale_y: axis_pow2_scale(extent_y),
            })
    }

    pub(crate) const fn scale_x(self) -> f64 {
        self.scale_x
    }

    pub(crate) const fn scale_y(self) -> f64 {
        self.scale_y
    }

    pub(crate) fn frame_xy(self, x: f64, y: f64) -> XY {
        XY::new(
            scaled_residual(x, self.origin.x, self.scale_x),
            scaled_residual(y, self.origin.y, self.scale_y),
        )
    }

    pub(crate) fn frame_point(self, point: Point) -> Point {
        let xy = self.frame_xy(point.x, point.y);
        Point::new_raw_xy(xy.x, xy.y)
    }

    pub(crate) fn unframe_xy(self, point: XY) -> XY {
        XY::new(
            (self.origin.x * self.scale_x + point.x) / self.scale_x,
            (self.origin.y * self.scale_y + point.y) / self.scale_y,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn axis_scale_uses_the_exact_binary_exponent_at_boundaries() {
        assert_eq!(
            axis_pow2_scale(0.25_f64.next_down()).to_bits(),
            8.0_f64.to_bits()
        );
        assert_eq!(
            axis_pow2_scale(f64::from_bits((1_u64 << 52) - 1)).to_bits(),
            2.0_f64.powi(1023).to_bits()
        );
    }

    #[test]
    fn axis_scale_keeps_zero_and_nonfinite_inputs_as_identity() {
        for value in [0.0, -0.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert_eq!(axis_pow2_scale(value).to_bits(), 1.0_f64.to_bits());
        }
    }

    #[test]
    fn overflowing_finite_residuals_keep_a_finite_scaled_operand_pair() {
        let magnitude = 1.0e308_f64;
        let extent = axis_extent([magnitude], -magnitude).expect("finite input");
        let scale = axis_pow2_scale(extent);
        assert!(scaled_residual(magnitude, -magnitude, scale).is_finite());
    }
}
