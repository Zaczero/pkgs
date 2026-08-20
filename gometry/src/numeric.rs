//! Validated `f64` newtypes: zero-cost `repr(transparent)` wrappers that reject
//! invalid scalar inputs at construction time via the canonical
//! [`GeometryErrorKind`](crate::geometry::GeometryErrorKind) constructors.

use std::ops::Deref;

use crate::error::Result;
use crate::geometry::GeometryErrorKind;

/// A finite `f64`.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Finite(f64);

/// A finite non-negative `f64` (`>= 0.0`).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct NonNegative(f64);

/// A finite positive `f64` (`> 0.0`).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Positive(f64);

/// Validated reciprocal of a projected axis's linear unit-to-metre factor.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct AxisScale(f64);

impl Finite {
    pub(crate) fn try_new(name: &'static str, value: f64) -> Result<Self> {
        if value.is_finite() {
            Ok(Self(value))
        } else {
            Err(GeometryErrorKind::finite(name, value))
        }
    }

    pub(crate) const fn get(self) -> f64 {
        self.0
    }
}

impl NonNegative {
    pub(crate) fn try_new(name: &'static str, value: f64) -> Result<Self> {
        if value.is_finite() && value >= 0.0 {
            Ok(Self(value))
        } else {
            Err(GeometryErrorKind::non_negative_finite(name, value))
        }
    }

    pub(crate) const fn get(self) -> f64 {
        self.0
    }
}

impl Positive {
    pub(crate) fn try_new(name: &'static str, value: f64) -> Result<Self> {
        if value.is_finite() && value > 0.0 {
            Ok(Self(value))
        } else {
            Err(GeometryErrorKind::positive_finite(name, value))
        }
    }

    pub(crate) const fn get(self) -> f64 {
        self.0
    }
}

impl AxisScale {
    pub(crate) const IDENTITY: Self = Self(1.0);

    pub(crate) fn from_unit_to_metre(unit_to_metre: f64) -> Option<Self> {
        if !unit_to_metre.is_finite() || unit_to_metre <= 0.0 {
            return None;
        }
        let inverse = 1.0 / unit_to_metre;
        inverse.is_finite().then_some(Self(inverse))
    }

    pub(crate) const fn inverse(self) -> f64 {
        self.0
    }

    /// Undo a framed coordinate without overflowing a shared false origin.
    ///
    /// The ordinary expression `value / scale - origin` can overflow while
    /// both the framed value and the final residual are finite.  Subtracting
    /// in the scaled frame keeps that common case finite; the alternate form
    /// is retained for the genuinely overflowing subtraction case.
    pub(crate) fn unframe(self, value: f64, origin: f64) -> f64 {
        let scaled_origin = origin * self.0;
        let result = if scaled_origin.is_finite() {
            let delta = value - scaled_origin;
            if delta.is_finite() {
                delta / self.0
            } else {
                value / self.0 - origin
            }
        } else {
            value / self.0 - origin
        };
        debug_assert!(
            result.is_finite(),
            "finite framed coordinate residual must remain finite"
        );
        result
    }
}

impl Deref for Finite {
    type Target = f64;

    fn deref(&self) -> &f64 {
        &self.0
    }
}

impl Deref for NonNegative {
    type Target = f64;

    fn deref(&self) -> &f64 {
        &self.0
    }
}

impl Deref for Positive {
    type Target = f64;

    fn deref(&self) -> &f64 {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::AxisScale;

    #[test]
    fn unframe_keeps_finite_shared_origins_finite() {
        let scale = AxisScale(1e-100);
        let origin = 1e100;
        let framed = (origin + 1.0) * scale.0;
        assert!(scale.unframe(framed, origin).is_finite());

        let adjacent = f64::from_bits(origin.to_bits() + 1);
        let adjacent_framed = adjacent * scale.0;
        assert!(scale.unframe(adjacent_framed, adjacent).is_finite());
    }

    #[test]
    fn unframe_uses_scaled_delta_for_opposite_extremes() {
        let scale = AxisScale(1e-200);
        let value = -1e307;
        let origin = 1e307;
        let residual = scale.unframe(value * scale.0, origin);
        assert!(residual.is_finite());
        assert!(residual < 0.0);
    }
}
