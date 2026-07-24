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

const _: () = assert!(std::mem::size_of::<Finite>() == std::mem::size_of::<f64>());
const _: () = assert!(std::mem::size_of::<NonNegative>() == std::mem::size_of::<f64>());
const _: () = assert!(std::mem::size_of::<Positive>() == std::mem::size_of::<f64>());
