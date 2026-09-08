//! Semantic values for periodic geographic coordinates.

mod interval;
mod value;
mod view;

pub(crate) use interval::{CircularInterval, LongitudeWindows, union_circular_intervals};
use thiserror::Error;
pub(crate) use value::{
    AngularFactor, CanonicalLongitude, GeographicFrame, Latitude, PeriodicAxis, RingTurn,
    SeamIdentity, TurnDirection,
};
pub(crate) use view::{
    GeographicDiagnostic, GeographicInputs, GeographicRing, GeographicViewError, RingDomain,
    RingSummary, ScalarGeographicView,
};

#[derive(Clone, Copy, Debug, Error, PartialEq)]
pub(crate) enum GeographicError {
    #[error("{name} must be a positive finite angular factor, got {value}")]
    InvalidAngularFactor { name: &'static str, value: f64 },
    #[error("{name} must be finite, got {value}")]
    NonFinite { name: &'static str, value: f64 },
    #[error("latitude {value} is outside the physical axis limits")]
    LatitudeOutOfRange { value: f64 },
    #[error("circular interval sweep {sweep} is outside [0, {period})")]
    InvalidSweep { sweep: f64, period: f64 },
    #[error("ring seam transit count overflowed")]
    SeamTransitOverflow,
}

#[cfg(test)]
mod tests;
