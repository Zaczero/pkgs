//! Semantic values for periodic geographic coordinates.

mod envelope;
mod interval;
pub(crate) mod lonlat;
mod value;
mod view;

pub(crate) use envelope::GeographicSupportEnvelope;
pub(crate) use interval::{CircularInterval, LongitudeWindows, union_circular_intervals};
use thiserror::Error;
pub(crate) use value::{
    AngularFactor, CanonicalLongitude, ChartStep, EdgeDirection, FullTurnPole, GeographicFrame,
    Latitude, PeriodicAxis, Pole, QuotientVertex, RingTurn, SeamIdentity, TurnDirection,
};
#[cfg(test)]
pub(crate) use view::GeographicAdmissionWork;
pub(crate) use view::{
    GeographicAdmissionError, GeographicCollectionOwners, GeographicDiagnostic, GeographicInputs,
    GeographicRing, GeographicViewError, RingDomain, RingSummary, ScalarGeographicView,
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
    #[error("ring chart-turn count overflowed")]
    ChartTurnOverflow,
    #[error(
        "ring winds once around the globe and spans the equator (latitudes {south} to {north}); the pole it bounds is ambiguous. Close it explicitly through the pole line or split it"
    )]
    AmbiguousFullTurnRing { south: f64, north: f64 },
}

#[cfg(test)]
mod tests;
