//! Semantic values for periodic geographic coordinates.

mod chain;
pub(crate) mod chart;
mod computed_edge;
mod envelope;
mod interval;
pub(crate) mod lonlat;
mod value;
mod view;

pub(crate) use chain::ChartSpanGuard;
pub(crate) use chart::{ChartPointSite, PointSelectionChart};
pub(crate) use computed_edge::{
    ComputedEdgeIntent, ComputedLongitudeRelation, ComputedSampleOrdinates, ComputedVertexIntent,
    publish_certified_minor_chain, publish_computed_chain,
    publish_computed_chain_with_vertex_intents,
    publish_computed_chain_with_vertex_intents_and_samples,
};
pub(crate) use envelope::{GeographicSupportEnvelope, GeographicSupportProducts};
pub(crate) use interval::{CircularInterval, LongitudeWindows, union_circular_intervals};
use thiserror::Error;
#[cfg(test)]
pub(crate) use value::source_admission_counts;
pub(crate) use value::{
    AngularFactor, CanonicalLongitude, ChartConnection, ChartPole, ChartStep, EdgeDirection,
    FullTurnPole, GeodeticPole, GeographicAxisOrientation, GeographicFrame, Latitude, PeriodicAxis,
    QuotientVertex, RetainedChartDisplacement, RetainedLongitudeClass, RetainedLongitudeSite,
    RetainedNativeLongitude, RingTurn, SeamIdentity, TurnDirection, retained_native_private,
};
pub(crate) use view::{
    GeographicAdmissionError, GeographicCollectionOwners, GeographicDiagnostic, GeographicInputs,
    GeographicLocation, GeographicRing, GeographicViewError, RingDomain, RingSummary,
    ScalarGeographicView, ring_gate,
};

#[derive(Clone, Copy, Debug, Error, PartialEq)]
pub(crate) enum GeographicError {
    #[error("{name} must be a positive finite angular factor, got {value}")]
    InvalidAngularFactor { name: &'static str, value: f64 },
    #[error("{name} must be finite, got {value}")]
    NonFinite { name: &'static str, value: f64 },
    #[error("latitude {value} is outside the physical axis limits")]
    LatitudeOutOfRange { value: f64 },
    #[error("ring chart-turn count overflowed")]
    ChartTurnOverflow,
    #[error(
        "ring winds around the globe and spans the equator (latitudes {south} to {north}); the pole it bounds is ambiguous. Close it explicitly through the pole line or split it"
    )]
    AmbiguousFullTurnRing { south: f64, north: f64 },
}

#[cfg(test)]
mod tests;
