//! Semantic values for periodic geographic coordinates.

mod chain;
mod computed_edge;
mod envelope;
mod interval;
pub(crate) mod lonlat;
mod value;
mod view;

pub(crate) use chain::ChartSpanGuard;
pub(crate) use computed_edge::{
    ComputedEdgeIntent, ComputedEdgeInterval, ComputedEdgeSample, ComputedPublication,
    ComputedVertexIntent, publish_certified_minor_chain, publish_computed_chain,
    publish_computed_chain_with_vertex_intents,
    publish_computed_chain_with_vertex_intents_and_intervals,
    publish_computed_chain_with_vertex_intents_and_trace, edge_reads_correctly,
};
pub(crate) use envelope::GeographicSupportEnvelope;
pub(crate) use interval::{CircularInterval, LongitudeWindows, union_circular_intervals};
use thiserror::Error;
#[cfg(test)]
pub(crate) use value::source_admission_counts;
pub(crate) use value::{
    AngularFactor, CanonicalLongitude, ChartPole, ChartStep, EdgeDirection, FullTurnPole,
    GeodeticPole, GeographicAxisOrientation, GeographicFrame, Latitude, PeriodicAxis,
    QuotientVertex, RetainedChartDisplacement, RetainedLongitudeSite, RingTurn, SeamIdentity,
    TurnDirection,
};
pub(crate) use value::{RetainedNativeLongitude, retained_native_private};
#[cfg(test)]
pub(crate) use view::GeographicAdmissionWork;
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
