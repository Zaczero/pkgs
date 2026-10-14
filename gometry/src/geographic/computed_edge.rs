use super::{
    ChartStep, EdgeDirection, GeographicError, GeographicFrame, RetainedChartDisplacement,
    RetainedLongitudeClass,
};
use crate::error::Result;
use crate::geographic::retained_exact::edges::{
    retained_computed_samples_connection, retained_edge_from_chart_witness,
    retained_edge_from_lifted_binary64_pair, retained_exact_interval_class,
    retained_exact_publisher_sample, retained_exact_split_piece_count,
    retained_native_interval_class, retained_native_publisher_sample,
    retained_native_split_piece_count,
};
use crate::geographic::retained_exact::sites::RetainedComputedSample;
#[cfg(test)]
use crate::geometry::interpolate_optional_ordinate;
use crate::geometry::{
    CoordSeq, Coordinates, ExpansionBudget, GeometryErrorKind, MOrdinate, Point, ZOrdinate,
    interpolate_subdivision_ordinate,
};

mod aliases;
mod intents;
mod publish;
mod vertices;

pub(crate) use intents::{
    ComputedEdgeIntent, ComputedLongitudeRelation, ComputedSampleOrdinates, ComputedVertexIntent,
};
pub(crate) use publish::{
    publish_certified_minor_chain, publish_computed_chain,
    publish_computed_chain_with_vertex_intents,
    publish_computed_chain_with_vertex_intents_and_samples,
    publish_computed_chain_with_vertex_intents_and_samples_with_edge_ranges,
    publish_preserved_minor_chain,
};

#[cfg(test)]
mod tests;
