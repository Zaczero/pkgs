use std::collections::BTreeMap;
use std::ops::Range;

use self::plan::QEmissionPlan;
use super::exact::{
    ExactChartPoint, ExactQuotientKey, ExactSourceRange, FallibleAvl, SourceEdgeId,
    compare_quotient_keys, compare_quotient_keys_with_work, quotient_key_for_chart_position,
    quotient_key_for_chart_position_with_work,
};
use super::q::{
    CanonicalQNodeKey, ChartSpan, ChartSpanId, ExactGeographicTopology, GlobalFaceId, QDartId,
    QNodeId, QPortId,
};
use crate::error::{ResourceError, Result};
use crate::geographic::{
    ChartPole, ComputedEdgeIntent, ComputedSampleOrdinates, ComputedVertexIntent, GeographicFrame,
    Latitude, publish_computed_chain_with_vertex_intents_and_samples,
};
use crate::geometry::embedding_completion::literals::{
    LiteralPoint, LiteralPolygon, LiteralSequence, ShapeLiteralLayout,
};
use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
use crate::geometry::embedding_search::account::{EvaluationBudget, SearchAccount};
#[cfg(test)]
use crate::geometry::ordinates::AxisValue;
use crate::geometry::ordinates::{
    NodeOrdinates, RequestRowId, RequestRowInput, RootCatalog, RootId, RootRange, RootRequest,
    SourceArcMetricAccess, TraceRows, TraceRowsBuilder, resolve_request_rows,
};
pub(super) use crate::geometry::publication::ChartCutSide as ChartSide;
use crate::geometry::publication::{
    ChartRestriction, DirectedChartRestrictionUse, FinishedGeometry, SyntheticChartVertex,
};
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{
    ChartMetric, CoordSeq, CoordinateAxes, ExpansionBudget, GeometryErrorKind, MOrdinate, Point,
    Polygon, Ring, Shape, ZOrdinate,
};

mod builder;
pub(in crate::geometry::topology) mod cut_ports;
pub(in crate::geometry::topology) mod draw;
mod families;
pub(in crate::geometry::topology) mod line_paths;
mod materialize;
mod nodes;
pub(in crate::geometry::topology) mod ordinates;
pub(in crate::geometry::topology) mod plan;
mod port_authority;
pub(in crate::geometry::topology) mod prepare;
mod presentation;
mod publication;
mod recipes;
pub(in crate::geometry::topology) mod ring_roles;
pub(in crate::geometry::topology) mod sites;
pub(in crate::geometry::topology) mod source_publication;
pub(in crate::geometry::topology) mod source_reuse;
mod trace;
pub(in crate::geometry::topology) mod write;
pub(super) use self::builder::{EmissionBuilder, ExpandedEmissionPlan};
pub(in crate::geometry) use self::families::FeatureFamilyRegistry;
pub(super) use self::families::{
    FeatureFamily, FeatureFamilyBounds, FeatureFamilyParent, NativeCutFeature, SampleProducerKind,
    native_cut_features,
};
use self::materialize::NativeChordPoints;
#[cfg(test)]
pub(super) use self::materialize::NativePolygonLiteralUses;
pub(in crate::geometry) use self::materialize::QSourceImage;
pub(super) use self::materialize::{
    CompletedNativeDrawing, NativeFrozenDrawing, NativeMaterializedParts, SourceFrozenDrawing,
    materialize_native_diagnostic_lines, materialize_native_parts, write_completed_native,
};
pub(in crate::geometry) use self::nodes::EmissionPortId;
pub(super) use self::nodes::{
    ChartPartId, EmissionChord, EmissionChordId, EmissionNode, EmissionNodeId,
    EmissionNodeRegistry, EmissionPart, EmissionPort, NativeFaceId, NodeAuthority,
    OccurrenceSource, PortPosition, PortSpelling, VertexOrigin, WrittenOccurrence,
};
use self::ordinates::native_chord_points;
pub(super) use self::ordinates::{
    native_port_point, resolve_emission_ordinates, resolve_publication_ordinates,
    source_root_ranges_at_location,
};
pub(super) use self::port_authority::{
    NativePortData, merge_native_port_data, native_source_record_quotient_key,
};
pub(super) use self::presentation::{
    AssignedSite, NativeAreaEdge, NativeAreaEdgeKind, NativeGraphEdgeOwner, NativeSeamEvent,
    NativeSeamVertices, NativeSourceFaceGerm, NativeSourceSpanUse, SelectedNativeSeamBand,
    native_pole_edges, native_pole_walk_edges, native_seam_edges,
};
pub(super) use self::publication::FragmentRecipes;
pub(super) use self::recipes::{
    DirectedSourceRange, FragmentRecipe, FragmentRecipeId, PublicationInventory, PublishedFragment,
    PublishedSample, SealedQDrawing, SealedSourceDrawing, SelectedUse,
};
use self::recipes::{
    EndpointSide, PendingSourceRange, interpolate_exact_reference_coordinate,
    interpolate_exact_reference_point, pending_source_ranges, producer_ranges,
    producer_step_extends_run,
};
pub(in crate::geometry) use self::trace::SourceTrace;
use self::trace::SourceTraceSample;
pub(super) use self::trace::{
    DirectedSpan, RecipeWalk, RecipeWalkIndex, RecipeWalkOwner, SourceSampleOccurrence,
    TraceRequestLocation, index_recipe_walks, native_source_sample_occurrences, next_recipe_walk,
};

#[cfg(test)]
mod tests {
    use crate::geometry::topology::emission::families::{
        FeatureFamilyBound, NativeCutOccurrenceInterval, NativeCutOccurrenceRange,
    };
    use crate::geometry::topology::emission::materialize::NativeLiteralUses;
    use crate::geometry::topology::emission::presentation::directed_chart_restriction_use;
    use crate::geometry::topology::emission::trace::TracedSourceSample;
    include!("emission/tests/support.rs");
    include!("emission/ordinates/tests.rs");
    include!("emission/port_authority/tests.rs");
    include!("emission/publication/tests.rs");
    include!("emission/recipes/tests.rs");
    include!("emission/presentation/tests.rs");
    include!("emission/families/tests.rs");
    include!("emission/materialize/tests.rs");
    include!("emission/trace/tests.rs");
}
