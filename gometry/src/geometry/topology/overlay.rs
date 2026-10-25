//! Exact n-ary geographic set selection and output emission over global Q.

#[cfg(test)]
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use self::recipes::diagnostic_line_paths;
pub(in crate::geometry::topology) use self::recipes::{
    build_fragment_recipes, push_source_ring_recipes,
};
use self::select_boundaries::selected_area_boundaries;
use self::select_faces::{AreaSupport, FaceUnion, append_unique_rows, face_regions, select_faces};
use self::select_lines::{select_line_spans, select_points};
use super::emission::draw::{boundary_source_points_with_work, source_point_coordinates_with_work};
use super::emission::line_paths::{
    boundary_cycle_line_paths, boundary_owner_index, line_owner_index, line_steps,
    planned_line_paths,
};
use super::emission::ordinates::node_ordinates;
pub(in crate::geometry) use super::emission::plan::QProducer;
use super::emission::plan::{
    BoundaryCycle, BoundarySupport, DiagnosticKind, LineStep, PlannedAreaRing, PlannedQLine,
    PolygonPlan, QEmissionPlan, QLineOrigin, QLinePrimitive, SelectedLineSpan, SelectedRegionFact,
    SelectedRegionPoles, SpanEndpoint,
};
use super::emission::sites::{EmittedVertex, QCoordinates, node_injectivity_collisions_with_work};
#[cfg(test)]
use super::emission::sites::{certify_node_injectivity, node_injectivity_collisions};
#[cfg(test)]
use super::emission::source_reuse::cycle_belongs_to_source_ring;
use super::emission::source_reuse::{
    source_cycle_start_port, source_cycles_by_ring, source_ring_cycles_cover_boundary,
    source_ring_cycles_follow_source,
};
use super::emission::{
    CompletedQDrawing, DirectedSpan, FragmentRecipes, RecipeWalkOwner, SealedSourceDrawing,
    SelectedUse, write_completed_native,
};
#[cfg(test)]
use super::emission::{PublicationInventory, RecipeWalk};
use super::exact::{ExactSourceRange, ExactSourceRing, SourceRowId};
use super::linework::{
    DiagnosticSpan, even_odd_face_selection, minimal_face_selection, noded_spans,
};
use super::q::{ExactGeographicTopology, GlobalFaceId, QDartId, QNodeId, row_locations};
#[cfg(test)]
use super::q_embedding::{NearestQCertificate, certify_q_embedding_nearest};
use super::q_embedding::{
    QEmbeddingCertification, certify_and_search_q_embedding, certify_nearest_ring_roles,
};
use crate::error::{ResourceError, Result};
use crate::geographic::ChartPole;
#[cfg(test)]
use crate::geometry::embedding_completion::storage::LiteralCheck;
use crate::geometry::embedding_search::SearchOutcome;
use crate::geometry::embedding_search::account::{EvaluationBudget, EvaluationOnly, SearchAccount};
use crate::geometry::ordinates::SourceArcMetricAccess;
use crate::geometry::overlay::OverlayOp;
use crate::geometry::publication::{PendingPublicationRow, finish_geographic};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{CoordinateAxes, ExpansionBudget, GeometryErrorKind, Loc, Shape};
#[cfg(test)]
use crate::geometry::{MOrdinate, XY, ZOrdinate};

#[cfg(test)]
std::thread_local! {
    static Q_OVERLAY_PLAN_BUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(in crate::geometry::topology) fn take_q_overlay_plan_build_count() -> usize {
    Q_OVERLAY_PLAN_BUILDS.with(|count| count.replace(0))
}

#[cfg(test)]
fn record_q_overlay_plan_build() {
    Q_OVERLAY_PLAN_BUILDS.with(|count| count.set(count.get().saturating_add(1)));
}

mod publish;
pub(crate) use self::producer_drawing::exact_geographic_polygonize_full_from_shapes;
pub(in crate::geometry) use self::producer_drawing::{
    exact_geographic_linework_drawing_from_shapes, exact_geographic_linework_from_shapes,
    exact_geographic_linework_pending_row,
};
#[cfg(test)]
use self::publish::publish_q_overlay;
pub(in crate::geometry::topology) use self::publish::resolve_q_embedding;
#[cfg(test)]
use self::publish::write_certified_q_emission;
#[cfg(test)]
use self::publish::{CertifiedQEmission, QEmbeddingChoice};
mod plan_boundary;
mod plan_linework;
mod plan_overlay;
pub(in crate::geometry) mod producer_drawing;
mod recipes;
pub(in crate::geometry::topology) mod select_boundaries;
pub(in crate::geometry::topology) mod select_faces;
pub(in crate::geometry::topology) mod select_lines;
pub(in crate::geometry::topology) use self::select_boundaries::coverage_area_boundaries;
pub(in crate::geometry::topology) use self::select_faces::{
    coverage_face_regions, selected_face_roots, selected_region_facts,
};

/// Select all dimensions from the one exact source chart and its global Q.
/// Rounding happens only after face, span, and node selection is complete.
pub(in crate::geometry) fn exact_geographic_overlay<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    publish::exact_geographic_overlay(topology, operation, declared_axes, strict, metric_access)
}

pub(in crate::geometry) fn exact_geographic_overlay_pending_row<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    logical_row: usize,
) -> Result<PendingPublicationRow> {
    publish::exact_geographic_overlay_pending_row(
        topology,
        operation,
        declared_axes,
        strict,
        metric_access,
        logical_row,
    )
}

pub(in crate::geometry) fn exact_geographic_boundary<M: crate::geometry::ChartMetric>(
    topology: &ExactGeographicTopology,
    declared_axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    publish::exact_geographic_boundary(topology, declared_axes, metric_access)
}

pub(in crate::geometry) fn exact_geographic_boundary_pending_row<
    M: crate::geometry::ChartMetric,
>(
    topology: &ExactGeographicTopology,
    declared_axes: CoordinateAxes,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
    logical_row: usize,
) -> Result<PendingPublicationRow> {
    publish::exact_geographic_boundary_pending_row(
        topology,
        declared_axes,
        metric_access,
        logical_row,
    )
}

const fn selects_support(
    count: usize,
    first: bool,
    row_count: usize,
    operation: OverlayOp,
) -> bool {
    match operation {
        OverlayOp::Union => count != 0,
        OverlayOp::Intersection => count == row_count,
        OverlayOp::Difference => first && count == 1,
        OverlayOp::SymmetricDifference => !count.is_multiple_of(2),
    }
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Result-returning tests use assertions for behavior and ? for fallible setup"
)]
mod tests {
    use std::collections::BTreeMap;

    use crate::geometry::LineSeq;
    use crate::geometry::embedding_completion::literals::ShapeLiteralLayout;
    use crate::geometry::embedding_completion::storage::FrozenLiteralBuffers;
    use crate::geometry::ordinates::NodeOrdinates;
    use crate::geometry::test_support::{
        argentina_uruguay_source_arc_polygons, polygon as fixture_polygon,
    };
    use crate::geometry::topology::emission::draw::{
        boundary_source_points, source_point_coordinates,
    };
    use crate::geometry::topology::emission::index_recipe_walks;
    use crate::geometry::topology::emission::plan::{QEmbeddingEmission, QEmissionDrawing};
    use crate::geometry::topology::emission::sites::{EmittedNodeKey, emitted_node_key};
    use crate::geometry::topology::emission::source_publication::SourcePublication;
    use crate::geometry::topology::emission::write::{
        SourcePathContext, WrittenDrawingParts, rotate_area_ring_vertices, seal_output_parts,
        write_drawing, write_ring,
    };
    use crate::geometry::topology::q::QPortId;
    use crate::geometry::topology::{ExactChartPoint, QSourceImage};
    use crate::{CoordSeq, Point, Polygon, Ring};
    include!("overlay/tests/support.rs");
    include!("overlay/plan_overlay/tests.rs");
    include!("overlay/recipes/tests.rs");
    include!("overlay/plan_linework/tests.rs");
    include!("emission/source_reuse/tests.rs");
    include!("overlay/q_embedding_tests.rs");
    include!("overlay/tests/ordinates.rs");
    include!("overlay/tests/source_publication.rs");
    include!("overlay/select_faces/tests.rs");
    include!("overlay/tests/ring_roles.rs");
    include!("emission/sites/tests.rs");
    include!("overlay/select_boundaries/tests.rs");
}

#[cfg(test)]
mod pooled_linework_tests;
